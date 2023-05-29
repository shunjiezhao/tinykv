package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/log"
	"go.uber.org/zap"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (resp *kvrpcpb.GetResponse, err error) {
	/*
		1. get latch lock ( avoid client concurrent read)
		2. new transaction with version
		3. get lock ( check if have prev lock not release)
		4. get value if not set not found
	*/
	// Your Code Here (4B).
	resp = &kvrpcpb.GetResponse{}
	keysToLatch := [][]byte{req.GetKey()}
	lk := server.Latches.AcquireLatches(keysToLatch)
	if lk != nil {
		log.Error(string(req.GetKey()), " is locked")
		resp.Error = &kvrpcpb.KeyError{Locked: &kvrpcpb.LockInfo{}} // locked
		return
	}
	defer server.Latches.ReleaseLatches(keysToLatch)

	storageReader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		log.Error("get storageReader failed", err)
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return
	}
	txn := mvcc.NewMvccTxn(storageReader, req.GetVersion())
	lock, err := server.getLock(txn, req.GetKey(), req.GetVersion())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return resp, err
	}
	if lock != nil {
		resp.Error = &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				Key:         req.GetKey(),
				LockTtl:     lock.Ttl,
			},
		}

		return resp, nil
	}

	resp.Value, err = txn.GetValue(req.GetKey())
	if err != nil {
		log.Error("get cf failed")
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return resp, err
	}
	if len(resp.Value) == 0 {
		log.Info(zap.String("key", string(req.GetKey())), "not found")
		resp.NotFound = true
		return
	}
	return
}

func (server *Server) getLock(txn *mvcc.MvccTxn, key []byte, ts uint64) (*mvcc.Lock, error) {
	lock, err := txn.GetLock(key)
	if err != nil {
		log.Error("get lock failed")
		return nil, err
	}
	// lock == nil  means no lock
	if lock != nil && lock.Ts < ts {
		log.Error(string(key), "have prev lock not release")
		return lock, nil
	}
	return nil, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (resp *kvrpcpb.PrewriteResponse, err error) {
	// Your Code Here (4B).
	/*
		1.set latch lock
		2.get most recent record ( find prev commit ts < prev lock ts)
		3.validate all lock
		4.set default value and set lock
	*/
	resp = &kvrpcpb.PrewriteResponse{}
	var keysToLatch [][]byte
	for _, m := range req.GetMutations() {
		keysToLatch = append(keysToLatch, m.Key)
	}
	lk := server.Latches.AcquireLatches(keysToLatch)
	if lk != nil {
		log.Error("keys is locked")
		return
	}
	defer server.Latches.ReleaseLatches(keysToLatch)

	storageReader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		log.Error("get storageReader failed", err)
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return
	}
	txn := mvcc.NewMvccTxn(storageReader, req.GetStartVersion())

	for _, key := range keysToLatch {
		write, commitTs, err := txn.MostRecentWrite(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return resp, err
		}
		if write != nil && commitTs >= req.GetStartVersion() {
			log.Error("conflict")
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    write.StartTS,
					ConflictTs: commitTs,
					Key:        key,
				},
			})
			return resp, nil
		}
	}

	for _, m := range req.GetMutations() {
		if lk, err := server.getLock(txn, m.Key, req.GetStartVersion()); err != nil || lk != nil {
			if lk != nil {
				resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
					Conflict: &kvrpcpb.WriteConflict{
						StartTs:    lk.Ts,
						ConflictTs: lk.Ts,
						Key:        m.Key,
						Primary:    lk.Primary,
					},
				})
			}
			if err != nil {
				if regionErr, ok := err.(*raft_storage.RegionError); ok {
					resp.RegionError = regionErr.RequestErr
					return resp, nil
				}
			}
			return resp, nil
		}
	}
	for _, mutation := range req.GetMutations() { // set delete value
		switch mutation.GetOp() {
		case kvrpcpb.Op_Put:
			txn.PutValue(mutation.Key, mutation.Value)
		case kvrpcpb.Op_Del:
			txn.DeleteValue(mutation.Key)
		}
		txn.PutLock(mutation.Key, &mvcc.Lock{
			Primary: req.GetPrimaryLock(),
			Ts:      req.GetStartVersion(),
			Ttl:     req.GetLockTtl(),
			Kind:    mvcc.WriteKindFromProto(mutation.Op),
		})
	}
	err = server.storage.Write(req.GetContext(), txn.Writes())
	return resp, err
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	/*
		1.set latch lock
		2. check recommitting a transaction ( check current write start ts == req.start ts)
		3.validate lock is exist ( maybe ttl is timeout)
		3.1 if don't exist, return error
		3.2 if exist, check startTs == req.startTs, if not may be conflict
		4. del lock
		5. set write
	*/
	resp := &kvrpcpb.CommitResponse{}
	var keysToLatch [][]byte
	for _, key := range req.Keys {
		keysToLatch = append(keysToLatch, key)
	}
	lk := server.Latches.AcquireLatches(keysToLatch)
	if lk != nil {
		log.Error("keys is locked")
		return resp, nil
	}
	defer server.Latches.ReleaseLatches(keysToLatch)

	storageReader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		log.Error("get storageReader failed", err)
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}

	txn := mvcc.NewMvccTxn(storageReader, req.GetStartVersion())
	for _, key := range req.Keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			log.Error("get value failed", err)
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return resp, err
		}
		if write != nil && write.Kind != mvcc.WriteKindRollback && write.StartTS == req.GetStartVersion() {
			return resp, nil
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			log.Error("get value failed", err)
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return resp, err
		}

		if lock == nil || lock.Ts != req.GetStartVersion() { // conflict not this transaction
			resp.Error = &kvrpcpb.KeyError{}
			log.Error("lock is not exist")
			resp.Error.Retryable = "true"
			return resp, nil
		}
		txn.DeleteLock(key)
		txn.PutWrite(key, req.GetCommitVersion(), &mvcc.Write{
			StartTS: req.GetStartVersion(),
			Kind:    lock.Kind,
		})
	}

	err = server.storage.Write(req.GetContext(), txn.Writes())
	return resp, err
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	/*
		1. get scanner
		2. next
		3. if don't get key, return ( exhausted )
		4. if get lock set err, key and continue
		5. construct KvPair
	*/

	resp := &kvrpcpb.ScanResponse{}
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		log.Error("get storageReader failed", err)
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.GetVersion())
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()
	var key, value []byte
	for i := uint32(0); i < req.GetLimit(); {
		key, value, err = scanner.Next()
		if err != nil {
			log.Error("scanner next failed", err)
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		if key == nil {
			break
		}

		lock, err := txn.GetLock(key)
		if err != nil {
			log.Error("get lock failed", err)
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		if lock != nil && lock.Ts <= req.GetVersion() {
			resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{
					Locked: &kvrpcpb.LockInfo{
						PrimaryLock: lock.Primary,
						LockVersion: lock.Ts,
						Key:         key,
						LockTtl:     lock.Ttl,
					},
				},
				Key: key,
			})
			i++
			continue
		}
		if value != nil { // if not delete
			resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{
				Key:   key,
				Value: value,
			})
			i++
		}
	}
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		log.Error("get storageReader failed", err)
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.GetLockTs())
	write, commitTs, err := txn.CurrentWrite(req.GetPrimaryKey())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	if write != nil && write.Kind != mvcc.WriteKindRollback {
		resp.CommitVersion = commitTs
		return resp, nil
	}
	lock, err := txn.GetLock(req.GetPrimaryKey())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	if lock == nil { // 没有锁
		resp.Action = kvrpcpb.Action_LockNotExistRollback
		txn.PutWrite(req.GetPrimaryKey(), req.GetLockTs(), &mvcc.Write{
			StartTS: req.GetLockTs(),
			Kind:    mvcc.WriteKindRollback,
		})
		return resp, server.storage.Write(req.GetContext(), txn.Writes())
	}
	if mvcc.PhysicalTime(lock.Ts)+lock.Ttl <= mvcc.PhysicalTime(req.GetCurrentTs()) { // 超时
		txn.DeleteLock(req.GetPrimaryKey())
		txn.DeleteValue(req.GetPrimaryKey())
		txn.PutWrite(req.GetPrimaryKey(), req.GetLockTs(), &mvcc.Write{
			StartTS: req.GetLockTs(),
			Kind:    mvcc.WriteKindRollback,
		}) // roll back

		resp.Action = kvrpcpb.Action_TTLExpireRollback
	}
	return resp, server.storage.Write(req.GetContext(), txn.Writes())
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.BatchRollbackResponse{}
	reader, _ := server.storage.Reader(req.GetContext())
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())
	for _, key := range req.GetKeys() {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, server.storage.Write(req.GetContext(), txn.Writes())
			}
			return nil, err
		}
		if write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				continue
			} else {
				resp.Error = &kvrpcpb.KeyError{Abort: "true"}
				return resp, nil
			}
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, server.storage.Write(req.GetContext(), txn.Writes())
			}
			return nil, err
		}
		if lock == nil || lock.Ts != req.GetStartVersion() { // 不是当前事务的锁
			txn.PutWrite(key, req.GetStartVersion(), &mvcc.Write{
				StartTS: req.GetStartVersion(),
				Kind:    mvcc.WriteKindRollback,
			}) // roll back
			continue
		}
		txn.DeleteLock(key)
		txn.DeleteValue(key)
		txn.PutWrite(key, req.GetStartVersion(), &mvcc.Write{
			StartTS: req.GetStartVersion(),
			Kind:    mvcc.WriteKindRollback,
		}) // roll back
	}
	return resp, server.storage.Write(req.GetContext(), txn.Writes())
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
