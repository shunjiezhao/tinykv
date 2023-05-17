package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	storeDB := engine_util.CreateDB(conf.DBPath, false)
	//raftDB := engine_util.CreateDB(conf.DBPath, conf.Raft)

	var storage StandAloneStorage
	storage.engine = engine_util.NewEngines(storeDB, nil, conf.DBPath, conf.DBPath)
	return &storage
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engine.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	reader := &StandaloneReader{}
	reader.txn = s.engine.Kv.NewTransaction(false)
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	if err := s.engine.Kv.Update(func(txn *badger.Txn) error {
		for _, op := range batch {
			storeKey := engine_util.KeyWithCF(op.Cf(), op.Key())
			var err error
			if op.Value() == nil { // DELETE
				err = txn.Delete(storeKey)
			} else { // PUT
				err = txn.Set(storeKey, op.Value())
			}
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		log.Debugf("write on storage is error %v", err)
		return err
	}
	return nil
}
