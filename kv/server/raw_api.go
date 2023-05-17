package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(&kvrpcpb.Context{})
	if err != nil {
		return nil, err
	}
	val, err := reader.GetCF(req.GetCf(), req.GetKey())
	var resp kvrpcpb.RawGetResponse
	if err != nil {
		log.Debug("don't have the key", string(req.Key))
		resp.Error = err.Error()
		return nil, err
	}
	resp.NotFound = val == nil // val == nil , it's not found[see GetCF]
	resp.Value = val           // has value
	log.Debug("get value %v", string(resp.Value))
	return &resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	var batch []storage.Modify
	batch = append(batch, storage.Modify{
		Data: storage.Put{
			Key:   req.GetKey(),
			Value: req.GetValue(),
			Cf:    req.Cf,
		},
	})
	var resp kvrpcpb.RawPutResponse
	if err := server.storage.Write(&kvrpcpb.Context{}, batch); err != nil {
		resp.Error = err.Error()
	}
	return &resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	var batch []storage.Modify
	batch = append(batch, storage.Modify{
		Data: storage.Delete{
			Key: req.GetKey(),
			Cf:  req.Cf,
		},
	})
	var resp kvrpcpb.RawDeleteResponse
	if err := server.storage.Write(&kvrpcpb.Context{}, batch); err != nil {
		resp.Error = err.Error()
	}
	return &resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(&kvrpcpb.Context{})
	if err != nil {
		return nil, err
	}
	iter := reader.IterCF(req.GetCf())
	defer iter.Close()

	var resp kvrpcpb.RawScanResponse
	var KvPairs []*kvrpcpb.KvPair
	for iter.Seek(req.GetStartKey()); iter.Valid(); iter.Next() {
		if len(KvPairs)+1 > int(req.GetLimit()) {
			break
		}

		item := iter.Item()
		k := item.Key()
		log.Debugf("key=%s\n", k)
		dst := make([]byte, item.ValueSize())
		valueCopy, err := item.ValueCopy(dst)
		if err != nil {
			panic(err)
		}
		pair := kvrpcpb.KvPair{}
		pair.Key = k
		pair.Value = valueCopy

		KvPairs = append(KvPairs, &pair)
	}
	resp.Kvs = KvPairs
	return &resp, nil
}
