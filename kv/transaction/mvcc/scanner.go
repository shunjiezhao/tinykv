package mvcc

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	nextKey []byte
	txn     *MvccTxn
	iter    engine_util.DBIterator
	done    bool
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	scanner := Scanner{
		nextKey: startKey,
		txn:     txn,
	}
	scanner.iter = txn.Reader.IterCF(engine_util.CfWrite)
	return &scanner
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	/*
		1. if don't have next return
		2. seek and find next key
		3. if userKey(next key) != nextKey, set next key = userKey goto step 1
		4. find the first key != userKey, if exhausted set done = true
										  if find set next key = find key
		5. get default value if write.Kind == delete, value is nil
		6. return
	*/
	if scan.done {
		return nil, nil, nil
	}
	scan.iter.Seek(EncodeKey(scan.nextKey, scan.txn.StartTS))
	if !scan.iter.Valid() {
		return nil, nil, nil
	}
	item := scan.iter.Item()
	userKey := DecodeUserKey(item.KeyCopy(nil))
	if !bytes.Equal(userKey, scan.nextKey) {
		scan.nextKey = userKey // to find userKey
		return scan.Next()
	}

	for { // find next key != userKey
		scan.iter.Next()
		if !scan.iter.Valid() {
			scan.done = true
			break // not don't have next
		}

		nextKey := DecodeUserKey(scan.iter.Item().KeyCopy(nil))
		if !bytes.Equal(nextKey, userKey) {
			scan.nextKey = nextKey
			break // find next key != userKey
		}
	}

	value, err := item.ValueCopy(nil)
	if err != nil {
		return nil, nil, err
	}
	write, err := ParseWrite(value)
	if err != nil {
		return nil, nil, err
	}
	if write.Kind == WriteKindDelete {
		return userKey, nil, nil
	}
	value, err = scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(userKey, write.StartTS))
	if err != nil {
		return nil, nil, err
	}
	return userKey, value, nil

}
