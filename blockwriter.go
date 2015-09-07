package boltfs

import (
	"encoding/binary"
)

type blockWriter struct {
	txFn    func(func(tx Transaction) error) error
	path    BucketPath
	block   uint64
	written int64
}

func (i *blockWriter) Blocks() int {
	return int(i.block)
}
func (i *blockWriter) Written() int64 {
	return i.written
}
func (i *blockWriter) Write(p []byte) (int, error) {
	blockID := make([]byte, 8)
	binary.LittleEndian.PutUint64(blockID, i.block)
	err := i.txFn(func(tx Transaction) error {
		return i.path.BucketFrom(tx).Put(blockID, p)
	})
	if err != nil {
		return 0, err
	}
	i.block++
	plen := len(p)
	i.written += int64(plen)
	return plen, nil
}
