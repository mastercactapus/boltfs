package boltfs

import (
	"encoding/binary"
	"github.com/boltdb/bolt"
)

type blockWriter struct {
	fs       *boltFs
	id       []byte
	block    uint64
	lastSize int
}

func (i *blockWriter) Blocks() int {
	return int(i.block - 1)
}
func (i *blockWriter) LastSize() int {
	return i.lastSize
}
func (i *blockWriter) Write(p []byte) (int, error) {
	blockID := make([]byte, 8)
	binary.LittleEndian.PutUint64(blockID, i.block)
	err := i.fs.db.Batch(func(tx *bolt.Tx) error {
		bk := tx.Bucket(i.fs.bucket)
		bk = bk.Bucket([]byte(inodesKey))
		bk = bk.Bucket(i.id)
		return bk.Put(blockID, p)
	})
	if err != nil {
		return 0, err
	}
	i.block++
	i.lastSize = len(p)
	return i.lastSize, nil
}
