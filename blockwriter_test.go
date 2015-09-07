package boltfs

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

type bwMockTx struct {
	path BucketPath
	id   []byte
	data []byte
}

func (t *bwMockTx) Reset() {
	t.path = make(BucketPath, 0, 10)
	t.id = nil
	t.data = nil
}
func (t *bwMockTx) Bucket(key []byte) Bucket {
	t.path = append(t.path, key)
	return t
}
func (t *bwMockTx) Put(key, val []byte) error {
	t.id = key
	t.data = val
	return nil
}
func (t *bwMockTx) Get([]byte) []byte {
	panic("not implemented")
}
func (t *bwMockTx) CreateBucket([]byte) (Bucket, error) {
	panic("not implemented")
}
func (t *bwMockTx) DeleteBucket([]byte) error {
	panic("not implemented")
}
func (t *bwMockTx) CreateBucketIfNotExists([]byte) (Bucket, error) {
	panic("not implemented")
}
func (t *bwMockTx) Cursor() Cursor {
	panic("not implemented")
}
func (t *bwMockTx) Commit() error {
	panic("not implemented")
}
func (t *bwMockTx) Rollback() error {
	panic("not implemented")
}

func TestBlockWriter_Write(t *testing.T) {
	tx := &bwMockTx{}
	fn := func(fn func(Transaction) error) error {
		return fn(tx)
	}
	path := BucketPath{[]byte("foo"), []byte("bar"), []byte("baz")}
	bw := &blockWriter{txFn: fn, path: path}

	tx.Reset()
	buf := []byte("hello world")
	n, err := bw.Write(buf)
	assert.NoError(t, err, "write call")
	assert.EqualValues(t, 11, n, "data size")
	assert.EqualValues(t, 1, bw.Blocks(), "number of blocks")
	assert.EqualValues(t, 11, bw.Written(), "last written size")
	assert.Equal(t, []byte{0, 0, 0, 0, 0, 0, 0, 0}, tx.id, "first written block ID")
	assert.Equal(t, "hello world", string(tx.data), "first written value")

	tx.Reset()
	buf = []byte("okay")
	n, err = bw.Write(buf)
	assert.NoError(t, err, "write call")
	assert.EqualValues(t, 4, n, "data size")
	assert.EqualValues(t, 2, bw.Blocks(), "number of blocks")
	assert.EqualValues(t, 15, bw.Written(), "last written size")
	assert.Equal(t, []byte{1, 0, 0, 0, 0, 0, 0, 0}, tx.id, "second written block ID")
	assert.Equal(t, "okay", string(tx.data), "second written value")

	tx.Reset()
	fn = func(fn func(Transaction) error) error {
		return fmt.Errorf("fail for test")
	}
	bw = &blockWriter{txFn: fn, path: path}
	n, err = bw.Write(buf)
	assert.EqualValues(t, 0, n, "written data on error")
	assert.Error(t, err)
}
