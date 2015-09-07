package boltfs

import (
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"io"
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
	Convey("Should write data to the correct block", t, func() {
		tx.Reset()
		n, err := io.WriteString(bw, "hello world")
		So(err, ShouldBeNil)
		So(n, ShouldEqual, 11)
		So(bw.Blocks(), ShouldEqual, 1)
		So(bw.Written(), ShouldEqual, 11)
		So(tx.id, ShouldResemble, []byte{0, 0, 0, 0, 0, 0, 0, 0})
		So(string(tx.data), ShouldEqual, "hello world")
		So(tx.path, ShouldResemble, path)
	})
	Convey("Should increment block and written count", t, func() {
		tx.Reset()
		n, err := io.WriteString(bw, "okay")
		So(err, ShouldBeNil)
		So(n, ShouldEqual, 4)
		So(bw.Blocks(), ShouldEqual, 2)
		So(bw.Written(), ShouldEqual, 15)
		So(tx.id, ShouldResemble, []byte{1, 0, 0, 0, 0, 0, 0, 0})
		So(string(tx.data), ShouldEqual, "okay")
	})

	tx.Reset()
	fn = func(fn func(Transaction) error) error {
		return fmt.Errorf("fail for test")
	}
	bw = &blockWriter{txFn: fn, path: path}
	Convey("Should return no data written on error", t, func() {
		n, err := bw.Write([]byte("foo"))
		So(err, ShouldNotBeNil)
		So(n, ShouldEqual, 0)
	})
}
