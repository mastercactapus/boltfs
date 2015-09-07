package boltfs

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"gopkg.in/vmihailenco/msgpack.v2"
	"io"
	"testing"
	"time"
)

func hashBP(p BucketPath) string {
	h := sha256.New()
	for _, part := range p {
		h.Write(part)
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

type wfTxMock struct {
	written map[string][]byte
	deleted []string
}
type wfTxMockBucket struct {
	*wfTxMock
	path BucketPath
}

func (tx *wfTxMock) Reset() {
	tx.written = make(map[string][]byte, 100)
	tx.deleted = make([]string, 0, 100)
}
func (tx *wfTxMock) Bucket(key []byte) Bucket {
	return &wfTxMockBucket{tx, NewBucketPath(key)}
}
func (tx *wfTxMock) CreateBucketIfNotExists(key []byte) (Bucket, error) {
	if string(key) == "fail" {
		return nil, fmt.Errorf("forced failure")
	}
	return tx.Bucket(key), nil
}
func (tx *wfTxMock) Commit() error {
	panic("not implemented")
}
func (tx *wfTxMock) Rollback() error {
	panic("not implemented")
}
func (tx *wfTxMock) Cursor() Cursor {
	panic("not implemented")
}
func (tx *wfTxMock) CreateBucket([]byte) (Bucket, error) {
	panic("not implemented")
}
func (tx *wfTxMock) DeleteBucket([]byte) error {
	panic("not implemented")
}

func (tx *wfTxMockBucket) Bucket(key []byte) Bucket {
	tx.path = tx.path.Join(key)
	return tx
}
func (tx *wfTxMockBucket) Get(key []byte) []byte {
	return tx.written[hashBP(tx.path.Join(key))]
}
func (tx *wfTxMockBucket) Put(key, val []byte) error {
	tx.written[hashBP(tx.path.Join(key))] = val
	return nil
}
func (tx *wfTxMockBucket) DeleteBucket(key []byte) error {
	tx.deleted = append(tx.deleted, hashBP(tx.path.Join(key)))
	return nil
}

func (tx *wfTxMockBucket) CreateBucket(key []byte) (Bucket, error) {
	panic("not implemented")
}

func (tx *wfTxMockBucket) Cursor() Cursor {
	panic("not implemented")
}

func TestWriteableFile(t *testing.T) {
	sPath := NewBucketPath([]byte("fs"), []byte("foo"))
	iPath := NewBucketPath([]byte("inodes"), []byte("inode"))

	tx := &wfTxMock{}
	txFn := func(fn func(tx Transaction) error) error {
		return fn(tx)
	}

	Convey("When writing to a new file", t, func() {
		tx.Reset()
		wf := newWritableFile(txFn, 5, iPath, sPath)
		n, err := io.WriteString(wf, "hello world!")
		So(n, ShouldEqual, 12)
		So(err, ShouldBeNil)
		err = wf.Close()
		So(err, ShouldBeNil)

		Convey("Should write blocks to inode section", func() {
			b1 := tx.written[hashBP(iPath.Join([]byte{0, 0, 0, 0, 0, 0, 0, 0}))]
			b2 := tx.written[hashBP(iPath.Join([]byte{1, 0, 0, 0, 0, 0, 0, 0}))]
			b3 := tx.written[hashBP(iPath.Join([]byte{2, 0, 0, 0, 0, 0, 0, 0}))]
			So(string(b1), ShouldEqual, "hello")
			So(string(b2), ShouldEqual, " worl")
			So(string(b3), ShouldEqual, "d!")
		})

		Convey("Should write a correct file stat section", func() {
			data := tx.written[hashBP(sPath)]
			So(data, ShouldNotBeNil)
			var stat fileStat
			err := msgpack.Unmarshal(data, &stat)
			So(err, ShouldBeNil)
			So(stat.Dir, ShouldBeFalse)
			So(stat.Length, ShouldEqual, 12)
			So(stat.BlockSize, ShouldEqual, 5)
			So(stat.Inode, ShouldResemble, iPath)
			So(stat.Filename, ShouldEqual, "foo")
			So(stat.MTime, ShouldHappenBetween, time.Now().Add(-time.Minute), time.Now())
		})
	})

	Convey("When replacing an existing file", t, func() {
		tx.Reset()
		var stat fileStat
		iPathDel := iPath.Join([]byte("deleteme"))
		stat.Inode = iPathDel
		data, err := msgpack.Marshal(&stat)
		if err != nil {
			panic(err)
		}
		tx.written[hashBP(sPath)] = data
		wf := newWritableFile(txFn, 5, iPath, sPath)
		n, err := io.WriteString(wf, "hello world!")
		So(n, ShouldEqual, 12)
		So(err, ShouldBeNil)
		err = wf.Close()
		So(err, ShouldBeNil)

		Convey("Should write blocks to inode section", func() {
			b1 := tx.written[hashBP(iPath.Join([]byte{0, 0, 0, 0, 0, 0, 0, 0}))]
			b2 := tx.written[hashBP(iPath.Join([]byte{1, 0, 0, 0, 0, 0, 0, 0}))]
			b3 := tx.written[hashBP(iPath.Join([]byte{2, 0, 0, 0, 0, 0, 0, 0}))]
			So(string(b1), ShouldEqual, "hello")
			So(string(b2), ShouldEqual, " worl")
			So(string(b3), ShouldEqual, "d!")
		})

		Convey("Should write a correct file stat section", func() {
			data := tx.written[hashBP(sPath)]
			So(data, ShouldNotBeNil)
			var stat fileStat
			err := msgpack.Unmarshal(data, &stat)
			So(err, ShouldBeNil)
			So(stat.Dir, ShouldBeFalse)
			So(stat.Length, ShouldEqual, 12)
			So(stat.BlockSize, ShouldEqual, 5)
			So(stat.Inode, ShouldResemble, iPath)
			So(stat.Filename, ShouldEqual, "foo")
			So(stat.MTime, ShouldHappenBetween, time.Now().Add(-time.Minute), time.Now())
		})

		Convey("Should delete old inode section", func() {
			So(tx.deleted, ShouldContain, hashBP(iPathDel))
		})
	})

	Convey("When handling failures", t, func() {
		tx.Reset()
		wf := newWritableFile(txFn, 5, iPath, sPath)
		err := wf.Close()
		So(err, ShouldBeNil)
		Convey("Should return error when writing to a closed file", func() {
			n, err := io.WriteString(wf, "hello")
			So(n, ShouldEqual, 0)
			So(err, ShouldNotBeNil)

			err = wf.Close()
			So(err, ShouldNotBeNil)
		})
		Convey("Should wipe current inode on failure", func() {
			tx.Reset()
			wf := newWritableFile(txFn, 5, iPath, sPath)
			wf.wc.Close()
			err := wf.Close()
			So(err, ShouldNotBeNil)
			So(tx.deleted, ShouldContain, hashBP(iPath))

			tx.Reset()
			wf = newWritableFile(txFn, 5, iPath, sPath.Join([]byte("fail"), []byte("foo")))
			err = wf.Close()
			So(err, ShouldNotBeNil)
			So(tx.deleted, ShouldContain, hashBP(iPath))
		})

	})
}
