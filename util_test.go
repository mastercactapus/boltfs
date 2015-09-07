package boltfs

import (
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

type bWatcher struct {
	path       BucketPath
	created    BucketPath
	createdINE BucketPath
	deleted    BucketPath
}

func (b *bWatcher) Reset() {
	b.path = NewBucketPath()
}
func (b *bWatcher) Bucket(key []byte) Bucket {
	if string(key) == "missing" {
		return nil
	}
	b.path = b.path.Join(key)
	return b
}
func (b *bWatcher) DeleteBucket(key []byte) error {
	b.deleted = b.path.Join(key)
	return nil
}
func (b *bWatcher) CreateBucket(key []byte) (Bucket, error) {
	b.path = b.path.Join(key)
	b.created = b.path
	return b, nil
}
func (b *bWatcher) CreateBucketIfNotExists(key []byte) (Bucket, error) {
	if string(key) == "fail" {
		return nil, fmt.Errorf("forced failure")
	}
	b.path = b.path.Join(key)
	b.createdINE = b.path.Join(key)
	return b, nil
}
func (b *bWatcher) Cursor() Cursor {
	panic("not implemented")
}
func (b *bWatcher) Get([]byte) []byte {
	panic("not implemented")
}
func (b *bWatcher) Put([]byte, []byte) error {
	panic("not implemented")
}

func TestBucketPath_Join(t *testing.T) {
	Convey("The result should be the same as a new one", t, func() {
		p := NewBucketPath([]byte("foo"), []byte("bar"))
		joined := p.Join([]byte("baz"), []byte("bin"))
		expected := NewBucketPath([]byte("foo"), []byte("bar"), []byte("baz"), []byte("bin"))
		So(joined, ShouldResemble, expected)
	})
	Convey("The result should not reference the old memory", t, func() {
		p := NewBucketPath([]byte("foo"), []byte("bar"))
		joined := p.Join([]byte("baz"), []byte("bin"))
		expected := NewBucketPath([]byte("foo"), []byte("bar"), []byte("baz"), []byte("bin"))
		So(joined, ShouldResemble, expected)
		p[0] = []byte("brok")
		So(joined, ShouldResemble, expected)
	})
}

func TestBucketPath_DeleteFrom(t *testing.T) {
	bw := &bWatcher{}
	bw.Reset()

	path := NewBucketPath([]byte("foo"), []byte("bar"))
	Convey("Should delete a bucket", t, func() {
		bw.Reset()
		err := path.DeleteFrom(bw)
		So(err, ShouldBeNil)
		So(bw.deleted, ShouldResemble, path)
	})
	Convey("Should ignore if the bucket is already missing", t, func() {
		path := NewBucketPath([]byte("foo"), []byte("missing"), []byte("bar"))
		bw.Reset()
		err := path.DeleteFrom(bw)
		So(err, ShouldBeNil)
	})
}
func TestBucketPath_CreateFrom(t *testing.T) {
	bw := &bWatcher{}
	bw.Reset()

	path := NewBucketPath([]byte("foo"), []byte("bar"))
	Convey("Should create a bucket", t, func() {
		bw.Reset()
		bk, err := path.CreateFrom(bw)
		So(err, ShouldBeNil)
		So(bw.created, ShouldResemble, path)
		So(bk, ShouldNotBeNil)
	})
	Convey("Should not error if bucket parent is missing", t, func() {
		path := NewBucketPath([]byte("foo"), []byte("missing"), []byte("bar"))
		bw.Reset()
		bk, err := path.CreateFrom(bw)
		So(err, ShouldBeNil)
		So(bk, ShouldNotBeNil)
		So(bw.created, ShouldResemble, path)
	})
	Convey("Should bubble up an error if occurred", t, func() {
		path := NewBucketPath([]byte("foo"), []byte("missing"), []byte("fail"), []byte("bar"))
		bw.Reset()
		bk, err := path.CreateFrom(bw)
		So(err, ShouldNotBeNil)
		So(bk, ShouldBeNil)
	})

}
