package boltfs

import (
	"github.com/boltdb/bolt"
	. "github.com/smartystreets/goconvey/convey"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestBoltFS(t *testing.T) {
	SetDefaultFailureMode(FailureHalts)
	os.Remove("test.db")
	defer os.Remove("test.db")
	db, err := bolt.Open("test.db", 0644, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	fs, err := NewFileSystem(NewBoltDB(db), NewBucketPath([]byte("test")))
	if err != nil {
		t.Fatal(err)
	}

	Convey("Should store and retrieve a single file", t, func() {
		wc, err := fs.Create("foo")
		So(err, ShouldBeNil)

		_, err = io.WriteString(wc, "hello world!")
		So(err, ShouldBeNil)

		err = wc.Close()
		So(err, ShouldBeNil)

		rc, err := fs.Open("foo")
		So(err, ShouldBeNil)

		data, err := ioutil.ReadAll(rc)
		So(err, ShouldBeNil)

		So(string(data), ShouldEqual, "hello world!")
	})

	err = db.Update(func(tx *bolt.Tx) error {
		bk, err := tx.CreateBucket([]byte("badVersion"))
		if err != nil {
			return err
		}
		bkb, err := tx.CreateBucket([]byte("corruptVersion"))
		if err != nil {
			return err
		}
		err = bkb.Put([]byte(versionKey), []byte{0, 0})
		if err != nil {
			return err
		}

		return bk.Put([]byte(versionKey), []byte{0, 0, 0, 0, 0, 0, 0, 0})
	})
	if err != nil {
		t.Fatal(err)
	}

	Convey("Should refuse to open on version mismatch", t, func() {
		fs, err := NewFileSystem(NewBoltDB(db), NewBucketPath([]byte("badVersion")))
		So(fs, ShouldBeNil)
		So(err, ShouldNotBeNil)
	})
	Convey("Should not panic on corrupt version", t, func() {
		fs, err := NewFileSystem(NewBoltDB(db), NewBucketPath([]byte("corruptVersion")))
		So(fs, ShouldBeNil)
		So(err, ShouldNotBeNil)
	})

}
