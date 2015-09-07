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
	Convey("Should store and retrieve a single file", t, func() {
		SetDefaultFailureMode(FailureHalts)
		os.Remove("test.db")
		defer os.Remove("test.db")
		db, err := bolt.Open("test.db", 0644, nil)
		So(err, ShouldBeNil)
		defer db.Close()

		fs, err := NewFileSystem(NewBoltDB(db), NewBucketPath([]byte("test")))
		So(err, ShouldBeNil)

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
}
