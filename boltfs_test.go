package boltfs

import (
	"github.com/boltdb/bolt"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestBoltFS(t *testing.T) {
	os.Remove("test.db")
	defer os.Remove("test.db")
	db, err := bolt.Open("test.db", 0644, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	fs, err := NewFileSystem(db, []byte("test"))
	if err != nil {
		t.Fatal(err)
	}
	wc, err := fs.Create("foo")
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.WriteString(wc, "hello world!")
	if err != nil {
		t.Error(err)
	}
	err = wc.Close()
	if err != nil {
		t.Error(err)
	}

	rc, err := fs.Open("foo")
	if err != nil {
		t.Fatal(err)
	}
	data, err := ioutil.ReadAll(rc)
	if err != nil {
		t.Error(err)
	}
	if string(data) != "hello world!" {
		t.Error("expected 'hello world!' but got:", string(data))
	}

}
