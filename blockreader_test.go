package boltfs

import (
	"encoding/binary"
	. "github.com/smartystreets/goconvey/convey"
	"io"
	"strings"
	"testing"
)

type mockCursor struct {
	data  []string
	index int
}

func (c *mockCursor) current() ([]byte, []byte) {
	if c.index >= len(c.data) {
		return nil, nil
	}
	k := make([]byte, 8)
	binary.LittleEndian.PutUint64(k, uint64(c.index))
	return k, []byte(c.data[c.index])
}

func (c *mockCursor) First() ([]byte, []byte) {
	c.index = 0
	return c.current()
}
func (c *mockCursor) Next() ([]byte, []byte) {
	c.index++
	return c.current()
}
func (c *mockCursor) Seek(k []byte) ([]byte, []byte) {
	newIndex := int(binary.LittleEndian.Uint64(k))
	if newIndex < 0 || newIndex >= len(c.data) {
		return nil, nil
	}
	c.index = newIndex
	return c.current()
}

func TestBlockReader_Seek(t *testing.T) {
	c := &mockCursor{data: []string{"apple", "orang", "foobr", "okay"}}
	br := newBlockReader(c, 5, 19)
	buf := make([]byte, 3)
	Convey("Should seek from start when whence is '0'", t, func() {
		br.Seek(2, 0)
		br.Read(buf)
		So(string(buf), ShouldEqual, "ple")
	})
	Convey("Should seek from current pos when whence is '1'", t, func() {
		br.Seek(-5, 1)
		br.Read(buf)
		So(string(buf), ShouldEqual, "app")
	})
	Convey("Should seek from end when whence is '2'", t, func() {
		br.Seek(-3, 2)
		br.Read(buf)
		So(string(buf), ShouldEqual, "kay")
	})
	br.Seek(0, 0)
	Convey("Should refuse to seek with invalid whence", t, func() {
		n, err := br.Seek(0, 3)
		So(n, ShouldEqual, 0)
		So(err, ShouldNotBeNil)
	})
	Convey("Should refuse to seek past end of file", t, func() {
		n, err := br.Seek(1, 2)
		So(n, ShouldEqual, 0)
		So(err, ShouldNotBeNil)
	})
	Convey("Should refuse to seek past beginning file", t, func() {
		n, err := br.Seek(0, -2)
		So(n, ShouldEqual, 0)
		So(err, ShouldNotBeNil)
	})
}

func TestBlockReader_Read(t *testing.T) {
	data := []string{"apple", "orang", "foobr", "okay"}
	dataStr := strings.Join(data, "")
	c := &mockCursor{data: data}
	br := newBlockReader(c, 5, 19)

	buf := make([]byte, 19)
	Convey("Should always fill the buffer if possible", t, func() {
		n, _ := br.Read(buf)
		So(n, ShouldEqual, len(dataStr))
		So(string(buf), ShouldEqual, dataStr)
	})
	Convey("Should return EOF when empty", t, func() {
		n, err := br.Read(buf)
		So(n, ShouldEqual, 0)
		So(err, ShouldEqual, io.EOF)
	})

	br.Seek(0, 0)
	buf = make([]byte, 15)

	Convey("Should read partial data", t, func() {
		n, err := br.Read(buf)
		So(n, ShouldEqual, 15)
		So(string(buf), ShouldEqual, dataStr[:15])
		So(err, ShouldBeNil)
	})
	Convey("Should read into a bigger buffer", t, func() {
		n, err := br.Read(buf)
		So(n, ShouldEqual, 4)
		So(err, ShouldEqual, io.EOF)
		So(string(buf[:4]), ShouldEqual, dataStr[len(dataStr)-4:])
	})
}
