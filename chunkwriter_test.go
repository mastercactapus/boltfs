package boltfs

import (
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"io"
	"testing"
)

type writeLogger struct {
	writes    []string
	failAfter int
}

func newWriteLogger() *writeLogger {
	return &writeLogger{writes: make([]string, 0, 100)}
}

func (w *writeLogger) Reset() {
	w.writes = make([]string, 0, 100)
}
func (w *writeLogger) Write(p []byte) (int, error) {
	if w.failAfter > 0 && len(w.writes) >= w.failAfter {
		return 0, fmt.Errorf("fail for test")
	}
	w.writes = append(w.writes, string(p))
	return len(p), nil
}

func TestChunkedWriter_Write(t *testing.T) {
	Convey("Should write to underlying stream in chunks", t, func() {
		wl := newWriteLogger()

		w := NewChunkedWriter(wl, 5)

		io.WriteString(w, "hey")
		io.WriteString(w, "there")
		io.WriteString(w, "foo")
		io.WriteString(w, "bar")
		io.WriteString(w, "baz")
		io.WriteString(w, "woahthere")
		io.WriteString(w, "hey")

		expected := []string{"heyth", "erefo", "obarb", "azwoa", "hther"}
		So(wl.writes, ShouldResemble, expected)
		expected = append(expected, "eheya")
		io.WriteString(w, "a")
		So(wl.writes, ShouldResemble, expected)
	})

	Convey("Should only consume blockSize chunks in error event", t, func() {
		wl := newWriteLogger()
		wl.failAfter = 1
		w := NewChunkedWriter(wl, 5)
		n, err := io.WriteString(w, "hello world")
		So(err, ShouldNotBeNil)
		So(n, ShouldEqual, 5)

		wl.Reset()
		wl.failAfter = 2
		n, err = io.WriteString(w, "hello world")
		So(err, ShouldBeNil)
		So(n, ShouldEqual, 11)

		n, err = io.WriteString(w, "hello")
		So(err, ShouldNotBeNil)
		So(n, ShouldEqual, 0)
	})
}

func TestChunkedWriter_Close(t *testing.T) {
	wl := newWriteLogger()

	w := NewChunkedWriter(wl, 5)

	Convey("Should flush pending bytes", t, func() {
		io.WriteString(w, "okay")
		So(wl.writes, ShouldResemble, []string{})
		err := w.Close()
		So(err, ShouldBeNil)
		So(wl.writes, ShouldResemble, []string{"okay"})
	})
	Convey("Should not accept new data after", t, func() {
		err := w.Close()
		So(err, ShouldNotBeNil)
		So(wl.writes, ShouldResemble, []string{"okay"})
		n, err := io.WriteString(w, "woot")
		So(n, ShouldEqual, 0)
		So(err, ShouldNotBeNil)
		So(wl.writes, ShouldResemble, []string{"okay"})
	})
}
