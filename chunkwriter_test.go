package boltfs

import (
	"fmt"
	"github.com/stretchr/testify/assert"
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

func TestChunkedWriter_Write_Errors(t *testing.T) {
	wl := newWriteLogger()
	wl.failAfter = 1
	w := NewChunkedWriter(wl, 5)
	n, err := io.WriteString(w, "hello world")
	assert.Error(t, err)
	assert.EqualValues(t, 5, n, "written bytes")
	wl.Reset()
	wl.failAfter = 2
	n, err = io.WriteString(w, "hello world")
	assert.NoError(t, err)
	assert.EqualValues(t, 11, n, "written bytes")

	n, err = io.WriteString(w, "hello")
	assert.Error(t, err)
	assert.EqualValues(t, 0, n, "written bytes")

}

func TestChunkedWriter_Write(t *testing.T) {
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
	for i, val := range expected {
		assert.Equal(t, val, wl.writes[i], "write #%d failed", i+1)
	}
	assert.Len(t, wl.writes, 5, "number of write calls")
	io.WriteString(w, "a")
	assert.Len(t, wl.writes, 6, "number of write calls")

	assert.Equal(t, "eheya", wl.writes[5], "last write")

}

func TestChunkedWriter_Flush(t *testing.T) {
	wl := newWriteLogger()

	w := NewChunkedWriter(wl, 5)

	io.WriteString(w, "okay")
	assert.Len(t, wl.writes, 0, "number of write calls")
	w.Close()
	assert.Len(t, wl.writes, 1, "number of write calls")
	assert.Equal(t, "okay", wl.writes[0], "flushed data")
	w.Close()
	assert.Len(t, wl.writes, 1, "number of write calls")
	assert.Equal(t, "okay", wl.writes[0], "flushed data")
	n, err := io.WriteString(w, "woot")
	assert.Error(t, err)
	assert.EqualValues(t, 0, n, "written bytes when closed")
	assert.Len(t, wl.writes, 1, "number of write calls")
	assert.Equal(t, "okay", wl.writes[0], "flushed data")

}
