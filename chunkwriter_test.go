package boltfs

import (
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
)

type writeLogger struct {
	writes []string
}

func newWriteLogger() *writeLogger {
	return &writeLogger{writes: make([]string, 0, 100)}
}

func (w *writeLogger) Write(p []byte) (int, error) {
	w.writes = append(w.writes, string(p))
	return len(p), nil
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
	w.Flush()
	assert.Len(t, wl.writes, 1, "number of write calls")
	assert.Equal(t, "okay", wl.writes[0], "flushed data")

}
