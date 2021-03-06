package boltfs

import (
	"fmt"
	"io"
)

type ChunkedWriter struct {
	w    io.Writer
	size int
	buf  []byte
	pos  int
}

func NewChunkedWriter(w io.Writer, chunkSize int) *ChunkedWriter {
	return &ChunkedWriter{w: w, size: chunkSize, buf: make([]byte, chunkSize)}
}

func (cw *ChunkedWriter) Close() error {
	if cw.w == nil {
		return fmt.Errorf("closed")
	}
	var err error
	if cw.pos > 0 {
		_, err = cw.w.Write(cw.buf[:cw.pos])
	}
	cw.w = nil
	return err
}

func (cw *ChunkedWriter) Write(p []byte) (int, error) {
	if cw.w == nil {
		return 0, fmt.Errorf("closed")
	}
	plen := len(p)
	total := plen + cw.pos
	// if smaller, copy and return
	if total < cw.size {
		copy(cw.buf[cw.pos:], p)
		cw.pos = total
		return plen, nil
	}
	var read int
	var err error
	// at this point we know that combined they are larger
	// if there is something in the buffer, then we must finish
	// that block first, before looping through p
	if cw.pos != 0 {
		copy(cw.buf[cw.pos:], p)
		_, err = cw.w.Write(cw.buf)
		if err != nil {
			return 0, err
		}
		p = p[cw.size-cw.pos:]
		read = cw.size - cw.pos
	}

	// while the p slice is bigger than chunk size
	// write chunks directly out
	var l int
	for l = len(p); l > cw.size; l = len(p) {
		_, err = cw.w.Write(p[:cw.size])
		if err != nil {
			return read, err
		}
		read += cw.size
		p = p[cw.size:]
	}
	copy(cw.buf, p)
	cw.pos = l
	read += l

	return read, nil
}
