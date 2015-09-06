package boltfs

import (
	"encoding/binary"
	"fmt"
	"github.com/boltdb/bolt"
	"io"
)

type blockReader struct {
	c         *bolt.Cursor
	blockSize int64
	length    int64
	pos       int64
	cblock    []byte
}

func newBlockReader(bk *bolt.Bucket, blockSize, length int64) *blockReader {
	c := bk.Cursor()
	_, fblock := c.First()
	return &blockReader{c: c, blockSize: blockSize, length: length, cblock: fblock}
}

func (br *blockReader) Seek(offset int64, whence int) (int64, error) {
	var newPos int64
	switch whence {
	case 0:
		newPos = offset
	case 1:
		newPos = br.pos + offset
	case 2:
		newPos = br.length + offset
	default:
		return br.pos, fmt.Errorf("expected whence to be 0,1, or 2")
	}
	if newPos < 0 || newPos > br.length {
		return br.pos, fmt.Errorf("new position is beyond contents of file")
	}
	br.pos = newPos
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(newPos/br.blockSize))
	_, br.cblock = br.c.Seek(b)
	rem := newPos % br.blockSize
	br.cblock = br.cblock[rem:]
	return newPos, nil
}

// TODO: read multiple blocks if p is big enough
func (br *blockReader) Read(p []byte) (int, error) {
	if br.pos == br.length {
		return 0, io.EOF
	}
	plen := len(p)
	clen := len(br.cblock)
	if plen < clen {
		copy(p, br.cblock)
		br.cblock = br.cblock[plen:]
		br.pos += int64(plen)
		return plen, nil
	}
	copy(p, br.cblock)
	br.pos += int64(clen)
	p = p[clen:]
	_, br.cblock = br.c.Next()
	return clen, nil
}
