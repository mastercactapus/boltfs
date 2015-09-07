package boltfs

import (
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"io"
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
	br.Seek(2, 0)
	buf := make([]byte, 3)
	br.Read(buf)
	assert.Equal(t, "ple", string(buf))

	br.Seek(-5, 1)
	br.Read(buf)
	assert.Equal(t, "app", string(buf))

	br.Seek(-3, 2)
	br.Read(buf)
	assert.Equal(t, "kay", string(buf))

	br.Seek(0, 0)
	n, err := br.Seek(2, 3)
	assert.EqualValues(t, 0, n, "position after invalid seek")
	assert.Error(t, err)

	n, err = br.Seek(-1, 0) //can't seek before 0
	assert.EqualValues(t, 0, n, "position after invalid seek")
	assert.Error(t, err)

}

func TestBlockReader_Read(t *testing.T) {
	c := &mockCursor{data: []string{"apple", "orang", "foobr", "okay"}}
	br := newBlockReader(c, 5, 19)

	buf := make([]byte, 19)
	n, err := br.Read(buf)
	if n != 19 {
		t.Errorf("did not read all bytes (got %d of %d)", n, 19)
	}
	if err != io.EOF {
		t.Error("should have returned EOF on EOF")
	}

	n, err = br.Read(buf)
	if n != 0 {
		t.Error("should have stayed at EOF")
	}
	if err != io.EOF {
		t.Error("shoudl have continued to return EOF")
	}

	br.Seek(0, 0)

	buf = make([]byte, 15)
	n, err = br.Read(buf)
	if string(buf) != "appleorangfoobr" || n != 15 {
		t.Error("did not read expected data")
	}
	if err != nil {
		t.Error("returned error, when there shouldn't be")
	}
	n, err = br.Read(buf)
	if n != 4 || string(buf[:4]) != "okay" {
		t.Error("did not finish properly")
	}
	if err != io.EOF {
		t.Error("did not return EOF")
	}
}
