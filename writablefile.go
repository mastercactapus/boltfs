package boltfs

import (
	"fmt"
	"gopkg.in/vmihailenco/msgpack.v2"
	"io"
	"time"
)

type WriteFlusher interface {
	io.Writer
	Flush() error
}

type writableFile struct {
	txFn         func(func(tx Transaction) error) error
	iPath, sPath BucketPath
	length       int64
	blockSize    int64
	wf           WriteFlusher
}

func newWritableFile(txFn func(func(tx Transaction) error) error, blockSize int64, inodePath, statPath BucketPath) *writableFile {
	return &writableFile{
		txFn:      txFn,
		sPath:     statPath,
		iPath:     inodePath,
		blockSize: blockSize,
		wf:        NewChunkedWriter(&blockWriter{txFn: txFn, path: inodePath}, int(blockSize)),
	}
}

func (f *writableFile) Write(p []byte) (int, error) {
	if f.wf == nil {
		return 0, fmt.Errorf("file is closed")
	}
	n, err := f.wf.Write(p)
	f.length += int64(n)
	return n, err
}

func (f *writableFile) wipeInode() error {
	return f.txFn(func(tx Transaction) error {
		return f.iPath.DeleteFrom(tx)
	})
}

func (f *writableFile) Close() error {
	if f.wf == nil {
		return fmt.Errorf("file is closed")
	}
	err := f.wf.Flush()
	if err != nil {
		f.wipeInode()
		return err
	}
	f.wf = nil

	stat := fileStat{Dir: false, Length: f.length, BlockSize: f.blockSize, Inode: f.iPath, MTime: time.Now()}
	data, err := msgpack.Marshal(&stat)
	if err != nil {
		f.wipeInode()
		return err
	}

	err = f.txFn(func(tx Transaction) error {
		bkName := f.sPath[:len(f.sPath)-1]
		statKey := f.sPath[len(f.sPath)-1]
		bk, err := BucketPath(bkName).MkFrom(tx)
		if err != nil {
			return err
		}
		oldData := bk.Get(statKey)
		var oldStat fileStat
		err = msgpack.Unmarshal(oldData, &oldStat)
		if err == nil {
			// attempt to delete old inode stuff
			oldStat.Inode.DeleteFrom(tx)
		}
		return bk.Put(statKey, data)
	})

	if err != nil {
		f.wipeInode()
		return err
	}
	return nil
}
