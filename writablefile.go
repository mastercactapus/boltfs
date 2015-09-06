package boltfs

import (
	"fmt"
	"github.com/boltdb/bolt"
	"gopkg.in/vmihailenco/msgpack.v2"
	"time"
)

type writableFile struct {
	file, dir string
	fs        *boltFs
	inode     []byte
	bw        *blockWriter
	cw        *ChunkedWriter
	blockSize int64
}

func (f *writableFile) Write(p []byte) (int, error) {
	if f.cw == nil {
		return 0, fmt.Errorf("file is closed")
	}
	return f.cw.Write(p)
}

func (f *writableFile) wipeInode() error {
	// if failed, delete data
	return f.fs.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(f.fs.bucket).Bucket([]byte("inodes")).DeleteBucket(f.inode)
	})
}

func (f *writableFile) Close() error {
	err := f.cw.Flush()
	if err != nil {
		f.wipeInode()
		return err
	}
	f.cw = nil
	size := f.blockSize*int64(f.bw.Blocks()) + int64(f.bw.LastSize())

	stat := fileStat{Dir: false, Length: size, BlockSize: f.blockSize, Inode: f.inode, MTime: time.Now()}
	data, err := msgpack.Marshal(&stat)
	if err != nil {
		f.wipeInode()
		return err
	}

	err = f.fs.db.Update(func(tx *bolt.Tx) error {
		bk, err := f.fs.mkDirBucket(tx, f.dir)
		if err != nil {
			return err
		}
		oldData := bk.Get([]byte(f.file))
		var oldStat fileStat
		err = msgpack.Unmarshal(oldData, &oldStat)
		if err == nil {
			// attempt to delete old inode stuff
			tx.Bucket(f.fs.bucket).Bucket([]byte("inodes")).DeleteBucket(oldStat.Inode)
		}
		return bk.Put([]byte(f.file), data)
	})

	if err != nil {
		f.wipeInode()
		return err
	}
	return nil
}
