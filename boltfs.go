package boltfs

import (
	"encoding/binary"
	"fmt"
	"github.com/boltdb/bolt"
	"gopkg.in/vmihailenco/msgpack.v2"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"time"
)

const blockSize int64 = 32768
const inodeIndexKey = "inode_index"
const versionKey = "boltfs_version"
const fsKey = "fs"
const inodesKey = "inodes"
const Version = 1

//TODO: FileSystem and File change to interfaces

type FileSystem interface {
	http.FileSystem
	Create(string) (io.WriteCloser, error)
}

type boltFs struct {
	db     *bolt.DB
	bucket []byte
}

type fileStat struct {
	Fullname  string
	Dir       bool
	Length    int64
	BlockSize int64
	Inode     []byte
	MTime     time.Time
}

type readableFile struct {
	pos  int64
	tx   *bolt.Tx
	bk   *bolt.Bucket
	stat fileStat
	br   *blockReader
}

func NewFileSystem(db *bolt.DB, bucket []byte) (FileSystem, error) {
	err := db.Update(func(tx *bolt.Tx) error {
		bk, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}
		data := bk.Get([]byte(versionKey))
		if len(data) > 0 {
			v := binary.LittleEndian.Uint64(data)
			if v != Version {
				return fmt.Errorf("existing fs version mismatch %d!=%d", v, Version)
			}
		} else {
			data = make([]byte, 8)
			binary.LittleEndian.PutUint64(data, uint64(Version))
			err = bk.Put([]byte(versionKey), data)
			if err != nil {
				return err
			}
		}

		_, err = bk.CreateBucketIfNotExists([]byte(fsKey))
		if err != nil {
			return err
		}
		_, err = bk.CreateBucketIfNotExists([]byte(inodesKey))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &boltFs{db: db, bucket: bucket}, nil
}
func (fs *boltFs) nextInode() ([]byte, error) {
	val := make([]byte, 8)
	err := fs.db.Update(func(tx *bolt.Tx) error {
		bk := tx.Bucket(fs.bucket)
		b := bk.Get([]byte(inodeIndexKey))
		var index uint64
		if len(b) == 8 {
			copy(val, b)
			index = binary.LittleEndian.Uint64(b)
		} else {
			b = make([]byte, 8)
		}
		index++
		binary.LittleEndian.PutUint64(b, index)
		err := bk.Put([]byte(inodeIndexKey), b)
		if err != nil {
			return err
		}
		_, err = tx.Bucket(fs.bucket).Bucket([]byte(inodesKey)).CreateBucket(val)
		return err
	})
	if err != nil {
		return nil, err
	}
	return val, nil
}
func (fs *boltFs) getDirBucket(tx *bolt.Tx, name string) *bolt.Bucket {
	parts := strings.Split(strings.TrimPrefix(name, "/"), "/")
	bk := tx.Bucket(fs.bucket)
	if bk == nil {
		return nil
	}
	bk = bk.Bucket([]byte(fsKey))
	if bk == nil {
		return nil
	}

	for _, part := range parts {
		if part == "" {
			continue
		}
		bk = bk.Bucket([]byte(part))
		if bk == nil {
			return nil
		}
	}
	return bk
}
func (fs *boltFs) mkDirBucket(tx *bolt.Tx, name string) (*bolt.Bucket, error) {
	parts := strings.Split(strings.TrimPrefix(name, "/"), "/")
	bk, err := tx.CreateBucketIfNotExists(fs.bucket)
	if err != nil {
		return nil, err
	}
	bk, err = bk.CreateBucketIfNotExists([]byte(fsKey))
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(parts); i++ {
		if parts[i] == "" {
			continue
		}
		bk, err = bk.CreateBucketIfNotExists([]byte(parts[i]))
		if err != nil {
			return nil, err
		}
	}
	return bk, nil
}

func (fs *boltFs) Create(name string) (io.WriteCloser, error) {
	dir, file := path.Split(name)
	if file == "" {
		return nil, fmt.Errorf("open %s: is a directory", name)
	}

	inode, err := fs.nextInode()
	if err != nil {
		return nil, err
	}

	bw := &blockWriter{fs: fs, id: inode}
	cw := NewChunkedWriter(bw, int(blockSize))

	return &writableFile{
		inode: inode,
		bw:    bw,
		cw:    cw,
		dir:   dir,
		file:  file,
		fs:    fs,
	}, nil
}

func (fs *boltFs) Open(name string) (http.File, error) {
	dir, file := path.Split(name)
	tx, err := fs.db.Begin(false)
	if err != nil {
		return nil, err
	}
	bk := fs.getDirBucket(tx, dir)
	if bk == nil {
		tx.Commit()
		return nil, fmt.Errorf("file not found")
	}

	var rf readableFile
	data := bk.Get([]byte(file))
	if len(data) == 0 {
		tx.Commit()
		return nil, fmt.Errorf("file not found")
	}
	//nil means bucket, 0-length means not found
	if data == nil {
		rf.bk = bk.Bucket([]byte(file))
		rf.stat.Dir = true
		rf.stat.Fullname = name
	} else {
		err = msgpack.Unmarshal(data, &rf.stat)
		if err != nil {
			tx.Commit()
			return nil, err
		}
		ibk := tx.Bucket(fs.bucket).Bucket([]byte(inodesKey)).Bucket(rf.stat.Inode)
		rf.br = newBlockReader(ibk.Cursor(), rf.stat.BlockSize, rf.stat.Length)
	}

	return &rf, nil
}

func (rf *readableFile) Read(p []byte) (int, error) {
	if rf.br == nil {
		return 0, fmt.Errorf("is a directory")
	}
	return rf.br.Read(p)
}
func (rf *readableFile) Seek(offset int64, whence int) (int64, error) {
	if rf.br == nil {
		return 0, fmt.Errorf("is a directory")
	}
	return rf.br.Seek(offset, whence)
}
func (rf *readableFile) Stat() (os.FileInfo, error) {
	return rf.stat, nil
}
func (rf *readableFile) Readdir(limit int) ([]os.FileInfo, error) {
	if rf.bk == nil {
		return nil, fmt.Errorf("is a file")
	}
	inf := make([]os.FileInfo, 0, rf.bk.Stats().KeyN)
	c := rf.bk.Cursor()
	k, v := c.First()
	i := 1
	var err error
	var stat fileStat
	for k != nil {
		name := string(k)
		if v == nil {
			inf = append(inf, fileStat{Dir: true, Fullname: rf.stat.Fullname + "/" + name})
			continue
		}
		err = msgpack.Unmarshal(v, &stat)
		if err != nil {
			return nil, err
		}
		inf = append(inf, stat)
		if i == limit {
			break
		}
	}
	return inf, nil
}
func (rf *readableFile) Close() error {
	rf.tx.Commit()
	rf.tx = nil
	return nil
}

func (s fileStat) IsDir() bool {
	return s.Dir
}
func (s fileStat) ModTime() time.Time {
	return s.MTime
}
func (s fileStat) Name() string {
	return s.Fullname
}
func (s fileStat) Size() int64 {
	return s.Length
}
func (s fileStat) Sys() interface{} {
	return nil
}
func (s fileStat) Mode() os.FileMode {
	if s.Dir {
		return os.ModeDir | 0777
	} else {
		return 0666
	}
}
