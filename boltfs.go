package boltfs

import (
	"encoding/binary"
	"fmt"
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
	db   DB
	path BucketPath
}

type fileStat struct {
	Fullname  string
	Dir       bool
	Length    int64
	BlockSize int64
	Inode     BucketPath
	MTime     time.Time
}

type readableFile struct {
	pos  int64
	tx   Transaction
	bk   Bucket
	stat fileStat
	br   *blockReader
}

func NewFileSystem(db DB, path BucketPath) (FileSystem, error) {
	err := db.Update(func(tx Transaction) error {
		bk, err := path.MkFrom(tx)
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

	return &boltFs{db: db, path: path}, nil
}
func (fs *boltFs) nextInode() (BucketPath, error) {
	var bpath BucketPath
	val := make([]byte, 8)
	err := fs.db.Update(func(tx Transaction) error {
		bk := fs.path.BucketFrom(tx)
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
		bpath = fs.path.Join([]byte(inodesKey), val)
		_, err = bpath.CreateFrom(tx)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return bpath, nil
}

func (fs *boltFs) fsPath(name string) BucketPath {
	parts := strings.Split(strings.TrimPrefix(name, "/"), "/")
	p := make(BucketPath, len(fs.path), len(fs.path)+len(parts))
	copy(p, fs.path)
	for _, part := range parts {
		if part == "" {
			continue
		}
		p = append(p, []byte(part))
	}
	return p
}

func (fs *boltFs) Create(name string) (io.WriteCloser, error) {
	_, file := path.Split(name)
	if file == "" {
		return nil, fmt.Errorf("open %s: is a directory", name)
	}
	statPath := fs.fsPath(name)
	inodePath, err := fs.nextInode()
	if err != nil {
		return nil, err
	}

	return newWritableFile(fs.db.Batch, blockSize, inodePath, statPath), nil
}

func (fs *boltFs) Open(name string) (http.File, error) {
	dir, file := path.Split(name)
	tx, err := fs.db.Begin(false)
	if err != nil {
		return nil, err
	}
	dirPath := fs.fsPath(dir)
	bk := dirPath.BucketFrom(tx)
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
		ibk := rf.stat.Inode.BucketFrom(tx)
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
	inf := make([]os.FileInfo, 0, 100)
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
