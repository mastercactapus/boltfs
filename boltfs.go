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
	Filename  string
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
	c    Cursor
	br   *blockReader
}

func NewFileSystem(db DB, path BucketPath) (FileSystem, error) {
	err := db.Update(func(tx Transaction) error {
		bk, err := path.MkFrom(tx)
		if err != nil {
			return err
		}
		data := bk.Get([]byte(versionKey))
		if len(data) == 8 {
			v := binary.LittleEndian.Uint64(data)
			if v != Version {
				return fmt.Errorf("existing fs version mismatch %d!=%d", v, Version)
			}
		} else if len(data) > 0 {
			return fmt.Errorf("could not read existing fs version")
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
		}
		index++
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, index)
		err := bk.Put([]byte(inodeIndexKey), buf)
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
	p := make(BucketPath, len(fs.path), len(fs.path)+len(parts)+1)
	copy(p, fs.path)
	p = append(p, []byte(fsKey))
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
	fmt.Println("Open", name)
	var dir, file string
	if name == "/" {
		dir = ""
		file = ""
	} else if strings.HasSuffix(name, "/") {
		dir = strings.TrimSuffix(name, "/")
		file = ""
	} else {
		dir, file = path.Split(name)
	}

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
	rf.tx = tx
	if file == "" {
		rf.bk = bk
		rf.stat.Dir = true
		rf.stat.Filename = path.Base(dir)
		return &rf, nil
	}

	dbk := bk.Bucket([]byte(file))
	if dbk != nil {
		rf.bk = dbk
		rf.stat.Dir = true
		rf.stat.Filename = file
		return &rf, nil
	}

	data := bk.Get([]byte(file))
	if len(data) == 0 || data == nil {
		tx.Commit()
		return nil, fmt.Errorf("file not found")
	}

	err = msgpack.Unmarshal(data, &rf.stat)
	if err != nil {
		tx.Commit()
		return nil, err
	}
	ibk := rf.stat.Inode.BucketFrom(tx)
	rf.br = newBlockReader(ibk.Cursor(), rf.stat.BlockSize, rf.stat.Length)

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
	if limit == 0 {
		limit = 1
	}
	initialSize := limit
	if initialSize < 1 {
		initialSize = 250
	}
	inf := make([]os.FileInfo, 0, initialSize)
	var k, v []byte
	if rf.c == nil {
		rf.c = rf.bk.Cursor()
		k, v = rf.c.First()
	} else {
		k, v = rf.c.Next()
	}

	var err error
	var stat fileStat
	for ; k != nil; k, v = rf.c.Next() {
		name := string(k)
		if v == nil {
			inf = append(inf, fileStat{Dir: true, Filename: name})
			continue
		}
		err = msgpack.Unmarshal(v, &stat)
		if err != nil {
			return nil, err
		}
		inf = append(inf, stat)
		if len(inf) == limit {
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
	return s.Filename
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
