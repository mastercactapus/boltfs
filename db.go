package boltfs

import (
	"github.com/boltdb/bolt"
)

type DB interface {
	Begin(writable bool) (Transaction, error)
	Update(func(Transaction) error) error
	View(func(Transaction) error) error
	Batch(func(Transaction) error) error
}

type Transaction interface {
	Bucketer
	Commit() error
	Rollback() error
}

type Bucketer interface {
	CreateBucketIfNotExists(key []byte) (Bucket, error)
	CreateBucket(key []byte) (Bucket, error)
	DeleteBucket(key []byte) error
	Bucket(key []byte) Bucket
	Cursor() Cursor
}

type Bucket interface {
	Bucketer
	Put(key, val []byte) error
	Get(key []byte) []byte
}

type Cursor interface {
	First() ([]byte, []byte)
	Next() ([]byte, []byte)
	Seek([]byte) ([]byte, []byte)
}

type boltDB struct {
	*bolt.DB
}
type boltTx struct {
	*bolt.Tx
}
type boltBk struct {
	bk *bolt.Bucket
}

func NewBoltDB(db *bolt.DB) DB {
	return &boltDB{DB: db}
}

func (b *boltDB) Begin(writable bool) (Transaction, error) {
	tx, err := b.DB.Begin(writable)
	return &boltTx{tx}, err
}
func (b *boltDB) Update(fn func(tx Transaction) error) error {
	return b.DB.Update(func(tx *bolt.Tx) error {
		return fn(&boltTx{tx})
	})
}
func (b *boltDB) View(fn func(tx Transaction) error) error {
	return b.DB.View(func(tx *bolt.Tx) error {
		return fn(&boltTx{tx})
	})
}
func (b *boltDB) Batch(fn func(tx Transaction) error) error {
	return b.DB.Batch(func(tx *bolt.Tx) error {
		return fn(&boltTx{tx})
	})
}

func (tx *boltTx) CreateBucketIfNotExists(key []byte) (Bucket, error) {
	bk, err := tx.Tx.CreateBucketIfNotExists(key)
	if bk == nil {
		return nil, err
	}
	return &boltBk{bk}, err
}
func (tx *boltTx) CreateBucket(key []byte) (Bucket, error) {
	bk, err := tx.Tx.CreateBucket(key)
	if bk == nil {
		return nil, err
	}
	return &boltBk{bk}, err
}

func (tx *boltTx) Bucket(key []byte) Bucket {
	bk := tx.Tx.Bucket(key)
	if bk == nil {
		return nil
	}
	return &boltBk{bk}
}
func (tx *boltTx) Cursor() Cursor {
	return tx.Tx.Cursor()
}

func (bk *boltBk) CreateBucketIfNotExists(key []byte) (Bucket, error) {
	b, err := bk.bk.CreateBucketIfNotExists(key)
	if b == nil {
		return nil, err
	}
	return &boltBk{b}, err
}
func (bk *boltBk) CreateBucket(key []byte) (Bucket, error) {
	b, err := bk.bk.CreateBucket(key)
	if b == nil {
		return nil, err
	}
	return &boltBk{b}, err
}
func (bk *boltBk) DeleteBucket(key []byte) error {
	return bk.bk.DeleteBucket(key)
}
func (bk *boltBk) Bucket(key []byte) Bucket {
	b := bk.bk.Bucket(key)
	if b == nil {
		return nil
	}
	return &boltBk{b}
}
func (bk *boltBk) Cursor() Cursor {
	return bk.bk.Cursor()
}
func (bk *boltBk) Get(key []byte) []byte {
	return bk.bk.Get(key)
}
func (bk *boltBk) Put(key, val []byte) error {
	return bk.bk.Put(key, val)
}
