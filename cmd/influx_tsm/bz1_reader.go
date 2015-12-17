package main

import (
	"time"

	"github.com/boltdb/bolt"
)

type BZ1Reader struct {
	path string
	db   *bolt.DB // underlying database
}

func NewBZ1Reader(path string) *BZ1Reader {
	return &BZ1Reader{
		path: path,
	}
}

func (b *BZ1Reader) Open() error {
	// Open underlying storage.
	db, err := bolt.Open(b.path, 0666, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}
	b.db = db

	return nil
}

func (b *BZ1Reader) Next() (int64, []byte) {
	return 0, nil
}
