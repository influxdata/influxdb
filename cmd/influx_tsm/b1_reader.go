package main

import (
	"time"

	"github.com/boltdb/bolt"
)

type B1Reader struct {
	path string
}

func NewB1Reader(path string) *B1Reader {
	return &B1Reader{
		path: path,
	}
}

func (b *B1Reader) Open() error {
	// Open underlying storage.
	db, err := bolt.Open(b.path, 0666, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}
	b.db = db

	return nil
}

func (b *B1Reader) Next() (int64, []byte) {
	return 0, nil
}

func (b *B1Reader) Close() error {
	return nil
}
