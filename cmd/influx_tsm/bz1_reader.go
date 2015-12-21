package main

import (
	"time"

	"github.com/boltdb/bolt"
	"github.com/golang/snappy"
)

type BZ1Reader struct {
	path string
	db   *bolt.DB
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

	var data []byte
	err = b.db.View(func(tx *bolt.Tx) error {
		buf := tx.Bucket([]byte("meta")).Get([]byte("series"))
		if b == nil {
			// No data in this shard.
			return nil
		}

		data, err = snappy.Decode(nil, buf)
		if err != nil {
			return err
		}
		return nil
	})

	return nil
}

// Next returns the next timestamp and values available. It returns -1 for the
// timestamp when no values remain.
func (b *BZ1Reader) Next() (int64, []byte) {
	return 0, nil
}

func (b *BZ1Reader) Close() error {
	return nil
}
