package kv

import (
	"context"
	"errors"
)

var (
	// ErrKeyNotFound is the error returned when the key requested is not found.
	ErrKeyNotFound = errors.New("key not found")
	// ErrTxNotWritable is the error returned when an mutable operation is called during
	// a non-writable transaction.
	ErrTxNotWritable = errors.New("transaction is not writable")
)

// Store is an interface for a generic key value store. It is modeled after
// the boltdb database struct.
type Store interface {
	// View opens up a transaction that will not write to any data. Implementing interfaces
	// should take care to ensure that all view transactions do not mutate any data.
	View(func(Tx) error) error
	// Update opens up a transaction that will mutate data.
	Update(func(Tx) error) error
}

// Tx is a transaction in the store.
type Tx interface {
	Bucket(b []byte) (Bucket, error)
	Context() context.Context
	WithContext(ctx context.Context)
}

// Bucket is the abstraction used to perform get/put/delete/get-many operations
// in a key value store.
type Bucket interface {
	Get(key []byte) ([]byte, error)
	Cursor() (Cursor, error)
	// Put should error if the transaction it was called in is not writable.
	Put(key, value []byte) error
	// Delete should error if the transaction it was called in is not writable.
	Delete(key []byte) error
}

// Cursor is an abstraction for iterating/ranging through data. A concrete implementation
// of a cursor can be found in cursor.go.
type Cursor interface {
	Seek(prefix []byte) (k []byte, v []byte)
	First() (k []byte, v []byte)
	Last() (k []byte, v []byte)
	Next() (k []byte, v []byte)
	Prev() (k []byte, v []byte)
}
