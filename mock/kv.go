package mock

import (
	"context"

	"github.com/influxdata/influxdb/kv"
)

var _ (kv.Store) = (*Store)(nil)

// Store is a mock kv.Store
type Store struct {
	ViewFn   func(func(kv.Tx) error) error
	UpdateFn func(func(kv.Tx) error) error
}

// View opens up a transaction that will not write to any data. Implementing interfaces
// should take care to ensure that all view transactions do not mutate any data.
func (s *Store) View(ctx context.Context, fn func(kv.Tx) error) error {
	return s.ViewFn(fn)
}

// Update opens up a transaction that will mutate data.
func (s *Store) Update(ctx context.Context, fn func(kv.Tx) error) error {
	return s.UpdateFn(fn)
}

var _ (kv.Tx) = (*Tx)(nil)

// Tx is mock of a kv.Tx.
type Tx struct {
	BucketFn      func(b []byte) (kv.Bucket, error)
	ContextFn     func() context.Context
	WithContextFn func(ctx context.Context)
}

// Bucket possibly creates and returns bucket, b.
func (t *Tx) Bucket(b []byte) (kv.Bucket, error) {
	return t.BucketFn(b)
}

// Context returns the context associated with this Tx.
func (t *Tx) Context() context.Context {
	return t.ContextFn()
}

// WithContext associates a context with this Tx.
func (t *Tx) WithContext(ctx context.Context) {
	t.WithContextFn(ctx)
}

var _ (kv.Bucket) = (*Bucket)(nil)

// Bucket is the abstraction used to perform get/put/delete/get-many operations
// in a key value store
type Bucket struct {
	GetFn    func(key []byte) ([]byte, error)
	CursorFn func() (kv.Cursor, error)
	PutFn    func(key, value []byte) error
	DeleteFn func(key []byte) error
}

// Get returns a key within this bucket. Errors if key does not exist.
func (b *Bucket) Get(key []byte) ([]byte, error) {
	return b.GetFn(key)
}

// Cursor returns a cursor at the beginning of this bucket.
func (b *Bucket) Cursor() (kv.Cursor, error) {
	return b.CursorFn()
}

// Put should error if the transaction it was called in is not writable.
func (b *Bucket) Put(key, value []byte) error {
	return b.PutFn(key, value)
}

// Delete should error if the transaction it was called in is not writable.
func (b *Bucket) Delete(key []byte) error {
	return b.DeleteFn(key)
}

var _ (kv.Cursor) = (*Cursor)(nil)

// Cursor is an abstraction for iterating/ranging through data. A concrete implementation
// of a cursor can be found in cursor.go.
type Cursor struct {
	SeekFn  func(prefix []byte) (k []byte, v []byte)
	FirstFn func() (k []byte, v []byte)
	LastFn  func() (k []byte, v []byte)
	NextFn  func() (k []byte, v []byte)
	PrevFn  func() (k []byte, v []byte)
}

// Seek moves the cursor forward until reaching prefix in the key name.
func (c *Cursor) Seek(prefix []byte) (k []byte, v []byte) {
	return c.SeekFn(prefix)
}

// First moves the cursor to the first key in the bucket.
func (c *Cursor) First() (k []byte, v []byte) {
	return c.FirstFn()
}

// Last moves the cursor to the last key in the bucket.
func (c *Cursor) Last() (k []byte, v []byte) {
	return c.LastFn()
}

// Next moves the cursor to the next key in the bucket.
func (c *Cursor) Next() (k []byte, v []byte) {
	return c.NextFn()
}

// Prev moves the cursor to the prev key in the bucket.
func (c *Cursor) Prev() (k []byte, v []byte) {
	return c.PrevFn()
}
