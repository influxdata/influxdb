package inmem

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/google/btree"
	"github.com/influxdata/influxdb/v2/kv"
)

// ensure *KVStore implement kv.SchemaStore interface
var _ kv.SchemaStore = (*KVStore)(nil)

// cursorBatchSize is the size of a batch sent by a forward cursors
// tree iterator
const cursorBatchSize = 1000

// KVStore is an in memory btree backed kv.Store.
type KVStore struct {
	mu      sync.RWMutex
	buckets map[string]*Bucket
	ro      map[string]*bucket
}

// NewKVStore creates an instance of a KVStore.
func NewKVStore() *KVStore {
	return &KVStore{
		buckets: map[string]*Bucket{},
		ro:      map[string]*bucket{},
	}
}

// View opens up a transaction with a read lock.
func (s *KVStore) View(ctx context.Context, fn func(kv.Tx) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return fn(&Tx{
		kv:       s,
		writable: false,
		ctx:      ctx,
	})
}

// Update opens up a transaction with a write lock.
func (s *KVStore) Update(ctx context.Context, fn func(kv.Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return fn(&Tx{
		kv:       s,
		writable: true,
		ctx:      ctx,
	})
}

// CreateBucket creates a bucket with the provided name if one
// does not exist.
func (s *KVStore) CreateBucket(ctx context.Context, name []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.buckets[string(name)]
	if !ok {
		bkt := &Bucket{btree: btree.New(2)}
		s.buckets[string(name)] = bkt
		s.ro[string(name)] = &bucket{Bucket: bkt}
	}

	return nil
}

// DeleteBucket creates a bucket with the provided name if one
// does not exist.
func (s *KVStore) DeleteBucket(ctx context.Context, name []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.buckets, string(name))

	return nil
}

// Lock and unlock are only used when doing a backup, so panic if they are called here.
func (s *KVStore) Lock() {
	panic("not implemented")
}

func (s *KVStore) Unlock() {
	panic("not implemented")
}

func (s *KVStore) Backup(ctx context.Context, w io.Writer) error {
	panic("not implemented")
}

func (s *KVStore) CreateBucketManifests(ctx context.Context, w io.Writer) error {
	panic("not implemented")
}

func (s *KVStore) Restore(ctx context.Context, r io.Reader) error {
	panic("not implemented")
}

// Flush removes all data from the buckets.  Used for testing.
func (s *KVStore) Flush(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range s.buckets {
		b.btree.Clear(false)
	}
}

// Buckets returns the names of all buckets within inmem.KVStore.
func (s *KVStore) Buckets(ctx context.Context) [][]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()

	buckets := make([][]byte, 0, len(s.buckets))
	for b := range s.buckets {
		buckets = append(buckets, []byte(b))
	}
	return buckets
}

// Tx is an in memory transaction.
// TODO: make transactions actually transactional
type Tx struct {
	kv       *KVStore
	writable bool
	ctx      context.Context
}

// Context returns the context for the transaction.
func (t *Tx) Context() context.Context {
	return t.ctx
}

// WithContext sets the context for the transaction.
func (t *Tx) WithContext(ctx context.Context) {
	t.ctx = ctx
}

// Bucket retrieves the bucket at the provided key.
func (t *Tx) Bucket(b []byte) (kv.Bucket, error) {
	bkt, ok := t.kv.buckets[string(b)]
	if !ok {
		return nil, fmt.Errorf("bucket %q: %w", string(b), kv.ErrBucketNotFound)
	}

	if t.writable {
		return bkt, nil
	}

	return t.kv.ro[string(b)], nil
}

// Bucket is a btree that implements kv.Bucket.
type Bucket struct {
	mu    sync.RWMutex
	btree *btree.BTree
}

type bucket struct {
	kv.Bucket
}

// Put wraps the put method of a kv bucket and ensures that the
// bucket is writable.
func (b *bucket) Put(_, _ []byte) error {
	return kv.ErrTxNotWritable
}

// Delete wraps the delete method of a kv bucket and ensures that the
// bucket is writable.
func (b *bucket) Delete(_ []byte) error {
	return kv.ErrTxNotWritable
}

type item struct {
	key   []byte
	value []byte
}

// Less is used to implement btree.Item.
func (i *item) Less(b btree.Item) bool {
	j, ok := b.(*item)
	if !ok {
		return false
	}

	return bytes.Compare(i.key, j.key) < 0
}

// Get retrieves the value at the provided key.
func (b *Bucket) Get(key []byte) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	i := b.btree.Get(&item{key: key})

	if i == nil {
		return nil, kv.ErrKeyNotFound
	}

	j, ok := i.(*item)
	if !ok {
		return nil, fmt.Errorf("error item is type %T not *item", i)
	}

	return j.value, nil
}

// Get retrieves a batch of values for the provided keys.
func (b *Bucket) GetBatch(keys ...[]byte) ([][]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	values := make([][]byte, len(keys))

	for idx, key := range keys {
		i := b.btree.Get(&item{key: key})

		if i == nil {
			// leave value as nil slice
			continue
		}

		j, ok := i.(*item)
		if !ok {
			return nil, fmt.Errorf("error item is type %T not *item", i)
		}

		values[idx] = j.value
	}

	return values, nil
}

// Put sets the key value pair provided.
func (b *Bucket) Put(key []byte, value []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	_ = b.btree.ReplaceOrInsert(&item{key: key, value: value})
	return nil
}

// Delete removes the key provided.
func (b *Bucket) Delete(key []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	_ = b.btree.Delete(&item{key: key})
	return nil
}

// Cursor creates a static cursor from all entries in the database.
func (b *Bucket) Cursor(opts ...kv.CursorHint) (kv.Cursor, error) {
	var o kv.CursorHints
	for _, opt := range opts {
		opt(&o)
	}

	// TODO we should do this by using the Ascend/Descend methods that
	//  the btree provides.
	pairs, err := b.getAll(&o)
	if err != nil {
		return nil, err
	}

	return kv.NewStaticCursor(pairs), nil
}

func (b *Bucket) getAll(o *kv.CursorHints) ([]kv.Pair, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	fn := o.PredicateFn

	var pairs []kv.Pair
	var err error
	b.btree.Ascend(func(i btree.Item) bool {
		j, ok := i.(*item)
		if !ok {
			err = fmt.Errorf("error item is type %T not *item", i)
			return false
		}

		if fn == nil || fn(j.key, j.value) {
			pairs = append(pairs, kv.Pair{Key: j.key, Value: j.value})
		}

		return true
	})

	if err != nil {
		return nil, err
	}

	return pairs, nil
}

type pair struct {
	kv.Pair
	err error
}

// ForwardCursor returns a directional cursor which starts at the provided seeked key
func (b *Bucket) ForwardCursor(seek []byte, opts ...kv.CursorOption) (kv.ForwardCursor, error) {
	config := kv.NewCursorConfig(opts...)
	if config.Prefix != nil && !bytes.HasPrefix(seek, config.Prefix) {
		return nil, fmt.Errorf("seek bytes %q not prefixed with %q: %w", string(seek), string(config.Prefix), kv.ErrSeekMissingPrefix)
	}

	var (
		pairs = make(chan []pair)
		stop  = make(chan struct{})
		send  = func(batch []pair) bool {
			if len(batch) == 0 {
				return true
			}

			select {
			case pairs <- batch:
				return true
			case <-stop:
				return false
			}
		}
	)

	go func() {
		defer close(pairs)

		var (
			batch     []pair
			fn        = config.Hints.PredicateFn
			iterate   = b.ascend
			skipFirst = config.SkipFirst
			seen      int
		)

		if config.Direction == kv.CursorDescending {
			iterate = b.descend
			if len(seek) == 0 {
				if item, ok := b.btree.Max().(*item); ok {
					seek = item.key
				}
			}
		}

		b.mu.RLock()
		iterate(seek, config, func(i btree.Item) bool {
			select {
			case <-stop:
				// if signalled to stop then exit iteration
				return false
			default:
			}

			// if skip first
			if skipFirst {
				skipFirst = false
				return true
			}

			// enforce limit
			if config.Limit != nil && seen >= *config.Limit {
				return false
			}

			j, ok := i.(*item)
			if !ok {
				batch = append(batch, pair{err: fmt.Errorf("error item is type %T not *item", i)})

				return false
			}

			if config.Prefix != nil && !bytes.HasPrefix(j.key, config.Prefix) {
				return false
			}

			if fn == nil || fn(j.key, j.value) {
				batch = append(batch, pair{Pair: kv.Pair{Key: j.key, Value: j.value}})
				seen++
			}

			if len(batch) < cursorBatchSize {
				return true
			}

			if send(batch) {
				// batch flushed successfully so we can
				// begin a new batch
				batch = nil

				return true
			}

			// we've been signalled to stop
			return false
		})
		b.mu.RUnlock()

		// send if any left in batch
		send(batch)
	}()

	return &ForwardCursor{pairs: pairs, stop: stop}, nil
}

func (b *Bucket) ascend(seek []byte, config kv.CursorConfig, it btree.ItemIterator) {
	b.btree.AscendGreaterOrEqual(&item{key: seek}, it)
}

func (b *Bucket) descend(seek []byte, config kv.CursorConfig, it btree.ItemIterator) {
	b.btree.DescendLessOrEqual(&item{key: seek}, it)
}

// ForwardCursor is a kv.ForwardCursor which iterates over an in-memory btree
type ForwardCursor struct {
	pairs <-chan []pair

	cur []pair
	n   int

	stop   chan struct{}
	closed bool
	// error found during iteration
	err error
}

// Err returns a non-nil error when an error occurred during cursor iteration.
func (c *ForwardCursor) Err() error {
	return c.err
}

// Close releases the producing goroutines for the forward cursor.
// It blocks until the producing goroutine exits.
func (c *ForwardCursor) Close() error {

	if c.closed {
		return nil
	}

	close(c.stop)

	c.closed = true

	return nil
}

// Next returns the next key/value pair in the cursor
func (c *ForwardCursor) Next() ([]byte, []byte) {
	if c.err != nil || c.closed {
		return nil, nil
	}

	if c.n >= len(c.cur) {
		var ok bool
		c.cur, ok = <-c.pairs
		if !ok {
			return nil, nil
		}

		c.n = 0
	}

	pair := c.cur[c.n]
	c.err = pair.err
	c.n++

	return pair.Key, pair.Value
}
