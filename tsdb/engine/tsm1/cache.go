package tsm1

import (
	"fmt"
	"sort"
	"sync"
)

var ErrCacheMemoryExceeded = fmt.Errorf("cache maximum memory size exceeded")
var ErrCacheInvalidCheckpoint = fmt.Errorf("invalid checkpoint")

type checkpoints []uint64

func (a checkpoints) Len() int           { return len(a) }
func (a checkpoints) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a checkpoints) Less(i, j int) bool { return a[i] < a[j] }

// entry is a set of values and some metadata.
type entry struct {
	values Values // All stored values.
	size   uint64 // Total Number of point-calculated bytes stored by this entry.
}

// newEntry returns a new instance of entry.
func newEntry() *entry {
	return &entry{}
}

// add adds the given values to the entry.
func (e *entry) add(values []Value) {
	e.values = append(e.values, values...)
	e.size += uint64(Values(values).Size())
}

// entries maps checkpoints to entry objects.
type entries struct {
	m    map[uint64]*entry
	size uint64
}

// newEntries returns an instance of entries.
func newEntries() *entries {
	return &entries{
		m: make(map[uint64]*entry),
	}
}

// add adds checkpointed values to the entries object.
func (a *entries) add(checkpoint uint64, values []Value) {
	e, ok := a.m[checkpoint]
	if !ok {
		e = newEntry()
		a.m[checkpoint] = e
	}
	oldSize := e.size
	e.add(values)
	a.size += e.size - oldSize
}

// evict evicts all data associated with checkpoints up to and including
// the given checkpoint.
func (a *entries) evict(checkpoint uint64) {
	for k, v := range a.m {
		if k > checkpoint {
			continue
		}
		a.size -= v.size
		delete(a.m, k)
	}
}

// clone returns a copy of all underlying Values. Values are not sorted, nor deduped.
func (a *entries) clone() Values {
	var checkpoints checkpoints
	for k, _ := range a.m {
		checkpoints = append(checkpoints, k)
	}
	sort.Sort(checkpoints)

	var values Values
	for _, k := range checkpoints {
		values = append(values, a.m[k].values...)
	}
	return values
}

// dedupe returns a copy of all underlying Values. Values are deduped and sorted.
func (a *entries) dedupe() Values {
	return a.clone().Deduplicate()
}

// Cache maintains an in-memory store of Values for a set of keys. As data is added to the cache
// it will evict older data as necessary to make room for the new entries.
type Cache struct {
	mu         sync.RWMutex
	store      map[string]*entries
	checkpoint uint64
	size       uint64
	maxSize    uint64
}

// NewCache returns an instance of a cache which will use a maximum of maxSize bytes of memory.
func NewCache(maxSize uint64) *Cache {
	return &Cache{
		maxSize: maxSize,
		store:   make(map[string]*entries),
	}
}

// Write writes the set of values for the key to the cache. It associates the data with
// the given checkpoint. This function is goroutine-safe.
//
// TODO: This function is a significant potential bottleneck. It is possible that keys could
// be modified while an eviction process was taking place, so a big cache-level lock is in place.
// Need to revisit this. It's correct but may not be performant (it is the same as the existing
// design however)
func (c *Cache) Write(key string, values []Value, checkpoint uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Enough room in the cache?
	if c.size+uint64(Values(values).Size()) > c.maxSize {
		return ErrCacheMemoryExceeded
	}
	return c.write(key, values, checkpoint)
}

// WriteMulti writes the map of keys and associated values to the cache. It associates the
// data with the given checkpoint. This function is goroutine-safe.
func (c *Cache) WriteMulti(values map[string][]Value, checkpoint uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	totalSz := 0
	for _, v := range values {
		totalSz += Values(v).Size()
	}

	// Enough room in the cache?
	if c.size+uint64(totalSz) > c.maxSize {
		return ErrCacheMemoryExceeded
	}

	for k, v := range values {
		if err := c.write(k, v, checkpoint); err != nil {
			return err
		}
	}
	return nil
}

// SetCheckpoint informs the cache that updates received up to and including checkpoint
// can be safely evicted.
func (c *Cache) SetCheckpoint(checkpoint uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if checkpoint < c.checkpoint {
		return ErrCacheInvalidCheckpoint
	}
	c.checkpoint = checkpoint
	c.evict()
	return nil
}

// Size returns the number of point-calcuated bytes the cache currently uses.
func (c *Cache) Size() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.size
}

// MaxSize returns the maximum number of bytes the cache may consume.
func (c *Cache) MaxSize() uint64 {
	return c.maxSize
}

// Keys returns a sorted slice of all keys under management by the cache.
func (c *Cache) Keys() []string {
	var a []string
	for k, _ := range c.store {
		a = append(a, k)
	}
	sort.Strings(a)
	return a
}

// Checkpoint returns the current checkpoint for the cache.
func (c *Cache) Checkpoint() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.checkpoint
}

// Evict forces the cache to evict.
func (c *Cache) Evict() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.evict()
}

// Values returns a copy of all values, deduped and sorted, for the given key.
func (c *Cache) Values(key string) Values {
	values := func() Values {
		c.mu.RLock()
		defer c.mu.RUnlock()
		e, ok := c.store[key]
		if !ok {
			return nil
		}
		return e.clone()
	}()
	// Now have copy, so perform dedupe and sort outside of lock, unblocking
	// writes.

	if values == nil {
		return nil
	}
	return values.Deduplicate()
}

// evict instructs the cache to evict data up to and including the current checkpoint.
func (c *Cache) evict() {
	for key, entries := range c.store {
		oldSize := entries.size
		entries.evict(c.checkpoint)
		c.size -= oldSize - entries.size
		if entries.size == 0 {
			// All data for the key evicted.
			delete(c.store, key)
		}
	}
}

// write writes the set of values for the key to the cache. It associates the data with
// the given checkpoint. This function assumes the lock has been taken and does not
// enforce the cache size limits.
func (c *Cache) write(key string, values []Value, checkpoint uint64) error {
	if checkpoint < c.checkpoint {
		return ErrCacheInvalidCheckpoint
	}

	e, ok := c.store[key]
	if !ok {
		e = newEntries()
		c.store[key] = e
	}
	oldSize := e.size
	e.add(checkpoint, values)
	c.size += e.size - oldSize

	return nil
}
