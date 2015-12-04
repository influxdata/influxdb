package tsm1

import (
	"fmt"
	"sort"
	"sync"
)

var ErrCacheMemoryExceeded = fmt.Errorf("cache maximum memory size exceeded")
var ErrCacheInvalidCheckpoint = fmt.Errorf("invalid checkpoint")

// entry is a set of values and some metadata.
type entry struct {
	values   Values // All stored values.
	needSort bool   // true if the values are out of order and require deduping.
}

// newEntry returns a new instance of entry.
func newEntry() *entry {
	return &entry{}
}

// add adds the given values to the entry.
func (e *entry) add(values []Value) {
	// if there are existing values make sure they're all less than the first of
	// the new values being added
	l := len(e.values)
	if l != 0 {
		lastValTime := e.values[l-1].UnixNano()
		if lastValTime >= values[0].UnixNano() {
			e.needSort = true
		}
	}
	e.values = append(e.values, values...)

	// if there's only one value, we know it's sorted
	if len(values) <= 1 {
		return
	}

	// make sure the new values were in sorted order
	min := values[0].UnixNano()
	for _, v := range values[1:] {
		if min >= v.UnixNano() {
			e.needSort = true
			break
		}
	}
}

// deduplicate sorts and orders the entry's values. If values are already deduped and
// and sorted, the function does no work and simply returns.
func (e *entry) deduplicate() {
	if !e.needSort {
		return
	}
	e.values = e.values.Deduplicate()
	e.needSort = false
}

// Cache maintains an in-memory store of Values for a set of keys.
type Cache struct {
	mu      sync.RWMutex
	store   map[string]*entry
	size    uint64
	maxSize uint64

	// snapshots are the cache objects that are currently being written to tsm files
	// they're kept in memory while flushing so they can be queried along with the cache.
	// they are read only and should never be modified
	snapshots     []*Cache
	snapshotsSize uint64
}

// NewCache returns an instance of a cache which will use a maximum of maxSize bytes of memory.
func NewCache(maxSize uint64) *Cache {
	return &Cache{
		maxSize: maxSize,
		store:   make(map[string]*entry),
	}
}

// Write writes the set of values for the key to the cache. This function is goroutine-safe.
// It returns an error if the cache has exceeded its max size.
func (c *Cache) Write(key string, values []Value) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Enough room in the cache?
	newSize := c.size + uint64(Values(values).Size())
	if c.maxSize > 0 && newSize+c.snapshotsSize > c.maxSize {
		return ErrCacheMemoryExceeded
	}

	c.write(key, values)
	c.size = newSize

	return nil
}

// WriteMulti writes the map of keys and associated values to the cache. This function is goroutine-safe.
// It returns an error if the cache has exceeded its max size.
func (c *Cache) WriteMulti(values map[string][]Value) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	totalSz := 0
	for _, v := range values {
		totalSz += Values(v).Size()
	}

	// Enough room in the cache?
	newSize := c.size + uint64(totalSz)
	if c.maxSize > 0 && newSize+c.snapshotsSize > c.maxSize {
		return ErrCacheMemoryExceeded
	}

	for k, v := range values {
		c.write(k, v)
	}
	c.size = newSize

	return nil
}

// Snapshot will take a snapshot of the current cache, add it to the slice of caches that
// are being flushed, and reset the current cache with new values
func (c *Cache) Snapshot() *Cache {
	c.mu.Lock()
	defer c.mu.Unlock()

	snapshot := NewCache(c.maxSize)
	snapshot.store = c.store
	snapshot.size = c.size

	c.store = make(map[string]*entry)
	c.size = 0

	c.snapshots = append(c.snapshots, snapshot)
	c.snapshotsSize += snapshot.size

	// sort the snapshot before returning it. The compactor and any queries
	// coming in while it writes will need the values sorted
	for _, e := range snapshot.store {
		e.deduplicate()
	}

	return snapshot
}

// ClearSnapshot will remove the snapshot cache from the list of flushing caches and
// adjust the size
func (c *Cache) ClearSnapshot(snapshot *Cache) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, cache := range c.snapshots {
		if cache == snapshot {
			c.snapshots = append(c.snapshots[:i], c.snapshots[i+1:]...)
			c.snapshotsSize -= snapshot.size
			break
		}
	}
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

// Values returns a copy of all values, deduped and sorted, for the given key.
func (c *Cache) Values(key string) Values {
	c.mu.Lock()
	defer c.mu.Unlock()

	e := c.store[key]
	if e == nil {
		return nil
	}
	e.deduplicate()

	values := make(Values, len(e.values))
	copy(values, e.values)

	return values
}

// values returns the values for the key. It doesn't lock and assumes the data is
// already sorted. Should only be used in compact.go in the CacheKeyIterator
func (c *Cache) values(key string) Values {
	e := c.store[key]
	if e == nil {
		return nil
	}
	return e.values
}

// write writes the set of values for the key to the cache. This function assumes
// the lock has been taken and does not enforce the cache size limits.
func (c *Cache) write(key string, values []Value) {
	e, ok := c.store[key]
	if !ok {
		e = newEntry()
		c.store[key] = e
	}
	e.add(values)
}
