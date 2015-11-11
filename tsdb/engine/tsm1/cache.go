package tsm1

import (
	"fmt"
	"sync"
)

// entry is the set of all values received for a given key.
type entry struct {
	mu         sync.Mutex
	values     Values // All stored values.
	checkpoint uint64 // The checkpoint associated with the latest addition to values
	unsorted   bool   // Whether the data requires sorting and deduping before query.
	size       uint64 // Total Number of point-calculated bytes stored by this entry.
}

func newEntry() *entry {
	return &entry{}
}

// add adds the given values to the entry. Returns the increase in storage footprint.
func (e *entry) add(values []Value, chk uint64) uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()

	var inc uint64
	for _, v := range values {
		e.values = append(e.values, v)
		inc += uint64(v.Size())
		// Only mark unsorted if not already marked.
		if !e.unsorted {
			e.unsorted = e.values[len(values)-1].Time().UnixNano() >= v.Time().UnixNano()
		}
	}
	e.checkpoint = chk
	e.size += inc
	return inc
}

// dedupe prepares the entry for query.
func (e *entry) dedupe() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.unsorted {
		// Nothing to do.
		return
	}

	return
}

// Cache maintains an in-memory store of Values for a set of keys. As data is added to the cache
// it will evict older data as necessary to make room for the new entries.
type Cache struct {
	mu sync.RWMutex

	checkpoint uint64
	store      map[string]*entry
	size       uint64
	maxSize    uint64
}

// NewCache returns an instance of a cache which will use a maximum of maxSize bytes of memory.
func NewCache(maxSize uint64) *Cache {
	return &Cache{
		store:   make(map[string]*entry),
		maxSize: maxSize,
	}
}

// WriteKey writes the set of values for the key to the cache. It associates the data with
// the given checkpoint. This function is goroutine-safe and different keys can be updated
// concurrently.
func (c *Cache) Write(key string, values []Value, checkpoint uint64) error {
	if c.Size() > c.maxSize {
		// Try eviction.
		return fmt.Errorf("maximum memory usage exceeded")
	}

	c.mu.RLock()
	e, ok := c.store[key]
	c.mu.RUnlock()
	if !ok {
		// Check again under the write lock
		c.mu.Lock()
		e, ok = c.store[key]
		if !ok {
			e = newEntry()
			c.store[key] = e
		}
		c.mu.Unlock()
	}
	c.incrementSize(e.add(values, checkpoint))

	return nil

}

// SetCheckpoint informs the cache that updates received up to and including checkpoint can be
// safely evicted. Setting a checkpoint does not mean that eviction will actually occur.
func (c *Cache) SetCheckpoint(checkpoint uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.checkpoint = checkpoint
	return nil
}

// Evict forces the cache to evict all data with an associated checkpoint before the last
// checkpoint that was set. It returns the number of point-based bytes freed as a result.
// Eviction should normally be left to the cache itself.
func (c *Cache) Evict() (uint64, error) {
	return 0, nil
}

// Size returns the number of bytes the cache currently uses.
func (c *Cache) Size() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.size
}

// MaxSize returns the maximum number of bytes the cache may consume.
func (c *Cache) MaxSize() uint64 {
	return c.maxSize
}

// Checkpoint returns the current checkpoint for the cache.
func (c *Cache) Checkpoint() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.checkpoint
}

// incrementSize increments the bookeeping of current memory storage.
func (c *Cache) incrementSize(n uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.size += n
}
