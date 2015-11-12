package tsm1

import (
	"container/list"
	"fmt"
	"sync"
)

type lru struct {
	mu       sync.Mutex
	list     *list.List
	elements map[string]*list.Element
}

func newLRU() *lru {
	return &lru{
		list:     list.New(),
		elements: make(map[string]*list.Element),
	}
}

func (l *lru) MoveToFront(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.elements[key]
	if !ok {
		l.elements[key] = l.list.PushFront(key)
		return
	}
	l.list.MoveToFront(l.elements[key])
}

func (l *lru) Remove(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.list.Remove(l.elements[key])
}

func (l *lru) Front() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.list.Front().Value.(string)
}

func (l *lru) Back() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.list.Back().Value.(string)
}

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
	mu         sync.RWMutex
	store      map[string]*entry
	checkpoint uint64
	size       uint64
	maxSize    uint64

	lru *lru // List of entry keys from most recently accessed to least.
}

// NewCache returns an instance of a cache which will use a maximum of maxSize bytes of memory.
func NewCache(maxSize uint64) *Cache {
	return &Cache{
		maxSize: maxSize,
		store:   make(map[string]*entry),
		lru:     newLRU(),
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
			e := newEntry()
			c.store[key] = e
		}
		c.mu.Unlock()
	}
	c.incrementSize(e.add(values, checkpoint))

	// Mark entry as most-recently used.
	// Lock LRU
	c.lru.MoveToFront(key)
	// Unlock LRU

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

// Evict instructs the cache to evict data until all data with an associated checkpoint
// before the last checkpoint was set, or the memory footprint of the cache drops below
// the given size.
func (c *Cache) Evict(size uint64) (uint64, error) {
	// lock cache
	// defer lock of cache
	// Lock lru
	// Defer unlock of lru
	for e := l.Front(); e != nil; e = e.Next() {
		if c.size <= size {
			break
		}
		least := c.lru.Back()
		e := c.store[c.lru.Back()]
		delete(c.store, least)
	}
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
