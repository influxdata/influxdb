package tsm1

import (
	"container/list"
	"fmt"
	"sync"

	"github.com/influxdb/influxdb/tsdb"
)

// lru orders string keys from least-recently used to most-recently used. It is not
// goroutine safe.
type lru struct {
	list     *list.List
	elements map[string]*list.Element
}

// newLRU returns an initialized LRU.
func newLRU() *lru {
	return &lru{
		list:     list.New(),
		elements: make(map[string]*list.Element),
	}
}

// MoveToFront marks key as the most recently used key.
func (l *lru) MoveToFront(key string) {
	e, ok := l.elements[key]
	if !ok {
		l.elements[key] = l.list.PushFront(key)
		return
	}
	l.list.MoveToFront(e)
}

// Remove removes key from the LRU. If the key does not exist nothing happens.
func (l *lru) Remove(key string) {
	l.list.Remove(l.elements[key])
	delete(l.elements, key)
}

// Front returns the most-recently used key. If there is no such key, then "" is returned.
func (l *lru) Front() string {
	e := l.list.Front()
	if e == nil {
		return ""
	}
	return e.Value.(string)
}

// Back returns the least-recently used key. If there is no such key, then "" is returned.
func (l *lru) Back() string {
	e := l.list.Back()
	if e == nil {
		return ""
	}
	return e.Value.(string)
}

// Do iterates through the LRU, from least-recently used to most-recently used,
// calling the given function with each key.
func (l *lru) Do(f func(key string)) {
	for e := l.list.Back(); e != nil; e = e.Prev() {
		f(e.Value.(string))
	}
}

// entry is the set of all values received for a given key. It's analogous to a cache-line
// in the sense that it is the smallest unit that can be evicted from the cache.
type entry struct {
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
	if !e.unsorted {
		// Nothing to do.
		return
	}

	e.unsorted = false
	return
}

// clone returns a copy of the values for this entry.
func (e *entry) clone() []Value {
	a := make([]Value, len(e.values))
	copy(a, e.values)
	return a
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
// the given checkpoint. This function is goroutine-safe.
//
// TODO: This function is a significant potential bottleneck. It is possible that keys could
// be modified while an eviction process was taking place, so a big cache-level lock is in place.
// Need to revisit this. It's correct but may not be performant (it is the same as the existing
// design however)
func (c *Cache) Write(key string, values []Value, checkpoint uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.size > c.maxSize {
		// XXX should really also adjust by size of incoming data.
		c.evict(c.size - c.maxSize)
	}

	// Size OK now?
	if c.size > c.maxSize {
		return fmt.Errorf("cache maximum memory size exceeded")
	}

	e, ok := c.store[key]
	if !ok {
		e := newEntry()
		c.store[key] = e
	}
	c.size += e.add(values, checkpoint)

	// Mark entry as most-recently used.
	c.lru.MoveToFront(key)

	return nil
}

// SetCheckpoint informs the cache that updates received up to and including checkpoint can be
// safely evicted. Setting a checkpoint does not mean that eviction up to that point will actually occur.
func (c *Cache) SetCheckpoint(checkpoint uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.checkpoint = checkpoint
	return nil
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

// Evict instructs the cache to evict.
func (c *Cache) Evict(size uint64) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.evict(size)
}

// Cursor returns a cursor for the given key.
func (c *Cache) Cursor(key string) tsdb.Cursor {
	c.mu.RLock()
	defer c.mu.RUnlock()

	e, ok := c.store[key]
	if !ok {
		return nil
	}

	// Mark entry as most-recently used.
	c.lru.MoveToFront(key)

	e.dedupe()
	_ = e.clone()
	// Actually return a cursor
	return nil
}

// evict instructs the cache to evict data until all data with an associated checkpoint
// before the last checkpoint was set, or memory footprint decreases by the given size.
// Returns the number of point-calculated bytes that were actually evicted.
func (c *Cache) evict(size uint64) uint64 {
	// Get the list of keys which can be evicted.
	evictions := []string{}
	var n uint64
	c.lru.Do(func(key string) {
		if n >= size {
			// Requested amount of data already marked for eviction.
			return
		}

		entry := c.store[key]
		if entry.checkpoint > c.checkpoint {
			// Eviction would mean queries from the cache could be incorrect.
			return
		}
		evictions = append(evictions, key)
		n += entry.size
	})

	// Now, perform the actual evictions.
	for _, key := range evictions {
		delete(c.store, key)
		c.lru.Remove(key)
	}

	c.size -= n
	return n
}

// IncrementSize increments the bookeeping of current memory storage.
func (c *Cache) IncrementSize(n uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.size += n
}
