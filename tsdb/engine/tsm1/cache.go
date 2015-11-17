package tsm1

import (
	"container/list"
	"fmt"
	"sort"
	"sync"

	"github.com/influxdb/influxdb/tsdb"
)

var ErrCacheMemoryExceeded = fmt.Errorf("cache maximum memory size exceeded")
var ErrCacheInvalidCheckpoint = fmt.Errorf("invalid checkpoint")

type checkpoints []uint64

func (a checkpoints) Len() int           { return len(a) }
func (a checkpoints) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a checkpoints) Less(i, j int) bool { return a[i] < a[j] }

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
	if _, ok := l.elements[key]; ok {
		l.list.Remove(l.elements[key])
		delete(l.elements, key)
	}
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

// DoFromLeast iterates through the LRU, from least-recently used to most-recently used,
// calling the given function with each key.
func (l *lru) DoFromLeast(f func(key string)) {
	for e := l.list.Back(); e != nil; e = e.Prev() {
		f(e.Value.(string))
	}
}

// entry is the set of all values received for a given key.
type entry struct {
	values   Values // All stored values.
	unsorted bool   // Whether the data requires sorting and deduping before query.
	size     uint64 // Total Number of point-calculated bytes stored by this entry.
}

// newEntry returns a new instance of entry.
func newEntry() *entry {
	return &entry{}
}

// add adds the given values to the entry.
func (e *entry) add(values []Value) {
	for _, v := range values {
		// Only mark unsorted if not already marked.
		if !e.unsorted && len(e.values) > 1 {
			e.unsorted = e.values[len(e.values)-1].Time().UnixNano() >= v.Time().UnixNano()
		}
		e.values = append(e.values, v)
		e.size += uint64(v.Size())
	}
}

// dedupe remove duplicate entries for the same timestamp and sorts the entries.
func (e *entry) dedupe() {
	if !e.unsorted {
		return
	}
	e.values = e.values.Deduplicate()
	e.unsorted = false

	// Update size.
	e.size = 0
	for _, v := range e.values {
		e.size += uint64(v.Size())
	}
}

type entries struct {
	ee map[uint64]*entry
}

func newEntries() entries {
	return entries{
		ee: make(map[uint64]*entry),
	}
}

func (a entries) add(values []Value, checkpoint uint64) {
	e, ok := a.ee[checkpoint]
	if !ok {
		e = newEntry()
		a.ee[checkpoint] = e
	}
	e.add(values)
}

// purge deletes all data that is as old as the checkpoint. Returns point-calculated
// space freed-up.
func (a entries) purge(checkpoint uint64) uint64 {
	var size uint64
	for k, v := range a.ee {
		if k > checkpoint {
			continue
		}
		size += v.size
		delete(a.ee, k)
	}
	return size
}

// size returns point-calcuated storage size.
func (a entries) size() uint64 {
	var size uint64
	for _, v := range a.ee {
		size += v.size
	}
	return size
}

// clone returns the values for all entries under management, deduped and ordered by time.
func (a entries) clone() Values {
	var keys []uint64
	var values Values
	for k, _ := range a.ee {
		keys = append(keys, k)
	}
	sort.Sort(checkpoints(keys))

	for _, k := range keys {
		v := a.ee[k]
		v.dedupe()
		values = append(values, v.values...)
	}
	// XXX TO CONSIDER: it might be worth memoizing this.
	return values.Deduplicate()
}

// Cache maintains an in-memory store of Values for a set of keys. As data is added to the cache
// it will evict older data as necessary to make room for the new entries.
type Cache struct {
	mu         sync.RWMutex
	store      map[string]entries
	checkpoint uint64
	size       uint64
	maxSize    uint64

	lru *lru // List of entry keys from most recently accessed to least.
}

// NewCache returns an instance of a cache which will use a maximum of maxSize bytes of memory.
func NewCache(maxSize uint64) *Cache {
	return &Cache{
		maxSize: maxSize,
		store:   make(map[string]entries),
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
	if checkpoint < c.checkpoint {
		return ErrCacheInvalidCheckpoint
	}

	newSize := c.size + uint64(Values(values).Size())
	if newSize >= c.maxSize {
		c.evict(newSize - c.maxSize)
	}

	// Size OK now?
	if c.size >= c.maxSize {
		return ErrCacheMemoryExceeded
	}

	e, ok := c.store[key]
	if !ok {
		e = newEntries()
		c.store[key] = e
	}
	e.add(values, checkpoint)
	c.size = newSize

	// Mark entry as most-recently used.
	c.lru.MoveToFront(key)

	return nil
}

// SetCheckpoint informs the cache that updates received up to and including checkpoint
// can be safely evicted. Setting a checkpoint does not mean that eviction up to that
// point will actually occur.
func (c *Cache) SetCheckpoint(checkpoint uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if checkpoint < c.checkpoint {
		return ErrCacheInvalidCheckpoint
	}
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

	// e, ok := c.store[key]
	// if !ok {
	// 	return nil
	// }

	// // Mark entry as most-recently used.
	// c.lru.MoveToFront(key)

	// e.dedupe()
	// _ = e.clone()
	// // Actually return a cursor

	// Mark entry as most-recently used.
	c.lru.MoveToFront(key)
	return nil
}

// evict instructs the cache to evict data until all data with an associated checkpoint
// before the last checkpoint was set, or memory footprint decreases by the given size,
// whichever happens first. Returns the number of point-calculated bytes that were
// actually evicted.
func (c *Cache) evict(size uint64) uint64 {
	var freed uint64
	defer func() {
		c.size -= freed
	}()

	c.lru.DoFromLeast(func(key string) {
		e := c.store[key]
		freed += e.purge(c.checkpoint)
		if e.size() == 0 {
			// If the entry for the key is empty, remove all reference from the store.
			delete(c.store, key)
		}

		if freed >= size {
			return
		}
	})

	return freed
}
