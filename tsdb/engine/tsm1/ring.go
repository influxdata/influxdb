package tsm1

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash"
)

// partitions is the number of partitions we used in the ring's continuum. It
// basically defines the maximum number of partitions you can have in the ring.
// If a smaller number of partitions are chosen when creating a ring, then
// they're evenly spread across this many partitions in the ring.
const partitions = 4096

// ring is a structure that maps series keys to entries.
//
// ring is implemented as a crude hash ring, in so much that you can have
// variable numbers of members in the ring, and the appropriate member for a
// given series key can always consistently be found. Unlike a true hash ring
// though, this ring is not resizeable—there must be at most 256 members in the
// ring, and the number of members must always be a power of 2.
//
// ring works as follows: Each member of the ring contains a single store, which
// contains a map of series keys to entries. A ring always has 256 partitions,
// and a member takes up one or more of these partitions (depending on how many
// members are specified to be in the ring)
//
// To determine the partition that a series key should be added to, the series
// key is hashed and the first 8 bits are used as an index to the ring.
//
type ring struct {
	// The unique set of partitions in the ring.
	// len(partitions) <= len(continuum)
	partitions []*partition

	// A mapping of partition to location on the ring continuum. This is used
	// to lookup a partition.
	continuum []*partition

	// Number of keys within the ring. This is used to provide a hint for
	// allocating the return values in keys(). It will not be perfectly accurate
	// since it doesn't consider adding duplicate keys, or trying to remove non-
	// existent keys.
	keysHint int64
}

// newring returns a new ring initialised with n partitions. n must always be a
// power of 2, and for performance reasons should be larger than the number of
// cores on the host. The supported set of values for n is:
//
//     {1, 2, 4, 8, 16, 32, 64, 128, 256}.
//
func newring(n int) (*ring, error) {
	if n <= 0 || n > partitions {
		return nil, fmt.Errorf("invalid number of paritions: %d", n)
	}

	r := ring{
		continuum: make([]*partition, partitions), // maximum number of partitions.
	}

	// The trick here is to map N partitions to all points on the continuum,
	// such that the first eight bits of a given hash will map directly to one
	// of the N partitions.
	for i := 0; i < len(r.continuum); i++ {
		if (i == 0 || i%(partitions/n) == 0) && len(r.partitions) < n {
			r.partitions = append(r.partitions, &partition{
				store:          make(map[string]*entry),
				entrySizeHints: make(map[uint64]int),
			})
		}
		r.continuum[i] = r.partitions[len(r.partitions)-1]
	}
	return &r, nil
}

// reset resets the ring so it can be reused. Before removing references to entries
// within each partition it gathers sizing information to provide hints when
// reallocating entries in partition maps.
//
// reset is not safe for use by multiple goroutines.
func (r *ring) reset() {
	for _, partition := range r.partitions {
		partition.reset()
	}
	r.keysHint = 0
}

// getPartition retrieves the hash ring partition associated with the provided
// key.
func (r *ring) getPartition(key string) *partition {
	return r.continuum[int(xxhash.Sum64([]byte(key))%partitions)]
}

// entry returns the entry for the given key.
// entry is safe for use by multiple goroutines.
func (r *ring) entry(key string) (*entry, bool) {
	return r.getPartition(key).entry(key)
}

// write writes values to the entry in the ring's partition associated with key.
// If no entry exists for the key then one will be created.
// write is safe for use by multiple goroutines.
func (r *ring) write(key string, values Values) error {
	return r.getPartition(key).write(key, values)
}

// add adds an entry to the ring.
func (r *ring) add(key string, entry *entry) {
	r.getPartition(key).add(key, entry)
	atomic.AddInt64(&r.keysHint, 1)
}

// remove deletes the entry for the given key.
// remove is safe for use by multiple goroutines.
func (r *ring) remove(key string) {
	r.getPartition(key).remove(key)
	if r.keysHint > 0 {
		atomic.AddInt64(&r.keysHint, -1)
	}
}

// keys returns all the keys from all partitions in the hash ring. The returned
// keys will be in order if sorted is true.
func (r *ring) keys(sorted bool) []string {
	keys := make([]string, 0, atomic.LoadInt64(&r.keysHint))
	for _, p := range r.partitions {
		keys = append(keys, p.keys()...)
	}

	if sorted {
		sort.Strings(keys)
	}
	return keys
}

// apply applies the provided function to every entry in the ring under a read
// lock using a separate goroutine for each partition. The provided function
// will be called with each key and the corresponding entry. The first error
// encountered will be returned, if any. apply is safe for use by multiple
// goroutines.
func (r *ring) apply(f func(string, *entry) error) error {

	var (
		wg  sync.WaitGroup
		res = make(chan error, len(r.partitions))
	)

	for _, p := range r.partitions {
		wg.Add(1)

		go func(p *partition) {
			defer wg.Done()

			p.mu.RLock()
			for k, e := range p.store {
				if err := f(k, e); err != nil {
					res <- err
					p.mu.RUnlock()
					return
				}
			}
			p.mu.RUnlock()
		}(p)
	}

	go func() {
		wg.Wait()
		close(res)
	}()

	// Collect results.
	for err := range res {
		if err != nil {
			return err
		}
	}
	return nil
}

// applySerial is similar to apply, but invokes f on each partition in the same
// goroutine.
// apply is safe for use by multiple goroutines.
func (r *ring) applySerial(f func(string, *entry) error) error {
	for _, p := range r.partitions {
		p.mu.RLock()
		for k, e := range p.store {
			if err := f(k, e); err != nil {
				p.mu.RUnlock()
				return err
			}
		}
		p.mu.RUnlock()
	}
	return nil
}

// partition provides safe access to a map of series keys to entries.
type partition struct {
	mu    sync.RWMutex
	store map[string]*entry

	// entrySizeHints stores hints for appropriate sizes to pre-allocate the
	// []Values in an entry. entrySizeHints will only contain hints for entries
	// that were present prior to the most recent snapshot, preventing unbounded
	// growth over time.
	entrySizeHints map[uint64]int
}

// entry returns the partition's entry for the provided key.
// It's safe for use by multiple goroutines.
func (p *partition) entry(key string) (*entry, bool) {
	p.mu.RLock()
	e, ok := p.store[key]
	p.mu.RUnlock()
	return e, ok
}

// write writes the values to the entry in the partition, creating the entry
// if it does not exist.
// write is safe for use by multiple goroutines.
func (p *partition) write(key string, values Values) error {
	p.mu.RLock()
	e, ok := p.store[key]
	p.mu.RUnlock()
	if ok {
		// Hot path.
		return e.add(values)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Check again.
	if e, ok = p.store[key]; ok {
		return e.add(values)
	}

	// Create a new entry using a preallocated size if we have a hint available.
	hint, _ := p.entrySizeHints[xxhash.Sum64([]byte(key))]
	e, err := newEntryValues(values, hint)
	if err != nil {
		return err
	}

	p.store[key] = e
	return nil
}

// add adds a new entry for key to the partition.
func (p *partition) add(key string, entry *entry) {
	p.mu.Lock()
	p.store[key] = entry
	p.mu.Unlock()
}

// remove deletes the entry associated with the provided key.
// remove is safe for use by multiple goroutines.
func (p *partition) remove(key string) {
	p.mu.Lock()
	delete(p.store, key)
	p.mu.Unlock()
}

// keys returns an unsorted slice of the keys in the partition.
func (p *partition) keys() []string {
	p.mu.RLock()
	keys := make([]string, 0, len(p.store))
	for k := range p.store {
		keys = append(keys, k)
	}
	p.mu.RUnlock()
	return keys
}

// reset resets the partition by reinitialising the store. reset returns hints
// about sizes that the entries within the store could be reallocated with.
func (p *partition) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Collect the allocated sizes of values for each entry in the store.
	p.entrySizeHints = make(map[uint64]int)
	for k, entry := range p.store {
		// If the capacity is large then there are many values in the entry.
		// Store a hint to pre-allocate the next time we see the same entry.
		entry.mu.RLock()
		if cap(entry.values) > 128 { // 4 x the default entry capacity size.
			p.entrySizeHints[xxhash.Sum64([]byte(k))] = cap(entry.values)
		}
		entry.mu.RUnlock()
	}

	// Reset the store.
	p.store = make(map[string]*entry, len(p.store))
}
