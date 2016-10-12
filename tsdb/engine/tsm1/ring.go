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
const partitions = 256

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
	partitions []*partition // The unique set of partitions in the ring.
	continuum  []*partition // A mapping of parition to location on the ring continuum.

	// Number of entries held within the ring. This is used to provide a
	// hint for allocating a []string to return all keys. It will not be
	// perfectly accurate since it doesn't consider adding duplicate keys,
	// or trying to remove non-existent keys.
	entryN int64
}

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

// getPartition retrieves the hash ring partition associated with the provided
// key.
func (r *ring) getPartition(key string) *partition {
	return r.continuum[int(uint8(xxhash.Sum64([]byte(key))))]
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
	atomic.AddInt64(&r.entryN, 1)
}

// remove deletes the entry for the given key.
// remove is safe for use by multiple goroutines.
func (r *ring) remove(key string) {
	r.getPartition(key).remove(key)
	if r.entryN > 0 {
		atomic.AddInt64(&r.entryN, -1)
	}
}

// keys returns all the keys from all partitions in the hash ring. The returned
// keys will be in order if sorted is true.
func (r *ring) keys(sorted bool) []string {
	keys := make([]string, 0, atomic.LoadInt64(&r.entryN))
	for _, p := range r.partitions {
		keys = append(keys, p.keys()...)
	}

	if sorted {
		sort.Strings(keys)
	}
	return keys
}

// apply applies the provided function to every entry in the ring under a read
// lock. The provided function will be called with each key and the
// corresponding entry. The first error encountered will be returned, if any.
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

type partition struct {
	mu    sync.RWMutex
	store map[string]*entry
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
		return e.add(values)
	}

	e, err := newEntryValues(values)
	if err != nil {
		return err
	}

	p.mu.Lock()
	p.store[key] = e
	p.mu.Unlock()
	return nil
}

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
	for k, _ := range p.store {
		keys = append(keys, k)
	}
	p.mu.RUnlock()
	return keys
}
