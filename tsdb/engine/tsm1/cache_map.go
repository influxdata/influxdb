package tsm1

import (
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	xxhash "github.com/OneOfOne/xxhash/native"
)

var (
	bucketIds uint64 = 256 // must be a power of 2
	seed      uint64
)

func init() {
	if s := os.Getenv("CACHE_MAP_BUCKETS"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil {
			panic(err.Error())
		}
		bucketIds = uint64(n)
	}
	println("CACHE_MAP_BUCKETS", bucketIds)
	seed = uint64(rand.Int63())
	println("CACHE MAP SEED", seed)
}

// CacheStoreMap holds a sharded map from string keys to *entry objects.
// It uses a hash function to map keys to its buckets. As much as possible, it
// is intended to be a drop-in replacement to the plain map[string]*entry
// which the Cache used to use.
//
// TODO(rw): In the future, this could shard on semantically-meaningful key
// data. For example: map[MeasurementName]map[CanonicalTags]map[FieldName]...
type CacheStoreMap struct {
	Buckets []*Bucket
}

// NewCacheStoreMap creates a new CacheStoreMap with the global number of
// buckets.
func NewCacheStoreMap() *CacheStoreMap {
	m := &CacheStoreMap{
		Buckets: make([]*Bucket, bucketIds),
	}
	for i := range m.Buckets {
		m.Buckets[i] = NewBucket()
	}
	return m
}

// BucketFor fetches the *Bucket that this key maps to.
func (m *CacheStoreMap) BucketFor(key string) *Bucket {
	bucketId := stringToBucketId(key)
	return m.Buckets[bucketId]
}

func (m *CacheStoreMap) EntryFor(key string, create bool) (*entry, bool) {
	bucket := m.BucketFor(key)

	// fast path: check if this bucket has this key:
	bucket.RLock()
	e, ok := bucket.data[key]
	bucket.RUnlock()

	if ok {
		return e, true
	}

	// return if the caller does not wish to create the *entry:
	if !create {
		return nil, false
	}

	// slow path: insert
	e2 := newEntry()
	bucket.Lock()
	if bucket.data == nil {
		bucket.Init(1)
	}
	e, ok = bucket.data[key]
	if !ok {
		e = e2
		bucket.data[key] = e
	}
	bucket.Unlock()

	return e, true
}

// Delete removes the given key (and its data).
func (m *CacheStoreMap) Delete(key string) {
	bucket := m.BucketFor(key)

	bucket.Lock()
	delete(bucket.data, key)
	bucket.Unlock()
}

// ParallelMergeInto merges all data from the source into the destination. The
// return value is the number of bytes (from `(models.Point).Size()`) of
// data added to the destination object. `nWorkers` controls the parallelism.
func (src *CacheStoreMap) ParallelMergeInto(dst *CacheStoreMap, nWorkers int) uint64 {
	if nWorkers < 1 {
		nWorkers = 1
	}

	var sz uint64
	mergeJobs := make(chan [2]*Bucket, nWorkers)

	// Because the hash seed is shared between CacheStoreMap instances,
	// bucketed data can be copied over without using the hash functions.
	// This allows embarassingly-parallel operation.

	// For each bucket, create a job that merges the source bucket into
	// the destination bucket. Process them with `nWorkers` parallelism:
	wg := &sync.WaitGroup{}
	for i := 0; i < nWorkers; i++ {
		wg.Add(1)
		go func() {
			for mergeJob := range mergeJobs {
				thisSz := mergeJob[0].MergeInto(mergeJob[1])
				atomic.AddUint64(&sz, thisSz)
			}
			wg.Done()
		}()
	}

	// For each bucket, if there is any work to do, create a merge job:
	for i := range src.Buckets {
		if src.Buckets[i].data == nil {
			// no source data for this bucket, skip it
			continue
		}

		srcBucket := src.Buckets[i]
		dstBucket := dst.Buckets[i]
		mergeJobs <- [2]*Bucket{srcBucket, dstBucket}
	}
	close(mergeJobs)
	wg.Wait()

	return sz
}

// Bucket holds a 'shard' of cache map data. It is a thread-safe
// map[string]*entry instance.
type Bucket struct {
	sync.RWMutex
	data map[string]*entry
}

// NewBucket creates a new Bucket instance.
func NewBucket() *Bucket {
	return &Bucket{}
}

// Init initializes a Bucket with the given size hint. Using this at insertion
// time prevents us from doing unnecessary work if we have jsut a few series.
func (b *Bucket) Init(sizeHint int) {
	b.data = make(map[string]*entry, sizeHint)
}

// MergeInto merges one bucket's data into another. It locks the *entry
// instances (but not the bucket itself). This is in keeping with dropping in
// the existing Cache logic. The return value is the number of bytes of
// memory used by the objects added to the destination bucket.
func (src *Bucket) MergeInto(dst *Bucket) uint64 {
	if len(src.data) == 0 {
		// nothing to do
		return 0
	}

	// initialize if needed (with size hint)
	if dst.data == nil {
		dst.Init(len(src.data))
	}

	var sz uint64
	for k, e := range src.data {
		dstEntry, ok := dst.data[k]
		if !ok {
			// add an empty entry in the destination, because
			// none exists:
			dstEntry = newEntry()
			dst.data[k] = dstEntry
		}

		// read-lock the entry and extract its data:
		e.mu.RLock()
		dstEntry.add(e.values)
		needSort := e.needSort
		sz += uint64(Values(e.values).Size())
		e.mu.RUnlock()

		if needSort {
			dstEntry.needSort = true
		}
	}
	return sz
}

// stringToBucketId computes the bucket id for the given string.
func stringToBucketId(key string) uint64 {
	return xxhash.ChecksumString64S(key, seed) & (bucketIds - 1)
}
