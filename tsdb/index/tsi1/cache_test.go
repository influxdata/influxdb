package tsi1

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/influxdata/influxdb/tsdb"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// This function is used to log the components of disk size when DiskSizeBytes fails
func (i *Index) LogDiskSize(t *testing.T) {
	fs, err := i.RetainFileSet()
	if err != nil {
		t.Log("could not retain fileset")
	}
	defer fs.Release()
	var size int64
	// Get MANIFEST sizes from each partition.
	for count, p := range i.partitions {
		sz := p.manifestSize
		t.Logf("Partition %d has size %d", count, sz)
		size += sz
	}
	for _, f := range fs.files {
		sz := f.Size()
		t.Logf("Size of file %s is %d", f.Path(), sz)
		size += sz
	}
	t.Logf("Total size is %d", size)
}

func TestTagValueSeriesIDCache(t *testing.T) {
	m0k0v0 := tsdb.NewSeriesIDSet(1, 2, 3, 4, 5)
	m0k0v1 := tsdb.NewSeriesIDSet(10, 20, 30, 40, 50)
	m0k1v2 := tsdb.NewSeriesIDSet()
	m1k3v0 := tsdb.NewSeriesIDSet(900, 0, 929)

	cache := TestCache{NewTagValueSeriesIDCache(10)}
	cache.Has(t, "m0", "k0", "v0", nil)

	// Putting something in the cache makes it retrievable.
	cache.PutByString("m0", "k0", "v0", m0k0v0)
	cache.Has(t, "m0", "k0", "v0", m0k0v0)

	// Putting something else under the same key will not replace the original item.
	cache.PutByString("m0", "k0", "v0", tsdb.NewSeriesIDSet(100, 200))
	cache.Has(t, "m0", "k0", "v0", m0k0v0)

	// Add another item to the cache.
	cache.PutByString("m0", "k0", "v1", m0k0v1)
	cache.Has(t, "m0", "k0", "v0", m0k0v0)
	cache.Has(t, "m0", "k0", "v1", m0k0v1)

	// Add some more items
	cache.PutByString("m0", "k1", "v2", m0k1v2)
	cache.PutByString("m1", "k3", "v0", m1k3v0)
	cache.Has(t, "m0", "k0", "v0", m0k0v0)
	cache.Has(t, "m0", "k0", "v1", m0k0v1)
	cache.Has(t, "m0", "k1", "v2", m0k1v2)
	cache.Has(t, "m1", "k3", "v0", m1k3v0)
}

func TestTagValueSeriesIDCache_eviction(t *testing.T) {
	m0k0v0 := tsdb.NewSeriesIDSet(1, 2, 3, 4, 5)
	m0k0v1 := tsdb.NewSeriesIDSet(10, 20, 30, 40, 50)
	m0k1v2 := tsdb.NewSeriesIDSet()
	m1k3v0 := tsdb.NewSeriesIDSet(900, 0, 929)

	cache := TestCache{NewTagValueSeriesIDCache(4)}
	cache.PutByString("m0", "k0", "v0", m0k0v0)
	cache.PutByString("m0", "k0", "v1", m0k0v1)
	cache.PutByString("m0", "k1", "v2", m0k1v2)
	cache.PutByString("m1", "k3", "v0", m1k3v0)
	cache.Has(t, "m0", "k0", "v0", m0k0v0)
	cache.Has(t, "m0", "k0", "v1", m0k0v1)
	cache.Has(t, "m0", "k1", "v2", m0k1v2)
	cache.Has(t, "m1", "k3", "v0", m1k3v0)

	// Putting another item in the cache will evict m0k0v0
	m2k0v0 := tsdb.NewSeriesIDSet(8, 8, 8)
	cache.PutByString("m2", "k0", "v0", m2k0v0)
	if got, exp := cache.evictor.Len(), 4; got != exp {
		t.Fatalf("cache size was %d, expected %d", got, exp)
	}
	cache.HasNot(t, "m0", "k0", "v0")
	cache.Has(t, "m0", "k0", "v1", m0k0v1)
	cache.Has(t, "m0", "k1", "v2", m0k1v2)
	cache.Has(t, "m1", "k3", "v0", m1k3v0)
	cache.Has(t, "m2", "k0", "v0", m2k0v0)

	// Putting another item in the cache will evict m0k0v1. That  will mean
	// there will be no values left under the tuple {m0, k0}
	if _, ok := cache.cache[string("m0")][string("k0")]; !ok {
		t.Fatalf("Map missing for key %q", "k0")
	}

	m2k0v1 := tsdb.NewSeriesIDSet(8, 8, 8)
	cache.PutByString("m2", "k0", "v1", m2k0v1)
	if got, exp := cache.evictor.Len(), 4; got != exp {
		t.Fatalf("cache size was %d, expected %d", got, exp)
	}
	cache.HasNot(t, "m0", "k0", "v0")
	cache.HasNot(t, "m0", "k0", "v1")
	cache.Has(t, "m0", "k1", "v2", m0k1v2)
	cache.Has(t, "m1", "k3", "v0", m1k3v0)
	cache.Has(t, "m2", "k0", "v0", m2k0v0)
	cache.Has(t, "m2", "k0", "v1", m2k0v1)

	// Further, the map for all tag values for the tuple {m0, k0} should be removed.
	if _, ok := cache.cache[string("m0")][string("k0")]; ok {
		t.Fatalf("Map present for key %q, should be removed", "k0")
	}

	// Putting another item in the cache will evict m0k1v2. That  will mean
	// there will be no values left under the tuple {m0}
	if _, ok := cache.cache[string("m0")]; !ok {
		t.Fatalf("Map missing for key %q", "k0")
	}
	m2k0v2 := tsdb.NewSeriesIDSet(8, 9, 9)
	cache.PutByString("m2", "k0", "v2", m2k0v2)
	cache.HasNot(t, "m0", "k0", "v0")
	cache.HasNot(t, "m0", "k0", "v1")
	cache.HasNot(t, "m0", "k1", "v2")
	cache.Has(t, "m1", "k3", "v0", m1k3v0)
	cache.Has(t, "m2", "k0", "v0", m2k0v0)
	cache.Has(t, "m2", "k0", "v1", m2k0v1)
	cache.Has(t, "m2", "k0", "v2", m2k0v2)

	// The map for all tag values for the tuple {m0} should be removed.
	if _, ok := cache.cache[string("m0")]; ok {
		t.Fatalf("Map present for key %q, should be removed", "k0")
	}

	// Putting another item in the cache will evict m2k0v0 if we first get m1k3v0
	// because m2k0v0 will have been used less recently...
	m3k0v0 := tsdb.NewSeriesIDSet(1000)
	cache.Has(t, "m1", "k3", "v0", m1k3v0) // This makes it the most recently used rather than the least.
	cache.PutByString("m3", "k0", "v0", m3k0v0)

	cache.HasNot(t, "m0", "k0", "v0")
	cache.HasNot(t, "m0", "k0", "v1")
	cache.HasNot(t, "m0", "k1", "v2")
	cache.HasNot(t, "m2", "k0", "v0") // This got pushed to the back.

	cache.Has(t, "m1", "k3", "v0", m1k3v0) // This got saved because we looked at it before we added to the cache
	cache.Has(t, "m2", "k0", "v1", m2k0v1)
	cache.Has(t, "m2", "k0", "v2", m2k0v2)
	cache.Has(t, "m3", "k0", "v0", m3k0v0)
}

func TestTagValueSeriesIDCache_addToSet(t *testing.T) {
	cache := TestCache{NewTagValueSeriesIDCache(4)}
	cache.PutByString("m0", "k0", "v0", nil) // Puts a nil set in the cache.
	s2 := tsdb.NewSeriesIDSet(100)
	cache.PutByString("m0", "k0", "v1", s2)
	cache.Has(t, "m0", "k0", "v0", nil)
	cache.Has(t, "m0", "k0", "v1", s2)

	cache.addToSet([]byte("m0"), []byte("k0"), []byte("v0"), 20)  // No non-nil set exists so one will be created
	cache.addToSet([]byte("m0"), []byte("k0"), []byte("v1"), 101) // No non-nil set exists so one will be created
	cache.Has(t, "m0", "k0", "v1", tsdb.NewSeriesIDSet(100, 101))

	ss := cache.GetByString("m0", "k0", "v0")
	if !tsdb.NewSeriesIDSet(20).Equals(ss) {
		t.Fatalf("series id set was %v", ss)
	}

}

func TestTagValueSeriesIDCache_ConcurrentGetPut(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long test")
	}

	a := []string{"a", "b", "c", "d", "e"}
	rnd := func() []byte {
		return []byte(a[rand.Intn(len(a)-1)])
	}

	cache := TestCache{NewTagValueSeriesIDCache(100)}
	done := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				cache.Put(rnd(), rnd(), rnd(), tsdb.NewSeriesIDSet())
			}
		}()
	}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				_ = cache.Get(rnd(), rnd(), rnd())
			}
		}()
	}

	time.Sleep(10 * time.Second)
	close(done)
	wg.Wait()
}

type TestCache struct {
	*TagValueSeriesIDCache
}

func (c TestCache) Has(t *testing.T, name, key, value string, ss *tsdb.SeriesIDSet) {
	if got, exp := c.Get([]byte(name), []byte(key), []byte(value)), ss; !got.Equals(exp) {
		t.Helper()
		t.Fatalf("got set %v, expected %v", got, exp)
	}
}

func (c TestCache) HasNot(t *testing.T, name, key, value string) {
	if got := c.Get([]byte(name), []byte(key), []byte(value)); got != nil {
		t.Helper()
		t.Fatalf("got non-nil set %v for {%q, %q, %q}", got, name, key, value)
	}
}

func (c TestCache) GetByString(name, key, value string) *tsdb.SeriesIDSet {
	return c.Get([]byte(name), []byte(key), []byte(value))
}

func (c TestCache) PutByString(name, key, value string, ss *tsdb.SeriesIDSet) {
	c.Put([]byte(name), []byte(key), []byte(value), ss)
}

// TestTagValueSeriesIDCache_AtomicFieldAlignment guards the struct layout:
// fields accessed with sync/atomic (capacity and the stats counters) must be
// 64-bit aligned, or atomic ops panic on 32-bit platforms. The allocation
// base is guaranteed 8-byte aligned, so it suffices that these fields sit at
// 8-byte-multiple offsets. The leading layout has no sub-8-byte fields, so
// the offsets observed here (on any host) match those on 32-bit.
func TestTagValueSeriesIDCache_AtomicFieldAlignment(t *testing.T) {
	var c TagValueSeriesIDCache
	require.Zero(t, unsafe.Offsetof(c.capacity)%8, "capacity must be 8-byte aligned")
	require.Zero(t, unsafe.Offsetof(c.stats)%8, "stats counters must be 8-byte aligned")
}

func TestTagValueSeriesIDCache_Statistics(t *testing.T) {
	statValues := func(cache *TagValueSeriesIDCache) map[string]interface{} {
		stats := cache.Statistics(nil)
		require.Len(t, stats, 1)
		require.Equal(t, statTagValueCacheMeasurement, stats[0].Name)
		return stats[0].Values
	}

	cache := NewTagValueSeriesIDCache(2)

	// Initial state: all counters zero.
	require.Equal(t, map[string]interface{}{
		statTagValueCacheHit:            int64(0),
		statTagValueCacheMiss:           int64(0),
		statTagValueCacheEviction:       int64(0),
		statTagValueCacheShrinkEviction: int64(0),
		statTagValueCacheSize:           int64(0),
		statTagValueCacheCapacity:       int64(2),
	}, statValues(cache))

	// Miss on absent key.
	require.Nil(t, cache.Get([]byte("m0"), []byte("k0"), []byte("v0")))
	require.Equal(t, map[string]interface{}{
		statTagValueCacheHit:            int64(0),
		statTagValueCacheMiss:           int64(1),
		statTagValueCacheEviction:       int64(0),
		statTagValueCacheShrinkEviction: int64(0),
		statTagValueCacheSize:           int64(0),
		statTagValueCacheCapacity:       int64(2),
	}, statValues(cache))

	// Put, then Get the same key → one hit, size 1.
	s0 := tsdb.NewSeriesIDSet(1)
	cache.Put([]byte("m0"), []byte("k0"), []byte("v0"), s0)
	require.True(t, cache.Get([]byte("m0"), []byte("k0"), []byte("v0")).Equals(s0))
	require.Equal(t, map[string]interface{}{
		statTagValueCacheHit:            int64(1),
		statTagValueCacheMiss:           int64(1),
		statTagValueCacheEviction:       int64(0),
		statTagValueCacheShrinkEviction: int64(0),
		statTagValueCacheSize:           int64(1),
		statTagValueCacheCapacity:       int64(2),
	}, statValues(cache))

	// Add a second entry to fill the cache.
	s1 := tsdb.NewSeriesIDSet(2)
	cache.Put([]byte("m0"), []byte("k0"), []byte("v1"), s1)
	require.Equal(t, int64(2), statValues(cache)[statTagValueCacheSize])
	require.Equal(t, int64(0), statValues(cache)[statTagValueCacheEviction])
	require.Equal(t, int64(0), statValues(cache)[statTagValueCacheShrinkEviction])

	// Adding a third distinct entry must evict the least-recently-used.
	// LRU at this point is v0: it was Put first, Get-promoted to MRU, then
	// v1 was Put (making v1 MRU and v0 LRU). The eviction is a forced
	// eviction (write pressure on a full cache), so it lands in Evictions
	// rather than ShrinkEvictions.
	s2 := tsdb.NewSeriesIDSet(3)
	cache.Put([]byte("m0"), []byte("k0"), []byte("v2"), s2)
	require.Equal(t, map[string]interface{}{
		statTagValueCacheHit:            int64(1),
		statTagValueCacheMiss:           int64(1),
		statTagValueCacheEviction:       int64(1),
		statTagValueCacheShrinkEviction: int64(0),
		statTagValueCacheSize:           int64(2),
		statTagValueCacheCapacity:       int64(2),
	}, statValues(cache))
	// v0 was evicted; v1 and v2 must survive.
	got0, _ := cache.get([]byte("m0"), []byte("k0"), []byte("v0"))
	require.Nil(t, got0)
	got1, _ := cache.get([]byte("m0"), []byte("k0"), []byte("v1"))
	require.True(t, got1.Equals(s1))
	got2, _ := cache.get([]byte("m0"), []byte("k0"), []byte("v2"))
	require.True(t, got2.Equals(s2))
}

func TestTagValueSeriesIDCache_Statistics_EvictsTrueLRU(t *testing.T) {
	// Verifies that a Get on an existing entry promotes it to MRU, so a
	// subsequent insertion evicts the previously-second-oldest entry rather
	// than the just-touched one.
	cache := NewTagValueSeriesIDCache(2)

	s0 := tsdb.NewSeriesIDSet(1)
	s1 := tsdb.NewSeriesIDSet(2)
	s2 := tsdb.NewSeriesIDSet(3)

	cache.Put([]byte("m"), []byte("k"), []byte("v0"), s0)
	cache.Put([]byte("m"), []byte("k"), []byte("v1"), s1)

	// Touch v0 so it becomes MRU; v1 is now LRU.
	require.True(t, cache.Get([]byte("m"), []byte("k"), []byte("v0")).Equals(s0))

	// Inserting v2 must evict v1, not v0.
	cache.Put([]byte("m"), []byte("k"), []byte("v2"), s2)

	got0, _ := cache.get([]byte("m"), []byte("k"), []byte("v0"))
	require.True(t, got0.Equals(s0), "recently-touched key v0 should not have been evicted")
	got1, _ := cache.get([]byte("m"), []byte("k"), []byte("v1"))
	require.Nil(t, got1, "true LRU key v1 should have been evicted")
	got2, _ := cache.get([]byte("m"), []byte("k"), []byte("v2"))
	require.True(t, got2.Equals(s2), "newly inserted key v2 should be present")

	stats := cache.Statistics(nil)
	require.Equal(t, int64(1), stats[0].Values[statTagValueCacheEviction])
	require.Equal(t, int64(2), stats[0].Values[statTagValueCacheSize])
}

func TestTagValueSeriesIDCache_Statistics_Tags(t *testing.T) {
	cache := NewTagValueSeriesIDCache(1)
	tags := map[string]string{"database": "db0", "id": "42"}
	stats := cache.Statistics(tags)
	require.Len(t, stats, 1)
	require.Equal(t, tags, stats[0].Tags)
}

func TestDecideResize_PolicyTable(t *testing.T) {
	const (
		initialCap = int64(100)
		maxCap     = int64(800)
		minSamples = int64(100)
		target     = 0.95
	)
	// Counter offsets to simulate a fresh window for each row: each
	// row chooses (hitsW, missesW); we feed hits = hitsW, misses =
	// missesW with lastHits = lastMisses = 0.
	tests := []struct {
		name              string
		hits, misses      int64
		capacity, maxCap  int64
		minSamples        int64
		target            float64
		wantNewCap        int64
		wantGrow          bool
		wantGetsApprox    int64   // sanity check on the gets return
		wantRateBelowOnly float64 // 0 means don't check; otherwise rate must be < this
	}{
		{
			name: "below target with adequate samples grows (doubles)",
			hits: 800, misses: 200, capacity: initialCap, maxCap: maxCap,
			minSamples: minSamples, target: target,
			wantNewCap: initialCap * 2, wantGrow: true, wantGetsApprox: 1000,
			wantRateBelowOnly: target,
		},
		{
			name: "below target with adequate samples but at max does not grow",
			hits: 800, misses: 200, capacity: maxCap, maxCap: maxCap,
			minSamples: minSamples, target: target,
			wantNewCap: maxCap, wantGrow: false,
		},
		{
			name: "above target does not grow",
			hits: 1000, misses: 0, capacity: initialCap, maxCap: maxCap,
			minSamples: minSamples, target: target,
			wantNewCap: initialCap, wantGrow: false,
		},
		{
			name: "at target does not grow",
			hits: 950, misses: 50, capacity: initialCap, maxCap: maxCap,
			minSamples: minSamples, target: target,
			wantNewCap: initialCap, wantGrow: false,
		},
		{
			name: "below floor (any rate) does not grow",
			hits: 5, misses: 50, capacity: initialCap, maxCap: maxCap,
			minSamples: minSamples, target: target,
			wantNewCap: initialCap, wantGrow: false,
		},
		{
			name: "pure-write zero-reads window does not grow (no divide by zero)",
			hits: 0, misses: 0, capacity: initialCap, maxCap: maxCap,
			minSamples: minSamples, target: target,
			wantNewCap: initialCap, wantGrow: false,
		},
		{
			// Regression: with minSamples == 0 the gets<minSamples floor does
			// not fire on a pure-write window, so the policy must independently
			// gate gets == 0 — otherwise rate stays 0.0 and we grow with no
			// evidence.
			name: "pure-write zero-reads with minSamples=0 does not grow",
			hits: 0, misses: 0, capacity: initialCap, maxCap: maxCap,
			minSamples: 0, target: target,
			wantNewCap: initialCap, wantGrow: false,
		},
		{
			name: "grow capped at max when doubling would overshoot",
			hits: 100, misses: 900, capacity: 600, maxCap: maxCap,
			minSamples: minSamples, target: target,
			wantNewCap: maxCap, wantGrow: true,
		},
		{
			name: "tiny window at floor grows correctly when rate is below target",
			hits: 50, misses: 50, capacity: initialCap, maxCap: maxCap,
			minSamples: 100, target: target,
			wantNewCap: initialCap * 2, wantGrow: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newCap, gets, rate, grow := decideResize(
				tt.hits, tt.misses, 0, 0,
				tt.capacity, tt.maxCap, tt.minSamples, tt.target,
			)
			require.Equal(t, tt.wantGrow, grow, "grow")
			require.Equal(t, tt.wantNewCap, newCap, "newCap")
			if tt.wantGetsApprox > 0 {
				require.Equal(t, tt.wantGetsApprox, gets, "gets")
			}
			if tt.wantRateBelowOnly > 0 {
				require.Less(t, rate, tt.wantRateBelowOnly, "observed rate")
			}
		})
	}
}

func TestTagValueSeriesIDCache_AdaptiveGrowth_TriggersOnTurnover(t *testing.T) {
	// Tiny initial/max so the test runs in a handful of operations.
	// minSamples is also small so the floor is cleared by the test
	// traffic. We Get-then-Put each absent key, so every Put is
	// preceded by one miss; every Put past the cache's current size
	// causes one eviction.
	logger := zap.NewNop()
	cache := NewAdaptiveTagValueSeriesIDCache(2, 16, 0.99, tsdb.DefaultSeriesIDSetCacheShrinkConservatism, 4, logger)

	require.Equal(t, int64(2), atomic.LoadInt64(&cache.capacity))

	insert := func(seq int) {
		v := []byte{byte(seq)}
		require.Nil(t, cache.Get([]byte("m"), []byte("k"), v))
		cache.Put([]byte("m"), []byte("k"), v, tsdb.NewSeriesIDSet(uint64(seq)))
	}

	// Trace:
	//   insert 1: size 0→1
	//   insert 2: size 1→2 (cache full at cap=2)
	//   insert 3: size 2→2 (1 eviction)
	//   insert 4: size 2→2 (2 evictions → fires → cap doubles to 4)
	for i := 1; i <= 4; i++ {
		insert(i)
	}
	require.Equal(t, int64(4), atomic.LoadInt64(&cache.capacity), "after first turnover, capacity=4")

	// After grow to 4, two free slots. Need to refill before evicting:
	//   insert 5: size 2→3
	//   insert 6: size 3→4 (cache full at cap=4)
	//   insert 7..10: each evicts once. 4 evictions → fires → cap=8
	for i := 5; i <= 10; i++ {
		insert(i)
	}
	require.Equal(t, int64(8), atomic.LoadInt64(&cache.capacity), "after second turnover, capacity=8")

	// After grow to 8, four free slots. Refill then 8 evictions:
	//   insert 11..14: fill to size=8
	//   insert 15..22: 8 evictions → fires → cap=16 (= max)
	for i := 11; i <= 22; i++ {
		insert(i)
	}
	require.Equal(t, int64(16), atomic.LoadInt64(&cache.capacity), "after third turnover, capacity=16 (= max)")

	// Further evictions do not grow past max.
	// After grow to 16, eight free slots. Fill them, then drive enough
	// evictions to trigger another firing — which must not grow.
	for i := 23; i <= 50; i++ {
		insert(i)
	}
	require.Equal(t, int64(16), atomic.LoadInt64(&cache.capacity), "capacity stays at max")
}

func TestTagValueSeriesIDCache_AdaptiveGrowth_NoOpAtTarget(t *testing.T) {
	// Target so low that any nonzero hit rate satisfies it. We drive
	// evictions while also generating hits, so the windowed hit rate
	// stays comfortably above target and capacity must not grow.
	logger := zap.NewNop()
	cache := NewAdaptiveTagValueSeriesIDCache(2, 16, 0.01, tsdb.DefaultSeriesIDSetCacheShrinkConservatism, 4, logger)

	insertedSeqs := []int{}
	insert := func(seq int) {
		insertedSeqs = append(insertedSeqs, seq)
		v := []byte{byte(seq)}
		cache.Put([]byte("m"), []byte("k"), v, tsdb.NewSeriesIDSet(uint64(seq)))
	}
	hitOnSurvivors := func() {
		// Issue a Get on each key currently in the cache to bank hits.
		// Use the most-recent fixed-size window of recent inserts.
		start := len(insertedSeqs) - 2
		if start < 0 {
			start = 0
		}
		for _, seq := range insertedSeqs[start:] {
			cache.Get([]byte("m"), []byte("k"), []byte{byte(seq)})
		}
	}

	// Fill cache.
	for i := 1; i <= 2; i++ {
		insert(i)
	}
	// Force enough evictions to reach the trigger, but bank hits
	// between each Put so the window's hit rate is >= target=0.01.
	for i := 3; i <= 20; i++ {
		hitOnSurvivors()
		hitOnSurvivors()
		insert(i)
	}
	require.Equal(t, int64(2), atomic.LoadInt64(&cache.capacity), "capacity must stay at 2 when target is met")
}

func TestTagValueSeriesIDCache_AdaptiveDisabled_NoLogNoGrowth(t *testing.T) {
	core, logs := observer.New(zap.DebugLevel)
	logger := zap.New(core)

	// Legacy constructor → adaptive sizing disabled. We splice in the
	// observer logger so any unexpected log call is detected.
	cache := NewTagValueSeriesIDCache(2)
	cache.SetLogger(logger)

	for i := 1; i <= 30; i++ {
		v := []byte{byte(i)}
		cache.Put([]byte("m"), []byte("k"), v, tsdb.NewSeriesIDSet(uint64(i)))
	}

	require.Equal(t, int64(2), atomic.LoadInt64(&cache.capacity), "capacity must not change when adaptive sizing is disabled")
	require.Equal(t, 0, logs.Len(), "no log lines must be emitted when adaptive sizing is disabled")
}

// TestIndex_WithLogger_PropagatesToAdaptiveCache verifies that WithLogger,
// called after the adaptive cache has already been constructed in NewIndex
// (with the index's initial no-op logger), re-points the cache at the real
// logger so resize events are actually emitted. Regression test: previously
// WithLogger only updated i.logger and the cache kept its no-op logger.
func TestIndex_WithLogger_PropagatesToAdaptiveCache(t *testing.T) {
	const minSamples = tsdb.DefaultAdaptiveCacheMinSamples

	idx := NewIndex(nil, "db0",
		WithSeriesIDCacheSize(2),
		WithSeriesIDCacheMaxSize(16),
		WithSeriesIDCacheTargetHitRate(0.99),
	)

	core, logs := observer.New(zap.InfoLevel)
	idx.WithLogger(zap.New(core))

	cache := idx.tagValueCache

	// Fill the cache to capacity (2).
	cache.Put([]byte("m"), []byte("k"), []byte{0}, tsdb.NewSeriesIDSet(0))
	cache.Put([]byte("m"), []byte("k"), []byte{1}, tsdb.NewSeriesIDSet(1))

	// Bank enough misses to clear the per-window sample floor without
	// triggering evictions (Get never evicts).
	for i := 0; i < minSamples; i++ {
		require.Nil(t, cache.Get([]byte("m"), []byte("absent"), []byte{byte(i)}))
	}

	// Two more inserts of new keys cause two evictions; the second is the
	// `capacity`-th eviction, firing the policy. The window hit rate is
	// 0 (< 0.99 target) over >= minSamples gets, so the cache must grow.
	cache.Put([]byte("m"), []byte("k"), []byte{2}, tsdb.NewSeriesIDSet(2))
	cache.Put([]byte("m"), []byte("k"), []byte{3}, tsdb.NewSeriesIDSet(3))

	require.Equal(t, int64(4), atomic.LoadInt64(&cache.capacity), "cache should have grown after policy fired")
	require.Equal(t, 1, logs.Len(), "resize event must be emitted to the logger propagated by WithLogger")
	require.Equal(t, logMsgCacheCapacityIncreased, logs.All()[0].Message)
}

func TestAdaptiveWindowLen(t *testing.T) {
	tests := []struct {
		name         string
		n, minWindow int64
		target       float64
		want         int64
	}{
		{name: "typical 100 @ 0.95 ≈ 3n", n: 100, minWindow: 1, target: 0.95, want: 299},
		{name: "typical 1000 @ 0.95 ≈ 3n", n: 1000, minWindow: 1, target: 0.95, want: 2995},
		// At a low target the computed window (~0.69n) is below n; the n floor
		// raises it so we always sample at least as many times as there are items.
		{name: "n floor binds at low target", n: 100, minWindow: 1, target: 0.5, want: 100},
		{name: "minWindow floor binds for tiny cache", n: 2, minWindow: 100, target: 0.95, want: 100},
		{name: "n=1 too small", n: 1, minWindow: 100, target: 0.95, want: 0},
		{name: "n=0 too small", n: 0, minWindow: 100, target: 0.95, want: 0},
		// 1.0 - SmallestNonzeroFloat64 rounds to exactly 1.0; config rejects this
		// target, but the helper must not overflow — it returns the MaxInt64
		// sentinel ("effectively never").
		{name: "degenerate target rounding to 1 returns sentinel", n: 100, minWindow: 1, target: 1.0 - math.SmallestNonzeroFloat64, want: math.MaxInt64},
		// Out-of-range targets are rejected by the argument guard before the
		// logarithms run, returning the sentinel rather than NaN/±Inf/overflow.
		{name: "NaN target returns sentinel", n: 100, minWindow: 1, target: math.NaN(), want: math.MaxInt64},
		{name: "target above 1 returns sentinel", n: 100, minWindow: 1, target: 1.5, want: math.MaxInt64},
		{name: "zero target returns sentinel", n: 100, minWindow: 1, target: 0, want: math.MaxInt64},
		{name: "negative target returns sentinel", n: 100, minWindow: 1, target: -0.1, want: math.MaxInt64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, adaptiveWindowLen(tt.n, tt.minWindow, tt.target))
		})
	}
}

func TestNewAdaptiveTagValueSeriesIDCache_RejectsInvalidArguments(t *testing.T) {
	// NaN and out-of-range targets must panic; a NaN target would otherwise make
	// the grow/shrink rate comparisons behave unpredictably.
	for _, target := range []float64{math.NaN(), 0, 1, -0.1, 1.5} {
		require.Panics(t, func() {
			NewAdaptiveTagValueSeriesIDCache(2, 16, target, tsdb.DefaultSeriesIDSetCacheShrinkConservatism, 4, zap.NewNop())
		}, "target %v must be rejected", target)
	}
	// NaN, ±Inf and negative conservatism values must panic; the valid range is
	// [0, +Inf), so 0 and small positive values are accepted (covered separately).
	for _, c := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), -1, -0.0001} {
		require.Panics(t, func() {
			NewAdaptiveTagValueSeriesIDCache(2, 16, 0.95, c, 4, zap.NewNop())
		}, "conservatism %v must be rejected", c)
	}
	// Conservatism boundary values (0 = median admit; small positive) must NOT panic.
	for _, c := range []float64{0, 0.5, 1, 2.0, 10} {
		require.NotPanics(t, func() {
			NewAdaptiveTagValueSeriesIDCache(2, 16, 0.95, c, 4, zap.NewNop())
		}, "conservatism %v must be accepted", c)
	}
}

func TestDecideShrink_PolicyTable(t *testing.T) {
	const (
		target       = 0.95
		conservatism = tsdb.DefaultSeriesIDSetCacheShrinkConservatism
		// The eviction-gate rows below all use this window length (hitsW+missesW).
		rowGets = int64(1000)
	)
	// Derive the eviction-gate threshold from the same helper production uses,
	// so changing the conservatism constant doesn't require rewriting hardcoded
	// numbers. Eviction-gate test rows below use evictionsW relative to this.
	limit := atTargetEvictionGateLimit(rowGets, target, conservatism)
	tests := []struct {
		name                             string
		hitsW, missesW, evictionsW       int64
		capacity, size, warmCount, floor int64
		wantNewCap, wantEvict            int64
		wantShrink                       bool
	}{
		{
			name:  "no reads does not shrink",
			hitsW: 0, missesW: 0, evictionsW: 0,
			capacity: 100, size: 100, warmCount: 0, floor: 10,
			wantNewCap: 100, wantEvict: 0, wantShrink: false,
		},
		{
			// Evictions one above the gate limit must block shrink.
			name:  "evictions above gate limit blocks shrink",
			hitsW: rowGets, missesW: 0, evictionsW: limit + 1,
			capacity: 100, size: 100, warmCount: 20, floor: 10,
			wantNewCap: 100, wantEvict: 0, wantShrink: false,
		},
		{
			// Evictions exactly at the gate limit must NOT block shrink: the
			// cache is performing at/above the at-target − z·σ bound and has a
			// cold tail to shed.
			name:  "evictions at the gate limit passes through to shrink",
			hitsW: rowGets, missesW: 0, evictionsW: limit,
			capacity: 100, size: 100, warmCount: 20, floor: 10,
			wantNewCap: 50, wantEvict: 50, wantShrink: true,
		},
		{
			name:  "hit rate below target does not shrink",
			hitsW: 50, missesW: 50, evictionsW: 0,
			capacity: 100, size: 100, warmCount: 20, floor: 10,
			wantNewCap: 100, wantEvict: 0, wantShrink: false,
		},
		{
			name:  "slack: capacity above occupancy trims to size, no eviction",
			hitsW: 1000, missesW: 0, evictionsW: 0,
			capacity: 200, size: 100, warmCount: 80, floor: 50,
			wantNewCap: 100, wantEvict: 0, wantShrink: true,
		},
		{
			name:  "slack clamped to floor",
			hitsW: 1000, missesW: 0, evictionsW: 0,
			capacity: 200, size: 30, warmCount: 10, floor: 50,
			wantNewCap: 50, wantEvict: 0, wantShrink: true,
		},
		{
			name:  "slack no-op when size already at/below floor and capacity==floor",
			hitsW: 1000, missesW: 0, evictionsW: 0,
			capacity: 50, size: 30, warmCount: 10, floor: 50,
			wantNewCap: 50, wantEvict: 0, wantShrink: false,
		},
		{
			name:  "cold-tail bounded to half the cache",
			hitsW: 1000, missesW: 0, evictionsW: 0,
			capacity: 100, size: 100, warmCount: 20, floor: 10,
			wantNewCap: 50, wantEvict: 50, wantShrink: true,
		},
		{
			// Cold tail of 4900 (and size/2 of 2500) both exceed the per-event
			// cap, so the absolute bound is the binding one. Decay continues over
			// later windows (covered by TestTagValueSeriesIDCache_ShrinkRepeatsWhenCapped).
			name:  "cold-tail bounded by maxShrinkEvictPerEvent",
			hitsW: 1000, missesW: 0, evictionsW: 0,
			capacity: 5000, size: 5000, warmCount: 100, floor: 10,
			wantNewCap: 5000 - maxShrinkEvictPerEvent, wantEvict: maxShrinkEvictPerEvent, wantShrink: true,
		},
		{
			name:  "cold-tail sheds exactly the cold tail when under half",
			hitsW: 1000, missesW: 0, evictionsW: 0,
			capacity: 100, size: 100, warmCount: 60, floor: 10,
			wantNewCap: 60, wantEvict: 40, wantShrink: true,
		},
		{
			name:  "cold-tail clamped at floor",
			hitsW: 1000, missesW: 0, evictionsW: 0,
			capacity: 100, size: 100, warmCount: 5, floor: 30,
			wantNewCap: 50, wantEvict: 50, wantShrink: true,
		},
		{
			name:  "cold-tail no-op when everything was touched",
			hitsW: 1000, missesW: 0, evictionsW: 0,
			capacity: 100, size: 100, warmCount: 100, floor: 10,
			wantNewCap: 100, wantEvict: 0, wantShrink: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newCap, evict, shrink := decideShrink(
				tt.hitsW, tt.missesW, tt.evictionsW,
				tt.capacity, tt.size, tt.warmCount, tt.floor, target, conservatism,
			)
			require.Equal(t, tt.wantShrink, shrink, "shrink")
			require.Equal(t, tt.wantNewCap, newCap, "newCap")
			require.Equal(t, tt.wantEvict, evict, "evict")
		})
	}
}

// newFullAdaptiveCache returns an adaptive cache forced to the given capacity
// and filled with that many entries (values 0..capacity-1). The floor is 2, so
// shrink has room to act. Adaptive growth is not exercised (no evictions occur
// during setup), so the forced capacity stands in for a previously-grown cache.
func newFullAdaptiveCache(t *testing.T, capacity, minSamples int, target float64) *TagValueSeriesIDCache {
	t.Helper()
	c := NewAdaptiveTagValueSeriesIDCache(2, 1024, target, tsdb.DefaultSeriesIDSetCacheShrinkConservatism, minSamples, zap.NewNop())
	atomic.StoreInt64(&c.capacity, int64(capacity))
	for i := 0; i < capacity; i++ {
		c.Put([]byte("m"), []byte("k"), []byte{byte(i)}, tsdb.NewSeriesIDSet(uint64(i)))
	}
	require.Equal(t, int64(capacity), atomic.LoadInt64(&c.stats.Size), "setup occupancy")
	return c
}

func getVal(c *TagValueSeriesIDCache, v int) *tsdb.SeriesIDSet {
	return c.Get([]byte("m"), []byte("k"), []byte{byte(v)})
}

func existsVal(c *TagValueSeriesIDCache, v int) bool {
	c.Lock()
	defer c.Unlock()
	return c.exists("m", "k", string([]byte{byte(v)}))
}

func TestTagValueSeriesIDCache_ShrinkColdTail(t *testing.T) {
	// Full cache of 10; touch only {0,1,2} for one window. The window completes;
	// with a 100% hit rate and zero evictions, capacity trims toward the warm
	// footprint, bounded to half the cache per event. Verifies the RIGHT (deepest,
	// untouched) entries are shed and the warm set + recently-inserted cold
	// entries survive.
	cache := newFullAdaptiveCache(t, 10, 8, 0.5)

	w := adaptiveWindowLen(10, 8, 0.5)
	for i := int64(0); i < w; i++ {
		require.NotNil(t, getVal(cache, int(i%3)))
	}

	require.Equal(t, int64(5), atomic.LoadInt64(&cache.capacity), "capacity = size - min(coldTail, size/2) = 10-5")
	require.Equal(t, int64(5), atomic.LoadInt64(&cache.stats.Size))

	// Warm {0,1,2} survive; the deepest untouched {3,4,5,6,7} are evicted;
	// the most-recently-inserted cold {8,9} survive (they are above the LRU tail).
	for _, v := range []int{0, 1, 2, 8, 9} {
		require.True(t, existsVal(cache, v), "expected value %d to survive", v)
	}
	for _, v := range []int{3, 4, 5, 6, 7} {
		require.False(t, existsVal(cache, v), "expected value %d to be evicted", v)
	}
}

func TestTagValueSeriesIDCache_ShrinkSlack(t *testing.T) {
	// Capacity 10 but only 4 entries (slack). A quiet, all-hit window trims
	// capacity down to the occupancy with no eviction.
	cache := NewAdaptiveTagValueSeriesIDCache(2, 1024, 0.5, tsdb.DefaultSeriesIDSetCacheShrinkConservatism, 8, zap.NewNop())
	atomic.StoreInt64(&cache.capacity, 10)
	for i := 0; i < 4; i++ {
		cache.Put([]byte("m"), []byte("k"), []byte{byte(i)}, tsdb.NewSeriesIDSet(uint64(i)))
	}

	w := adaptiveWindowLen(4, 8, 0.5)
	for i := int64(0); i < w; i++ {
		require.NotNil(t, getVal(cache, int(i%4)))
	}

	require.Equal(t, int64(4), atomic.LoadInt64(&cache.capacity), "slack branch trims capacity to occupancy")
	require.Equal(t, int64(4), atomic.LoadInt64(&cache.stats.Size), "no eviction in the slack branch")
	for v := 0; v < 4; v++ {
		require.True(t, existsVal(cache, v), "value %d must survive a slack trim", v)
	}
}

func TestTagValueSeriesIDCache_NoShrinkWhenAllTouched(t *testing.T) {
	// Full cache; touch every entry within the window. warmCount == size, so
	// there is no cold tail and capacity is unchanged.
	cache := newFullAdaptiveCache(t, 10, 16, 0.5) // window >= 16, enough to touch all 10

	w := adaptiveWindowLen(10, 16, 0.5)
	for i := int64(0); i < w; i++ {
		require.NotNil(t, getVal(cache, int(i%10)))
	}

	require.Equal(t, int64(10), atomic.LoadInt64(&cache.capacity), "capacity must not shrink when the whole cache is in use")
	require.Equal(t, int64(10), atomic.LoadInt64(&cache.stats.Size))
}

// TestTagValueSeriesIDCache_ShrinkRepeatsWhenCapped verifies that when the
// unbounded cold-tail shed would exceed maxShrinkEvictPerEvent, a single event
// sheds exactly the cap and subsequent windows continue the decay — the claim
// made by decideColdTail's doc comment.
//
// Cache size 3074 with a warm set of 10 and floor 2:
//   - Window 1: unbounded shed 3064 → size/2 bound 1537 → cap 1024. New cap 2050.
//   - Window 2: unbounded shed 2040 → size/2 bound 1025 → cap 1024. New cap 1026.
//
// The post-shrink cooldown is adaptiveWindowLen(newCap, samples, target), which
// matches the next window's length, so the cooldown elapses exactly when window 2
// ends — no manual cooldown reset is needed (and the natural cycle is itself
// useful coverage).
func TestTagValueSeriesIDCache_ShrinkRepeatsWhenCapped(t *testing.T) {
	const (
		target  = 0.5
		size    = 3074
		warm    = 10
		samples = 8
	)
	cache := NewAdaptiveTagValueSeriesIDCache(2, 4096, target, tsdb.DefaultSeriesIDSetCacheShrinkConservatism, samples, zap.NewNop())
	atomic.StoreInt64(&cache.capacity, size)

	// 2-byte values give a unique key per i in [0, 65536); the single-byte
	// values used by newFullAdaptiveCache would collide past 255.
	keys := make([][]byte, size)
	for i := 0; i < size; i++ {
		keys[i] = []byte{byte(i >> 8), byte(i & 0xFF)}
		cache.Put([]byte("m"), []byte("k"), keys[i], tsdb.NewSeriesIDSet(uint64(i)))
	}
	require.Equal(t, int64(size), atomic.LoadInt64(&cache.stats.Size), "setup occupancy")

	drive := func(w int64) {
		for i := int64(0); i < w; i++ {
			require.NotNil(t, cache.Get([]byte("m"), []byte("k"), keys[int(i)%warm]))
		}
	}

	// Window 1: cap clamps the first shrink at maxShrinkEvictPerEvent.
	drive(adaptiveWindowLen(size, samples, target))
	afterFirst := int64(size - maxShrinkEvictPerEvent)
	require.Equal(t, int64(maxShrinkEvictPerEvent), atomic.LoadInt64(&cache.stats.ShrinkEvictions),
		"first shrink should hit maxShrinkEvictPerEvent exactly")
	require.Equal(t, afterFirst, atomic.LoadInt64(&cache.capacity),
		"capacity reflects the capped shed")

	// Window 2: same workload after the cooldown elapses; cap clamps again.
	// ShrinkEvictions accumulates, so the second shrink is visible as a second
	// 1024 increment.
	drive(adaptiveWindowLen(afterFirst, samples, target))
	require.Equal(t, int64(2*maxShrinkEvictPerEvent), atomic.LoadInt64(&cache.stats.ShrinkEvictions),
		"second shrink should add another maxShrinkEvictPerEvent — decay continues")
	require.Equal(t, afterFirst-int64(maxShrinkEvictPerEvent), atomic.LoadInt64(&cache.capacity),
		"capacity drops by the cap on each successive event")
}

// TestTagValueSeriesIDCache_ShrinkAfterPutEvictsBoundary is a regression
// test for a bug where a Put during an in-progress shrink window evicted
// the LRU boundary on an all-warm cache and nil-ed deepestTouched. A
// subsequent narrow Get re-seeded deepestTouched at the touched element
// (now at the front), so the warm-count walk gave warmCount=1 and
// decideColdTail trimmed the cache, evicting entries that HAD been
// touched this window. The fix recedes deepestTouched to e.Prev() (the
// new back of the now-smaller list, still warm) instead of nil-ing it:
// Put is reached after a Get miss so the new front element is itself
// warm, the warm set after the Put is the entire (now smaller) cache,
// and the existing "boundary == Back" short-circuit then prevents shrink.
func TestTagValueSeriesIDCache_ShrinkAfterPutEvictsBoundary(t *testing.T) {
	// minSamples=1 so the rate-floor doesn't block; target=0.95 so the
	// eviction-gate threshold stays at the 1-eviction floor for w=14.
	cache := newFullAdaptiveCache(t, 5, 1, 0.95)

	// Touch every entry: cache is fully warm, boundary = LRU.
	for i := 0; i < 5; i++ {
		require.NotNil(t, getVal(cache, i))
	}

	// Put a new key. The LRU IS the boundary; the Put-induced eviction
	// would have nil-ed deepestTouched pre-fix. With the fix, it recedes
	// to the new back (still warm).
	cache.Put([]byte("m"), []byte("k"), []byte{99}, tsdb.NewSeriesIDSet(99))

	// Narrow post-Put access: touch ONLY entry 4 (already at the front),
	// driving the window to completion without disturbing the new boundary.
	// Pre-fix, the first such Get re-seeded deepestTouched at 4 and the
	// walk gave warmCount=1, triggering a shrink to ~size/2 that evicted
	// warm entries 1 and 2. Post-fix, deepestTouched stays at the new
	// back, the "boundary == Back" short-circuit fires, and no shrink runs.
	w := adaptiveWindowLen(5, 1, 0.95)
	for i := int64(0); i < w; i++ {
		require.NotNil(t, getVal(cache, 4))
	}

	require.Equal(t, int64(5), atomic.LoadInt64(&cache.capacity),
		"capacity must not shrink: post Put-evict the cache is full-warm")
	require.Equal(t, int64(0), atomic.LoadInt64(&cache.stats.ShrinkEvictions),
		"no shrink trim should have run")
	require.Equal(t, int64(1), atomic.LoadInt64(&cache.stats.Evictions),
		"the Put-induced eviction lands in Evictions, not ShrinkEvictions")

	require.False(t, existsVal(cache, 0), "entry 0 was evicted by the Put")
	for _, v := range []int{1, 2, 3, 4, 99} {
		require.True(t, existsVal(cache, v), "entry %d must survive", v)
	}
}

func TestTagValueSeriesIDCache_ShrinkCooldown(t *testing.T) {
	// A capacity change sets a Gets-based cooldown that suppresses shrink until
	// it elapses. Seed a long cooldown and verify shrink is gated across several
	// windows, then clear it and confirm the next completed window shrinks.
	cache := newFullAdaptiveCache(t, 10, 8, 0.5)
	w := adaptiveWindowLen(10, 8, 0.5)
	cache.cooldownGets = 1000 // spans the windows driven below

	drive := func() {
		for i := int64(0); i < w; i++ { // one window (n stays 10 while no shrink)
			getVal(cache, int(i%3))
		}
	}

	drive()
	drive()
	drive()
	require.Equal(t, int64(10), atomic.LoadInt64(&cache.capacity), "shrink suppressed while cooling down")

	cache.Lock()
	cache.cooldownGets = 0
	cache.Unlock()
	drive()
	require.Equal(t, int64(5), atomic.LoadInt64(&cache.capacity), "shrink fires once the cooldown elapses")
}

func TestTagValueSeriesIDCache_ShrinkBoundaryReTouch(t *testing.T) {
	// Re-touching the deepest warm element must recede the boundary to its
	// predecessor, keeping warmCount equal to the true distinct-touched count.
	// A large minSamples keeps the window open so we can inspect mid-window.
	cache := NewAdaptiveTagValueSeriesIDCache(2, 1024, 0.5, tsdb.DefaultSeriesIDSetCacheShrinkConservatism, 1000, zap.NewNop())
	atomic.StoreInt64(&cache.capacity, 5)
	for i := 0; i < 5; i++ {
		cache.Put([]byte("m"), []byte("k"), []byte{byte(i)}, tsdb.NewSeriesIDSet(uint64(i)))
	}

	// Touch 0,1,2 (warm set {0,1,2}, deepest=0), then re-touch 0 (the boundary).
	for _, v := range []int{0, 1, 2, 0} {
		require.NotNil(t, getVal(cache, v))
	}

	cache.Lock()
	got := cache.warmCountLocked()
	cache.Unlock()
	require.Equal(t, int64(3), got, "warmCount must equal distinct-touched (3), not collapse on boundary re-touch")
}

func TestTagValueSeriesIDCache_ShrinkWindowFirstGetCountsInRate(t *testing.T) {
	// Regression: a shrink window opened on a Get must include that Get's
	// hit/miss in hitsW/missesW, matching its inclusion in getsSinceShrinkCheck
	// and deepestTouched. Otherwise a window opened by a miss looks like 100%
	// hit rate (the only miss is excluded from the baseline) and the rate gate
	// fails to block a spurious shrink.
	const target = 0.95
	cache := NewAdaptiveTagValueSeriesIDCache(2, 1024, target, tsdb.DefaultSeriesIDSetCacheShrinkConservatism, 8, zap.NewNop())
	atomic.StoreInt64(&cache.capacity, 10)
	for i := 0; i < 4; i++ {
		cache.Put([]byte("m"), []byte("k"), []byte{byte(i)}, tsdb.NewSeriesIDSet(uint64(i)))
	}

	w := adaptiveWindowLen(4, 8, target)
	// True rate (w-1)/w must be < target for the gate to block a shrink; any w
	// in [2, 19] satisfies this for target=0.95. Guard against future tuning.
	require.Greater(t, w, int64(1), "precondition: window must span >1 Get")
	require.Less(t, float64(w-1)/float64(w), target, "precondition: true rate < target")

	// First Get of the new window: absent key (a miss). Then fill the rest of
	// the window with hits on existing keys.
	require.Nil(t, getVal(cache, 99))
	for i := int64(1); i < w; i++ {
		require.NotNil(t, getVal(cache, int(i%4)))
	}

	// Pre-fix, the opening miss was excluded from missesW, so the window looked
	// like 100% hit rate and the slack branch trimmed capacity to size (4).
	require.Equal(t, int64(10), atomic.LoadInt64(&cache.capacity),
		"rate gate must block shrink when the window-opening miss is counted")
}

func TestTagValueSeriesIDCache_ShrinkGrowCooldownStamp(t *testing.T) {
	// A grow stamps the cooldown so a freshly-grown cache is not immediately
	// shrunk; the cooldown is sized to the new capacity, not the half-full
	// occupancy. Drive a grow and assert the cooldown matches.
	cache := NewAdaptiveTagValueSeriesIDCache(2, 16, 0.99, tsdb.DefaultSeriesIDSetCacheShrinkConservatism, 4, zap.NewNop())
	insert := func(seq int) {
		v := []byte{byte(seq)}
		require.Nil(t, cache.Get([]byte("m"), []byte("k"), v))
		cache.Put([]byte("m"), []byte("k"), v, tsdb.NewSeriesIDSet(uint64(seq)))
	}
	for i := 1; i <= 4; i++ { // drives one doubling (2 -> 4)
		insert(i)
	}
	require.Equal(t, int64(4), atomic.LoadInt64(&cache.capacity), "precondition: grow occurred")
	cache.Lock()
	cd := cache.cooldownGets
	cache.Unlock()
	require.Equal(t, adaptiveWindowLen(4, 4, 0.99), cd, "grow arms a cooldown sized to the new capacity")
}

// TestTagValueSeriesIDCache_Adaptive_Concurrent exercises the adaptive grow and
// shrink paths (and the lockless Statistics reader) under concurrency so the
// race detector validates the new shrink bookkeeping. The keyspace is small
// enough to generate both hits (boundary tracking) and eviction pressure
// (growth), and reads outnumber writes so shrink windows can fire.
func TestTagValueSeriesIDCache_Adaptive_Concurrent(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long test")
	}

	cache := NewAdaptiveTagValueSeriesIDCache(8, 256, 0.9, tsdb.DefaultSeriesIDSetCacheShrinkConservatism, 16, zap.NewNop())

	const (
		writers = 4
		readers = 8
		iters   = 50000
		keys    = 40 // > initial capacity, so writes create eviction pressure
	)
	key := func(i int) []byte { return []byte{byte(i % keys)} }

	// Start all goroutines simultaneously to maximize contention (per the
	// project's concurrency-test pattern): each acquires the read lock at the
	// start and blocks until the write lock is released.
	var start sync.RWMutex
	var concurrency, maxConcurrency atomic.Int64
	var wg sync.WaitGroup
	start.Lock()

	run := func(body func(i int)) {
		wg.Add(1)
		go func() {
			start.RLock()
			defer start.RUnlock()
			defer wg.Done()
			c := concurrency.Add(1)
			if old := maxConcurrency.Load(); c > old {
				maxConcurrency.CompareAndSwap(old, c)
			}
			for i := 0; i < iters; i++ {
				body(i)
			}
			concurrency.Add(-1)
		}()
	}

	for w := 0; w < writers; w++ {
		run(func(i int) { cache.Put([]byte("m"), []byte("k"), key(i), tsdb.NewSeriesIDSet(uint64(i))) })
	}
	for r := 0; r < readers; r++ {
		run(func(i int) { _ = cache.Get([]byte("m"), []byte("k"), key(i)) })
	}
	// A lockless Statistics sampler races against the atomic counters.
	run(func(int) { _ = cache.Statistics(nil) })

	start.Unlock() // release all goroutines at once
	wg.Wait()
	t.Logf("max concurrency: %d", maxConcurrency.Load())

	finalCap := atomic.LoadInt64(&cache.capacity)
	require.GreaterOrEqual(t, finalCap, int64(8), "capacity must never drop below the floor")
	require.LessOrEqual(t, finalCap, int64(256), "capacity must never exceed the max")
	require.Equal(t, int64(cache.evictor.Len()), atomic.LoadInt64(&cache.stats.Size), "size counter must track the evictor list")
}
