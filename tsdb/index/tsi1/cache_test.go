package tsi1

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
		statTagValueCacheIdleEviction:   int64(0),
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
		statTagValueCacheIdleEviction:   int64(0),
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
		statTagValueCacheIdleEviction:   int64(0),
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
		statTagValueCacheIdleEviction:   int64(0),
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
		{
			// Regression: a cache configured smaller than minSamples still grows.
			// Its window is only ~capacity Gets long under thrash, so the raw
			// minSamples floor could never be cleared; the floor is clamped to
			// capacity. Here capacity 50 < minSamples 100, gets == capacity, rate
			// 0 (< target), so growth must fire despite gets < minSamples.
			name: "sub-minSamples cache grows on a full-window thrash",
			hits: 0, misses: 50, capacity: 50, maxCap: maxCap,
			minSamples: 100, target: target,
			wantNewCap: 100, wantGrow: true,
		},
		{
			// The clamp lowers the floor to capacity, not to zero: a sub-minSamples
			// cache with fewer than capacity Gets still has too little evidence and
			// must not grow. capacity 50, gets 30 < 50 → gated.
			name: "sub-minSamples cache below its clamped floor does not grow",
			hits: 0, misses: 30, capacity: 50, maxCap: maxCap,
			minSamples: 100, target: target,
			wantNewCap: 50, wantGrow: false,
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
	cache := NewAdaptiveTagValueSeriesIDCache(2, 16, 0.99, tsdb.DefaultSeriesIDSetCacheShrinkConservatism, 4, 0, logger)

	require.Equal(t, int64(2), cache.capacity.Load())

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
	require.Equal(t, int64(4), cache.capacity.Load(), "after first turnover, capacity=4")

	// After grow to 4, two free slots. Need to refill before evicting:
	//   insert 5: size 2→3
	//   insert 6: size 3→4 (cache full at cap=4)
	//   insert 7..10: each evicts once. 4 evictions → fires → cap=8
	for i := 5; i <= 10; i++ {
		insert(i)
	}
	require.Equal(t, int64(8), cache.capacity.Load(), "after second turnover, capacity=8")

	// After grow to 8, four free slots. Refill then 8 evictions:
	//   insert 11..14: fill to size=8
	//   insert 15..22: 8 evictions → fires → cap=16 (= max)
	for i := 11; i <= 22; i++ {
		insert(i)
	}
	require.Equal(t, int64(16), cache.capacity.Load(), "after third turnover, capacity=16 (= max)")

	// Further evictions do not grow past max.
	// After grow to 16, eight free slots. Fill them, then drive enough
	// evictions to trigger another firing — which must not grow.
	for i := 23; i <= 50; i++ {
		insert(i)
	}
	require.Equal(t, int64(16), cache.capacity.Load(), "capacity stays at max")
}

func TestTagValueSeriesIDCache_AdaptiveGrowth_NoOpAtTarget(t *testing.T) {
	// Target so low that any nonzero hit rate satisfies it. We drive
	// evictions while also generating hits, so the windowed hit rate
	// stays comfortably above target and capacity must not grow.
	logger := zap.NewNop()
	cache := NewAdaptiveTagValueSeriesIDCache(2, 16, 0.01, tsdb.DefaultSeriesIDSetCacheShrinkConservatism, 4, 0, logger)

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
	require.Equal(t, int64(2), cache.capacity.Load(), "capacity must stay at 2 when target is met")
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

	require.Equal(t, int64(2), cache.capacity.Load(), "capacity must not change when adaptive sizing is disabled")
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

	require.Equal(t, int64(4), cache.capacity.Load(), "cache should have grown after policy fired")
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
	// Invalid arguments must NOT panic; the constructor logs the error and
	// returns a fixed-size (non-adaptive) cache so misconfiguration degrades to
	// a working cache. A non-adaptive cache is identified by maxCapacity == 0.
	const defaultC = tsdb.DefaultSeriesIDSetCacheShrinkConservatism
	cases := []struct {
		name             string
		initial, max     int
		target, cons     float64
		minSamples       int
		idleTimeout      time.Duration
		wantLogSubstring string
		wantFallbackSize int64 // size NewTagValueSeriesIDCache was called with
	}{
		{"initial zero", 0, 16, 0.95, defaultC, 4, 0, "initial must be > 0", int64(tsdb.DefaultSeriesIDSetCacheSize)},
		{"initial negative", -1, 16, 0.95, defaultC, 4, 0, "initial must be > 0", int64(tsdb.DefaultSeriesIDSetCacheSize)},
		{"max equal to initial", 2, 2, 0.95, defaultC, 4, 0, "max must be > initial", 2},
		{"max less than initial", 2, 1, 0.95, defaultC, 4, 0, "max must be > initial", 2},
		{"target NaN", 2, 16, math.NaN(), defaultC, 4, 0, "target must be in (0, 1)", 2},
		{"target zero", 2, 16, 0, defaultC, 4, 0, "target must be in (0, 1)", 2},
		{"target one", 2, 16, 1, defaultC, 4, 0, "target must be in (0, 1)", 2},
		{"target negative", 2, 16, -0.1, defaultC, 4, 0, "target must be in (0, 1)", 2},
		{"target above 1", 2, 16, 1.5, defaultC, 4, 0, "target must be in (0, 1)", 2},
		{"conservatism NaN", 2, 16, 0.95, math.NaN(), 4, 0, "shrinkConservatism", 2},
		{"conservatism +Inf", 2, 16, 0.95, math.Inf(1), 4, 0, "shrinkConservatism", 2},
		{"conservatism -Inf", 2, 16, 0.95, math.Inf(-1), 4, 0, "shrinkConservatism", 2},
		{"conservatism negative", 2, 16, 0.95, -1, 4, 0, "shrinkConservatism", 2},
		{"minSamples negative", 2, 16, 0.95, defaultC, -1, 0, "minSamples must be >= 0", 2},
		{"idleTimeout negative", 2, 16, 0.95, defaultC, 4, -1, "idleTimeout must be >= 0", 2},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			core, logs := observer.New(zap.ErrorLevel)
			c := NewAdaptiveTagValueSeriesIDCache(tt.initial, tt.max, tt.target, tt.cons, tt.minSamples, tt.idleTimeout, zap.New(core))
			require.NotNil(t, c)
			require.Equal(t, int64(0), c.maxCapacity, "fallback must be non-adaptive (maxCapacity == 0)")
			require.Equal(t, tt.wantFallbackSize, c.capacity.Load(), "fallback capacity")
			entries := logs.All()
			require.Len(t, entries, 2, "expected the specific error plus the fallback log")
			require.Contains(t, entries[0].Message, tt.wantLogSubstring)
			require.Contains(t, entries[1].Message, "falling back to fixed-size cache")
		})
	}

	// All invalid arguments are reported in a single construction: validation
	// does not stop at the first failure. Every argument here is invalid, so each
	// produces its own error log, followed by the fallback summary.
	t.Run("all invalid arguments reported", func(t *testing.T) {
		core, logs := observer.New(zap.ErrorLevel)
		c := NewAdaptiveTagValueSeriesIDCache(-1, -2, math.NaN(), math.Inf(1), -3, -1, zap.New(core))
		require.NotNil(t, c)
		require.Equal(t, int64(0), c.maxCapacity, "fallback must be non-adaptive (maxCapacity == 0)")
		require.Equal(t, int64(tsdb.DefaultSeriesIDSetCacheSize), c.capacity.Load(), "fallback capacity")

		entries := logs.All()
		require.Len(t, entries, 7, "six argument errors plus the fallback summary")
		require.Contains(t, entries[0].Message, "initial must be > 0")
		require.Contains(t, entries[1].Message, "max must be > initial")
		require.Contains(t, entries[2].Message, "target must be in (0, 1)")
		require.Contains(t, entries[3].Message, "shrinkConservatism")
		require.Contains(t, entries[4].Message, "minSamples must be >= 0")
		require.Contains(t, entries[5].Message, "idleTimeout must be >= 0")
		require.Contains(t, entries[6].Message, "falling back to fixed-size cache")
	})

	// Valid conservatism boundary values (0 = median admit; small positive)
	// must produce an adaptive cache and log nothing.
	for _, cons := range []float64{0, 0.5, 1, 2.0, 10} {
		core, logs := observer.New(zap.ErrorLevel)
		cache := NewAdaptiveTagValueSeriesIDCache(2, 16, 0.95, cons, 4, 0, zap.New(core))
		require.NotNil(t, cache)
		require.Equal(t, int64(16), cache.maxCapacity, "valid args must produce adaptive cache, conservatism=%v", cons)
		require.Empty(t, logs.All(), "valid args must not log, conservatism=%v", cons)
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
			// Cold tail of 19900 (and size/2 of 10000) both exceed the per-event
			// cap, so the absolute bound is the binding one. Decay continues over
			// later windows (covered by TestTagValueSeriesIDCache_ShrinkRepeatsWhenCapped).
			name:  "cold-tail bounded by maxShrinkEvictPerEvent",
			hitsW: 1000, missesW: 0, evictionsW: 0,
			capacity: 20000, size: 20000, warmCount: 100, floor: 10,
			wantNewCap: 20000 - maxShrinkEvictPerEvent, wantEvict: maxShrinkEvictPerEvent, wantShrink: true,
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

// decideShrink implements the full capacity-decay policy as a pure function of
// the window's counters, the current occupancy, and the observed access
// footprint. It mirrors decideResize so the policy can be exercised with integer
// literals. It composes decideShrinkPre and decideColdTail; production code calls
// those two directly so the O(size) footprint walk that produces warmCount is
// only performed when the cold-tail branch is actually reached (see
// maybeShrinkLocked).
//
// Gates (all required): at least one read; window evictions at or below the
// scaled at-target expectation (see atTargetEvictionGateLimit); and a windowed
// hit rate >= target (the cache is serving its working set well). When gated
// through, one of two branches fires:
//   - slack: capacity exceeds occupancy -> drop the unused headroom (no eviction).
//   - cold-tail: cache is full -> shed the LRU tail that went untouched this
//     window, down to the warm footprint (warmCount), clamped at floor, and
//     bounded to half the cache per event so the write lock is not held long.
//
// The evicted entries are always within the untouched cold tail, so warm entries
// are never shed.
func decideShrink(hitsW, missesW, evictionsW, capacity, size, warmCount, floor int64, target, conservatism float64) (newCap, evict int64, shrink bool) {
	newCap, shrink, coldTail := decideShrinkPre(hitsW, missesW, evictionsW, capacity, size, floor, target, conservatism)
	if coldTail {
		return decideColdTail(size, warmCount, floor)
	}
	return newCap, 0, shrink
}

// TestDecideIdleShrink_Table exercises the idle-step policy: the slack branch
// when capacity exceeds occupancy, otherwise one cold-tail step with an empty
// warm footprint, bounded by floor, size/2 and maxShrinkEvictPerEvent.
func TestDecideIdleShrink_Table(t *testing.T) {
	tests := []struct {
		name                  string
		capacity, size, floor int64
		wantNewCap, wantEvict int64
		wantShrink            bool
	}{
		{name: "slack drops headroom to occupancy", capacity: 10, size: 6, floor: 4, wantNewCap: 6, wantEvict: 0, wantShrink: true},
		{name: "slack floored", capacity: 10, size: 3, floor: 4, wantNewCap: 4, wantEvict: 0, wantShrink: true},
		{name: "headroom but at floor", capacity: 4, size: 3, floor: 4, wantNewCap: 4, wantEvict: 0, wantShrink: false},
		{name: "full at floor", capacity: 4, size: 4, floor: 4, wantNewCap: 4, wantEvict: 0, wantShrink: false},
		{name: "cold tail half-bound", capacity: 10, size: 10, floor: 2, wantNewCap: 5, wantEvict: 5, wantShrink: true},
		{name: "cold tail floor-bound", capacity: 6, size: 6, floor: 4, wantNewCap: 4, wantEvict: 2, wantShrink: true},
		{
			name: "cold tail absolute-bound", capacity: 20000, size: 20000, floor: 2,
			wantNewCap: 20000 - maxShrinkEvictPerEvent, wantEvict: maxShrinkEvictPerEvent, wantShrink: true,
		},
		{
			// size/2 equals maxShrinkEvictPerEvent: both bounds give the same answer.
			name: "half-bound equals absolute bound", capacity: 2 * maxShrinkEvictPerEvent, size: 2 * maxShrinkEvictPerEvent, floor: 2,
			wantNewCap: maxShrinkEvictPerEvent, wantEvict: maxShrinkEvictPerEvent, wantShrink: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newCap, evict, shrink := decideIdleShrink(tt.capacity, tt.size, tt.floor)
			require.Equal(t, tt.wantShrink, shrink, "shrink")
			require.Equal(t, tt.wantNewCap, newCap, "newCap")
			require.Equal(t, tt.wantEvict, evict, "evict")
		})
	}
}

// TestAtTargetEvictionGateLimit_Table exercises the gate-threshold helper across
// a range of z (conservatism) values — including a mid-(0,1) z and a z>3 — that
// the constructor's NotPanics check accepts but no behavior test exercises. With
// gets=1000 and target=0.95, mu = 1000*0.05 = 50 and sigma = sqrt(1000*0.95*0.05)
// ~= 6.8920, so the int64 floor of mu - z*sigma is computable to one count.
func TestAtTargetEvictionGateLimit_Table(t *testing.T) {
	tests := []struct {
		name      string
		gets      int64
		target, z float64
		want      int64
	}{
		// z=0 admits at the mean; covers the median-admit case the doc calls out.
		{name: "z=0 admits at the mean", gets: 1000, target: 0.95, z: 0, want: 50},
		// z in (0, 1): small relaxation below the mean. 50 - 0.5*6.892 = 46.554.
		{name: "z=0.5 (in (0,1)) lowers limit below mean", gets: 1000, target: 0.95, z: 0.5, want: 46},
		// Default production conservatism: 50 - 2.5*6.892 = 32.770.
		{name: "z=2.5 (default) lowers limit further", gets: 1000, target: 0.95, z: 2.5, want: 32},
		// z>3 deep below the mean drives the result negative; the floor at 1 binds.
		{name: "z=10 (>3) floors at 1", gets: 1000, target: 0.95, z: 10, want: 1},
		// Different targets shift the mean: T=0.5 -> mu=500; T=0.99 -> mu=10.
		{name: "target=0.5 widens the mean", gets: 1000, target: 0.5, z: 0, want: 500},
		{name: "target=0.99 narrows the mean", gets: 1000, target: 0.99, z: 0, want: 10},
		// Tiny window where mu < 1: the floor binds even at z=0.
		{name: "gets=1 floors at 1", gets: 1, target: 0.95, z: 0, want: 1},
		// Out-of-range inputs return 1 (fail-safe: gate any eviction).
		{name: "gets=0 fail-safe", gets: 0, target: 0.95, z: 0, want: 1},
		{name: "gets=-1 fail-safe", gets: -1, target: 0.95, z: 0, want: 1},
		{name: "target=0 fail-safe", gets: 1000, target: 0, z: 0, want: 1},
		{name: "target=1 fail-safe", gets: 1000, target: 1, z: 0, want: 1},
		{name: "target=NaN fail-safe", gets: 1000, target: math.NaN(), z: 0, want: 1},
		{name: "z=-0.1 fail-safe", gets: 1000, target: 0.95, z: -0.1, want: 1},
		{name: "z=NaN fail-safe", gets: 1000, target: 0.95, z: math.NaN(), want: 1},
		{name: "z=+Inf fail-safe", gets: 1000, target: 0.95, z: math.Inf(1), want: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, atTargetEvictionGateLimit(tt.gets, tt.target, tt.z))
		})
	}
}

// TestDecideShrink_ConservatismVariation locks in the scale-adaptive margin the
// gate provides: with the same window evictions, raising z lowers the admission
// threshold and flips the gate from pass to block. TestDecideShrink_PolicyTable
// only exercises the default z (2.5); this test exercises z values in (0, 1) and
// z > 3 to confirm both ends of the legal range affect shrink behavior.
func TestDecideShrink_ConservatismVariation(t *testing.T) {
	const (
		target  = 0.95
		rowGets = int64(1000)
	)
	// At target=0.95 and gets=1000, mu=50. The cold-tail shape mirrors a row
	// from TestDecideShrink_PolicyTable: full cache (size=capacity=100), warm
	// prefix of 20, hit rate 1.0 so the hit-rate gate always passes.
	tests := []struct {
		name         string
		conservatism float64
		evictionsW   int64
		wantShrink   bool
	}{
		// z=0 admits at the mean (limit=50). evictionsW=50 is NOT > 50, gate
		// passes, cold-tail branch shrinks. Documents the median-admit boundary.
		{name: "z=0 admits at the median", conservatism: 0, evictionsW: 50, wantShrink: true},
		// z=0.5 (in (0,1)) lowers limit to 46; evictionsW=50 > 46 blocks. Proves
		// a small in-range z already tightens the gate.
		{name: "z=0.5 (in (0,1)) blocks above lowered limit", conservatism: 0.5, evictionsW: 50, wantShrink: false},
		// z=10 (>3) drives the limit to the floor (1); evictionsW=50 > 1 blocks
		// hard. Proves a very conservative operator setting suppresses shrink
		// under normal eviction pressure.
		{name: "z=10 (>3) blocks above the floored limit", conservatism: 10, evictionsW: 50, wantShrink: false},
		// z=10 still admits when evictions sit at the floor: 1 > 1 is false, so
		// even a very conservative cache can shrink when traffic is genuinely quiet.
		{name: "z=10 (>3) admits at the floor", conservatism: 10, evictionsW: 1, wantShrink: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, shrink := decideShrink(
				rowGets, 0, tt.evictionsW,
				100, 100, 20, 10, target, tt.conservatism,
			)
			require.Equal(t, tt.wantShrink, shrink)
		})
	}
}

// newFullAdaptiveCache returns an adaptive cache forced to the given capacity
// and filled with that many entries (values 0..capacity-1). The floor is 2, so
// shrink has room to act. Adaptive growth is not exercised (no evictions occur
// during setup), so the forced capacity stands in for a previously-grown cache.
func newFullAdaptiveCache(t *testing.T, capacity, minSamples int, target float64) *TagValueSeriesIDCache {
	t.Helper()
	c := NewAdaptiveTagValueSeriesIDCache(2, 1024, target, tsdb.DefaultSeriesIDSetCacheShrinkConservatism, minSamples, 0, zap.NewNop())
	c.capacity.Store(int64(capacity))
	for i := 0; i < capacity; i++ {
		c.Put([]byte("m"), []byte("k"), []byte{byte(i)}, tsdb.NewSeriesIDSet(uint64(i)))
	}
	require.Equal(t, int64(capacity), c.stats.Size.Load(), "setup occupancy")
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

	require.Equal(t, int64(5), cache.capacity.Load(), "capacity = size - min(coldTail, size/2) = 10-5")
	require.Equal(t, int64(5), cache.stats.Size.Load())

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
	cache := NewAdaptiveTagValueSeriesIDCache(2, 1024, 0.5, tsdb.DefaultSeriesIDSetCacheShrinkConservatism, 8, 0, zap.NewNop())
	cache.capacity.Store(10)
	for i := 0; i < 4; i++ {
		cache.Put([]byte("m"), []byte("k"), []byte{byte(i)}, tsdb.NewSeriesIDSet(uint64(i)))
	}

	w := adaptiveWindowLen(4, 8, 0.5)
	for i := int64(0); i < w; i++ {
		require.NotNil(t, getVal(cache, int(i%4)))
	}

	require.Equal(t, int64(4), cache.capacity.Load(), "slack branch trims capacity to occupancy")
	require.Equal(t, int64(4), cache.stats.Size.Load(), "no eviction in the slack branch")
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

	require.Equal(t, int64(10), cache.capacity.Load(), "capacity must not shrink when the whole cache is in use")
	require.Equal(t, int64(10), cache.stats.Size.Load())
}

// TestTagValueSeriesIDCache_ShrinkRepeatsWhenCapped verifies that when the
// unbounded cold-tail shed would exceed maxShrinkEvictPerEvent, a single event
// sheds exactly the cap and subsequent windows continue the decay — the claim
// made by decideColdTail's doc comment.
//
// Cache size 24578 (3·8192 + 2) with a warm set of 10 and floor 2:
//   - Window 1: unbounded shed 24568 → size/2 bound 12289 → cap 8192. New cap 16386.
//   - Window 2: unbounded shed 16376 → size/2 bound 8193 → cap 8192. New cap 8194.
//
// The post-shrink cooldown is adaptiveWindowLen(newCap, samples, target), which
// matches the next window's length, so the cooldown elapses exactly when window 2
// ends — no manual cooldown reset is needed (and the natural cycle is itself
// useful coverage).
func TestTagValueSeriesIDCache_ShrinkRepeatsWhenCapped(t *testing.T) {
	const (
		target  = 0.5
		size    = 3*maxShrinkEvictPerEvent + 2
		warm    = 10
		samples = 8
	)
	cache := NewAdaptiveTagValueSeriesIDCache(2, 32768, target, tsdb.DefaultSeriesIDSetCacheShrinkConservatism, samples, 0, zap.NewNop())
	cache.capacity.Store(size)

	// 2-byte values give a unique key per i in [0, 65536); the single-byte
	// values used by newFullAdaptiveCache would collide past 255.
	keys := make([][]byte, size)
	for i := 0; i < size; i++ {
		keys[i] = []byte{byte(i >> 8), byte(i & 0xFF)}
		cache.Put([]byte("m"), []byte("k"), keys[i], tsdb.NewSeriesIDSet(uint64(i)))
	}
	require.Equal(t, int64(size), cache.stats.Size.Load(), "setup occupancy")

	drive := func(w int64) {
		for i := int64(0); i < w; i++ {
			require.NotNil(t, cache.Get([]byte("m"), []byte("k"), keys[int(i)%warm]))
		}
	}

	// Window 1: cap clamps the first shrink at maxShrinkEvictPerEvent.
	drive(adaptiveWindowLen(size, samples, target))
	afterFirst := int64(size - maxShrinkEvictPerEvent)
	require.Equal(t, int64(maxShrinkEvictPerEvent), cache.stats.ShrinkEvictions.Load(),
		"first shrink should hit maxShrinkEvictPerEvent exactly")
	require.Equal(t, afterFirst, cache.capacity.Load(),
		"capacity reflects the capped shed")

	// Window 2: same workload after the cooldown elapses; cap clamps again.
	// ShrinkEvictions accumulates, so the second shrink is visible as a second
	// maxShrinkEvictPerEvent increment.
	drive(adaptiveWindowLen(afterFirst, samples, target))
	require.Equal(t, int64(2*maxShrinkEvictPerEvent), cache.stats.ShrinkEvictions.Load(),
		"second shrink should add another maxShrinkEvictPerEvent — decay continues")
	require.Equal(t, afterFirst-int64(maxShrinkEvictPerEvent), cache.capacity.Load(),
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

	require.Equal(t, int64(5), cache.capacity.Load(),
		"capacity must not shrink: post Put-evict the cache is full-warm")
	require.Equal(t, int64(0), cache.stats.ShrinkEvictions.Load(),
		"no shrink trim should have run")
	require.Equal(t, int64(1), cache.stats.Evictions.Load(),
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
	require.Equal(t, int64(10), cache.capacity.Load(), "shrink suppressed while cooling down")

	cache.Lock()
	cache.cooldownGets = 0
	cache.Unlock()
	drive()
	require.Equal(t, int64(5), cache.capacity.Load(), "shrink fires once the cooldown elapses")
}

func TestTagValueSeriesIDCache_ShrinkBoundaryReTouch(t *testing.T) {
	// Re-touching the deepest warm element must recede the boundary to its
	// predecessor, keeping warmCount equal to the true distinct-touched count.
	// A large minSamples keeps the window open so we can inspect mid-window.
	cache := NewAdaptiveTagValueSeriesIDCache(2, 1024, 0.5, tsdb.DefaultSeriesIDSetCacheShrinkConservatism, 1000, 0, zap.NewNop())
	cache.capacity.Store(5)
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
	cache := NewAdaptiveTagValueSeriesIDCache(2, 1024, target, tsdb.DefaultSeriesIDSetCacheShrinkConservatism, 8, 0, zap.NewNop())
	cache.capacity.Store(10)
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
	require.Equal(t, int64(10), cache.capacity.Load(),
		"rate gate must block shrink when the window-opening miss is counted")
}

func TestTagValueSeriesIDCache_ShrinkGrowCooldownStamp(t *testing.T) {
	// A grow stamps the cooldown so a freshly-grown cache is not immediately
	// shrunk; the cooldown is sized to the new capacity, not the half-full
	// occupancy. Drive a grow and assert the cooldown matches.
	cache := NewAdaptiveTagValueSeriesIDCache(2, 16, 0.99, tsdb.DefaultSeriesIDSetCacheShrinkConservatism, 4, 0, zap.NewNop())
	insert := func(seq int) {
		v := []byte{byte(seq)}
		require.Nil(t, cache.Get([]byte("m"), []byte("k"), v))
		cache.Put([]byte("m"), []byte("k"), v, tsdb.NewSeriesIDSet(uint64(seq)))
	}
	for i := 1; i <= 4; i++ { // drives one doubling (2 -> 4)
		insert(i)
	}
	require.Equal(t, int64(4), cache.capacity.Load(), "precondition: grow occurred")
	cache.Lock()
	cd := cache.cooldownGets
	cache.Unlock()
	require.Equal(t, adaptiveWindowLen(4, 4, 0.99), cd, "grow arms a cooldown sized to the new capacity")
}

func TestTagValueSeriesIDCache_ShrinkResetsGrowWindow(t *testing.T) {
	// After a shrink, the grow policy's per-window state still describes the
	// pre-shrink window. If evictionsSinceCheck happens to be near or above the
	// new smaller capacity, the next forced eviction would fire
	// maybeResizeLocked immediately against stale hit/miss baselines. A shrink
	// must reset all three so the grow window restarts post-shrink.
	cache := newFullAdaptiveCache(t, 10, 8, 0.5)

	// Pre-seed the grow-window state to look "almost ready to fire", with
	// baselines from a moment that predates the shrink window.
	cache.Lock()
	cache.evictionsSinceCheck = 7
	cache.lastHits = 99
	cache.lastMisses = 99
	cache.Unlock()

	// Drive a window of all hits on a warm subset of 3 to trigger a shrink.
	w := adaptiveWindowLen(10, 8, 0.5)
	for i := int64(0); i < w; i++ {
		require.NotNil(t, getVal(cache, int(i%3)))
	}
	require.Less(t, cache.capacity.Load(), int64(10),
		"precondition: shrink should have fired")

	cache.Lock()
	defer cache.Unlock()
	require.Equal(t, int64(0), cache.evictionsSinceCheck,
		"evictionsSinceCheck must reset so the next forced eviction starts a fresh grow window")
	require.Equal(t, cache.stats.Hits.Load(), cache.lastHits,
		"lastHits must snap to the current hit count after shrink")
	require.Equal(t, cache.stats.Misses.Load(), cache.lastMisses,
		"lastMisses must snap to the current miss count after shrink")
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

	cache := NewAdaptiveTagValueSeriesIDCache(8, 256, 0.9, tsdb.DefaultSeriesIDSetCacheShrinkConservatism, 16, 0, zap.NewNop())

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

	finalCap := cache.capacity.Load()
	require.GreaterOrEqual(t, finalCap, int64(8), "capacity must never drop below the floor")
	require.LessOrEqual(t, finalCap, int64(256), "capacity must never exceed the max")
	require.Equal(t, int64(cache.evictor.Len()), cache.stats.Size.Load(), "size counter must track the evictor list")
}

// TestTagValueSeriesIDCache_AtFloor_SkipsShrinkBookkeeping verifies that while
// the cache sits at its floor (capacity == minCapacity) a shrink can never fire,
// so the read path skips all shrink window bookkeeping: it tracks no footprint
// and starts no window. Regression guard for the floor gate — without it, the
// first hit would start a window and set deepestTouched.
func TestTagValueSeriesIDCache_AtFloor_SkipsShrinkBookkeeping(t *testing.T) {
	const floor = 8
	cache := NewAdaptiveTagValueSeriesIDCache(floor, 64, 0.5, tsdb.DefaultSeriesIDSetCacheShrinkConservatism, 4, 0, zap.NewNop())
	require.Equal(t, int64(floor), cache.capacity.Load())
	require.Equal(t, int64(floor), cache.minCapacity)

	// Fill exactly to the floor (full, no eviction).
	for i := 0; i < floor; i++ {
		cache.Put([]byte("m"), []byte("k"), []byte{byte(i)}, tsdb.NewSeriesIDSet(uint64(i)))
	}
	require.Equal(t, int64(floor), cache.stats.Size.Load())

	// A handful of hits on a subset — fewer than one window. Without the gate this
	// would already have a live window and a tracked footprint.
	for i := 0; i < 3; i++ {
		require.NotNil(t, getVal(cache, i%3))
	}
	cache.Lock()
	window, gets, deepest := cache.shrinkWindowGets, cache.getsSinceShrinkCheck, cache.deepestTouched
	cache.Unlock()
	require.Zero(t, window, "no shrink window may start at the floor")
	require.Zero(t, gets, "no window gets may accumulate at the floor")
	require.Nil(t, deepest, "no footprint may be tracked at the floor")

	// Over many more would-be windows, nothing is ever shed and capacity holds.
	for i := 0; i < 500; i++ {
		require.NotNil(t, getVal(cache, i%3))
	}
	require.Equal(t, int64(floor), cache.capacity.Load(), "capacity must stay at the floor")
	require.Equal(t, int64(floor), cache.stats.Size.Load(), "nothing may be evicted at the floor")
	require.Zero(t, cache.stats.ShrinkEvictions.Load(), "no shrink evictions at the floor")
}

// TestTagValueSeriesIDCache_FloorGateReopensAfterGrowth proves the floor gate is
// not a one-way latch: growth from the floor is unaffected (it runs on the Put
// path), and once capacity rises above the floor the read path resumes shrink
// bookkeeping so the cache can shrink again.
func TestTagValueSeriesIDCache_FloorGateReopensAfterGrowth(t *testing.T) {
	const floor = 4
	cache := NewAdaptiveTagValueSeriesIDCache(floor, 64, 0.5, tsdb.DefaultSeriesIDSetCacheShrinkConservatism, 4, 0, zap.NewNop())
	require.Equal(t, int64(floor), cache.capacity.Load())

	// Get-miss then Put each distinct key, driving evictions that grow capacity.
	insert := func(seq int) {
		v := []byte{byte(seq)}
		require.Nil(t, cache.Get([]byte("m"), []byte("k"), v))
		cache.Put([]byte("m"), []byte("k"), v, tsdb.NewSeriesIDSet(uint64(seq)))
	}
	for i := 0; i < 60; i++ {
		insert(i)
	}
	peak := cache.capacity.Load()
	require.Greater(t, peak, int64(floor), "growth must not be blocked by the floor gate")

	// Repeatedly hit a tiny, most-recently-inserted (still-resident) warm set.
	// Once the post-grow cooldown drains and a pure-hit window completes, the cold
	// tail is shed — which can only happen if the gate reopened above the floor.
	warm := []int{57, 58, 59}
	var shrank bool
	for round := 0; round < 5000 && !shrank; round++ {
		for _, v := range warm {
			getVal(cache, v)
		}
		shrank = cache.capacity.Load() < peak && cache.stats.ShrinkEvictions.Load() > 0
	}
	require.True(t, shrank, "cache must shrink after growth (floor gate must reopen)")
	require.GreaterOrEqual(t, cache.capacity.Load(), int64(floor), "must never shrink below the floor")
}

// benchmarkGetHit measures the cost of a cache hit with adaptive sizing enabled,
// for a cache forced to `capacity` with floor `floor`. When capacity == floor the
// floor gate skips the per-hit footprint tracking and the shrink window
// machinery; when capacity > floor that bookkeeping runs. The working set is the
// whole cache so the above-floor case never actually shrinks (warmCount == size),
// keeping the two benchmarks an apples-to-apples comparison of the overhead the
// gate removes.
func benchmarkGetHit(b *testing.B, capacity, floor int) {
	cache := NewAdaptiveTagValueSeriesIDCache(floor, 1<<20, 0.5, tsdb.DefaultSeriesIDSetCacheShrinkConservatism, tsdb.DefaultAdaptiveCacheMinSamples, 0, zap.NewNop())
	cache.capacity.Store(int64(capacity))
	for i := 0; i < capacity; i++ {
		cache.Put([]byte("m"), []byte("k"), []byte{byte(i)}, tsdb.NewSeriesIDSet(uint64(i)))
	}
	name, key := []byte("m"), []byte("k")
	val := []byte{0}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		val[0] = byte(i % capacity)
		cache.Get(name, key, val)
	}
}

// At the floor (capacity == minCapacity): the shrink bookkeeping is skipped.
func BenchmarkTagValueSeriesIDCache_GetHit_AtFloor(b *testing.B) {
	benchmarkGetHit(b, 64, 64)
}

// Above the floor (capacity > minCapacity): the shrink bookkeeping runs.
func BenchmarkTagValueSeriesIDCache_GetHit_AboveFloor(b *testing.B) {
	benchmarkGetHit(b, 64, 2)
}

// idleKey returns a 2-byte value so up to 65536 entries are unique.
func idleKey(i int) []byte { return []byte{byte(i >> 8), byte(i)} }

// newIdleAdaptiveCache returns an adaptive cache with the given floor and idle
// timeout, forced to capacity and holding entries values 0..entries-1 (front
// entries-1, back 0). No evictions occur during setup, so the forced capacity
// stands in for a previously-grown cache.
func newIdleAdaptiveCache(t *testing.T, floor, capacity, entries int, idle time.Duration) *TagValueSeriesIDCache {
	t.Helper()
	require.LessOrEqual(t, entries, capacity, "setup must not evict")
	c := NewAdaptiveTagValueSeriesIDCache(floor, 1<<20, 0.5, tsdb.DefaultSeriesIDSetCacheShrinkConservatism, 8, idle, zap.NewNop())
	require.Equal(t, int64(1<<20), c.maxCapacity, "setup must produce an adaptive cache")
	c.capacity.Store(int64(capacity))
	for i := 0; i < entries; i++ {
		c.Put([]byte("m"), []byte("k"), idleKey(i), tsdb.NewSeriesIDSet(uint64(i)))
	}
	require.Equal(t, int64(entries), c.stats.Size.Load(), "setup occupancy")
	return c
}

func idleGet(c *TagValueSeriesIDCache, i int) *tsdb.SeriesIDSet {
	return c.Get([]byte("m"), []byte("k"), idleKey(i))
}

func idleExists(c *TagValueSeriesIDCache, i int) bool {
	c.Lock()
	defer c.Unlock()
	return c.exists("m", "k", string(idleKey(i)))
}

// newIdleState returns sweeper state as of t0 with the cache's current Get count.
func newIdleState(c *TagValueSeriesIDCache, t0 time.Time) *idleSweepState {
	return &idleSweepState{lastGets: c.stats.Hits.Load() + c.stats.Misses.Load(), idleSince: t0}
}

// requireIdleSurvivors asserts exactly which keys are present and absent.
func requireIdleSurvivors(t *testing.T, c *TagValueSeriesIDCache, present, absent []int) {
	t.Helper()
	for _, v := range present {
		require.True(t, idleExists(c, v), "expected value %d to survive", v)
	}
	for _, v := range absent {
		require.False(t, idleExists(c, v), "expected value %d to be evicted", v)
	}
}

// requireIdleStep ticks at now and asserts a step from oldCap to newCap
// shedding evicted entries.
func requireIdleStep(t *testing.T, c *TagValueSeriesIDCache, st *idleSweepState, now time.Time, oldCap, newCap, evicted int64) {
	t.Helper()
	e, ok := c.idleTick(st, now)
	require.True(t, ok, "expected an idle step at %v", now)
	require.Equal(t, resizeEvent{oldCap: oldCap, newCap: newCap, evicted: evicted}, e)
	require.Equal(t, newCap, c.capacity.Load(), "capacity")
	c.Lock()
	n := int64(c.evictor.Len())
	c.Unlock()
	require.Equal(t, n, c.stats.Size.Load(), "stats.Size must track the list")
}

// requireNoIdleStep ticks at now and asserts nothing changed.
func requireNoIdleStep(t *testing.T, c *TagValueSeriesIDCache, st *idleSweepState, now time.Time) {
	t.Helper()
	capBefore, sizeBefore := c.capacity.Load(), c.stats.Size.Load()
	_, ok := c.idleTick(st, now)
	require.False(t, ok, "expected no idle step at %v", now)
	require.Equal(t, capBefore, c.capacity.Load(), "capacity")
	require.Equal(t, sizeBefore, c.stats.Size.Load(), "size")
}

var idleT0 = time.Unix(1_700_000_000, 0)

func TestTagValueSeriesIDCache_IdleTick_ActiveCacheNeverSteps(t *testing.T) {
	const D = time.Hour
	c := newIdleAdaptiveCache(t, 2, 10, 10, D)
	st := newIdleState(c, idleT0)

	// A Get between every pair of ticks keeps the cache active, however far
	// apart the ticks are.
	for k := 1; k <= 5; k++ {
		require.NotNil(t, idleGet(c, k))
		now := idleT0.Add(time.Duration(k) * D)
		requireNoIdleStep(t, c, st, now)
		require.Equal(t, now, st.idleSince, "an active tick restarts the silent stretch")
	}
	require.Equal(t, int64(10), c.capacity.Load())
	require.Zero(t, c.stats.IdleEvictions.Load())

	// Silence: no step until exactly D has passed.
	last := st.idleSince
	requireNoIdleStep(t, c, st, last.Add(D-time.Nanosecond))
	requireIdleStep(t, c, st, last.Add(D), 10, 5, 5)
}

func TestTagValueSeriesIDCache_IdleTick_InitialThenQuarterCadence_KeepsMRU(t *testing.T) {
	const D = time.Hour
	const Q = D / 4
	c := newIdleAdaptiveCache(t, 2, 10, 10, D)
	st := newIdleState(c, idleT0)

	// Touch {0,1,2}: list is front 2,1,0,9,8,7,6,5,4,3 back.
	for _, v := range []int{0, 1, 2} {
		require.NotNil(t, idleGet(c, v))
	}
	requireNoIdleStep(t, c, st, idleT0) // active
	for k := 1; k <= 3; k++ {
		requireNoIdleStep(t, c, st, idleT0.Add(time.Duration(k)*Q))
	}

	// Step 1 at D: min(10-2, 10/2) = 5 from the tail.
	requireIdleStep(t, c, st, idleT0.Add(D), 10, 5, 5)
	requireIdleSurvivors(t, c, []int{0, 1, 2, 8, 9}, []int{3, 4, 5, 6, 7})

	// Step 2 one quarter later: min(5-2, 5/2) = 2.
	requireIdleStep(t, c, st, idleT0.Add(D+Q), 5, 3, 2)
	requireIdleSurvivors(t, c, []int{0, 1, 2}, []int{3, 4, 5, 6, 7, 8, 9})

	// Step 3: min(3-2, 3/2) = 1; the LRU of the touched set goes.
	requireIdleStep(t, c, st, idleT0.Add(D+2*Q), 3, 2, 1)
	requireIdleSurvivors(t, c, []int{1, 2}, []int{0, 3, 4, 5, 6, 7, 8, 9})

	// At the floor: nothing more to do.
	requireNoIdleStep(t, c, st, idleT0.Add(D+3*Q))

	require.Equal(t, int64(8), c.stats.IdleEvictions.Load())
	require.Zero(t, c.stats.Evictions.Load(), "idle steps are not forced evictions")
	require.Zero(t, c.stats.ShrinkEvictions.Load(), "idle steps are not footprint shrinks")
	require.Equal(t, int64(2), c.stats.Size.Load())
}

func TestTagValueSeriesIDCache_IdleTick_SlackStepThenColdTail(t *testing.T) {
	const D = time.Hour
	core, logs := observer.New(zap.InfoLevel)
	c := newIdleAdaptiveCache(t, 4, 10, 6, D)
	c.SetLogger(zap.New(core))
	st := newIdleState(c, idleT0)

	// Step 1: slack, capacity 10 → occupancy 6, nothing evicted.
	requireIdleStep(t, c, st, idleT0.Add(D), 10, 6, 0)
	requireIdleSurvivors(t, c, []int{0, 1, 2, 3, 4, 5}, nil)
	require.Equal(t, 1, logs.FilterMessage(logMsgCacheIdleShrink).Len(), "the slack step is logged")

	// Step 2: cold tail, min(6-4, 6/2) = 2 from the tail.
	requireIdleStep(t, c, st, idleT0.Add(D+D/4), 6, 4, 2)
	requireIdleSurvivors(t, c, []int{2, 3, 4, 5}, []int{0, 1})
	require.Equal(t, int64(2), c.stats.IdleEvictions.Load())
}

func TestTagValueSeriesIDCache_IdleTick_SlackToFloorBelowOccupancy(t *testing.T) {
	const D = time.Hour
	c := newIdleAdaptiveCache(t, 4, 10, 3, D)
	st := newIdleState(c, idleT0)

	// Occupancy 3 is below the floor 4: capacity drops to the floor, and the
	// entries all stay.
	requireIdleStep(t, c, st, idleT0.Add(D), 10, 4, 0)
	requireIdleSurvivors(t, c, []int{0, 1, 2}, nil)
	requireNoIdleStep(t, c, st, idleT0.Add(D+D/4))
	require.Zero(t, c.stats.IdleEvictions.Load())
}

func TestTagValueSeriesIDCache_IdleTick_AbsoluteBound(t *testing.T) {
	const D = time.Hour
	const n = 20000
	c := newIdleAdaptiveCache(t, 2, n, n, D)
	st := newIdleState(c, idleT0)

	// Step 1: min(19998, 10000, 8192) — the absolute bound binds.
	requireIdleStep(t, c, st, idleT0.Add(D), n, n-maxShrinkEvictPerEvent, maxShrinkEvictPerEvent)
	for v := 0; v < n; v++ {
		require.Equal(t, v >= maxShrinkEvictPerEvent, idleExists(c, v), "value %d", v)
	}

	// Step 2: min(11806, 5904, 8192) — size/2 binds.
	const after1 = n - maxShrinkEvictPerEvent
	requireIdleStep(t, c, st, idleT0.Add(D+D/4), after1, after1-after1/2, after1/2)
	for v := 0; v < n; v++ {
		require.Equal(t, v >= maxShrinkEvictPerEvent+after1/2, idleExists(c, v), "value %d", v)
	}
	require.Equal(t, int64(maxShrinkEvictPerEvent+after1/2), c.stats.IdleEvictions.Load())
}

func TestTagValueSeriesIDCache_IdleTick_GetResetsClock(t *testing.T) {
	const D = time.Hour
	const Q = D / 4
	c := newIdleAdaptiveCache(t, 2, 10, 10, D)
	st := newIdleState(c, idleT0)

	requireIdleStep(t, c, st, idleT0.Add(D), 10, 5, 5)
	requireIdleSurvivors(t, c, []int{5, 6, 7, 8, 9}, []int{0, 1, 2, 3, 4})

	// A Get (between ticks, at ~D+Q/2) on the LRU survivor 5 promotes it.
	require.NotNil(t, idleGet(c, 5))
	requireNoIdleStep(t, c, st, idleT0.Add(D+Q))
	require.Equal(t, idleT0.Add(D+Q), st.idleSince, "the Get restarts the silent stretch")

	// Less than D of silence since the restart: no steps.
	for k := 2; k <= 4; k++ {
		requireNoIdleStep(t, c, st, idleT0.Add(D+time.Duration(k)*Q))
	}

	// Exactly D of silence: step 2 sheds the tail {6,7}, not the touched 5.
	requireIdleStep(t, c, st, idleT0.Add(2*D+Q), 5, 3, 2)
	requireIdleSurvivors(t, c, []int{5, 8, 9}, []int{6, 7})

	// Quarter cadence resumes.
	requireIdleStep(t, c, st, idleT0.Add(2*D+2*Q), 3, 2, 1)
	requireIdleSurvivors(t, c, []int{5, 9}, []int{8})
}

func TestTagValueSeriesIDCache_IdleTick_AddToSetIsNotActivity(t *testing.T) {
	const D = time.Hour
	const Q = D / 4
	c := newIdleAdaptiveCache(t, 2, 10, 10, D)
	st := newIdleState(c, idleT0)

	// Write-path maintenance of cached sets, including on the LRU entry 0,
	// neither counts as a read nor promotes the entry.
	for k := 1; k <= 3; k++ {
		c.Lock()
		c.addToSet([]byte("m"), []byte("k"), idleKey(0), uint64(1000+k))
		c.Unlock()
		c.Delete([]byte("m"), []byte("k"), idleKey(1), 1)
		requireNoIdleStep(t, c, st, idleT0.Add(time.Duration(k)*Q))
	}
	require.Equal(t, idleT0, st.idleSince, "addToSet/Delete must not restart the silent stretch")

	requireIdleStep(t, c, st, idleT0.Add(D), 10, 5, 5)
	requireIdleSurvivors(t, c, []int{5, 6, 7, 8, 9}, []int{0, 1, 2, 3, 4})
}

func TestTagValueSeriesIDCache_IdleTick_ResetsWindows(t *testing.T) {
	const D = time.Hour
	c := newIdleAdaptiveCache(t, 2, 10, 10, D)

	// Open a Get-driven shrink window (window length 10 > 3 Gets).
	for _, v := range []int{7, 8, 9} {
		require.NotNil(t, idleGet(c, v))
	}
	c.Lock()
	require.Positive(t, c.shrinkWindowGets, "precondition: a shrink window is open")
	// A stale boundary at the tail, and grow-window state that looks almost
	// ready to fire against old baselines.
	c.deepestTouched = c.evictor.Back()
	c.evictionsSinceCheck = 7
	c.lastHits, c.lastMisses = 99, 99
	c.Unlock()

	st := newIdleState(c, idleT0)
	requireIdleStep(t, c, st, idleT0.Add(D), 10, 5, 5)

	c.Lock()
	require.Nil(t, c.deepestTouched, "footprint boundary cleared")
	require.Zero(t, c.shrinkWindowGets, "Get-driven shrink window ended")
	require.Zero(t, c.evictionsSinceCheck, "grow window restarted")
	require.Equal(t, c.stats.Hits.Load(), c.lastHits)
	require.Equal(t, c.stats.Misses.Load(), c.lastMisses)
	require.Equal(t, adaptiveWindowLen(5, c.minSamples, c.targetHitRate), c.cooldownGets, "cooldown sized to the new capacity")
	c.Unlock()

	// Gets afterwards run the shrink window machinery cleanly.
	for i := 0; i < 50; i++ {
		idleGet(c, 5+i%5)
	}

	// Get-miss + Put on new keys regrows through the real grow policy.
	for i := 100; i < 160; i++ {
		require.Nil(t, idleGet(c, i))
		c.Put([]byte("m"), []byte("k"), idleKey(i), tsdb.NewSeriesIDSet(uint64(i)))
	}
	grown := c.capacity.Load()
	require.Greater(t, grown, int64(5), "the cache must regrow after an idle step")

	// A later silent stretch steps again. Growth doubles capacity ahead of
	// occupancy, so this first step is the slack branch.
	st = newIdleState(c, idleT0.Add(10*D))
	size := c.stats.Size.Load()
	require.Less(t, size, grown, "precondition: headroom after growth")
	requireIdleStep(t, c, st, idleT0.Add(11*D), grown, size, 0)
}

func TestTagValueSeriesIDCache_IdleTick_LogsOncePerStep(t *testing.T) {
	const D = time.Hour
	const Q = D / 4
	core, logs := observer.New(zap.DebugLevel)
	c := newIdleAdaptiveCache(t, 2, 10, 10, D)
	c.SetLogger(zap.New(core))
	st := newIdleState(c, idleT0)

	require.NotNil(t, idleGet(c, 9))
	requireNoIdleStep(t, c, st, idleT0) // active: no log
	requireNoIdleStep(t, c, st, idleT0.Add(Q))
	require.Zero(t, logs.Len(), "no log for active or quiet ticks")

	type step struct{ oldCap, newCap, evicted int64 }
	want := []step{{10, 5, 5}, {5, 3, 2}, {3, 2, 1}}
	for k, w := range want {
		requireIdleStep(t, c, st, idleT0.Add(D+time.Duration(k)*Q), w.oldCap, w.newCap, w.evicted)
	}
	requireNoIdleStep(t, c, st, idleT0.Add(D+3*Q)) // at the floor: no log

	entries := logs.All()
	require.Len(t, entries, len(want), "one log line per step")
	for k, w := range want {
		require.Equal(t, logMsgCacheIdleShrink, entries[k].Message)
		require.Equal(t, zap.InfoLevel, entries[k].Level)
		require.Equal(t, map[string]interface{}{
			"old_capacity": w.oldCap,
			"new_capacity": w.newCap,
			"min_capacity": int64(2),
			"evicted":      w.evicted,
			"idle_timeout": D,
		}, entries[k].ContextMap())
	}
}

func TestTagValueSeriesIDCache_IdleTick_DisabledIsFree(t *testing.T) {
	for name, c := range map[string]*TagValueSeriesIDCache{
		"adaptive with idleTimeout 0": newIdleAdaptiveCache(t, 2, 10, 10, 0),
		"fixed-size":                  NewTagValueSeriesIDCache(10),
	} {
		t.Run(name, func(t *testing.T) {
			c.capacity.Store(10)
			c.startIdleSweeper()
			c.Lock()
			require.Nil(t, c.sweepClosing, "no sweeper may start when disabled")
			c.Unlock()
			c.stopIdleSweeper() // no-op

			st := newIdleState(c, idleT0)
			requireNoIdleStep(t, c, st, idleT0.Add(time.Hour))
			require.Zero(t, c.stats.IdleEvictions.Load())
		})
	}
}

func TestTagValueSeriesIDCache_IdleSweeper_StartStop(t *testing.T) {
	const D = 40 * time.Millisecond // tick every 10ms
	c := newIdleAdaptiveCache(t, 2, 8, 8, D)

	settled := func() bool { return c.capacity.Load() == 2 && c.stats.Size.Load() == 2 }

	c.startIdleSweeper()
	c.Lock()
	first := c.sweepClosing
	c.Unlock()
	require.NotNil(t, first, "sweeper must start")
	c.startIdleSweeper() // second start is a no-op
	c.Lock()
	require.True(t, first == c.sweepClosing, "second start must not replace the running sweeper")
	c.Unlock()

	// Two steps: 8 → 4 → 2.
	require.Eventually(t, settled, 2*time.Second, 5*time.Millisecond)
	requireIdleSurvivors(t, c, []int{6, 7}, []int{0, 1, 2, 3, 4, 5})

	c.stopIdleSweeper()
	c.stopIdleSweeper() // idempotent
	c.Lock()
	require.Nil(t, c.sweepClosing)
	require.Nil(t, c.sweepDone)
	c.Unlock()

	// Refill while stopped, then restart: the cache decays again.
	c.Lock()
	c.capacity.Store(8)
	c.Unlock()
	for i := 100; i < 106; i++ {
		c.Put([]byte("m"), []byte("k"), idleKey(i), tsdb.NewSeriesIDSet(uint64(i)))
	}
	require.Equal(t, int64(8), c.stats.Size.Load())
	c.startIdleSweeper()
	defer c.stopIdleSweeper()
	require.Eventually(t, settled, 2*time.Second, 5*time.Millisecond)
	requireIdleSurvivors(t, c, []int{104, 105}, []int{6, 7, 100, 101, 102, 103})
	require.Equal(t, int64(12), c.stats.IdleEvictions.Load())
}

// TestTagValueSeriesIDCache_IdleTick_GetBeforeLockAbandonsStep blocks idleTick
// on the cache lock, lands a Get while holding it, and checks the tick abandons
// the step and restarts the silent stretch. If the tick goroutine has not yet
// reached the lock when the Get lands, the lockless check catches the Get
// instead; the outcome is the same either way.
func TestTagValueSeriesIDCache_IdleTick_GetBeforeLockAbandonsStep(t *testing.T) {
	const D = time.Hour
	c := newIdleAdaptiveCache(t, 2, 10, 10, D)
	st := newIdleState(c, idleT0)
	now := idleT0.Add(D)

	type result struct {
		e  resizeEvent
		ok bool
	}
	res := make(chan result, 1)
	c.Lock()
	go func() {
		e, ok := c.idleTick(st, now)
		res <- result{e, ok}
	}()
	time.Sleep(20 * time.Millisecond) // let the tick pass its lockless checks and block
	_, hit := c.get([]byte("m"), []byte("k"), idleKey(9))
	require.True(t, hit)
	c.Unlock()

	r := <-res
	require.False(t, r.ok, "a Get after the decision must abandon the step")
	require.Equal(t, resizeEvent{}, r.e)
	require.Equal(t, int64(10), c.capacity.Load(), "capacity")
	require.Equal(t, int64(10), c.stats.Size.Load(), "size")
	require.Zero(t, c.stats.IdleEvictions.Load())
	require.Equal(t, now, st.idleSince, "the silent stretch restarts at the tick")
	require.Equal(t, c.stats.Hits.Load()+c.stats.Misses.Load(), st.lastGets)

	// Silence from here steps D after the restarted stretch, not before.
	requireNoIdleStep(t, c, st, now.Add(D-time.Nanosecond))
	requireIdleStep(t, c, st, now.Add(D), 10, 5, 5)
}

// TestTagValueSeriesIDCache_IdleSweeper_StopIgnoresLaterStart races a stop
// against a start. The stop must return once the sweeper it stopped exits, even
// when the start lands after the stop's swap and its sweeper stays running.
func TestTagValueSeriesIDCache_IdleSweeper_StopIgnoresLaterStart(t *testing.T) {
	const rounds = 1000
	c := newIdleAdaptiveCache(t, 2, 8, 8, time.Hour) // never ticks during the test

	for r := 0; r < rounds; r++ {
		c.startIdleSweeper()

		var mu sync.RWMutex
		var wg sync.WaitGroup
		stopped := make(chan struct{})
		mu.Lock()
		wg.Add(2)
		go func() {
			mu.RLock()
			defer mu.RUnlock()
			defer wg.Done()
			c.stopIdleSweeper()
			close(stopped)
		}()
		go func() {
			mu.RLock()
			defer mu.RUnlock()
			defer wg.Done()
			c.startIdleSweeper()
		}()
		mu.Unlock() // start both at once

		select {
		case <-stopped:
		case <-time.After(5 * time.Second):
			require.FailNow(t, "stopIdleSweeper waited on a sweeper it did not stop", "round %d", r)
		}
		wg.Wait()

		// Whichever order they ran in, one stop leaves nothing running.
		c.stopIdleSweeper()
		c.Lock()
		require.Nil(t, c.sweepClosing, "round %d", r)
		require.Nil(t, c.sweepDone, "round %d", r)
		c.Unlock()
	}
}

// TestTagValueSeriesIDCache_IdleSweeper_Concurrent interleaves idle shrink
// steps (direct, and through idleTick's lockless decision) with
// Get/Put/Delete/addToSet traffic and the lockless Statistics reader under the
// race detector, then checks the cache's structural invariants.
func TestTagValueSeriesIDCache_IdleSweeper_Concurrent(t *testing.T) {
	const (
		floor   = 4
		maxCap  = 256
		keys    = 600 // > maxCap, so Puts keep evicting and growing
		workers = 4   // each of Get, Put, Delete, addToSet
		iters   = 20000
	)
	c := NewAdaptiveTagValueSeriesIDCache(floor, maxCap, 0.9, tsdb.DefaultSeriesIDSetCacheShrinkConservatism, 16, time.Hour, zap.NewNop())
	require.Equal(t, int64(maxCap), c.maxCapacity)
	name, key := []byte("m"), []byte("k")

	// Start grown and full so the first idle step sheds entries whatever the
	// scheduler's interleaving (at -cpu=1 the steppers may run before any Put).
	c.capacity.Store(maxCap)
	for i := 0; i < maxCap; i++ {
		c.Put(name, key, idleKey(i), tsdb.NewSeriesIDSet(uint64(i)))
	}
	require.Equal(t, int64(maxCap), c.stats.Size.Load(), "setup occupancy")

	var tickSteps, tickAbandoned atomic.Int64
	bodies := []func(i int){
		func(i int) { _ = c.Get(name, key, idleKey(i%keys)) },
		func(i int) { c.Put(name, key, idleKey((i*7)%keys), tsdb.NewSeriesIDSet(uint64(i))) },
		func(i int) { c.Delete(name, key, idleKey((i*3)%keys), uint64(i)) },
		func(i int) {
			c.Lock()
			c.addToSet(name, key, idleKey((i*5)%keys), uint64(i))
			c.Unlock()
		},
		// Idle steps, bypassing the idle decision so they interleave with traffic.
		func(int) {
			c.Lock()
			e, ok := c.idleShrinkLocked()
			c.Unlock()
			if ok {
				c.logIdleShrink(e)
			}
		},
		// Idle ticks through the full decision: state that is already idleTimeout
		// silent as of the current Get count, so the lockless checks pass unless a
		// Get lands first, and the re-check under the lock races the Get workers.
		// The state is per call because idleSweepState is single-owner.
		func(int) {
			st := newIdleState(c, idleT0)
			now := idleT0.Add(time.Hour)
			switch _, ok := c.idleTick(st, now); {
			case ok:
				tickSteps.Add(1)
			case st.idleSince.Equal(now):
				tickAbandoned.Add(1) // a Get moved the counter before or under the lock
			}
		},
		func(int) { _ = c.Statistics(nil) },
	}
	total := len(bodies) * workers

	// Barrier: every goroutine increments concurrency before arriving and
	// decrements only after its work, so the high-water mark is exactly total.
	var concurrency, maxConcurrency atomic.Int64
	var arrived, wg sync.WaitGroup
	proceed := make(chan struct{})
	arrived.Add(total)
	for _, body := range bodies {
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				n := concurrency.Add(1)
				for {
					old := maxConcurrency.Load()
					if n <= old || maxConcurrency.CompareAndSwap(old, n) {
						break
					}
				}
				arrived.Done()
				<-proceed
				for i := 0; i < iters; i++ {
					body(i)
				}
				concurrency.Add(-1)
			}()
		}
	}
	arrived.Wait()
	close(proceed)
	wg.Wait()
	t.Logf("max concurrency: %d", maxConcurrency.Load())
	require.Equal(t, int64(total), maxConcurrency.Load(), "all goroutines must overlap")
	t.Logf("idleTick: %d steps, %d abandoned on a moved Get counter", tickSteps.Load(), tickAbandoned.Load())

	c.Lock()
	defer c.Unlock()
	listLen := int64(c.evictor.Len())
	var mapLen int64
	for _, mmap := range c.cache {
		for _, tkmap := range mmap {
			mapLen += int64(len(tkmap))
		}
	}
	require.Equal(t, listLen, c.stats.Size.Load(), "stats.Size must track the list")
	require.Equal(t, listLen, mapLen, "map-reachable elements must equal the list length")
	require.LessOrEqual(t, listLen, c.capacity.Load(), "size <= capacity")
	require.GreaterOrEqual(t, c.capacity.Load(), int64(floor), "capacity >= floor")
	require.LessOrEqual(t, c.capacity.Load(), int64(maxCap), "capacity <= max")
	require.Positive(t, c.stats.IdleEvictions.Load(), "idle steps must have shed entries")
}
