package tsi1

import (
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
		statTagValueCacheHit:      int64(0),
		statTagValueCacheMiss:     int64(0),
		statTagValueCacheEviction: int64(0),
		statTagValueCacheSize:     int64(0),
		statTagValueCacheCapacity: int64(2),
	}, statValues(cache))

	// Miss on absent key.
	require.Nil(t, cache.Get([]byte("m0"), []byte("k0"), []byte("v0")))
	require.Equal(t, map[string]interface{}{
		statTagValueCacheHit:      int64(0),
		statTagValueCacheMiss:     int64(1),
		statTagValueCacheEviction: int64(0),
		statTagValueCacheSize:     int64(0),
		statTagValueCacheCapacity: int64(2),
	}, statValues(cache))

	// Put, then Get the same key → one hit, size 1.
	s0 := tsdb.NewSeriesIDSet(1)
	cache.Put([]byte("m0"), []byte("k0"), []byte("v0"), s0)
	require.True(t, cache.Get([]byte("m0"), []byte("k0"), []byte("v0")).Equals(s0))
	require.Equal(t, map[string]interface{}{
		statTagValueCacheHit:      int64(1),
		statTagValueCacheMiss:     int64(1),
		statTagValueCacheEviction: int64(0),
		statTagValueCacheSize:     int64(1),
		statTagValueCacheCapacity: int64(2),
	}, statValues(cache))

	// Add a second entry to fill the cache.
	s1 := tsdb.NewSeriesIDSet(2)
	cache.Put([]byte("m0"), []byte("k0"), []byte("v1"), s1)
	require.Equal(t, int64(2), statValues(cache)[statTagValueCacheSize])
	require.Equal(t, int64(0), statValues(cache)[statTagValueCacheEviction])

	// Adding a third distinct entry must evict the least-recently-used.
	// LRU at this point is v0: it was Put first, Get-promoted to MRU, then
	// v1 was Put (making v1 MRU and v0 LRU).
	s2 := tsdb.NewSeriesIDSet(3)
	cache.Put([]byte("m0"), []byte("k0"), []byte("v2"), s2)
	require.Equal(t, map[string]interface{}{
		statTagValueCacheHit:      int64(1),
		statTagValueCacheMiss:     int64(1),
		statTagValueCacheEviction: int64(1),
		statTagValueCacheSize:     int64(2),
		statTagValueCacheCapacity: int64(2),
	}, statValues(cache))
	// v0 was evicted; v1 and v2 must survive.
	require.Nil(t, cache.get([]byte("m0"), []byte("k0"), []byte("v0")))
	require.True(t, cache.get([]byte("m0"), []byte("k0"), []byte("v1")).Equals(s1))
	require.True(t, cache.get([]byte("m0"), []byte("k0"), []byte("v2")).Equals(s2))
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

	require.True(t, cache.get([]byte("m"), []byte("k"), []byte("v0")).Equals(s0),
		"recently-touched key v0 should not have been evicted")
	require.Nil(t, cache.get([]byte("m"), []byte("k"), []byte("v1")),
		"true LRU key v1 should have been evicted")
	require.True(t, cache.get([]byte("m"), []byte("k"), []byte("v2")).Equals(s2),
		"newly inserted key v2 should be present")

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
	cache := NewAdaptiveTagValueSeriesIDCache(2, 16, 0.99, 4, logger)

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
	cache := NewAdaptiveTagValueSeriesIDCache(2, 16, 0.01, 4, logger)

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
