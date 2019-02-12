package tsi1

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb/tsdb"
)

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
