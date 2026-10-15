package tsi1

import (
	"strings"
	"testing"

	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestTagValueSeriesIDCache_Metrics(t *testing.T) {
	globalCacheRegistry.mu.Lock()
	globalCacheRegistry.caches = make(map[*TagValueSeriesIDCache]prometheus.Labels)
	globalCacheRegistry.mu.Unlock()

	t.Cleanup(func() {
		globalCacheRegistry.mu.Lock()
		globalCacheRegistry.caches = make(map[*TagValueSeriesIDCache]prometheus.Labels)
		globalCacheRegistry.mu.Unlock()
	})

	tags1 := tsdb.EngineTags{
		Bucket: "db1",
		Id:     "shard1",
		Path:   "/path1",
	}

	tags2 := tsdb.EngineTags{
		Bucket: "db2",
		Id:     "shard2",
		Path:   "/path2",
	}

	c1 := NewTagValueSeriesIDCache(10)
	c2 := NewTagValueSeriesIDCache(20)

	globalCacheRegistry.register(c1, tags1)
	globalCacheRegistry.register(c2, tags2)

	c1.stats.Hits.Add(5)
	c1.stats.Misses.Add(15)
	c1.stats.Evictions.Add(2)
	c1.stats.ShrinkEvictions.Add(1)
	c1.stats.Size.Store(8)
	c1.capacity.Store(10)

	c2.stats.Hits.Add(50)
	c2.stats.Misses.Add(150)
	c2.stats.Evictions.Add(20)
	c2.stats.ShrinkEvictions.Add(10)
	c2.stats.Size.Store(18)
	c2.capacity.Store(20)

	expected := `
# HELP storage_tsi1_cache_capacity Current capacity of the TagValueSeriesIDCache
# TYPE storage_tsi1_cache_capacity gauge
storage_tsi1_cache_capacity{bucket="db1",engine="",id="shard1",path="/path1",walPath=""} 10
storage_tsi1_cache_capacity{bucket="db2",engine="",id="shard2",path="/path2",walPath=""} 20
# HELP storage_tsi1_cache_evictions_total Total number of forced evictions in the TagValueSeriesIDCache
# TYPE storage_tsi1_cache_evictions_total counter
storage_tsi1_cache_evictions_total{bucket="db1",engine="",id="shard1",path="/path1",walPath=""} 2
storage_tsi1_cache_evictions_total{bucket="db2",engine="",id="shard2",path="/path2",walPath=""} 20
# HELP storage_tsi1_cache_hits_total Total number of TagValueSeriesIDCache hits
# TYPE storage_tsi1_cache_hits_total counter
storage_tsi1_cache_hits_total{bucket="db1",engine="",id="shard1",path="/path1",walPath=""} 5
storage_tsi1_cache_hits_total{bucket="db2",engine="",id="shard2",path="/path2",walPath=""} 50
# HELP storage_tsi1_cache_misses_total Total number of TagValueSeriesIDCache misses
# TYPE storage_tsi1_cache_misses_total counter
storage_tsi1_cache_misses_total{bucket="db1",engine="",id="shard1",path="/path1",walPath=""} 15
storage_tsi1_cache_misses_total{bucket="db2",engine="",id="shard2",path="/path2",walPath=""} 150
# HELP storage_tsi1_cache_shrink_evictions_total Total number of shrink evictions in the TagValueSeriesIDCache
# TYPE storage_tsi1_cache_shrink_evictions_total counter
storage_tsi1_cache_shrink_evictions_total{bucket="db1",engine="",id="shard1",path="/path1",walPath=""} 1
storage_tsi1_cache_shrink_evictions_total{bucket="db2",engine="",id="shard2",path="/path2",walPath=""} 10
# HELP storage_tsi1_cache_size Current number of elements in the TagValueSeriesIDCache
# TYPE storage_tsi1_cache_size gauge
storage_tsi1_cache_size{bucket="db1",engine="",id="shard1",path="/path1",walPath=""} 8
storage_tsi1_cache_size{bucket="db2",engine="",id="shard2",path="/path2",walPath=""} 18
`

	err := testutil.CollectAndCompare(
		globalCacheRegistry,
		strings.NewReader(expected),
	)

	if err != nil {
		t.Fatalf("metrics mismatch: %v", err)
	}

	globalCacheRegistry.deregister(c1)

	expectedAfter := `
# HELP storage_tsi1_cache_capacity Current capacity of the TagValueSeriesIDCache
# TYPE storage_tsi1_cache_capacity gauge
storage_tsi1_cache_capacity{bucket="db2",engine="",id="shard2",path="/path2",walPath=""} 20
# HELP storage_tsi1_cache_evictions_total Total number of forced evictions in the TagValueSeriesIDCache
# TYPE storage_tsi1_cache_evictions_total counter
storage_tsi1_cache_evictions_total{bucket="db2",engine="",id="shard2",path="/path2",walPath=""} 20
# HELP storage_tsi1_cache_hits_total Total number of TagValueSeriesIDCache hits
# TYPE storage_tsi1_cache_hits_total counter
storage_tsi1_cache_hits_total{bucket="db2",engine="",id="shard2",path="/path2",walPath=""} 50
# HELP storage_tsi1_cache_misses_total Total number of TagValueSeriesIDCache misses
# TYPE storage_tsi1_cache_misses_total counter
storage_tsi1_cache_misses_total{bucket="db2",engine="",id="shard2",path="/path2",walPath=""} 150
# HELP storage_tsi1_cache_shrink_evictions_total Total number of shrink evictions in the TagValueSeriesIDCache
# TYPE storage_tsi1_cache_shrink_evictions_total counter
storage_tsi1_cache_shrink_evictions_total{bucket="db2",engine="",id="shard2",path="/path2",walPath=""} 10
# HELP storage_tsi1_cache_size Current number of elements in the TagValueSeriesIDCache
# TYPE storage_tsi1_cache_size gauge
storage_tsi1_cache_size{bucket="db2",engine="",id="shard2",path="/path2",walPath=""} 18
`

	err = testutil.CollectAndCompare(
		globalCacheRegistry,
		strings.NewReader(expectedAfter),
	)

	if err != nil {
		t.Fatalf("metrics mismatch after deregister: %v", err)
	}
}
