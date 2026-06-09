package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/influxdata/influxdb/tsdb"
	"github.com/stretchr/testify/require"
)

// TestServer_AdaptiveTSICache_GrowAndShrink exercises the adaptive TSI tag-value
// series-ID cache end to end through the real write + query path, starting from
// the default initial cache size and driving repeated growth and shrinkage with
// high-cardinality data. Behavior is verified through the operator-facing
// SHOW STATS surface (the tsi1_cache measurement, including the capacity and
// shrink_eviction fields this feature adds).
//
// The cache is a tsi1-only feature that must be enabled via config, so the test
// configures and runs its own in-process server; it skips on inmem, remote
// servers, and -short.
func TestServer_AdaptiveTSICache_GrowAndShrink(t *testing.T) {
	t.Parallel()

	if RemoteEnabled() {
		t.Skip("Skipping. Needs an in-process server configured with the adaptive cache")
	}
	if indexType != tsdb.TSI1IndexName {
		t.Skip("Skipping. Adaptive TSI series-ID cache is a tsi1-only feature")
	}
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}

	const (
		// N distinct host tag values. N must comfortably exceed maxCap so the
		// working set never fits the cache and capacity pins at the max during
		// growth.
		N = 2000

		floorCap = 100 // initial cache size == floor (minCapacity)
		maxCap   = 800 // capacity doubles 100 -> 200 -> 400 -> 800

		// Growth driver. /.+/ matches every host but NOT the empty string, so it
		// routes to matchTagValueEqualNotEmptySeriesIDIterator, which does one
		// cache Get/Put per matching tag value (driving misses and evictions).
		// NOTE: /.*/ matches the empty string and would route to the
		// equal-empty path, which skips the per-value cache entirely -- it would
		// silently never exercise the cache. Do not use /.*/ here.
		growthQuery = `SELECT count(value) FROM db0.rp0.cpu WHERE host =~ /.+/`

		// Shrink driver. The highest-sorted hosts (server001950..server001999)
		// are the most-recently-Put values and are therefore guaranteed resident
		// after growth, so every Get is a pure hit (no forced evictions in the
		// shrink window). 50 values is well under floorCap, so the warm set stays
		// pinned at the front of the LRU and the untouched cold tail is shed.
		warmQuery = `SELECT count(value) FROM db0.rp0.cpu WHERE host =~ /server0019[5-9][0-9]/`
	)

	c := NewConfig()
	c.Data.SeriesIDSetCacheSize = floorCap          // default small initial size
	c.Data.SeriesIDSetCacheMaxSize = maxCap         // enables adaptive sizing
	c.Data.SeriesIDSetCacheTargetHitRate = 0.5      // keeps adaptive windows ~= occupancy
	c.Data.SeriesIDSetCacheShrinkConservatism = 2.5 // default; not relied upon (windows are pure-hit)
	require.NoError(t, c.Data.Validate(), "adaptive cache config must validate")

	s := OpenDefaultServer(c)
	defer s.Close()

	// Write N high-cardinality series at a single timestamp so all points land
	// in one shard (one cache). Zero-pad host numbers so lexical order matches
	// numeric order, which is what makes the warm-set choice above deterministic.
	ts := mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()
	var buf bytes.Buffer
	for i := 0; i < N; i++ {
		fmt.Fprintf(&buf, "cpu,host=server%06d value=100 %d\n", i, ts)
	}
	s.MustWrite("db0", "rp0", buf.String(), nil)

	// Sanity: a single shard means a single tsi1_cache, so the stat deltas below
	// describe one cache rather than an aggregate of several.
	require.Len(t, s.(*LocalServer).Server.TSDBStore.ShardIDs(), 1,
		"all writes should land in a single shard")

	// Two grow->shrink cycles demonstrate repeated growth and shrinkage. Each
	// cycle captures the cumulative eviction/shrink_eviction counters at its
	// start and asserts the per-cycle delta is positive.
	for cycle := 0; cycle < 2; cycle++ {
		// ---- GROWTH ----
		capBefore, _, hitBefore, missBefore, evBefore, _ := tsiCacheStats(t, s)

		for i := 0; i < 3; i++ {
			_, err := s.Query(growthQuery)
			require.NoErrorf(t, err, "cycle %d: growth query", cycle)
		}

		peakCap, _, hitAfter, missAfter, evAfter, _ := tsiCacheStats(t, s)

		// Prove the cache was actually exercised before trusting capacity moves;
		// this fails loudly if the query ever stops pushing the tag predicate
		// down to the tag-value cache.
		require.Greaterf(t, hitAfter+missAfter, hitBefore+missBefore,
			"cycle %d: cache must be exercised (hits+misses should increase)", cycle)
		require.Greaterf(t, peakCap, capBefore,
			"cycle %d: capacity should grow above %d", cycle, capBefore)
		require.LessOrEqualf(t, peakCap, int64(maxCap),
			"cycle %d: capacity must not exceed the configured max", cycle)
		require.Greaterf(t, evAfter, evBefore,
			"cycle %d: forced evictions should increase", cycle)
		require.Greaterf(t, missAfter, missBefore,
			"cycle %d: cache misses should increase", cycle)

		// ---- SHRINK (poll until observed) ----
		// A shrink needs a full window of pure-hit Gets to elapse after the
		// post-grow cooldown drains. The exact number of Gets the planner issues
		// per query is not predictable, so poll until the capacity drops and a
		// shrink eviction is recorded, rather than hard-coding counts. Warm
		// queries are batched between SHOW STATS reads because each SHOW STATS
		// recomputes database cardinality.
		_, _, _, _, _, shrinkBefore := tsiCacheStats(t, s)

		var capNow, shrinkNow int64
		const maxBatches = 200
		for batch := 0; batch < maxBatches; batch++ {
			for i := 0; i < 20; i++ {
				_, err := s.Query(warmQuery)
				require.NoErrorf(t, err, "cycle %d: warm query", cycle)
			}
			capNow, _, _, _, _, shrinkNow = tsiCacheStats(t, s)
			if capNow < peakCap && shrinkNow > shrinkBefore {
				break
			}
		}

		require.Lessf(t, capNow, peakCap,
			"cycle %d: capacity should shrink below the grown peak %d", cycle, peakCap)
		require.GreaterOrEqualf(t, capNow, int64(floorCap),
			"cycle %d: capacity must not drop below the floor", cycle)
		require.Greaterf(t, shrinkNow, shrinkBefore,
			"cycle %d: shrink_eviction should increase", cycle)
	}
}

// tsiCacheStats runs SHOW STATS and returns the aggregated tsi1_cache counters.
// Rows are summed across shards (one is expected). The measurement and field
// names are hardcoded because they are unexported in package tsi1. JSON numbers
// decode into interface{} as float64, so they are coerced back to int64.
func tsiCacheStats(t *testing.T, s Server) (capacity, size, hit, miss, evict, shrinkEvict int64) {
	t.Helper()

	raw, err := s.Query("SHOW STATS")
	require.NoError(t, err)

	var resp struct {
		Results []struct {
			Series []struct {
				Name    string          `json:"name"`
				Columns []string        `json:"columns"`
				Values  [][]interface{} `json:"values"`
			} `json:"series"`
		} `json:"results"`
	}
	require.NoError(t, json.Unmarshal([]byte(raw), &resp))

	field := func(cols []string, row []interface{}, name string) int64 {
		for i, c := range cols {
			if c == name && i < len(row) {
				if f, ok := row[i].(float64); ok {
					return int64(f)
				}
			}
		}
		return 0
	}

	var found bool
	for _, res := range resp.Results {
		for _, ser := range res.Series {
			if ser.Name != "tsi1_cache" || len(ser.Values) == 0 {
				continue
			}
			found = true
			row := ser.Values[0]
			capacity += field(ser.Columns, row, "capacity")
			size += field(ser.Columns, row, "size")
			hit += field(ser.Columns, row, "hit")
			miss += field(ser.Columns, row, "miss")
			evict += field(ser.Columns, row, "eviction")
			shrinkEvict += field(ser.Columns, row, "shrink_eviction")
		}
	}
	require.True(t, found, "SHOW STATS did not surface a tsi1_cache row")
	return
}
