package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeVars mirrors the shape influxd's /debug/vars actually emits: statistics
// under synthesized "<name>:<path>:<id>" keys, mixed with non-statistic keys
// that must be skipped, and Go memstats.
func fakeVars(shards int, seriesPerShard int64, hit, miss, evict, size, capacity int64) string {
	out := map[string]any{
		"build":    map[string]any{"Version": "1.12.0"},
		"cmdline":  []string{"influxd"},
		"memstats": map[string]any{"HeapInuse": 18 << 30, "Sys": 24 << 30},
		// Up for 30 days: longer than any window the tests use, so the
		// window-versus-uptime check stays quiet unless a test shortens it.
		"system": map[string]any{
			"currentTime": "2026-09-16T00:00:00Z",
			"started":     "2026-08-17T00:00:00Z",
			"uptime":      "720h0m0s",
		},
	}
	for i := range shards {
		id := fmt.Sprintf("%d", i+1)
		path := "/var/lib/influxdb/data/telemetry/autogen/" + id
		tags := map[string]string{
			"id":              id,
			"database":        "telemetry",
			"retentionPolicy": "autogen",
			"path":            path,
			"indexType":       "tsi1",
		}
		out["tsi1_cache:"+path+":"+id] = map[string]any{
			"name": "tsi1_cache",
			"tags": tags,
			"values": map[string]any{
				"hit": hit, "miss": miss, "eviction": evict,
				"shrink_eviction": 0, "size": size, "capacity": capacity,
			},
		}
		out["shard:"+path+":"+id] = map[string]any{
			"name": "shard",
			"tags": tags,
			"values": map[string]any{
				"seriesCreate": seriesPerShard,
				"diskBytes":    1 << 30,
			},
		}
	}
	b, _ := json.Marshal(out)
	return string(b)
}

// withCacheBytes adds the "bytes" field to every tsi1_cache statistic in a
// /debug/vars payload, simulating a server new enough to publish the gauge.
// Servers without it simply omit the field, which is the fallback path
// TestProbeEndToEnd covers.
func withCacheBytes(vars string, bytesPerShard int64) string {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(vars), &raw); err != nil {
		panic(err)
	}
	for k, v := range raw {
		if !strings.HasPrefix(k, "tsi1_cache") {
			continue
		}
		var s map[string]any
		if err := json.Unmarshal(v, &s); err != nil {
			panic(err)
		}
		s["values"].(map[string]any)["bytes"] = bytesPerShard
		b, err := json.Marshal(s)
		if err != nil {
			panic(err)
		}
		raw[k] = b
	}
	b, err := json.Marshal(raw)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func seriesResponse(columns []string, rows ...[]any) string {
	type series struct {
		Name    string            `json:"name"`
		Tags    map[string]string `json:"tags,omitempty"`
		Columns []string          `json:"columns"`
		Values  [][]any           `json:"values"`
	}
	vals := make([][]any, 0, len(rows))
	vals = append(vals, rows...)
	b, _ := json.Marshal(map[string]any{
		"results": []map[string]any{{
			"series": []series{{Name: "x", Columns: columns, Values: vals}},
		}},
	})
	return string(b)
}

// taggedSeriesResponse emits one series per shard id, the shape a
// GROUP BY "id" query returns. peaks maps shard id to its value.
func taggedSeriesResponse(columns []string, peaks map[string]string) string {
	type series struct {
		Name    string            `json:"name"`
		Tags    map[string]string `json:"tags,omitempty"`
		Columns []string          `json:"columns"`
		Values  [][]any           `json:"values"`
	}
	ids := make([]string, 0, len(peaks))
	for id := range peaks {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	all := make([]series, 0, len(ids))
	for _, id := range ids {
		v, _ := strconv.ParseFloat(peaks[id], 64)
		all = append(all, series{
			Name:    "tsi1_cache",
			Tags:    map[string]string{"id": id},
			Columns: columns,
			Values:  [][]any{{"2026-09-16T00:00:00Z", v}},
		})
	}
	b, _ := json.Marshal(map[string]any{
		"results": []map[string]any{{"series": all}},
	})
	return string(b)
}

// taggedActivityResponse emits one hit/miss/evict series per shard id, the
// shape the per-shard activity query returns.
func taggedActivityResponse(byShard map[string][3]float64) string {
	type series struct {
		Name    string            `json:"name"`
		Tags    map[string]string `json:"tags,omitempty"`
		Columns []string          `json:"columns"`
		Values  [][]any           `json:"values"`
	}
	ids := make([]string, 0, len(byShard))
	for id := range byShard {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	all := make([]series, 0, len(ids))
	for _, id := range ids {
		v := byShard[id]
		all = append(all, series{
			Name:    "tsi1_cache",
			Tags:    map[string]string{"id": id},
			Columns: []string{"time", "hit", "miss", "evict"},
			Values:  [][]any{{"2026-09-16T00:00:00Z", v[0], v[1], v[2]}},
		})
	}
	b, _ := json.Marshal(map[string]any{
		"results": []map[string]any{{"series": all}},
	})
	return string(b)
}

// newFakeInstance serves the endpoints the tool reads. Query responses are
// selected by matching on the statement text, the same way an operator would
// recognize them.
func newFakeInstance(t *testing.T, vars string) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("/debug/vars", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, vars)
	})
	mux.HandleFunc("/query", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("q")
		w.Header().Set("Content-Type", "application/json")

		switch {
		case strings.Contains(q, `"runtime"`):
			// Hourly heap peaks, ascending, so the p95 is near the top.
			rows := make([][]any, 0, 100)
			for i := range 100 {
				rows = append(rows, []any{"2026-09-16T00:00:00Z", float64(10+i) * (1 << 30) / 100})
			}
			fmt.Fprint(w, seriesResponse([]string{"time", "max"}, rows...))

		case strings.Contains(q, `sum("d_hit")`) && strings.HasSuffix(strings.TrimSpace(q), `GROUP BY "id"`):
			// The per-shard form of the activity query. Shard 1 is busy and
			// meeting any sane target; shard 3 is busy and below 0.99.
			fmt.Fprint(w, taggedActivityResponse(map[string][3]float64{
				"1": {590000, 10000, 9000},
				"3": {9500, 500, 400},
			}))

		case strings.Contains(q, `sum("d_hit")`):
			fmt.Fprint(w, seriesResponse(
				[]string{"time", "hit", "miss", "evict"},
				[]any{"2026-09-16T00:00:00Z", 600000.0, 400000.0, 350000.0}))

		case strings.Contains(q, `max("size")`):
			// GROUP BY "id" puts the shard id in the series tags, which is how
			// PeakShardSizes attributes a peak to a shard.
			fmt.Fprint(w, taggedSeriesResponse(
				[]string{"time", "size"},
				map[string]string{"1": "100", "2": "0", "3": "740"}))

		case strings.Contains(q, "SHOW SERIES CARDINALITY"):
			fmt.Fprint(w, seriesResponse([]string{"cardinality estimation"}, []any{1000000.0}))

		case strings.Contains(q, "SHOW TAG KEYS"):
			fmt.Fprint(w, seriesResponse([]string{"tagKey"},
				[]any{"host"}, []any{"region"}))

		case strings.Contains(q, "SHOW TAG VALUES CARDINALITY"):
			// "region" is the narrow key and sets the worst case. This case must
			// precede the plain SHOW TAG VALUES case below, which it prefixes.
			if strings.Contains(q, `"region"`) {
				fmt.Fprint(w, seriesResponse([]string{"key", "count"}, []any{"region", 4.0}))
			} else {
				fmt.Fprint(w, seriesResponse([]string{"key", "count"}, []any{"host", 5000.0}))
			}

		case strings.Contains(q, "SHOW TAG VALUES"):
			fmt.Fprint(w, seriesResponse([]string{"key", "value"},
				[]any{"region", "us-east"}, []any{"region", "us-west"}))

		case strings.Contains(q, "SHOW SHARDS"):
			fmt.Fprint(w, seriesResponse(
				[]string{"id", "database", "retention_policy", "shard_group", "start_time", "end_time"},
				[]any{1.0, "telemetry", "autogen", 1.0, "2026-09-01T00:00:00Z", "2026-09-08T00:00:00Z"}))

		case strings.HasPrefix(strings.TrimSpace(q), "SELECT count(*)"):
			// The probe query. The fake's /debug/vars is static, so the gauge
			// will not move and the probe will read this as "already cached".
			fmt.Fprint(w, `{"results":[{}]}`)

		default:
			http.Error(w, `{"error":"unexpected query"}`, http.StatusBadRequest)
		}
	})
	mux.HandleFunc("/debug/pprof/heap", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, heapProfileFixture)
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestFetchVars(t *testing.T) {
	srv := newFakeInstance(t, fakeVars(12, 500_000, 9000, 1000, 800, 100, 100))
	c := NewClient(srv.URL, "", "", 5*time.Second, false)

	snap, err := c.FetchVars(context.Background())
	require.NoError(t, err)

	require.Equal(t, int64(12), snap.TSIShardCount())
	require.Len(t, snap.Shards, 12)
	require.Equal(t, 12, snap.IndexTypes["tsi1"])
	require.Equal(t, int64(18<<30), snap.HeapInuse)
	require.Equal(t, []string{"telemetry"}, snap.Databases())

	require.Equal(t, int64(12*100), snap.TotalCapacity())
	require.Equal(t, int64(12*100), snap.TotalSize())

	act := snap.Activity()
	require.Equal(t, int64(12*9000), act.Hits)
	require.Equal(t, int64(12*1000), act.Misses)
	require.Equal(t, int64(12*800), act.Evictions)

	// The cumulative counters from a single scrape are not a window.
	require.False(t, act.Sampled)
}

func TestFetchVarsSkipsNonStatisticKeys(t *testing.T) {
	// "build", "cmdline" and "memstats" must not be mistaken for statistics.
	srv := newFakeInstance(t, fakeVars(1, 1000, 1, 1, 0, 1, 100))
	c := NewClient(srv.URL, "", "", 5*time.Second, false)

	snap, err := c.FetchVars(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(1), snap.TSIShardCount())
	require.Len(t, snap.Shards, 1)
}

func TestHeapPercentile(t *testing.T) {
	srv := newFakeInstance(t, fakeVars(1, 1000, 1, 1, 0, 1, 100))
	c := NewClient(srv.URL, "", "", 5*time.Second, false)

	p95, err := c.HeapPercentile(context.Background(), "7d", 0.95)
	require.NoError(t, err)
	// 100 hourly peaks running from 10% to ~109% of a GiB; nearest-rank p95 is
	// the 95th, i.e. 104/100 GiB.
	peak := float64(104) * (1 << 30) / 100
	require.Equal(t, int64(peak), p95)
}

func TestHistoricalActivity(t *testing.T) {
	srv := newFakeInstance(t, fakeVars(1, 1000, 1, 1, 0, 1, 100))
	c := NewClient(srv.URL, "", "", 5*time.Second, false)

	act, err := c.HistoricalActivity(context.Background(), "7d")
	require.NoError(t, err)
	require.True(t, act.Sampled)
	require.Equal(t, int64(600000), act.Hits)
	require.Equal(t, int64(400000), act.Misses)
	require.Equal(t, int64(350000), act.Evictions)
	require.InDelta(t, 0.6, act.HitRate(), 1e-9)
}

func TestFetchSchemaAndEstimate(t *testing.T) {
	srv := newFakeInstance(t, fakeVars(1, 500_000, 1, 1, 0, 1, 100))
	c := NewClient(srv.URL, "", "", 5*time.Second, false)

	sc, err := c.FetchSchema(context.Background(), "telemetry", 50)
	require.NoError(t, err)
	require.False(t, sc.Truncated)
	require.Equal(t, int64(1_000_000), sc.SeriesCardinality["x"])
	require.Equal(t, int64(4), sc.TagValueCount["x"]["region"])
	require.Equal(t, int64(5000), sc.TagValueCount["x"]["host"])

	// The shard holds 500k of the measurement's 1M series, so "region" (4
	// values) covers 125k series per entry.
	worst, typical := EstimateEntryBytes(500_000, sc, 0)
	require.Equal(t, EntryBytes(125_000, 0), worst)
	require.Equal(t, EntryBytes(100, 0), typical)
}

func TestPeakShardSizes(t *testing.T) {
	srv := newFakeInstance(t, fakeVars(3, 1000, 1, 1, 0, 1, 100))
	c := NewClient(srv.URL, "", "", 5*time.Second, false)

	peaks, err := c.PeakShardSizes(context.Background(), "7d")
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"1": 100, "2": 0, "3": 740}, peaks)

	// Shard 2 never held an entry, so only two of the three are active. This
	// is the count the sweep-exposure warning is derived from, and it must use
	// "> 0" rather than "> floor": a full cache at the fixed default size sits
	// at exactly the floor and is very much active.
	var active int
	for _, p := range peaks {
		if p > 0 {
			active++
		}
	}
	require.Equal(t, 2, active)
}

func TestMeasureCacheBytes(t *testing.T) {
	srv := newFakeInstance(t, fakeVars(1, 1000, 1, 1, 0, 1, 100))
	c := NewClient(srv.URL, "", "", 5*time.Second, false)

	got, err := c.MeasureCacheBytes(context.Background(), "innerLockingPut")
	require.NoError(t, err)
	_, want := scaleHeapSample(2, 1048576, 1048576)
	require.Equal(t, want, got)
}

func TestProbeEndToEnd(t *testing.T) {
	// 60 shards of 500k series each, a busy cache at the default size, on an
	// r5.2xlarge. The narrow "region" tag key sets the worst-case entry size,
	// which should collapse the cap.
	srv := newFakeInstance(t, fakeVars(60, 500_000, 600_000, 400_000, 350_000, 100, 100))

	o := &options{
		timeout:      5 * time.Second,
		window:       "7d",
		heapPct:      0.95,
		schemaProbe:  true,
		idSpanFactor: 2,
		maxTagKeys:   50,
		th:           Defaults(),
	}

	r := &InstanceResult{
		Name: "node-1", URL: srv.URL,
		InstanceType: "r5.2xlarge", Config: "medium",
		RAMBytes: 64 * gib,
	}
	probe(context.Background(), r, o)

	require.Empty(t, r.Err)
	require.Equal(t, int64(60), r.TSIShards)
	require.Equal(t, int64(60), r.TotalShards)
	require.Equal(t, "_internal", r.HeapSource)
	require.Equal(t, "_internal/7d", r.ActivitySource)
	require.True(t, r.Activity.Sampled)

	// Only 2 of the 60 shards have ever held an entry, so the instance is
	// flagged as exposed to sweep-driven growth.
	require.Equal(t, int64(2), r.ActiveShards)
	require.Condition(t, func() bool {
		for _, w := range r.Warnings {
			if strings.Contains(w, "sweep-driven growth") {
				return true
			}
		}
		return false
	}, "expected a sweep-exposure warning, got %v", r.Warnings)

	// The schema reports 1M series and the run's -id-span-factor is 2, so the
	// estimate is costed over a 2M-id span rather than fully dispersed.
	require.Equal(t, "schema-worst", r.BytesPerEntrySource)
	require.Equal(t, EntryBytes(125_000, 2_000_000), r.BytesPerEntry)
	require.Equal(t, EntryBytes(100, 2_000_000), r.BytesPerEntryTypical)

	// The conservation model is what sizes the instance: the schema says
	// "region" has 4 values, so only 4 entries can be that expensive, and
	// filling a larger cap must draw on "host" whose entries are tiny.
	require.Equal(t, "conservation", r.CostModel)
	require.Equal(t, VerdictOK, r.Rec.Verdict, "reason: %s", r.Rec.Reason)
	require.Positive(t, r.Rec.MaxSize)
	require.LessOrEqual(t, r.Rec.WorstCaseBytes, r.Rec.BudgetBytes,
		"the modelled worst case must fit the budget it was derived from")

	// The same instance under the per-entry model, which assumes every entry is
	// as expensive as the worst: it cannot justify any cap at all. This is the
	// 29x pessimism conservation removes, and the reason the old model reported
	// TOO_TIGHT on instances that are in fact fine.
	perEntry := Classify(r.TSIShards, r.RAMBytes, r.HeapP95,
		PerEntryModel(r.TSIShards, r.BytesPerEntry), r.Activity, Defaults())
	require.Equal(t, VerdictTooTight, perEntry.Verdict)
	require.Greater(t, r.Rec.MaxSize, perEntry.MaxSize)
}

func TestProbePrefersBytesStat(t *testing.T) {
	// 60 shards holding 100 entries each at 4 KiB per entry. The gauge is
	// exact, so it must win over both the heap profile and the schema probe —
	// note the schema here would otherwise yield a 250 KB worst case and a
	// TOO_TIGHT verdict, as TestProbeEndToEnd shows.
	vars := withCacheBytes(
		fakeVars(60, 500_000, 600_000, 400_000, 350_000, 100, 100),
		100*4096)
	srv := newFakeInstance(t, vars)

	o := &options{
		timeout:      5 * time.Second,
		window:       "7d",
		heapPct:      0.95,
		schemaProbe:  true,
		idSpanFactor: 2,
		maxTagKeys:   50,
		measureHeap:  true,
		th:           Defaults(),
	}

	r := &InstanceResult{
		Name: "node-1", URL: srv.URL,
		InstanceType: "r5.2xlarge", Config: "medium",
		RAMBytes: 64 * gib,
	}
	probe(context.Background(), r, o)

	require.Equal(t, "stat", r.BytesPerEntrySource)
	require.Equal(t, int64(4096), r.BytesPerEntry)
	require.Equal(t, VerdictOK, r.Rec.Verdict, "reason: %s", r.Rec.Reason)
	require.Equal(t, int64(1600), r.Rec.MaxSize)
}

func TestProbeFallsBackWhenBytesStatIsZero(t *testing.T) {
	// A server that publishes the gauge but whose caches are empty reports 0.
	// That must read as "unavailable" and fall through, not as "free".
	vars := withCacheBytes(
		fakeVars(60, 500_000, 600_000, 400_000, 350_000, 100, 100), 0)
	srv := newFakeInstance(t, vars)

	o := &options{
		timeout:      5 * time.Second,
		window:       "7d",
		heapPct:      0.95,
		schemaProbe:  true,
		idSpanFactor: 2,
		maxTagKeys:   50,
		th:           Defaults(),
	}

	r := &InstanceResult{
		Name: "node-1", URL: srv.URL,
		InstanceType: "r5.2xlarge", Config: "medium",
		RAMBytes: 64 * gib,
	}
	probe(context.Background(), r, o)

	require.Equal(t, "schema-worst", r.BytesPerEntrySource)
}

func TestProbeFallsBackToBoundForUnmeasurablePairs(t *testing.T) {
	// The gauge does not move for a value that is already resident, and the
	// hottest predicates are the most likely to be resident. Skipping those
	// would bias the answer low — the unsafe direction — so an unmeasurable pair
	// must contribute its schema bound instead of nothing.
	//
	// The fake serves a static /debug/vars, so every probe reads a zero delta:
	// the whole run takes the fallback path.
	vars := withCacheBytes(
		fakeVars(1, 1_000_000, 600_000, 400_000, 350_000, 100, 100), 100*4096)
	srv := newFakeInstance(t, vars)
	c := NewClient(srv.URL, "", "", 5*time.Second, false)

	o := &options{
		timeout: 5 * time.Second, maxTagKeys: 50,
		probePairs: 2, probeValues: 2, idSpanFactor: 2.0,
		th: Defaults(),
	}
	r := &InstanceResult{Name: "node-1", URL: srv.URL, RAMBytes: 64 * gib}

	snap, err := c.FetchVars(context.Background())
	require.NoError(t, err)

	got, measured, err := probeWorstEntry(context.Background(), c, r, snap, o)
	require.NoError(t, err)
	require.Zero(t, measured, "no value should have been measurable")

	// "region" has 4 values over a 1M-series shard, so k = 250 000 against a
	// 2M-id span. The fallback must be exactly that pair's bound.
	require.Equal(t, EntryBytes(250_000, 2_000_000), got)
	require.Greater(t, got, int64(0), "an unmeasurable probe must not return zero")

	require.Condition(t, func() bool {
		for _, w := range r.Warnings {
			if strings.Contains(w, "no measurable value") {
				return true
			}
		}
		return false
	}, "the operator must be told the probe fell back, got %v", r.Warnings)
}

func TestProbeEndToEndWithMeasuredBytes(t *testing.T) {
	// The same instance, but with a measured bytes-per-entry supplied. Small
	// entries leave room, so the first-pass cap becomes the binding constraint.
	srv := newFakeInstance(t, fakeVars(60, 500_000, 600_000, 400_000, 350_000, 100, 100))

	o := &options{
		timeout:       5 * time.Second,
		window:        "7d",
		heapPct:       0.95,
		bytesPerEntry: 4096,
		idSpanFactor:  2, // the simple model needs the span too, or it costs every series as its own container
		th:            Defaults(),
	}

	r := &InstanceResult{
		Name: "node-1", URL: srv.URL,
		InstanceType: "r5.2xlarge", Config: "medium",
		RAMBytes: 64 * gib,
	}
	probe(context.Background(), r, o)

	require.Equal(t, "flag", r.BytesPerEntrySource)
	require.Equal(t, VerdictOK, r.Rec.Verdict, "reason: %s", r.Rec.Reason)
	require.Equal(t, int64(1600), r.Rec.MaxSize)
	require.True(t, r.Rec.CappedByFirstPass)
}

func TestPartialAuthFailureIsReported(t *testing.T) {
	// The realistic secured-instance shape: /debug/vars is served without
	// authentication, /query is not. Every InfluxQL step then 401s and degrades
	// into its own fallback warning, so without a central check the run yields a
	// confident-looking row with no hint that credentials were the problem.
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/vars", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, fakeVars(2, 500_000, 1, 1, 0, 1, 100))
	})
	mux.HandleFunc("/query", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, `{"error":"unable to parse authentication credentials"}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	r := &InstanceResult{Name: "n", URL: srv.URL, RAMBytes: 64 * gib}
	probe(context.Background(), r, &options{
		timeout: 5 * time.Second, window: "7d", heapPct: 0.95,
		schemaProbe: true, maxTagKeys: 50, idSpanFactor: 2, th: Defaults(),
	})

	require.True(t, r.AuthFailed, "a 401 on /query must be reported even though /debug/vars succeeded")
	require.Empty(t, r.Err, "the instance was reachable; this is not a connection error")
	require.Condition(t, func() bool {
		for _, w := range r.Warnings {
			if strings.Contains(w, "rejected with 401") {
				return true
			}
		}
		return false
	}, "expected a consolidated auth warning, got %v", r.Warnings)
}

func TestAuthErrorIsDistinguishable(t *testing.T) {
	for _, code := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(code)
				fmt.Fprint(w, `{"error":"nope"}`)
			}))
			t.Cleanup(srv.Close)

			c := NewClient(srv.URL, "", "", 5*time.Second, false)
			_, err := c.FetchVars(context.Background())
			require.Error(t, err)
			require.True(t, IsAuthError(err), "%d must produce an AuthError", code)

			n, ae := c.AuthFailures()
			require.Equal(t, 1, n)
			require.Contains(t, ae.Body, "nope")
		})
	}

	// A non-auth failure must not be mistaken for one.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	c := NewClient(srv.URL, "", "", 5*time.Second, false)
	_, err := c.FetchVars(context.Background())
	require.Error(t, err)
	require.False(t, IsAuthError(err))
	n, _ := c.AuthFailures()
	require.Zero(t, n)
}

func TestVerboseLogsEveryRequest(t *testing.T) {
	srv := newFakeInstance(t, fakeVars(1, 1000, 1, 1, 0, 1, 100))

	var lines []string
	c := NewClient(srv.URL, "", "", 5*time.Second, false)
	c.Label = "inst"
	c.Verbose = func(format string, args ...any) {
		lines = append(lines, fmt.Sprintf(format, args...))
	}

	_, err := c.FetchVars(context.Background())
	require.NoError(t, err)
	_, err = c.Query(context.Background(), "db", `SHOW TAG KEYS ON "db"`)
	require.NoError(t, err)

	require.Len(t, lines, 2)
	require.Contains(t, lines[0], "[inst]")
	require.Contains(t, lines[0], "/debug/vars")
	require.Contains(t, lines[1], "200")
	require.Contains(t, lines[1], `SHOW TAG KEYS ON "db"`,
		"the statement must appear, so a slow query can be identified")

	n, total, slowest, desc := c.Stats()
	require.Equal(t, 2, n)
	require.Positive(t, total)
	require.LessOrEqual(t, slowest, total)
	require.NotEmpty(t, desc)
}

func TestFetchSchemaIsMemoizedPerRun(t *testing.T) {
	// The cost model and the bytes-per-entry resolution both want the schema.
	// Without memoization every cardinality query is issued twice.
	var queries int
	base := newFakeInstance(t, fakeVars(1, 1000, 1, 1, 0, 1, 100))
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/query" {
			queries++
		}
		http.Redirect(w, r, base.URL+r.URL.Path+"?"+r.URL.RawQuery, http.StatusTemporaryRedirect)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "", "", 5*time.Second, false)
	first, err := c.FetchSchema(context.Background(), "telemetry", 50)
	require.NoError(t, err)
	after := queries
	require.Positive(t, after)

	second, err := c.FetchSchema(context.Background(), "telemetry", 50)
	require.NoError(t, err)
	require.Same(t, first, second, "the second call must reuse the cached profile")
	require.Equal(t, after, queries, "the second call must issue no queries")

	// A different tag-key cap is a different profile and must not reuse it.
	_, err = c.FetchSchema(context.Background(), "telemetry", 1)
	require.NoError(t, err)
	require.Greater(t, queries, after)
}

func TestSaveLoadRoundTrip(t *testing.T) {
	// The expensive half of a run is the profiles; they must survive the round
	// trip, or -load is only useful for instances that needed no schema.
	dir := t.TempDir()
	path := dir + "/run.json"

	collected := []InstanceResult{{
		Name: "node-a", URL: "http://a:8086", InstanceType: "r5.2xlarge", Config: "medium",
		RAMBytes: 64 * gib, HeapP95: 20 * gib, HeapSource: "_internal",
		TSIShards: 2, BytesPerEntry: 4096, BytesPerEntrySource: "stat",
		Activity: CacheActivity{Hits: 600, Misses: 400, Evictions: 350, Sampled: true},
		Profiles: []ShardProfile{{ShardID: "1", Keys: []KeyProfile{
			{Measurement: "m", Key: "region", Values: 4, SeriesInShard: 1_000_000},
			{Measurement: "m", Key: "host", Values: 100_000, SeriesInShard: 1_000_000},
		}}},
		SimpleProfiles: []SimpleShardProfile{{ShardID: "1", Series: 1_000_000, TagKeys: 2}},
		Warnings:       []string{"something worth keeping"},
	}}

	o := options{window: "7d", heapPct: 0.95, maxTagKeys: 50, schemaProbe: true, model: "auto", idSpanFactor: 2, th: Defaults()}
	require.NoError(t, saveRun(path, collected, &o))

	run, err := loadRun(path)
	require.NoError(t, err)
	require.Equal(t, savedRunVersion, run.Version)
	require.Equal(t, "7d", run.Collection.Window)
	require.Len(t, run.Instances, 1)

	got := run.Instances[0]
	require.Equal(t, collected[0].Profiles, got.Profiles, "conservation profile must survive")
	require.Equal(t, collected[0].SimpleProfiles, got.SimpleProfiles, "simple profile must survive")
	require.Equal(t, collected[0].Activity, got.Activity)
	require.Equal(t, collected[0].Warnings, got.Warnings, "warnings describe the data, so they are kept")
}

func TestRescoreAppliesNewParameters(t *testing.T) {
	run := &savedRun{
		Version:    savedRunVersion,
		Collection: savedCollection{Model: "auto", Window: "7d"},
		Instances: []InstanceResult{{
			Name: "node-a", RAMBytes: 64 * gib, HeapP95: 20 * gib, TSIShards: 1,
			BytesPerEntry: 4096,
			Profiles: []ShardProfile{{ShardID: "1", Keys: []KeyProfile{
				{Measurement: "m", Key: "region", Values: 4, SeriesInShard: 1_000_000},
			}}},
			SimpleProfiles: []SimpleShardProfile{{ShardID: "1", Series: 1_000_000, TagKeys: 1}},
		}},
	}

	o := options{model: "auto", th: Defaults()}
	first := rescore(run, &o)
	require.Equal(t, "conservation", first[0].CostModel)
	require.Equal(t, VerdictOK, first[0].Rec.Verdict)

	t.Run("RAM supplied after collection", func(t *testing.T) {
		// The case this exists for: collection happened, the instance type was
		// wrong or absent, and the fix must not require collecting again.
		o := options{model: "auto", instanceType: "r5.large", th: Defaults()}
		got := rescore(run, &o)
		require.Equal(t, int64(16)*gib, got[0].RAMBytes)
		require.Less(t, got[0].Rec.BudgetBytes, first[0].Rec.BudgetBytes,
			"a smaller instance must get a smaller budget")
	})

	t.Run("thresholds re-tuned", func(t *testing.T) {
		th := Defaults()
		th.BudgetFrac = 0.01
		o := options{model: "auto", th: th}
		got := rescore(run, &o)
		require.Less(t, got[0].Rec.BudgetBytes, first[0].Rec.BudgetBytes)
	})

	t.Run("model switched from the same file", func(t *testing.T) {
		o := options{model: "simple", th: Defaults()}
		got := rescore(run, &o)
		require.Equal(t, "simple", got[0].CostModel)
		require.Positive(t, got[0].Rec.MaxSize)
	})

	t.Run("rescore never contacts anything", func(t *testing.T) {
		// A URL that would fail instantly if it were dialled.
		run := *run
		run.Instances = append([]InstanceResult(nil), run.Instances...)
		run.Instances[0].URL = "http://127.0.0.1:1"
		o := options{model: "auto", th: Defaults()}
		got := rescore(&run, &o)
		require.Equal(t, VerdictOK, got[0].Rec.Verdict)
		require.Empty(t, got[0].Err)
	})
}

func TestFetchDatabaseCardinality(t *testing.T) {
	srv := newFakeInstance(t, fakeVars(1, 1000, 0, 0, 0, 0, 100))
	c := NewClient(srv.URL, "", "", 5*time.Second, false)
	got, err := c.FetchDatabaseCardinality(context.Background(), "telemetry")
	require.NoError(t, err)
	require.Equal(t, int64(1_000_000), got)
}

func TestHistoricalActivityByShard(t *testing.T) {
	srv := newFakeInstance(t, fakeVars(3, 1000, 0, 0, 0, 0, 100))
	c := NewClient(srv.URL, "", "", 5*time.Second, false)

	got, err := c.HistoricalActivityByShard(context.Background(), "7d")
	require.NoError(t, err)
	require.Len(t, got, 2, "only shards with history appear")
	require.Equal(t, CacheActivity{Hits: 590000, Misses: 10000, Evictions: 9000, Sampled: true}, got["1"])
	require.InDelta(t, 0.95, got["3"].HitRate(), 1e-9)
}

func TestRescoreRecomputesTarget(t *testing.T) {
	// A saved file from before the target existed carries 0.00, and a target of
	// 0.0 in the config snippet disables adaptive sizing. Rescoring must derive
	// the target from the saved evidence under the current flag, never copy it.
	base := InstanceResult{
		Name: "node-a", RAMBytes: 64 * gib, HeapP95: 20 * gib, TSIShards: 1,
		Profiles: []ShardProfile{{ShardID: "1", Keys: []KeyProfile{
			{Measurement: "m", Key: "region", Values: 4, SeriesInShard: 1_000_000},
		}}},
	}
	run := func(in InstanceResult) *savedRun {
		return &savedRun{Version: savedRunVersion, Collection: savedCollection{Model: "auto"}, Instances: []InstanceResult{in}}
	}

	t.Run("no evidence takes the flag", func(t *testing.T) {
		o := options{model: "auto", targetHitRate: 0.90, th: Defaults()}
		got := rescore(run(base), &o)
		require.InDelta(t, 0.90, got[0].TargetHitRate, 1e-9)
		require.NotEmpty(t, got[0].TargetReason)
	})

	t.Run("a demonstrated rate raises it", func(t *testing.T) {
		in := base
		in.Activity = CacheActivity{Hits: 97000, Misses: 3000, Evictions: 100, Sampled: true} // 0.97
		o := options{model: "auto", targetHitRate: 0.90, th: Defaults()}
		got := rescore(run(in), &o)
		require.InDelta(t, 0.95, got[0].TargetHitRate, 1e-9, "0.97 less the margin, rounded down")
	})

	t.Run("a pinned ceiling lowers it below the flag", func(t *testing.T) {
		in := base
		in.PinnedShards, in.PinnedHitRate = 3, 0.88
		o := options{model: "auto", targetHitRate: 0.90, th: Defaults()}
		got := rescore(run(in), &o)
		require.InDelta(t, 0.86, got[0].TargetHitRate, 1e-9, "the ceiling is authoritative")
	})

	t.Run("a saved target is not copied through", func(t *testing.T) {
		in := base
		in.TargetHitRate = 0.97 // stale: saved under a different flag
		o := options{model: "auto", targetHitRate: 0.90, th: Defaults()}
		got := rescore(run(in), &o)
		require.InDelta(t, 0.90, got[0].TargetHitRate, 1e-9)
	})
}

func TestRescoreInfersAdaptiveSizingFromTotals(t *testing.T) {
	// A file saved by a build without the config diagnostics still carries the
	// capacity total; one that is not a multiple of the shard count can only
	// come from adaptive growth.
	in := InstanceResult{
		Name: "node-a", RAMBytes: 64 * gib, HeapP95: 20 * gib, TSIShards: 251,
		TotalCapacity:  311449, // 251 * 1240 + 209: the production clone
		SimpleProfiles: []SimpleShardProfile{{ShardID: "1", Series: 1000, TagKeys: 2}},
	}
	run := &savedRun{Version: savedRunVersion, Collection: savedCollection{Model: "auto"}, Instances: []InstanceResult{in}}
	o := options{model: "auto", targetHitRate: 0.90, th: Defaults()}

	got := rescore(run, &o)
	require.Condition(t, func() bool {
		for _, w := range got[0].Warnings {
			if strings.Contains(w, "adaptive sizing appears to be enabled") {
				return true
			}
		}
		return false
	}, "got %v", got[0].Warnings)

	again := rescore(&savedRun{Version: savedRunVersion, Collection: savedCollection{Model: "auto"}, Instances: got}, &o)
	var n int
	for _, w := range again[0].Warnings {
		if strings.Contains(w, "adaptive sizing appears to be enabled") {
			n++
		}
	}
	require.Equal(t, 1, n, "re-scoring a re-scored file must not stack the warning")
}

func TestRescoreRefusesAModelItHasNoDataFor(t *testing.T) {
	// Silently scoring with a weaker bound than asked for is how a saved file
	// turns into a wrong answer.
	run := &savedRun{
		Version:    savedRunVersion,
		Collection: savedCollection{Model: "simple"},
		Instances: []InstanceResult{{
			Name: "node-a", RAMBytes: 64 * gib, HeapP95: 20 * gib, TSIShards: 1,
			SimpleProfiles: []SimpleShardProfile{{ShardID: "1", Series: 1000, TagKeys: 2}},
		}},
	}

	o := options{model: "conservation", th: Defaults()}
	got := rescore(run, &o)
	require.Equal(t, "none", got[0].CostModel)
	require.Equal(t, VerdictUnknown, got[0].Rec.Verdict)
	require.Condition(t, func() bool {
		for _, w := range got[0].Warnings {
			if strings.Contains(w, `collected with -model "simple"`) {
				return true
			}
		}
		return false
	}, "the warning must name the model the file was collected with, got %v", got[0].Warnings)
}

func TestLoadRunRejectsBadFiles(t *testing.T) {
	dir := t.TempDir()

	_, err := loadRun(dir + "/absent.json")
	require.Error(t, err)

	p := dir + "/garbage.json"
	require.NoError(t, os.WriteFile(p, []byte("not json"), 0o600))
	_, err = loadRun(p)
	require.ErrorContains(t, err, "parsing")

	p = dir + "/wrongversion.json"
	require.NoError(t, os.WriteFile(p, []byte(`{"version":99,"instances":[{"name":"a"}]}`), 0o600))
	_, err = loadRun(p)
	require.ErrorContains(t, err, "format version 99")

	p = dir + "/empty.json"
	require.NoError(t, os.WriteFile(p, []byte(`{"version":1,"instances":[]}`), 0o600))
	_, err = loadRun(p)
	require.ErrorContains(t, err, "no instances")
}

func TestProbeUnreachable(t *testing.T) {
	r := &InstanceResult{Name: "down", URL: "http://127.0.0.1:1", RAMBytes: 64 * gib}
	probe(context.Background(), r, &options{timeout: time.Second, th: Defaults()})

	require.NotEmpty(t, r.Err, "an unreachable instance must be reported, not panic")
	require.Equal(t, VerdictUnknown, r.Rec.Verdict)
}

func TestProbeInmemInstance(t *testing.T) {
	// No tsi1_cache statistics at all: an inmem-index instance.
	srv := newFakeInstance(t, `{"memstats":{"HeapInuse":100,"Sys":200}}`)

	r := &InstanceResult{Name: "inmem", URL: srv.URL, RAMBytes: 64 * gib}
	probe(context.Background(), r, &options{timeout: 5 * time.Second, th: Defaults()})

	require.Empty(t, r.Err)
	require.Equal(t, VerdictNotTSI, r.Rec.Verdict)
}

func TestReadInventory(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/fleet.csv"
	require.NoError(t, writeFile(path, `# fleet inventory
name,url,instance_type,config,bytes_per_entry
node-1,http://a:8086,r5.2xlarge,medium,
node-2,http://b:8086,r5.8xlarge,large,4096
node-3,http://c:8086,r5.large,,
`))

	got, err := readInventory(path)
	require.NoError(t, err)
	require.Len(t, got, 3)

	require.Equal(t, "node-1", got[0].Name)
	require.Equal(t, "medium", got[0].Config)
	require.Zero(t, got[0].BytesPerEntry)

	require.Equal(t, int64(4096), got[1].BytesPerEntry)
	require.Equal(t, "inventory", got[1].BytesPerEntrySource)

	require.Equal(t, "default", got[2].Config, "a blank config must default")
}

func TestReadInventoryErrors(t *testing.T) {
	dir := t.TempDir()

	t.Run("missing required column", func(t *testing.T) {
		p := dir + "/bad1.csv"
		require.NoError(t, writeFile(p, "name,config\nnode-1,medium\n"))
		_, err := readInventory(p)
		require.ErrorContains(t, err, `missing required column "url"`)
	})

	t.Run("header only", func(t *testing.T) {
		p := dir + "/bad2.csv"
		require.NoError(t, writeFile(p, "name,url\n"))
		_, err := readInventory(p)
		require.ErrorContains(t, err, "at least one instance")
	})

	t.Run("unparsable bytes_per_entry", func(t *testing.T) {
		p := dir + "/bad3.csv"
		require.NoError(t, writeFile(p, "name,url,bytes_per_entry\nn,http://a,abc\n"))
		_, err := readInventory(p)
		require.ErrorContains(t, err, "bytes_per_entry")
	})
}

func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o600)
}

func TestFetchVarsParsesSystemUptime(t *testing.T) {
	srv := newFakeInstance(t, fakeVars(1, 1000, 0, 0, 0, 0, 100))
	c := NewClient(srv.URL, "", "", 5*time.Second, false)
	snap, err := c.FetchVars(context.Background())
	require.NoError(t, err)
	require.Equal(t, 720*time.Hour, snap.Uptime)
	require.Equal(t, time.Date(2026, 8, 17, 0, 0, 0, 0, time.UTC), snap.Started.UTC())

	// A build without the block reports nothing rather than failing.
	bare := strings.Replace(fakeVars(1, 1000, 0, 0, 0, 0, 100), `"system"`, `"not-system"`, 1)
	srv2 := newFakeInstance(t, bare)
	snap, err = NewClient(srv2.URL, "", "", 5*time.Second, false).FetchVars(context.Background())
	require.NoError(t, err)
	require.Zero(t, snap.Uptime)
}

func TestProbeWarnsWhenWindowExceedsUptime(t *testing.T) {
	// A process up for an hour, asked for seven days of history: the history
	// belongs to something else.
	young := strings.Replace(fakeVars(3, 1000, 600, 400, 350, 100, 100), `"uptime":"720h0m0s"`, `"uptime":"1h0m0s"`, 1)
	require.Contains(t, young, `"1h0m0s"`, "fixture must have been rewritten")
	srv := newFakeInstance(t, young)

	o := &options{window: "7d", heapPct: 0.95, model: "auto", schemaProbe: true, maxTagKeys: 50, idSpanFactor: 2, th: Defaults(), targetHitRate: 0.9}
	r := InstanceResult{Name: "young", URL: srv.URL, RAMBytes: 64 * gib}
	probe(context.Background(), &r, o)

	require.Equal(t, int64(3600), r.UptimeSeconds)
	require.True(t, hasWarning(&r, "the process has been up 1h0m0s but -window is 7d"), "got %v", r.Warnings)
	require.True(t, hasWarning(&r, "Re-run with -window 1h"), "the hint should name a window that fits: %v", r.Warnings)

	// And the long-running fixture stays quiet.
	old := InstanceResult{Name: "old", URL: newFakeInstance(t, fakeVars(3, 1000, 600, 400, 350, 100, 100)).URL, RAMBytes: 64 * gib}
	probe(context.Background(), &old, o)
	require.False(t, hasWarning(&old, "the process has been up"), "got %v", old.Warnings)
}

func TestProbeAuditsSuppliedConfig(t *testing.T) {
	// The build publishes no adaptive settings; the operator knows the instance
	// runs max-size 100000 at target 0.99. Every shard sits at capacity 2000,
	// so nothing is pinned at 100000, but the audit must still price the cap,
	// and with the flags the pinned check runs too when a cap is reached.
	// The fake's two-key schema has only 5004 distinct values per shard, but
	// entries for values that do not exist still cost the overhead each, so a
	// cap of 100000 on 3 shards prices at about 100 MB: it fits a 64 GiB
	// instance's budget, and the audit must say so with a real safe cap rather
	// than the "saturates, any cap is safe" verdict an earlier version gave.
	// The per-entry model on a small instance is what makes 100000 unaffordable.
	srv := newFakeInstance(t, fakeVars(3, 1000, 600, 400, 350, 100, 2000))
	o := &options{window: "7d", heapPct: 0.95, model: "auto", schemaProbe: true, maxTagKeys: 50, idSpanFactor: 2,
		th: Defaults(), targetHitRate: 0.9, currentMaxSize: 100_000, currentTarget: 0.99}
	fits := InstanceResult{Name: "node", URL: srv.URL, RAMBytes: 64 * gib}
	probe(context.Background(), &fits, o)
	require.Equal(t, "flags", fits.CacheConfigSource)
	require.Equal(t, int64(100_000), fits.CacheConfig.MaxSize)
	require.NotNil(t, fits.Audit)
	require.False(t, fits.Audit.OverBudget, "%+v", *fits.Audit)
	require.Positive(t, fits.Audit.SafeCap, "a finite cap always exists: overhead grows with every entry")
	require.GreaterOrEqual(t, fits.Audit.WorstCaseBytes, int64(3*100_000*entryOverheadBytes),
		"entries beyond the schema are charged at the overhead each")
	require.True(t, hasWarning(&fits, "the configured max-size 100000 (flags) fits"), "got %v", fits.Warnings)

	o.model, o.bytesPerEntry = "per-entry", 4096
	r := InstanceResult{Name: "node", URL: srv.URL, RAMBytes: 4 * gib}
	probe(context.Background(), &r, o)

	require.Equal(t, "flags", r.CacheConfigSource)
	require.NotNil(t, r.Audit)
	require.Equal(t, int64(100_000), r.Audit.MaxSize)
	require.True(t, r.Audit.OverBudget, "100000 entries at 4 KiB on every shard cannot fit: %+v", *r.Audit)
	require.Positive(t, r.Audit.SafeCap)
	require.True(t, hasWarning(&r, "the configured max-size 100000 (flags)"), "got %v", r.Warnings)
	require.True(t, r.Audit.BelowTarget, "the 7d window serves 0.60 against 0.99")

	// Same instance, but the cap is the one every shard already sits at: now the
	// windowed per-shard rates decide what is pinned. Shard 3 serves 0.95 over
	// the window, shard 1 serves 0.983; against 0.99 both are below target.
	o.currentMaxSize = 2000
	r2 := InstanceResult{Name: "node", URL: srv.URL, RAMBytes: 64 * gib}
	probe(context.Background(), &r2, o)
	require.Equal(t, 3, r2.PinnedShards, "shard 2 has no window and falls back to its lifetime 0.60; got %v", r2.Warnings)
	// The pinned shards' pooled rate is (590000+9500+600)/(600000+10000+1000)
	// = 0.982; less the margin and rounded down, 0.96 — above the 0.90 floor,
	// because the ceiling is authoritative in either direction.
	require.InDelta(t, 0.96, r2.TargetHitRate, 1e-9, "the pinned ceiling sets the target recommendation")
}

// shardsLike builds n identical simple profiles, the shape of the production
// clone: many shards of similar size.
func shardsLike(n int, series, tagKeys int64) []SimpleShardProfile {
	out := make([]SimpleShardProfile, 0, n)
	for i := range n {
		out = append(out, SimpleShardProfile{ShardID: strconv.Itoa(i + 1), Series: series, TagKeys: tagKeys})
	}
	return out
}

func TestRescoreAuditsSuppliedConfig(t *testing.T) {
	// A saved file from a build without the diagnostics, re-scored with the
	// configuration the operator has since learned.
	run := &savedRun{
		Version:    savedRunVersion,
		Collection: savedCollection{Model: "auto", Window: "7d"},
		Instances: []InstanceResult{{
			Name: "node-a", RAMBytes: 32 * gib, HeapP95: 15 * gib, TSIShards: 251,
			Activity:       CacheActivity{Hits: 9036, Misses: 964, Sampled: true},
			UptimeSeconds:  3600,
			SimpleProfiles: shardsLike(251, 68_000, 6),
		}},
	}
	o := &options{model: "auto", targetHitRate: 0.90, th: Defaults(), currentMaxSize: 100_000, currentTarget: 0.95}
	got := rescore(run, o)

	require.NotNil(t, got[0].Audit)
	require.Equal(t, "flags", got[0].Audit.Source)
	require.True(t, got[0].Audit.OverBudget)
	require.True(t, got[0].Audit.BelowTarget)
	require.True(t, hasWarning(&got[0], "the configured max-size 100000 (flags)"), "got %v", got[0].Warnings)
	require.True(t, hasWarning(&got[0], "growth toward max-size 100000 is still in progress"), "got %v", got[0].Warnings)
	require.True(t, hasWarning(&got[0], "the process has been up 1h0m0s but -window is 7d"), "got %v", got[0].Warnings)

	// Re-scoring that result without the flags keeps the configuration and
	// its provenance, re-audits it once, and does not stack the warnings.
	plain := rescore(&savedRun{Version: savedRunVersion, Collection: savedCollection{Model: "auto", Window: "7d"}, Instances: got}, &options{model: "auto", targetHitRate: 0.90, th: Defaults()})
	require.NotNil(t, plain[0].Audit, "the config learned on the previous re-score is saved with the row and still audits")
	require.Equal(t, "flags", plain[0].CacheConfigSource)
	var n int
	for _, w := range plain[0].Warnings {
		if strings.Contains(w, "the configured max-size") {
			n++
		}
	}
	require.Equal(t, 1, n, "audit warnings must be replaced, not stacked: %v", plain[0].Warnings)
}

func TestRescoreAppliesIDSpanFlag(t *testing.T) {
	// A file collected before the span was recorded is costed as fully
	// dispersed and says so. Supplying the span tightens the bound and
	// retires the warning.
	run := &savedRun{
		Version:    savedRunVersion,
		Collection: savedCollection{Model: "auto", Window: "7d"},
		Instances: []InstanceResult{{
			Name: "node-a", RAMBytes: 64 * gib, HeapP95: 20 * gib, TSIShards: 1,
			Profiles: []ShardProfile{{ShardID: "1", Keys: []KeyProfile{
				{Measurement: "m", Key: "region", Values: 4, SeriesInShard: 1_000_000},
			}}},
		}},
	}
	dispersed := rescore(run, &options{model: "auto", targetHitRate: 0.9, th: Defaults()})
	require.True(t, hasWarning(&dispersed[0], "no series id span"), "got %v", dispersed[0].Warnings)

	spanned := rescore(run, &options{model: "auto", targetHitRate: 0.9, th: Defaults(), idSpan: 1_000_000})
	require.False(t, hasWarning(&spanned[0], "no series id span"), "got %v", spanned[0].Warnings)
	require.Equal(t, int64(1_000_000), spanned[0].Profiles[0].Keys[0].IDSpan)
	require.Less(t, spanned[0].Rec.WorstCaseBytes, dispersed[0].Rec.WorstCaseBytes,
		"a known span can only tighten the bound")
	require.Equal(t, VerdictOK, spanned[0].Rec.Verdict, "reason: %s", spanned[0].Rec.Reason)
}
