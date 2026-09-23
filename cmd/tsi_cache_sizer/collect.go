package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Collection side: everything that talks to an instance. Three sources, in
// order of how much they cost the instance being measured:
//
//	/debug/vars              instant, free, gives per-shard cache and shard
//	                         stats plus Go memstats
//	InfluxQL on _internal    cheap, gives the history /debug/vars cannot
//	                         (heap p95, per-shard peaks) — 7 day retention
//	InfluxQL schema probe    meta queries; the expensive one, and the only way
//	                         to get bytes-per-entry without a grown canary
//	/debug/pprof/heap        measures bytes-per-entry directly; only meaningful
//	                         once a canary's cache has actually grown
//
// Nothing here writes to the instance.

// Client talks to one influxd.
type Client struct {
	BaseURL  string
	Username string
	Password string
	HTTP     *http.Client

	// Label identifies this instance in verbose output. With -concurrency above
	// 1 several clients log at once, so every line carries it.
	Label string

	// Verbose, when non-nil, receives one line per HTTP request: what was
	// asked, how long it took, and what came back. Writes go to stderr so the
	// report on stdout stays machine-readable.
	Verbose func(format string, args ...any)

	// Request accounting, for the per-instance summary. Guarded because probes
	// and scrapes can be issued from more than one goroutine per instance.
	mu       sync.Mutex
	requests int
	elapsed  time.Duration
	slowest  time.Duration
	slowDesc string

	// Auth failures are counted centrally rather than detected at a call site,
	// because on a typical secured instance they are *partial*: /debug/vars is
	// served without authentication while /query is not. FetchVars therefore
	// succeeds, every InfluxQL step 401s, and each one degrades into its own
	// "unavailable, falling back" warning — producing a confident-looking but
	// degraded answer with no indication that credentials were the problem.
	authFailures int
	lastAuthErr  *AuthError

	// schemas memoizes FetchSchema for the life of one run. The cost model and
	// the bytes-per-entry resolution each want the same profile, and without
	// this every SHOW TAG VALUES CARDINALITY is issued twice — 40 requests
	// instead of 22 on a two-database instance, and proportionally worse on a
	// production schema with dozens of tag keys. A schema does not change
	// underneath a single run.
	schemas map[string]*Schema
}

// AuthFailures returns how many requests were rejected with 401/403, and the
// most recent such error. A non-zero count means the run is not based on
// complete data however healthy the rest of it looks.
func (c *Client) AuthFailures() (int, *AuthError) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.authFailures, c.lastAuthErr
}

func (c *Client) recordAuthFailure(e *AuthError) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.authFailures++
	c.lastAuthErr = e
}

// AuthError marks a 401 or 403. It is distinguished from other failures because
// every collection step in this tool degrades to a fallback on error — which is
// right for a missing statistic and wrong for a credential problem, where every
// subsequent step will fail the same way and the run will quietly produce a
// weaker answer instead of telling you why.
type AuthError struct {
	Path   string
	Status string
	Body   string
}

func (e *AuthError) Error() string {
	return fmt.Sprintf("authentication/authorization failed for %s: %s: %s", e.Path, e.Status, e.Body)
}

// IsAuthError reports whether err is, or wraps, an AuthError.
func IsAuthError(err error) bool {
	var ae *AuthError
	return errors.As(err, &ae)
}

// Stats returns how many requests this client made, the total and the slowest,
// with a description of the slowest. Answers "is it stuck on one enormous query
// or grinding through hundreds of small ones?".
func (c *Client) Stats() (requests int, total, slowest time.Duration, slowestDesc string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.requests, c.elapsed, c.slowest, c.slowDesc
}

// describeRequest renders a request for a log line: the path, plus the InfluxQL
// statement when there is one, whitespace-collapsed and truncated.
func describeRequest(path string, params url.Values) string {
	q := params.Get("q")
	if q == "" {
		return path
	}
	q = strings.Join(strings.Fields(q), " ")
	if len(q) > 160 {
		q = q[:160] + "…"
	}
	if db := params.Get("db"); db != "" {
		return fmt.Sprintf("%s db=%s %s", path, db, q)
	}
	return fmt.Sprintf("%s %s", path, q)
}

// NewClient returns a Client for baseURL (e.g. "http://host:8086").
func NewClient(baseURL, username, password string, timeout time.Duration, insecure bool) *Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if insecure {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	return &Client{
		BaseURL:  strings.TrimRight(baseURL, "/"),
		Username: username,
		Password: password,
		HTTP:     &http.Client{Timeout: timeout, Transport: tr},
	}
}

func (c *Client) get(ctx context.Context, path string, params url.Values) ([]byte, error) {
	u := c.BaseURL + path
	if len(params) > 0 {
		u += "?" + params.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	if c.Username != "" {
		req.SetBasicAuth(c.Username, c.Password)
	}

	desc := describeRequest(path, params)
	start := time.Now()
	resp, err := c.HTTP.Do(req)
	if err != nil {
		took := time.Since(start)
		c.record(took, desc)
		// A timeout here reads very differently from a refused connection, and
		// both are common on a first run against an unfamiliar host, so say
		// which it was rather than only that "it failed".
		c.logf("FAIL  %8s  %s  <- %v", took.Round(time.Millisecond), desc, err)
		if ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("%s: timed out after %s (raise -timeout if the query is simply slow): %w", path, c.HTTP.Timeout, err)
		}
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	took := time.Since(start)
	c.record(took, desc)
	if err != nil {
		c.logf("FAIL  %8s  %s  <- reading body: %v", took.Round(time.Millisecond), desc, err)
		return nil, err
	}

	c.logf("%-4d  %8s  %7s  %s", resp.StatusCode, took.Round(time.Millisecond), humanBytes(int64(len(body))), desc)

	if resp.StatusCode != http.StatusOK {
		// Truncate: an error page can be long, and the first line is the part
		// that identifies the problem (auth, pprof disabled, wrong port).
		snippet := strings.TrimSpace(string(body))
		if len(snippet) > 200 {
			snippet = snippet[:200] + "..."
		}
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			ae := &AuthError{Path: path, Status: resp.Status, Body: snippet}
			c.recordAuthFailure(ae)
			return nil, ae
		}
		return nil, fmt.Errorf("%s: %s: %s", path, resp.Status, snippet)
	}
	return body, nil
}

func (c *Client) logf(format string, args ...any) {
	if c.Verbose == nil {
		return
	}
	if c.Label != "" {
		c.Verbose("[%s] "+format, append([]any{c.Label}, args...)...)
		return
	}
	c.Verbose(format, args...)
}

func (c *Client) record(took time.Duration, desc string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requests++
	c.elapsed += took
	if took > c.slowest {
		c.slowest, c.slowDesc = took, desc
	}
}

// CacheStat is one shard's tsi1_cache statistic.
type CacheStat struct {
	ShardID         string
	Database        string
	RetentionPolicy string
	Hit             int64
	Miss            int64
	Eviction        int64
	ShrinkEviction  int64
	Size            int64
	Capacity        int64

	// Bytes is the cache's own estimate of its heap footprint. Absent on
	// servers predating the stat, where it reads as 0.
	Bytes int64
}

// ShardStat is one shard's shard statistic. SeriesCreate is engine.SeriesN(),
// the shard's current series count — the input to the per-shard entry cost
// estimate.
type ShardStat struct {
	ShardID         string
	Database        string
	RetentionPolicy string
	IndexType       string
	SeriesCreate    int64
}

// VarsSnapshot is one scrape of /debug/vars.
type VarsSnapshot struct {
	Caches map[string]CacheStat // keyed by shard id
	Shards map[string]ShardStat // keyed by shard id

	HeapInuse int64
	Sys       int64

	// IndexTypes counts shards by index type, so an instance running a mix of
	// tsi1 and inmem shards is visible rather than being reduced to one label.
	IndexTypes map[string]int

	// Cache is the instance's live TSI cache configuration, from the config
	// diagnostics. Absent on builds that do not publish the adaptive settings,
	// where MaxSize and TargetHitRate read as 0.
	Cache CacheConfig

	// Uptime and Started come from the "system" diagnostics block. They matter
	// because every _internal figure is read over a window, and a window longer
	// than the process has been up spans a previous process — or, for a clone of
	// a data directory, the source instance's history — joined at a counter
	// reset that non_negative_difference silently drops. Zero when the block is
	// absent.
	Uptime  time.Duration
	Started time.Time
}

// CacheConfig is what the instance is currently configured to do, as opposed to
// what it is currently doing. Needed to interpret the capacity and hit-rate
// statistics: a capacity of 2000 means one thing against a max-size of 2000 and
// another against 100000.
type CacheConfig struct {
	Floor         int64   `json:"floor"`
	MaxSize       int64   `json:"max_size"`
	TargetHitRate float64 `json:"target_hit_rate"`
	Conservatism  float64 `json:"shrink_conservatism"`
}

// Adaptive reports whether adaptive sizing is switched on.
func (c CacheConfig) Adaptive() bool { return c.MaxSize > 0 && c.TargetHitRate > 0 }

// TSIShardCount returns the number of shards with a TSI series-id-set cache.
// This is taken from the presence of a tsi1_cache statistic rather than from
// the indexType tag, because the cache's existence is exactly what matters and
// tsi1.Index.Statistics returns nothing when there is no cache.
func (v *VarsSnapshot) TSIShardCount() int64 { return int64(len(v.Caches)) }

// Databases returns the distinct databases that have at least one tsi1 shard.
func (v *VarsSnapshot) Databases() []string {
	seen := map[string]bool{}
	var out []string
	for id := range v.Caches {
		db := v.Caches[id].Database
		if db == "" {
			if s, ok := v.Shards[id]; ok {
				db = s.Database
			}
		}
		if db != "" && !seen[db] {
			seen[db] = true
			out = append(out, db)
		}
	}
	return out
}

// Activity sums the raw counters across every shard. The counters are
// cumulative, so a single snapshot is only meaningful as one end of a pair.
func (v *VarsSnapshot) Activity() CacheActivity {
	var a CacheActivity
	for _, c := range v.Caches {
		a.Hits += c.Hit
		a.Misses += c.Miss
		a.Evictions += c.Eviction
	}
	return a
}

// TotalCapacity and TotalSize sum the per-shard gauges, the two numbers an
// operator alerts on after rollout.
func (v *VarsSnapshot) TotalCapacity() int64 {
	var n int64
	for _, c := range v.Caches {
		n += c.Capacity
	}
	return n
}

func (v *VarsSnapshot) TotalSize() int64 {
	var n int64
	for _, c := range v.Caches {
		n += c.Size
	}
	return n
}

// TotalBytes sums the per-shard bytes gauge. It is 0 on servers predating the
// stat, which the caller must treat as "unavailable" rather than "empty".
func (v *VarsSnapshot) TotalBytes() int64 {
	var n int64
	for _, c := range v.Caches {
		n += c.Bytes
	}
	return n
}

// CapacitySpread returns the smallest and largest per-shard capacity, and
// whether every shard has the same one. A fixed-size cache gives every shard
// the configured size, so differing capacities are conclusive evidence that
// adaptive sizing is enabled — evidence that matters on builds whose config
// diagnostics do not publish the adaptive settings, where snap.Cache reads as
// disabled. uniform is false for an empty snapshot.
func (v *VarsSnapshot) CapacitySpread() (lo, hi int64, uniform bool) {
	first := true
	for _, c := range v.Caches {
		if first {
			lo, hi, first = c.Capacity, c.Capacity, false
			continue
		}
		lo = min(lo, c.Capacity)
		hi = max(hi, c.Capacity)
	}
	return lo, hi, !first && lo == hi
}

type statEntry struct {
	Name   string            `json:"name"`
	Tags   map[string]string `json:"tags"`
	Values map[string]any    `json:"values"`
}

type memStatsEntry struct {
	HeapInuse int64 `json:"HeapInuse"`
	Sys       int64 `json:"Sys"`
}

// FetchVars scrapes /debug/vars. The endpoint emits one JSON object per
// statistic under a synthesized key ("tsi1_cache:<path>:<id>"), mixed in with
// non-statistic keys like "memstats" and "build", so each value is decoded
// opportunistically and anything that is not a statistic is skipped.
func (c *Client) FetchVars(ctx context.Context) (*VarsSnapshot, error) {
	body, err := c.get(ctx, "/debug/vars", nil)
	if err != nil {
		return nil, err
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("parsing /debug/vars: %w", err)
	}

	snap := &VarsSnapshot{
		Caches:     map[string]CacheStat{},
		Shards:     map[string]ShardStat{},
		IndexTypes: map[string]int{},
	}

	if m, ok := raw["memstats"]; ok {
		var ms memStatsEntry
		if err := json.Unmarshal(m, &ms); err == nil {
			snap.HeapInuse = ms.HeapInuse
			snap.Sys = ms.Sys
		}
	}

	// Process uptime, under "system": a Go duration string and an RFC 3339
	// start time.
	if m, ok := raw["system"]; ok {
		var sys struct {
			Uptime  string    `json:"uptime"`
			Started time.Time `json:"started"`
		}
		if err := json.Unmarshal(m, &sys); err == nil {
			if d, err := time.ParseDuration(sys.Uptime); err == nil && d > 0 {
				snap.Uptime = d
			}
			snap.Started = sys.Started
		}
	}

	// The tsdb config, registered with the monitor under "config".
	if m, ok := raw["config"]; ok {
		var cfg map[string]any
		if err := json.Unmarshal(m, &cfg); err == nil {
			snap.Cache = CacheConfig{
				Floor:         cfgInt(cfg, "series-id-set-cache-size"),
				MaxSize:       cfgInt(cfg, "series-id-set-cache-max-size"),
				TargetHitRate: cfgFloat(cfg, "series-id-set-cache-target-hit-rate"),
				Conservatism:  cfgFloat(cfg, "series-id-set-cache-shrink-conservatism"),
			}
		}
	}

	for key, msg := range raw {
		if key == "memstats" {
			continue
		}
		var s statEntry
		if err := json.Unmarshal(msg, &s); err != nil || s.Name == "" {
			continue
		}
		id := s.Tags["id"]
		if id == "" {
			continue
		}

		switch s.Name {
		case "tsi1_cache":
			snap.Caches[id] = CacheStat{
				ShardID:         id,
				Database:        s.Tags["database"],
				RetentionPolicy: s.Tags["retentionPolicy"],
				Hit:             statInt(s.Values, "hit"),
				Miss:            statInt(s.Values, "miss"),
				Eviction:        statInt(s.Values, "eviction"),
				ShrinkEviction:  statInt(s.Values, "shrink_eviction"),
				Size:            statInt(s.Values, "size"),
				Capacity:        statInt(s.Values, "capacity"),
				Bytes:           statInt(s.Values, "bytes"),
			}
		case "shard":
			it := s.Tags["indexType"]
			snap.Shards[id] = ShardStat{
				ShardID:         id,
				Database:        s.Tags["database"],
				RetentionPolicy: s.Tags["retentionPolicy"],
				IndexType:       it,
				SeriesCreate:    statInt(s.Values, "seriesCreate"),
			}
			if it != "" {
				snap.IndexTypes[it]++
			}
		}
	}
	return snap, nil
}

// ParseWindow converts an InfluxQL duration literal such as "7d", "36h" or
// "2w" into a time.Duration. InfluxQL accepts a single integer with one of the
// units below; Go's ParseDuration knows none of d or w and would reject the
// most common inputs, so this is written out.
func ParseWindow(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	if i == 0 {
		return 0, fmt.Errorf("window %q has no number", s)
	}
	n, err := strconv.ParseInt(s[:i], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("window %q: %w", s, err)
	}
	units := map[string]time.Duration{
		"ns": time.Nanosecond, "u": time.Microsecond, "µ": time.Microsecond, "ms": time.Millisecond,
		"s": time.Second, "m": time.Minute, "h": time.Hour, "d": 24 * time.Hour, "w": 7 * 24 * time.Hour,
	}
	unit, ok := units[s[i:]]
	if !ok {
		return 0, fmt.Errorf("window %q: unknown unit %q", s, s[i:])
	}
	return time.Duration(n) * unit, nil
}

func cfgInt(cfg map[string]any, key string) int64 {
	if f, ok := asFloat(cfg[key]); ok {
		return int64(f)
	}
	return 0
}

func cfgFloat(cfg map[string]any, key string) float64 {
	if f, ok := asFloat(cfg[key]); ok {
		return f
	}
	return 0
}

// statInt reads a numeric statistic value. JSON numbers decode to float64, but
// a value may also arrive as a json.Number or a string depending on how it was
// encoded, so all three are handled.
func statInt(vals map[string]any, key string) int64 {
	v, ok := vals[key]
	if !ok {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return int64(n)
	case json.Number:
		i, _ := n.Int64()
		return i
	case string:
		i, _ := strconv.ParseInt(n, 10, 64)
		return i
	default:
		return 0
	}
}

// --- InfluxQL ---------------------------------------------------------------

type queryResponse struct {
	Results []struct {
		Series []struct {
			Name    string            `json:"name"`
			Tags    map[string]string `json:"tags"`
			Columns []string          `json:"columns"`
			Values  [][]any           `json:"values"`
		} `json:"series"`
		Error string `json:"error"`
	} `json:"results"`
	Error string `json:"error"`
}

// Query runs an InfluxQL statement and returns the first result's series.
func (c *Client) Query(ctx context.Context, db, q string) (*queryResponse, error) {
	params := url.Values{"q": []string{q}}
	if db != "" {
		params.Set("db", db)
	}
	body, err := c.get(ctx, "/query", params)
	if err != nil {
		return nil, err
	}
	var qr queryResponse
	if err := json.Unmarshal(body, &qr); err != nil {
		return nil, fmt.Errorf("parsing query response: %w", err)
	}
	if qr.Error != "" {
		return nil, fmt.Errorf("query %q: %s", q, qr.Error)
	}
	for _, r := range qr.Results {
		if r.Error != "" {
			return nil, fmt.Errorf("query %q: %s", q, r.Error)
		}
	}
	return &qr, nil
}

// columnIndex returns the position of a column name, or -1.
func columnIndex(cols []string, name string) int {
	for i, c := range cols {
		if c == name {
			return i
		}
	}
	return -1
}

func asFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(n, 64)
		return f, err == nil
	default:
		return 0, false
	}
}

// HeapPercentile returns the requested percentile of hourly peak HeapInUse over
// the window, from the monitor's own history in _internal.
//
// Hourly maxima rather than raw samples: the 10s monitor interval makes the raw
// series dominated by idle periods, and it is the peaks that have to fit under
// the healthy line.
func (c *Client) HeapPercentile(ctx context.Context, window string, p float64) (int64, error) {
	q := fmt.Sprintf(`SELECT max("HeapInUse") FROM "_internal"."monitor"."runtime" WHERE time > now() - %s GROUP BY time(1h) fill(none)`, window)
	qr, err := c.Query(ctx, "_internal", q)
	if err != nil {
		return 0, err
	}
	var vals []float64
	for _, r := range qr.Results {
		for _, s := range r.Series {
			idx := columnIndex(s.Columns, "max")
			if idx < 0 {
				continue
			}
			for _, row := range s.Values {
				if idx >= len(row) {
					continue
				}
				if f, ok := asFloat(row[idx]); ok {
					vals = append(vals, f)
				}
			}
		}
	}
	if len(vals) == 0 {
		return 0, fmt.Errorf("no runtime history in _internal over %s", window)
	}
	return int64(Percentile(vals, p)), nil
}

// HistoricalActivity sums per-shard hit/miss/eviction deltas over the window
// from _internal.
//
// non_negative_difference is used rather than max-min so a restart inside the
// window (which resets the counters to zero) contributes no negative delta. The
// inner query produces per-shard 5-minute deltas; the outer sums them across
// every shard and bucket.
func (c *Client) HistoricalActivity(ctx context.Context, window string) (CacheActivity, error) {
	q := fmt.Sprintf(`SELECT sum("d_hit") AS hit, sum("d_miss") AS miss, sum("d_evict") AS evict FROM `+
		`(SELECT non_negative_difference(max("hit")) AS d_hit, `+
		`non_negative_difference(max("miss")) AS d_miss, `+
		`non_negative_difference(max("eviction")) AS d_evict `+
		`FROM "_internal"."monitor"."tsi1_cache" WHERE time > now() - %s GROUP BY time(5m), "id" fill(none))`, window)

	qr, err := c.Query(ctx, "_internal", q)
	if err != nil {
		return CacheActivity{}, err
	}
	var a CacheActivity
	for _, r := range qr.Results {
		for _, s := range r.Series {
			hi, mi, ei := columnIndex(s.Columns, "hit"), columnIndex(s.Columns, "miss"), columnIndex(s.Columns, "evict")
			for _, row := range s.Values {
				if hi >= 0 && hi < len(row) {
					if f, ok := asFloat(row[hi]); ok {
						a.Hits += int64(f)
					}
				}
				if mi >= 0 && mi < len(row) {
					if f, ok := asFloat(row[mi]); ok {
						a.Misses += int64(f)
					}
				}
				if ei >= 0 && ei < len(row) {
					if f, ok := asFloat(row[ei]); ok {
						a.Evictions += int64(f)
					}
				}
			}
		}
	}
	if a.Hits+a.Misses == 0 {
		return CacheActivity{}, fmt.Errorf("no tsi1_cache history in _internal over %s", window)
	}
	a.Sampled = true
	return a, nil
}

// HistoricalActivityByShard is HistoricalActivity broken out per shard: the
// same deltas, summed within each shard id rather than across them. It is what
// the pinned-shard check needs, because the policy acts on a recent windowed
// rate and a shard's lifetime hit/miss counters lag that badly after a change.
func (c *Client) HistoricalActivityByShard(ctx context.Context, window string) (map[string]CacheActivity, error) {
	q := fmt.Sprintf(`SELECT sum("d_hit") AS hit, sum("d_miss") AS miss, sum("d_evict") AS evict FROM `+
		`(SELECT non_negative_difference(max("hit")) AS d_hit, `+
		`non_negative_difference(max("miss")) AS d_miss, `+
		`non_negative_difference(max("eviction")) AS d_evict `+
		`FROM "_internal"."monitor"."tsi1_cache" WHERE time > now() - %s GROUP BY time(5m), "id" fill(none)) GROUP BY "id"`, window)

	qr, err := c.Query(ctx, "_internal", q)
	if err != nil {
		return nil, err
	}
	out := map[string]CacheActivity{}
	for _, r := range qr.Results {
		for _, s := range r.Series {
			id := s.Tags["id"]
			if id == "" {
				continue
			}
			a := out[id]
			hi, mi, ei := columnIndex(s.Columns, "hit"), columnIndex(s.Columns, "miss"), columnIndex(s.Columns, "evict")
			for _, row := range s.Values {
				if hi >= 0 && hi < len(row) {
					if f, ok := asFloat(row[hi]); ok {
						a.Hits += int64(f)
					}
				}
				if mi >= 0 && mi < len(row) {
					if f, ok := asFloat(row[mi]); ok {
						a.Misses += int64(f)
					}
				}
				if ei >= 0 && ei < len(row) {
					if f, ok := asFloat(row[ei]); ok {
						a.Evictions += int64(f)
					}
				}
			}
			a.Sampled = a.Hits+a.Misses > 0
			out[id] = a
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no per-shard tsi1_cache history in _internal over %s", window)
	}
	return out, nil
}

// SampleActivity measures a window directly, by differencing two /debug/vars
// scrapes. The fallback for instances where the monitor is not storing to
// _internal. A short sample is a much weaker signal than the historical query —
// it can easily miss the daily peak — so the caller should prefer
// HistoricalActivity and report which one was used.
func (c *Client) SampleActivity(ctx context.Context, d time.Duration) (CacheActivity, error) {
	first, err := c.FetchVars(ctx)
	if err != nil {
		return CacheActivity{}, err
	}
	select {
	case <-ctx.Done():
		return CacheActivity{}, ctx.Err()
	case <-time.After(d):
	}
	second, err := c.FetchVars(ctx)
	if err != nil {
		return CacheActivity{}, err
	}

	a0, a1 := first.Activity(), second.Activity()
	delta := CacheActivity{
		Hits:      a1.Hits - a0.Hits,
		Misses:    a1.Misses - a0.Misses,
		Evictions: a1.Evictions - a0.Evictions,
		Sampled:   true,
	}
	// A restart between scrapes resets the counters; a negative delta is not a
	// measurement, so report it as no sample rather than as zero activity.
	if delta.Hits < 0 || delta.Misses < 0 || delta.Evictions < 0 {
		return CacheActivity{}, fmt.Errorf("counters went backwards between scrapes (restart?)")
	}
	return delta, nil
}

// PeakShardSizes returns each shard's peak occupancy and capacity over the
// window. The count of shards whose peak exceeded the floor is N_active: when
// it is far below the total shard count, the instance is exposed to
// sweep-driven growth, because a shard that stops being queried never shrinks.
func (c *Client) PeakShardSizes(ctx context.Context, window string) (map[string]int64, error) {
	q := fmt.Sprintf(`SELECT max("size") AS size FROM "_internal"."monitor"."tsi1_cache" WHERE time > now() - %s GROUP BY "id"`, window)
	qr, err := c.Query(ctx, "_internal", q)
	if err != nil {
		return nil, err
	}
	out := map[string]int64{}
	for _, r := range qr.Results {
		for _, s := range r.Series {
			id := s.Tags["id"]
			idx := columnIndex(s.Columns, "size")
			if id == "" || idx < 0 {
				continue
			}
			var peak float64
			for _, row := range s.Values {
				if idx < len(row) {
					if f, ok := asFloat(row[idx]); ok && f > peak {
						peak = f
					}
				}
			}
			out[id] = int64(peak)
		}
	}
	return out, nil
}

// --- Schema probe -----------------------------------------------------------

// Schema holds what the estimate needs about one database: each measurement's
// series cardinality, and each (measurement, tag key) pair's distinct value
// count.
type Schema struct {
	Database string

	// SeriesCardinality is the per-measurement HLL estimate.
	SeriesCardinality map[string]int64

	// TagValueCount is measurement -> tag key -> distinct values.
	TagValueCount map[string]map[string]int64

	// Truncated records that the tag key list was cut off at the configured
	// limit, so the worst case may be understated.
	Truncated bool
}

// TotalSeries is the sum of the per-measurement cardinalities, the denominator
// for a measurement's share of a shard.
func (s *Schema) TotalSeries() int64 {
	var n int64
	for _, c := range s.SeriesCardinality {
		n += c
	}
	return n
}

// FetchTagKeyCounts returns each measurement's tag key count from a single
// SHOW TAG KEYS. This is all the simple bound needs — no per-key cardinality
// probe — so it is one meta query per database rather than one per tag key.
func (c *Client) FetchTagKeyCounts(ctx context.Context, db string) (map[string]int64, error) {
	qr, err := c.Query(ctx, db, fmt.Sprintf(`SHOW TAG KEYS ON %q`, db))
	if err != nil {
		return nil, err
	}
	out := map[string]int64{}
	for _, r := range qr.Results {
		for _, s := range r.Series {
			idx := columnIndex(s.Columns, "tagKey")
			if idx < 0 {
				continue
			}
			for _, row := range s.Values {
				if idx < len(row) {
					if k, ok := row[idx].(string); ok && k != "" {
						out[s.Name]++
					}
				}
			}
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no tag keys returned for %q", db)
	}
	return out, nil
}

// FetchDatabaseCardinality returns the database-wide series cardinality
// estimate from a single SHOW SERIES CARDINALITY (no FROM clause, so one
// number rather than one per measurement). It bounds the series id span for
// the simple model, which does not run the per-measurement schema probe.
func (c *Client) FetchDatabaseCardinality(ctx context.Context, db string) (int64, error) {
	qr, err := c.Query(ctx, db, fmt.Sprintf(`SHOW SERIES CARDINALITY ON %q`, db))
	if err != nil {
		return 0, err
	}
	var total int64
	for _, r := range qr.Results {
		for _, s := range r.Series {
			idx := columnIndex(s.Columns, "cardinality estimation")
			if idx < 0 {
				idx = columnIndex(s.Columns, "count")
			}
			if idx < 0 && len(s.Columns) > 0 {
				idx = len(s.Columns) - 1
			}
			for _, row := range s.Values {
				if idx >= 0 && idx < len(row) {
					if f, ok := asFloat(row[idx]); ok && f > 0 {
						total += int64(f)
					}
				}
			}
		}
	}
	if total <= 0 {
		return 0, fmt.Errorf("no cardinality returned for %q", db)
	}
	return total, nil
}

// MaxTagKeys returns the largest tag key count over all measurements. The
// payload term is really sum over measurements of S_m*T_m; using the maximum T
// over the shard's whole series count is an upper bound on that, and needs only
// one number per database.
func MaxTagKeys(counts map[string]int64) int64 {
	var max int64
	for _, n := range counts {
		if n > max {
			max = n
		}
	}
	return max
}

// FetchSchema probes one database.
//
// Cost: two meta queries plus one per tag key. The per-key query returns all
// measurements at once, so the count scales with tag keys, not with
// measurements. These are meta queries against the index and can be slow on a
// high-cardinality instance — this is the expensive part of the tool, and the
// reason a measured bytes-per-entry short-circuits it.
func (c *Client) FetchSchema(ctx context.Context, db string, maxTagKeys int) (*Schema, error) {
	// Keyed on maxTagKeys too: a different cap would produce a different (and
	// differently truncated) profile.
	key := fmt.Sprintf("%s\x00%d", db, maxTagKeys)
	c.mu.Lock()
	if cached, ok := c.schemas[key]; ok {
		c.mu.Unlock()
		c.logf("      schema %q: reusing profile from earlier in this run", db)
		return cached, nil
	}
	c.mu.Unlock()

	sc := &Schema{
		Database:          db,
		SeriesCardinality: map[string]int64{},
		TagValueCount:     map[string]map[string]int64{},
	}

	// Per-measurement series cardinality. The FROM clause is what selects the
	// count_hll rewrite (query/statement_rewriter.go); without it the statement
	// returns a single database-wide number instead of one row per measurement.
	qr, err := c.Query(ctx, db, fmt.Sprintf(`SHOW SERIES CARDINALITY ON %q FROM /.*/`, db))
	if err != nil {
		return nil, fmt.Errorf("series cardinality: %w", err)
	}
	for _, r := range qr.Results {
		for _, s := range r.Series {
			idx := columnIndex(s.Columns, "cardinality estimation")
			if idx < 0 && len(s.Columns) > 0 {
				idx = len(s.Columns) - 1
			}
			for _, row := range s.Values {
				if idx >= 0 && idx < len(row) {
					if f, ok := asFloat(row[idx]); ok && f > 0 {
						sc.SeriesCardinality[s.Name] += int64(f)
					}
				}
			}
		}
	}
	if len(sc.SeriesCardinality) == 0 {
		return nil, fmt.Errorf("no measurements returned for %q", db)
	}

	// Distinct tag keys across the database.
	qr, err = c.Query(ctx, db, fmt.Sprintf(`SHOW TAG KEYS ON %q`, db))
	if err != nil {
		return nil, fmt.Errorf("tag keys: %w", err)
	}
	keySet := map[string]bool{}
	for _, r := range qr.Results {
		for _, s := range r.Series {
			idx := columnIndex(s.Columns, "tagKey")
			if idx < 0 {
				continue
			}
			for _, row := range s.Values {
				if idx < len(row) {
					if k, ok := row[idx].(string); ok && k != "" {
						keySet[k] = true
					}
				}
			}
		}
	}

	keys := make([]string, 0, len(keySet))
	for k := range keySet {
		keys = append(keys, k)
	}
	if maxTagKeys > 0 && len(keys) > maxTagKeys {
		keys = keys[:maxTagKeys]
		sc.Truncated = true
	}

	// Distinct values per (measurement, tag key). One query per key: on a
	// production schema this is the bulk of a run, so it announces its size
	// before starting rather than going quiet for minutes.
	c.logf("      schema %q: %d measurements, %d tag keys -> %d cardinality queries",
		db, len(sc.SeriesCardinality), len(keys), len(keys))
	for i, k := range keys {
		q := fmt.Sprintf(`SHOW TAG VALUES CARDINALITY ON %q WITH KEY = %q`, db, k)
		_ = i
		qr, err := c.Query(ctx, db, q)
		if err != nil {
			// One unreadable key should not void the whole probe; the worst
			// case it would have contributed is simply missing, which the
			// Truncated flag advertises.
			sc.Truncated = true
			continue
		}
		for _, r := range qr.Results {
			for _, s := range r.Series {
				idx := columnIndex(s.Columns, "count")
				if idx < 0 {
					continue
				}
				for _, row := range s.Values {
					if idx >= len(row) {
						continue
					}
					if f, ok := asFloat(row[idx]); ok && f > 0 {
						if sc.TagValueCount[s.Name] == nil {
							sc.TagValueCount[s.Name] = map[string]int64{}
						}
						sc.TagValueCount[s.Name][k] = int64(f)
					}
				}
			}
		}
	}

	c.mu.Lock()
	if c.schemas == nil {
		c.schemas = map[string]*Schema{}
	}
	c.schemas[key] = sc
	c.mu.Unlock()

	return sc, nil
}

// EstimateEntryBytes derives worst-case and typical bytes-per-entry for a shard
// holding shardSeries series, given the schema of its database.
//
// For each (measurement m, tag key k) the number of series one cache entry
// covers is estimated as
//
//	k_est = shardSeries * (C(m) / total C) / V(m,k)
//
// that is, the shard's share of the measurement divided evenly across the tag
// key's distinct values. Two assumptions, both granted by the problem this tool
// serves: a measurement's share of a shard matches its share of the database,
// and tag values are roughly balanced. Skew makes individual entries larger
// than the mean, which is why the worst case (the max over pairs, i.e. the
// lowest-cardinality tag key on the largest measurement) is what drives the
// budget while the median is reported only for context.
// idSpan is an upper bound on the database's series id space; see EntryBytes for
// why it must be a bound rather than an estimate, and why 0 disables the
// container term.
func EstimateEntryBytes(shardSeries int64, sc *Schema, idSpan int64) (worst, typical int64) {
	if sc == nil || shardSeries <= 0 {
		return 0, 0
	}
	total := sc.TotalSeries()
	if total <= 0 {
		return 0, 0
	}

	var samples []int64
	for m, card := range sc.SeriesCardinality {
		keys := sc.TagValueCount[m]
		if len(keys) == 0 || card <= 0 {
			continue
		}
		// The measurement's series within this shard.
		inShard := float64(shardSeries) * (float64(card) / float64(total))
		for _, v := range keys {
			if v <= 0 {
				continue
			}
			b := EntryBytes(int64(inShard/float64(v)), idSpan)
			samples = append(samples, b)
			if b > worst {
				worst = b
			}
		}
	}
	if len(samples) == 0 {
		return 0, 0
	}

	f := make([]float64, len(samples))
	for i, s := range samples {
		f[i] = float64(s)
	}
	typical = int64(Percentile(f, 0.5))
	return worst, typical
}

// --- Probing -----------------------------------------------------------------

// ShardWindow is a time range that selects exactly one shard.
type ShardWindow struct {
	ID              string
	Database        string
	RetentionPolicy string
	Start           time.Time
}

// ShardWindows lists each shard's time range via SHOW SHARDS, so a probe query
// can be aimed at one specific shard.
func (c *Client) ShardWindows(ctx context.Context, db string) (map[string]ShardWindow, error) {
	qr, err := c.Query(ctx, db, "SHOW SHARDS")
	if err != nil {
		return nil, err
	}
	out := map[string]ShardWindow{}
	for _, r := range qr.Results {
		for _, s := range r.Series {
			idIdx := columnIndex(s.Columns, "id")
			rpIdx := columnIndex(s.Columns, "retention_policy")
			stIdx := columnIndex(s.Columns, "start_time")
			if idIdx < 0 || stIdx < 0 {
				continue
			}
			for _, row := range s.Values {
				if idIdx >= len(row) || stIdx >= len(row) {
					continue
				}
				id := fmt.Sprint(row[idIdx])
				if f, ok := asFloat(row[idIdx]); ok {
					id = strconv.FormatInt(int64(f), 10)
				}
				st, _ := row[stIdx].(string)
				t, err := time.Parse(time.RFC3339, st)
				if err != nil {
					continue
				}
				w := ShardWindow{ID: id, Database: s.Name, Start: t}
				if rpIdx >= 0 && rpIdx < len(row) {
					w.RetentionPolicy, _ = row[rpIdx].(string)
				}
				if w.Database == "" {
					w.Database = db
				}
				out[id] = w
			}
		}
	}
	return out, nil
}

// TagValues returns up to limit values of a tag key on a measurement.
func (c *Client) TagValues(ctx context.Context, db, measurement, key string, limit int) ([]string, error) {
	q := fmt.Sprintf(`SHOW TAG VALUES FROM %q WITH KEY = %q LIMIT %d`, measurement, key, limit)
	qr, err := c.Query(ctx, db, q)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, r := range qr.Results {
		for _, s := range r.Series {
			vi := columnIndex(s.Columns, "value")
			if vi < 0 {
				continue
			}
			for _, row := range s.Values {
				if vi < len(row) {
					if v, ok := row[vi].(string); ok && v != "" {
						out = append(out, v)
					}
				}
			}
		}
	}
	return out, nil
}

// ProbeEntryBytes measures the exact heap cost of the cache entry for one
// (measurement, tag key, tag value) on one shard, by differencing the bytes
// gauge across a query that materializes that entry.
//
// The query's time range is a 1 ms window at the shard's start, so it selects
// exactly the intended shard and reads essentially no data: the tag predicate is
// resolved when the shard's iterator is built, before any points are read, so
// the entry is populated whether or not the window contains data. Measured cost
// on a 250 000-series predicate is ~0.6 s and no disk scan.
//
// Returns 0 with ok=false when the query did not create exactly one entry. The
// common cause is that the value was already cached, in which case the gauge
// does not move; the caller should try another value rather than read 0 as free.
//
// This is not free of side effects: it adds entries to the shard's cache, which
// may evict others under LRU. It is read-only with respect to data.
func (c *Client) ProbeEntryBytes(ctx context.Context, w ShardWindow, measurement, key, value string) (int64, bool, error) {
	start := w.Start.UTC().UnixNano()
	q := fmt.Sprintf(`SELECT count(*) FROM %q.%q.%q WHERE %q = '%s' AND time >= %d AND time < %d`,
		w.Database, w.RetentionPolicy, measurement, key, escapeSingleQuotes(value), start, start+int64(time.Millisecond))

	before, err := c.cacheTotalsForShard(ctx, w.ID)
	if err != nil {
		return 0, false, err
	}
	if _, err := c.Query(ctx, w.Database, q); err != nil {
		return 0, false, err
	}
	after, err := c.cacheTotalsForShard(ctx, w.ID)
	if err != nil {
		return 0, false, err
	}

	if after.size-before.size != 1 {
		return 0, false, nil
	}
	return after.bytes - before.bytes, true, nil
}

// escapeSingleQuotes makes a tag value safe to embed in an InfluxQL string
// literal. Tag values come from the server, but a value containing a quote would
// otherwise produce a syntax error or change the predicate's meaning.
func escapeSingleQuotes(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), `'`, `\'`)
}

type shardTotals struct{ size, bytes int64 }

// cacheTotalsForShard reads one shard's cache gauges. Probing differences a
// single shard rather than the instance total so that concurrent query traffic
// on other shards cannot contaminate the measurement.
func (c *Client) cacheTotalsForShard(ctx context.Context, shardID string) (shardTotals, error) {
	snap, err := c.FetchVars(ctx)
	if err != nil {
		return shardTotals{}, err
	}
	cs, ok := snap.Caches[shardID]
	if !ok {
		return shardTotals{}, fmt.Errorf("no tsi1_cache statistic for shard %s", shardID)
	}
	return shardTotals{size: cs.Size, bytes: cs.Bytes}, nil
}

// --- Heap profile -----------------------------------------------------------

var (
	heapHeaderRe = regexp.MustCompile(`^heap profile: \d+: \d+ \[\d+: \d+\] @ heap/(\d+)`)
	heapRecordRe = regexp.MustCompile(`^(\d+): (\d+) \[(\d+): (\d+)\] @`)
	heapFrameRe  = regexp.MustCompile(`^#\s+0x[0-9a-fA-F]+\s+(\S+)`)
)

// MeasureCacheBytes returns the in-use heap attributable to the TSI cache, by
// parsing the legacy text heap profile from /debug/pprof/heap?debug=1 and
// summing records whose stack contains focus.
//
// Why innerLockingPut is the right focus: the retained bitmaps are allocated by
// SeriesIDSet.Clone called from TagValueSeriesIDCache.innerLockingPut, along
// with the list element, map entries and interned strings for the same entry.
// The clone on the *hit* path is allocated in Index.TagValueSeriesIDIterator
// instead and is transient, so it does not match and is correctly excluded.
//
// Sampling: the text format reports raw sampled counts, unlike the protobuf
// format which the runtime scales on the way out. Each record is therefore
// scaled here by the same 1/(1-exp(-avgSize/rate)) factor the runtime applies,
// where rate is half the value in the profile header (the header reports
// 2*MemProfileRate for historical reasons). Without this the result understates
// the true heap, which is the unsafe direction.
func (c *Client) MeasureCacheBytes(ctx context.Context, focus string) (int64, error) {
	body, err := c.get(ctx, "/debug/pprof/heap", url.Values{"debug": []string{"1"}})
	if err != nil {
		return 0, err
	}
	return parseHeapText(strings.NewReader(string(body)), focus)
}

// parseHeapText is split out from MeasureCacheBytes so the parser can be tested
// against a fixed profile without a server.
func parseHeapText(r io.Reader, focus string) (int64, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<24)

	var rate int64
	var total int64

	// Current record, accumulated until its frame lines run out.
	var curObjects, curBytes int64
	var curMatch bool
	var inRecord bool

	flush := func() {
		if inRecord && curMatch {
			_, scaled := scaleHeapSample(curObjects, curBytes, rate)
			total += scaled
		}
		inRecord, curObjects, curBytes, curMatch = false, 0, 0, false
	}

	for sc.Scan() {
		line := sc.Text()

		if rate == 0 {
			if m := heapHeaderRe.FindStringSubmatch(line); m != nil {
				v, err := strconv.ParseInt(m[1], 10, 64)
				if err == nil && v > 1 {
					// The header carries 2*MemProfileRate.
					rate = v / 2
				}
				continue
			}
		}

		if m := heapRecordRe.FindStringSubmatch(line); m != nil {
			flush()
			inRecord = true
			curObjects, _ = strconv.ParseInt(m[1], 10, 64)
			curBytes, _ = strconv.ParseInt(m[2], 10, 64)
			continue
		}

		if strings.HasPrefix(line, "#\t") || strings.HasPrefix(line, "# 0x") {
			if fm := heapFrameRe.FindStringSubmatch(line); fm != nil {
				if strings.Contains(fm[1], focus) {
					curMatch = true
				}
			}
			continue
		}

		// Any other line (the trailing "# runtime.MemStats" block, blank
		// lines) ends the current record.
		flush()
	}
	flush()

	if err := sc.Err(); err != nil {
		return 0, err
	}
	if rate == 0 {
		return 0, fmt.Errorf("heap profile header not found (is this a debug=1 text profile?)")
	}
	return total, nil
}

// scaleHeapSample undoes the runtime's Poisson sampling for one record,
// mirroring runtime/pprof.scaleHeapSample. An object of size s is sampled with
// probability 1-exp(-s/rate), so dividing by that probability recovers an
// unbiased estimate of the true totals.
func scaleHeapSample(count, size, rate int64) (int64, int64) {
	if count == 0 || size == 0 {
		return 0, 0
	}
	if rate <= 1 {
		// Every allocation was sampled; the counts are already exact.
		return count, size
	}
	avg := float64(size) / float64(count)
	scale := 1 / (1 - math.Exp(-avg/float64(rate)))
	return int64(float64(count) * scale), int64(float64(size) * scale)
}
