// Command tsi_cache_sizer computes a safe per-instance and per-configuration
// value for series-id-set-cache-max-size across a fleet of InfluxDB 1.x
// instances.
//
// It reads only from InfluxQL and the /debug endpoints, and writes nothing. The
// method it implements — the memory budget, the entry cost model, the benefit
// filter and the per-configuration rollup — is documented in
// TSI_ADAPTIVE_CACHE_SIZING_METHOD.md at the repository root; the flags below
// map one to one onto the thresholds described there.
//
// Usage:
//
//	tsi_cache_sizer -inventory fleet.csv
//	tsi_cache_sizer -url http://host:8086 -instance-type r5.2xlarge
//
// The inventory is a CSV with a header row and the columns:
//
//	name,url,instance_type,config[,bytes_per_entry]
//
// bytes_per_entry is optional; supply it once a canary has produced a measured
// value (see -measure-heap) to skip the schema probe and its meta queries.
package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"
)

// InstanceResult is everything the tool learned about one instance, including
// the recommendation. Errors are captured per instance rather than aborting the
// run: one unreachable host should not cost you the rest of the fleet.
type InstanceResult struct {
	Name         string `json:"name"`
	URL          string `json:"url"`
	InstanceType string `json:"instance_type"`
	Config       string `json:"config"`

	RAMBytes   int64  `json:"ram_bytes"`
	HeapP95    int64  `json:"heap_p95_bytes"`
	HeapSource string `json:"heap_source"`

	TSIShards    int64          `json:"tsi_shards"`
	TotalShards  int64          `json:"total_shards"`
	ActiveShards int64          `json:"active_shards"`
	IndexTypes   map[string]int `json:"index_types,omitempty"`

	BytesPerEntry        int64  `json:"bytes_per_entry"`
	BytesPerEntryTypical int64  `json:"bytes_per_entry_typical,omitempty"`
	BytesPerEntrySource  string `json:"bytes_per_entry_source"`

	Activity       CacheActivity `json:"activity"`
	ActivitySource string        `json:"activity_source"`

	// CostModel names which bound produced the recommendation, and
	// ShardProfiles how many shards the conservation model could profile.
	CostModel     string `json:"cost_model"`
	ShardProfiles int    `json:"shard_profiles,omitempty"`

	// CacheConfig is what the instance is currently configured to do, and
	// CacheConfigSource says whether that was read from the instance or supplied
	// with -current-max-size/-current-target. PinnedShards counts shards grown
	// to max-size while still below target — growth with no stopping signal —
	// and PinnedHitRate is the rate they settled at, which is the workload's
	// achievable ceiling.
	CacheConfig       CacheConfig `json:"cache_config"`
	CacheConfigSource string      `json:"cache_config_source,omitempty"`
	PinnedShards      int         `json:"pinned_shards,omitempty"`
	PinnedHitRate     float64     `json:"pinned_hit_rate,omitempty"`

	// Audit is the assessment of the configuration the instance is running,
	// when one is known; nil otherwise. See ConfigAudit.
	Audit *ConfigAudit `json:"config_audit,omitempty"`

	// UptimeSeconds is the process uptime at collection, 0 when the instance
	// did not report it. Kept so a re-score can still judge the history window.
	UptimeSeconds int64 `json:"uptime_seconds,omitempty"`

	// TargetHitRate is the per-instance recommendation, derived from what this
	// instance has demonstrated rather than taken from a flag, and TargetReason
	// says what it was derived from.
	TargetHitRate float64 `json:"target_hit_rate"`
	TargetReason  string  `json:"target_reason"`

	// The profiles the cost models are built from. A CostModel is a closure and
	// cannot be serialized, so these are what -save preserves and -load rebuilds
	// from; they are also the expensive half of a run. Both sets are kept when
	// available, so a saved file can be re-evaluated under a different -model
	// without contacting the instance again.
	Profiles       []ShardProfile       `json:"profiles,omitempty"`
	SimpleProfiles []SimpleShardProfile `json:"simple_profiles,omitempty"`

	TotalCapacity int64 `json:"total_capacity"`
	TotalSize     int64 `json:"total_size"`

	Rec Recommendation `json:"recommendation"`

	Warnings []string `json:"warnings,omitempty"`
	Err      string   `json:"error,omitempty"`

	// AuthFailed marks a 401/403, which is reported separately from other
	// errors because it invalidates the whole run rather than one step.
	AuthFailed bool `json:"auth_failed,omitempty"`
}

// HeapFraction is the p95 heap as a fraction of RAM, the number the eligibility
// screen is stated in.
func (r *InstanceResult) HeapFraction() float64 {
	if r.RAMBytes <= 0 {
		return 0
	}
	return float64(r.HeapP95) / float64(r.RAMBytes)
}

func (r *InstanceResult) warn(format string, args ...any) {
	r.Warnings = append(r.Warnings, fmt.Sprintf(format, args...))
}

type options struct {
	inventory    string
	url          string
	name         string
	instanceType string
	config       string
	ramBytes     int64

	username string
	password string
	insecure bool
	timeout  time.Duration

	window        string
	heapPct       float64
	sample        time.Duration
	concurrency   int
	schemaProbe   bool
	maxTagKeys    int
	measureHeap   bool
	focus         string
	bytesPerEntry int64
	probePairs    int
	probeValues   int
	idSpanFactor  float64
	model         string
	explain       bool
	verbose       bool

	save      string
	load      string
	dryRun    bool
	listTypes bool

	targetHitRate float64
	format        string

	// currentMaxSize and currentTarget describe a configuration the operator
	// knows the instance is running, for builds that do not publish it.
	currentMaxSize int64
	currentTarget  float64

	// gaugeHeapFactor converts figures read from the cache's bytes gauge
	// (probe deltas, the resident mean) to heap; see resolveBytesPerEntry.
	gaugeHeapFactor float64

	// idSpan, when set, is an operator-supplied upper bound on the series id
	// space that overrides the schema-derived one on every profile. It exists
	// for saved files collected before the span was recorded, which are
	// otherwise costed as fully dispersed.
	idSpan int64

	th Thresholds
}

// applyIDSpan stamps an operator-supplied span onto every profile. It must be
// an upper bound on ids ever created in the database (see KeyProfile.IDSpan);
// a value below the truth understates the bound.
func applyIDSpan(r *InstanceResult, span int64) {
	if span <= 0 {
		return
	}
	for i := range r.Profiles {
		for j := range r.Profiles[i].Keys {
			r.Profiles[i].Keys[j].IDSpan = span
		}
	}
	for i := range r.SimpleProfiles {
		r.SimpleProfiles[i].IDSpan = span
	}
}

// knownConfig returns the cache configuration to audit: the operator's flags
// when given (they describe what the operator knows the instance runs, and
// outrank a build that publishes nothing), otherwise what the instance
// reported, with source saying which. source is where reported came from —
// "instance" for a fresh scrape, or whatever a saved file recorded, so a
// re-score without flags keeps the provenance of a configuration supplied on
// an earlier one.
func (o *options) knownConfig(reported CacheConfig, source string, floor int64) (CacheConfig, string) {
	if source == "" {
		source = "instance"
	}
	if o.currentMaxSize <= 0 && o.currentTarget <= 0 {
		return reported, source
	}
	cfg := reported
	if o.currentMaxSize > 0 {
		cfg.MaxSize = o.currentMaxSize
	}
	if o.currentTarget > 0 {
		cfg.TargetHitRate = o.currentTarget
	}
	if cfg.Floor <= 0 {
		cfg.Floor = floor
	}
	return cfg, "flags"
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	def := Defaults()
	var o options

	flag.StringVar(&o.inventory, "inventory", "", "CSV inventory: name,url,instance_type,config[,bytes_per_entry]")
	flag.StringVar(&o.url, "url", "", "single instance URL (instead of -inventory)")
	flag.StringVar(&o.name, "name", "", "name for the single instance (default: its host)")
	flag.StringVar(&o.instanceType, "instance-type", "", "EC2 instance type for the single instance, e.g. r5.2xlarge")
	flag.StringVar(&o.config, "config", "default", "configuration name for the single instance")
	flag.Var((*byteSize)(&o.ramBytes), "ram-bytes", "override RAM, e.g. 192G or 206158430208 (for instance types not in the table); suffixes are binary")

	flag.StringVar(&o.username, "username", os.Getenv("INFLUX_USERNAME"), "basic auth username (env INFLUX_USERNAME)")
	flag.StringVar(&o.password, "password", os.Getenv("INFLUX_PASSWORD"), "basic auth password (env INFLUX_PASSWORD)")
	flag.BoolVar(&o.insecure, "insecure", false, "skip TLS certificate verification")
	flag.DurationVar(&o.timeout, "timeout", 2*time.Minute, "per-request timeout")

	flag.StringVar(&o.window, "window", "7d", "history window for _internal queries (the monitor RP retains 7d)")
	flag.Float64Var(&o.heapPct, "heap-percentile", 0.95, "percentile of hourly peak heap to use as the steady-state figure")
	flag.DurationVar(&o.sample, "sample", 60*time.Second, "fallback live sample window when _internal has no history (0 disables)")
	flag.IntVar(&o.concurrency, "concurrency", 4, "instances to probe in parallel")
	flag.BoolVar(&o.schemaProbe, "schema", true, "derive bytes-per-entry from the schema via meta queries")
	flag.IntVar(&o.maxTagKeys, "max-tag-keys", 50, "cap on tag keys probed per database (0 = no cap)")
	flag.BoolVar(&o.measureHeap, "measure-heap", false, "measure bytes-per-entry from /debug/pprof/heap (canaries only)")
	flag.StringVar(&o.focus, "focus", "innerLockingPut", "stack frame substring identifying retained cache allocations")
	flag.Var((*byteSize)(&o.bytesPerEntry), "bytes-per-entry", "override bytes-per-entry for every instance, e.g. 4K or 4096")
	flag.IntVar(&o.probePairs, "probe-pairs", 0, "measure entry cost by querying this many of the schema's worst (measurement, tag key) pairs (0 = do not probe)")
	flag.IntVar(&o.probeValues, "probe-values", 3, "tag values to probe per pair")
	flag.BoolVar(&o.verbose, "v", false, "log every HTTP request to stderr with timing and status, plus a per-instance summary")
	flag.BoolVar(&o.explain, "explain", false, "print how the conservation bound decomposes across candidate caps, including j (tag keys drawn on)")
	flag.StringVar(&o.model, "model", "auto", "cost model: auto, conservation (full schema profile), simple (series count + tag key count), or per-entry")
	flag.Float64Var(&o.idSpanFactor, "id-span-factor", 2.0, "series id span as a multiple of current cardinality; must be an upper bound (0 = unknown: every series costed as its own roaring container)")
	flag.Var((*byteSize)(&o.idSpan), "id-span", "upper bound on the series id space (ids ever created in the database), e.g. 250000 or 1M; overrides the schema-derived span on every profile, for -load of files that predate it")
	flag.Float64Var(&o.gaugeHeapFactor, "gauge-heap-factor", 2.0, "multiplier from the tsi1_cache bytes gauge (serialized set sizes) to heap, applied to -probe-pairs and the resident mean; 1.02 for dense sets, ~2 for sets spread over a dozen containers, up to ~11 for one member per container")

	flag.StringVar(&o.save, "save", "", "write the collected data to this JSON file, so it can be re-scored later without contacting the instances")
	flag.StringVar(&o.load, "load", "", "re-score a previously -saved file under the current thresholds; contacts nothing")
	flag.BoolVar(&o.dryRun, "dry-run", false, "run preflight and print what would be collected, then stop")
	flag.BoolVar(&o.listTypes, "list-instance-types", false, "list the instance types with known RAM, and exit")

	// 0.90, not the 0.85 this started at. The hazard is a target above the
	// workload's achievable hit rate, not a high target — measured, 0.99 against
	// a closed working set costs 1.04x the memory of 0.95, while on a 5%-novel
	// workload (ceiling 0.95) 0.99 holds 21x the memory of 0.94 for the same
	// hit rate. A low target's failure is quieter but real: 0.90 settled at 558
	// entries and 0.906 where 700 entries served 0.9486. This value is only the
	// floor; per instance it is raised toward whatever the instance has
	// demonstrated. See RecommendTarget.
	flag.Float64Var(&o.targetHitRate, "target-hit-rate", 0.90,
		"floor for the recommended target hit rate; raised per instance toward the rate it has demonstrated")
	flag.StringVar(&o.format, "format", "table", "output format: table, json or csv")

	// For builds that do not publish the adaptive settings in their config
	// diagnostics. Supplying them turns on the audit of the running
	// configuration and the pinned-shard check, both of which otherwise need the
	// diagnostics.
	flag.Int64Var(&o.currentMaxSize, "current-max-size", 0,
		"series-id-set-cache-max-size the instance is known to run (when its build does not publish it); enables the configuration audit")
	flag.Float64Var(&o.currentTarget, "current-target", 0,
		"series-id-set-cache-target-hit-rate the instance is known to run (when its build does not publish it)")

	flag.Float64Var(&o.th.HealthyLine, "healthy-line", def.HealthyLine, "fraction of RAM below which an instance is healthy")
	flag.Float64Var(&o.th.BudgetFrac, "budget-frac", def.BudgetFrac, "hard ceiling on the cache's share of RAM")
	flag.Float64Var(&o.th.HeadroomShare, "headroom-share", def.HeadroomShare, "fraction of remaining headroom the cache may use")
	flag.Float64Var(&o.th.BenefitHitRate, "benefit-hit-rate", def.BenefitHitRate, "hit rate at or above which the cache already fits")
	flag.Float64Var(&o.th.BenefitEvictionRate, "benefit-eviction-rate", def.BenefitEvictionRate, "evictions per get below which eviction is negligible")
	flag.Int64Var(&o.th.MinUsefulMultiple, "min-useful-multiple", def.MinUsefulMultiple, "multiples of the floor a max size must reach to be worth a restart")
	flag.Int64Var(&o.th.FirstPassCapMultiple, "first-pass-cap-multiple", def.FirstPassCapMultiple, "cap on the first rollout, in multiples of the floor")
	flag.Int64Var(&o.th.Floor, "floor", def.Floor, "configured series-id-set-cache-size")

	flag.Parse()

	if o.th.Floor <= 0 {
		return fmt.Errorf("-floor must be > 0")
	}
	if o.concurrency < 1 {
		o.concurrency = 1
	}

	if o.listTypes {
		listInstanceTypes(o.th)
		return nil
	}

	// Re-scoring a saved collection contacts nothing, so it skips both instance
	// resolution and preflight's RAM check — supplying the RAM that was missing
	// is usually the reason for loading in the first place.
	if o.load != "" {
		run, err := loadRun(o.load)
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "loaded %d instance(s) collected %s (window %s, model %q)\n",
			len(run.Instances), run.CollectedAt.Format(time.RFC3339), run.Collection.Window, run.Collection.Model)
		if o.window != run.Collection.Window || o.maxTagKeys != run.Collection.MaxTagKeys {
			fmt.Fprintf(os.Stderr, "note: -window/-max-tag-keys shape what is collected, not how it is scored; "+
				"the saved values (%s, %d) still apply. Re-collect to change them.\n",
				run.Collection.Window, run.Collection.MaxTagKeys)
		}
		return emit(rescore(run, &o), o)
	}

	instances, err := loadInstances(&o)
	if err != nil {
		return err
	}
	if len(instances) == 0 {
		flag.Usage()
		return fmt.Errorf("no instances: pass -inventory, -url, or -load")
	}

	// Everything that can be checked without touching the network is checked
	// now. Collection against a production instance can run for tens of
	// minutes; discovering a missing -instance-type afterwards, and getting no
	// recommendation for it, is the failure this exists to prevent.
	if err := preflight(instances, &o); err != nil {
		return err
	}
	if o.dryRun {
		describePlan(instances, &o)
		return nil
	}

	if o.schemaProbe && o.bytesPerEntry == 0 && !o.measureHeap {
		fmt.Fprintf(os.Stderr,
			"note: the schema probe runs meta queries (2 + one per tag key, up to %d) against each database.\n"+
				"      These read the index and can be slow on high-cardinality instances. Pass -schema=false\n"+
				"      with -bytes-per-entry, or a bytes_per_entry column in the inventory, to skip them.\n\n",
			o.maxTagKeys)
	}

	results := probeAll(context.Background(), instances, &o)

	// Save before rendering: a formatting error must not cost a 20-minute
	// collection.
	if o.save != "" {
		if err := saveRun(o.save, results, &o); err != nil {
			return fmt.Errorf("saving to %s: %w", o.save, err)
		}
	}

	return emit(results, o)
}

func emit(results []InstanceResult, o options) error {
	switch o.format {
	case "json":
		return emitJSON(results, o)
	case "csv":
		return emitCSV(results)
	case "table":
		return emitTable(results, o)
	default:
		return fmt.Errorf("unknown -format %q", o.format)
	}
}

// listInstanceTypes prints the instance types whose RAM is known, so a failed
// preflight can be fixed without guessing at the spelling. The budget column
// uses the run's own -budget-frac rather than a fixed 5%, so it matches what
// this invocation would actually allot.
func listInstanceTypes(t Thresholds) {
	types := make([]string, 0, len(r5RAMBytes))
	for name := range r5RAMBytes {
		types = append(types, name)
	}
	sort.Slice(types, func(i, j int) bool {
		if r5RAMBytes[types[i]] != r5RAMBytes[types[j]] {
			return r5RAMBytes[types[i]] < r5RAMBytes[types[j]]
		}
		return types[i] < types[j]
	})

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "INSTANCE TYPE\tRAM\tBUDGET (%.3g%% of RAM)\n", t.BudgetFrac*100)
	for _, name := range types {
		ram := r5RAMBytes[name]
		fmt.Fprintf(w, "%s\t%s\t%s\n", name, humanBytes(ram), humanBytes(int64(t.BudgetFrac*float64(ram))))
	}
	w.Flush()
	fmt.Println("\nFor anything not listed, pass -ram-bytes.")
	fmt.Println("The budget shown is the ceiling; an instance close to -healthy-line gets less.")
}

// loadInstances builds the instance list from the inventory file or the
// single-instance flags, resolving RAM for each.
func loadInstances(o *options) ([]InstanceResult, error) {
	var out []InstanceResult

	if o.inventory != "" {
		rows, err := readInventory(o.inventory)
		if err != nil {
			return nil, err
		}
		out = rows
	} else if o.url != "" {
		name := o.name
		if name == "" {
			name = hostOf(o.url)
		}
		out = []InstanceResult{{
			Name:         name,
			URL:          o.url,
			InstanceType: o.instanceType,
			Config:       o.config,
		}}
	}

	for i := range out {
		r := &out[i]
		if o.ramBytes > 0 {
			r.RAMBytes = o.ramBytes
			continue
		}
		if ram, ok := RAMForInstanceType(r.InstanceType); ok {
			r.RAMBytes = ram
		} else if r.InstanceType != "" {
			r.warn("unknown instance type %q; supply -ram-bytes", r.InstanceType)
		} else {
			r.warn("no instance type given; supply -ram-bytes")
		}
	}
	return out, nil
}

// readInventory parses the CSV inventory. The header is required so columns can
// be reordered and the optional bytes_per_entry column omitted.
func readInventory(path string) ([]InstanceResult, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	cr := csv.NewReader(f)
	cr.TrimLeadingSpace = true
	cr.FieldsPerRecord = -1
	cr.Comment = '#'

	recs, err := cr.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}
	if len(recs) < 2 {
		return nil, fmt.Errorf("%s: need a header row and at least one instance", path)
	}

	idx := map[string]int{}
	for i, h := range recs[0] {
		idx[strings.ToLower(strings.TrimSpace(h))] = i
	}
	for _, required := range []string{"name", "url"} {
		if _, ok := idx[required]; !ok {
			return nil, fmt.Errorf("%s: missing required column %q", path, required)
		}
	}

	field := func(rec []string, col string) string {
		i, ok := idx[col]
		if !ok || i >= len(rec) {
			return ""
		}
		return strings.TrimSpace(rec[i])
	}

	var out []InstanceResult
	for lineno, rec := range recs[1:] {
		if len(rec) == 0 || strings.TrimSpace(strings.Join(rec, "")) == "" {
			continue
		}
		r := InstanceResult{
			Name:         field(rec, "name"),
			URL:          field(rec, "url"),
			InstanceType: field(rec, "instance_type"),
			Config:       field(rec, "config"),
		}
		if r.Name == "" || r.URL == "" {
			return nil, fmt.Errorf("%s line %d: name and url are required", path, lineno+2)
		}
		if r.Config == "" {
			r.Config = "default"
		}
		if v := field(rec, "bytes_per_entry"); v != "" {
			b, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("%s line %d: bytes_per_entry: %w", path, lineno+2, err)
			}
			r.BytesPerEntry = b
			r.BytesPerEntrySource = "inventory"
		}
		out = append(out, r)
	}
	return out, nil
}

func hostOf(u string) string {
	s := strings.TrimPrefix(strings.TrimPrefix(u, "http://"), "https://")
	if i := strings.IndexAny(s, "/:"); i > 0 {
		return s[:i]
	}
	return s
}

// probeAll fans out across the fleet with a bounded worker pool.
func probeAll(ctx context.Context, instances []InstanceResult, o *options) []InstanceResult {
	results := make([]InstanceResult, len(instances))
	copy(results, instances)

	sem := make(chan struct{}, o.concurrency)
	var wg sync.WaitGroup

	for i := range results {
		wg.Add(1)
		go func(r *InstanceResult) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			probe(ctx, r, o)
		}(&results[i])
	}
	wg.Wait()

	sort.Slice(results, func(i, j int) bool {
		if results[i].Config != results[j].Config {
			return results[i].Config < results[j].Config
		}
		return results[i].Name < results[j].Name
	})
	return results
}

// probe gathers everything for one instance and classifies it. Each step
// degrades rather than aborting: a missing input produces a warning and, where
// it matters, an UNKNOWN verdict, so a partially reachable fleet still yields a
// usable table.
func probe(ctx context.Context, r *InstanceResult, o *options) {
	c := NewClient(r.URL, o.username, o.password, o.timeout, o.insecure)
	if o.verbose {
		c.Label, c.Verbose = r.Name, vlogf
		started := time.Now()
		vlogf("[%s] === start: %s ===", r.Name, r.URL)
		defer func() {
			n, total, slowest, desc := c.Stats()
			vlogf("[%s] === done in %s: %d requests, %s in HTTP ===",
				r.Name, time.Since(started).Round(time.Millisecond), n, total.Round(time.Millisecond))
			if n > 0 {
				vlogf("[%s]     slowest %s: %s", r.Name, slowest.Round(time.Millisecond), desc)
			}
		}()
	}

	snap, err := c.FetchVars(ctx)
	if err != nil {
		r.Err = err.Error()
		reason := "unreachable"
		if IsAuthError(err) {
			reason = "authentication failed"
			r.AuthFailed = true
		}
		r.Rec = Recommendation{Verdict: VerdictUnknown, Reason: reason}
		return
	}
	vphase(o, r, "collecting: %d TSI shards, %d databases", snap.TSIShardCount(), len(snap.Databases()))

	// What the instance is currently configured to do, which is what makes the
	// capacity statistics interpretable. The pinned check itself runs after the
	// activity history is fetched, since it prefers the windowed rates.
	snap.Cache, r.CacheConfigSource = o.knownConfig(snap.Cache, "instance", o.th.Floor)
	r.CacheConfig = snap.Cache
	if snap.Cache.Adaptive() {
		vphase(o, r, "adaptive sizing is on (%s): floor %d, max %d, target %.2f",
			r.CacheConfigSource, snap.Cache.Floor, snap.Cache.MaxSize, snap.Cache.TargetHitRate)
	} else {
		checkCapacitiesAgainstConfig(r, snap, o.th.Floor)
	}

	// Every _internal figure below is read over -window. If the process has
	// not been up that long, the window spans a previous process, or the
	// source instance for a clone of a data directory, and the figures cannot
	// be attributed to this configuration.
	r.UptimeSeconds = int64(snap.Uptime / time.Second)
	checkWindowAgainstUptime(r, o.window)

	r.TSIShards = snap.TSIShardCount()
	r.TotalShards = int64(len(snap.Shards))
	r.IndexTypes = snap.IndexTypes
	r.TotalCapacity = snap.TotalCapacity()
	r.TotalSize = snap.TotalSize()

	if r.TSIShards == 0 {
		r.Rec = Recommendation{Verdict: VerdictNotTSI, Reason: "no tsi1 shards"}
		return
	}

	// Heap p95 from the monitor's own history; instant memstats is a poor
	// substitute (it is a single sample, not a peak) so it is flagged loudly.
	vphase(o, r, "heap percentile from _internal over %s", o.window)
	if p95, err := c.HeapPercentile(ctx, o.window, o.heapPct); err == nil {
		r.HeapP95 = p95
		r.HeapSource = "_internal"
	} else {
		r.HeapP95 = snap.HeapInuse
		r.HeapSource = "memstats(instant)"
		r.warn("no heap history (%v); using an instant sample, which understates the peak", err)
	}

	// Activity, for the benefit filter.
	vphase(o, r, "cache activity from _internal over %s", o.window)
	var byShard map[string]CacheActivity
	if act, err := c.HistoricalActivity(ctx, o.window); err == nil {
		r.Activity = act
		r.ActivitySource = "_internal/" + o.window
		// Best effort: the pinned check below prefers windowed per-shard rates
		// and falls back to lifetime counters for any shard missing here.
		if m, err := c.HistoricalActivityByShard(ctx, o.window); err == nil {
			byShard = m
		}
	} else if o.sample > 0 {
		if act, serr := c.SampleActivity(ctx, o.sample); serr == nil {
			r.Activity = act
			r.ActivitySource = "live/" + o.sample.String()
			r.warn("no cache history (%v); benefit judged from a %s live sample", err, o.sample)
		} else {
			r.ActivitySource = "none"
			r.warn("no cache activity measured (%v; %v)", err, serr)
		}
	} else {
		r.ActivitySource = "none"
		r.warn("no cache activity measured (%v)", err)
	}

	// Growth with no stopping signal: capacity at the ceiling while the cache is
	// still below target. The memory past saturation buys nothing while the
	// condition lasts.
	if snap.Cache.Adaptive() {
		if pinned, observed := PinnedShards(snap, byShard); len(pinned) > 0 {
			r.PinnedShards = len(pinned)
			r.PinnedHitRate = observed
			basis := "lifetime counters"
			if byShard != nil {
				basis = "the " + o.window + " window"
			}
			r.warn("%d of %d shards are pinned at max-size (%d) while serving %.3f (over %s) against a target of %.2f. "+
				"The growth policy stops only when the hit rate reaches target, so a target above what the "+
				"workload can achieve removes its stopping signal and it grows to the ceiling regardless of "+
				"whether growth still helps. The rate it has settled at is the achievable ceiling on those shards: "+
				"set series-id-set-cache-target-hit-rate just below %.3f. The pin lasts only while the workload's "+
				"novel-predicate rate stays above 1-target and the memory stays inside the modelled worst case, "+
				"so one that comes and goes with the workload may be acceptable as is.",
				len(pinned), snap.TSIShardCount(), snap.Cache.MaxSize, observed, basis, snap.Cache.TargetHitRate, observed)
		}
	}

	// N_active: shards that held any entry at any point in the window, i.e.
	// shards the query workload actually touches. Deliberately "> 0" rather
	// than "> floor": before a rollout every cache is pinned at the fixed size,
	// so a full shard sits exactly at the floor and a "> floor" test would
	// report every instance as having no active shards at all.
	//
	// A large gap between this and the total shard count is sweep exposure: a
	// backfill or downsample pass that touches the idle shards can grow all of
	// them to the cap, and none of them will shrink afterwards.
	if peaks, err := c.PeakShardSizes(ctx, o.window); err == nil {
		for _, p := range peaks {
			if p > 0 {
				r.ActiveShards++
			}
		}
		if r.ActiveShards > 0 && r.TSIShards/max(r.ActiveShards, 1) > 3 {
			r.warn("only %d of %d tsi shards active: exposed to sweep-driven growth (idle shards never shrink)",
				r.ActiveShards, r.TSIShards)
		}
	}

	// Preferred: the conservation model, built from the schema and each shard's
	// series count. It bounds the cache without needing to know which predicates
	// the workload queries, which is the input that varies most between
	// instances and over time.
	model, source := buildCostModel(ctx, c, r, snap, o)
	if o.idSpan > 0 {
		// The operator's bound outranks the schema-derived one; rebuild the
		// model over the stamped profiles.
		applyIDSpan(r, o.idSpan)
		dropWarnings(r, "no series id span")
		model, source = modelFromProfiles(r, o, o.model)
	}
	r.CostModel = source

	// Auth is usually *partial*: /debug/vars is unauthenticated while /query is
	// not, so the run gets shard stats and then silently loses every InfluxQL
	// input. Check once, at the end, rather than at each of the ten call sites.
	if n, ae := c.AuthFailures(); n > 0 {
		r.AuthFailed = true
		r.warn("%d request(s) rejected with %s — heap history, cache activity and the schema probe "+
			"were all unavailable, so this row is a fallback, not a measurement", n, ae.Status)
		r.warn("set -username/-password (or INFLUX_USERNAME/INFLUX_PASSWORD); note /debug/vars is " +
			"served without auth while /query is not, which is why the run got this far")
	}

	// The target is derived from this instance's own evidence: the ceiling when
	// shards are pinned at max-size, otherwise the rate it has already
	// demonstrated, floored at -target-hit-rate.
	observed := r.PinnedHitRate
	if observed == 0 && r.Activity.Sampled {
		observed = r.Activity.HitRate()
	}
	r.TargetHitRate, r.TargetReason = RecommendTarget(observed, r.PinnedShards > 0, o.targetHitRate)

	r.Rec = Classify(r.TSIShards, r.RAMBytes, r.HeapP95, model, r.Activity, o.th)
	auditRunningConfig(r, model, o)
}

// auditRunningConfig checks the configuration the instance is known to run
// against the same budget and model the recommendation used, and records the
// result on r with a warning when it matters. The recommendation sizes from
// scratch; this is what catches a cap already in place that the budget does
// not support.
func auditRunningConfig(r *InstanceResult, model CostModel, o *options) {
	budget := MemoryBudget(r.RAMBytes, r.HeapP95, o.th)
	safeCap := LargestCapacity(budget, o.th.Floor, math.MaxInt64/4, model)
	a, ok := AuditConfig(r.CacheConfig, r.CacheConfigSource, budget, safeCap, model, r.Activity)
	if !ok {
		return
	}
	r.Audit = &a

	switch {
	case a.OverBudget && a.SafeCap > 0:
		r.warn("the configured max-size %d (%s) is %.0fx the largest ladder value the budget permits (%d): the modelled "+
			"worst case at %d is %s against a %s budget (%.1fx). Every shard can reach it, and a shard that stops being "+
			"queried keeps what it grew to.",
			a.MaxSize, a.Source, a.SafeCapMultiple, a.SafeCap, a.MaxSize,
			humanBytes(a.WorstCaseBytes), humanBytes(a.BudgetBytes), a.BudgetMultiple)
	case a.OverBudget:
		r.warn("the configured max-size %d (%s) has a modelled worst case of %s against a %s budget (%.1fx), and no "+
			"ladder value fits the budget at all",
			a.MaxSize, a.Source, humanBytes(a.WorstCaseBytes), humanBytes(a.BudgetBytes), a.BudgetMultiple)
	default:
		r.warn("the configured max-size %d (%s) fits: modelled worst case %s against a %s budget",
			a.MaxSize, a.Source, humanBytes(a.WorstCaseBytes), humanBytes(a.BudgetBytes))
	}
	if a.BelowTarget && r.PinnedShards == 0 {
		r.warn("the windowed hit rate %.3f is below the configured target %.2f and no shard is pinned yet, so growth "+
			"toward max-size %d is still in progress; whether it stops short depends on each shard's ceiling, which "+
			"the aggregate cannot settle",
			a.ObservedHitRate, a.TargetHitRate, a.MaxSize)
	}
}

// checkWindowAgainstUptime warns when the history window is longer than the
// process has been running. The uptime is 0 on builds without the system
// diagnostics block, in which case nothing can be said.
func checkWindowAgainstUptime(r *InstanceResult, window string) {
	if r.UptimeSeconds <= 0 {
		return
	}
	w, err := ParseWindow(window)
	if err != nil {
		return
	}
	up := time.Duration(r.UptimeSeconds) * time.Second
	if w <= up {
		return
	}
	r.warn("the process has been up %s but -window is %s: the _internal history spans a previous process, or the "+
		"source instance if this is a clone of a data directory, joined at a counter reset. The hit rate, activity and "+
		"heap p95 cannot be attributed to the configuration now running. Re-run with -window %s or wait.",
		up.Round(time.Minute), window, shortWindow(up))
}

// shortWindow renders a duration as the largest whole InfluxQL unit that fits,
// for the re-run hint.
func shortWindow(d time.Duration) string {
	switch {
	case d >= 24*time.Hour:
		return fmt.Sprintf("%dd", int64(d/(24*time.Hour)))
	case d >= time.Hour:
		return fmt.Sprintf("%dh", int64(d/time.Hour))
	default:
		return fmt.Sprintf("%dm", max(int64(d/time.Minute), 1))
	}
}

// buildCostModel chooses how this instance's worst-case cache size is bounded.
//
// The conservation model is preferred and needs only the schema plus each
// shard's series count. It is used whenever a schema profile can be built, and
// the per-entry figure is still resolved alongside it for the report — the two
// are complementary: conservation bounds the whole cache, bytes-per-entry
// describes a single entry.
//
// The per-entry model is the fallback for instances where the schema is
// unavailable (meta queries disabled, or an operator-supplied figure with
// -schema=false). It is far looser; the returned label says which was used so a
// recommendation is never silently the weaker one.
func buildCostModel(ctx context.Context, c *Client, r *InstanceResult, snap *VarsSnapshot, o *options) (CostModel, string) {
	// An unset model must not silently select the weakest bound.
	want := o.model
	if want == "" {
		want = "auto"
	}

	// Cheap bytes-per-entry sources only, for now: the operator's figure, a
	// probe, the gauge, the heap profile. The schema-derived estimate is
	// deliberately deferred — it costs one cardinality query per tag key per
	// database, and only the per-entry model actually needs it. Running it up
	// front made -model simple pay the full probe it exists to avoid: measured
	// 12 cardinality queries and 6.6s on a 12-tag-key schema, for a number the
	// simple bound never reads.
	resolveBytesPerEntry(ctx, c, r, snap, o, false)

	if want == "conservation" || want == "auto" {
		if !o.schemaProbe {
			r.warn("conservation model needs the schema probe; -schema=false disables it")
		} else if profiles, err := buildShardProfiles(ctx, c, r, snap, o); err != nil {
			r.warn("conservation model unavailable (%v); falling back", err)
		} else if len(profiles) > 0 {
			r.ShardProfiles = len(profiles)
			r.Profiles = profiles
			if o.explain {
				explain(r, profiles)
			}
			// The schema is cached by now, so filling in the estimate for the
			// report costs nothing — and so is deriving the simple profile,
			// which lets a saved file be re-scored under -model simple and
			// covers any shard the schema probe could not.
			resolveBytesPerEntry(ctx, c, r, snap, o, true)
			if simple, err := buildSimpleProfiles(ctx, c, r, snap, o.idSpanFactor); err == nil {
				r.SimpleProfiles = simple
			}
			return conservationWithCoverage(r, profiles, r.SimpleProfiles)
		}
		if want == "conservation" {
			return nil, "none"
		}
	}

	if want == "simple" || want == "auto" {
		if profiles, err := buildSimpleProfiles(ctx, c, r, snap, o.idSpanFactor); err != nil {
			r.warn("simple model unavailable (%v); falling back", err)
		} else if len(profiles) > 0 {
			r.ShardProfiles = len(profiles)
			r.SimpleProfiles = profiles
			warnIfLacksIDSpan(r, nil, profiles)
			return SimpleModel(profiles), "simple"
		}
		if want == "simple" {
			return nil, "none"
		}
	}

	// Last resort. This is the only model whose answer depends on the
	// schema-derived estimate, so it is the only one that pays for it.
	if r.BytesPerEntry == 0 {
		resolveBytesPerEntry(ctx, c, r, snap, o, true)
	}
	if r.BytesPerEntry > 0 {
		return PerEntryModel(r.TSIShards, r.BytesPerEntry), "per-entry"
	}
	return nil, "none"
}

// conservationWithCoverage builds the conservation model over the profiled
// shards, costs any shard only the simple profile covers with the simple bound,
// and says so. Shards with neither profile are reported too: they contribute
// nothing to the bound, which is the one case the operator has to judge.
func conservationWithCoverage(r *InstanceResult, profiles []ShardProfile, simple []SimpleShardProfile) (CostModel, string) {
	n, series := UncoveredShards(profiles, simple)
	if n > 0 {
		r.warn("the schema probe covered %d of %d tsi shards; %d shard(s) holding %d series are costed with the "+
			"simple bound instead", len(profiles), r.TSIShards, n, series)
	}
	if covered := int64(len(profiles) + n); covered < r.TSIShards {
		r.warn("%d of %d tsi shards have no profile at all and contribute nothing to the bound", r.TSIShards-covered, r.TSIShards)
	}
	warnIfLacksIDSpan(r, profiles, simple)
	return CombinedModel(profiles, simple), "conservation"
}

// warnIfLacksIDSpan says when a profile carries no series id span, which
// happens for files saved before the container term existed and for
// databases whose cardinality could not be read. The bound is then computed
// with every series in its own roaring container: safe, and for large sets
// far above what the instance would really pay.
func warnIfLacksIDSpan(r *InstanceResult, profiles []ShardProfile, simple []SimpleShardProfile) {
	if LacksIDSpan(profiles, simple) && !hasWarning(r, "no series id span") {
		r.warn("some profiles carry no series id span, so their roaring container cost is bounded as one container per " +
			"series (fully dispersed). That is safe but can be far above the real cost on large sets; re-collect with a " +
			"build of this tool that records the span (-id-span-factor) to tighten it")
	}
}

// checkCapacitiesAgainstConfig runs when the config diagnostics do not report
// adaptive sizing, which on a build that predates those diagnostics means only
// that the tool cannot see the setting, not that it is off. The capacity
// statistics settle it: a fixed-size cache gives every shard the same capacity,
// so differing capacities prove adaptive growth, and a uniform capacity that is
// not the configured floor says -floor is wrong for this instance. Either way
// the pinned check could not run and the config snippet's floor is unverified,
// and the operator should know that rather than read the row as a fixed-size
// baseline.
func checkCapacitiesAgainstConfig(r *InstanceResult, snap *VarsSnapshot, floor int64) {
	lo, hi, uniform := snap.CapacitySpread()
	switch {
	case len(snap.Caches) == 0:
	case !uniform:
		r.warn("adaptive sizing appears to be enabled (per-shard capacity ranges from %d to %d) but this build does not "+
			"publish the adaptive settings, so the pinned-shard check could not run, the current max-size and target are "+
			"unknown, and the hit rate below was measured at a mean capacity of %d, not at the floor. The floor in the "+
			"config snippet is the -floor flag, unverified.", lo, hi, snap.TotalCapacity()/int64(len(snap.Caches)))
	case lo != floor:
		r.warn("every shard reports capacity %d but -floor is %d; the config snippet's series-id-set-cache-size and the "+
			"doubling ladder are computed from -floor, so pass -floor %d if that is the configured size", lo, floor, lo)
	}
}

// buildSimpleProfiles gathers the four numbers the simple bound needs per
// shard. One SHOW TAG KEYS and one SHOW SERIES CARDINALITY per database plus
// the free seriesCreate stat — no per-key cardinality probing. The cardinality
// bounds the series id span, which the container term needs; when it cannot be
// read the shard is costed as fully dispersed and the run says so.
func buildSimpleProfiles(ctx context.Context, c *Client, r *InstanceResult, snap *VarsSnapshot, idSpanFactor float64) ([]SimpleShardProfile, error) {
	keysByDB := map[string]int64{}
	spanByDB := map[string]int64{}
	for _, db := range snap.Databases() {
		counts, err := c.FetchTagKeyCounts(ctx, db)
		if err != nil {
			r.warn("tag key count failed for %q: %v", db, err)
			continue
		}
		keysByDB[db] = MaxTagKeys(counts)
		if card, err := c.FetchDatabaseCardinality(ctx, db); err != nil {
			r.warn("series cardinality failed for %q (%v): its shards are costed as fully dispersed", db, err)
		} else {
			spanByDB[db] = SpanFromCardinality(card, idSpanFactor)
		}
	}
	if len(keysByDB) == 0 {
		return nil, fmt.Errorf("no tag key counts could be read")
	}

	var out []SimpleShardProfile
	for id := range snap.Caches {
		sh, ok := snap.Shards[id]
		if !ok || sh.SeriesCreate <= 0 {
			continue
		}
		if t, ok := keysByDB[sh.Database]; ok && t > 0 {
			out = append(out, SimpleShardProfile{ShardID: id, Series: sh.SeriesCreate, TagKeys: t, IDSpan: spanByDB[sh.Database]})
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no shard could be profiled")
	}
	return out, nil
}

// buildShardProfiles turns the schema plus per-shard series counts into the
// (measurement, tag key) profile the conservation bound walks.
//
// A measurement's share of a shard is taken to match its share of the database,
// which the stable-schema premise makes reasonable and which only affects how
// the shard's series are apportioned between measurements — the total is exact.
func buildShardProfiles(ctx context.Context, c *Client, r *InstanceResult, snap *VarsSnapshot, o *options) ([]ShardProfile, error) {
	schemas := map[string]*Schema{}
	for _, db := range snap.Databases() {
		sc, err := c.FetchSchema(ctx, db, o.maxTagKeys)
		if err != nil {
			r.warn("schema probe failed for %q: %v", db, err)
			continue
		}
		if sc.Truncated {
			r.warn("tag key list truncated for %q; the bound may understate", db)
		}
		schemas[db] = sc
	}
	if len(schemas) == 0 {
		return nil, fmt.Errorf("no database schema could be read")
	}

	var out []ShardProfile
	for id := range snap.Caches {
		sh, ok := snap.Shards[id]
		if !ok || sh.SeriesCreate <= 0 {
			continue
		}
		sc, ok := schemas[sh.Database]
		if !ok {
			continue
		}
		total := sc.TotalSeries()
		if total <= 0 {
			continue
		}

		// The id span bounds how many roaring containers one entry's set can
		// occupy; it is a database property, so every key in the shard shares
		// it. See KeyProfile.IDSpan for what 0 means.
		span := idSpanBound(sc, o.idSpanFactor)
		p := ShardProfile{ShardID: id}
		for m, card := range sc.SeriesCardinality {
			inShard := int64(float64(sh.SeriesCreate) * (float64(card) / float64(total)))
			for key, values := range sc.TagValueCount[m] {
				if values > 0 && inShard > 0 {
					p.Keys = append(p.Keys, KeyProfile{
						Measurement:   m,
						Key:           key,
						Values:        values,
						SeriesInShard: inShard,
						IDSpan:        span,
					})
				}
			}
		}
		if len(p.Keys) > 0 {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no shard could be profiled")
	}
	return out, nil
}

// resolveBytesPerEntry fills in B for the instance, in order of trust: a value
// supplied by the operator, a probe, the gauge, the heap profile, then the
// schema-derived worst case.
// allowSchema gates the schema-derived estimate, which is the expensive source:
// one cardinality query per tag key per database. Callers pass true only when
// the value will actually be used, or when the schema is already cached from
// building a cost model. See buildCostModel.
func resolveBytesPerEntry(ctx context.Context, c *Client, r *InstanceResult, snap *VarsSnapshot, o *options, allowSchema bool) {
	if o.bytesPerEntry > 0 {
		r.BytesPerEntry = o.bytesPerEntry
		r.BytesPerEntrySource = "flag"
		return
	}
	if r.BytesPerEntry > 0 { // from the inventory
		return
	}

	// Probing measures the entries the schema says are the most expensive,
	// rather than inferring their cost. It outranks the gauge because the gauge
	// is a mean over whatever happens to be resident, which need not include the
	// worst predicate at all — and the worst predicate is what sizes the cap.
	// Both the probe and the gauge read the cache's bytes statistic, which
	// counts each set at its serialized size: 2 bytes per member and next to
	// nothing per container, against ~96 bytes per container and 2.5 per
	// member in heap. The factor converts to heap. It is a single number for
	// what is really a shape-dependent ratio (1.02x on dense sets, 1.9x on the
	// reference 16-container set, 11x with one member per container), so the
	// source label carries it and the README says when to raise it.
	heapFactor := max(o.gaugeHeapFactor, 1)
	scale := func(gauge int64) int64 { return int64(float64(gauge) * heapFactor) }
	label := func(source string) string {
		if heapFactor == 1 {
			return source
		}
		return fmt.Sprintf("%s×%.1f", source, heapFactor)
	}

	if o.probePairs > 0 {
		if b, n, err := probeWorstEntry(ctx, c, r, snap, o); err != nil {
			r.warn("probe failed (%v); falling back", err)
		} else if b > 0 {
			r.BytesPerEntry = scale(b)
			r.BytesPerEntrySource = label(fmt.Sprintf("probe(%d)", n))
			return
		} else {
			r.warn("probe measured no entries; falling back")
		}
	}

	// The cache's own bytes gauge, when the server publishes it. Free, but it
	// is a *mean* over whatever is currently resident, and a mean is not a safe
	// stand-in for the entry that sizes the cap. On a workload mixing cheap and
	// expensive predicates it lands well below the worst one — measured at
	// 0.18x of the true worst entry on the reference instance in
	// scripts/tsi_cache_experiment — and it is serialized size besides.
	//
	// So it ranks below probing and is used as a convenience default, not as an
	// authority: good for triage and for watching a rollout, not for committing
	// to a max-size on an instance with a wide spread of predicate costs. The
	// warning says so, since the source column alone does not.
	if b, sz := snap.TotalBytes(), r.TotalSize; b > 0 && sz > 0 {
		r.BytesPerEntry = scale(b / sz)
		r.BytesPerEntrySource = label("stat")
		if o.probePairs == 0 {
			r.warn("bytes-per-entry is the mean of resident entries, which understates the worst predicate; use -probe-pairs to measure it")
		}
		return
	}

	if o.measureHeap {
		bytes, err := c.MeasureCacheBytes(ctx, o.focus)
		switch {
		case err != nil:
			r.warn("heap measurement failed (%v); falling back", err)
		case r.TotalSize <= 0:
			r.warn("heap measured but the cache holds no entries; falling back")
		case bytes <= 0:
			r.warn("heap profile attributed no bytes to %q; falling back", o.focus)
		default:
			r.BytesPerEntry = bytes / r.TotalSize
			r.BytesPerEntrySource = "measured"
			return
		}
	}

	if !o.schemaProbe || !allowSchema {
		return
	}

	// Probe each database once, then cost every shard against its own
	// database's schema and its own series count.
	schemas := map[string]*Schema{}
	for _, db := range snap.Databases() {
		sc, err := c.FetchSchema(ctx, db, o.maxTagKeys)
		if err != nil {
			r.warn("schema probe failed for %q: %v", db, err)
			continue
		}
		if sc.Truncated {
			r.warn("tag key list truncated for %q; worst case may be understated", db)
		}
		schemas[db] = sc
	}
	if len(schemas) == 0 {
		return
	}

	var worst, typical int64
	for id := range snap.Caches {
		sh, ok := snap.Shards[id]
		if !ok || sh.SeriesCreate <= 0 {
			continue
		}
		sc, ok := schemas[sh.Database]
		if !ok {
			continue
		}
		w, t := EstimateEntryBytes(sh.SeriesCreate, sc, idSpanBound(sc, o.idSpanFactor))
		if w > worst {
			worst = w
		}
		if t > typical {
			typical = t
		}
	}
	if worst > 0 {
		r.BytesPerEntry = worst
		r.BytesPerEntryTypical = typical
		r.BytesPerEntrySource = "schema-worst"
	}
}

// idSpanBound converts a database's current series cardinality into an upper
// bound on its series id space; see SpanFromCardinality. A schema that could
// not be read yields 0, which every model costs as fully dispersed.
func idSpanBound(sc *Schema, factor float64) int64 {
	if sc == nil {
		return 0
	}
	return SpanFromCardinality(sc.TotalSeries(), factor)
}

// probeWorstEntry measures real cache entries for the (measurement, tag key)
// pairs the schema says are the most expensive, and returns the largest measured
// cost along with the number of entries measured.
//
// Ranking comes from the schema; the number itself comes from the server. That
// split matters: the schema can identify which predicates are dangerous (low
// cardinality on a large measurement) but cannot say what they cost, because
// cost depends on the order the series were created.
func probeWorstEntry(ctx context.Context, c *Client, r *InstanceResult, snap *VarsSnapshot, o *options) (int64, int, error) {
	// Probe the largest shard: entry cost scales with the shard's series count,
	// so the biggest shard bounds the rest.
	var target ShardStat
	for id := range snap.Caches {
		if sh, ok := snap.Shards[id]; ok && sh.SeriesCreate > target.SeriesCreate {
			target = sh
		}
	}
	if target.ShardID == "" || target.Database == "" {
		return 0, 0, fmt.Errorf("no shard with a series count to probe")
	}

	windows, err := c.ShardWindows(ctx, target.Database)
	if err != nil {
		return 0, 0, err
	}
	w, ok := windows[target.ShardID]
	if !ok {
		return 0, 0, fmt.Errorf("shard %s not listed by SHOW SHARDS", target.ShardID)
	}

	sc, err := c.FetchSchema(ctx, target.Database, o.maxTagKeys)
	if err != nil {
		return 0, 0, err
	}
	total := sc.TotalSeries()
	if total <= 0 {
		return 0, 0, fmt.Errorf("no cardinality for %q", target.Database)
	}

	// Rank (measurement, tag key) pairs by how many series one value covers.
	type pair struct {
		measurement, key string
		k                float64
	}
	var pairs []pair
	for m, card := range sc.SeriesCardinality {
		inShard := float64(target.SeriesCreate) * (float64(card) / float64(total))
		for key, values := range sc.TagValueCount[m] {
			if values > 0 {
				pairs = append(pairs, pair{m, key, inShard / float64(values)})
			}
		}
	}
	if len(pairs) == 0 {
		return 0, 0, fmt.Errorf("no (measurement, tag key) pairs to probe")
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].k > pairs[j].k })
	if len(pairs) > o.probePairs {
		pairs = pairs[:o.probePairs]
	}

	idSpan := idSpanBound(sc, o.idSpanFactor)

	var worst int64
	var measured, unmeasured int
	for _, p := range pairs {
		values, err := c.TagValues(ctx, target.Database, p.measurement, p.key, o.probeValues)
		if err != nil {
			r.warn("probe: tag values for %s.%s: %v", p.measurement, p.key, err)
			values = nil
		}

		var best int64
		var got int
		for _, v := range values {
			b, ok, err := c.ProbeEntryBytes(ctx, w, p.measurement, p.key, v)
			if err != nil {
				r.warn("probe %s.%s=%s: %v", p.measurement, p.key, v, err)
				continue
			}
			if !ok {
				// The gauge did not move, so there is nothing to read here. The
				// usual cause is that the value was already resident — and the
				// hottest predicates are the most likely to be, which is exactly
				// why a skipped value must not be scored as cheap.
				continue
			}
			got++
			if b > best {
				best = b
			}
		}

		if got == 0 {
			// Nothing measurable for this pair. Fall back to its bound rather
			// than dropping it: an unmeasurable pair is usually the hottest one,
			// and omitting it would bias the answer low, which is the unsafe
			// direction.
			best = EntryBytes(int64(p.k), idSpan)
			unmeasured++
		}

		measured += got
		if best > worst {
			worst = best
		}
	}

	if unmeasured > 0 {
		r.warn("probe: %d of %d pairs had no measurable value (already cached?); used the schema bound for those",
			unmeasured, len(pairs))
	}
	return worst, measured, nil
}

// explain prints, for a range of candidate caps, how the conservation bound
// decomposes — and in particular j, the number of tag keys the fill draws on.
//
// j is the term that cannot be inferred from an instance's series count and tag
// key count: it depends on the sorted tag-value cardinalities. Two schemas
// identical in every other respect can differ several-fold in j at the same
// capacity, which is the difference between a cap being affordable and not.
func explain(r *InstanceResult, profiles []ShardProfile) {
	fmt.Printf("\nCONSERVATION BREAKDOWN — %s (%d shards profiled)\n\n", r.Name, len(profiles))

	// Total series across the profiled shards, used to express the payload as an
	// effective tag key count. A (measurement, tag key) pair contributes 2*S_m,
	// so dividing the payload by 2*SigmaS gives the number that is directly
	// comparable to T and to the runbook's key budget — unlike the raw pair
	// count, which on a multi-measurement database (_internal, say) runs into
	// the dozens while contributing almost nothing.
	var sigmaS int64
	for _, p := range profiles {
		for _, k := range p.Keys {
			if k.SeriesInShard > 0 {
				sigmaS += k.SeriesInShard
				break // SeriesInShard is per measurement; count each shard once
			}
		}
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "n\tK(n)+1 (RULE OF THUMB)\tEFF KEYS (ACTUAL)\tPAIRS DRAWN\tSCHEMA ENTRIES\tPAYLOAD\tCONTAINERS\tOVERHEAD\tTOTAL\tOVERHEAD SHARE")

	for _, n := range []int64{100, 1000, 10000, 100000, 1000000} {
		var agg CacheBreakdown
		var pairs, kn float64
		for _, p := range profiles {
			b := ShardCacheBreakdown(n, p.Keys)
			agg.Payload += b.Payload
			agg.Containers += b.Containers
			agg.Overhead += b.Overhead
			agg.SchemaEntries += b.SchemaEntries
			agg.Entries += b.Entries
			agg.Total += b.Total
			pairs += b.KeysDrawn

			// K(n): (measurement, tag key) pairs with fewer than n values. Only
			// these can be drawn in full, so K(n)+1 bounds the pairs drawn — the
			// rule of thumb an operator can evaluate from schema knowledge,
			// without this tool. Taken as the per-shard maximum, since the cap
			// applies per shard.
			if c := NarrowKeyCount(n, p.Keys); c > kn {
				kn = c
			}
		}

		// Effective tag keys: payload expressed in units of "one whole tag key".
		var effKeys float64
		if sigmaS > 0 {
			effKeys = float64(agg.Payload) / float64(bytesPerSeriesID*sigmaS)
		}
		if agg.Total == 0 {
			continue
		}

		// Schema entries below n*shards means the payload and container terms
		// have stopped growing; the overhead term keeps growing with the cap,
		// because an entry exists for any value a query names.
		placed := fmt.Sprintf("%d", agg.SchemaEntries)
		if agg.SchemaEntries < agg.Entries {
			placed += fmt.Sprintf(" (payload exhausted; %d more at overhead only)", agg.Entries-agg.SchemaEntries)
		}

		fmt.Fprintf(w, "%d\t%.2f\t%.2f\t%.2f\t%s\t%s\t%s\t%s\t%s\t%.0f%%\n",
			n, kn+1, effKeys, pairs, placed,
			humanBytes(agg.Payload), humanBytes(agg.Containers), humanBytes(agg.Overhead), humanBytes(agg.Total),
			100*float64(agg.Overhead)/float64(agg.Total))
	}
	w.Flush()
}

// savedRun is the on-disk form of a collection. Only *collected* data is
// stored; thresholds are not, because re-tuning them without re-collecting is
// the point of saving. What the thresholds were at collection time is recorded
// for context only.
type savedRun struct {
	Version     int              `json:"version"`
	CollectedAt time.Time        `json:"collected_at"`
	Collection  savedCollection  `json:"collection"`
	Thresholds  Thresholds       `json:"thresholds_at_collection"`
	Instances   []InstanceResult `json:"instances"`
}

// savedCollection records the settings that shaped what was gathered, as
// opposed to how it was scored. Changing one of these does require a re-run,
// and the load path says so rather than silently applying a stale value.
type savedCollection struct {
	Window      string  `json:"window"`
	HeapPct     float64 `json:"heap_percentile"`
	MaxTagKeys  int     `json:"max_tag_keys"`
	SchemaProbe bool    `json:"schema_probe"`
	Model       string  `json:"model"`
	ProbePairs  int     `json:"probe_pairs"`
}

const savedRunVersion = 1

func saveRun(path string, results []InstanceResult, o *options) error {
	run := savedRun{
		Version:     savedRunVersion,
		CollectedAt: time.Now().UTC(),
		Collection: savedCollection{
			Window: o.window, HeapPct: o.heapPct, MaxTagKeys: o.maxTagKeys,
			SchemaProbe: o.schemaProbe, Model: o.model, ProbePairs: o.probePairs,
		},
		Thresholds: o.th,
		Instances:  results,
	}
	b, err := json.MarshalIndent(run, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o600); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "saved collected data for %d instance(s) to %s\n", len(results), path)
	return nil
}

func loadRun(path string) (*savedRun, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var run savedRun
	if err := json.Unmarshal(b, &run); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	if run.Version != savedRunVersion {
		return nil, fmt.Errorf("%s: saved with format version %d, this build reads %d",
			path, run.Version, savedRunVersion)
	}
	if len(run.Instances) == 0 {
		return nil, fmt.Errorf("%s: no instances in file", path)
	}
	return &run, nil
}

// rescore recomputes recommendations from saved data under the current
// thresholds and -model, contacting nothing.
//
// RAM may be supplied now even though it was missing at collection time — the
// case this exists for — via -ram-bytes, -instance-type, or an inventory matched
// by name.
func rescore(run *savedRun, o *options) []InstanceResult {
	byName := map[string]InstanceResult{}
	if o.inventory != "" {
		if rows, err := readInventory(o.inventory); err == nil {
			for _, row := range rows {
				byName[row.Name] = row
			}
		}
	}

	out := make([]InstanceResult, len(run.Instances))
	copy(out, run.Instances)

	for i := range out {
		r := &out[i]

		// Re-resolve RAM from whatever the operator has supplied this time.
		if row, ok := byName[r.Name]; ok && row.InstanceType != "" {
			r.InstanceType, r.Config = row.InstanceType, row.Config
			if ram, ok := RAMForInstanceType(row.InstanceType); ok {
				r.RAMBytes = ram
			}
		}
		if o.instanceType != "" {
			r.InstanceType = o.instanceType
			if ram, ok := RAMForInstanceType(o.instanceType); ok {
				r.RAMBytes = ram
			}
		}
		if o.ramBytes > 0 {
			r.RAMBytes = o.ramBytes
		}

		// Warnings from collection are kept — they describe the data, which has
		// not changed — but the recommendation is recomputed from scratch. An
		// operator-supplied id span replaces a missing or stale one first.
		if o.idSpan > 0 {
			applyIDSpan(r, o.idSpan)
			dropWarnings(r, "no series id span")
		}
		model, source := modelFromProfiles(r, o, run.Collection.Model)
		r.CostModel = source
		if o.explain && len(r.Profiles) > 0 {
			explain(r, r.Profiles)
		}
		r.Rec = Classify(r.TSIShards, r.RAMBytes, r.HeapP95, model, r.Activity, o.th)

		// A configuration supplied now, for a file whose build published none,
		// is the case the audit flags exist for. Supplying one on a re-score
		// cannot run the pinned check, which needs the per-shard counters the
		// file does not hold.
		r.CacheConfig, r.CacheConfigSource = o.knownConfig(r.CacheConfig, r.CacheConfigSource, o.th.Floor)
		r.Audit = nil
		// The audit is recomputed, so its earlier warnings would be stale or
		// duplicated; every other warning describes the data and is kept.
		dropWarnings(r, "the configured max-size", "growth toward max-size")
		auditRunningConfig(r, model, o)
		if r.UptimeSeconds > 0 && !hasWarning(r, "the process has been up") {
			checkWindowAgainstUptime(r, run.Collection.Window)
		}

		// The target is recomputed too, from the saved evidence and the current
		// -target-hit-rate. An earlier version copied the saved value through,
		// which for a file collected before the field existed was 0.00 — and a
		// target of 0.0 in the config snippet disables adaptive sizing.
		observed := r.PinnedHitRate
		if observed == 0 && r.Activity.Sampled {
			observed = r.Activity.HitRate()
		}
		r.TargetHitRate, r.TargetReason = RecommendTarget(observed, r.PinnedShards > 0, o.targetHitRate)

		// A file saved by an older version has no capacity-spread evidence, but
		// the totals it does carry can still show adaptive sizing: a fixed size
		// makes the capacity total a multiple of the shard count.
		if !r.CacheConfig.Adaptive() && r.TSIShards > 0 && r.TotalCapacity%r.TSIShards != 0 &&
			!hasWarning(r, "adaptive sizing appears to be enabled") {
			r.warn("adaptive sizing appears to be enabled (the capacity total %d is not a multiple of %d shards) but the "+
				"build did not publish the adaptive settings, so the pinned-shard check did not run and the hit rate was "+
				"measured at a mean capacity of %d, not at the floor. The config snippet's floor is the -floor flag, unverified.",
				r.TotalCapacity, r.TSIShards, r.TotalCapacity/r.TSIShards)
		}
	}
	return out
}

// dropWarnings removes every warning containing any of the substrings, for a
// re-score that is about to recompute what they described.
func dropWarnings(r *InstanceResult, substrs ...string) {
	kept := r.Warnings[:0]
	for _, w := range r.Warnings {
		stale := false
		for _, s := range substrs {
			if strings.Contains(w, s) {
				stale = true
				break
			}
		}
		if !stale {
			kept = append(kept, w)
		}
	}
	r.Warnings = kept
}

// hasWarning reports whether one of r's warnings contains s, so a rescore can
// avoid repeating a warning the collection already recorded.
func hasWarning(r *InstanceResult, s string) bool {
	for _, w := range r.Warnings {
		if strings.Contains(w, s) {
			return true
		}
	}
	return false
}

// modelFromProfiles rebuilds a cost model from saved profiles, honouring the
// current -model. It never falls back to a model whose data was not collected;
// it says so instead, because silently scoring with a weaker bound than asked
// for is how a saved file turns into a wrong answer.
// collectedWith is the -model the file was gathered under, used only to explain
// why the data for a different one is absent.
func modelFromProfiles(r *InstanceResult, o *options, collectedWith string) (CostModel, string) {
	want := o.model
	if want == "" {
		want = "auto"
	}

	if want == "conservation" || want == "auto" {
		if len(r.Profiles) > 0 {
			return conservationWithCoverage(r, r.Profiles, r.SimpleProfiles)
		}
		if want == "conservation" {
			r.warn("-model conservation needs a schema profile, which this file does not contain "+
				"(it was collected with -model %q). Re-collect with -model conservation or auto.", collectedWith)
			return nil, "none"
		}
	}
	if want == "simple" || want == "auto" {
		if len(r.SimpleProfiles) > 0 {
			warnIfLacksIDSpan(r, nil, r.SimpleProfiles)
			return SimpleModel(r.SimpleProfiles), "simple"
		}
		if want == "simple" {
			r.warn("-model simple needs tag key counts, which this file does not contain "+
				"(it was collected with -model %q). Re-collect to use it.", collectedWith)
			return nil, "none"
		}
	}
	if r.BytesPerEntry > 0 {
		return PerEntryModel(r.TSIShards, r.BytesPerEntry), "per-entry"
	}
	return nil, "none"
}

// byteSize is a flag value accepting either a plain byte count or a size with a
// unit suffix: 5G, 1.5GiB, 512M, 4K, 68719476736.
//
// Suffixes are binary (1024-based), including the ambiguous short and "B" forms
// — G, GB and GiB all mean 2^30. That matches the r5 RAM table, where a
// "64 GiB" instance is 64<<30, and humanBytes' output, which labels 2^30 as "G".
// The result is that a figure read off the report can be passed straight back
// in, which is worth more here than honouring the SI reading of "GB".
type byteSize int64

var byteSizeUnits = map[string]int64{
	"": 1, "b": 1,
	"k": 1 << 10, "kb": 1 << 10, "kib": 1 << 10,
	"m": 1 << 20, "mb": 1 << 20, "mib": 1 << 20,
	"g": 1 << 30, "gb": 1 << 30, "gib": 1 << 30,
	"t": 1 << 40, "tb": 1 << 40, "tib": 1 << 40,
	"p": 1 << 50, "pb": 1 << 50, "pib": 1 << 50,
}

func (b *byteSize) String() string {
	if b == nil || *b == 0 {
		return "0"
	}
	return humanBytes(int64(*b))
}

func (b *byteSize) Set(s string) error {
	s = strings.TrimSpace(s)
	if s == "" {
		return fmt.Errorf("empty size")
	}

	// Split the leading number from the unit. Done by hand rather than with a
	// regexp so the two halves can be reported separately when either is wrong.
	i := 0
	for i < len(s) && (s[i] == '+' || s[i] == '-' || s[i] == '.' || (s[i] >= '0' && s[i] <= '9')) {
		i++
	}
	num, unit := s[:i], strings.ToLower(strings.TrimSpace(s[i:]))
	if num == "" {
		return fmt.Errorf("%q has no number", s)
	}

	mult, ok := byteSizeUnits[unit]
	if !ok {
		return fmt.Errorf("unknown unit %q in %q (use K, M, G, T, or none for bytes)", unit, s)
	}

	// Parsed as a float so 1.5G works; a size is not required to be integral in
	// its own unit.
	v, err := strconv.ParseFloat(num, 64)
	if err != nil {
		return fmt.Errorf("%q is not a number", num)
	}
	if v < 0 {
		return fmt.Errorf("%q is negative", s)
	}
	bytes := v * float64(mult)
	if bytes > float64(math.MaxInt64) {
		return fmt.Errorf("%q overflows int64", s)
	}

	*b = byteSize(bytes)
	return nil
}

// validModels are the accepted -model values. An unrecognised one used to fall
// through every branch and silently land on the weakest bound.
var validModels = []string{"auto", "conservation", "simple", "per-entry"}

// preflight rejects a run that cannot produce a recommendation, before any
// collection happens.
//
// It reports every problem it finds rather than the first, because fixing one
// and rediscovering the next after another long collection is the same failure
// again. Only conditions knowable without the network belong here; anything
// requiring a request stays in probe.
func preflight(instances []InstanceResult, o *options) error {
	var problems []string
	add := func(format string, args ...any) {
		problems = append(problems, fmt.Sprintf(format, args...))
	}

	if !slices.Contains(validModels, o.model) {
		add("-model %q is not one of %s", o.model, strings.Join(validModels, ", "))
	}
	if o.heapPct <= 0 || o.heapPct > 1 {
		add("-heap-percentile must be in (0, 1], got %v", o.heapPct)
	}
	if o.idSpanFactor < 0 {
		add("-id-span-factor must be >= 0, got %v", o.idSpanFactor)
	}
	// 0 is "unset" (options built without the flag) and reads as 1.
	if o.gaugeHeapFactor != 0 && o.gaugeHeapFactor < 1 {
		add("-gauge-heap-factor must be >= 1 (the gauge never exceeds heap), got %v", o.gaugeHeapFactor)
	}
	if o.th.Floor <= 0 {
		add("-floor must be > 0, got %d", o.th.Floor)
	}
	if o.th.HealthyLine <= 0 || o.th.HealthyLine > 1 {
		add("-healthy-line must be in (0, 1], got %v", o.th.HealthyLine)
	}
	if o.th.BudgetFrac <= 0 {
		add("-budget-frac must be > 0, got %v", o.th.BudgetFrac)
	}

	// Flag combinations that guarantee no cost model, and so no recommendation.
	if o.model == "conservation" && !o.schemaProbe {
		add("-model conservation needs the schema probe, but -schema=false disables it")
	}
	if o.model == "per-entry" && !o.schemaProbe && o.bytesPerEntry == 0 && !o.measureHeap {
		add("-model per-entry has no source for bytes-per-entry: pass -bytes-per-entry, " +
			"or enable -schema, or use -measure-heap")
	}

	// RAM is the one required input knowable before collection, and the one
	// most easily forgotten: without it every instance ends at UNKNOWN however
	// much was gathered.
	// minPlausibleRAM catches the unit-omission typo — "-ram-bytes 64" meaning
	// 64G — which suffix support makes more likely, not less. No InfluxDB host
	// has under a gibibyte, and the resulting budget would be a few bytes.
	const minPlausibleRAM = 1 << 30

	var noRAM, tinyRAM []string
	for _, in := range instances {
		switch {
		case in.RAMBytes <= 0:
			noRAM = append(noRAM, in.Name)
		case in.RAMBytes < minPlausibleRAM:
			tinyRAM = append(tinyRAM, fmt.Sprintf("%s (%s)", in.Name, humanBytes(in.RAMBytes)))
		}
	}
	if len(tinyRAM) > 0 {
		add("implausibly small RAM for %d instance(s): %s\n"+
			"      -ram-bytes takes a byte count unless given a unit: 64 is 64 bytes, 64G is 64 GiB.",
			len(tinyRAM), strings.Join(tinyRAM, ", "))
	}
	if len(noRAM) > 0 {
		shown := noRAM
		if len(shown) > 8 {
			shown = append(append([]string{}, shown[:8]...), fmt.Sprintf("… and %d more", len(noRAM)-8))
		}
		add("no RAM known for %d instance(s): %s\n"+
			"      Set -instance-type (see -list-instance-types), -ram-bytes, or an instance_type\n"+
			"      column in the inventory. Without it no recommendation can be produced.",
			len(noRAM), strings.Join(shown, ", "))
	}

	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("preflight failed, nothing was collected:\n  - %s", strings.Join(problems, "\n  - "))
}

// describePlan prints what a run would do without doing it.
func describePlan(instances []InstanceResult, o *options) {
	fmt.Printf("Preflight OK. %d instance(s), model %q.\n\n", len(instances), o.model)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "INSTANCE\tTYPE\tRAM\tCONFIG\tURL")
	for _, in := range instances {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
			in.Name, dash(in.InstanceType), humanBytes(in.RAMBytes), in.Config, in.URL)
	}
	w.Flush()

	fmt.Println("\nPer instance this run would issue:")
	fmt.Println("  1 request   /debug/vars")
	fmt.Println("  3 queries   _internal (heap p95, cache activity, per-shard peaks)")
	switch o.model {
	case "simple":
		fmt.Println("  2 queries   SHOW TAG KEYS and SHOW SERIES CARDINALITY, per database")
	case "per-entry":
		fmt.Printf("  2 queries   per database, plus 1 per tag key (up to %d) — the slow part\n", o.maxTagKeys)
	default:
		fmt.Printf("  2 queries   per database, plus 1 per tag key (up to %d) — the slow part\n", o.maxTagKeys)
		fmt.Println("              (-model simple replaces this with one SHOW TAG KEYS per database)")
	}
	if o.probePairs > 0 {
		fmt.Printf("  %d queries   entry-cost probes (%d pairs x %d values)\n",
			o.probePairs*o.probeValues, o.probePairs, o.probeValues)
	}
	fmt.Println("\nRun without -dry-run to collect. Add -save <file> to keep the raw data.")
}

// vlogf writes one verbose line to stderr. Stderr rather than stdout so the
// report stays pipeable into jq or a spreadsheet while -v is on.
func vlogf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
}

// vphase logs a step boundary, so a long run shows which phase it is in rather
// than only which request it is on.
func vphase(o *options, r *InstanceResult, format string, args ...any) {
	if !o.verbose {
		return
	}
	vlogf("[%s] --- "+format, append([]any{r.Name}, args...)...)
}

// --- Output -----------------------------------------------------------------

func emitTable(results []InstanceResult, o options) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

	fmt.Fprintln(w, "INSTANCE\tTYPE\tCONFIG\tRAM\tHEAP p95\t%RAM\tTSI SH\tB/ENTRY\tSRC\tHIT\tBUDGET\tMODEL\tWORST CASE\tMAX SIZE\tVERDICT")
	for _, r := range results {
		hit := "-"
		if r.Activity.Sampled && r.Activity.Hits+r.Activity.Misses > 0 {
			hit = fmt.Sprintf("%.3f", r.Activity.HitRate())
		}
		maxSize := "-"
		if r.Rec.MaxSize > 0 {
			maxSize = strconv.FormatInt(r.Rec.MaxSize, 10)
			if r.Rec.CappedByFirstPass {
				maxSize += "*"
			}
		}
		pctRAM := "-"
		if r.RAMBytes > 0 && r.HeapP95 > 0 {
			pctRAM = fmt.Sprintf("%.0f%%", r.HeapFraction()*100)
		}

		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			r.Name, dash(r.InstanceType), r.Config,
			humanBytes(r.RAMBytes), humanBytes(r.HeapP95), pctRAM,
			r.TSIShards,
			humanBytes(r.BytesPerEntry), dash(r.BytesPerEntrySource),
			hit, humanBytes(r.Rec.BudgetBytes), dash(r.CostModel),
			humanBytes(r.Rec.WorstCaseBytes), maxSize, string(r.Rec.Verdict))
	}
	if err := w.Flush(); err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("* capped by -first-pass-cap-multiple, not by the memory budget: room to raise in a later pass.")
	fmt.Println("WORST CASE and BUDGET are heap bytes; the tsi1_cache bytes gauge reports serialized set sizes")
	fmt.Println("and reads below the worst case (1.02x on dense sets, up to ~11x on dispersed ones).")

	// Per-configuration rollup.
	fmt.Println()
	fmt.Println("PER-CONFIGURATION ROLLUP (the minimum over each configuration's eligible members)")
	fmt.Println()

	w = tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "CONFIG\tMEMBERS\tELIGIBLE\tMAX SIZE\tTARGET HIT RATE\tBINDING INSTANCE\tEXCLUDED")
	for _, rc := range RollUp(results) {
		maxSize, target := "-", "-"
		if rc.MaxSize > 0 {
			maxSize = strconv.FormatInt(rc.MaxSize, 10)
			target = fmt.Sprintf("%.2f", rc.TargetHitRate)
			if rc.TargetBinding != rc.Binding {
				target += " (" + rc.TargetBinding + ")"
			}
		}
		fmt.Fprintf(w, "%s\t%d\t%d\t%s\t%s\t%s\t%s\n",
			rc.Config, rc.Members, rc.Eligible, maxSize, target,
			dash(rc.Binding), excludedSummary(rc.Excluded))
	}
	if err := w.Flush(); err != nil {
		return err
	}

	// Config snippet for each configuration with a recommendation.
	for _, rc := range RollUp(results) {
		if rc.MaxSize == 0 {
			continue
		}
		fmt.Printf("\n[%s]  # canary: %s\n", rc.Config, rc.Binding)
		fmt.Printf("  series-id-set-cache-size = %d\n", o.th.Floor)
		fmt.Printf("  series-id-set-cache-max-size = %d\n", rc.MaxSize)
		fmt.Printf("  series-id-set-cache-target-hit-rate = %.2f\n", rc.TargetHitRate)
		fmt.Printf("  series-id-set-cache-shrink-conservatism = 2.5\n")
		// Say where the target came from: a number derived from one member's
		// measured ceiling reads very differently from a default, and the
		// operator should know which they are being handed.
		for _, r := range results {
			if r.Name == rc.TargetBinding {
				fmt.Printf("  # target: %s\n", r.TargetReason)
				break
			}
		}
	}

	// The audit of a configuration already in place, when one is known. It
	// gets its own block because it answers a different question from the
	// table — not "what should this run" but "is what it runs safe".
	var audited bool
	for _, r := range results {
		if r.Audit == nil {
			continue
		}
		if !audited {
			fmt.Println()
			fmt.Println("RUNNING CONFIGURATION AUDIT")
			fmt.Println()
			w = tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintln(w, "INSTANCE\tMAX SIZE\tTARGET\tSOURCE\tWORST CASE\tBUDGET\tOVER\tSAFE CAP\tMULTIPLE\tWINDOWED HIT")
			audited = true
		}
		a := r.Audit
		over := "no"
		if a.OverBudget {
			over = fmt.Sprintf("%.1fx", a.BudgetMultiple)
		}
		hit := "-"
		if a.ObservedHitRate > 0 {
			hit = fmt.Sprintf("%.3f", a.ObservedHitRate)
			if a.BelowTarget {
				hit += " (below target)"
			}
		}
		fmt.Fprintf(w, "%s\t%d\t%.2f\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			r.Name, a.MaxSize, a.TargetHitRate, a.Source,
			humanBytes(a.WorstCaseBytes), humanBytes(a.BudgetBytes), over,
			dashInt(a.SafeCap), dashMultiple(a.SafeCapMultiple), hit)
	}
	if audited {
		if err := w.Flush(); err != nil {
			return err
		}
	}

	// An auth failure invalidates the whole run, so it gets its own banner
	// rather than being one warning among many.
	var authFailed []string
	for _, r := range results {
		if r.AuthFailed {
			authFailed = append(authFailed, r.Name)
		}
	}
	if len(authFailed) > 0 {
		fmt.Printf("\nAUTHENTICATION FAILED on %d instance(s): %s\n", len(authFailed), strings.Join(authFailed, ", "))
		fmt.Println("  Nothing below is based on real data for those. Set -username/-password")
		fmt.Println("  (or INFLUX_USERNAME/INFLUX_PASSWORD) and re-run; -v shows the exact response.")
	}

	// Warnings and errors last, so they are the final thing on screen.
	var any bool
	for _, r := range results {
		if r.Err == "" && len(r.Warnings) == 0 {
			continue
		}
		if !any {
			fmt.Println("\nWARNINGS")
			any = true
		}
		if r.Err != "" {
			fmt.Printf("  %s: ERROR: %s\n", r.Name, r.Err)
		}
		for _, warn := range r.Warnings {
			fmt.Printf("  %s: %s\n", r.Name, warn)
		}
	}

	fmt.Println()
	fmt.Println("A restart is required for any change; SIGHUP does not reload TSI settings.")
	return nil
}

func excludedSummary(m map[Verdict]int) string {
	if len(m) == 0 {
		return "-"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, string(k))
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, m[Verdict(k)]))
	}
	return strings.Join(parts, " ")
}

func emitJSON(results []InstanceResult, o options) error {
	out := struct {
		Instances     []InstanceResult `json:"instances"`
		Configs       []ConfigRollup   `json:"configs"`
		TargetHitRate float64          `json:"target_hit_rate"`
		Thresholds    Thresholds       `json:"thresholds"`
	}{results, RollUp(results), o.targetHitRate, o.th}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

func emitCSV(results []InstanceResult) error {
	w := csv.NewWriter(os.Stdout)
	defer w.Flush()

	if err := w.Write([]string{
		"name", "instance_type", "config", "ram_bytes", "heap_p95_bytes",
		"tsi_shards", "active_shards", "bytes_per_entry", "bytes_per_entry_source",
		"cost_model", "worst_case_bytes", "hit_rate", "budget_bytes", "max_size", "verdict", "reason",
	}); err != nil {
		return err
	}
	for _, r := range results {
		hit := ""
		if r.Activity.Sampled && r.Activity.Hits+r.Activity.Misses > 0 {
			hit = fmt.Sprintf("%.4f", r.Activity.HitRate())
		}
		if err := w.Write([]string{
			r.Name, r.InstanceType, r.Config,
			strconv.FormatInt(r.RAMBytes, 10), strconv.FormatInt(r.HeapP95, 10),
			strconv.FormatInt(r.TSIShards, 10), strconv.FormatInt(r.ActiveShards, 10),
			strconv.FormatInt(r.BytesPerEntry, 10), r.BytesPerEntrySource,
			r.CostModel, strconv.FormatInt(r.Rec.WorstCaseBytes, 10),
			hit, strconv.FormatInt(r.Rec.BudgetBytes, 10),
			strconv.FormatInt(r.Rec.MaxSize, 10), string(r.Rec.Verdict), r.Rec.Reason,
		}); err != nil {
			return err
		}
	}
	return w.Error()
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func dashInt(n int64) string {
	if n <= 0 {
		return "-"
	}
	return strconv.FormatInt(n, 10)
}

func dashMultiple(m float64) string {
	if m <= 0 {
		return "-"
	}
	return fmt.Sprintf("%.0fx", m)
}

// humanBytes formats a byte count for the table. Binary units, because the
// inputs are RAM sizes and Go heap figures.
func humanBytes(b int64) string {
	if b <= 0 {
		return "-"
	}
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%dB", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit && exp < 4; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%c", float64(b)/float64(div), "KMGTP"[exp])
}
