# tsi_cache_sizer

Computes a safe per-instance and per-configuration value for
`series-id-set-cache-max-size` across a fleet of InfluxDB 1.x instances.

It reads only from InfluxQL and the `/debug` endpoints, and **writes nothing**.
The method it implements is documented in `TSI_ADAPTIVE_CACHE_SIZING_METHOD.md`
at the repository root; the flags map one to one onto the thresholds described
there.

```bash
go build ./cmd/tsi_cache_sizer

./tsi_cache_sizer -inventory fleet.csv
./tsi_cache_sizer -url http://host:8086 -instance-type r5.2xlarge
```

## Inventory

CSV with a header row. `instance_type`, `config` and `bytes_per_entry` are
optional; `#` starts a comment line.

```csv
name,url,instance_type,config,bytes_per_entry
node-a,http://10.0.1.10:8086,r5.2xlarge,medium,
node-b,http://10.0.1.11:8086,r5.8xlarge,large,4096
```

Supply `bytes_per_entry` once a canary has produced a measured value (see
`-measure-heap`) to skip the schema probe and its meta queries entirely.

## What it reads

| Source | Used for | Cost |
|---|---|---|
| `/debug/vars` | shard inventory, index type, per-shard series counts, cache counters, the `bytes` gauge, memstats | free |
| `SELECT ... FROM "_internal"."monitor"."runtime"` | heap p95 over the window | trivial |
| `SELECT ... FROM "_internal"."monitor"."tsi1_cache"` | windowed hit/miss/eviction, per-shard peaks | trivial |
| `SHOW SERIES CARDINALITY` / `SHOW TAG KEYS` / `SHOW TAG VALUES CARDINALITY` | schema profile, series id span, schema-derived bytes-per-entry | **meta queries; the expensive part** (the simple model needs only the first two, once per database) |
| `/debug/pprof/heap?debug=1` | measured bytes-per-entry (`-measure-heap`) | one profile |

The monitor retention policy keeps 7 days, so `-window` above `7d` returns
nothing extra.

## How the cap is chosen: conservation of cardinality, costed in heap

By default the tool does **not** size from a per-entry cost. It uses the
partition identity: a tag key's values divide a measurement's series exactly
once, so caching all of them holds `S` series' worth of roaring payload
regardless of the key's cardinality. Filling a large cap forces you onto
progressively wider keys with progressively cheaper entries, so
`capacity × worst entry` is structurally impossible.

The identity is stated in series; the bound is stated in heap bytes, and the
two are not the same thing. The cache's `bytes` gauge, and the earlier form of
this bound, counted each set at its *serialized* size: 2 bytes per member and
almost nothing per roaring container. Live heap pays more, and the difference
is not small for small or spread-out sets:

| set shape | gauge | heap | ratio |
|---|---|---|---|
| empty set | 40 B | 134 B | 3.4× |
| 1 id | 44 B | 210 B | 4.8× |
| 500 ids across 16 containers (the reference `rack_rr`) | 1 072 B | 2 007 B | 1.9× |
| 100 ids, one per container | 440 B | 4 879 B | **11.1×** |
| 4 096 ids in one container | 8 234 B | 8 397 B | 1.02× |

So the bound charges three things per key:

```
bound(C) = Σ over keys taken, narrowest first, of
             take · ( 2.5·S_m/V           payload: 2.5 B per member, size-class slack included
                    + 96·c                 c = min(S_m/V, ceil(idSpan/65536)) roaring containers, 96 B each
                    + 352 )                the entry itself, with an empty set, plus a 64 B string allowance
         + (C − schema entries) · 352      entries for values that do not exist, up to the cap
```

Inputs are each shard's `seriesCreate`, the schema's tag-value cardinalities,
and an upper bound on the database's series id span (its cardinality times
`-id-span-factor`). **Query patterns do not appear**, which is what lets a
result from one instance transfer to another.

Measured at C = 1600 against the worst cache the schema can produce, where
"measured" is the gauge and therefore a floor on heap:

| | bytes | over gauge |
|---|---|---|
| measured (gauge) | 6.8 MB | — |
| conservation bound, heap-calibrated | ~15 MB | 2.1× |
| `cap × worst entry` | 200.4 MB | 29.3× |

**Two assumptions to know about.** The fill costs each key's entries at
`S/V` series each, that is, it assumes a tag key's series divide evenly across
its values. The identity pins a key's total at `S` only once *every* value of
the key is taken; for a key taken in part, one dominant value can hold nearly
`S` where the model charges `S/V`. So the conservation figure is the worst case
under uniform tag values, and the bound that holds under any skew is the simple
model's `2.5·S·min(T, n)`. The gap is small when the cap exceeds the number of
tag keys, because every narrow key is then taken in full: about 3% on the
reference production profile, where 1600 entries cover all 161 keys of the
largest shard. It grows on a schema where a large measurement has a key with far
more values than the cap.

The second is the id span. Series ids are never reused and interleave across
the eight series-file partitions, so the number of 65 536-id blocks a set can
touch is `ceil(ids ever created / 65536)`. The tool bounds that from the
database's current cardinality times `-id-span-factor` (default 2). When no
span is known — a saved file from before it was recorded, or a database whose
cardinality could not be read — every series is costed as its own container.
That is safe and can be very loose: on the reference shard the simple bound is
10× higher without the span than with it. The run warns, and `-id-span` lets
you supply a bound by hand for a `-load`.

Shards the schema probe cannot cover are costed with the simple bound rather
than dropped, and the run says how many and how many series they hold. A shard
with neither profile contributes nothing, and the run says that too.

The `MODEL` column says which bound was used (`conservation`, `simple` or
`per-entry`), and `WORST CASE` is the modelled figure at the recommended cap.
Budget and worst case are **live-heap bytes**, as `HeapInUse` reports them;
with the default GC target the resident cost is roughly double, which is what
the 50% healthy line allows for. The `bytes` gauge reads below the worst case
by the ratios in the table above, so after rollout compare it against the
worst case with that in mind, or alert on `capacity` and `size`.

### The cheap model: `-model simple`

Needs only each shard's `seriesCreate`, one `SHOW TAG KEYS` and one
`SHOW SERIES CARDINALITY` per database — no per-key cardinality probe, so no
meta query per tag key:

```
bytes(n) <= 2.5*S*min(T, n)  +  96*min(S*min(T, n), n*C)  +  A*n
              payload             containers, C = ceil(idSpan/65536)   overhead
```

`A = 352` is the fixed heap cost of a cache entry: 152 bytes of element,
evictor node and map slot, 136 for an empty `SeriesIDSet` (the wrapper plus the
roaring bitmap struct), plus a 64-byte allowance for the interned
measurement/key/value strings. Measured in live heap, an entry with an empty
set costs 286 bytes plus its strings; the cache's gauge reports 192 plus
strings for the same entry.

Note that **`A*n` on its own is not a bound** — it omits the payload, which is
conserved per tag key rather than per entry, and it is violated at every measured
capacity (by 47× at n=4). All three terms are needed, and the container term is
the one the earlier form of this bound lacked: it is bounded by the series
drawn on and by `C` containers per entry, and without the span only the first
bound applies.

Below the crossover the schema-bounded terms dominate and the cap is a weak
lever; above it the overhead grows without limit, because an entry exists for
any value a query names whether or not the value exists.

| model | bound at n = 1600 | vs gauge | inputs |
|---|---|---|---|
| conservation | ~15 MB | 2.1× | full schema profile + id span |
| simple | ~25 MB | 3.6× | series count + tag key count + cardinality |
| per-entry | 213 MB | 31× | bytes-per-entry |

`-model auto` (the default) takes conservation when the schema is readable, then
simple, then per-entry.

### The recommended target hit rate is derived, not fixed

`-target-hit-rate` is a **floor** (default 0.90), not the answer. The target's job
is to stop growth at saturation, and where that sits is a property of the
workload, so the recommendation comes from what each instance has demonstrated:

| evidence | recommendation |
|---|---|
| shards pinned at max-size | just under the rate they settled at — that rate *is* the achievable ceiling. Authoritative, and may go below the floor. |
| a hit rate above the floor at the current size | just under it. LRU is a stack algorithm, so a larger cache cannot serve a lower hit rate on the same workload; anything already achieved stays achievable. |
| neither | the floor. |

```
[default]  # canary: prod-clone
  series-id-set-cache-max-size = 1600
  series-id-set-cache-target-hit-rate = 0.92
  # target: just below the 0.948 ceiling observed on shards pinned at max-size
```

For a shared configuration the **lowest** recommendation among eligible members
binds, for the same reason the tightest memory budget binds `max-size`: a target
above one member's ceiling removes that member's stopping signal. The rollup
names the binding member when it differs from the one that bound `max-size`.

The second rule has a limit worth knowing. The stack-algorithm argument holds
per shard, and the rate the tool sees is the instance aggregate, dominated by
its busiest shards. A target just under the aggregate can still sit above the
ceiling of a cold shard, which then pins at `max-size`. That costs memory
inside the modelled worst case, not safety, and the pinned check reports it
after rollout. The same applies across a fleet: the lowest ceiling binds a
shared target, and the instances that could have gone higher pay in hit rate,
not memory.

Why 0.90 rather than the 0.85 this started at: the hazard is a target above the
achievable hit rate, not a high target. Measured, 0.99 against a closed working
set costs 1.04× the memory of 0.95, while on a 5 %-novel workload (ceiling
0.95) 0.99 holds 21× the memory of 0.94 for the same hit rate
(`TestAdaptiveTarget_SameWorkloadDifferentTargets`). Meanwhile a low target
fails quietly but really — 0.90 settled at 558 entries and 0.906 where 700
entries served 0.9486.

`-load` derives the target again from the saved evidence under the current
`-target-hit-rate`; it never copies a saved value through. (An earlier version
did, and a file saved before the field existed produced a snippet with a target
of `0.00`, which disables adaptive sizing.)

### Pinned at max-size: growth with no stopping signal

The run flags any shard whose **capacity has reached `max-size` while the hit
rate is still below target**:

```
prod-clone: 3 of 12 shards are pinned at max-size (1600) while serving 0.948
  (over the 7d window) against a target of 0.99. The growth policy stops only
  when the hit rate reaches target, so a target above what the workload can
  achieve removes its stopping signal and it grows to the ceiling regardless of
  whether growth still helps. The rate it has settled at is the achievable
  ceiling on those shards: set series-id-set-cache-target-hit-rate just below
  0.948. The pin lasts only while the workload's novel-predicate rate stays
  above 1-target and the memory stays inside the modelled worst case, so one
  that comes and goes with the workload may be acceptable as is.
```

The growth rule's only brake is `hit rate < target`; it has no notion of
marginal gain per entry. If some fraction of gets are for predicates never seen
before — which no cache size can serve — the target may sit above anything the
workload can reach, and the brake never engages.

Measured on a workload with an achievable ceiling of 0.95, hit rate saturated at
capacity 700; the climb from there to 8192 cost **11.9× the bytes for +0.00044
hit rate** (`TestHitRate_SaturatesWellBeforeMaxSize`). By contrast the climb *to*
saturation was 1.41× the bytes for +0.108.

Three things to know about the condition:

- **It is not permanent.** Both shrink gates require the cache to be at or
  above target, and neither can pass while the novel rate exceeds `1 − target`.
  When that rate drops, the gates open and the existing shrink policy sheds the
  cache back to its working set at up to 1024 entries per window, about four
  days for a 16 384-entry cache at 2 gets/sec
  (`TestAdaptiveTarget_PinnedCacheRecoversWhenNovelRateDrops`).
- **A shard that stops being queried never shrinks at all**, because shrink is
  driven from the read path. Idle shards hold whatever they grew to, which is
  why `max-size` is sized against every shard and not only the active ones.
- **The rate is measured over the `_internal` window, per shard**, not from the
  lifetime counters. The policy acts on a recent windowed rate, and the lifetime
  ratio carries every miss of the climb, so it reads low for a long time after a
  cache reaches the ceiling. Lifetime is the fallback when `_internal` has no
  history for a shard.

The diagnosis is the fix: the rate a pinned cache settles at *is* the workload's
achievable ceiling, so set the target just under it. This requires a build that
publishes the adaptive settings in its config diagnostics. On older builds the
check cannot run, so the tool looks at the capacity statistics instead: a
fixed-size cache gives every shard the same capacity, and differing capacities
prove adaptive sizing is on. When it finds that, it says so, and it says that
the hit rate in the table was measured at the mean capacity it computes rather
than at the floor, and that the floor in the config snippet is the `-floor` flag
rather than anything read from the instance. A uniform capacity that is not
`-floor` gets a warning of its own.

### Auditing a configuration that is already running

The sizing procedure works from scratch. It does not, on its own, say whether a
cap already in place is safe — and a cap set by hand, or by an earlier method,
can sit far above what the budget supports while the table reports a clean
recommendation beside it. So whenever the running configuration is known, the
run also prices it against the same budget and model.

This is the production clone profiled in `prod-47728f5b-us-east-1-data-3-clone.json`
(an r5.xlarge, 251 TSI shards, heap p95 at 48 % of RAM). Its build publishes no
adaptive settings, so the sizing table alone said nothing about what was already
running. The instance is in fact configured with
`series-id-set-cache-max-size = 100000` and a target of 0.95.

The file was saved before the tool recorded the series id span, so on its own it
now re-scores as `TOO_TIGHT` with a warning that every series is being costed as
its own roaring container. The database is a stable IoT population of about
100 000 series across 251 shards, so a span of 250 000 (current cardinality with
a generous churn allowance) is a defensible bound to supply by hand; re-collect
to have the tool record it. Re-scoring with the span and the known
configuration:

```bash
tsi_cache_sizer -load prod-47728f5b-us-east-1-data-3-clone.json \
  -id-span 250000 -current-max-size 100000 -current-target 0.95
```

```
RUNNING CONFIGURATION AUDIT

INSTANCE   MAX SIZE  TARGET  SOURCE  WORST CASE  BUDGET  OVER   SAFE CAP  MULTIPLE  WINDOWED HIT
localhost  100000    0.95    flags   9.3G        332.1M  28.7x  1600      62x       0.904 (below target)

WARNINGS
  localhost: the schema probe covered 199 of 251 tsi shards; 52 shard(s) holding 78 series
    are costed with the simple bound instead
  localhost: the configured max-size 100000 (flags) is 62x the largest ladder value the
    budget permits (1600): the modelled worst case at 100000 is 9.3G against a 332.1M
    budget (28.7x). Every shard can reach it, and a shard that stops being queried keeps
    what it grew to.
  localhost: the windowed hit rate 0.904 is below the configured target 0.95 and no shard
    is pinned yet, so growth toward max-size 100000 is still in progress; whether it
    stops short depends on each shard's ceiling, which the aggregate cannot settle
```

Reading that row for this instance:

- **`WORST CASE` 9.3G against `BUDGET` 332.1M.** The budget is small because the
  heap already sits 2 points under the 50 % healthy line, so the headroom share
  is all that is left. At 100 000 entries per shard, `-explain` puts 6.5G in
  per-entry overhead, 1.0G in roaring container structure and 74M in payload:
  the schema can back 19.1M of the 25.1M entries with a real tag value and the
  rest cost overhead only. The earlier form of this bound said 5.9G for the
  same cap; the difference is heap-calibrated constants and the container term.
  This is an OOM path, not a rounding error.
- **`SAFE CAP` 1600, `MULTIPLE` 62x.** The largest ladder value the budget
  permits is 1600, at 252M worst case, and it is the recommendation as well.
  The configured cap is 62× that. With a span of 1M the safe cap would be 800,
  and with 10M nothing on the ladder fits — which is why the span is an input
  worth measuring rather than guessing high.
- **`WINDOWED HIT` 0.904 (below target).** Over the 7-day window the instance
  served 0.904 against a target of 0.95, at a mean capacity of 1 241 — growth is
  in progress. Nothing is pinned yet because nothing has reached 100 000: the
  doubling ladder from 100 needs about 303 k misses per shard, roughly 17 weeks
  at this instance's query rate, so a weekly canary would not have seen the
  plateau. Whether growth stops short of the cap depends on each shard's
  achievable ceiling, which the aggregate cannot settle.

What this instance needs, from the audit: bring `max-size` down to 1600, keep
the target at or below the 0.90 floor until per-shard windowed rates say
otherwise, re-collect so the span is measured rather than assumed, and let the
clone run a full window on its own before trusting the 7-day figures (see the
next section). The audit is not persuadable by hit rate: the memory bound holds
whatever the target does.

`SOURCE` is `instance` when the build publishes the adaptive settings in its
config diagnostics, or `flags` when they were supplied. The flags exist for
builds that publish nothing. They also switch on the pinned-shard check, which
needs to know `max-size` to recognise a shard sitting at it. On a `-load` they
audit but cannot run the pinned check, since the saved file does not carry
per-shard counters.

The audit's `OVER` column is the worst case over the budget; `MULTIPLE` is the
configured cap over the largest ladder value that fits. The JSON output carries
the whole audit under `config_audit`.

### History that predates the process

Every `_internal` figure is read over `-window`. If the process has not been up
that long, the window spans a previous process — or, for a clone of a data
directory, the source instance, since `_internal` is an ordinary database in
that directory — joined at a counter reset that `non_negative_difference`
silently drops. The hit rate, activity and heap p95 then cannot be attributed
to the configuration now running. The run reads `system.uptime` from
`/debug/vars` and warns when the window exceeds it, naming a window that fits:

```
prod-clone: the process has been up 26h0m0s but -window is 7d: the _internal
  history spans a previous process, or the source instance if this is a clone
  of a data directory, joined at a counter reset. ... Re-run with -window 1d
  or wait.
```

### Entries beyond the schema: there is no "saturates"

An earlier version marked instances whose whole schema fit the budget with `+`
and called them safe at any cap. That was wrong. The schema bounds the payload
and the containers, not the number of entries: a query for a tag value that
does not exist still creates an entry — the file set returns an empty set
iterator for an absent value, the merge is a set iterator, and `Put` stores it
— at the fixed overhead each, and nothing checks existence first. A dashboard
variable pointing at a decommissioned host, a template resolving to a value
from another database, or a typo each add one. On the reference instance,
whose schema-bounded terms come to about 525 MB, a cap of 100 000 000 would
admit tens of gigabytes per shard of such entries.

So every bound charges `A` for every entry up to the cap, `-explain` reports
how many of a cap's entries the schema can back and how many are "at overhead
only", and the ladder walk always finds a finite cap.

## How bytes-per-entry is resolved

Still reported, and still useful for understanding a single predicate, auditing
the model, and the `per-entry` fallback when the schema is unavailable.

In order of trust, first hit wins:

1. `-bytes-per-entry`, or the inventory column — an operator-supplied figure.
2. **`-probe-pairs N`** (source `probe`) — the accurate path, and the one to use
   when the answer matters. See below.
3. **The `bytes` stat** (source `stat`), divided by total occupancy. Exact, free,
   and zero-effort — but it is a *mean*, see the caveat below.
4. `-measure-heap` — parses the heap profile and divides the bytes attributed to
   `innerLockingPut` by total occupancy. Sampled rather than exact; useful to
   audit the gauge.
5. The schema bound — computed, never measured. The fallback for builds without
   the gauge.

### Measured accuracy, and the gauge-to-heap factor

The probe and the `stat` source both read the cache's `bytes` gauge, which
counts each set at its serialized size. Heap is larger by a ratio that depends
on the set's shape (see the table at the top): 1.02× for a dense set, 1.9× for
the reference 16-container sets, 11× for one member per container. The tool
multiplies gauge-derived figures by `-gauge-heap-factor` (default 2.0) and
labels the source with it (`probe(5)×2.0`, `stat×2.0`). Raise it for a
database with a large id span and small sets; a heap profile (`-measure-heap`)
is the only source that needs no factor.

All four against the same instance (1M series, one shard, worst entry
131 317 B in the gauge, about 132 200 B in heap — from
`scripts/tsi_cache_experiment`):

| source | result | vs. heap worst | note |
|---|---|---|---|
| `probe` | 131 277 B → ×2.0 = 262 554 B | 1.99× | measures the worst predicates; the factor covers shape |
| schema bound, `-id-span-factor 1` | 132 960 B | 1.01× | heap-calibrated, assumes no churn |
| schema bound, `-id-span-factor 2` (default) | 135 936 B | 1.03× | the churn allowance costs little on a dense set |
| `stat` | 23 170 B → ×2.0 = 46 340 B | **0.35× — under** | a mean, not a maximum |

**The `stat` mean is not a safe worst-case figure** even after the factor. It
averages whatever is resident, cheap entries included, so on a mixed workload
it lands well below the predicate that actually sizes the cap. It is fine for
watching a rollout and for order-of-magnitude triage; it is the wrong input for
choosing `max-size` on an instance with a wide spread of predicate costs. Probe
when the number matters, and prefer the schema bound when the id span is known.

### Probing

`-probe-pairs N` ranks (measurement, tag key) pairs by how many series one value
covers, then for the worst N it queries up to `-probe-values` real values on the
largest shard and reads the exact cost from the gauge delta.

The probe query uses a 1 ms window at the shard's start, so it selects one shard
and reads essentially no data — the tag predicate is resolved when the shard's
iterator is built, before any points are read. Measured cost on a 250 000-series
predicate: **0.57 s, no disk scan**.

Two things to know:

- **It is not side-effect free.** It adds entries to that shard's cache, which
  may evict others under LRU. It is read-only with respect to data.
- **A value that is already resident cannot be measured** — the gauge does not
  move. Those are skipped, and if no value of a pair is measurable the pair
  contributes its *schema bound* rather than nothing, because the hottest
  predicates are the most likely to be resident and dropping them would bias the
  answer low. The run warns when this happens.

### The schema bound

For each (measurement, tag key), estimate the series one entry covers and bound
its cost:

```
cost <= A + 96n + min(2.5k, 8192n),  n = min(k, ceil(idSpan/65536))
```

`A` is the entry's fixed cost (352). `96n` is the fixed heap cost of its
roaring containers, which the gauge charges at 2 bytes each and which dominates
for small sets spread across the id space. The last term is the payload, at the
lesser of the array rate (2.5 bytes per member, size-class slack included) and
the bitmap ceiling; an array container near its 4 096-member limit costs as
much as a bitmap once its slice is rounded up, which is why the ceiling applies
per container rather than only past the crossover. Against the strided
reference layouts, measured in live heap, the bound is within 1.5×.

`-id-span-factor` converts current cardinality into an upper bound on the series
ID space. IDs are never reused in 1.x, so the span is the number of series *ever*
created; the default of 2.0 tolerates a database that has created twice what it
now holds. **Understating it understates cost**, so raise it if series are
frequently dropped and recreated. Setting it to 0 means "unknown", which costs
every series as its own container: safe, and much looser.

What no schema query can tell you is whether a tag value's series are clustered
or smeared through the ID space — that depends on the order they were written.
The bound covers the smeared case, and a clustered instance's real cost can sit
up to ~8× below it. Only probing, or a heap profile, distinguishes the two.

## Verdicts

| Verdict | Meaning |
|---|---|
| `NOT_TSI` | no tsi1 shards; nothing to size |
| `NO_HEADROOM` | heap p95 at or above the healthy line |
| `NO_BENEFIT` | already at or above the benefit hit rate with negligible eviction |
| `TOO_TIGHT` | a safe cap exists but is too near the floor to justify a restart |
| `UNKNOWN` | a required input was missing; see the warnings |
| `OK` | candidate, at the printed max size |

A `*` on the max size means the first-pass cap bound it, not the memory budget:
there is room to raise it in a later pass once the first one is proven. The JSON
output carries both values: `MaxSize` is the recommendation and `RawMaxSize` is
the largest ladder value the budget permits, so the room is visible rather than
implied.

## Flags worth knowing

```
-save prod.json              keep the collected data for later re-scoring
-load prod.json              re-score saved data; contacts nothing
-dry-run                     preflight and print the plan, then stop
-list-instance-types         instance types with known RAM, and exit
-v                           log every HTTP request to stderr: status, timing, statement
-model auto                  auto | conservation | simple | per-entry
-probe-pairs 5               measure the worst N predicates directly (recommended)
-probe-values 3              tag values to try per pair
-id-span-factor 2.0          series id span as a multiple of current cardinality (0 = unknown, fully dispersed)
-id-span 250000              explicit id span bound; overrides the schema's, for -load of files that predate it
-gauge-heap-factor 2.0       multiplier from the bytes gauge (serialized) to heap for -probe-pairs and stat
-window 7d                   history window for _internal queries
-current-max-size N          max-size the instance is known to run, for builds that do not publish it
-current-target T            target-hit-rate the instance is known to run; with the above, enables the audit
-heap-percentile 0.95        percentile of hourly peak heap to treat as steady state
-schema=false                skip the meta queries (requires -bytes-per-entry)
-measure-heap                measure bytes-per-entry from the heap profile
-sample 60s                  fallback live window when _internal has no history
-format table|json|csv
-concurrency 4
```

Policy thresholds — `-healthy-line`, `-budget-frac`, `-headroom-share`,
`-benefit-hit-rate`, `-benefit-eviction-rate`, `-min-useful-multiple`,
`-first-pass-cap-multiple`, `-floor` — default to the values argued for in the
method document. Changing them changes the safety argument.

## Before a long run: preflight and `-dry-run`

Everything checkable without the network is checked before any collection, and
**all** problems are reported at once — fixing one and rediscovering the next
after another long run is the same failure twice:

```
$ tsi_cache_sizer -url http://prod:8086
error: preflight failed, nothing was collected:
  - no RAM known for 1 instance(s): prod
      Set -instance-type (see -list-instance-types), -ram-bytes, or an instance_type
      column in the inventory. Without it no recommendation can be produced.
```

RAM is the input most easily forgotten and the only required one knowable up
front: without it every instance ends at `UNKNOWN` however much was gathered.
Preflight also rejects an unrecognised `-model` (which previously fell through
to the weakest bound), out-of-range thresholds, and flag combinations that
cannot yield a cost model — `-model conservation` with `-schema=false`, say.

`-list-instance-types` prints the types with known RAM and what each would be
allotted at the current `-budget-frac`.

`-ram-bytes` and `-bytes-per-entry` take a plain byte count or a size with a
suffix — `192G`, `1.5GiB`, `512M`, `4K`. **Suffixes are binary**: `G`, `GB` and
`GiB` all mean 2³⁰, matching the RAM table (a "64 GiB" instance is `64<<30`) and
the report's own output, so a figure read off the report can be pasted back in.
A value with no suffix is bytes, so `-ram-bytes 64` is 64 *bytes* — preflight
rejects implausibly small RAM rather than letting that through.

`-dry-run` runs preflight and reports what the run *would* issue, then stops:

```
Per instance this run would issue:
  1 request   /debug/vars
  3 queries   _internal (heap p95, cache activity, per-shard peaks)
  2 queries   per database, plus 1 per tag key (up to 50) — the slow part
              (-model simple replaces this with one SHOW TAG KEYS per database)
```

## Collect once, re-score many times (`-save` / `-load`)

Collection against a production instance can take tens of minutes. `-save`
writes the collected data to JSON; `-load` re-scores it under different
thresholds, a different `-model`, or a different instance type — **contacting
nothing**. Measured: 5.19 s to collect, 0.004 s to re-score.

```bash
tsi_cache_sizer -url http://prod:8086 -instance-type r5.8xlarge -save prod.json
tsi_cache_sizer -load prod.json -budget-frac 0.01     # re-tune the budget
tsi_cache_sizer -load prod.json -model simple         # compare bounds
tsi_cache_sizer -load prod.json -instance-type r5.large   # what if it were smaller?
tsi_cache_sizer -load prod.json -explain              # the bound's decomposition, from the saved profiles
```

Both profile sets are saved when available, so a file collected under
`-model auto` can be re-scored as `conservation` or `simple` later. A file
collected under `-model simple` has no schema profile, and asking for
conservation says so rather than quietly using a weaker bound:

```
prod: -model conservation needs a schema profile, which this file does not
      contain (it was collected with -model "simple"). Re-collect with
      -model conservation or auto.
```

**Thresholds are not restored from the file** — re-tuning them is the point.
What they were at collection time is recorded for context. `-window` and
`-max-tag-keys` shape *what is gathered* rather than how it is scored, so
changing them requires a re-run; the load path says so.

The save happens before rendering, so a formatting error cannot cost a long
collection.

## Debugging a run (`-v`)

`-v` logs every HTTP request to **stderr** — status, elapsed, response size, and
the InfluxQL statement — plus a per-instance summary naming the slowest request.
stdout stays clean, so `-format json -v | jq` still works.

```
[prod] === start: http://host:8086 ===
[prod] 200        2ms    13.3K  /debug/vars
[prod] --- collecting: 2 TSI shards, 2 databases
[prod] --- heap percentile from _internal over 7d
[prod] 401         0s      55B  /query db=_internal SELECT max("HeapInUse") FROM ...
[prod]       schema "prod": 1 measurements, 5 tag keys -> 5 cardinality queries
[prod] === done in 42ms: 22 requests, 40ms in HTTP ===
[prod]     slowest 5ms: /query db=prod SHOW TAG VALUES CARDINALITY ON "prod" WITH KEY = "k02"
```

Every line carries the instance name, since `-concurrency` above 1 interleaves
them; use `-concurrency 1` for a clean trace.

### Is it auth, or is it just slow?

**Auth on InfluxDB 1.x is partial**: `/debug/vars` is served *without*
authentication while `/query` is not. So a run against a secured instance gets
the shard statistics, then 401s on every InfluxQL step and degrades to a weaker
model — which looks like a successful run with some warnings, not like a
credential problem.

The tool now counts 401/403 across the whole run and reports it as a banner
above the warnings:

```
AUTHENTICATION FAILED on 1 instance(s): prod-clone
  Nothing below is based on real data for those. Set -username/-password
  (or INFLUX_USERNAME/INFLUX_PASSWORD) and re-run; -v shows the exact response.
```

**Slow, rather than broken**, looks different: `-v` shows the statement sitting
there, and the summary names it. The schema probe is almost always the culprit —
it issues one `SHOW TAG VALUES CARDINALITY` per tag key per database, announced
up front as `-> N cardinality queries`. If that is the problem, `-model simple`
needs only one `SHOW TAG KEYS` per database, or `-schema=false` with
`-bytes-per-entry` skips meta queries entirely.

A request that exceeds `-timeout` (default 2m) says so explicitly rather than
reporting a bare failure.

## Caveats

- **The per-configuration value is the minimum over that configuration's
  eligible members.** If that minimum is too low to be useful, split the
  configuration rather than raising the value.
- **`ACT SH` counts shards that have held any entry in the window.** A large gap
  to `TSI SH` is sweep exposure: a backfill or downsample pass can grow the idle
  shards to the cap, and a shard that stops being queried never shrinks.
- **The output is a starting point for a canary, not a fleet rollout.** Hold
  each canary for a full weekly cycle; peak usage is reached over days, and a
  freshly restarted instance always looks fine because every cache resets to the
  floor.
- **Any change requires a restart.** `SIGHUP` does not reload TSI settings.
