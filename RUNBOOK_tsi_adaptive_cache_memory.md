# Runbook: Sizing the Adaptive TSI Series-ID Cache Without OOMing a Node

**Applies to:** InfluxDB 1.x (`master-1.x`) builds containing
`e1f7dcc265cbc34a04a447cf74f213cba5d273ad` (feat: adaptive sizing for the TSI
tag-value series-ID cache) and `70aa5b6d8fdbeac3772151de60bcf0532cd421cd`
(fix: permit small TSI series caches to grow). TSI index only — no effect on
`inmem`.

**Audience:** Support / SRE responding to a request to enable or raise
`series-id-set-cache-max-size`.

**Goal:** Decide a safe value for `series-id-set-cache-max-size` on a node that
is stable today, and be able to defend the number.

**Which part do I need?**

- **One instance, or a handful, with shell + pprof access:** Sections 1-4, then 6.
- **Hundreds or thousands of instances, no per-instance profiling:** Sections 1
  and 2 for background, then **Section 5 (Fleet rollout)**, which replaces the
  per-instance procedure with heuristics computable from InfluxQL and
  `/debug/vars` alone.
- Section 7 (Traps) applies to both. Read it before building any alert.

---

## 1. Background: what the setting does and why it is risky

`TagValueSeriesIDCache` (`tsdb/index/tsi1/cache.go`) is a per-shard LRU mapping
`{measurement, tagKey, tagValue} -> *tsdb.SeriesIDSet` (a roaring bitmap). It is
consulted from exactly one place — `Index.TagValueSeriesIDIterator`
(`tsdb/index/tsi1/index.go:1130`) — so it accelerates **query-time index
resolution only**. It does not reduce TSM data reads.

### Enabling

Adaptive sizing turns on only when **both** are non-zero:

```toml
[data]
  series-id-set-cache-size              = 100   # I: floor, and the starting capacity
  series-id-set-cache-max-size          = 0     # M: ceiling. 0 = adaptive disabled
  series-id-set-cache-target-hit-rate   = 0.0   # T: 0 = adaptive disabled
  series-id-set-cache-shrink-conservatism = 2.5 # z: sigmas below at-target eviction mean
```

Config validation (`tsdb/config.go:315`) requires `M > I > 0` and
`T` in `[0, 1)`. Setting only one of `M`/`T` is rejected at startup.

### The five facts that make memory hard to predict

1. **Capacity is counted in entries, not bytes.** Nothing in `cache.go` ever
   inspects entry size. An entry's cost is set by how many series that one tag
   value matches. Two shards at the same capacity can differ 1000x in bytes.

2. **The cache is per shard.** `tsdb.NewIndex` is called from `shard.go:359`, so
   every open TSI shard has an independent cache that grows on its own evidence.
   Worst case is `M x (number of open TSI shards)`.

3. **Shards are never unloaded.** There is no cold-shard eviction path in
   `tsdb.Store`. Once open, a shard and its cache stay resident.

4. **Shrink is read-driven and gated.** `checkShrink` runs only from `Get`. A
   shard that grew while hot and then rolls out of the query range freezes at
   its high-water mark permanently. Growth doubles on demand; shrink needs a
   full window of Gets (`~n·ln(1/(1−T))`, 2.3n at target 0.90), hit rate >=
   target, AND windowed evictions below `mu - z*sigma`, and then sheds at most
   `min(size/2, 1024)` entries. The post-shrink cooldown runs concurrently with
   the next window, so once the gates open a cache far above its working set
   halves every window: a 16 384-entry cache is back at 500 in about four days
   at 2 gets/sec. While the target sits above what the workload can reach the
   gates never open.

5. **GC doubles it.** Nothing in the tree calls `SetGCPercent` or
   `SetMemoryLimit`, so Go defaults apply: **heap target = live x 2 at
   GOGC=100**. A 4 GB increase in cached bitmaps is ~8 GB of RSS.

---

## 2. The formula

Solve for the largest safe `M` rather than testing a guess:

```
Budget  = 0.80 x RAM_total  -  RSS_now  -  PageCacheReserve

M_safe  = I  +  Budget / ( 2 x N_plan x B_bar x K )
```

| Term | Meaning | Source |
|---|---|---|
| `I` | `series-id-set-cache-size` | config |
| `B_bar` | measured bytes per cache entry | Step 3.1 |
| `N_plan` | TSI shards that will grow, including future ones | Step 3.2 |
| `K` | skew safety factor (use `2`) | Step 3.3 |
| `2 x` | GC headroom at GOGC=100 | Step 3.4 |
| `PageCacheReserve` | memory left for mmap'd TSM/TSI page cache | Step 3.4 |

---

## 3. Procedure

### 3.0 Pre-flight

- [ ] Confirm the build contains both commits (`git log --oneline | grep -i adaptive`,
      or check for the `tsi1_cache` measurement in `_internal`).
- [ ] Confirm `indexType = tsi1` on the shards in question.
- [ ] Record `RAM_total` and current process `RSS_now`.
- [ ] Confirm `_internal` monitoring is on (default: enabled, 10s interval,
      `monitor/config.go:14,20`).

> **You do not need to enable the feature to do any of the measurement below.**
> `Engine.Statistics` appends `e.index.Statistics(tags)` unconditionally
> (`tsdb/engine/tsm1/engine.go:761`), so `tsi1_cache` stats are emitted even
> with adaptive sizing disabled.

### 3.1 Measure bytes per entry (`B_bar`)

`pprof-enabled` defaults to `true` (`services/httpd/config.go:88`) — no config
change, no restart.

```bash
curl -s 'http://localhost:8086/debug/pprof/heap' > heap.prof
go tool pprof -inuse_space -focus='TagValueSeriesIDCache' -top /usr/bin/influxd heap.prof
```

The focus regex captures everything the cache **retains**:

- `(*TagValueSeriesIDCache).innerLockingPut` -> `list.PushFront`,
  `mapassign_faststr`, and `SeriesIDSet.Clone` -> `roaringArray.clone` ->
  container clones (the bitmap payload)
- `(*TagValueSeriesIDCache).addToSet` -> `roaring.Add` container growth
  (write path)

and excludes the per-hit `ss.Clone()` in `Index.TagValueSeriesIDIterator`
(`index.go:1134`), which is transient, because that is not a cache method.

Call the reported in-use total `C_bytes`. Denominator:

```sql
SELECT sum("size") FROM "_internal".."tsi1_cache"
 WHERE time > now() - 1m GROUP BY time(10s)
```

```
B_bar = C_bytes / E_now
```

**Take the measurement at a workload peak.** Heap profiles sample at one per
512 KiB by default (`MemProfileRate`); pprof scales the estimate, so aggregate
totals are sound — do not read individual small call sites.

Rough interpretation: `< 1 KB` means low-cardinality predicates and lots of
room; tens of KB or more means your tag values match large series sets, and you
should be conservative.

### 3.2 Count the shards that will actually grow (`N_plan`)

Growth fires only from `checkEviction` — the cache must be **full and evicting**.
Shards that never fill sit at `I` forever and cost nothing.

```sql
-- Eviction pressure per shard over the last hour.
SELECT last("eviction") - first("eviction") AS evictions_1h
  FROM "_internal".."tsi1_cache"
 WHERE time > now() - 1h GROUP BY "id","database";

-- Current hit rate per shard: will raising M even help?
SELECT (last("hit")  - first("hit")) /
       (last("hit")  - first("hit") + last("miss") - first("miss")) AS hit_rate
  FROM "_internal".."tsi1_cache"
 WHERE time > now() - 1h GROUP BY "id","database";
```

`tsi1_cache` points inherit the shard's default tags — `id`, `database`,
`retentionPolicy`, `path`, `engine`, `indexType` (`tsdb/shard.go:189`).

- Shards with `evictions_1h > 0` are the ones that will climb.
- **Add the shards that do not exist yet.** Because shards are never unloaded
  and grown caches do not shrink without reads, plan for the steady-state shard
  count (`retention / shard duration`, per database/RP), not today's count.

**Go/no-go on value:** a shard already at `hit_rate >= 0.95` with
`evictions_1h = 0` will gain nothing from a larger `M`.

### 3.3 Pick the skew factor (`K`)

`B_bar` is measured over the *resident* entries — the LRU head. The entries you
add by raising `M` are the ones currently being evicted (the cold tail), and
nothing in the admission path is size-aware. They are not a random sample of
what you measured.

- Default: `K = 2`.
- Go higher if the query mix is regex-heavy.
  `matchTagValueEqualNotEmptySeriesIDIterator` (`tsdb/index.go:2755`) issues one
  `Get`/`Put` per *matching tag value*, so a single `/.+/` predicate sweeps every
  tag value — including the high-cardinality ones — into the cache in one pass.

### 3.4 Convert to RSS and set the reserve

- **GC headroom:** multiply live-heap delta by `1 + GOGC/100` = **2** at
  defaults. Go also returns freed pages to the OS lazily, so plan on the peak.
- **`PageCacheReserve` is not optional.** Free memory on the node is currently
  doing real work as page cache for mmap'd TSM and TSI files. Consuming it will
  not OOM the node — it will quietly turn a stable node into an I/O-bound one.
  Reserve what you measure, or 20-25% of RAM as a default.

### 3.5 The unconditional check (often fails — that is expected)

Independently of any measurement, per-entry bytes are hard-bounded by
cardinality:

```
B_max  ~=  352  +  96 x C  +  min( 2.5 x S_shard , 8192 x C )   bytes,   C = ceil(S_db / 65536)
```

(2.5 bytes per member rather than roaring's serialized 2 because array
containers are Go slices rounded up to a size class; 96 bytes per container is
the struct, interface value, key and copy-on-write flag the serialized figure
omits. With `8192 x C = S_db / 8`, the payload term is the same `S_db / 8`
ceiling as before; the container term is new and matters when `S_db` is large
and the entry's set is small.)

Inputs:

```sql
-- S_db
SHOW SERIES CARDINALITY ON <db>;          -- or SHOW SERIES EXACT CARDINALITY ON <db>

-- S_shard (per shard current series count)
SELECT last("seriesCreate") FROM "_internal".."shard"
 WHERE time > now() - 5m AND "indexType" = 'tsi1' GROUP BY "id";
```

> `seriesCreate` is misleadingly named: `shard.go:304,321` populates it from
> `engine.SeriesN()`, the shard's **current** series count, not a counter.

If `I + Budget / (2 x N_plan x B_max)` still exceeds your target `M`, you are
safe regardless of how wrong Step 3.1 was — approve and stop.

**Expect this to fail at any interesting `M`.** Worked example: 64 GB node,
28 GB RSS, 12 GB page cache reserve, 40 shards, `I = 100`:

```
Budget = 0.8 x 64 - 28 - 12 = 11.2 GB

Measured path   (B_bar = 9 KB, K = 2):        M_safe          ~= 7,600
Unconditional   (S_db = 20M -> B_max 2.5 MB): M_unconditional ~=   156
```

A 50x gap. This is not a reason to refuse the change — it is the reason
Section 4 exists.

---

## 4. Make the failure mode bounded

### 4.1 Set `GOMEMLIMIT` (highest-value action)

A Go runtime variable, honored automatically (this tree builds on Go 1.26). It
needs no InfluxDB code change and it bounds precisely where this feature's
growth lands: the Go heap.

```bash
# systemd drop-in, or container env
GOMEMLIMIT=54GiB        # ~80-85% of the cgroup/node limit
```

It converts "OOM kill" into "GC works harder" — a CPU symptom visible in
`_internal` before it becomes an outage.

State both caveats in the ticket:

- It is a **soft** limit and cannot constrain non-heap growth (mmap, page cache).
- Set below ~1.5x live heap it causes GC thrash. Measure `runtime.HeapAlloc`
  first.

### 4.2 Climb the ladder one rung at a time

Capacity doubles: `I -> 2I -> 4I -> ...`, clamped at `M`
(`decideResize`, `cache.go:610-616`). So:

1. Set `M = 2I`. Restart. Observe one full workload cycle.
2. Measure real `ΔRSS`, replacing `B_bar x K` with an observed number.
3. Set `M = 4I`. Repeat.

Each rung is a controlled experiment, and the ladder means you can never
overshoot the rung you set.

### 4.3 Know the rollback before you start

`[data]` settings are **not** SIGHUP-reloadable — `Server.ApplyReloadedConfig`
(`cmd/influxd/run/server.go:611`) handles only httpd, OpenTSDB TLS, and
subscriber TLS. Consequences:

- The change requires a **restart** to take effect.
- `series-id-set-cache-max-size = 0` + restart reverts it completely.
- The caches are pure in-memory with **no persisted high-water mark**, so a
  restart resets every shard to `I` instantly. Rollback is fast and total.

---

## 5. Fleet rollout (hundreds to thousands of instances)

Sections 3 and 4 assume you can profile the node. At fleet scale you cannot:
every input must come from InfluxQL (`_internal`, `SHOW STATS`,
`SHOW DIAGNOSTICS`) or `/debug/vars`. That rules out a formula and leaves
heuristics. This section gives six, each a query plus a threshold.

### 5.1 What the two surfaces actually give you

| Quantity | Source | Notes |
|---|---|---|
| Cache entries, per shard | `tsi1_cache.size`, `.capacity` | emitted even with adaptive **disabled** (`tsdb/engine/tsm1/engine.go:761`) |
| Cache pressure | `tsi1_cache.hit` / `.miss` / `.eviction` / `.shrink_eviction` | lifetime counters; reset to 0 on process restart |
| Shard series count | `shard.seriesCreate` | misleading name: `engine.SeriesN()`, the **current** count (`tsdb/shard.go:304,321`) |
| Database series / measurements | `database.numSeries`, `.numMeasurements` | `tsdb/store.go:48-49` |
| Heap | `runtime.HeapAlloc`, `Sys`, `HeapInUse`, `HeapReleased`, `NumGC`, `PauseTotalNs` | `monitor/service.go:388` |
| Write-cache bytes | `tsm1_cache.memBytes` | per shard; the main heap confounder you can subtract |
| Query wall time | `queryExecutor.queryDurationNs`, `.queriesFinished` | `query/executor.go:46-51` |
| Index type | `indexType` tag on shard-scoped stats | filter to `tsi1` |

**Not available, and you will reach for both:** per-tag-value cardinality, and
the count of distinct `{measurement, tagKey, tagValue}` tuples per shard
(`V_s`). See 5.7 for how to measure `V_s` empirically instead.

### 5.2 The conservation law that makes fleet rules possible

For measurement `m` and tag key `k`, the values of `k` **partition** the series
carrying `k`, so `sum over v of card(m,k,v) <= S_m`. Therefore, per shard:

```
sum over ALL cacheable tuples of card  <=  sum over m of ( K_m x S_m )
```

and serialized payload is **<= 2 bytes per series ID, always** (array container
`2n`; bitmap container 8192 flat at `n >= 4097`, so under 2/id; run containers
never occur). In heap the member payload is 2.5 bytes (slice size-class slack)
and every roaring container adds ~96 bytes of structure however few members it
holds, so a set spread one member per container costs ~60 bytes per series
against 2 in the serialized figure; the container count per entry is capped at
`C = ceil(S_db / 65536)`.

The operational consequence is that **the two things that would make an
instance expensive are mutually exclusive** for the payload:

- **Many cache entries** => a high-cardinality tag key => each entry's series
  set is small => ~400 B/entry, or ~350 + 96·(containers) when the few members
  are scattered across the id space.
- **Large cache entries** => a low-cardinality tag key => few distinct values
  => the payload saturates below `M`.

You cannot have both. This is the load-bearing argument for shipping a single
`M` across instances with unknown data shapes — for the payload. It does **not**
bound the entry count: a query for a tag value that does not exist still
creates an entry (the file set returns an empty set for an absent value and the
index caches it), so the structural term runs all the way to `M` on any shard
whatever its schema. An earlier version of this section said raising `M` "does
nothing" on a saturated shard; that was wrong.

### 5.3 Know which shard population you are counting

Three different numbers, easy to conflate:

| | Definition | When | Used for |
|---|---|---|---|
| `N_saturated_now` | `size == capacity` **and** evictions rising | before | how much of the budget lands on day one (5.4) |
| `N_tsi_total` | every TSI shard, at **steady-state** count | before | the budget denominator (5.5) |
| `N_at_ceiling` | `capacity == M` | after | rollout observation (5.9) |

Use **`N_tsi_total`** for the budget. A shard that is quiet today can be put
under eviction pressure tomorrow by a new dashboard or regex predicate —
nothing about the shard changes, only the query mix. And because shards are
never unloaded, use the steady-state count (`retention / shard duration`,
summed per database/RP), not today's.

Counting each — count the **returned series**, since `GROUP BY "id"` yields one
per shard:

```sql
-- N_tsi_total (today's; scale by retention / shard duration for steady state)
SELECT last("size") FROM "_internal".."tsi1_cache"
 WHERE time > now() - 5m AND "indexType" = 'tsi1' GROUP BY "id";

-- N_at_ceiling, after rollout: count rows where last("capacity") == M
SELECT last("capacity") FROM "_internal".."tsi1_cache"
 WHERE time > now() - 1h AND "indexType" = 'tsi1' GROUP BY "id";
```

### 5.4 H1 — Triage: most instances carry zero risk

Growth fires only from `checkEviction`, which requires the cache to be **full
and evicting**.

```sql
SELECT last("eviction") - first("eviction") AS evict_7d,
       last("size")     - last("capacity")  AS slack
  FROM "_internal".."tsi1_cache"
 WHERE time > now() - 7d AND "indexType" = 'tsi1'
 GROUP BY "id";
```

**Rule:** a shard can grow only if `evict_7d > 0` **and** `slack == 0`.
Everything else is a guaranteed no-op — raising `M` cannot allocate a byte
there. `N_saturated_now` = count of shards meeting both.

Convenient property: `size == capacity` is the same predicate before and after
the change. Before it means "full at `I`, would grow"; after it means "full at
its current rung, will keep growing." One alert covers both phases.

**Value check — will it help at all?**

```sql
SELECT (last("hit")  - first("hit")) /
       (last("hit")  - first("hit") + last("miss") - first("miss")) AS hit_rate
  FROM "_internal".."tsi1_cache"
 WHERE time > now() - 7d AND "indexType" = 'tsi1' GROUP BY "id";
```

`hit_rate >= target` => won't grow => ship `M` there for free, and expect no
benefit either.

### 5.5 H2 — Budget-first sizing: `M` is per-shard, so multiply by shard count

`M` is a **per-shard** ceiling (`tsdb.NewIndex` from `shard.go:359` — one cache
per `tsi1.Index`, not per partition) and nothing in the code couples it to
shard count. A value that is comfortable at 40 shards is catastrophic at
10,000.

```
ceiling(RSS)  ~=  N_tsi_total x (M - I) x 2 KB                              <- structural, exact
                + N_tsi_total x min( M x S_db / 500 , K_bar x S_shard x 108 ) <- payload, bounded
```

Constants:

- **2 KB** = ~1 KB fixed per-entry overhead (Appendix A) x 2 for GOGC=100.
- **`M x S_db / 500`** — scatter ceiling. Containers per entry cap at
  `ceil(S_db / 65536)` (Appendix B); worst case ~68 B per sparse container;
  `2 x 68 / 65536 ~= 1/482`.
- **`K_bar x S_shard x 108`** — conservation ceiling (5.2): `2 B/id` payload
  plus ~52 B/container, doubled for GC = 108 B per (series x tag key).
  **Independent of `M`** — raising `M` moves you toward it, never past it.
  (`tsi_cache_sizer` uses 2.5 and 96 undoubled, i.e. 197 doubled, a more
  conservative calibration of the same two terms; either is fine here.)

`S_db` from `database.numSeries`; `S_shard` from `shard.seriesCreate`;
`K_bar` (mean tag keys per measurement) from one cheap `SHOW TAG KEYS`, or a
fleet default of 8-10.

**Invert it.** Do not pick `M` and check it; pick a budget and derive `M`:

```
M  =  I  +  Budget / ( N_tsi_total x 2 KB )
```

| Shards | `M` for a 4 GB budget | `M` for a 10 GB budget |
|---|---|---|
| 40 | ~48,900 | ~122,000 |
| 500 | ~4,000 | ~9,900 |
| 2,000 | ~1,100 | ~2,500 |
| 10,000 | ~295 | ~590 |

Worked check of the top-left corner of the risk: 10,000 shards at `M = 10,000`
gives `(10000-100) x 2 KB x 10000 = 189 GiB` of structural term alone. That is
the number that makes the case for budget-first sizing.

**The structural term is the part you can guarantee** — it is exact and needs
no assumptions about data shape. Size `M` so that term alone fits, and treat
the payload term as what `GOMEMLIMIT` (5.8) catches.

**Cheap pre-check.** Shards whose cache never fills today touch fewer than `I`
distinct tuples and will not grow:

```sql
SELECT max("size") FROM "_internal".."tsi1_cache"
 WHERE time > now() - 7d AND "indexType" = 'tsi1' GROUP BY "id";
```

Count rows with `max("size") < I`. On instances with many historical shards
this is usually a large fraction, putting realistic exposure far below the
ceiling. It is a "today" signal, not workload-proof — a new query pattern can
raise the working set on any shard.

### 5.6 H3 — Self-calibrate bytes-per-entry from restarts (no pprof)

`tsi1_cache.hit` is allocated fresh at startup, so **a counter reset marks a
restart** and is detectable in `_internal`. Between restart and cache
saturation, `sum(tsi1_cache.size)` climbs 0 -> E while heap climbs. That is a
free natural experiment on every node.

```sql
SELECT last("HeapAlloc") FROM "_internal".."runtime"
 WHERE time > <restart> AND time < <restart> + 30m GROUP BY time(10s);
SELECT sum("memBytes")   FROM "_internal".."tsm1_cache"
 WHERE time > <restart> AND time < <restart> + 30m GROUP BY time(10s);
SELECT sum("size")       FROM "_internal".."tsi1_cache"
 WHERE time > <restart> AND time < <restart> + 30m GROUP BY time(10s);
```

**Rule:** `B_bar ~= delta(HeapAlloc - sum(tsm1_cache.memBytes)) / delta(sum(tsi1_cache.size))`

Subtracting `tsm1_cache.memBytes` removes the dominant confounder (write-cache
warm-up). Mmap'd file pages are not in `HeapAlloc` at all, so they do not
contaminate it. Per instance this is noisy; **across a fleet it is not.**
Collect from every restart, take the p95, and you have an empirical `B_bar`
with no profiling and no intervention — narrowing H2's ~7x uncertainty band to
roughly 1.5x.

### 5.7 H4 — Measure the working set; the gain is a step function

Until capacity exceeds the query workload's working set of distinct
`{measurement, tagKey, tagValue}` tuples, the cache keeps thrashing — LRU under
a regex sweep is close to worst case, since each entry is evicted just before
the next refresh needs it. Once capacity crosses the working set, hit rate
jumps toward 1.0 and stays there.

So `M` should be set from **evidence about the working set**, not from whatever
the budget allows. On a canary, set `M` very high and read where it settles:

```sql
SELECT last("size"), last("capacity"),
       last("eviction") - first("eviction") AS evict
  FROM "_internal".."tsi1_cache"
 WHERE time > now() - 2h AND "indexType" = 'tsi1' GROUP BY "id";
```

When `evict` falls to ~0 and `size` plateaus **below** `capacity`, that plateau
**is** the working set for that shard. Take the p95 across a representative
cohort and set fleet `M` just above it. This usually lands far below the
budget ceiling from 5.5, which is the outcome you want: the step-function gain
at minimum memory cost.

**Regime detection, one number per instance:**

```
entries_per_1k_series = 1000 x sum(tsi1_cache.size) / sum(shard.seriesCreate)
```

- **High** => high-cardinality tag key => entries are small => cost is
  essentially the structural term. Predictable, low risk.
- **Low** => few distinct tuples => the payload saturates below `M`. Lower
  risk, but not inert: the structural term still runs to `M` if the query mix
  names values that do not exist (decommissioned hosts in a dashboard variable,
  another database's values, typos), because each of those is an entry too.
- The middle is where H3's measured `B_bar` matters.

### 5.8 H5 — Scale the guardrail from the same data

```
GOMEMLIMIT = 0.80 x (cgroup / node memory limit)
      subject to  >= 1.5 x p99(runtime.HeapAlloc) over the last 30 days
```

Both terms are computable per instance from `_internal`, so this can be
generated for the whole fleet without manual work. The floor is what keeps you
out of GC thrash.

This is the piece that makes a fleet rollout defensible: it converts "one
instance somewhere has a data shape we mispredicted" from an OOM kill into
elevated GC CPU, visible in `runtime.NumGC` / `PauseTotalNs`, and reversible
with a config change and restart.

### 5.9 H6 — Rollout shape and alerting

Capacity climbs `I -> 2I -> 4I -> ...` clamped at `M`
(`decideResize`, `cache.go:610-616`), so each rung is self-limiting. Ship in
doublings across cohorts.

Alert on the tail: `capacity == M` **and** the windowed hit rate below target.
Those shards' targets exceed what their workload can reach; the rate they have
settled at is that ceiling, and the target belongs just under it. Judge the
rate over an `_internal` window, not from the lifetime counters, which carry
every miss of the climb. The condition clears on its own when the shard's
novel-predicate rate drops below `1 − target`, so one that comes and goes with
the workload while `Σ bytes` stays in budget can be left alone; a sustained one
means lower the target on that instance.

**Prove the benefit** before and after, with the same query mix:

```sql
SELECT (last("queryDurationNs") - first("queryDurationNs")) /
       (last("queriesFinished")  - first("queriesFinished")) AS mean_ns
  FROM "_internal".."queryExecutor" WHERE time > now() - 1h;
```

If `hit_rate` climbs materially but `mean_ns` does not move, index resolution
was not the bottleneck and the memory is buying nothing. Roll back.

### 5.10 Heuristic summary

| # | Rule | Inputs | Purpose |
|---|---|---|---|
| H1 | Risk only if `evict_7d > 0` **and** `size == capacity` | `tsi1_cache` | eliminates most of the fleet |
| H2 | `M = I + Budget / (N_tsi_total x 2 KB)` | `tsi1_cache`, `database.numSeries`, `shard.seriesCreate` | budget-first sizing |
| H3 | `B_bar` from post-restart heap-vs-entries slope | `runtime`, `tsm1_cache`, `tsi1_cache` | narrows H2 from ~7x to ~1.5x |
| H4 | Working-set plateau on a canary; `entries_per_1k_series` | `tsi1_cache`, `shard` | sets `M` from evidence; cohorts |
| H5 | `GOMEMLIMIT = 0.8 x limit`, floor `1.5 x p99 HeapAlloc` | `runtime` | bounds the failure mode |
| H6 | Ship in doublings; alert on `capacity == M` + rising `miss` | `tsi1_cache`, `queryExecutor` | finds the tail; proves value |

**One-sentence version for the fleet owner:** conservation of cardinality means
an instance cannot have both many cache entries and large ones, so the
guaranteed cost of raising `M` is `(M - I) x ~2 KB x N_tsi_total` — pick `M`
from that budget, not from a per-shard intuition — and the uncertain remainder
is bounded by data shape rather than by `M`, and caught by `GOMEMLIMIT`.

### 5.11 What the memory buys

The cache is consulted from one place, `Index.TagValueSeriesIDIterator`
(`index.go:1130`). It accelerates:

- **Tag predicates in `WHERE`** (`=`, `!=`, `=~`, `!~`) — the main consumer
  (`tsdb/index.go:2603,2630,2658`)
- **`SHOW SERIES` / `SHOW MEASUREMENTS` with an authorizer present**
  (`tsdb/index.go:1719,1908,2001`); open authorizers short-circuit first

It does **not** help queries without a tag predicate, and it does **not**
reduce TSM data reads — series resolution is upstream of reading points. It
makes writes marginally slower (more resident measurements => more locked
`addToSet` calls, `index.go:757-785`).

A miss costs 8 partitions x every index file in the fileset, each doing a
tag-block lookup, a roaring unmarshal off mmap, a `Merge`, and an `AndNot` for
tombstones. A hit is three map lookups and one `Clone()` — one to two orders of
magnitude less work on that step.

The win concentrates where the amplification factor is high: regex predicates
route through `matchTagValueEqualNotEmptySeriesIDIterator`
(`tsdb/index.go:2755`), which issues **one Get/Put per matching tag value**. A
`host =~ /.+/` over 500 hosts spanning 20 shards is 10,000 index resolutions
for one query. So expect visible gains on **metadata-heavy, high-fan-out,
low-data** queries (dashboard panels with regex/multi-value tag filters,
template-variable population) and little on long-range aggregations where TSM
decode dominates.

**Cost and benefit are aligned — until they are not.** A shard grows only under
eviction pressure, which requires active querying, so memory is spent where
benefit is delivered. The leak is stickiness: `checkShrink` runs only from
`Get`, and there is no cold-shard unload path, so a shard that was hot last
week keeps its grown cache indefinitely. On instances with many
time-partitioned shards and a sliding query window, the residue accumulates.
Mitigations: keep `M` modest, and remember that **a restart resets every cache
to `I`** (Section 4.3) — ordinary maintenance restarts reclaim the residue for
free.

---

## 6. Monitoring after the change

| Signal | Source | Interpretation |
|---|---|---|
| `tsi1_cache.capacity` per shard | `_internal` | Pinned at `M` with the windowed hit rate below target => target above the workload's ceiling; you are exposed to the upper bound until the novel-predicate rate drops |
| `tsi1_cache.bytes` per shard | `_internal` | The cache's own footprint estimate, at **serialized** set sizes: 1.02× below heap for dense sets, 3–5× for the empty and one-id sets that fill most caches, up to 11× for dispersed ones. Trend on it; divide the budget by ~2 before alerting on it |
| `tsi cache capacity increased` | influxd log | Carries `old_capacity`, `new_capacity`, `hit_rate`, `gets_window`; exact timestamps for bracketing a heap diff |
| `tsi1_cache.shrink_eviction` | `_internal` | Flat while capacity is high => memory is being retained, not released |
| `tsi1_cache.hit` / `miss` | `_internal` | Whether you bought anything |
| `runtime.HeapAlloc`, `runtime.Sys` | `_internal` | The actual cost |
| Process RSS | node exporter / `ps` | The number that OOMs you |

Bracketing query for a growth episode (use the log timestamps):

```sql
SELECT last("HeapAlloc") FROM "_internal".."runtime"    WHERE time > ... GROUP BY time(10s);
SELECT sum("size")       FROM "_internal".."tsi1_cache" WHERE time > ... GROUP BY time(10s);
```

---

## 7. Traps

Four things that look like answers but are not.

### 7.1 `SHOW STATS FOR 'indexes'` does not include this cache

It returns `memoryBytes` from `Store.IndexBytes()` -> `tsi1.Index.Bytes()`
(`tsdb/index/tsi1/index.go:240-266`), which walks partitions, sketches, path and
config fields and **never touches `i.tagValueCache`**. It will not move as the
cache grows by gigabytes.

Useful only as a **control**: if `memoryBytes` is flat while heap climbs during a
`capacity increased` episode, you have confirmed the cache is the mover.

*(Arguably a defect worth filing independently.)*

### 7.2 `dumptsi`'s tag-value "Series data size" is always 0

The summary reads `ve.SeriesData()`, which returns the legacy uvarint field
`e.series.data` (`tag_block.go:341`). But `encodeTagValueFlag`
(`tag_block.go:820`) unconditionally sets `TagValueSeriesIDSetFlag`, so every
index file written by current code takes the roaring branch and leaves
`series.data` nil.

`SeriesN()` **is** valid (the count is parsed before the flag branch), so use
`dumptsi`'s counts, ignore its sizes.

### 7.3 `SHOW SERIES CARDINALITY ... FROM ... GROUP BY <tag>` is silently ignored

The parser accepts sources, condition and dimensions
(`influxql@v1.4.1/parser.go:1006`), but
`executeShowSeriesCardinalityStatement` (`coordinator/statement_executor.go:963`)
passes only the database name to `TSDBStore.SeriesCardinality` and discards all
three. You always get the database-wide estimate.

For per-tag-value cardinality online, count rows from
`SHOW SERIES FROM <m> WHERE <k>='<v>'`.

### 7.4 `SHOW DIAGNOSTICS` cannot confirm the rollout

On builds before the fix in this tree, `tsdb.Config.Diagnostics()`
(`tsdb/config.go:397-416`) reports `series-id-set-cache-size` but **not**
`series-id-set-cache-max-size`, `series-id-set-cache-target-hit-rate`, or
`series-id-set-cache-shrink-conservatism`. This tree adds all three, so
`SHOW DIAGNOSTICS FOR 'config-data'` and `/debug/vars` under `config` now
confirm the rollout directly.

On older builds the readback is indirect. A fixed-size cache gives every shard
the same capacity, so **differing per-shard capacities prove adaptive sizing is
on** — the production clone this was checked against had a capacity total that
was not a multiple of its shard count. `tsi_cache_sizer` makes that inference
and, given the configuration with `-current-max-size`/`-current-target`, audits
it against the budget and runs the pinned-shard check. An instance that
received the config but has no shard that has grown yet is still
indistinguishable from one that did not receive it.

---

## 8. Decision summary for the ticket

**Single instance (Sections 3-4):**

1. Run Step 3.1 **before touching config** — one curl and one query, no restart.
   This turns the question from speculation into arithmetic.
2. Compute `M_safe`. Run the Step 3.5 unconditional check.
3. If unconditional passes: approve `M`, done.
4. If it fails (usual): set `GOMEMLIMIT`, raise `M` by **one doubling**, restart,
   measure, repeat.
5. If the customer wants to jump straight to a large `M`: the honest answer is
   that **no config-only calculation can prove safety at realistic cardinality**.
   `GOMEMLIMIT` plus the ladder is the guarantee, not the math.

**Fleet (Section 5):**

1. Run H1 triage. Most instances cannot grow at all — approve those immediately.
2. Count `N_tsi_total` at steady state. Derive `M` from a memory budget
   (H2), **not** from a per-shard intuition. `M` multiplies by shard count.
3. Measure the working set on a canary cohort (H4) and lower `M` to just above
   the p95. This is usually far below the budget ceiling, and the gain is a
   step function — capacity below the working set buys little.
4. Deploy `GOMEMLIMIT` fleet-wide (H5) before the config change.
5. Ship in doublings by cohort; alert on `capacity == M` with rising `miss`
   (H6); confirm `queryExecutor.queryDurationNs` actually moved. If hit rate
   improved and query time did not, index resolution was not the bottleneck —
   roll back.

---

## Appendix A — Measured per-entry cost

Measured in-package against a real `TagValueSeriesIDCache`: `HeapAlloc` deltas
over 20,000 entries after forced GC, single shared measurement and tag key,
12-byte tag values unless noted.

| Set cardinality | Container shape | Bytes / entry |
|---|---|---|
| 1 | 1 array ctr | 383 |
| 16 | 1 array ctr | 404 |
| 128 | 1 array ctr | 628 |
| 1,024 | 1 array ctr | 2,420 |
| 4,096 | 1 array ctr (max) | 8,593 |
| 4,097 | 1 bitmap ctr | 8,608 |
| ~32,768 | ~1.5 bitmap ctr | 12,208 |
| ~131,072 | ~4.5 bitmap ctr | 36,940 |
| ~524,288 | ~16.6 bitmap ctr | 135,872 |
| 128, 64-byte tag value | 1 array ctr | 680 |
| 128, 240-byte tag value | 1 array ctr | 862 |
| 128, distinct measurement per entry | 1 array ctr | 1,156 |

Struct sizes: `list.Element` 40 B, `seriesIDCacheElement` 56 B,
`tsdb.SeriesIDSet` 32 B, `TagValueSeriesIDCache` 216 B.

**Fixed overhead ~370 B/entry**, independent of set size: `list.Element` (48 B
size class) + `seriesIDCacheElement` (64) + per-entry private copies of the
measurement and tag-key strings, since `innerLockingPut` does `string(name)` /
`string(key)` per entry (32) + `SeriesIDSet` (32) + `roaring.Bitmap`/
`roaringArray` struct (128) + the three `roaringArray` slices (48) + amortized
leaf-map slot (~31).

Add `+L_v` for the tag value string (size-class rounded), and `+~530 B` if the
entry is the only one for its measurement (extra map headers).

## Appendix B — Payload rules (verified against `influxdata/roaring@fc520f41`)

```go
func (ac *arrayContainer)  getSizeInBytes() int { return ac.getCardinality() * 2 }  // 2 B / id
func (bc *bitmapContainer) getSizeInBytes() int { return len(bc.bitmap) * 8 }       // 8192 B flat
// arrayDefaultMaxSize = 4096
```

InfluxDB never calls `RunOptimize`, so only array and bitmap containers occur
(plus the rare all-ones run container produced by `repairAfterLazy`). Therefore:

```
P_serialized(entry) <= min( 2 x card , 8192 x C )        C = distinct 64Ki blocks touched
P_heap(entry)       <= 96 x C  +  min( 2.5 x card , 8192 x C )
```

The heap line is what the budget pays: each container is a Go slice rounded up
to a size class (2.5 bytes per member covers it) inside a ~96-byte structure —
container struct, interface value, key and copy-on-write flag — that the
serialized size does not count. Measured on cloned sets: a 1-id set is 210 B in
heap against 44 serialized; 100 ids one per container are 4 879 B against 440.

Series IDs are **database-global**: `SeriesPartition` hands out `seq` starting at
`partitionID+1` with stride `SeriesFilePartitionN = 8` across 8 partitions
(`tsdb/series_partition.go:465`), and `SeriesIDSet` truncates to `uint32`. So
max series ID ~= `S_db` (ids ever created, never reused) and
`C <= ceil(S_db / 65536)`. Since `8192/65536 = 1/8`:

```
P_max(entry) = 96 x C  +  min( 2.5 x S_shard , S_db / 8 )   bytes
B_max(entry) ~= 352 + P_max(entry)
```

**Do not sum `B_max` across entries.** `P_max` is a *single-entry* worst case
and cannot be realized by many entries at once: cardinality is conserved
(Section 5.2), so a tag key's values share a budget of `S_m` series IDs however
they are split. Aggregate payload across all entries in a shard is bounded by
`2 x sum over m of (K_m x S_m)`, which is a far smaller number. Use the
conservation bound for totals and `B_max` only for reasoning about one entry.

## Appendix C — The `bytes` gauge, and the gap it leaves

The `bytes` field on `TagValueSeriesIDCacheStatistics` now exists in this tree,
maintained incrementally as this appendix once proposed: added in
`innerLockingPut` after the `Clone()`, subtracted in `evictLRULocked`, and
re-accounted in `addToSet`/`delete`.

It does not close the gap, because `tsdb.SeriesIDSet.Bytes()`
(`tsdb/series_set.go:31`) counts 24 bytes of mutex, 8 of pointer and roaring's
`GetSizeInBytes()`, which — contrary to what this appendix once claimed — is the
**serialized** size: 2 bytes per array member, 8 192 per bitmap container, and
next to nothing per container or for the bitmap struct itself. Against live
heap (Appendix A's method, on cloned sets) it reads 40 B for an empty set that
costs 134, 44 B for a one-id set that costs 210, and 440 B for 100 ids spread
one per container that cost 4 879. It is within 2 % only for dense sets.

So the gauge is a trend signal and a floor, not the footprint. To size against
it, divide the budget by a factor for the cache's set shapes (about 2 for a mix
like the reference instance; more for small dispersed sets). A heap-accurate
gauge would add ~134 bytes per set and ~96 per container, which
`roaring.Bitmap.Stats()` can count but at O(containers) per update — the same
cost class as `GetSizeInBytes` today, so feasible, but a change to the storage
engine rather than to this tool. Until then the sizing bound in
`cmd/tsi_cache_sizer` is calibrated against heap, not the gauge, and the
`-gauge-heap-factor` flag converts gauge-derived measurements.
