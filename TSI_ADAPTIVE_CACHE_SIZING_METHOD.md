# Determining Safe Adaptive TSI Cache Configurations

A method for choosing `series-id-set-cache-max-size` and `series-id-set-cache-target-hit-rate`
across a fleet of heterogeneous InfluxDB 1.x instances, using only data obtainable from InfluxQL
queries and the `/debug` endpoints.

**Goal:** get the query-performance benefit of adaptive TSI caching on instances that will actually
benefit, without pushing any currently-stable instance into an OOM.

**Operating definition of stable:** typical RAM usage at or below 50%, with the remaining headroom
reserved to absorb compaction, backfill, downsampling, and query bursts. Any memory the TSI cache
takes comes out of that burst reserve, so the reserve is the budget we are spending.

All source references are to this tree (`master-1.x`).

> **Deciding about one specific instance?** Use
> [`RUNBOOK_tsi_cache_sizing.md`](RUNBOOK_tsi_cache_sizing.md) instead — the
> decision procedure without the derivations. Most instances resolve there from
> two numbers and no tooling. This document is why that procedure is correct.

---

## Part 1 — Mechanism facts that constrain the answer

These come from reading `tsdb/index/tsi1/cache.go`, `tsdb/index/tsi1/index.go`, and
`tsdb/index.go`. They are not background; each one changes the arithmetic.

| Fact | Source | Consequence for sizing |
|---|---|---|
| There is one cache per **shard** (`Index.tagValueCache`), not one per instance | `tsi1/index.go:170,226` | Worst-case heap is `max-size × n_shards × bytes_per_entry` |
| An entry is one `(measurement, tag key, tag value)` mapped to a cloned roaring `SeriesIDSet` covering **that shard's** matching series | `tsi1/index.go:1129-1160` | Entries are **not** uniform size; bytes/entry is proportional to the matching series count |
| Entries are created only by `tag = 'v'` and `tag != 'v'` predicates; regex predicates take `matchTagValueSeriesIDIterator` and bypass the cache | `tsdb/index.go:2600,2644` | Cache fill is driven by equality-predicate *variety*, not by query volume |
| Capacity **doubles** toward max when the windowed hit rate falls below target while evicting, checked once per `capacity` forced evictions | `tsi1/cache.go:456-477,565-618` | Growth is fast — minutes under load |
| Shrink is driven from the `Get` path only, gated on hit rate ≥ target **and** window evictions below `μ − z·σ`, and trims at most `min(size/2, 1024)` entries per window. The post-shrink cooldown runs concurrently with the next window, so consecutive shrinks are one window apart, not two | `tsi1/cache.go:655-723,920-972`; `TestAdaptiveTarget_ShrinkCadenceIsOneWindow` | Shrink is blocked outright while the cache is below target; once the gates open it halves a cold cache per window |
| Because shrink is `Get`-driven, **a shard that stops being queried never shrinks** | `tsi1/cache.go:655` | Memory grown during a sweep is retained indefinitely |
| `Engine.Free()` on idle shards frees the TSM write cache and file store — not the TSI cache | `tsm1/engine.go:999` | Idling a shard does not reclaim its TSI cache |
| Capacity never falls below `series-id-set-cache-size` | `tsi1/cache.go:938-942,956-963` | The floor is a permanent per-shard allocation |
| Exposed stats are `hit`, `miss`, `eviction`, `shrink_eviction`, `size`, `capacity`, `bytes`. `bytes` counts each set at its *serialized* size (`SeriesIDSet.Bytes()` wraps roaring's `GetSizeInBytes`), which is 1.02× below heap for dense sets and up to 11× below for sets spread one member per container; there is still **no bytes-based cap** | `tsi1/cache.go`, `tsdb/series_set.go:31` | `bytes` is a floor on the cache's heap, not the heap; the cap you configure is still an entry count, so you convert a byte budget into one yourself with a heap-calibrated model |
| `SIGHUP` reload covers only httpd, OpenTSDB TLS, and subscriber config | `run/server.go:611`, `run/command.go:248` | Every change, including rollback, requires a restart |
| The TSI cache exists only for `index-version = tsi1`; the default is `inmem` | `tsdb/config.go:17-18` | Confirm index type per instance before anything else |
| `min-samples` (100) is not TOML-configurable | `tsdb/config.go:85`, `tsi1/index.go:232` | Three knobs only: `max-size`, `target-hit-rate`, `shrink-conservatism` |

Two of these dominate everything else:

1. **The cap is an entry count, but the risk is bytes.** The `bytes` stat now *measures* the risk,
   but nothing in the implementation *bounds* it.
2. **Memory is acquired in minutes and released in days, or never.** Once the shrink gates open,
   a 16 384-entry cache decays to a 500-entry working set in about 700 k gets — four days at
   2 gets/sec (`TestAdaptiveTarget_PinnedCacheRecoversWhenNovelRateDrops`). While the target sits
   above what the workload can reach the gates stay shut, and a shard that is not queried never
   shrinks at all. Do not plan around shrink reclaiming memory for you.

---

## Part 2 — The entry cost model

> **If your build publishes the `bytes` stat, read it instead** — Part 3B and 3C show how. This cost
> model exists to estimate what the gauge now reports directly. It remains the fallback for older
> builds, and remains how to reason about a shard whose cache has not yet been populated, since a
> gauge can only describe the entries the cache actually holds.

```
bytes_per_entry  ≤  352  +  96·n  +  min(2.5·k, 8192·n)        n = min(k, ceil(idSpan / 65536))

   k  = series in THIS shard matching that (measurement, tag key, tag value)
   n  = roaring containers the set occupies
```

Roaring stores members in 2-byte arrays while a container is sparse and in 8 KiB bitmaps per
65 536-ID block once dense. In heap each container also carries ~96 bytes of structure (its
struct, the interface value holding it, its key and copy-on-write flag), an array's backing slice
is rounded up to a size class (2.5 bytes per member covers it), and an entry's fixed cost is 352
bytes: 152 for the `list.Element` and the three levels of map entry, 136 for an empty
`SeriesIDSet` (wrapper plus the roaring bitmap struct), and a 64-byte allowance for the interned
`name`/`key`/`value` strings.

**Every "measured" figure in this part is the cache's `bytes` gauge, which counts serialized
size.** Heap is higher, by a ratio that depends on the set's shape:

| set shape | gauge | heap | ratio |
|---|---|---|---|
| empty set | 40 B | 134 B | 3.4× |
| 1 id | 44 B | 210 B | 4.8× |
| 500 ids across 16 containers (`rack_rr`) | 1 072 B | 2 007 B | 1.9× |
| 100 ids, one per container | 440 B | 4 879 B | **11.1×** |
| 4 096 ids in one container | 8 234 B | 8 397 B | 1.02× |

The constants above were calibrated against live heap (sets built by merge-then-`Clone` as `Put`
stores them, measured with `runtime.MemStats`); the gauge figures below are kept because they are
what was recorded, and they remain floors.

Since `k ≈ S_shard(m) / V(m,k)` — series in the measurement divided by the distinct values of that
tag key — the hazard is **low-cardinality tag keys on high-cardinality measurements**:

Measured on a 1M-series shard, one entry each, same cache:

| Predicate shape | k | bytes/entry | at `max-size` 1600 × 60 shards |
|---|---|---|---|
| `host = ...`, 1M hosts | 1 | 207 B | 19 MB |
| `rack = ...`, 2 000 racks | 500 | 1 241 B | 114 MB |
| `zone = ...`, 50 zones | 20 000 | 40 241 B | 3.6 GB |
| `region = ...`, 4 regions | 250 000 | 131 317 B | **11.7 GB** |

**634× between the cheapest and dearest entry**, in one cache, on one shard, over the same series.
This is the single reason a fleet-wide entry cap has to be derived from each configuration's *worst*
member rather than its average — and the reason the cap should eventually be denominated in bytes.

The same experiment shows the consequence directly: 50 `host` entries cost 10 KB, while 50 `zone`
entries cost 1.97 MB. **Same cap, 190× the memory.**

**How loose is the upper bound?** Measured on a real instance — 1M series, one measurement, one
shard, reading the `bytes` gauge. `scripts/tsi_cache_experiment` reproduces this in about a minute;
see its README for the design.

| tag key | values | k | ID layout | actual | `2k + 300` | model error |
|---|---|---|---|---|---|---|
| `host` | 1 000 000 | 1 | — | 207 B | 302 B | 1.5× |
| `rack_rr` | 2 000 | 500 | strided | 1 241 B | 1 300 B | **1.0×** |
| `zone_rr` | 50 | 20 000 | strided | 40 241 B | 40 300 B | **1.0×** |
| `grid_rr` | 16 | 62 500 | strided | 125 143 B | 125 300 B | **1.0×** |
| `zone_blk` | 50 | 20 000 | clustered | 8 405 B | 40 300 B | 4.8× |
| `grid_blk` | 16 | 62 500 | clustered | 8 515 B | 125 300 B | 14.7× |
| `region_blk` | 4 | 250 000 | clustered | 16 711 B | 500 300 B | **29.9×** |

Two things to take from this:

**The bound is exact when IDs are sparse in the ID space** — the strided rows measure 2.00–2.01 bytes
per series. If you must estimate, you are not being paranoid; you are being precise about the worst
case.

**It is up to 30× loose when they are clustered.** Roaring stores values by their high 16 bits: a
container spans 65 536 IDs and switches from a 2-byte-per-value array to a flat 8 KiB bitmap above
4 096 values, so a value whose series are contiguous collapses into a handful of dense containers.
Series IDs are assigned in creation order, so whether a tag value's series are clustered is decided
by **write order** — a region-by-region backfill and a live round-robin ingest of the very same data
differ by 15× in cache cost (`grid_blk` 8 515 B vs `grid_rr` 125 143 B, identical cardinality and
identical `k`).

### Closing the gap

Two of these error sources are reducible, and the tooling now reduces both.

**The 3.8× on `region_rr` was modelling slop, and is gone.** A roaring container
costs at most 8 KiB however many members it holds, so a set concentrated in few
containers can never reach `2.5k`. Adding that term, and the per-container
structure the gauge omits —

```
cost <= 352 + 96n + min(2.5k, 8192n),  n = min(k, ceil(idSpan/65536))
```

— puts the bound within **1.01× to 1.5×** of every strided row measured in heap
(`rack_rr` 2 170 B, `zone_rr` 42 020 B, `grid_rr` 125 950 B, `region_rr`
132 177 B, each including the 152-byte element structure). It needs an upper
bound on the series ID span, which is current cardinality inflated by churn (IDs
are never reused in 1.x). Without the span every member is costed as its own
container — safe, and for large sets far above the truth — so the span is an
input, not an option.

**The rest is not model error — it is worst-case versus actual.** The bound is
tight against the smeared layout and sits up to ~15× above a clustered one, and
no schema query can say which an instance has, because it depends on write order.
That can only be measured.

**So measure it.** `tsi_cache_sizer -probe-pairs N` ranks the schema's worst
(measurement, tag key) pairs, queries real values of each on the largest shard,
and reads the exact entry cost from the gauge delta. The query uses a 1 ms window
at the shard's start: the tag predicate resolves when the iterator is built,
before any points are read, so the entry is populated with no data scan —
**0.57 s for a 250 000-series predicate.**

Accuracy against the same instance, worst entry 131 317 B in the gauge and about
132 200 B in heap:

| source | result | vs. heap worst |
|---|---|---|
| probe (gauge delta) × 2.0 heap factor | 262 554 B | 1.99× |
| bound, span factor 1 | 132 960 B | **1.01×** |
| bound, span factor 2 (default) | 135 936 B | 1.03× |
| `bytes` stat (mean of resident) × 2.0 | 46 340 B | **0.35× — under** |

**Two corrections to the earlier guidance in this document.** The `bytes` stat is
a *mean* over resident entries, and a mean is not a safe stand-in for the worst
entry: on a workload mixing cheap and expensive predicates it lands well below
the one that sizes the cap. And the probe reads the gauge, so it measures
serialized size, not heap; the tool multiplies it by `-gauge-heap-factor`
(default 2.0), which is right for the reference sets and low for a database
whose sets are spread one member per container. Use the gauge to watch a
rollout; use the **heap-calibrated schema bound** with a known id span to choose
`max-size`, the probe with its factor as a check, and a heap profile (Part 3C)
for the last word.

Note on precision: the `bytes` stat is built on `SeriesIDSet.Bytes()` (`tsdb/series_set.go:31`),
which reports 24 bytes for the mutex, 8 for the pointer and roaring's serialized size. Against live
heap that is 3.4× under for an empty set, 4.8× for a one-id set and 1.9× for a 16-container set of
500; only dense sets come within 2 %. It is not "slightly" under, as this note once said. Ranking: a
heap profile first, the heap-calibrated bound second, the gauge for trend.

---

## Part 2b — Conservation of cardinality

Everything above sizes a *single entry*. That was the wrong unit, and it is why the
per-entry bound was 30× loose and kept declaring viable instances `TOO_TIGHT`.

**The identity.** Every series in a measurement carries exactly one value of each
tag key, so a tag key's values *partition* the measurement's series:

```
Σ over values v of |series(key, v)|  =  S        exactly
```

Caching every value of one tag key therefore holds `S` series' worth of roaring
payload — **whatever that key's cardinality**. A wide key gives many tiny
entries; a narrow key gives few enormous ones; the member total is identical.
The identity is exact in members. In bytes it bounds only the member payload,
2.5·S in heap; the containers those members sit in and the entries that hold
them are charged separately below. Measured on the 1M-series shard, in the
gauge:

| tag key | values | bytes/entry | key total | bytes/series |
|---|---|---|---|---|
| `region_rr` | 4 | 131 317 | 525 268 | 0.53 |
| `grid_rr` | 16 | 125 143 | 2 002 288 | 2.00 |
| `zone_rr` | 50 | 40 241 | 2 012 050 | 2.01 |
| `rack_rr` | 2 000 | 1 241 | 2 482 000 | 2.48 |

Cardinality spans 500×; the key total never exceeds ~2.5 bytes per series.

**Why the naive bound was impossible.** "Capacity × largest entry" assumes all C
entries can be as expensive as the worst one. They cannot: the worst entries come
from the narrowest key, and a key with V values offers only V of them. Filling a
larger cap forces you onto progressively wider keys with progressively cheaper
entries.

**The bound.** Take the most expensive entries first, key by key. Treating the
entries within a key as uniform makes this a fractional knapsack with uniform
value per item inside each group, and greedy is *exactly* optimal for that:

```
bound(C) = Σ over keys taken, narrowest first, of
             take · ( 2.5·S_m/V  +  96·c  +  352 )       c = min(S_m/V, ceil(idSpan/65536))
         + (C − entries the schema can supply) · 352
```

Three terms per entry: the member payload at the heap rate, the fixed cost of
the roaring containers the set occupies, and the entry itself. The container
term is bounded per key by `96·min(S_m, V·C)` where `C` is the number of
65 536-id blocks the database's id space spans, and it is what the earlier
`2·S_m·take/V + take·overhead` form lacked: on a set with one member per
container it is 38× the payload. The last line is the entries the cap can hold
beyond anything the schema supplies — see "Entries beyond the schema" below.

**It is the worst case under uniform tag values, not under any distribution.**
The identity pins a key's total at `S` members only once *every* value of the
key is taken. For a key taken in part, one dominant value can hold nearly `S`
where the fill charges `S/V`. The bound that holds under any skew is the simple
model's `2.5·S·min(T, n)` below, which is why it reads higher. The gap is small when the
cap exceeds the number of tag keys, since every narrow key is then taken in
full — about 3 % on the reference production profile, where 1600 entries cover
all 161 keys of the largest shard — and grows on a schema where a large
measurement has a key with far more values than the cap. The 2.0× figure below
came from a synthetic instance with uniform values, so it did not exercise this.

Measured at C = 1600 on the reference instance, filling the cache with the most
expensive entries the schema can supply. The measurement is the gauge, so it is
a floor on heap:

| | bytes | over gauge |
|---|---|---|
| measured worst-case fill (gauge) | 6.8 MB | — |
| **conservation bound, heap-calibrated** | **~15 MB** | **2.1×** |
| naive `cap × worst entry` | 200.4 MB | 29.3× |

**Entries beyond the schema: there is no saturation.** An earlier version of
this document called saturation "the strongest result": once every entry the
schema can produce is resident, more capacity buys nothing, so an instance
whose schema ceiling fit its budget needed no sizing at all. That is false. The
schema bounds the payload and the containers, not the number of entries. A
query for a tag value that does not exist still creates an entry — the file set
returns an empty set iterator for an absent value, the merge of set iterators
is a set iterator, and `Put` stores it; nothing checks existence first
(`tsi1/index.go:1130-1160`, `tsi1/file_set.go:343-369`). A dashboard variable
pointing at a decommissioned host, a template resolving to a value from another
database, or a typo each add one entry at 352 bytes, and only the cap bounds
how many. On the reference instance, whose schema-bounded terms come to about
525 MB, a cap of 100 000 000 would admit tens of gigabytes per shard of such
entries. So:

```
bound(C) = schema-bounded part (payload + containers, finite)  +  352 · C
```

The first part stops growing once the schema is exhausted; the second never
does. `tsi_cache_sizer -explain` reports both, and the `+` marker is gone.

**What it does not do.** Conservation holds *per tag key*. Across keys it
multiplies: T keys of 2 values each give 2T entries of S/2 series. The multiplier
is the number of tag keys the cap draws on — small, schema-fixed, and directly in
the formula. The user-facing claim "many entries and large entries are mutually
exclusive" is exactly true within a key and true up to a factor of T overall.

### Is there a single constant? Almost — and the "almost" is the useful part

The tempting shortcut is `bytes ≤ A·n` for some universal A. **It is not a bound.**
Checked against the measured fill it is violated at every capacity:

| n | measured (gauge) | `A·n` at A=352 | holds? |
|---|---|---|---|
| 4 | 67 006 | 1 408 | no, 47× under |
| 40 | 2 849 324 | 14 080 | no |
| 1 600 | 7 159 384 | 563 200 | no, 13× under |

`A·n` accounts only for per-entry *overhead*. The roaring *payload* is conserved
**per tag key**, so it does not scale with `n` at all, and the roaring
*containers* are bounded both per key and per entry. Separating them gives a
bound that holds everywhere:

```
bytes(n)  ≤  2.5·S·min(T, n)   +   96·min(S·min(T, n), n·C)   +   A·n
              └── payload ──┘       └───── containers ─────┘       └ overhead ┘

              C = ceil(idSpan / 65536), the 65 536-id blocks the database spans
```

**A is genuinely universal and exactly known, in heap.** A cache entry's fixed
cost is

```
A = 288 + len(measurement) + len(tag key) + len(tag value)
```

— 152 bytes of `seriesIDCacheElement`, evictor node and map slot, plus 136 for
an empty `SeriesIDSet`: its mutex and pointer in a 32-byte allocation and the
roaring bitmap struct with its three empty slices. Measured in live heap: 3
bytes of strings → 291 B. The gauge reports 192 plus strings for the same entry,
which is where this document's earlier `A = 192` came from. The tool uses 352,
which covers string totals up to 64 bytes.

**The container term is the one the earlier form lacked, and it can dominate.**
Each roaring container costs ~96 bytes of structure however few members it
holds. A key's values between them hold `S` series, so at most `S` containers;
and each entry's set occupies at most `C` containers, so `n` entries occupy at
most `n·C`. Without the id span only the first bound applies, and it charges
96 bytes per series per key — the fully dispersed case, safe and loose: on the
reference shard the simple bound is 10× higher without the span than with it.

**Below the crossover the schema-bounded terms size the cache; above it the
overhead grows without limit**, because an entry exists for any value a query
names, existing or not. At n = 1600 on the reference shard the overhead is
about 2 % of the bound.

The conserved quantity, correctly normalised, is per *series* rather than per
entry: **≈ 2.5·T bytes of payload per series in the shard plus up to 96·T of
container structure**, the latter bounded by `96·n·C` per shard. With T
typically 5–20 and a dense id space the payload dominates; with a large id
space and small sets the containers do.

This is the `simple` cost model (`-model simple`): it needs only `seriesCreate`,
one `SHOW TAG KEYS` and one `SHOW SERIES CARDINALITY` per database — no per-key
cardinality probe. Measured on the reference instance at n = 1600, against the
gauge figure:

| model | bound | vs gauge | inputs |
|---|---|---|---|
| conservation | ~15 MB | 2.1× | full schema profile + id span |
| **simple** | **~25 MB** | **3.6×** | series count + tag key count + cardinality |
| per-entry | 213 MB | 31× | bytes-per-entry |

### Worst case for an instance that fills the cache

When an instance has enough cardinality and enough query variety to occupy all `n`
entries on every shard, the cap is the binding constraint and the worst case is:

```
                 ┌ payload: conserved per tag key, independent of n ┐   ┌ containers: per key and per entry ┐   ┌ overhead ┐
B_instance(n)  =      2.5 · Σ_s [ S_s · j_s(n) ]                     +   96 · Σ_s min(S_s · j_s(n), n · C)    +   A · n · N
```

**Variables**

| symbol | meaning | where from | typical |
|---|---|---|---|
| `n` | configured `series-id-set-cache-max-size` | the setting being chosen | 100 – 16 000 |
| `N` | TSI shards on the instance — **all** of them, not just active ones, because a shard that stops being queried never shrinks | count of `tsi1_cache` entries in `/debug/vars` | 10 – 500 |
| `S_s` | series in shard `s` | `shard` stat `seriesCreate` | 10⁴ – 10⁷ |
| `j_s(n)` | tag keys the fill draws on in shard `s`: the count whose cumulative cardinality, narrowest first, reaches `n`. Fractional for the last, partly-taken key | sorted `V` profile per (measurement, tag key) | 2 – T |
| `T_s` | tag keys per measurement — the ceiling on `j_s` | `SHOW TAG KEYS` | 5 – 20 |
| `A` | fixed heap cost of one entry = `288 + len(measurement) + len(tag key) + len(tag value)` | schema naming | 300 – 400 |
| `C` | 65 536-id blocks the database's id space spans = `ceil(ids ever created / 65536)`; ids are never reused, so bound it by current cardinality × churn factor | `SHOW SERIES CARDINALITY` × `-id-span-factor` | 2 – 200 |
| `2.5` | heap bytes per member in an array container: roaring's 2, plus size-class slack | fixed | ≤ 1.19× measured |
| `96` | heap bytes of structure per roaring container, however few members it holds | fixed | 47 – 76 measured |

`j_s(n)` is the only term needing the cardinality profile. Bounding it by `min(T, n)`
gives the closed form the `simple` model uses:

```
B_instance(n)  ≤  2.5 · T · ΣS  +  96 · min(T · ΣS, n · C · N)  +  A · n · N          where ΣS = Σ_s S_s
```

`ΣS` is a single free number: sum `seriesCreate` across TSI shards.

**Solve for the cap:** with the container term at its per-entry bound,

```
n_max  =  (G − 2.5·T·ΣS) / ((A + 96·C) · N)
```

**Feasibility test — run this first.** The payload term does not contain `n`, so if

```
2.5 · T · ΣS  >  G
```

no cap is small enough. The instance's exposure is set by cardinality and schema
shape, not by the setting. Either raise the budget or leave that instance on the
fixed default.

Worked, at a 5 %-of-RAM budget:

Taking each database as a stable population the size of one shard, with a churn
factor of 2 for the id span:

| instance | shards | series/shard | T | C | payload | per entry `A + 96·C` | verdict |
|---|---|---|---|---|---|---|---|
| r5.2xlarge | 40 | 300 k | 10 | 10 | 0.30 GB | 1.3 KB | `n_max` ≈ 55 000 |
| r5.4xlarge | 60 | 500 k | 12 | 16 | 0.90 GB | 1.9 KB | `n_max` ≈ 48 000 |
| r5.8xlarge, dense | 120 | 5 M | 20 | 153 | 30.0 GB | 15 KB | **infeasible at any n** |

In the first two the cap is nowhere near binding at anything you would plausibly
configure. The third is where the real risk lives, and no choice of `max-size`
addresses it. Note how much `C` matters in the per-entry column: an earlier
version of this table, without the container term, put `n_max` at 312 000 and
400 000 for the first two rows. A database that has churned through many more
series than it holds has a larger `C` and a lower `n_max` than shown.

**One caution on the closed form.** `min(T, n)` assumes the fill exhausts every tag
key, which needs `n ≥ E` (the total distinct tag values in the shard). An instance
whose cap is below `E` has a smaller true `j_s(n)`, so the closed form overstates
the schema-bounded terms — by about 1.7× on the reference instance (3.6× against
the gauge figure for the simple bound, 2.1× for the exact fill). Before acting on
an `infeasible` verdict from the closed form, re-check with the exact `j_s(n)`
from `-model conservation`, which may well be feasible.

**What the closed form assumes:** a measurement's share of a shard matching its
share of the database; every tag key present on every series (absent keys only
lower it). It does *not* assume balanced tag values: skew moves cost between a
key's entries but not the key's total, and the closed form charges every drawn
key in full. That is the difference from the exact `j_s(n)` fill, which does
assume balance. **What it excludes:** clustering, which can only reduce cost
below the 2-bytes-per-series ceiling — measured as low as 0.07.

### Why this transfers between instances

The bound's inputs are:

| input | where from | stability |
|---|---|---|
| `S_m` per shard | `shard` stat `seriesCreate` | free, stable |
| `V` per (measurement, tag key) | schema meta queries | stable by premise |
| id span (`C`) | `SHOW SERIES CARDINALITY` × churn factor, or `-id-span` | stable; must be an upper bound, and grows with churn |
| `C` | the setting being chosen | shared |

**Query patterns are absent.** They decide *which* entries are cached; conservation
bounds the total regardless. That is what makes a canary generalize: the variable
that differs most between instances, and drifts most over time, does not appear.

So "instance A was stable at C" extends to instance B as an arithmetic check,
`bound_B(C) ≤ budget_B`, computed from two free metrics — not as a hope that B's
workload resembles A's. And for any instance where `ceiling_B ≤ budget_B`, no
check is needed at all.

---

## Part 3 — Data sources

### A. `/debug/vars` — instant, no `_internal` dependency

Per-shard cache stats appear keyed as `tsi1_cache:<path>:<id>` (`services/httpd/handler.go:2516-2551`),
alongside `memstats` and the `runtime` statistic.

```bash
curl -s localhost:8086/debug/vars | jq '
  [to_entries[] | select(.key|startswith("tsi1_cache")) | .value.values]
  | {shards: length,
     size:     (map(.size)|add),
     bytes:    (map(.bytes // 0)|add),
     capacity: (map(.capacity)|add),
     hit:      (map(.hit)|add),
     miss:     (map(.miss)|add),
     evict:    (map(.eviction)|add),
     shrink:   (map(.shrink_eviction)|add)}'
```

Two scrapes N seconds apart give rates. `.memstats.HeapInuse` and `.memstats.Sys` give the heap side.
This is the right tool when `monitor.store-enabled` is false.

### B. InfluxQL against `_internal` — history

The monitor service writes every statistic to `_internal` by default, at a 10 s interval
(`monitor/config.go:12-20`), so `tsi1_cache`, `shard`, and `runtime` are all queryable as time series.

```sql
-- Per-shard cache behaviour over 24h.
SELECT non_negative_difference(max("hit"))      AS d_hit,
       non_negative_difference(max("miss"))     AS d_miss,
       non_negative_difference(max("eviction")) AS d_evict,
       max("size")     AS size,
       max("capacity") AS cap
FROM "_internal"."monitor"."tsi1_cache"
WHERE time > now() - 24h
GROUP BY time(5m), "id", "database" fill(none);

-- Fleet-wide total capacity and occupancy right now.
SELECT sum("capacity") FROM
  (SELECT last("capacity") AS capacity
   FROM "_internal"."monitor"."tsi1_cache"
   WHERE time > now() - 10m GROUP BY "id");

-- Per-shard series count and index type. seriesCreate is engine.SeriesN()
-- (tsdb/shard.go:305,322); indexType gates eligibility.
SELECT last("seriesCreate")
FROM "_internal"."monitor"."shard"
WHERE time > now() - 10m
GROUP BY "id", "database", "retentionPolicy", "indexType";

-- Heap headroom over two weeks.
SELECT max("HeapInUse"), max("Sys")
FROM "_internal"."monitor"."runtime"
WHERE time > now() - 14d GROUP BY time(1h);
```

Compute the hit-rate ratio client-side. Do not try to divide two `non_negative_difference` results
inside one statement. Counters reset on restart, which `non_negative_difference` handles; a plain
`max - min` over a window does not.

Use the windowed deltas, never the lifetime `hit/(hit+miss)` ratio from `/debug/vars`, when judging
whether a cache is meeting its target. The policy acts on a recent windowed rate, and the lifetime
ratio carries every miss of the climb, so it reads low for a long time after a cache reaches its
ceiling. `tsi_cache_sizer` runs the per-shard form of the query above (`GROUP BY "id"` on the outer
statement) for exactly this reason.

### C. `/debug/pprof/heap` — ground truth for bytes

Retained bitmaps are allocated inside `innerLockingPut` → `SeriesIDSet.Clone`. The transient clone on
the *hit* path is allocated in `TagValueSeriesIDIterator` (`tsi1/index.go:1134`), so a focus on
`innerLockingPut` cleanly separates retained cache bytes from query-path churn.

```bash
curl -s "http://localhost:8086/debug/pprof/heap" > heap.prof
go tool pprof -inuse_space -focus='innerLockingPut' -top heap.prof
```

```
measured_bytes_per_entry = inuse_space(innerLockingPut) / Σ size
```

This is the most valuable single number in the whole exercise. Caveat: heap profiles are sampled at
`MemProfileRate` (512 KiB default), so aggregate totals are unbiased but noisy when the total is
small. `/debug/pprof/heap?debug=1` gives a text form; `/debug/pprof/all` bundles an archive.
Both require `[http] pprof-enabled = true` (the default).

### D. Schema ceiling from InfluxQL alone — no canary required

```sql
-- Per-measurement series cardinality (HLL estimate, cheap).
-- The FROM clause is what selects the count_hll rewrite
-- (query/statement_rewriter.go:199-240); without it you get one db-wide number.
SHOW SERIES CARDINALITY ON "db" FROM /.*/;

-- Distinct tag values per (measurement, tag key). Iterate the keys from SHOW TAG KEYS;
-- a regex on KEY collapses all keys into one count per measurement, which is not what you want.
SHOW TAG KEYS ON "db";
SHOW TAG VALUES CARDINALITY ON "db" WITH KEY = "<key>";

-- Shard inventory.
SHOW SHARDS;
```

Combine with per-shard `seriesCreate` from (B) to get `k` for each predicate shape. Assume a
measurement's share of a shard's series matches its share of the database's series; the problem
statement grants that schema and cardinality are stable per instance, which is what makes this
substitution sound.

### E. `SHOW QUERIES` — the variable half

Sample every few seconds for an hour and extract `tag = 'value'` predicates. This tells you which of
the schema's predicate shapes are actually exercised. It is the only part of the input that the
problem statement warns may drift, which is why it informs the *expected* sizing and never the
*safety* bound.

---

## Part 4 — The procedure

Stages 0 through 3 and 5 are automated by `cmd/tsi_cache_sizer` (see its
README); Stage 4 is the manual canary the tool's output is meant to target.

```bash
go build ./cmd/tsi_cache_sizer
./tsi_cache_sizer -inventory fleet.csv
```

### Stage 0 — Eligibility screen

Exclude any instance where:

- `indexType != tsi1` (no TSI cache exists — `tsdb/config.go:17`)
- p95 `HeapInUse` > 50% of RAM
- p99 `HeapInUse` > 70% of RAM

Those instances have no headroom to lend, by the stated definition of healthy.

Also establish what the instance already runs. On a build that publishes the adaptive settings in
its config diagnostics, read them from `/debug/vars`. On an older build, look at the per-shard
`capacity` statistics: a fixed-size cache gives every shard the same capacity, so differing
capacities prove adaptive sizing is already on, and a uniform capacity other than the floor means
the configured size is not what you assumed. The production clone this method was checked against
reported a capacity total that was not a multiple of its shard count — adaptive sizing on, at an
unknown `max-size` — which changes how every later stage's numbers read. `tsi_cache_sizer` makes
this check and says so. When you know the configuration, pass it with `-current-max-size` and
`-current-target`: the tool then prices that cap against the same budget and model the
recommendation uses (the clone's 100 000 came out at 9.3 GiB against a 332 MiB budget, 62× the
largest cap that fits, with an assumed id span of 250 000 supplied by `-id-span` because the saved
profile predates the field) and runs the pinned-shard check against it.

Check the process uptime against the history window too. `_internal` is an ordinary database in
the data directory, so a clone carries the source instance's history, and a window longer than the
uptime spans both, joined at a counter reset that `non_negative_difference` drops silently. The
tool reads `system.uptime` from `/debug/vars` and warns when `-window` exceeds it.

### Stage 1 — Does it even help?

Per shard, from `tsi1_cache`: if `d_hit / (d_hit + d_miss) ≥ 0.95` and `d_evict ≈ 0` at the default
capacity of 100, the working set already fits. Adaptive sizing buys nothing there but risk.

Only instances showing **sustained eviction together with a low hit rate** are candidates. This
filter alone will cut the rollout set substantially, and it is the entire "helpful" half of the goal
— everything after this is the "safe" half.

### Stage 2 — Memory budget

```
G_i = min( 0.05 · RAM_i ,  0.50 · (0.50 · RAM_i − Heap_p95_i) )
```

Capped at 5% of RAM, tapering to zero as an instance approaches the 50% line. Properties worth
noting: an instance at 40% of RAM gets the full 5%; an instance at 50% gets nothing and is correctly
excluded; the burst reserve for compaction and backfill is never more than a tenth consumed.

`G` is denominated in live-heap bytes, as `HeapInUse` reports them and as the `bytes` gauge and the
cost models count them. With the default GC target the resident cost of each live byte is roughly
double; the 50% healthy line is what allows for that, so do not apply a second factor.

r5 reference (5% of RAM):

| Instance | RAM | 5% budget |
|---|---|---|
| r5.large | 16 GiB | 819 MB |
| r5.xlarge | 32 GiB | 1.6 GB |
| r5.2xlarge | 64 GiB | 3.2 GB |
| r5.4xlarge | 128 GiB | 6.4 GB |
| r5.8xlarge | 256 GiB | 12.8 GB |
| r5.12xlarge | 384 GiB | 19.2 GB |
| r5.16xlarge | 512 GiB | 25.6 GB |
| r5.24xlarge | 768 GiB | 38.4 GB |

### Stage 3 — Convert the budget to an entry cap

**Preferred: the conservation bound (Part 2b).** Build a `(measurement, tag key) →
(values, series-in-shard)` profile from the schema plus each shard's
`seriesCreate`, then take the largest capacity whose adversarial fill fits `G_i`.
This needs no per-entry figure at all, and it is what `tsi_cache_sizer` uses by
default. It needs the series id span; without one every series is costed as its
own roaring container, which is safe and loose, and the tool says so. There is
no "any capacity is safe" outcome: the per-entry overhead grows with the cap
whatever the schema holds (Part 2b, "Entries beyond the schema").

Every TSI shard must contribute to the sum. A shard the schema probe cannot
profile is costed with the simple bound rather than dropped, since a failed
probe on one database must not silently remove all of its shards from the
bound; the tool reports how many shards fell back and how many series they hold.

**Fallback, when the schema is unavailable:**

```
max_size_i = G_i / (N_i · B_i)
```

This is the bound conservation replaces. It overestimates by ~29× because it
assumes a cache can be filled entirely with worst-case entries, which the
partition identity forbids. Expect it to report `TOO_TIGHT` on instances that are
in fact fine.

- `N_i` — TSI shard count, from `SHOW SHARDS` filtered by `indexType`.
- `B_i` — bytes per entry: the Part 2 schema worst case for the a-priori screen, replaced by the
  pprof-measured value from Stage 4 once a canary exists.

Use **total** shard count for the hard bound. Separately measure `N_active` — shards that have held
any entry at all — over a multi-day window. (Not "exceeded the floor": before a rollout every cache
is pinned at the fixed size, so a full shard sits exactly at the floor and that test would report
no active shards anywhere.) If `N_total / N_active > 3`, the instance is
exposed to sweep-driven growth from downsampling or backfills touching historical shards. Because
idle shards never shrink, that exposure is real and you must take the conservative number.

### Stage 4 — Canary, one per configuration

Pick the highest-`N_i · B_i` member of the configuration, not a typical one.

Settings: `max-size` at roughly 16× the default, and `target-hit-rate` from `tsi_cache_sizer` rather
than a fixed number — 0.90 as a floor, raised toward the rate the canary has demonstrated. Watch for
shards pinning at `max-size` while their *windowed* rate is below target; that means the target is
above their achievable ceiling, and the rate they settle at is the value to set it just under. Judge
it over the `_internal` window, not from lifetime counters. A pin that comes and goes with the
workload, while `bytes` stays inside the modelled worst case, may be acceptable as is: it clears on
its own when the novel-predicate traffic subsides.

Run for at least 7 days, covering a full weekly query cycle and at least one backfill or downsample
window. Record:

- steady-state `Σ size` and `Σ capacity`
- pprof cache bytes (Part 3C) and the derived `bytes_per_entry`
- `HeapInUse` delta against the pre-change baseline
- resize log lines: `"tsi cache capacity increased"` / `"tsi cache capacity decreased"`, which carry
  `old_capacity`, `new_capacity`, `hit_rate`, `gets_window`, `evicted`
  (`tsi1/cache.go:631-646`)

The capacity plateau where growth stops **is** the working set. That is the number worth knowing.

### Stage 5 — Set the configuration value

```
max_size(config C) = min over i ∈ C of max_size_i
```

If that minimum is not at least ~4× the default of 100, adaptive sizing is not worth enabling for
that configuration. Split the configuration or carve out the outlier instances instead of accepting
a value that only helps the median member.

---

## Part 5 — Verify the grouping before building size-tiered configurations

The premise "larger configurations for larger instances get higher max sizes" holds only if
`N_i · B_i` does not grow with instance size. It usually does: bigger instances carry more shards
**and** higher cardinality, and both sit in the denominator of Stage 3.

**Compute `G_i / (N_i · B_i)` for every instance and check whether it actually correlates with
instance size before committing to a size-keyed tier scheme.** If it does not, the correct grouping
key is `N · B` — schema shape — and instance size is a proxy that will silently under-protect the
densest instances in each tier.

Worked contrast, both r5.2xlarge, 64 GiB, `G = 3.2 GB`, `N = 60` shards:

| Schema | B | `max_size_i` | Verdict |
|---|---|---|---|
| Wide tags (`host`, `sensor_id`) | 4 KB | ~13 000 → set **8192** | Large win available |
| Narrow tags (`region`, `env`) | 250 KB | ~213 | Not worth enabling |

Identical instance size, identical budget, two orders of magnitude apart in the safe cap.

---

## Part 6 — Knob guidance

### `target-hit-rate` is the real control, not `max-size`

It sets both the equilibrium size and the reclaim responsiveness, and it moves them in the same
direction. From `atTargetEvictionGateLimit` and `adaptiveWindowLen` (`tsi1/cache.go:836-911`):

| target | grows until | shrink eviction gate (m=1000 gets, z=2.5) | shrink window |
|---|---|---|---|
| 0.80 | 80% hit rate | ≤ 168 evictions per 1000 gets | 1.6 n gets |
| 0.90 | 90% | ≤ 76 | 2.3 n |
| 0.95 | 95% | ≤ 33 | 3.0 n |
| 0.99 | 99% | ≤ 5 | 4.6 n |

At 0.99, nearly every real workload has enough tail to grow to `max-size` and essentially never
passes the shrink gate — the worst case becomes the expected case.

**Superseded — see the measurements below.** Two things in the table are wrong. The gate column is
evaluated at a fixed 1000-get window, but the real window scales with `n`, and at realistic sizes
the gate is *milder* at higher targets: at n = 8192 it asks for a hit rate 0.0072 above target at
0.85 and 0.0013 above at 0.99. And the hazard turned out not to be a high target but a target above
the workload's achievable hit rate, which is a different thing and not knowable from a fixed number.
`tsi_cache_sizer` now derives the target per instance (floor 0.90, raised toward whatever that
instance has demonstrated, or set just under the ceiling when shards are pinned at max-size), and
the lowest recommendation binds a shared configuration.

Two limits on that derivation. The "already demonstrated" rule rests on LRU being a stack algorithm,
which holds per shard; the rate the tool sees is the instance aggregate, dominated by its busiest
shards, so a target just under it can still exceed a cold shard's ceiling and pin that shard. And
for a shared configuration the member with the lowest ceiling binds everyone. Both cost hit rate on
the instances that could have gone higher, or memory inside the modelled worst case on the ones
that pin; neither costs safety. The asymmetry is the point: a target too high for an instance
costs bounded, self-clearing memory, while a target too low costs hit rate, so a uniform target
should be pitched at the fleet's highest-ε member with a margin of at least 0.005 below its ceiling.

### Measured, not just modelled — and the cost depends entirely on whether the working set fits

`TestAdaptiveTarget_BoundedWorkingSet` and `TestAdaptiveTarget_UnreachableTargetPinsAtMax` in
`tsdb/index/tsi1/cache_test.go` drive the real cache through both cases.

**Never compare capacities.** Capacity is a limit checked against `evictor.Len()`, not an
allocation: a slot that is never filled costs nothing, and `decideShrinkPre`'s slack branch trims
capacity down to occupancy once a window completes. Capacity also *oscillates* as the policy grows
and trims, so a point sample of it reports where in that cycle the run stopped. Occupancy and
`bytes` are the stable quantities.

**When the working set fits** (2 000 predicates, cache able to hold them):

| | target 0.85 | target 0.95 |
|---|---|---|
| capacity when sampled | 3 408 | 1 905 |
| **entries actually resident** | **1 759** | **1 905** |
| **bytes held** | **354 336** | **383 759 (1.08×)** |
| forced evictions | 53 331 | 19 830 (2.7× fewer) |
| reclaim pace after the set collapses | 4.81 gets/entry | 6.60 (1.37× slower) |

**8 % more memory for 2.7× less eviction churn.** A higher target is close to free here, and the
slack capacity it carries costs nothing at all. The 1.37× reclaim difference is the window length —
3.00 n against 1.90 n — measured per entry reclaimed, since the two started from different
capacities.

**When the working set does not fit** — a steady trickle of novel predicates caps the achievable hit
rate near 0.95, so nothing above that is reachable:

| | target 0.90 | target 0.99 |
|---|---|---|
| capacity (max 4 096, recurring pool 500) | 558 | **4 096 — pinned at max** |
| **entries resident** | **558** | **4 096** |
| **bytes held** | **112 539** | **844 762 (7.5×)** |
| unchanged after 100 000 further gets | yes | yes |
| lifetime hit rate | 0.906 | 0.948 |

Here the cache is full, so capacity *is* occupancy and the memory is entirely real: **7.5×, held for
as long as the novel trickle lasts, for 4 points of hit rate** — and those points come from holding
the 500-entry recurring pool, not from the other 3 500 entries, which chase compulsory misses no
cache size can serve.

So the risk of a high target is not that it wastes memory in general. It is that **when the working
set exceeds what the cache can hold, a target above the achievable hit rate converts the max-size
ceiling from a bound into the operating point** while the condition holds.

It holds exactly as long as the novel-predicate rate stays above `1 − target`. Both shrink gates
require the cache to be at or above target, so neither can pass until then; once the rate drops the
gates open and the existing policy reclaims the memory with no configuration change
(`TestAdaptiveTarget_PinnedCacheRecoversWhenNovelRateDrops`): from 16 384 entries to the 500-entry
working set in about 700 k gets at a 0 % or 0.5 % novel rate. At a rate that puts the ceiling exactly
on the target it stays pinned — the hit-rate gate passes at equality, the eviction gate does not.

Two corrections to how this document once described the shrink side. The `(1−T)·n` figure is the
floor on what is *detectably* cold under uniform access, not the amount shed: each event trims
`min(size/2, 1024, cold tail)`, and in the pinned scenario the cold tail at window end is about 74 %
of the cache. And the post-shrink cooldown runs concurrently with the next observation window, so
consecutive shrinks are one window apart, not two (`TestAdaptiveTarget_ShrinkCadenceIsOneWindow`).
When a working set collapses, each event halves the cache — geometric, not linear.

### The hit rate saturates well before max-size

A reasonable objection: if `max-size` is set inside the memory budget, spending
that budget to raise the hit rate is exactly what the cache is for. That holds —
**provided the memory is buying hit rate.** Past saturation it is not.
Measured at fixed capacities on a workload with an achievable ceiling of 0.95
(`TestHitRate_SaturatesWellBeforeMaxSize`):

| capacity | hit rate | bytes |
|---|---|---|
| 500 | 0.8404 | 100 741 |
| 600 | 0.9337 | 121 153 |
| **700** | **0.9486** | **141 796** |
| 1 000 | 0.9490 | 203 890 |
| 2 000 | 0.9490 | 410 890 |
| 8 192 | 0.9490 | 1 692 634 |

Up to saturation: **1.41× the bytes for +0.108 hit rate**. Past it: **11.9× the
bytes for +0.00044**. The entries beyond saturation hold predicates that will
never be queried again — the LRU tail is churning novel arrivals.

The growth rule's only brake is `hit rate < target`; it has no notion of marginal
gain per entry. A target below the achievable ceiling stops it at saturation. A
target above the ceiling **removes the stopping condition**, and it climbs to
`max-size` whether or not growth still helps. That — not memory waste in the
abstract — is what makes an unreachable target qualitatively different from a
merely high one.

Two things about the shrink side, one of them a correction. An earlier version of
this document blamed the hit-rate gate sitting ahead of the slack branch, citing a
live instance holding capacity 200 against 107 resident. That state is not
reachable: every miss is followed by a `Put` (the file set always returns a set
iterator, including an empty one for a nonexistent tag value), so free slots fill
while the trickle lasts, and if it stops the windowed rate rises to 1.0 and the
slack branch fires within about 1000 gets. On a full cache it is the eviction
gate, checked first and stricter than the hit-rate gate at every target, that
blocks; both encode "at or above target" and neither passes while the ceiling is
below it. What does hold is that an idle shard never shrinks, so the fleet-wide
worst case must be sized against every shard, not the active ones.

**So the target should sit just below the workload's achievable ceiling** — high
enough to reach saturation, low enough to stop there. The ceiling is observable
rather than guessable: a cache pinned at `max-size` has settled at it.
`tsi_cache_sizer` flags exactly this and reports the observed rate.

### Over-provisioning `max-size` is not the hazard

A natural worry is that setting `max-size` far above the working set lets
capacity run away. It does not. `TestAdaptiveTarget_OverProvisionedMaxIsSafe`
runs three caches from the configured floor with `max-size` at **32× the working
set**:

| | target | workload | final capacity | bytes |
|---|---|---|---|---|
| A | 0.95 | closed set of 500 | 479 | 96 169 |
| B | 0.99 | 500 + 5 % novel (ceiling ≈ 0.95) | **16 384 — max** | **3 183 646** |
| C | **0.99** | closed set of 500 | 984 | 100 390 |

**A** never approaches the ceiling: growth stops when the hit rate reaches
target, and the slack branch returns the headroom without evicting anything.
Doubling overshoots by up to 2×, briefly — the 984 in C is exactly that,
mid-oscillation.

**B** climbs monotonically to `max-size` with occupancy following it, because
there is always a fresh predicate to fill the next slot. 33× A's memory for a
*lower* hit rate (0.9486 against 0.9582) — though A and B differ in workload as
well as target. The like-for-like figure, from
`TestAdaptiveTarget_SameWorkloadDifferentTargets` on B's 5 %-novel workload, is
**21×**: target 0.94 settles at 748 entries and 151 kB, target 0.99 at 16 384
entries and 3.18 MB, both serving 0.9486.

**C is the control**: the same 0.99 that ran away in B, against a workload that
can deliver it. 1.04× A's memory, 0.032× B's.

So the hazard is not a high target, nor a generous `max-size`. It is
`target > achievable hit rate`, sustained. While it holds the cost is whatever
`max-size` allows, which is why `max-size` is the memory bound and the target is
only the efficiency control; everything else is safe at any `max-size`.

### Shrink feasibility check

```
seconds_per_shrink_evaluation ≈ (2.3 × capacity) / gets_per_sec      [at target 0.90]
```

with `gets_per_sec` from `Δ(hit + miss) / Δt` in `_internal`. An 8192-entry cache on a shard serving
2 Gets/sec needs ~2.6 hours per *evaluation*, and each successful one sheds at most 1024 entries
(`maxShrinkEvictPerEvent`, `tsi1/cache.go:40`). Consecutive events are one window apart — the
cooldown overlaps the next window rather than following it — so the full decay of that cache to a
few hundred entries is roughly eight events, or about a day. At target 0.99 the window is 4.6 ×
capacity and a 16 384-entry cache takes about four days. If this number comes out in weeks, treat
`max-size` as a permanent allocation and size it as such.

### `shrink-conservatism`

Leave it at the 2.5 default unless you have confirmed shrink can fire at all on that instance.
Lowering it to 1.0–1.5 speeds reclaim at the cost of grow/shrink oscillation risk. It cannot rescue a
cache whose shrink windows never complete — check feasibility first.

---

## Part 7 — Guardrails, rollout, rollback

- **Alert on `Σ bytes` crossing the budget divided by the gauge-to-heap factor** (or on
  `Σ capacity × B` on builds without the gauge), not on heap. Heap moves for many unrelated
  reasons and an alert on it teaches you nothing about the cache. The division matters: the gauge
  counts serialized set sizes and sits 1.5× to 5× below heap on a cache of small sets, so an alert
  at the raw budget fires late by that factor.
- **Also alert on `Σ capacity` rising while `Σ size` stays flat.** That is capacity acquired and not
  used — the signature of a sweep across historical shards.
- **And on any shard at `max-size` whose windowed hit rate is below target.** That is the
  unreachable-target signature; the rate it has settled at is that shard's ceiling. If it clears on
  its own within the weekly cycle and `Σ bytes` stayed inside the budget, note it; if it is
  sustained, lower the target for that instance.
- **Hold each canary for a full weekly cycle.** Peak usage is reached over days; a freshly restarted
  instance always looks fine because every cache resets to the floor.
- **Rollback is a config edit plus a restart.** `SIGHUP` will not apply TSI settings. Budget for the
  restart in the runbook, and note that the restart itself resets every cache to the floor, which
  will mask the very condition you were diagnosing.
- **Cap the first pass at ~16× the default regardless of what the budget allows.** The realistic win
  is capturing working sets of a few hundred to a few thousand predicates. The tail beyond that is
  where the OOM risk lives without a matching query-performance return.

---

## Part 8 — The `bytes` stat, and the cap that should follow it

**Done.** `tsi1_cache` now publishes `bytes`, an incrementally maintained gauge of the cache's heap
footprint: the series id sets plus each entry's structural overhead. It is accounted exactly at
insertion and eviction, and re-accounted when a set grows or shrinks in place as series are created
or dropped — so it tracks the long-lived entries on a still-growing measurement, which are precisely
the large ones. Cost on the series-creation path is 62 ns for a one-container set and 354 ns for a
256-container set (a set spanning 16M series ids), with no allocations; it does not touch the point
write path at all.

That replaces the estimate-then-canary chain for **trending** the risk. It does not replace it for
**measuring** it — the gauge counts each set at its serialized size and sits 1.02× to 11× below
heap depending on the set's shape, and accounting real heap per container would need
`roaring.Bitmap.Stats()` on every update — nor for **bounding** it, because the configured cap is
still an entry count.

The remaining change is a byte-denominated cap — `series-id-set-cache-max-bytes` alongside
`max-size`, with growth refusing to double once the gauge is over budget. That is the actual fix for
the 500×-per-entry variance in Part 2: it makes a single fleet-wide value safe across instances whose
per-entry costs differ by orders of magnitude, which is exactly the property Part 5 says a shared
configuration cannot otherwise have. Until it exists, the per-configuration value must still be
derived from each configuration's worst member.

---

## Appendix — Quick reference

**Enable (per instance, `[data]` section):**

```toml
series-id-set-cache-size = 100                    # floor; unchanged
series-id-set-cache-max-size = 8192               # from Stage 5
series-id-set-cache-target-hit-rate = 0.92        # derived, not fixed: see Part 6
series-id-set-cache-shrink-conservatism = 2.5     # default
```

Both `max-size` and `target-hit-rate` must be set together, `size` must be > 0, and `max-size` must
exceed `size`, or config validation rejects the file (`tsdb/config.go:326-337`). A restart is
required.

**Disable:** set `series-id-set-cache-max-size = 0` and `series-id-set-cache-target-hit-rate = 0.0`,
then restart. Behaviour reverts exactly to the fixed 100-entry cache of prior versions.

**Stat fields on `tsi1_cache`:** `hit`, `miss`, `eviction` (forced, under write pressure),
`shrink_eviction` (voluntary, from the shrink policy), `size` (occupancy), `capacity` (current cap),
`bytes` (estimated heap footprint of everything held).
Tagged with `id`, `database`, `retentionPolicy`, `path`, `walPath`, `engine`, `indexType`.

**The two numbers to alert on** are `bytes` summed across shards (against the budget from Stage 2)
and `capacity` rising while `size` stays flat (sweep exposure):

```sql
SELECT sum("bytes") FROM
  (SELECT last("bytes") AS bytes FROM "_internal"."monitor"."tsi1_cache"
   WHERE time > now() - 10m GROUP BY "id");
```
