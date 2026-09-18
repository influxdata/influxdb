# Runbook: is `series-id-set-cache-max-size = n` safe on this instance?

**Audience:** SRE and support, deciding whether to enable or raise adaptive TSI
caching on a specific instance.

**Time:** most instances resolve in Step 0 or Step 2, from one or two numbers
and no tooling.

Derivations, measurements and the tool are in
`TSI_ADAPTIVE_CACHE_SIZING_METHOD.md` and `cmd/tsi_cache_sizer/`. This page is
just the decision procedure.

---

## The one thing to understand first

The cache holds one entry per `(measurement, tag key, tag value)` that a
`WHERE tag = 'value'` predicate has touched, per shard — **including values that
do not exist**, which get an empty entry. A tag key's values divide the
measurement's series exactly once, so:

> **Caching a whole tag key holds every series of the measurement once —
> whether that key has 4 values or 4 million.**

A wide key gives many tiny entries; a narrow key gives few enormous ones; the
series total is the same. So the cap `n` controls memory less than it looks
like it should below the point where the schema is exhausted, and more than it
looks above it. What controls memory is **how many tag keys the cache can draw
on**, plus a per-entry cost that runs all the way to `n`.

Four constants, in heap bytes (the cache's own `bytes` gauge counts serialized
sizes and reads below these):

| | |
|---|---|
| **2.5 bytes** | per series, per tag key drawn on (payload) |
| **96 bytes** | per roaring container a set occupies; a set touches at most `C` |
| **352 bytes** | per cache entry, flat overhead, existing value or not |
| **`C`** | 65 536-id blocks the database spans: `ceil(2 × series cardinality / 65536)` |

---

## Step 0 — The `du` screen (one command, no schema knowledge)

Needs shell access on the host, and nothing else — no InfluxQL, no `/debug`.

```bash
DATA=/var/lib/influxdb/data
du -sb "$DATA"/*/*/*/index 2>/dev/null \
  | awk '{s+=$1; n++} END {printf "tsi_shards=%d  index_bytes=%d (%.1f MB)\n", n, s, s/1048576}'
```

The `.tsi` tag blocks store the same series ID sets the cache holds — as
serialized roaring bitmaps — so the cache's *payload* can never exceed what is
on disk. Heap adds slack on the payload, structure per roaring container that
the disk figure barely counts, and the per-entry overhead:

```
cache  ≤  1.25 × index_bytes  +  (352 + 96 × C) × n × tsi_shards
```

`C` is the number of 65 536-id blocks the database spans (Step 1); if you do
not have it yet, the second term with `C = 16` covers a database of up to a
million series ever created.

### ▶ If that total is comfortably under `G`: **safe at `n`. Stop.**

`G` is 5% of RAM for a healthy instance — see Step 1 for the table and for when
an instance is too hot to touch at all.

### ▶ Otherwise: go to Step 1. Failing here does **not** mean unsafe.

The bound is loose by design. On disk each set is stored once per series-file
partition (eight of them); the cache holds one merged copy, and merging a roaring
set can only shrink it. Measured against a fully-populated cache, in the gauge
(serialized sizes, so the actual heap is somewhat higher than the first row):

| | |
|---|---|
| actual cache, every possible entry (gauge) | 1 675 453 B |
| `du(index)` across 3 shards | 3 353 195 B |
| payload part of the bound, `1.25 × du` | 4 191 494 B — 2.5× the gauge |

### Three things to know

**The `diskBytes` stat will not do.** `Engine.DiskSize()` is the TSM file store
plus WAL; the TSI index is not in it. This check needs the filesystem.

**It drifts with compaction.** `du` counts every live compaction level (L1, L2,
L3 coexist) and the `.tsl` log files, so the number moves even when the schema
does not. That only ever makes it larger, so the bound holds — it just gets
looser.

**Do not invert it to estimate per-entry memory.** The tempting next step —
`index_bytes / entries` — does not work. The disk-to-memory ratio depends on how
dense a set is in the ID space, and measured across densities it swings from
1.01× to 29.16×. Two real schemas on the same instance size gave 1.55× and 2.11×.
Use Step 3 for per-entry reasoning.

---

## Step 1 — Collect four numbers

**`ΣS` — total series across TSI shards** (one command):

```bash
HOST=http://influx-host:8086
curl -s "$HOST/debug/vars" | jq '
  [ to_entries[]
    | select(.key | startswith("shard:"))
    | .value | select(.tags.indexType == "tsi1")
    | .values.seriesCreate
  ] | {tsi_shards: length, total_series: add}'
```

`tsi_shards` is **`N`**. If `total_series` is `null`, the instance has no TSI
shards — it is on the `inmem` index, has no TSI cache, and **nothing here
applies. Stop.**

**`G` — the memory budget.** If the instance's p95 heap is comfortably under 40%
of RAM, use **`G` = 5% of RAM**. If it is near or above 50%, the instance has no
burst reserve to lend and **should not be changed. Stop.**

| instance | RAM | `G` (5%) |
|---|---|---|
| r5.large | 16 GiB | 0.8 GB |
| r5.xlarge | 32 GiB | 1.6 GB |
| r5.2xlarge | 64 GiB | 3.2 GB |
| r5.4xlarge | 128 GiB | 6.4 GB |
| r5.8xlarge | 256 GiB | 12.8 GB |
| r5.12xlarge | 384 GiB | 19.2 GB |
| r5.16xlarge | 512 GiB | 25.6 GB |
| r5.24xlarge | 768 GiB | 38.4 GB |

To check the heap properly:

```sql
SELECT max("HeapInUse") FROM "_internal"."monitor"."runtime"
WHERE time > now() - 7d GROUP BY time(1h)
```

Take the p95 of those hourly peaks. If it exceeds 50% of RAM, stop.

**`T` — tag keys per measurement.** `SHOW TAG KEYS ON "<db>"`; take the largest
count on any one measurement. Usually 5–20.

**`C` — id blocks the database spans.** `SHOW SERIES CARDINALITY ON "<db>"`
gives the current series count; ids are never reused, so the span is that
number times a churn allowance (2 is the tool's default), and
`C = ceil(span / 65536)`. A stable 100 000-series database has `C = 4`; one
that has churned through 10 million has `C = 306`. **Understating `C`
understates the cost** — if series are frequently dropped and recreated, use a
larger factor.

---

## Step 2 — The key budget, at this `n`

The per-entry cost runs to the cap whatever the schema holds, so take it off
the top first:

```
OVERHEAD    =  (352 + 96 × C) × n × N
KEY BUDGET  =  (G − OVERHEAD) / (2.5 × ΣS)
```

### ▶ If `KEY BUDGET > T`: **safe at this `n`.**

The schema's whole payload fits what is left of the budget after the overhead.
Set `n` and move on. **No cardinality profile, no canary, no tooling.** Note
that this is "safe at this `n`", not "safe at any `n`": an earlier version of
this runbook said the latter, and it was wrong — a query for a tag value that
does not exist still creates an entry, so the overhead term is bounded only by
the cap.

### ▶ If `KEY BUDGET ≤ T`, or `OVERHEAD` alone exceeds `G`: go to Step 3.

### Worked, at `n = 1600`

| instance | N | series/shard | ΣS | C | G | OVERHEAD | KEY BUDGET | verdict |
|---|---|---|---|---|---|---|---|---|
| r5.2xlarge | 40 | 300 k | 12 M | 10 | 3.2 GB | 0.08 GB | **104** | safe at 1600 |
| r5.4xlarge | 60 | 500 k | 30 M | 16 | 6.4 GB | 0.18 GB | **83** | safe at 1600 |
| r5.8xlarge, dense | 120 | 5 M | 600 M | 153 | 12.8 GB | 2.9 GB | **6.6** | → Step 3 |

(`C` here takes each database as a stable population the size of one shard,
churn factor 2.) Real schemas have 5–20 tag keys, so a key budget above ~20
clears any of them. The first two instances are done in one command.

---

## Step 3 — Count the narrow tag keys

Only tag keys with **fewer than `n` distinct values** can be drawn on in full.
Count them **on the measurement holding most of your series**; call that `K(n)`.

> **Count on the dominant measurement, not across the database.** A tag key costs
> 2.5 bytes per series *of its own measurement*. A database with one big
> measurement and twenty tiny ones (every instance has `_internal`) has dozens of
> tag keys that together cost almost nothing. Counting them all is still an upper
> bound, but a useless one — on a test instance it read 59 when the real figure
> was under 8. If two or three measurements carry comparable shares, count each
> one's narrow keys and weight by its share of the series.

```
worst case  ≈  2.5 × ΣS × (K(n) + 1)   +   96 × min( ΣS × (K(n) + 1) , n × C × N )   +   352 × n × N
                └──── payload ────┘        └──────────── containers ────────────┘       └─ overhead ─┘
```

The container term is the smaller of "one container per series drawn on" and
"`C` containers per entry"; with `C` known the second usually binds, and it is
already inside `OVERHEAD` from Step 2.

### ▶ Safe at `n` if `K(n) + 1 < KEY BUDGET`

(strictly, if the whole expression is under `G`; with `OVERHEAD` taken off the
top in Step 2, the key count is the part that decides it)

You usually know `K(n)` without querying: at `n = 1 000` it is the keys like
`env`, `region`, `dc`, `cluster`, `tier` — the dimension tags, not the identity
tags. To confirm:

```sql
SHOW TAG KEYS ON "<db>"
SHOW TAG VALUES CARDINALITY ON "<db>" WITH KEY = "<key>"   -- per key
```

### Rule of thumb in one line

```
cache MB  ≈  2.5 × (millions of series across all shards) × (narrow tag keys + 1)
           + (0.35 + 0.1 × C) × (n × N / 1 000 000)
```

### Raising an existing cap

Going from `n₁` to `n₂` **unlocks the tag keys whose cardinality falls between
them**, at 2.5 bytes per series each, and adds `(352 + 96 × C) × (n₂ − n₁) × N`
of overhead whatever the schema. Check the absolute figure at `n₂` rather than
the delta — a partly-drawn key can complete across the step.

### Worked — the dense instance, `KEY BUDGET = 6.6` at `n = 1600`

| its schema | `K(1600)+1` | payload | + containers + overhead | at `n=1600` |
|---|---|---|---|---|
| few narrow keys (`2,5,10,1k,2k,5k,…`) | 4 | 6.0 GB | 9.5 GB | **fits** |
| many narrow keys (`2,3,5,8,12,20,32,…`) | 15 | 22.5 GB | 26 GB | **over** |

Same instance size, same cardinality, same tag key count — opposite answers. This
is why Step 3 exists and why instance size alone is not a safe grouping key.

---

## Step 4 — If Step 3 is marginal, measure it

```bash
go build ./cmd/tsi_cache_sizer
./tsi_cache_sizer -url "$HOST" -instance-type r5.8xlarge -model conservation -explain
```

`-explain` prints the real figure beside the Step 3 rule of thumb, for a range of
caps:

```
n      K(n)+1 (RULE OF THUMB)  EFF KEYS (ACTUAL)  PAIRS DRAWN  SCHEMA ENTRIES                                  PAYLOAD  CONTAINERS  OVERHEAD  TOTAL    OVERHEAD SHARE
100    7.99                    17.48              1203.60      19900                                           5.3M     5.7M        6.7M      17.7M    38%
1000   10.96                   85.67              1833.83      199000                                          26.2M    53.6M       66.8M     146.7M   46%
10000  10.96                   193.35             3495.36      1950736 (payload exhausted; 39264 more at overhead only)  59.2M  410.5M  668.0M  1.1G  59%
```

**`EFF KEYS` is the number to compare against your key budget.** It is the
payload expressed in whole-tag-key units, so it is directly comparable to `T`.

`PAIRS DRAWN` counts `(measurement, tag key)` pairs and is *not* comparable — on
this instance `_internal` pushes it past 60 while contributing almost nothing.
It is shown only to make that distinction visible.

`SCHEMA ENTRIES` is how many of the cap's entries a real tag value can back;
past that the payload and container columns stop growing and only the overhead
does, which is what "payload exhausted" marks. `CONTAINERS` is the term Step 3
bounds with `C`; it needs the id span, which the tool takes from
`SHOW SERIES CARDINALITY` times `-id-span-factor`. A saved profile from before
the span was recorded is costed as fully dispersed and says so; pass `-id-span`
to supply one.

`K(n)+1` is always ≥ `EFF KEYS`, so Step 3 never says "fits" when this says
otherwise. Typical slack is 1–2 keys, so an instance that fails Step 3 narrowly
may still pass here.

Two meta queries per database plus one per tag key. No canary, no restart,
read-only.

---

## Step 5 — Apply

```toml
[data]
  series-id-set-cache-size = 100                    # floor; leave alone
  series-id-set-cache-max-size = 1600               # n, from above
  series-id-set-cache-target-hit-rate = 0.90        # floor; see below
  series-id-set-cache-shrink-conservatism = 2.5     # default
```

Both `max-size` and `target-hit-rate` must be set together, and `max-size` must
exceed `size`, or the config is rejected at startup.

### Setting the target

0.90 is a floor, not the answer. The target's only job is to stop growth once the
hit rate is good enough, so **it must sit below what the workload can actually
achieve**. Above that ceiling the stopping condition is never met: the cache
grows to `max-size` and both shrink gates stay shut, because each requires the
cache to be at or above target. It stays there as long as the workload's
novel-predicate rate stays above `1 − target`; when that rate drops the gates
open and the cache decays back to its working set on its own (about four days
for a 16 384-entry cache at 2 gets/sec). So the pin is bounded by `max-size`
and episodic, not permanent — but while it lasts the memory buys no hit rate.

Raise it toward what the instance has already shown you:

- **If shards sit at `max-size` while their windowed hit rate is below
  target**, the rate they have settled at *is* the ceiling. Set the target about
  0.02 below it. Judge the rate over the `_internal` window, never from the
  lifetime `hit/(hit+miss)` counters, which carry every miss of the climb and
  read low for a long time afterwards.
- **Otherwise**, the hit rate observed at the current cache size is a floor on
  the ceiling — a bigger cache cannot do worse — so a target a little under it is
  safe. That argument holds per shard; the aggregate is dominated by the busy
  shards, so a cold shard can still pin. It costs memory inside the bound, not
  safety.
- `tsi_cache_sizer` does both and prints the reasoning; for a shared
  configuration, the lowest recommendation among members binds. A target too
  high for one member costs bounded, self-clearing memory; too low costs hit
  rate. Pitch a shared target at the member with the highest novel rate.

A target that is too low is a milder failure than one that is too high, but it is
not free: measured, 0.90 settled at 558 entries and 0.906 where 700 entries would
have served 0.9486.

**A restart is required. `SIGHUP` does not reload TSI settings.**

---

## Step 6 — Watch

```sql
-- total cache bytes; compare against G
SELECT sum("bytes") FROM
  (SELECT last("bytes") AS bytes FROM "_internal"."monitor"."tsi1_cache"
   WHERE time > now() - 10m GROUP BY "id");

-- capacity rising while occupancy stays flat = sweep exposure
SELECT last("capacity"), last("size") FROM "_internal"."monitor"."tsi1_cache"
WHERE time > now() - 1h GROUP BY "id";
```

**Alert on `Σ bytes` against the budget divided by two — not on heap.** Heap
moves for many unrelated reasons. The division is because the gauge counts each
set at its serialized size and sits 1.5× to 5× below heap on a cache of small
sets (up to 11× for sets spread one member per container), so an alert at the
raw budget fires late.

**Also alert on any shard at `max-size` whose windowed hit rate is below
target.** That is the unreachable-target signature. If it clears on its own
within the weekly cycle and `Σ bytes` stayed inside the budget, note it; if it
is sustained, lower the target for that instance.

Hold for a **full week** before declaring success, and longer on a
low-traffic instance: the doubling ladder from 100 to a cap of 100 000 needs
about 300 k misses per shard, which at a few hundred thousand gets per shard
per week is months. Peak usage builds over that whole climb, and every cache
resets to the floor on restart, so a freshly restarted instance always looks
fine. If the process has been up for less than the window you are reading, the
`_internal` history belongs partly to a previous process — or, on a clone of a
data directory, to the source instance.

---

## Rollback

Set `series-id-set-cache-max-size = 0` and
`series-id-set-cache-target-hit-rate = 0.0`, then **restart**. Behaviour reverts
exactly to the fixed 100-entry cache of prior versions.

The restart also empties every cache, which will mask whatever you were
diagnosing. Capture `/debug/vars` first.

---

## Gotchas

**A shard that stops being queried never shrinks.** Shrink is driven from the
read path. Memory acquired during a backfill or downsample sweep across
historical shards is held until restart. This is why `N` in the formula is *all*
TSI shards, not just the active ones.

**The `bytes` stat is a mean of serialized sizes — do not size from it.** It
averages whatever is resident, cheap entries included (measured at 0.18× of the
true worst entry on a mixed workload), and it counts each set at its serialized
size, which is 1.02× below heap for a dense set, 3× to 5× for the empty and
one-id sets that fill most caches, and up to 11× for a set spread one member
per roaring container. It is the right thing to *trend* on and the wrong thing
to size from.

**Memory is acquired in minutes and released in days, or never.** Growth doubles
on demand. Shrink is gated on the cache being at or above target, needs
`~2.3 × capacity` queries per window (at target 0.90) to evaluate, and sheds up
to half the cache or 1 024 entries per event, one event per window — a cache
far above its working set halves each window and a 16 384-entry cache is back
at 500 in about four days at 2 gets/sec. It does not shrink at all while the
target is above what the workload can reach, and an idle shard never shrinks.
Size `max-size` as a permanent allocation for those two cases.

**An entry exists for every value queried, existing or not.** A dashboard
variable pointing at a decommissioned host, a template resolving to another
database's values, or a typo each cost an entry. The schema bounds the payload,
not the entry count; only the cap does.

**Instances on the `inmem` index have no TSI cache.** Check `indexType` first;
the default `index-version` is `inmem`, not `tsi1`.

---

## Quick reference

```
index_bytes   du -sb <data>/*/*/*/index                filesystem (NOT diskBytes)
ΣS            sum of seriesCreate over TSI shards      /debug/vars
N             count of TSI shards                      /debug/vars
T             tag keys on the largest measurement      SHOW TAG KEYS
C             ceil(2 × series cardinality / 65536)     SHOW SERIES CARDINALITY
K(n)          narrow tag keys on the DOMINANT           SHOW TAG VALUES CARDINALITY
              measurement (fewer than n values)
G             5% of RAM (if p95 heap < 40% of RAM)

OVERHEAD    =  (352 + 96×C) × n × N

  1.25 × index_bytes + OVERHEAD  <  G   →  safe at n     (Step 0, loose but free)

KEY BUDGET  =  (G − OVERHEAD) / (2.5 × ΣS)

  KEY BUDGET > T         →  safe at this n
  K(n) + 1 < KEY BUDGET  →  safe at n
  otherwise              →  tsi_cache_sizer -model conservation -explain

worst case  =  2.5 × ΣS × (K(n)+1)  +  96 × min(ΣS × (K(n)+1), n×C×N)  +  352 × n × N
cache MB    ≈  2.5 × (millions of series) × (narrow tag keys + 1)  +  (0.35 + 0.1×C) × n×N/1e6

bytes gauge   serialized sizes: heap is 1.02× (dense) to 11× (dispersed) higher
```
