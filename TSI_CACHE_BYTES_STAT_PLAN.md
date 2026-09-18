# Implementation Plan: `bytes` Statistic for the TSI Tag-Value Series-ID Cache

**Status:** design sketch, not implemented
**Target:** `master-1.x`
**Package:** `tsdb/index/tsi1`, with small additions to `tsdb`

---

## 1. Problem

`TagValueSeriesIDCache` (`tsdb/index/tsi1/cache.go`) is a per-shard LRU of
`{measurement, tagKey, tagValue} -> *tsdb.SeriesIDSet` (roaring bitmaps). The
adaptive-sizing commits (`e1f7dcc265`, `70aa5b6d8f`) added `hit`, `miss`,
`eviction`, `shrink_eviction`, `size` and `capacity` statistics under the
`tsi1_cache` measurement.

**All of these count entries. None counts bytes.** Capacity — including
`series-id-set-cache-max-size`, the ceiling operators are asked to set — is
denominated in LRU slots, and nothing in `cache.go` ever inspects how large an
entry is. An entry's cost is set by how many series the tag value matches, so
two shards at identical capacity can differ by three orders of magnitude in
resident bytes.

Consequences today:

- Operators cannot size `series-id-set-cache-max-size` against a memory budget.
- Support cannot answer "how much will raising the max cost?" without a heap
  profile, which is not available at fleet scale.
- `SHOW STATS FOR 'indexes'` does **not** help: `tsi1.Index.Bytes()`
  (`tsdb/index/tsi1/index.go:240-266`) walks partitions, sketches, path and
  config fields and never touches `i.tagValueCache`. It stays flat while the
  cache grows by gigabytes. (Arguably a separate defect; see §9.)

**Goal:** an always-on `bytes` field on the `tsi1_cache` statistic that
reasonably estimates the Go-heap cost of the cache, plus an opt-in exact
recount for audits.

---

## 2. Prerequisite finding: the cache holds no mmap-backed bytes

This must be understood before writing any accounting, and it is not obvious
from reading `cache.go` alone.

On a miss, the merged set handed to `Put` **does** alias the mmap'd index file:

1. `TagBlockValueElem.SeriesIDSet()` (`tag_block.go:375`) calls
   `SeriesIDSet.UnmarshalBinaryUnsafe` -> `roaring.Bitmap.FromBuffer`.
2. `roaringArray.fromBuffer` (`roaringarray.go:615-650` in
   `influxdata/roaring@fc520f41`) builds containers directly over the mmap'd
   buffer via `byteSliceAsUint16Slice` / `byteSliceAsUint64Slice`, and sets
   `ra.needCopyOnWrite[i] = true` for every container.
3. `FileSet.TagValueSeriesIDIterator` (`file_set.go:343`) merges with
   `SeriesIDSet.Merge` -> `roaring.FastOr` -> `lazyOR` -> `appendCopy`, whose
   test is:

   ```go
   copyonwrite := (ra.copyOnWrite && sa.copyOnWrite) || sa.needsCopyOnWrite(startingindex)
   ```

   `sa.needsCopyOnWrite` is true, so the merged bitmap **shares** the
   mmap-backed containers.

What makes the cache safe is that `lazyOR` builds `answer := NewBitmap()`,
whose `highlowcontainer.copyOnWrite` is `false`. So the `ss = ss.Clone()` in
`innerLockingPut` reaches `roaringArray.clone()` (`roaringarray.go:253-283`)
and takes the **deep-copy** branch, calling `container.clone()` on every
container; `arrayContainer.clone()` and `bitmapContainer.clone()` both do real
`make` + `copy`.

**Therefore every byte reachable from a cache entry is Go heap**, and the
accounting needs no mmap-exclusion logic. Excluding mmap is also the right
choice on the merits: the kernel can drop and refault those pages.

### Action item (independent of this plan)

The comment on that clone currently reads:

```go
// Ensure our SeriesIDSet is go heap backed.
if ss != nil {
    ss = ss.Clone()
}
```

It is load-bearing, not defensive: without it, cache entries would alias index
files that get unmapped on compaction. Strengthen the comment so nobody
removes the clone as an optimization. Suggested:

```go
// Clone to break the copy-on-write sharing established by
// roaringArray.fromBuffer: the merged set's containers alias the mmap'd index
// file, which is unmapped on compaction, and the cache outlives the retained
// FileSet. clone() deep-copies here because the lazyOR result's copyOnWrite is
// false. Do not remove this to save an allocation.
```

---

## 3. What there is to count

Per entry, all retained under the cache's own lock:

| Object | Bytes | Notes |
|---|---|---|
| `list.Element` | 40 -> **48** | Go size class |
| `seriesIDCacheElement` | 56 -> **64** | Go size class |
| `name`, `key`, `value` backing arrays | `sum sizeclass(len)` | **not shared** — `innerLockingPut` does `string(name)` per entry |
| leaf map slot | ~**31** amortized | `map[string]*list.Element`: 16 hdr + 8 ptr + 1 tophash, / (6.5/8) load |
| extra map headers | ~**500** | only for the first entry of a measurement / tag key |
| `tsdb.SeriesIDSet` | **32** | RWMutex 24 + ptr 8 |
| `roaring.Bitmap` + `roaringArray` | **128** | 5 slice headers + bool |
| `keys` / `containers` / `needCopyOnWrite` | `sc(2C) + sc(16C) + sc(C)` | C = container count |
| per array container | `24 + sc(2*card_c)` | struct (slice header) + content |
| per bitmap container | `32 + 8192` | struct + fixed bitmap; content is exact, page-sized |
| per run container | `40 + sc(4*runs)` | rare — only from `repairAfterLazy` on a full container |

Roaring container rules, verified against `influxdata/roaring@fc520f41`:

```go
func (ac *arrayContainer)  getSizeInBytes() int { return ac.getCardinality() * 2 }  // 2 B / id
func (bc *bitmapContainer) getSizeInBytes() int { return len(bc.bitmap) * 8 }       // 8192 B flat
// arrayDefaultMaxSize = 4096
```

InfluxDB never calls `RunOptimize`, so only array and bitmap containers occur
in practice, plus the rare all-ones run container from `repairAfterLazy`.

---

## 4. Calibration data

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

**Model check, `card=1`:**

```
48 + 64 + (8 + 8 + 16) + 31 + 32 + 128 + (8 + 16 + 8) + (24 + 8)  =  399 B
```

against 383 B measured — within 5%.

**Model check, per additional bitmap container:** predicted `8192 + ~52`;
measured `(36,940 - 12,208) / 3 = 8,244`. Within 1%.

The model is calibrated. Reproduce these numbers with a throwaway
`*_test.go` in `tsdb/index/tsi1` that fills a cache and diffs
`runtime.MemStats.HeapAlloc` after `runtime.GC()`.

---

## 5. Estimator

### Tier 1 — cheap, no roaring introspection, ~10% error

Uses `SeriesIDSet.Bytes()`, which already exists (`tsdb/series_set.go:31`) and
returns `24 + 8 + bitmap.GetSizeInBytes()`. `GetSizeInBytes` counts container
*content* but omits the ~52 B/container of struct + interface slot + parallel
slice overhead, so add it back:

```
entryBytes ~= 240
            + sizeclass(len(name)) + sizeclass(len(key)) + sizeclass(len(value))
            + ss.Bytes()
            + 52 * containers
```

Requires a container count, which `GetSizeInBytes` does not provide — so in
practice Tier 1 needs the same accessor as Tier 2 and there is little reason to
prefer it. Documented here only to explain why `SeriesIDSet.Bytes()` alone is
not sufficient.

### Tier 2 — exact container accounting (recommended)

The fork exports `Bitmap.Stats()` (`roaring.go:1401`), returning `Containers`,
`ArrayContainers`, `BitmapContainers`, `RunContainers` and their content bytes.
`Stats()` is O(C), which is free at `Put` time — the clone has just walked
every container anyway.

---

## 6. Where to hook it

All mutation points already hold the cache write lock, except one.

| Site | Effect | Lock held |
|---|---|---|
| `innerLockingPut` (after `Clone`) | `+entryBytes` | write |
| `evictLRULocked` | `-entryBytes` | write |
| shrink trim (`maybeShrinkLocked`) | via `evictLRULocked` | write |
| `addToSet` / `delete` | in-place bitmap mutation | **read** |

### Store the size on the element

`seriesIDCacheElement` is 56 bytes and already occupies the 64-byte size class,
so adding an `int64` field is **allocation-free**. This matters because
eviction must subtract exactly what `Put` added; recomputing at eviction time
would be wrong if `addToSet` has mutated the set since.

```go
type seriesIDCacheElement struct {
    name        string
    key         string
    value       string
    SeriesIDSet *tsdb.SeriesIDSet
    // bytes is the estimated Go-heap cost of this entry as measured at Put.
    // Stored so eviction subtracts exactly what insertion added, even if
    // addToSet/delete have since mutated the set. Free: 56 -> 64 bytes stays
    // inside the same allocator size class.
    bytes int64
}
```

### The in-place mutation problem

`addToSet` (`tsdb/index/tsi1/index.go:781`) runs under `c.RLock()` and mutates
cached bitmaps via `roaring.Add`. Two obstacles:

- The delta is not cheap to measure: `Add` may grow the content slice into the
  next size class, or promote array -> bitmap, a single **+8,160 byte** jump.
- `addToSet` is called per tag pair per new series on the write path;
  recomputing `Stats()` there is not acceptable.

Options considered:

| | Approach | Verdict |
|---|---|---|
| (a) | Document `bytes` as a Put-time figure; accept drift | **ship this** |
| (b) | Re-baseline during the shrink policy's existing O(size) `warmCountLocked` walk | reject — only fires when adaptive sizing is on *and* capacity is above the floor, so fixed-size caches never re-baseline; conditional behavior is more confusing than the drift |
| (c) | Opt-in exact recount via a dedicated `SHOW STATS` module | **ship this too** |

(a) is exact for entries never mutated — which is every entry on a shard that
is no longer taking writes, i.e. the historical majority. Drift is one-sided
(the estimate reads low).

(c) has direct precedent in the tree: `executeShowStatsStatement`
(`coordinator/statement_executor.go:1010`) already gates
`SHOW STATS FOR 'indexes'` behind an explicit module name, with the comment
*"The cost of collecting indexes metrics grows with the size of the indexes, so
only collect this stat when explicitly requested."*

### Why not just walk the LRU in `Statistics()`

The monitor calls `Statistics` every 10 s by default
(`monitor/config.go:20`). An O(size) walk with a `Stats()` call per entry,
under the cache write lock, would stall every query on that shard on a fixed
cadence — and there is one cache per shard. Not acceptable as an always-on
cost.

---

## 7. Code sketch

### 7.1 `tsdb/series_set.go` — expose container stats

```go
// BitmapStats returns container-level statistics for the underlying roaring
// bitmap. Exposed so callers can estimate heap footprint without importing
// roaring directly.
func (s *SeriesIDSet) BitmapStats() roaring.Statistics {
    s.RLock()
    defer s.RUnlock()
    return s.bitmap.Stats()
}
```

### 7.2 `tsdb/index/tsi1/cache.go` — the estimator

```go
// sizeclass rounds n up to Go's allocator size class for small objects.
// Above 32768 the allocator rounds to whole 8192-byte pages.
// See runtime/sizeclasses.go.
func sizeclass(n int) int64 { ... }

// estimateEntryBytes returns the estimated Go-heap cost of one cache entry.
//
// mmap-backed memory is out of scope by construction: Put clones the set
// before storing it, and roaringArray.clone takes the deep-copy branch because
// the merged bitmap's copyOnWrite is false, so every container reachable from
// a cached entry is heap-allocated. See the comment on the clone in
// innerLockingPut.
//
// Constants are calibrated against measured HeapAlloc deltas; the model is
// within ~5% for single-container entries and ~1% per additional bitmap
// container. It deliberately excludes allocator span slack and GC headroom
// (process RSS runs ~2x live heap at the default GOGC=100).
func estimateEntryBytes(name, key, value string, ss *tsdb.SeriesIDSet) int64 {
    const (
        listElem  = 48  // container/list.Element, 40B -> 48B class
        cacheElem = 64  // seriesIDCacheElement, 56B -> 64B class
        idSet     = 32  // tsdb.SeriesIDSet: RWMutex 24 + ptr 8
        bitmapHdr = 128 // roaring.Bitmap{roaringArray}: 5 slice headers + bool
        mapSlot   = 31  // amortized leaf map entry at 6.5/8 load factor
    )
    n := int64(listElem + cacheElem + idSet + bitmapHdr + mapSlot)
    n += sizeclass(len(name)) + sizeclass(len(key)) + sizeclass(len(value))
    if ss == nil {
        return n
    }

    st := ss.BitmapStats()
    c := int64(st.Containers)

    // roaringArray parallel slices: keys []uint16, containers []container,
    // needCopyOnWrite []bool.
    n += sizeclass(int(2*c)) + sizeclass(int(16*c)) + sizeclass(int(c))

    // Container structs plus content. Round each type's total rather than each
    // container individually: within a few percent, and O(1).
    n += 24*int64(st.ArrayContainers) + sizeclass(int(st.ArrayContainerBytes))
    n += 32*int64(st.BitmapContainers) + int64(st.BitmapContainerBytes) // 8192 each, exact
    n += 40*int64(st.RunContainers) + sizeclass(int(st.RunContainerBytes))
    return n
}
```

### 7.3 Counter and call sites

```go
const statTagValueCacheBytes = "bytes"

type TagValueSeriesIDCacheStatistics struct {
    Hits            atomic.Int64
    Misses          atomic.Int64
    Evictions       atomic.Int64
    ShrinkEvictions atomic.Int64
    Size            atomic.Int64
    Bytes           atomic.Int64 // estimated Go-heap cost; see estimateEntryBytes
}
```

```go
// innerLockingPut, after ss = ss.Clone()
n := estimateEntryBytes(nameStr, keyStr, valueStr, ss)
listElement := c.evictor.PushFront(&seriesIDCacheElement{
    name: nameStr, key: keyStr, value: valueStr, SeriesIDSet: ss, bytes: n,
})
c.stats.Bytes.Add(n)
```

```go
// evictLRULocked, alongside the existing map deletes
c.stats.Bytes.Add(-listElement.bytes)
```

Both sites already hold the write lock; `atomic.Int64` is used only so
`Statistics` can sample without taking the lock, matching the existing
treatment of `capacity`.

```go
// Statistics()
statTagValueCacheBytes: c.stats.Bytes.Load(),
```

### 7.4 Opt-in exact recount

```go
// RecountBytes walks the LRU and recomputes the byte estimate for every entry,
// re-baselining stats.Bytes against in-place mutations made by addToSet and
// delete. O(size * containers) under the write lock — call it from an
// explicitly requested diagnostic, never on the monitor's periodic path.
func (c *TagValueSeriesIDCache) RecountBytes() int64 { ... }
```

Surface it the way `SHOW STATS FOR 'indexes'` is surfaced — an explicit module
name in `executeShowStatsStatement`, not part of the default statistics sweep.

---

## 8. Files to change

- [ ] `tsdb/series_set.go` — add `BitmapStats()`
- [ ] `tsdb/index/tsi1/cache.go` — `sizeclass`, `estimateEntryBytes`,
      `Bytes` counter, `bytes` field on `seriesIDCacheElement`, hooks in
      `innerLockingPut` and `evictLRULocked`, `statTagValueCacheBytes` in
      `Statistics`, `RecountBytes`
- [ ] `tsdb/index/tsi1/cache.go` — strengthen the clone comment (§2)
- [ ] `coordinator/statement_executor.go` — opt-in module for `RecountBytes`
- [ ] `tsdb/index/tsi1/cache_test.go` — tests (§10)
- [ ] `etc/config.sample.toml` — mention the `bytes` field where the adaptive
      settings are documented, so operators know to budget against it
- [ ] `RUNBOOK_tsi_adaptive_cache_memory.md` — once shipped, this replaces
      Step 3.1 (heap profiling) and fleet heuristic H3 (restart regression)
      with a single query

---

## 9. Related defects found while designing this

Both are independent of this plan and worth filing separately.

1. **`tsi1.Index.Bytes()` omits the tag value cache**
   (`tsdb/index/tsi1/index.go:240-266`). `SHOW STATS FOR 'indexes'` therefore
   reports a `memoryBytes` that does not move as the cache grows. Once `bytes`
   exists, folding `c.stats.Bytes.Load()` into `Index.Bytes()` is a one-liner
   and makes the existing stat honest.

2. **`tsdb.Config.Diagnostics()` omits the three adaptive settings**
   (`tsdb/config.go:397-416`). It reports `series-id-set-cache-size` but not
   `-max-size`, `-target-hit-rate` or `-shrink-conservatism`, so
   `SHOW DIAGNOSTICS FOR 'config-data'` cannot confirm a fleet rollout. Three
   lines added to the `RowFromMap` call.

---

## 10. Testing

Per repo convention, new tests use `testify/require` — no `t.Fatal` /
`t.Errorf`; `require` on the test goroutine, `assert` off it.

**Unit — `estimateEntryBytes` is a pure function.** Table-driven over set
shapes: nil set, empty set, 1 id, 4,096 ids (array max), 4,097 ids (bitmap
promotion), multi-container, long tag values. Assert monotonicity in
cardinality and that the bitmap-promotion step is ~8,192.

**Unit — counter bookkeeping.**
- `Bytes` returns to 0 after every entry is evicted.
- `Bytes` never goes negative under interleaved Put/evict.
- Shrink trim decrements by the same amount insertion added.
- A `Put` of an already-present tuple (the `exists` early return) does not
  double-count.

**Accuracy — calibration guard.** Build a cache of N entries of known shape,
force GC, compare `stats.Bytes.Load()` against the `HeapAlloc` delta. Assert
agreement within a generous band (say 20%) so the test is a regression guard
on the constants rather than a flaky measurement. Mark it `testing.Short()`-
skippable; it is slow (the calibration run in §4 took ~70 s).

**Drift — documented, not asserted away.** A test that Puts, then calls
`addToSet` many times, then `RecountBytes`, and asserts the recount is `>=` the
incremental counter — pinning the one-sided direction of the drift.

---

## 11. Explicitly out of scope

- **mmap'd index and TSM pages.** Excluded by construction (§2), and correctly
  so — the kernel can drop and refault them.
- **Go allocator slack above size classes** — span fragmentation, `HeapIdle`
  not yet returned to the OS. Typically 5-15%.
- **GC headroom.** This measures *live* bytes. Process RSS runs roughly 2x
  that at GOGC=100 (nothing in the tree calls `SetGCPercent` or
  `SetMemoryLimit`). Alert thresholds should be set against live heap, with the
  2x applied when comparing to a container limit.
- **Transient clones.** Every cache *hit* returns `ss.Clone()` to the caller
  (`tsdb/index/tsi1/index.go:1134`). Short-lived; belongs to the query, not the
  cache.
- **Byte-based eviction.** Making `capacity` a byte budget rather than an entry
  count is a much larger change to the adaptive policy (`decideResize`,
  `decideShrinkPre`, `decideColdTail` all reason in entries). This plan only
  makes the cost observable. Byte-based admission would be a sensible follow-on
  once there is a year of `bytes` data to design against.

---

## 12. Validation after implementation

```bash
curl -s 'http://localhost:8086/debug/pprof/heap' \
  | go tool pprof -inuse_space -focus='TagValueSeriesIDCache' -top /usr/bin/influxd -
```

versus:

```sql
SELECT sum("bytes") FROM "_internal".."tsi1_cache" WHERE time > now() - 1m GROUP BY time(10s)
```

Expect agreement within ~10% on a cache not taking writes. Systematic
divergence points at the map-overhead terms (`mapSlot`, and the ~500 B of extra
headers for the first entry of a measurement) — the weakest part of the model,
since bucket overhead depends on how the three-level map is shaped by the
workload and a single amortized constant cannot capture "one entry per
measurement" versus "thousands of values under one key". If that proves
material in practice, the fix is to account the map headers at the sites in
`innerLockingPut` that actually create them (the two `goto EVICT` branches that
allocate a new `map[string]...`), rather than amortizing.
