# Adaptive TSI cache: what happens when `target-hit-rate` exceeds what the workload can achieve

**Summary.** Setting `series-id-set-cache-target-hit-rate` above the hit rate a
workload can actually reach converts the adaptive cache into a fixed one pinned
at `series-id-set-cache-max-size` for as long as the workload stays that way.
Capacity climbs monotonically to the ceiling, occupancy follows it, and neither
shrink path can fire because both shrink gates require the cache to be at or
above target. Measured on the same workload: **21× the memory for the same hit
rate** as a target set just under the ceiling.

The pin is not permanent. It holds exactly as long as the workload's novel-query
rate stays above `1 − target`. When that rate drops, the existing shrink policy
reclaims the memory in days, not months, with no configuration change. What is
lost is the memory during the episode, and the fact that nothing in the growth
rule notices it is buying nothing.

Each part of the policy behaves as written. The problem is an interaction, and it
is not visible from any single function.

---

## 1. The condition

A tag-value predicate the cache has never held cannot be served at any capacity.
Call the steady-state fraction of such gets ε — new hosts, new pods, a dashboard
someone built this morning, an ad-hoc query. Then

```
achievable hit rate  ≤  1 − ε
```

If `target > 1 − ε` the target is unreachable. ε is a property of the workload,
varies by instance and over time, and is observable: once a cache is larger
than its working set, the miss rate in `tsi1_cache` `hit`/`miss` *is* ε. A
pinned cache reports it directly (section 5).

How tight this is, computed against the shrink eviction gate for an 8192-entry
cache:

| ε (novel gets) | achievable | highest target at which shrink can still fire |
|---|---|---|
| 0.1 % | 0.999 | 0.998 |
| 1 % | 0.990 | 0.988 |
| 2 % | 0.980 | 0.977 |
| 5 % | 0.950 | 0.946 |

A target of 0.99 is fine at ε = 0.1 % and pinned at ε = 2 %.

Note the gate, not the target, sets the bound: at ε = 1 % a target of 0.99
equals the ceiling exactly, growth stalls, and the hit-rate gate passes, but the
eviction gate does not (measured: pinned at 15 360 of 16 384 indefinitely).

---

## 2. What goes wrong, and why

### 2.1 Growth has no stopping signal other than the target

`decideResize` grows when `rate < target` and `capacity < maxCapacity`. There is
no term for marginal gain — nothing asks whether the previous doubling improved
anything. When the target is unreachable, `rate >= target` is never satisfied, so
the cache doubles every window until it reaches `maxCapacity`, and then sits
there.

This is the root cause. The shrink gates determine why nothing reverses it.

### 2.2 Both shrink gates require the cache to be at or above target

`decideShrinkPre` evaluates in this order:

```go
if evictionsW > atTargetEvictionGateLimit(...) { return ... }  // eviction gate
if float64(hitsW)/float64(gets) < target      { return ... }  // hit-rate gate
if capacity > size                            { ... }         // slack branch
return capacity, false, true                                  // cold tail
```

Both gates encode "the cache is meeting its target", and neither can pass while
the target is unreachable. Which one blocks depends on whether the cache is
full:

- **Full cache (the pinned case).** Occupancy follows capacity because every
  novel predicate fills a slot, so the cache is always full and always evicting.
  The eviction gate is checked first and is the stricter of the two at every
  target (section 2.5): at n = 8192 and target 0.99 it needs a hit rate of
  0.9913 against the hit-rate gate's 0.99. The hit-rate gate is never reached.
  The code comment on `decideShrinkPre` says as much: the hit-rate gate "is kept
  for defense".
- **Capacity above occupancy.** No forced evictions, so the eviction gate passes
  trivially and the hit-rate gate is what blocks the slack branch. This state
  cannot persist under a novel trickle: every miss is followed by a `Put`
  (`Index.TagValueSeriesIDIterator` always gets a set iterator back, including
  an empty one for a nonexistent tag value), so the free slots fill. If the
  trickle stops, the windowed hit rate goes to 1.0 and the slack branch fires.
  Measured (floor 100, max 200, target 0.99, pinned with 107 resident): capacity
  200 → 107 within 1 000 gets of the last novel predicate. Slack below target is
  transient, and unfilled slack costs no memory in any case.

An earlier version of this note reported a live instance sitting stable at
capacity 200 with 107 resident and a hit rate of 0.948. That state is not
reachable under the code above; the 0.948 was almost certainly the lifetime
`hit/(hit+miss)` ratio, which is not the windowed rate the policy uses.

### 2.3 What shrink can reclaim, and how fast

`adaptiveWindowLen` is `n·ln(1/(1−T))` gets, derived so that under uniform access
over n items the expected untouched fraction after one window is ≤ `(1 − T)`.
That is a floor on what is *detectably* cold, not a bound on what gets shed. The
amount shed per event is `decideColdTail`'s `min(size/2, 1024, cold tail)`, and
the cold tail is whatever the workload left untouched.

Two things an earlier version of this note got wrong:

- **The cooldown does not add a window.** `cooldownGets` ticks down during the
  next observation window, not after it, and both are sized to the same n after a
  shrink. Measured shrink-to-shrink intervals (contracting working set, target
  0.95): 4 957, 2 479, 1 239, 619 gets — each exactly `window(oldCap)`, never
  `2·window`. Pinned by `TestAdaptiveTarget_ShrinkCadenceIsOneWindow`, which
  also asserts that each event halves the cache (1 655 → 828 → 414 → 207 → 104).
- **The cold tail is not `(1 − T)`.** In the pinned scenario B below, the warm
  footprint at window end is ~4 270 of 16 384 entries (the 500-entry working set
  plus the ~3 770 novel entries pushed in front of it during the window). 74 %
  of the cache is cold, and each event would shed the 1 024-entry cap. Under a
  working set that has contracted, each event halves the cache.

So the pace is set by `1024 / window(n)` for a large cold cache and by halving
for a small one, and the target enters only through the window length,
`ln(1/(1−T))`: 1.9n at 0.85, 3.0n at 0.95, 4.6n at 0.99. The measured pace
difference between 0.85 and 0.95 on a contracting working set is 1.37×
(section 4.3), not the 4.7× the untouched-fraction model predicts.

To decay a 16 384-entry cache to a 500-entry working set once the gates open:
about 700 k gets, or 4 days at 2 gets/sec (section 4.4).

### 2.4 Not the post-grow cooldown

`maybeResizeLocked` sets `cooldownGets = adaptiveWindowLen(newCap, ...)` after
every grow, and it is tempting to read that as suppressing every shrink
evaluation during the climb. It does not: growth needs `capacity` forced
evictions, which needs the doubled cache to refill first, and the refill takes
long enough that windows complete with the cooldown expired. Counted during
scenario B's climb from 100 to 16 384: **28 shrink evaluations**, every one
rejected by the gates. The cooldown changes nothing about the outcome.

### 2.5 Not the eviction gate's strictness

Worth stating, because it is the intuitive suspect.
`atTargetEvictionGateLimit` is *milder* at higher targets for realistic cache
sizes: the window scales with `n`, and a larger window tightens the band. At
n = 8192:

| target | window m | gate | hit rate needed | margin over target |
|---|---|---|---|---|
| 0.85 | 15 541 | 2 219 | 0.8572 | +0.0072 |
| 0.95 | 24 540 | 1 141 | 0.9535 | +0.0035 |
| 0.99 | 37 724 | 328 | 0.9913 | **+0.0013** |

The statistical gate asks for *less* headroom at 0.99 than at 0.85. It is not
what makes a high target dangerous. It looks like the problem if evaluated at a
fixed small window such as m = 1000, which is how we first mis-read it.

It is, however, the binding gate on a full cache at every target — the margin
column is positive in every row — which is why section 1's table is computed
against it.

---

## 3. What this is **not**

Four plausible diagnoses the measurements rule out. Each would send an
investigation the wrong way.

**Not caused by a large `max-size`.** With a reachable target, capacity tracks
the working set however high the ceiling is. Scenario A below runs with
`max-size` at 32× the working set and never exceeds 984. (`max-size` does set
the *cost* of the pin when it happens: the multiplier is `max-size` over the
saturation size.)

**Not caused by capacity exceeding the working set.** That is the case the slack
branch handles, and handles well: capacity forced to 8000 against a working set
of 500 returned to 478 within 200 k gets. Capacity above occupancy is when shrink
works *best* — the cache is not evicting, so the eviction gate passes trivially.

**Not caused by a high target as such.** 0.99 against a workload that can deliver
it is fine: scenario C holds 1.04× the memory of a 0.95 target.

**Not permanent.** The pin lasts while ε > 1 − target. When the novel rate drops
the gates open and the existing shrink policy reclaims the memory (section 4.4).
A workload whose ad-hoc queries stop overnight releases the memory overnight and
regrows in the morning.

The failure needs `target > achievable`, sustained. While it holds, the cost is
whatever `max-size` allows.

---

## 4. Measurements

From tests in `tsdb/index/tsi1/cache_test.go`, driving the real cache from its
configured floor — never forcing capacity, since a real shard starts at the floor
after every restart. Deterministic, well under a second each.

### 4.1 `TestAdaptiveTarget_OverProvisionedMaxIsSafe`

floor 100, `max-size` 16384 (32× the working set), working set 500, 800 k gets.

| | target | workload | final capacity | occupancy | memory | hit rate |
|---|---|---|---|---|---|---|
| **A** | 0.95 | closed set of 500 | 479 | 479 | 94 kB | 0.9582 |
| **B** | 0.99 | 500 + 5 % novel (ceiling ≈ 0.95) | **16 384 (max)** | 15 614 | **3.04 MB** | 0.9486 |
| **C** | 0.99 | closed set of 500 | 984 | 500 | 98 kB | 0.9879 |

B's trajectory, sampled every 100 k gets — monotonic, with occupancy following
capacity because there is always a fresh predicate to fill the next slot:

| gets | 100 k | 200 k | 300 k | 400 k | 500 k | 600 k | 700 k | 800 k |
|---|---|---|---|---|---|---|---|---|
| capacity | 3 200 | 6 400 | 6 400 | 12 800 | 12 800 | 12 800 | 12 800 | 16 384 |
| occupancy | 3 014 | 4 814 | 6 400 | 8 414 | 12 800 | 12 800 | 12 800 | 15 614 |
| memory | 596 kB | 953 kB | 1.24 MB | 1.64 MB | 2.49 MB | 2.49 MB | 2.49 MB | 3.04 MB |
| hit rate | 0.9389 | 0.9444 | 0.9463 | 0.9472 | 0.9478 | 0.9481 | 0.9484 | 0.9486 |

A and B differ in workload as well as target, so B/A (33×) overstates what the
target alone costs. The like-for-like comparison is 4.5.

### 4.2 `TestHitRate_SaturatesWellBeforeMaxSize`

Hit rate against *fixed* capacities, same 500-predicate pool with 5 % novel:

| capacity | hit rate | memory |
|---|---|---|
| 500 | 0.8404 | 98 kB |
| 600 | 0.9337 | 118 kB |
| **700** | **0.9486** | **138 kB** |
| 1 000 | 0.9490 | 199 kB |
| 2 000 | 0.9490 | 401 kB |
| 8 192 | 0.9490 | 1.61 MB |

Ceiling 0.9500. **Up to saturation: 1.41× the memory for +0.108 hit rate. Past
it: 11.9× the memory for +0.00044.**

This is the part that matters for the reasonable objection that "we set
`max-size` inside the memory budget, so spending it on hit rate is the point."
That holds in general, and it is the documented contract: `tsdb.Config` says the
cache "grows adaptively until either the target rate is met or the max size is
reached". Past saturation the memory is not buying hit rate: the entries beyond
~700 hold predicates that will never be queried again, because the LRU tail is
churning novel arrivals.

### 4.3 Steady state with a reachable target (`TestAdaptiveTarget_BoundedWorkingSet`)

Same code path, working set 2000, targets 0.85 and 0.95, sampled after the policy
settles:

| | target 0.85 | target 0.95 |
|---|---|---|
| entries resident | 1 759 | 1 905 |
| memory held | 346 kB | 375 kB (**1.08×**) |
| forced evictions | 53 331 | 19 830 (2.7× fewer) |
| reclaim pace after contraction | 4.81 gets/entry | 6.60 gets/entry (1.37× slower) |

With a reachable target the choice costs **8 % more memory for 2.7× less eviction
churn**, and reclaim is modestly slower. The large differences appear only when
the target is unreachable.

One measurement caveat that cost us time: **capacity oscillates** — grow-double,
then slack-trim — so a point sample of it reports where in that cycle the run
stopped rather than a steady state. Comparing capacities across configurations
gives nonsense: at one sampling point 0.85 read 1706 and 0.95 read 3788, and
300 k gets later the ordering had reversed. Occupancy and `bytes` are stable;
compare those.

### 4.4 `TestAdaptiveTarget_PinnedCacheRecoversWhenNovelRateDrops`

Scenario B, pinned at 16 384 after 800 k gets, then the novel rate changed and
the run continued. Capacity sampled every 100 k gets:

| novel rate after pin | +200 k | +400 k | +600 k | +700 k | +800 k | +1 M |
|---|---|---|---|---|---|---|
| 0 % | 14 590 | 11 518 | 6 398 | **494** | 496 | 496 |
| 0.5 % | 15 094 | 12 022 | 6 902 | 2 806 | **520** | 511 |
| 1 % (ceiling = target) | 15 360 | 15 360 | 15 360 | 15 360 | 15 360 | 15 360 |

Each event sheds the 1 024-entry cap once per window (~75 k gets at n = 16 384,
target 0.99). At 2 gets/sec the full recovery is about 4 days. The 1 % row is
the knife edge from section 1: the hit-rate gate passes at exactly 0.99, the
eviction gate does not.

### 4.5 `TestAdaptiveTarget_SameWorkloadDifferentTargets`

500-predicate pool with 5 % novel (ceiling 0.95), floor 100, max 16 384, 800 k
gets:

| target | final capacity | occupancy | memory | hit rate |
|---|---|---|---|---|
| 0.85 | 516 | 516 | 104 kB | 0.8661 |
| 0.90 | 556 | 556 | 112 kB | 0.9038 |
| 0.94 | 748 | 748 | 151 kB | 0.9485 |
| 0.95 | 1 600 | 1 600 | 325 kB | 0.9486 |
| 0.99 | 16 384 | 15 614 | 3.18 MB | 0.9486 |

**0.99 holds 21× the memory of 0.94 for the same hit rate.** The 0.95 row is a
knife edge: the test's novel schedule is deterministic (exactly one in twenty),
so the windowed rate lands on 0.95 and growth stalls at 1 600. A real, noisy ε
would push it to the ceiling as well, which is why section 1 gives 0.946 as the
highest safe target at ε = 5 %.

---

## 5. Field diagnosis

The signature is **capacity at `max-size` while the windowed hit rate is below
target**. The rate a pinned cache has settled at *is* the workload's achievable
ceiling, so set the target just under it (the gate needs a margin of roughly
0.002 to 0.004; use section 1's table). If the pin comes and goes with the
workload, it may be acceptable as is: the memory is bounded by `max-size` and is
returned when the ad-hoc traffic subsides.

Two gaps we hit while building this:

- `tsdb.Config.Diagnostics()` published `series-id-set-cache-size` but not
  `-max-size`, `-target-hit-rate` or `-shrink-conservatism`, so `/debug/vars`
  could not say whether adaptive sizing was even enabled, let alone what it was
  aiming at — leaving the `tsi1_cache` `capacity` statistic sitting right beside
  it uninterpretable. Now added.
- `tsi1_cache` had no bytes gauge, so memory cost could only be inferred. Now
  added as `bytes`, maintained incrementally: exact at insertion and eviction,
  re-accounted when a set grows or shrinks in place. Cost on the series-creation
  path is 62 ns for a one-container set and 354 ns for a 256-container set, zero
  allocations; the point-write path is untouched. It counts each set at its
  serialized size, so it is a floor on heap rather than the heap: 1.02× under
  for dense sets, 3× to 5× under for the empty and one-id sets that dominate a
  cache like scenario B's, and up to 11× under for sets spread one member per
  roaring container. The memory figures in this document are gauge readings.

One diagnostic trap: `hit/(hit+miss)` from `/debug/vars` is a lifetime ratio. The
policy acts on a windowed rate, and the two diverge for a long time after a
workload change. Compute the rate from deltas.

`cmd/tsi_cache_sizer -explain` reports the condition and the observed ceiling.

---

## 6. Heterogeneous fleets

The question this raises for anyone running many instances: can one
`target-hit-rate` be rolled out across instances with different cardinalities
and query patterns? Not at the high end. A single moderate target with a
per-instance `max-size` is defensible; a single aggressive target is not.

### 6.1 The target cannot be uniform at the high end

The failure condition is `target > 1 − ε`, and ε is a per-instance property:
the fraction of that instance's gets that are for a predicate it has never held.
Instances with different query patterns have different ε. Section 1's table
says the rest: a fleet-wide 0.99 is safe on an instance with ε at 0.1 % and
pinned on one at 2 %. There is no high target that is safe everywhere without
knowing the highest ε in the fleet.

### 6.2 The two failure directions are asymmetric

- **Target too high for an instance.** Capacity climbs to `max-size` and stays
  there while ε remains above the gap. The cost is bounded by `max-size` and
  clears on its own when the ad-hoc traffic subsides (section 4.4).
- **Target too low for an instance.** Growth stops as soon as the windowed rate
  reaches the target, so the cache settles below saturation. The cost is hit
  rate: section 4.3 measured 2.7× the eviction churn at 0.85 versus 0.95 on the
  same working set, for 8 % less memory. That is query latency on the misses,
  not memory.

So a uniform target pitched at the fleet's highest-ε instance costs the
low-ε instances some hit rate and nothing else. A value around 0.90 is a
reasonable default for a fleet that has not been measured. Leave a margin of at
least 0.005 below the worst instance's ceiling; the eviction gate's margin
(section 2.5) makes a target within ~0.003 of the ceiling a knife edge where
growth stalls but shrink cannot fire (the 1 % row of section 4.4).

### 6.3 Cardinality sets the price, not the condition

ε and the working set are properties of the query pattern. Cardinality sets the
bytes per cached entry, because each entry is a series-ID bitmap. On a
high-cardinality instance a pinned slot costs far more than the ~200 bytes of
the test data, and a miss costs more too, since the fallback merges large
bitmaps across partitions. Two consequences:

- `max-size` is the only hard bound on memory, and it should be set per
  instance or per instance class from the memory budget and the observed
  `bytes` per entry, not copied fleet-wide as an entry count.
- The instances where a per-instance target is worth the effort are the
  high-cardinality ones, where the miss cost that a higher target buys down is
  largest.

### 6.4 Recommended practice

1. Set `max-size` per instance class from the memory budget and the measured
   bytes per entry. This is the cost bound; nothing else is.
2. Use one conservative target fleet-wide as the default.
3. Observe ε where it matters. Once a cache is above its working set, the miss
   rate in `tsi1_cache` is ε; compute it from deltas, not the lifetime ratio.
   Raise the target per instance only where the observed ceiling justifies it.
4. Alert on the signature: capacity at `max-size` with the windowed rate below
   target. That instance's target is above its ceiling. If the condition comes
   and goes with the workload it may be acceptable; if it is sustained, lower
   the target for that instance.

Heterogeneity is the strongest argument for the last option in section 7,
deriving the effective target from the observed ceiling: it is the only option
that adapts per instance without an operator measuring each one.

---

## 7. Options, for the team to weigh

None of these are implemented or tested. They are the shapes the analysis
suggests, with the objections we could see.

**Guidance only.** Document that the target must sit below the achievable hit
rate, that the achievable rate is a workload property readable from the cache's
own hit/miss counters, and that a sustained pin is bounded by `max-size` and
self-clears when the novel traffic subsides. Cheapest, and given section 4.4 it
may be enough.

**Reorder the shrink gates ahead of the slack branch.** Dropping capacity not
backed by entries evicts nothing and cannot lower the hit rate, so gating it on
either gate looks wrong on its face. But it addresses only the slack case, which
is transient under a novel trickle and costs no memory, and does nothing for the
pinned case, where the cache is full. Not worth doing for this problem.

**Add a marginal-gain term to the growth rule.** Record the hit rate before a
doubling and refuse the next one if the previous did not improve it by some
margin. This addresses the cause directly — it gives growth a second stopping
signal that does not depend on the target being reachable — and would have capped
scenario B near 700 entries instead of 16 384. Objections: it needs a per-step
memory of the previous rate and a threshold that is itself a tuning parameter,
and it can stop growth early on a workload whose gains diminish smoothly
(Zipfian access) rather than saturating.

**Derive the target rather than configure it.** The server could observe its own
ceiling once pinned and clamp the effective target below it. Self-correcting, and
the ceiling is already measurable from the stats; but the running config would
no longer match the file, which is its own problem.

The marginal-gain term is the only option that fixes the cause. Whether the
cause needs fixing depends on how the team weighs an episodic, `max-size`-bounded
overshoot that clears on its own against a new tuning parameter in the growth
rule. The team knows the policy's history better than we do.

---

## 8. Reproducing

```bash
go test -run 'TestAdaptiveTarget_|TestHitRate_SaturatesWellBeforeMaxSize' \
  -v ./tsdb/index/tsi1/
```

All print full trajectories.
`scripts/tsi_cache_experiment/run.sh` builds a throwaway instance for measuring
per-entry cost against a real server; it does not exercise adaptive sizing.
