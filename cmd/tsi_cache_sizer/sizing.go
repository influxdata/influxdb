package main

import (
	"fmt"
	"math"
	"sort"
)

// Sizing model for the adaptive TSI series-id-set cache.
//
// Everything in this file is a pure function of numbers gathered elsewhere, so
// the policy can be exercised by a table-driven test with integer literals
// rather than against a live server. The collection side (collect.go) decides
// where the numbers come from; this file decides what they mean.
//
// See TSI_ADAPTIVE_CACHE_SIZING_METHOD.md at the repository root for the
// derivation of each formula and for why the safe direction of each
// approximation is the one chosen here.

const (
	// DefaultCacheFloor mirrors tsdb.DefaultSeriesIDSetCacheSize, the default
	// (and minimum) per-shard capacity. It is duplicated rather than imported
	// because importing tsdb pulls the whole storage engine — and with it the
	// cgo/libflux dependency reachable through the tsdb subtree — into this
	// tool's build, which would stop an operator from cross-compiling a static
	// binary to drop onto a host. Keep in sync with tsdb/config.go.
	DefaultCacheFloor = 100

	// bytesPerSeriesID is roaring's serialized array-container cost per value,
	// and the rate the conservation identity is stated in: a container covers a
	// 65 536-wide block of the id space and stores its members as a sorted
	// uint16 array (2 bytes each) or, once dense, as a flat 8 KiB bitmap, so no
	// container exceeds 2 bytes per value on disk. It is what SeriesIDSet.Bytes()
	// — and therefore the cache's bytes gauge — counts. It is not what the heap
	// pays; see heapPayloadNum.
	bytesPerSeriesID = 2

	// heapPayloadNum/heapPayloadDen is the in-heap cost per array-container
	// member: 2.5 bytes. Array containers are Go slices, and a slice's backing
	// array is rounded up to a size class (a 4 098-byte array lands in the
	// 4 864-byte class) and, for sets that grew in place, carries doubling
	// slack. Measured against live heap across container fills from 1 to 4 096
	// members the ratio to the serialized 2 bytes never exceeded 1.19x on the
	// backing array alone; 1.25x covers that with margin.
	heapPayloadNum = 5
	heapPayloadDen = 2

	// roaringContainerSpan is the width of the id range one container covers.
	roaringContainerSpan = 65536

	// roaringBitmapContainerBytes is the flat cost of a dense container and the
	// ceiling on any single container's payload. An array container near its
	// 4 096-member limit costs the same once its slice is rounded up, which is
	// why the bound takes min(payload, 8192) per container rather than trusting
	// the array rate all the way to the crossover.
	roaringBitmapContainerBytes = 8192

	// roaringContainerHeapBytes is the fixed heap cost of one container beyond
	// its payload: the container struct (24 or 32 bytes), the interface value
	// that holds it (16), its key and copy-on-write flag in the parent's
	// slices, and allocator rounding on the struct. Measured at 47 to 76 bytes
	// per container on sets with 1 to 33 members per container; 96 leaves
	// margin. This is the term the serialized figure omits almost entirely
	// (it charges 2 bytes per container), and it is what makes a set with one
	// member per container cost ~60 bytes per series in heap against 2 in the
	// gauge — 11x on measured sets.
	roaringContainerHeapBytes = 96

	// seriesIDSetHeapBytes is the heap cost of an empty SeriesIDSet: the wrapper
	// (24-byte RWMutex and 8-byte pointer, in a 32-byte allocation) plus the
	// roaring.Bitmap struct with its three empty slices. Measured 134 bytes;
	// the gauge counts 40.
	seriesIDSetHeapBytes = 136

	// cacheElementHeapBytes is seriesIDCacheEntryOverhead on the server: the
	// seriesIDCacheElement (64), its list.Element in the evictor (40), and its
	// slot in the innermost map (48). Exact.
	cacheElementHeapBytes = 152

	// stringAllowanceBytes covers the interned measurement/key/value strings,
	// which vary with the schema's naming. The only soft term; deliberately
	// generous, because setting it too low makes the result stop being an
	// upper bound.
	stringAllowanceBytes = 64

	// entryOverheadBytes covers everything in an entry other than its roaring
	// containers: 352 bytes. Measured against live heap, an entry holding an
	// empty set costs 286 bytes plus its strings; the cache's own gauge reports
	// 192 plus strings for the same entry, because it counts the set at its
	// serialized size. It also acts as a floor, so a predicate matching no
	// series — including one for a tag value that does not exist, which the
	// index caches all the same — is not costed at zero.
	entryOverheadBytes = cacheElementHeapBytes + seriesIDSetHeapBytes + stringAllowanceBytes
)

// heapPayload is the in-heap cost of k members held in array containers,
// saturating instead of overflowing.
func heapPayload(k int64) int64 {
	if k <= 0 {
		return 0
	}
	if k > math.MaxInt64/heapPayloadNum {
		return math.MaxInt64
	}
	return k * heapPayloadNum / heapPayloadDen
}

// containersFor bounds the number of roaring containers a set of k members
// can occupy: one per member, and — when the id space is known to span at
// most idSpan ids — one per 65 536-id block. idSpan must be an upper bound;
// understating it understates the container count, which is the unsafe
// direction, so idSpan <= 0 ("unknown") yields the fully dispersed k.
func containersFor(k, idSpan int64) int64 {
	if k <= 0 {
		return 0
	}
	n := k
	if idSpan > 0 {
		if bySpan := (idSpan + roaringContainerSpan - 1) / roaringContainerSpan; bySpan < n {
			n = bySpan
		}
	}
	return max(n, 1)
}

// SpanFromCardinality converts a database's current series cardinality into
// an upper bound on its series id space: the number of series ever created,
// which is current cardinality inflated by churn (ids are never reused in
// 1.x). factor is that inflation. A factor <= 0 disables the bound and
// returns 0, which every consumer reads as "fully dispersed".
func SpanFromCardinality(cardinality int64, factor float64) int64 {
	if cardinality <= 0 || factor <= 0 {
		return 0
	}
	span := float64(cardinality) * factor
	if span >= float64(math.MaxInt64) {
		return math.MaxInt64
	}
	return int64(span)
}

// Verdict is the recommendation for one instance. The zero value is not
// meaningful; Classify always returns one of the constants below.
type Verdict string

const (
	// VerdictNotTSI means the instance has no tsi1 shards, so it has no TSI
	// series-id-set cache to size. Instances on the inmem index land here.
	VerdictNotTSI Verdict = "NOT_TSI"

	// VerdictNoHeadroom means the instance is already at or above the healthy
	// line, so it has no burst reserve to lend the cache.
	VerdictNoHeadroom Verdict = "NO_HEADROOM"

	// VerdictNoBenefit means the cache is already serving at or above the
	// benefit threshold with negligible eviction at the current fixed size:
	// the working set fits, and adaptive sizing would add risk without adding
	// hit rate.
	VerdictNoBenefit Verdict = "NO_BENEFIT"

	// VerdictTooTight means a safe max-size exists but is too close to the
	// floor to be worth a restart; the configuration this instance belongs to
	// should be split, or this instance excluded from it.
	VerdictTooTight Verdict = "TOO_TIGHT"

	// VerdictUnknown means required inputs were missing (typically the heap
	// p95, when _internal was unavailable), so no recommendation was computed.
	VerdictUnknown Verdict = "UNKNOWN"

	// VerdictOK means the instance is a candidate at the computed max-size.
	VerdictOK Verdict = "OK"
)

// Thresholds collects the tunable policy constants so they can be overridden
// from flags and pinned in tests. See Defaults for the recommended values and
// the reasoning behind each.
type Thresholds struct {
	// HealthyLine is the fraction of RAM below which an instance is considered
	// stable, with the remainder reserved for compaction, backfill and query
	// bursts.
	HealthyLine float64

	// BudgetFrac is the hard ceiling on the cache's share of RAM, regardless of
	// how much headroom an instance has.
	BudgetFrac float64

	// HeadroomShare is the fraction of the remaining distance to HealthyLine
	// that the cache may consume. Below 1.0 this leaves the burst reserve
	// mostly intact and makes the budget taper to zero as an instance
	// approaches the healthy line.
	HeadroomShare float64

	// BenefitHitRate is the windowed hit rate at or above which the cache is
	// considered to already fit its working set.
	BenefitHitRate float64

	// BenefitEvictionRate is the evictions-per-get rate below which eviction is
	// considered negligible. Both this and BenefitHitRate must be satisfied for
	// VerdictNoBenefit: a cache can show a high hit rate while still thrashing
	// on a hot tail.
	BenefitEvictionRate float64

	// MinUsefulMultiple is how many times the floor a max-size must reach
	// before it justifies a restart.
	MinUsefulMultiple int64

	// FirstPassCapMultiple bounds the first rollout regardless of budget. The
	// realistic win is working sets of a few hundred to a few thousand
	// predicates; the tail beyond that carries the OOM risk without a matching
	// return.
	FirstPassCapMultiple int64

	// Floor is the configured series-id-set-cache-size, the per-shard minimum
	// capacity and the base of the doubling ladder.
	Floor int64
}

// Defaults returns the recommended thresholds. The rationale for each value is
// in Parts 4, 5 and 7 of TSI_ADAPTIVE_CACHE_SIZING_METHOD.md.
func Defaults() Thresholds {
	return Thresholds{
		HealthyLine:          0.50,
		BudgetFrac:           0.05,
		HeadroomShare:        0.50,
		BenefitHitRate:       0.95,
		BenefitEvictionRate:  0.001,
		MinUsefulMultiple:    4,
		FirstPassCapMultiple: 16,
		Floor:                DefaultCacheFloor,
	}
}

// MemoryBudget returns the bytes of additional heap the TSI cache may consume
// on an instance with ramBytes of RAM whose p95 heap is heapP95Bytes.
//
//	G = min( BudgetFrac * RAM , HeadroomShare * (HealthyLine * RAM - heap_p95) )
//
// The first term is an absolute ceiling. The second makes the budget taper to
// zero as an instance approaches the healthy line, so an instance already
// sitting at that line is allotted nothing and is excluded by construction
// rather than by a separate rule. Returns 0 when the instance is at or over the
// line, or when either input is non-positive.
func MemoryBudget(ramBytes, heapP95Bytes int64, t Thresholds) int64 {
	if ramBytes <= 0 || heapP95Bytes < 0 {
		return 0
	}
	ram := float64(ramBytes)
	headroom := ram*t.HealthyLine - float64(heapP95Bytes)
	if headroom <= 0 {
		return 0
	}
	g := math.Min(ram*t.BudgetFrac, headroom*t.HeadroomShare)
	if g <= 0 {
		return 0
	}
	return int64(g)
}

// EntryBytes bounds the heap cost of one cache entry whose series id set
// matches k series drawn from an id space of idSpan ids.
//
//	cost <= A + 96*n + min(2.5k, 8192*n)
//	  where n = min(k, ceil(idSpan/65536)) is the container count
//
// Three terms. A is the entry's fixed structure (entryOverheadBytes). 96*n is
// the fixed heap cost of n containers, which the serialized figure the cache's
// gauge reports omits almost entirely, and which dominates for small sets
// spread across the id space: one member per container costs ~60 bytes per
// series in heap against 2 in the gauge. The last term is the payload, at the
// lesser of two costs per container — 2.5 bytes per member while sparse
// (heapPayloadNum), or a flat 8 KiB once dense or nearly so.
//
// **idSpan must be an upper bound on the id space**, not an estimate. Both
// container terms are increasing in n, so understating idSpan understates the
// cost, which is the unsafe direction. Series ids are never reused in 1.x, so
// the span is the number of series ever created in the database — at or above
// current cardinality, and further above it the more series have been dropped.
// Pass idSpan <= 0 when no trustworthy bound is available; n then falls back
// to k, the fully dispersed case, which is safe and can be very loose for
// large sets.
//
// What this does not capture: whether a given tag value's series are clustered
// or smeared depends on the order they were created, which no schema query
// reveals. Measured against live heap the bound is within 1.5x of the smeared
// layouts and up to ~8x above a clustered one. Probing an actual entry reads
// the gauge, not the heap; see Client.ProbeEntryBytes and the gauge-to-heap
// factor applied to it.
func EntryBytes(k, idSpan int64) int64 {
	if k <= 0 {
		return entryOverheadBytes
	}
	containers := containersFor(k, idSpan)

	// Guard every product before combining them: k is a cardinality estimate
	// scaled by a ratio, so an absurd input must saturate rather than wrap into
	// a negative (which would read as a tiny, unsafe entry cost).
	bitmapCost := int64(math.MaxInt64)
	if containers <= math.MaxInt64/roaringBitmapContainerBytes {
		bitmapCost = containers * roaringBitmapContainerBytes
	}
	payload := min(heapPayload(k), bitmapCost)

	if containers > (math.MaxInt64-entryOverheadBytes)/roaringContainerHeapBytes {
		return math.MaxInt64
	}
	fixed := int64(entryOverheadBytes) + containers*roaringContainerHeapBytes
	if payload > math.MaxInt64-fixed {
		return math.MaxInt64
	}
	return fixed + payload
}

// --- Conservation of cardinality --------------------------------------------
//
// Every series in a measurement carries exactly one value of each tag key, so a
// tag key's values *partition* the measurement's series:
//
//	sum over values v of |series(key, v)|  ==  S_measurement
//
// Caching every value of one tag key therefore costs at most 2*S bytes of
// serialized roaring payload, whatever that key's cardinality. A wide key
// yields many tiny entries; a narrow key yields few enormous ones; the payload
// total is the same. Measured across keys of 4, 16, 50 and 2000 values on a
// 1M-series shard, against the cache's gauge: 0.07 to 2.48 bytes per series,
// never above.
//
// That identity is true and it is the reason a cap expressed in entries is
// tractable at all. It is not, on its own, a bound on heap. The heap pays for
// three things the identity does not count, and the models below add each:
//
//   - the payload rate is 2.5 bytes per member in heap, not 2, because array
//     containers are Go slices rounded up to a size class (heapPayloadNum);
//   - every roaring container costs ~96 bytes of structure before its payload
//     (roaringContainerHeapBytes), so a key's values cost 96 * (containers
//     across all of them) on top of 2.5*S — bounded by 96 * min(S, V*C), where
//     C is the number of 65 536-id blocks the database's id space spans. When
//     every series sits in its own container that term is 38x the payload;
//   - an entry's fixed cost is 352 bytes (entryOverheadBytes), not the 192 the
//     gauge reports, because an empty SeriesIDSet is 134 bytes of heap.
//
// The measurements that validated the earlier 2-bytes-per-series form
// ("1.00x against strided", "2.0x over the measured fill") were made against
// the gauge, which reports serialized size; against live heap the same rows
// read 1.9x for a 16-container set of 500 and 11x for a set with one member
// per container. The constants above were calibrated against live heap.
//
// "Capacity times the largest entry" remains structurally impossible: the
// worst entries come from the narrowest keys, and a key with V values offers
// only V of them. To fill a large cap you must draw from progressively wider
// keys, whose entries are progressively cheaper. What the identity does not do
// is bound the cache across *different* tag keys — T keys each with 2 values
// give 2T entries of S/2 series each — nor bound the number of entries: a
// query for a tag value that does not exist still creates an entry, so the
// per-entry overhead runs to the cap whatever the schema holds.

// KeyProfile is one (measurement, tag key) in one shard: how many distinct
// values the key has, how many of the shard's series belong to that
// measurement, and an upper bound on the database's series id span, which
// decides how many roaring containers one entry's set can occupy. IDSpan 0
// means unknown and is costed as fully dispersed (one container per series):
// safe, and loose for large sets, so profiles collected before the field
// existed should be re-collected.
type KeyProfile struct {
	Measurement   string
	Key           string
	Values        int64
	SeriesInShard int64
	IDSpan        int64 `json:",omitempty"`
}

// EntryBytesForKey is the cost of one entry of this key: the measurement's
// series divided evenly across the key's values, costed by EntryBytes.
func (p KeyProfile) EntryBytesForKey() int64 {
	if p.Values <= 0 {
		return entryOverheadBytes
	}
	return EntryBytes(p.SeriesInShard/p.Values, p.IDSpan)
}

// ShardProfile is every tag key visible in one shard.
type ShardProfile struct {
	ShardID string
	Keys    []KeyProfile
}

// CacheBreakdown decomposes a shard's bound into the terms that behave
// differently, plus the quantities that decide which one dominates.
type CacheBreakdown struct {
	// Payload is the roaring member data: conserved per tag key, so it stops
	// growing once every key has been drawn on.
	Payload int64

	// Containers is the fixed heap cost of the roaring containers behind the
	// entries placed. Bounded per key by 96 * min(S, V*C); like the payload it
	// stops growing once the schema is exhausted.
	Containers int64

	// Overhead is A per entry, strictly linear in capacity: it keeps growing
	// past the schema, because an entry exists for any value queried, whether
	// or not the value exists.
	Overhead int64

	Total int64

	// KeysDrawn is j: how many tag keys the fill draws on. Fractional for the
	// last, partly-taken key. This is the term that cannot be derived from
	// (series, tag keys) alone — it depends on the sorted tag-value
	// cardinalities, and two schemas identical in every other respect can differ
	// several-fold in j at the same capacity.
	KeysDrawn float64

	// SchemaEntries is how many of the entries are backed by a distinct tag
	// value the schema holds: min(capacity, total distinct values). Entries is
	// the capacity itself. The difference is entries the schema cannot supply
	// but a query pattern can — values that do not exist — and they cost
	// overhead only.
	SchemaEntries int64
	Entries       int64
}

// ShardCacheBound returns the largest total the shard's cache can reach at the
// given capacity.
func ShardCacheBound(capacity int64, keys []KeyProfile) int64 {
	return ShardCacheBreakdown(capacity, keys).Total
}

// ShardCacheBreakdown computes the adversarial fill — the most expensive entries
// first — and reports its decomposition.
//
// Entries within a key are taken as uniform (S/V series each), so this is a
// fractional knapsack with a uniform value per item inside each group and
// greedy by cost-per-entry is exactly optimal for that model. It is the worst
// case under uniform tag values; see ConservationModel for what skew does.
// Capacity beyond the schema's distinct values is charged at the per-entry
// overhead, since the cache will hold an entry for any value a query names.
func ShardCacheBreakdown(capacity int64, keys []KeyProfile) CacheBreakdown {
	var out CacheBreakdown
	if capacity <= 0 {
		return out
	}

	ordered := make([]KeyProfile, len(keys))
	copy(ordered, keys)
	sort.SliceStable(ordered, func(i, j int) bool {
		return ordered[i].EntryBytesForKey() > ordered[j].EntryBytesForKey()
	})

	remaining := capacity
	for _, p := range ordered {
		if remaining <= 0 {
			break
		}
		if p.Values <= 0 {
			continue
		}
		take := min(p.Values, remaining)

		// One entry of this key holds k series in c containers. Its payload is
		// the lesser of the array rate and the bitmap ceiling per container;
		// taking every value of the key yields at most 2.5*SeriesInShard — the
		// conservation identity, at the heap rate.
		k := p.SeriesInShard / p.Values
		c := containersFor(k, p.IDSpan)
		payload := min(heapPayload(k), c*roaringBitmapContainerBytes)

		out.Payload += payload * take
		out.Containers += c * roaringContainerHeapBytes * take
		out.Overhead += take * entryOverheadBytes
		out.SchemaEntries += take
		out.KeysDrawn += float64(take) / float64(p.Values)
		remaining -= take
	}
	// Whatever the schema could not fill, a query pattern can: an entry per
	// value named, existing or not, at the fixed overhead each.
	out.Overhead += remaining * entryOverheadBytes
	out.Entries = capacity
	out.Total = out.Payload + out.Containers + out.Overhead
	return out
}

// SimpleShardBound bounds a shard's cache from four numbers: the capacity, the
// shard's series count, how many tag keys a measurement has, and an upper
// bound on the database's series id span.
//
//	bytes(n) <= 2.5*S*min(T, n)  +  96*min(S*min(T, n), n*C)  +  A*n
//	              ^payload            ^containers                 ^overhead
//
//	  where C = ceil(idSpan/65536), or unbounded when idSpan is unknown
//
// The three terms behave differently, and conflating them is what makes a
// naive "A times n" estimate wrong:
//
//   - **Overhead is per entry and universal.** A is the fixed structure of a
//     cache entry in heap: 152 bytes of element, evictor node and map slot, 136
//     for an empty SeriesIDSet, plus the interned measurement/key/value
//     strings. entryOverheadBytes carries it, and it is conserved across
//     instances. It is the only term that keeps growing past the schema.
//
//   - **Payload is per tag key and does not scale with n at all.** A key's values
//     partition the measurement, so every key contributes at most 2.5*S however
//     many of its values are cached. n can only decide how many *keys* get drawn
//     on, and that saturates at T.
//
//   - **Containers are per key too, and bounded two ways.** Each entry's set
//     occupies at most C containers, so n entries occupy at most n*C; and a
//     key's values between them hold S series, so at most S containers per
//     key. Without the id span only the second bound applies, and it charges
//     96 bytes per series per key — the fully dispersed case, safe and loose.
//
// So "A*n" alone is not a bound — measured against the reference fill it is
// violated at every capacity, by 47x at n=4, because it omits the payload
// entirely. Conversely, below the crossover the schema-bounded terms dominate
// and the cap is a weak lever; above it the overhead grows without limit.
//
// This needs only `seriesCreate`, one SHOW TAG KEYS and one SHOW SERIES
// CARDINALITY per database, so it is the cheap model: no per-key cardinality
// probe.
func SimpleShardBound(capacity, seriesInShard, tagKeys, idSpan int64) int64 {
	if capacity <= 0 || seriesInShard <= 0 || tagKeys <= 0 {
		return 0
	}

	// A cache holding n entries cannot have drawn on more than n tag keys.
	keys := min(tagKeys, capacity)

	payload := int64(math.MaxInt64)
	if p := heapPayload(seriesInShard); p < math.MaxInt64 && keys <= math.MaxInt64/p {
		payload = p * keys
	}

	// Containers: at most one per series drawn on, and at most C per entry.
	containers := int64(math.MaxInt64)
	if seriesInShard <= math.MaxInt64/keys {
		containers = seriesInShard * keys
	}
	if idSpan > 0 {
		c := containersFor(math.MaxInt64, idSpan) // blocks in the span
		if capacity <= math.MaxInt64/c {
			containers = min(containers, capacity*c)
		}
	}
	if containers > math.MaxInt64/roaringContainerHeapBytes {
		return math.MaxInt64
	}
	containers *= roaringContainerHeapBytes

	overhead := int64(math.MaxInt64)
	if capacity <= math.MaxInt64/entryOverheadBytes {
		overhead = capacity * entryOverheadBytes
	}
	if payload > math.MaxInt64-overhead || containers > math.MaxInt64-overhead-payload {
		return math.MaxInt64
	}
	return payload + containers + overhead
}

// SimpleShardProfile is the whole input the simple model needs for one shard.
// IDSpan is the same bound KeyProfile carries; 0 means unknown, costed as
// fully dispersed.
type SimpleShardProfile struct {
	ShardID string
	Series  int64
	TagKeys int64
	IDSpan  int64 `json:",omitempty"`
}

// SimpleModel bounds an instance with SimpleShardBound. Returns nil when there
// is nothing to profile; see ConservationModel for why absence must not read as
// zero cost.
func SimpleModel(shards []SimpleShardProfile) CostModel {
	if len(shards) == 0 {
		return nil
	}
	return func(capacity int64) int64 {
		var total int64
		for _, s := range shards {
			total += SimpleShardBound(capacity, s.Series, s.TagKeys, s.IDSpan)
		}
		return total
	}
}

// LacksIDSpan reports whether any profile was collected without the id span,
// so the caller can say the bound is costed as fully dispersed.
func LacksIDSpan(profiled []ShardProfile, simple []SimpleShardProfile) bool {
	for _, s := range profiled {
		for _, k := range s.Keys {
			if k.Values > 0 && k.SeriesInShard > 0 && k.IDSpan <= 0 {
				return true
			}
		}
	}
	for _, s := range simple {
		if s.Series > 0 && s.IDSpan <= 0 {
			return true
		}
	}
	return false
}

// CostModel maps a candidate per-shard capacity to the instance's worst-case
// cache bytes. It exists so the capacity search is independent of how the cost
// is derived.
type CostModel func(capacity int64) int64

// ConservationModel bounds an instance by summing each shard's adversarial fill.
// This is the preferred model: it needs only the schema and each shard's series
// count, both cheap and stable, and it does not depend on which predicates the
// workload actually queries.
//
// It is a worst case under one assumption: that a tag key's series divide
// evenly across its values. Conservation pins a key's total at 2*S only once
// every value of the key is taken; for a key taken in part, a single dominant
// value can cost nearly 2*S where this model charges 2*S/V. The bound that
// holds under any skew is SimpleShardBound, which is why the simple model reads
// higher. The gap is small when the cap exceeds the number of tag keys (every
// narrow key is then taken in full — about 3% on the reference production
// profile) and grows on a schema where a large measurement has a key with far
// more values than the cap.
//
// Returns nil when there is nothing to profile. A model that reported 0 for
// every capacity would read as "everything fits" and authorize an unbounded cap,
// so the absence of information must be nil rather than zero.
func ConservationModel(shards []ShardProfile) CostModel {
	return CombinedModel(shards, nil)
}

// CombinedModel bounds an instance with the conservation fill for every shard
// that has a schema profile and the simple bound for every shard that has only
// the cheap profile. A shard the schema probe could not cover must still cost
// something: summing only the profiled shards would let a failed probe on one
// database drop all of its shards from the bound with no trace, which is the
// same "absence reads as zero" failure ConservationModel refuses at the
// instance level. Returns nil when neither set has a shard.
func CombinedModel(profiled []ShardProfile, fallback []SimpleShardProfile) CostModel {
	covered := make(map[string]bool, len(profiled))
	for _, s := range profiled {
		covered[s.ShardID] = true
	}
	var uncovered []SimpleShardProfile
	for _, s := range fallback {
		if !covered[s.ShardID] {
			uncovered = append(uncovered, s)
		}
	}
	if len(profiled) == 0 && len(uncovered) == 0 {
		return nil
	}
	return func(capacity int64) int64 {
		var total int64
		for _, s := range profiled {
			total += ShardCacheBound(capacity, s.Keys)
		}
		for _, s := range uncovered {
			total += SimpleShardBound(capacity, s.Series, s.TagKeys, s.IDSpan)
		}
		return total
	}
}

// UncoveredShards returns the shards present in fallback but absent from
// profiled — the ones CombinedModel costs with the simple bound — and the series
// they hold, so the run can say how much of the instance the weaker bound
// covers.
func UncoveredShards(profiled []ShardProfile, fallback []SimpleShardProfile) (shards int, series int64) {
	covered := make(map[string]bool, len(profiled))
	for _, s := range profiled {
		covered[s.ShardID] = true
	}
	for _, s := range fallback {
		if !covered[s.ShardID] {
			shards++
			series += s.Series
		}
	}
	return shards, series
}

// PerEntryModel is the fallback when no schema profile is available: every entry
// on every shard costs bytesPerEntry. It is the bound conservation replaces, and
// it overestimates badly — 29.3x on the reference instance — because it assumes
// a cache can be filled entirely with worst-case entries.
// Returns nil when either input is missing, for the same reason as
// ConservationModel: no information must not look like no cost.
func PerEntryModel(shards, bytesPerEntry int64) CostModel {
	if shards <= 0 || bytesPerEntry <= 0 {
		return nil
	}
	return func(capacity int64) int64 {
		if capacity <= 0 {
			return 0
		}
		if capacity > math.MaxInt64/bytesPerEntry {
			return math.MaxInt64
		}
		per := capacity * bytesPerEntry
		if shards > math.MaxInt64/per {
			return math.MaxInt64
		}
		return per * shards
	}
}

// LargestCapacity returns the biggest capacity on the doubling ladder from floor
// whose modelled worst case still fits the budget, capped at ceiling. Returns 0
// when even the floor does not fit.
//
// The ladder is walked rather than binary-searched because the model is a step
// function of capacity (each tag key exhausts at its own cardinality), and the
// ladder values are the only ones the cache's doubling growth actually visits.
func LargestCapacity(budget, floor, ceiling int64, model CostModel) int64 {
	if budget <= 0 || floor <= 0 || model == nil {
		return 0
	}
	var best int64
	for c := floor; c <= ceiling; c *= 2 {
		if model(c) > budget {
			break
		}
		best = c
		if c > math.MaxInt64/2 {
			break
		}
	}
	return best
}

// CacheActivity summarizes a cache's observed behavior over a window, summed
// across every shard on one instance.
type CacheActivity struct {
	Hits      int64
	Misses    int64
	Evictions int64

	// Sampled is false when no window could be measured at all, in which case
	// the benefit filter must not be applied (absence of evidence is not
	// evidence that the cache is healthy).
	Sampled bool
}

// HitRate returns the windowed hit rate, or 0 when no gets were observed.
func (a CacheActivity) HitRate() float64 {
	gets := a.Hits + a.Misses
	if gets <= 0 {
		return 0
	}
	return float64(a.Hits) / float64(gets)
}

// EvictionRate returns evictions per get, or 0 when no gets were observed.
func (a CacheActivity) EvictionRate() float64 {
	gets := a.Hits + a.Misses
	if gets <= 0 {
		return 0
	}
	return float64(a.Evictions) / float64(gets)
}

// Recommendation is the per-instance result.
type Recommendation struct {
	Verdict Verdict

	// BudgetBytes is the heap the cache may consume, from MemoryBudget.
	BudgetBytes int64

	// RawMaxSize is the largest ladder value the budget permits, before the
	// first-pass cap. Reported so an operator can see how much of the
	// recommendation was policy and how much was arithmetic: when
	// CappedByFirstPass is set, this is the value a later pass could go to.
	RawMaxSize int64

	// MaxSize is the value to configure as series-id-set-cache-max-size. It is
	// 0 for every verdict other than VerdictOK.
	MaxSize int64

	// CappedByFirstPass records that the first-pass multiple, not the memory
	// budget, is what bounded MaxSize. Such an instance has room to go higher
	// in a later pass once the first one is proven.
	CappedByFirstPass bool

	// WorstCaseBytes is the modelled worst case at MaxSize, in heap bytes —
	// the number to compare against the budget. The `bytes` gauge reports
	// serialized set sizes and reads below this; see the README on the
	// gauge-to-heap factor.
	WorstCaseBytes int64 `json:"worst_case_bytes"`

	// Reason is a short human-readable explanation of the verdict.
	Reason string
}

// Classify applies the whole decision procedure to one instance.
//
// Order matters: the structural checks (is there a TSI cache at all, is there
// headroom) come before the benefit filter, which comes before the arithmetic.
// An instance that fails an earlier gate gets that verdict even if it would
// also fail a later one, because the earlier gate is the more actionable fact.
func Classify(tsiShards int64, ramBytes, heapP95Bytes int64, model CostModel, act CacheActivity, t Thresholds) Recommendation {
	if tsiShards <= 0 {
		return Recommendation{Verdict: VerdictNotTSI, Reason: "no tsi1 shards"}
	}
	if ramBytes <= 0 {
		return Recommendation{Verdict: VerdictUnknown, Reason: "instance RAM unknown"}
	}
	if heapP95Bytes <= 0 {
		return Recommendation{Verdict: VerdictUnknown, Reason: "heap p95 unavailable (no _internal history?)"}
	}

	budget := MemoryBudget(ramBytes, heapP95Bytes, t)
	if budget <= 0 {
		return Recommendation{
			Verdict: VerdictNoHeadroom,
			Reason:  "heap p95 at or above the healthy line",
		}
	}

	// The benefit filter is applied only against a window that was actually
	// measured. An unsampled instance falls through to the arithmetic and is
	// reported with its max-size, leaving the operator to confirm the benefit
	// separately rather than being told there is none.
	if act.Sampled && act.Hits+act.Misses > 0 &&
		act.HitRate() >= t.BenefitHitRate && act.EvictionRate() < t.BenefitEvictionRate {
		return Recommendation{
			Verdict:     VerdictNoBenefit,
			BudgetBytes: budget,
			Reason:      "working set already fits at the current size",
		}
	}

	if model == nil {
		return Recommendation{
			Verdict:     VerdictUnknown,
			BudgetBytes: budget,
			Reason:      "no cost model (schema probe skipped and no bytes-per-entry supplied)",
		}
	}

	// The unconstrained answer, then the first-pass cap. Kept separate so an
	// operator can see which of the two actually bound the result; an earlier
	// version overwrote raw with the cap, which hid the budget's answer exactly
	// when it mattered.
	raw := LargestCapacity(budget, t.Floor, math.MaxInt64/4, model)
	maxSize := raw
	capped := false
	if firstPass := t.Floor * t.FirstPassCapMultiple; maxSize > firstPass {
		maxSize = firstPass
		capped = true
	}

	if maxSize < t.Floor*t.MinUsefulMultiple {
		return Recommendation{
			Verdict:     VerdictTooTight,
			BudgetBytes: budget,
			RawMaxSize:  raw,
			Reason:      "safe cap is too close to the floor to justify a restart",
		}
	}

	// There is no "safe at any capacity" verdict. An earlier version reported
	// one when every entry the schema could produce fit the budget, but the
	// schema does not bound the entry count: a query for a tag value that does
	// not exist still creates an entry, at the fixed overhead each, up to the
	// cap. Only the cap bounds that.
	return Recommendation{
		Verdict:           VerdictOK,
		BudgetBytes:       budget,
		RawMaxSize:        raw,
		MaxSize:           maxSize,
		CappedByFirstPass: capped,
		WorstCaseBytes:    model(maxSize),
		Reason:            "candidate",
	}
}

const (
	// targetMargin is how far below a demonstrated hit rate a target is set.
	// Shrink requires the cache to beat target by roughly z*sqrt(T(1-T)/m),
	// which is 0.001 to 0.007 for realistic windows; 0.02 clears that with room
	// for the rate to drift.
	targetMargin = 0.02

	// maxRecommendedTarget is a defensive clamp, not a working limit. A hit rate
	// cannot exceed 1.0, so subtracting targetMargin already holds the
	// recommendation at or below 0.98; this only fires on out-of-range input,
	// such as corrupt counters or a hand-edited -load file.
	//
	// 0.98 is also about where the shrink side stops being worth more: the
	// observation window is n*ln(1/(1-T)) gets, 3.9n at 0.98 against 2.3n at
	// 0.90, and the eviction gate's margin over target narrows to a few
	// thousandths, so a target within that margin of the workload's ceiling
	// stalls growth without ever admitting a shrink. (What the window length
	// does not do is limit how much a shrink reclaims: each event sheds up to
	// half the cache or 1024 entries, whichever is smaller, whatever the target.)
	maxRecommendedTarget = 0.98
)

// RecommendTarget derives series-id-set-cache-target-hit-rate from what the
// instance has demonstrated, falling back to the supplied default.
//
// The target's job is to stop growth at saturation. Too low under-provisions —
// measured, a target of 0.90 settled at 558 entries and 0.906 when 700 entries
// would have served 0.9486. Too high removes the stopping signal and the cache
// climbs to max-size for as long as the workload's novel-predicate rate stays
// above 1-target; the memory is bounded by max-size and comes back when that
// rate drops, but while it lasts it buys no hit rate. Neither failure is
// symmetric with the other, and neither is knowable from a fixed number, so the
// evidence is used where it exists:
//
//   - pinned: the cache grew to max-size and still could not reach target, so
//     the rate it settled at *is* the workload's achievable ceiling. This is
//     authoritative and may push the target below the default.
//   - otherwise: the observed rate is a lower bound on the ceiling. LRU is a
//     stack algorithm, so a larger cache cannot serve a lower hit rate on the
//     same workload; anything already achieved at the current size remains
//     achievable at a larger one. Raising the target toward it is therefore
//     safe, while the default floors it when the evidence is weak.
//
// One limit on the second rule: the stack argument holds per shard, and the
// observed rate is the instance aggregate, dominated by its busiest shards. A
// target just under the aggregate can still sit above the ceiling of a cold
// shard, which then pins at max-size. That costs memory inside the modelled
// worst case, not safety, and the post-rollout pinned check reports it.
func RecommendTarget(observed float64, pinned bool, fallback float64) (float64, string) {
	roundDown := func(v float64) float64 { return math.Floor(v*100) / 100 }

	switch {
	case pinned && observed > 0:
		// Authoritative: this is the ceiling, and it may be low.
		t := roundDown(observed - targetMargin)
		if t > maxRecommendedTarget {
			t = maxRecommendedTarget
		}
		if t <= 0 {
			return fallback, "default (observed ceiling too low to target)"
		}
		return t, fmt.Sprintf("just below the %.3f ceiling observed on shards pinned at max-size", observed)

	case observed > fallback+targetMargin:
		t := roundDown(observed - targetMargin)
		if t > maxRecommendedTarget {
			t = maxRecommendedTarget
		}
		return t, fmt.Sprintf("%.3f already achieved at the current cache size, so it is achievable at a larger one", observed)

	default:
		return fallback, "default (no stronger evidence available)"
	}
}

// PinnedShards reports shards whose capacity has reached max-size while the
// cache is still serving below its target hit rate.
//
// That combination means the growth policy has no stopping signal. Its only
// brake is `hit rate < target`; it has no notion of marginal gain per entry. If
// the target sits above what the workload can achieve — because some fraction of
// gets are for predicates never seen before, which no cache size can serve —
// then the condition to stop growing is never satisfied, and capacity climbs to
// the ceiling whether or not growth is still buying hit rate.
//
// Measured, that matters: on a workload whose achievable rate was 0.95, hit rate
// saturated at capacity 700 and the remaining climb to 8192 cost 11.9x the bytes
// for +0.00044 hit rate. See TestHitRate_SaturatesWellBeforeMaxSize.
//
// The diagnosis is also the fix: when a cache is pinned like this, the hit rate
// it has settled at *is* the workload's achievable ceiling. Setting the target
// just below that observed rate restores the stopping signal. The pin is not
// permanent, though: it lasts while the novel-predicate rate stays above
// 1-target, and the shrink policy reclaims the memory once it drops
// (TestAdaptiveTarget_PinnedCacheRecoversWhenNovelRateDrops), so a pin that
// comes and goes with the workload and fits the budget may be acceptable as is.
//
// windowed, when supplied, gives each shard's hit/miss over a recent window and
// is preferred to the lifetime counters in snap. The policy acts on a windowed
// rate, and the lifetime ratio lags it for a long time after any change: a
// cache that has just climbed to the ceiling carries every miss of the climb in
// its lifetime figure, so the ceiling it reports reads low. Shards absent from
// windowed fall back to lifetime.
func PinnedShards(snap *VarsSnapshot, windowed map[string]CacheActivity) (pinned []string, observed float64) {
	if snap == nil || !snap.Cache.Adaptive() {
		return nil, 0
	}

	var hits, gets int64
	for id, c := range snap.Caches {
		if c.Capacity < snap.Cache.MaxSize {
			continue
		}
		h, total := c.Hit, c.Hit+c.Miss
		if w, ok := windowed[id]; ok && w.Hits+w.Misses > 0 {
			h, total = w.Hits, w.Hits+w.Misses
		}
		if total == 0 {
			continue
		}
		if float64(h)/float64(total) >= snap.Cache.TargetHitRate {
			continue // at the ceiling and meeting target: simply a big working set
		}
		pinned = append(pinned, id)
		hits += h
		gets += total
	}

	sort.Strings(pinned)
	if gets > 0 {
		observed = float64(hits) / float64(gets)
	}
	return pinned, observed
}

// ConfigAudit is the assessment of a configuration the instance is already
// running, as opposed to the one the tool would recommend. The sizing procedure
// works from scratch; this is the check that catches a cap set by hand, or by
// an earlier method, that the budget does not support.
type ConfigAudit struct {
	// MaxSize and TargetHitRate are the configured values under audit, and
	// Source says where they came from: the instance's config diagnostics, or
	// the -current-max-size/-current-target flags.
	MaxSize       int64   `json:"max_size"`
	TargetHitRate float64 `json:"target_hit_rate"`
	Source        string  `json:"source"`

	// WorstCaseBytes is the modelled worst case at MaxSize, the same bound the
	// recommendation is made against, so the two are directly comparable.
	WorstCaseBytes int64 `json:"worst_case_bytes"`
	BudgetBytes    int64 `json:"budget_bytes"`

	// OverBudget is WorstCaseBytes > BudgetBytes. BudgetMultiple is their ratio,
	// and SafeCapMultiple is MaxSize over the largest ladder value the budget
	// permits (0 when nothing fits), the number an operator has to divide by.
	OverBudget      bool    `json:"over_budget"`
	BudgetMultiple  float64 `json:"budget_multiple"`
	SafeCap         int64   `json:"safe_cap"`
	SafeCapMultiple float64 `json:"safe_cap_multiple"`

	// BelowTarget reports that the windowed aggregate hit rate is under the
	// configured target while the instance is not pinned at MaxSize: growth is
	// still in progress, and whether it stops short of MaxSize depends on the
	// workload's ceiling, which the aggregate cannot settle. ObservedHitRate is
	// that aggregate; 0 when no window was sampled.
	BelowTarget     bool    `json:"below_target"`
	ObservedHitRate float64 `json:"observed_hit_rate"`
}

// AuditConfig evaluates a running or supplied configuration against the same
// budget and cost model the recommendation uses. It returns false when there
// is nothing to audit: no adaptive configuration known, no cost model, or no
// budget. safeCap is the largest ladder value the budget permits.
func AuditConfig(cfg CacheConfig, source string, budget, safeCap int64, model CostModel, act CacheActivity) (ConfigAudit, bool) {
	if !cfg.Adaptive() || model == nil || budget <= 0 {
		return ConfigAudit{}, false
	}
	a := ConfigAudit{
		MaxSize:        cfg.MaxSize,
		TargetHitRate:  cfg.TargetHitRate,
		Source:         source,
		WorstCaseBytes: model(cfg.MaxSize),
		BudgetBytes:    budget,
	}
	a.OverBudget = a.WorstCaseBytes > budget
	a.BudgetMultiple = float64(a.WorstCaseBytes) / float64(budget)
	if safeCap > 0 {
		a.SafeCap = safeCap
		a.SafeCapMultiple = float64(cfg.MaxSize) / float64(safeCap)
	}
	if act.Sampled && act.Hits+act.Misses > 0 {
		a.ObservedHitRate = act.HitRate()
		a.BelowTarget = a.ObservedHitRate < cfg.TargetHitRate
	}
	return a, true
}

// Percentile returns the nearest-rank percentile of vals for p in [0,1].
// vals is sorted in place. Returns 0 for an empty slice.
func Percentile(vals []float64, p float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sort.Float64s(vals)
	if p <= 0 {
		return vals[0]
	}
	if p >= 1 {
		return vals[len(vals)-1]
	}
	// Nearest-rank: the smallest value at or above the p-th position. Ceil
	// keeps the rank in [1, len] so the index below is always in range.
	rank := min(max(int(math.Ceil(p*float64(len(vals)))), 1), len(vals))
	return vals[rank-1]
}

// r5RAMBytes maps AWS EC2 r5 instance types to their RAM. The r5 family is
// fixed at 8 GiB per vCPU, but the table is written out rather than computed so
// an unknown type is an explicit miss the operator has to resolve, not a
// silently plausible number.
var r5RAMBytes = map[string]int64{
	"r5.large":     16 << 30,
	"r5.xlarge":    32 << 30,
	"r5.2xlarge":   64 << 30,
	"r5.4xlarge":   128 << 30,
	"r5.8xlarge":   256 << 30,
	"r5.12xlarge":  384 << 30,
	"r5.16xlarge":  512 << 30,
	"r5.24xlarge":  768 << 30,
	"r5.metal":     768 << 30,
	"r5d.large":    16 << 30,
	"r5d.xlarge":   32 << 30,
	"r5d.2xlarge":  64 << 30,
	"r5d.4xlarge":  128 << 30,
	"r5d.8xlarge":  256 << 30,
	"r5d.12xlarge": 384 << 30,
	"r5d.16xlarge": 512 << 30,
	"r5d.24xlarge": 768 << 30,
}

// RAMForInstanceType returns the RAM for a known instance type. The second
// result is false for an unknown type, which the caller surfaces rather than
// guessing: an over-estimate of RAM produces an over-estimate of the budget,
// which is exactly the failure this tool exists to prevent.
func RAMForInstanceType(t string) (int64, bool) {
	b, ok := r5RAMBytes[t]
	return b, ok
}

// ConfigRollup is the per-configuration recommendation: the minimum over the
// configuration's eligible members, because a shared value must be safe for
// every instance that receives it.
type ConfigRollup struct {
	Config string

	// Members is every instance carrying this configuration, Eligible is the
	// subset that reached VerdictOK.
	Members  int
	Eligible int

	// MaxSize is the value to roll out, or 0 when no member is eligible.
	MaxSize int64

	// TargetHitRate is the value to roll out with it: the lowest recommended
	// among eligible members. A target above a member's achievable ceiling
	// removes that member's stopping signal, so the lowest ceiling binds a
	// shared configuration exactly as the tightest memory budget does.
	TargetHitRate float64

	// TargetBinding names the member whose ceiling set it.
	TargetBinding string

	// Binding names the member whose max-size set the minimum — the instance to
	// use as the canary, and the one to remove first if the value is too low to
	// be useful.
	Binding string

	// Excluded lists members that did not reach VerdictOK, with their verdicts.
	Excluded map[Verdict]int
}

// RollUp groups per-instance recommendations into per-configuration values.
// Instances that did not reach VerdictOK do not contribute a max-size but are
// still counted, so a configuration whose members mostly failed is visible as
// such rather than appearing to be a confident recommendation from one survivor.
func RollUp(instances []InstanceResult) []ConfigRollup {
	byConfig := map[string]*ConfigRollup{}
	order := []string{}

	for _, in := range instances {
		r, ok := byConfig[in.Config]
		if !ok {
			r = &ConfigRollup{Config: in.Config, Excluded: map[Verdict]int{}}
			byConfig[in.Config] = r
			order = append(order, in.Config)
		}
		r.Members++

		if in.Rec.Verdict != VerdictOK {
			r.Excluded[in.Rec.Verdict]++
			continue
		}
		r.Eligible++
		if r.MaxSize == 0 || in.Rec.MaxSize < r.MaxSize {
			r.MaxSize = in.Rec.MaxSize
			r.Binding = in.Name
		}
		if in.TargetHitRate > 0 && (r.TargetHitRate == 0 || in.TargetHitRate < r.TargetHitRate) {
			r.TargetHitRate = in.TargetHitRate
			r.TargetBinding = in.Name
		}
	}

	sort.Strings(order)
	out := make([]ConfigRollup, 0, len(order))
	for _, c := range order {
		out = append(out, *byConfig[c])
	}
	return out
}

// NarrowKeyCount returns K(n): the series-weighted count of (measurement, tag
// key) pairs with fewer than n distinct values.
//
// It bounds the payload, expressed in units of "one whole tag key" — that is,
// payload / (2 * shard series):
//
//	effective keys(n) <= K(n) + 1
//
// The greedy fill exhausts pairs narrowest-first and stops once it has n
// entries, so every *fully* drawn pair satisfies V < n; at most one further pair
// is partly drawn, hence the +1.
//
// **Weighted by each measurement's share of the shard**, because a pair
// contributes 2*S_m and not 2*S. Counting pairs unweighted makes a database of
// many tiny measurements dominate: on an instance carrying `_internal` alongside
// one real measurement, the unweighted count reads 59 while the payload is
// really under 8 tag keys' worth. Weighting removes that distortion, and for the
// common case of one dominant measurement it reduces to simply counting that
// measurement's narrow tag keys — which is the form the runbook states.
//
// Checked against eight measured points across two 20-key schemas: the bound
// held every time, with slack between 0.02 and 2.73 keys.
func NarrowKeyCount(capacity int64, keys []KeyProfile) float64 {
	// Shard total: each measurement's series counted once, however many tag
	// keys it carries.
	perMeasurement := map[string]int64{}
	for _, k := range keys {
		perMeasurement[k.Measurement] = k.SeriesInShard
	}
	var total int64
	for _, s := range perMeasurement {
		total += s
	}
	if total <= 0 {
		return 0
	}

	var n float64
	for _, k := range keys {
		if k.Values > 0 && k.Values < capacity {
			n += float64(k.SeriesInShard) / float64(total)
		}
	}
	return n
}
