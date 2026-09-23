package main

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const gib = int64(1) << 30

func TestMemoryBudget(t *testing.T) {
	th := Defaults()

	tests := []struct {
		name    string
		ram     int64
		heapP95 int64
		want    int64
	}{
		{
			// Well below the healthy line: the absolute 5% ceiling binds.
			name:    "healthy instance is capped by the budget fraction",
			ram:     64 * gib,
			heapP95: 20 * gib,      // 31% of RAM
			want:    3_435_973_836, // 5% of 64 GiB, truncated
		},
		{
			// Close to the line: the headroom share binds instead, and the
			// budget is strictly smaller than the 5% ceiling.
			name:    "near the line the headroom share binds",
			ram:     64 * gib,
			heapP95: 30 * gib, // 47% of RAM; headroom is 2 GiB
			want:    gib,      // 0.5 * 2 GiB
		},
		{
			name:    "exactly at the healthy line yields nothing",
			ram:     64 * gib,
			heapP95: 32 * gib,
			want:    0,
		},
		{
			name:    "above the healthy line yields nothing",
			ram:     64 * gib,
			heapP95: 40 * gib,
			want:    0,
		},
		{
			name:    "unknown RAM yields nothing",
			ram:     0,
			heapP95: 10 * gib,
			want:    0,
		},
		{
			name:    "negative heap is rejected rather than treated as free space",
			ram:     64 * gib,
			heapP95: -1,
			want:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, MemoryBudget(tt.ram, tt.heapP95, th))
		})
	}
}

func TestMemoryBudgetTapersMonotonically(t *testing.T) {
	// The budget must never increase as an instance gets hotter: that is the
	// property the whole safety argument rests on.
	th := Defaults()
	ram := 128 * gib

	var prev int64 = math.MaxInt64
	for heap := int64(0); heap <= ram; heap += gib {
		got := MemoryBudget(ram, heap, th)
		require.LessOrEqual(t, got, prev, "budget rose as heap grew, at heap=%d", heap)
		prev = got
	}
	require.Zero(t, prev, "budget must reach zero at full RAM")
}

func TestEntryBytes(t *testing.T) {
	const (
		A   = entryOverheadBytes
		ctr = roaringContainerHeapBytes
	)

	tests := []struct {
		name   string
		k      int64
		idSpan int64
		want   int64
	}{
		{"empty set costs only the overhead", 0, 0, A},
		{"negative is clamped to zero", -5, 0, A},
		{
			// One member, one container: the container's fixed heap cost plus
			// 2.5 bytes of payload, truncated.
			name: "single member", k: 1, idSpan: 0,
			want: A + ctr + 2,
		},
		{
			// No span given: every member is its own container. Safe, and for a
			// large set very loose — 96 bytes per series of container structure.
			name: "no span is fully dispersed", k: 125_000, idSpan: 0,
			want: A + 125_000*ctr + 125_000*5/2,
		},
		{
			// A quarter-of-the-shard predicate over a 1M id space: 16 containers
			// cap the payload at 16*8192, well under 2.5k = 625 000.
			name: "bitmap term caps a wide set", k: 250_000, idSpan: 1_000_000,
			want: A + 16*ctr + 16*roaringBitmapContainerBytes,
		},
		{
			// Same span, smaller k: 2.5k is below the bitmap cap, so it wins.
			name: "array term wins for a sparse set", k: 20_000, idSpan: 1_000_000,
			want: A + 16*ctr + 20_000*5/2,
		},
		{
			// Containers can never exceed members.
			name: "container count is capped by k", k: 3, idSpan: 1 << 40,
			want: A + 3*ctr + 3*5/2,
		},
		{"overflow saturates instead of wrapping", math.MaxInt64, 0, math.MaxInt64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, EntryBytes(tt.k, tt.idSpan))
		})
	}
}

func TestEntryBytesIsAnUpperBound(t *testing.T) {
	// The reference predicates from scripts/tsi_cache_experiment, costed in
	// live heap: each set built by merge-then-Clone as Put stores it, measured
	// with runtime.MemStats, plus the 152-byte element/list/map structure and
	// 11 bytes of strings. The earlier version of this test compared against
	// the cache's bytes gauge, which counts serialized size and reads 1.9x
	// below heap on the 16-container rows. The bound must sit at or above
	// every heap figure, and within 1.5x of the strided layouts, which are
	// the worst case.
	const idSpan = 1_000_000

	measured := []struct {
		name   string
		k      int64
		heap   int64
		tight  bool
		gauged bool // no heap measurement: the figure is the gauge, a floor
	}{
		{"host", 1, 210 + 163, false, false},
		{"rack_rr strided", 500, 2_007 + 163, true, false},
		{"zone_rr strided", 20_000, 41_857 + 163, true, false},
		{"grid_rr strided", 62_500, 125_787 + 163, true, false},
		{"region_rr strided", 250_000, 132_014 + 163, true, false},
		{"region_blk clustered", 250_000, 16_766 + 163, false, false},
		{"zone_blk clustered (gauge)", 20_000, 8_405, false, true},
		{"grid_blk clustered (gauge)", 62_500, 8_515, false, true},
	}

	for _, m := range measured {
		t.Run(m.name, func(t *testing.T) {
			got := EntryBytes(m.k, idSpan)
			require.GreaterOrEqual(t, got, m.heap,
				"the bound must never sit below a measured cost")
			if m.tight {
				require.LessOrEqual(t, float64(got), float64(m.heap)*1.5,
					"the bound must stay within 1.5x of the strided worst case")
			}
		})
	}
}

func TestEntryBytesDispersedSetsAreDominatedByContainers(t *testing.T) {
	// One member per container, measured in heap at 47 to 76 bytes per
	// container: 100 members cost 4 879 bytes of set against a gauge of 440.
	// The bound must cover that, and the container term must be what does it.
	got := EntryBytes(100, 100*roaringContainerSpan)
	require.GreaterOrEqual(t, got, int64(4_879+163))
	require.Less(t, float64(got), 2.5*float64(4_879+163), "and not by more than 2.5x")
	require.Greater(t, 100*int64(roaringContainerHeapBytes), heapPayload(100)*10,
		"the container structure, not the 2.5-byte payload, is what a dispersed set costs")
}

func TestEntryBytesGrowsWithIDSpan(t *testing.T) {
	// The container term is increasing in idSpan, which is why idSpan must be an
	// upper bound: understating it understates the cost. Pin that direction so
	// the invariant cannot be inverted by a future refactor.
	const k = 250_000

	var prev int64
	for _, span := range []int64{65_536, 500_000, 1_000_000, 4_000_000, 16_000_000} {
		got := EntryBytes(k, span)
		require.GreaterOrEqual(t, got, prev, "cost must not fall as the id span grows")
		prev = got
	}
	// And it converges on the span-free 2k bound rather than exceeding it.
	require.LessOrEqual(t, prev, EntryBytes(k, 0))
}

func TestShardCacheBoundConservesCardinality(t *testing.T) {
	// The identity the whole bound rests on: a tag key's values partition the
	// measurement's series, so caching all of them costs 2*S of payload
	// regardless of the key's cardinality. Four keys spanning 500x in
	// cardinality must produce the same payload.
	const S = 1_000_000

	for _, values := range []int64{4, 16, 50, 2000} {
		t.Run(fmt.Sprintf("%d values", values), func(t *testing.T) {
			// Span unknown: every series in its own container, so the bitmap
			// cap never binds and the payload is exactly the identity at the
			// heap rate.
			keys := []KeyProfile{{Measurement: "m", Key: "k", Values: values, SeriesInShard: S}}
			b := ShardCacheBreakdown(values, keys)
			require.Equal(t, heapPayload(S), b.Payload,
				"payload must be 2.5*S whatever the cardinality")
			require.Equal(t, values*entryOverheadBytes, b.Overhead)
			require.Equal(t, int64(S*roaringContainerHeapBytes), b.Containers,
				"fully dispersed: one container per series")

			// With the span known the bitmap cap can bind, and the payload can
			// only fall below the identity, never above it.
			known := []KeyProfile{{Measurement: "m", Key: "k", Values: values, SeriesInShard: S, IDSpan: 1_000_000}}
			kb := ShardCacheBreakdown(values, known)
			require.LessOrEqual(t, kb.Payload, heapPayload(S))
			require.LessOrEqual(t, kb.Containers, int64(16*roaringContainerHeapBytes)*values,
				"at most 16 containers per entry over a 1M id space")
			require.Less(t, kb.Total, b.Total, "knowing the span can only tighten the bound")
		})
	}
}

func TestShardCacheBoundTakesTheExpensiveKeysFirst(t *testing.T) {
	// Worst case means the adversarial fill. With a cap of 4 the bound must
	// choose the 4-value key (250 000 series each), not the 2000-value key.
	const S, span = 1_000_000, 1_000_000
	keys := []KeyProfile{
		{Measurement: "m", Key: "wide", Values: 2000, SeriesInShard: S, IDSpan: span},
		{Measurement: "m", Key: "narrow", Values: 4, SeriesInShard: S, IDSpan: span},
	}

	got := ShardCacheBound(4, keys)
	want := 4 * EntryBytes(S/4, span) // all of "narrow"
	require.Equal(t, want, got)

	// Filling past the narrow key must draw on the wide one, and each extra
	// entry there is far cheaper than a narrow one.
	step := ShardCacheBound(5, keys) - got
	require.Equal(t, EntryBytes(S/2000, span), step)
	require.Less(t, step, EntryBytes(S/4, span)/10,
		"the fifth entry must be far cheaper than a narrow-key entry")
}

func TestShardCacheBoundIsMonotonicAndPayloadSaturates(t *testing.T) {
	const S = 1_000_000
	keys := []KeyProfile{
		{Measurement: "m", Key: "a", Values: 4, SeriesInShard: S, IDSpan: S},
		{Measurement: "m", Key: "b", Values: 50, SeriesInShard: S, IDSpan: S},
		{Measurement: "m", Key: "c", Values: 2000, SeriesInShard: S, IDSpan: S},
	}

	var prev int64
	for _, c := range []int64{1, 4, 50, 100, 1000, 2054, 5000, 1 << 20} {
		got := ShardCacheBound(c, keys)
		require.GreaterOrEqual(t, got, prev, "bound must not fall as capacity rises")
		prev = got
	}

	// Beyond the 2054 entries the schema can supply, the payload and container
	// terms stop growing — but the overhead does not. An entry exists for any
	// value a query names, existing or not, so capacity past the schema is
	// charged at the fixed overhead each. An earlier version of this test
	// asserted the bound stopped growing here; that was the false "saturates"
	// verdict.
	full := ShardCacheBreakdown(2054, keys)
	huge := ShardCacheBreakdown(1<<20, keys)
	require.Equal(t, full.Payload, huge.Payload, "payload is bounded by the schema")
	require.Equal(t, full.Containers, huge.Containers, "and so are the containers")
	require.Equal(t, int64(2054), full.SchemaEntries)
	require.Equal(t, int64(2054), huge.SchemaEntries)
	require.Equal(t, int64(1<<20), huge.Entries)
	require.Equal(t, full.Total+(1<<20-2054)*entryOverheadBytes, huge.Total,
		"every entry past the schema costs exactly the overhead")

	// The payload ceiling is at most 2.5*S per tag key — three keys here — and
	// below it when the bitmap cap binds.
	require.LessOrEqual(t, full.Payload, 3*heapPayload(S))
	require.Equal(t, int64(2054*entryOverheadBytes), full.Overhead)
}

func TestLargestCapacityTerminatesOnASmallSchema(t *testing.T) {
	// A schema of 6 distinct values can still fill any cap with entries for
	// values that do not exist, so the ladder walk must find a finite cap that
	// exhausts the budget rather than running to its ceiling.
	keys := []KeyProfile{{Measurement: "m", Key: "k", Values: 6, SeriesInShard: 100, IDSpan: 1000}}
	model := ConservationModel([]ShardProfile{{ShardID: "1", Keys: keys}})
	got := LargestCapacity(1<<20, 100, math.MaxInt64/4, model)
	require.Positive(t, got)
	require.Less(t, got, int64(1<<20/entryOverheadBytes)*2, "bounded by overhead per entry")
	require.LessOrEqual(t, model(got), int64(1<<20))
	require.Greater(t, model(got*2), int64(1<<20))
}

// referenceFill is the measured worst-case fill from
// scripts/tsi_cache_experiment: 1M series, 9 tag keys, one shard, read from
// the cache's bytes gauge. The gauge counts serialized set sizes, so these are
// floors on heap, not heap; the strided rows of the same instance read 1.9x
// higher in heap.
var referenceFill = []struct {
	n        int64
	measured int64
}{
	{4, 67_006}, {8, 592_274}, {24, 847_726}, {40, 2_849_324},
	{90, 3_372_650}, {140, 5_384_740}, {1600, 7_159_384},
}

func TestSimpleShardBoundVsMeasuredFill(t *testing.T) {
	const S, T, span = int64(1_000_000), int64(9), int64(1_000_000)

	for _, f := range referenceFill {
		got := SimpleShardBound(f.n, S, T, span)
		require.GreaterOrEqual(t, got, f.measured,
			"bound must cover the measured fill at n=%d", f.n)
	}

	// At a realistic cap it stays within a small multiple of the gauge figure,
	// which is itself below heap; that is the trade for needing three numbers
	// instead of a schema profile.
	require.Less(t, float64(SimpleShardBound(1600, S, T, span)), float64(7_159_384)*4)
}

func TestSimpleShardBoundDispersedFallback(t *testing.T) {
	// Without the id span every series is costed as its own container, which
	// is safe and very loose: 96 bytes per series per key. Knowing the span
	// caps containers at n*C and pulls the bound down by an order of magnitude
	// on the reference shard.
	const S, T = int64(1_000_000), int64(9)
	unknown := SimpleShardBound(1600, S, T, 0)
	known := SimpleShardBound(1600, S, T, 1_000_000)
	require.Greater(t, unknown, known)
	require.Greater(t, float64(unknown)/float64(known), 10.0)
	require.Equal(t, S*T*roaringContainerHeapBytes,
		unknown-heapPayload(S)*T-1600*entryOverheadBytes, "the dispersed container term is 96*S*T")
}

func TestPerEntryConstantAloneIsNotABound(t *testing.T) {
	// The tempting shortcut "bytes <= A*n" omits the payload, which is conserved
	// per tag key rather than per entry. It fails at every measured capacity —
	// by 47x at the smallest — and this test exists so nobody reintroduces it.
	const S, T, span = int64(1_000_000), int64(9), int64(1_000_000)

	for _, f := range referenceFill {
		require.Less(t, f.n*entryOverheadBytes, f.measured,
			"A*n must be below the measurement at n=%d, i.e. not a bound", f.n)
		require.GreaterOrEqual(t, SimpleShardBound(f.n, S, T, span), f.measured,
			"adding the payload term must fix it at n=%d", f.n)
	}
}

func TestSimpleShardBoundSaturatesInTagKeys(t *testing.T) {
	// The payload and container terms cannot exceed their per-key ceilings,
	// so past T tag keys only the overhead grows. That is what makes the cap a
	// weak lever below the crossover, and the test pins it.
	const S, T, span = int64(1_000_000), int64(9), int64(1_000_000)

	capFree := heapPayload(S)*T + S*T*roaringContainerHeapBytes // both ceilings, at unlimited capacity
	crossover := capFree / entryOverheadBytes

	// Well below the crossover the cap contributes little.
	below := SimpleShardBound(1600, S, T, span)
	require.Less(t, float64(1600*entryOverheadBytes)/float64(below), 0.05,
		"below the crossover the cap should be under 5%% of the bound")

	// Above it the overhead dominates and the bound grows linearly in n.
	above := SimpleShardBound(crossover*4, S, T, span)
	require.Greater(t, float64(crossover*4*entryOverheadBytes)/float64(above), 0.5)

	// The schema-bounded terms never exceed their ceiling, whatever the capacity.
	require.Equal(t, capFree,
		SimpleShardBound(1<<40, S, T, span)-(1<<40)*entryOverheadBytes)
}

func TestSimpleShardBoundEdgeCases(t *testing.T) {
	require.Zero(t, SimpleShardBound(0, 1000, 5, 0))
	require.Zero(t, SimpleShardBound(100, 0, 5, 0))
	require.Zero(t, SimpleShardBound(100, 1000, 0, 0))

	// Fewer entries than tag keys: a cache of n entries cannot have drawn on
	// more than n keys. Span unknown, so the container term is 96 per series
	// per key drawn.
	require.Equal(t, heapPayload(1000)*2+1000*2*roaringContainerHeapBytes+2*entryOverheadBytes,
		SimpleShardBound(2, 1000, 5, 0))

	// With a span of one block, at most one container per entry.
	require.Equal(t, heapPayload(1000)*2+2*roaringContainerHeapBytes+2*entryOverheadBytes,
		SimpleShardBound(2, 1000, 5, 1000))

	require.Nil(t, SimpleModel(nil))
	require.NotNil(t, SimpleModel([]SimpleShardProfile{{Series: 1, TagKeys: 1}}))
}

func TestLacksIDSpan(t *testing.T) {
	require.False(t, LacksIDSpan(nil, nil))
	require.True(t, LacksIDSpan([]ShardProfile{{Keys: []KeyProfile{{Values: 4, SeriesInShard: 100}}}}, nil))
	require.False(t, LacksIDSpan([]ShardProfile{{Keys: []KeyProfile{{Values: 4, SeriesInShard: 100, IDSpan: 1}}}}, nil))
	require.True(t, LacksIDSpan(nil, []SimpleShardProfile{{Series: 100, TagKeys: 2}}))
	require.False(t, LacksIDSpan(nil, []SimpleShardProfile{{Series: 100, TagKeys: 2, IDSpan: 1}}))
}

func TestShardCacheBoundEdgeCases(t *testing.T) {
	keys := []KeyProfile{{Measurement: "m", Key: "k", Values: 4, SeriesInShard: 1000}}

	require.Zero(t, ShardCacheBound(0, keys))
	require.Zero(t, ShardCacheBound(-1, keys))
	// No schema at all still costs the overhead per entry: the cache holds an
	// entry for any value a query names.
	require.Equal(t, int64(100*entryOverheadBytes), ShardCacheBound(100, nil))

	// A key with no values contributes no payload rather than dividing by
	// zero; the capacity still costs the overhead per entry.
	require.Equal(t, int64(100*entryOverheadBytes), ShardCacheBound(100, []KeyProfile{{Values: 0, SeriesInShard: 1000}}))
}

func TestCostModelsRejectMissingInformation(t *testing.T) {
	// A model that returned 0 for every capacity would read as "everything
	// fits" and authorize an unbounded cap. Absence of information must be nil.
	require.Nil(t, PerEntryModel(0, 4096), "no shards")
	require.Nil(t, PerEntryModel(60, 0), "no entry cost")
	require.Nil(t, ConservationModel(nil), "no profile")
	require.NotNil(t, PerEntryModel(60, 4096))
	require.NotNil(t, ConservationModel([]ShardProfile{{ShardID: "1"}}))
}

func TestLargestCapacity(t *testing.T) {
	// A model costing 1 KiB per entry: the ladder value that fits 10 KiB is 800.
	model := func(c int64) int64 { return c * 1024 }

	require.Equal(t, int64(800), LargestCapacity(1024*1000, 100, 1<<20, model))
	require.Equal(t, int64(100), LargestCapacity(1024*100, 100, 1<<20, model))
	require.Zero(t, LargestCapacity(1024*99, 100, 1<<20, model), "not even the floor fits")
	require.Equal(t, int64(400), LargestCapacity(1<<40, 100, 400, model), "ceiling binds")
	require.Zero(t, LargestCapacity(0, 100, 1<<20, model))
	require.Zero(t, LargestCapacity(1<<40, 100, 1<<20, nil))
}

func TestConservationBeatsPerEntryOnMeasuredData(t *testing.T) {
	// The reference instance: 1M series in one shard, tag keys of 4/16/50/2000
	// values (the _blk and _rr pairs share a cardinality) plus a unique-per-
	// series key. Measured worst case at a cap of 1600 entries: 6.8 MB, with the
	// naive bound at 200.4 MB.
	const S = 1_000_000
	var keys []KeyProfile
	for _, v := range []int64{4, 4, 16, 16, 50, 50, 2000, 2000, S} {
		keys = append(keys, KeyProfile{Measurement: "m", Key: "k", Values: v, SeriesInShard: S, IDSpan: S})
	}
	profile := []ShardProfile{{ShardID: "1", Keys: keys}}

	// The measured fill is a gauge figure (serialized set sizes); the bound is
	// heap, so it sits above the measurement by more than the earlier 2.0x.
	const measured = 7_159_384 // bytes, from scripts/tsi_cache_experiment
	conservation := ConservationModel(profile)(1600)
	perEntry := PerEntryModel(1, EntryBytes(250_000, S))(1600)

	require.GreaterOrEqual(t, conservation, int64(measured),
		"the bound must cover what was actually measured")
	require.Less(t, float64(conservation), float64(measured)*3,
		"and must stay within 3x of the gauge figure")
	require.Greater(t, float64(perEntry)/float64(conservation), 10.0,
		"the per-entry bound should be an order of magnitude looser")
}

func TestByteSizeParsing(t *testing.T) {
	tests := []struct {
		in   string
		want int64
	}{
		// Plain byte counts still work, so existing invocations are unaffected.
		{"0", 0},
		{"4096", 4096},
		{"68719476736", 64 * gib},

		// Suffixes are binary, in every spelling.
		{"5G", 5 * gib},
		{"5g", 5 * gib},
		{"5GB", 5 * gib},
		{"5GiB", 5 * gib},
		{"64gb", 64 * gib},
		{"512M", 512 << 20},
		{"4K", 4 << 10},
		{"2T", 2 << 40},
		{"1P", 1 << 50},
		{"100B", 100},

		// Fractions: a size need not be integral in its own unit.
		{"1.5G", 1536 << 20},
		{"0.5T", 512 << 30},

		// Surrounding and internal whitespace.
		{" 5G ", 5 * gib},
		{"5 G", 5 * gib},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			var b byteSize
			require.NoError(t, b.Set(tt.in))
			require.Equal(t, tt.want, int64(b))
		})
	}
}

func TestByteSizeRejects(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"", "empty size"},
		{"abc", "has no number"},
		{"G", "has no number"},
		{"5X", `unknown unit "x"`},
		{"5 apples", `unknown unit "apples"`},
		{"5.5.5G", "is not a number"},
		{"-5G", "is negative"},
		{"99999999P", "overflows int64"},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			var b byteSize
			err := b.Set(tt.in)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.want)
			require.Zero(t, int64(b), "a rejected value must not be partially applied")
		})
	}
}

func TestByteSizeRoundTripsWithHumanBytes(t *testing.T) {
	// The report prints sizes with humanBytes; those strings must be accepted
	// back, which is the reason the suffixes are binary rather than SI.
	for _, v := range []int64{4 << 10, 512 << 20, 64 * gib, 768 * gib} {
		s := humanBytes(v)
		var b byteSize
		require.NoError(t, b.Set(s), "humanBytes produced %q, which -ram-bytes rejects", s)
		require.Equal(t, v, int64(b), "round trip of %q", s)
	}
}

func TestPreflight(t *testing.T) {
	ok := []InstanceResult{{Name: "a", RAMBytes: 64 * gib}}
	base := func() options {
		return options{model: "auto", heapPct: 0.95, idSpanFactor: 2, schemaProbe: true, th: Defaults()}
	}

	t.Run("valid run passes", func(t *testing.T) {
		o := base()
		require.NoError(t, preflight(ok, &o))
	})

	t.Run("missing RAM is caught before any collection", func(t *testing.T) {
		o := base()
		err := preflight([]InstanceResult{{Name: "a"}, {Name: "b"}}, &o)
		require.Error(t, err)
		require.Contains(t, err.Error(), "no RAM known for 2 instance(s): a, b")
		require.Contains(t, err.Error(), "nothing was collected")
	})

	t.Run("unit omitted from -ram-bytes", func(t *testing.T) {
		// "-ram-bytes 64" meaning 64G: plausible as a byte count, absurd as RAM,
		// and more likely now that suffixes exist.
		o := base()
		err := preflight([]InstanceResult{{Name: "a", RAMBytes: 64}}, &o)
		require.ErrorContains(t, err, "implausibly small RAM")
		require.ErrorContains(t, err, "64 is 64 bytes, 64G is 64 GiB")

		require.NoError(t, preflight([]InstanceResult{{Name: "a", RAMBytes: 1 << 30}}, &o),
			"1 GiB is the boundary and must be accepted")
	})

	t.Run("unknown model is rejected rather than falling through", func(t *testing.T) {
		o := base()
		o.model = "conservaton"
		require.ErrorContains(t, preflight(ok, &o), `-model "conservaton" is not one of`)
	})

	t.Run("conservation without the schema probe", func(t *testing.T) {
		o := base()
		o.model, o.schemaProbe = "conservation", false
		require.ErrorContains(t, preflight(ok, &o), "-model conservation needs the schema probe")
	})

	t.Run("per-entry with no source for bytes-per-entry", func(t *testing.T) {
		o := base()
		o.model, o.schemaProbe = "per-entry", false
		require.ErrorContains(t, preflight(ok, &o), "no source for bytes-per-entry")

		o.bytesPerEntry = 4096
		require.NoError(t, preflight(ok, &o), "supplying the figure resolves it")
	})

	t.Run("out-of-range thresholds", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			mut  func(*options)
			want string
		}{
			{"heap percentile", func(o *options) { o.heapPct = 1.5 }, "-heap-percentile"},
			{"negative id span", func(o *options) { o.idSpanFactor = -1 }, "-id-span-factor"},
			{"zero floor", func(o *options) { o.th.Floor = 0 }, "-floor"},
			{"healthy line above 1", func(o *options) { o.th.HealthyLine = 2 }, "-healthy-line"},
			{"zero budget fraction", func(o *options) { o.th.BudgetFrac = 0 }, "-budget-frac"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				o := base()
				tc.mut(&o)
				require.ErrorContains(t, preflight(ok, &o), tc.want)
			})
		}
	})

	t.Run("every problem is reported, not just the first", func(t *testing.T) {
		o := base()
		o.model, o.heapPct = "nope", 9
		err := preflight([]InstanceResult{{Name: "a"}}, &o)
		require.Error(t, err)
		for _, want := range []string{"-model", "-heap-percentile", "no RAM known"} {
			require.Contains(t, err.Error(), want,
				"fixing one problem and rediscovering the next costs another collection")
		}
	})
}

func TestClassify(t *testing.T) {
	th := Defaults()
	healthy := int64(20 * gib) // 31% of 64 GiB
	ram := 64 * gib

	busy := CacheActivity{Hits: 6000, Misses: 4000, Evictions: 3500, Sampled: true}
	satisfied := CacheActivity{Hits: 99000, Misses: 1000, Evictions: 0, Sampled: true}

	tests := []struct {
		name          string
		tsiShards     int64
		ram           int64
		heapP95       int64
		bytesPerEntry int64
		act           CacheActivity
		wantVerdict   Verdict
		wantMaxSize   int64
	}{
		{
			name:          "no tsi shards",
			tsiShards:     0,
			ram:           ram,
			heapP95:       healthy,
			bytesPerEntry: 4096,
			act:           busy,
			wantVerdict:   VerdictNotTSI,
		},
		{
			name:          "no headroom",
			tsiShards:     60,
			ram:           ram,
			heapP95:       40 * gib,
			bytesPerEntry: 4096,
			act:           busy,
			wantVerdict:   VerdictNoHeadroom,
		},
		{
			name:          "already fits",
			tsiShards:     60,
			ram:           ram,
			heapP95:       healthy,
			bytesPerEntry: 4096,
			act:           satisfied,
			wantVerdict:   VerdictNoBenefit,
		},
		{
			// A high hit rate with sustained eviction is still thrashing on a
			// hot tail, so the benefit filter must not fire.
			name:          "high hit rate but still evicting is a candidate",
			tsiShards:     60,
			ram:           ram,
			heapP95:       healthy,
			bytesPerEntry: 4096,
			act:           CacheActivity{Hits: 96000, Misses: 4000, Evictions: 4000, Sampled: true},
			wantVerdict:   VerdictOK,
			wantMaxSize:   1600,
		},
		{
			name:          "narrow tags leave too little room",
			tsiShards:     60,
			ram:           ram,
			heapP95:       healthy,
			bytesPerEntry: 250_000,
			act:           busy,
			wantVerdict:   VerdictTooTight,
		},
		{
			name:          "wide tags reach the first-pass cap",
			tsiShards:     60,
			ram:           ram,
			heapP95:       healthy,
			bytesPerEntry: 4096,
			act:           busy,
			wantVerdict:   VerdictOK,
			wantMaxSize:   1600, // 100 * 16, the first-pass cap
		},
		{
			name:          "unknown RAM",
			tsiShards:     60,
			ram:           0,
			heapP95:       healthy,
			bytesPerEntry: 4096,
			act:           busy,
			wantVerdict:   VerdictUnknown,
		},
		{
			name:          "unknown heap",
			tsiShards:     60,
			ram:           ram,
			heapP95:       0,
			bytesPerEntry: 4096,
			act:           busy,
			wantVerdict:   VerdictUnknown,
		},
		{
			name:          "unknown entry cost",
			tsiShards:     60,
			ram:           ram,
			heapP95:       healthy,
			bytesPerEntry: 0,
			act:           busy,
			wantVerdict:   VerdictUnknown,
		},
		{
			// An unsampled window must not be read as "already fits": absence
			// of evidence is not evidence the cache is healthy.
			name:          "unsampled activity still yields a recommendation",
			tsiShards:     60,
			ram:           ram,
			heapP95:       healthy,
			bytesPerEntry: 4096,
			act:           CacheActivity{},
			wantVerdict:   VerdictOK,
			wantMaxSize:   1600,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Classify(tt.tsiShards, tt.ram, tt.heapP95, PerEntryModel(tt.tsiShards, tt.bytesPerEntry), tt.act, th)
			require.Equal(t, tt.wantVerdict, got.Verdict, "reason: %s", got.Reason)
			require.Equal(t, tt.wantMaxSize, got.MaxSize)
			if got.Verdict != VerdictOK {
				require.Zero(t, got.MaxSize, "only an OK verdict may carry a max size")
			}
			if got.CappedByFirstPass {
				require.Greater(t, got.RawMaxSize, got.MaxSize,
					"when the first-pass cap binds, RawMaxSize must still show what the budget permits")
			} else if got.Verdict == VerdictOK {
				require.Equal(t, got.RawMaxSize, got.MaxSize)
			}
		})
	}
}

func TestClassifyReportsTheUncappedSize(t *testing.T) {
	// The reference profile: a 332 MiB budget permits 3200 on the ladder, the
	// first pass caps at 1600. Both must be visible, or the operator cannot see
	// how much of the recommendation was policy.
	th := Defaults()
	model := PerEntryModel(60, 4096) // 1600*60*4096 = 393 MB; 3200 -> 786 MB; 6400 -> 1.57 GB
	rec := Classify(60, 64*gib, 20*gib, model, CacheActivity{}, th)
	require.Equal(t, VerdictOK, rec.Verdict)
	require.True(t, rec.CappedByFirstPass)
	require.Equal(t, int64(1600), rec.MaxSize)
	require.Greater(t, rec.RawMaxSize, int64(1600), "the budget permits more than the first pass allows")
	require.LessOrEqual(t, model(rec.RawMaxSize), rec.BudgetBytes, "the raw size must itself fit the budget")
	require.Equal(t, model(rec.MaxSize), rec.WorstCaseBytes, "the worst case is at the recommended size, not the raw one")
}

func TestCombinedModelCostsUncoveredShards(t *testing.T) {
	// A shard the schema probe missed must not read as free. Shard 2 has only
	// the cheap profile, so it is costed with the simple bound.
	profiled := []ShardProfile{{ShardID: "1", Keys: []KeyProfile{
		{Measurement: "m", Key: "region", Values: 4, SeriesInShard: 1_000_000},
	}}}
	simple := []SimpleShardProfile{
		{ShardID: "1", Series: 1_000_000, TagKeys: 1}, // covered: must not be counted twice
		{ShardID: "2", Series: 500_000, TagKeys: 3},   // uncovered: simple bound applies
	}

	onlyProfiled := ConservationModel(profiled)(1600)
	combined := CombinedModel(profiled, simple)(1600)
	require.Equal(t, onlyProfiled+SimpleShardBound(1600, 500_000, 3, 0), combined,
		"the uncovered shard adds exactly its simple bound; the covered one adds nothing extra")

	n, series := UncoveredShards(profiled, simple)
	require.Equal(t, 1, n)
	require.Equal(t, int64(500_000), series)

	require.Nil(t, CombinedModel(nil, nil), "no information must be nil, not zero")
	require.NotNil(t, CombinedModel(nil, simple), "the cheap profile alone is still a bound")
}

func TestPinnedShardsPrefersWindowedRates(t *testing.T) {
	// Lifetime counters carry every miss of the climb. A shard that reached the
	// ceiling and is now meeting target over the recent window is not pinned,
	// and a shard whose lifetime looks fine but whose window is below target is.
	cfg := CacheConfig{Floor: 100, MaxSize: 2000, TargetHitRate: 0.99}
	snap := &VarsSnapshot{Cache: cfg, Caches: map[string]CacheStat{
		"climbed":  {Capacity: 2000, Hit: 9000, Miss: 1000}, // lifetime 0.90
		"decayed":  {Capacity: 2000, Hit: 99500, Miss: 500}, // lifetime 0.995
		"nowindow": {Capacity: 2000, Hit: 9400, Miss: 600},  // lifetime 0.94, no window: falls back
	}}
	windowed := map[string]CacheActivity{
		"climbed": {Hits: 9960, Misses: 40, Sampled: true},   // 0.996 now: the climb is over
		"decayed": {Hits: 9000, Misses: 1000, Sampled: true}, // 0.90 now: the workload changed
	}

	pinned, observed := PinnedShards(snap, windowed)
	require.Equal(t, []string{"decayed", "nowindow"}, pinned)
	// Both contribute their window (or lifetime) counts: (9000+9400)/(10000+10000).
	require.InDelta(t, 0.92, observed, 0.001)

	lifetimeOnly, _ := PinnedShards(snap, nil)
	require.Equal(t, []string{"climbed", "nowindow"}, lifetimeOnly,
		"without a window the lifetime ratio misreads the climbed shard as pinned")
}

func TestCapacitySpread(t *testing.T) {
	stat := func(c int64) CacheStat { return CacheStat{Capacity: c} }

	lo, hi, uniform := (&VarsSnapshot{Caches: map[string]CacheStat{"1": stat(100), "2": stat(100)}}).CapacitySpread()
	require.True(t, uniform)
	require.Equal(t, int64(100), lo)
	require.Equal(t, int64(100), hi)

	lo, hi, uniform = (&VarsSnapshot{Caches: map[string]CacheStat{"1": stat(100), "2": stat(1600), "3": stat(400)}}).CapacitySpread()
	require.False(t, uniform, "differing capacities prove adaptive sizing")
	require.Equal(t, int64(100), lo)
	require.Equal(t, int64(1600), hi)

	_, _, uniform = (&VarsSnapshot{}).CapacitySpread()
	require.False(t, uniform, "an empty snapshot is not evidence of anything")
}

func TestClassifyNeverExceedsBudget(t *testing.T) {
	// The recommendation must always fit the budget it was derived from:
	// max_size * shards * bytes_per_entry <= budget. This is the invariant that
	// keeps a rollout from OOMing an instance.
	th := Defaults()

	for _, shards := range []int64{1, 12, 60, 400, 5000} {
		for _, bpe := range []int64{500, 4096, 65536, 250_000} {
			for _, heap := range []int64{gib, 10 * gib, 25 * gib, 31 * gib} {
				rec := Classify(shards, 64*gib, heap, PerEntryModel(shards, bpe), CacheActivity{}, th)
				if rec.Verdict != VerdictOK {
					continue
				}
				used := rec.MaxSize * shards * bpe
				require.LessOrEqual(t, used, rec.BudgetBytes,
					"shards=%d bytes/entry=%d heap=%d max_size=%d exceeds budget",
					shards, bpe, heap, rec.MaxSize)
			}
		}
	}
}

func TestCacheActivityRates(t *testing.T) {
	tests := []struct {
		name         string
		act          CacheActivity
		wantHit      float64
		wantEviction float64
	}{
		{"no gets", CacheActivity{}, 0, 0},
		{"all hits", CacheActivity{Hits: 100}, 1, 0},
		{"all misses", CacheActivity{Misses: 100, Evictions: 100}, 0, 1},
		{"mixed", CacheActivity{Hits: 750, Misses: 250, Evictions: 200}, 0.75, 0.2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.InDelta(t, tt.wantHit, tt.act.HitRate(), 1e-9)
			require.InDelta(t, tt.wantEviction, tt.act.EvictionRate(), 1e-9)
		})
	}
}

func TestPercentile(t *testing.T) {
	tests := []struct {
		name string
		vals []float64
		p    float64
		want float64
	}{
		{"empty", nil, 0.95, 0},
		{"single", []float64{42}, 0.95, 42},
		{"median of odd", []float64{3, 1, 2}, 0.5, 2},
		{"p95 of 100", seq(1, 100), 0.95, 95},
		{"p0 is the minimum", seq(1, 100), 0, 1},
		{"p1 is the maximum", seq(1, 100), 1, 100},
		{"unsorted input is handled", []float64{9, 1, 5, 3, 7}, 0.5, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.InDelta(t, tt.want, Percentile(tt.vals, tt.p), 1e-9)
		})
	}
}

func seq(lo, hi int) []float64 {
	out := make([]float64, 0, hi-lo+1)
	for i := lo; i <= hi; i++ {
		out = append(out, float64(i))
	}
	return out
}

func TestRollUp(t *testing.T) {
	ok := func(name, config string, size int64) InstanceResult {
		return InstanceResult{
			Name: name, Config: config,
			Rec: Recommendation{Verdict: VerdictOK, MaxSize: size},
		}
	}
	bad := func(name, config string, v Verdict) InstanceResult {
		return InstanceResult{Name: name, Config: config, Rec: Recommendation{Verdict: v}}
	}

	got := RollUp([]InstanceResult{
		ok("a1", "large", 3200),
		ok("a2", "large", 800), // binding
		ok("a3", "large", 1600),
		bad("b1", "small", VerdictNoHeadroom),
		bad("b2", "small", VerdictTooTight),
		ok("c1", "medium", 1600),
	})

	require.Len(t, got, 3)

	byName := map[string]ConfigRollup{}
	for _, c := range got {
		byName[c.Config] = c
	}

	large := byName["large"]
	require.Equal(t, int64(800), large.MaxSize, "the minimum over members must win")
	require.Equal(t, "a2", large.Binding)
	require.Equal(t, 3, large.Members)
	require.Equal(t, 3, large.Eligible)

	small := byName["small"]
	require.Zero(t, small.MaxSize, "a configuration with no eligible member gets no value")
	require.Equal(t, 2, small.Members)
	require.Zero(t, small.Eligible)
	require.Equal(t, 1, small.Excluded[VerdictNoHeadroom])
	require.Equal(t, 1, small.Excluded[VerdictTooTight])

	require.Equal(t, int64(1600), byName["medium"].MaxSize)
	require.Equal(t, "c1", byName["medium"].Binding)
}

func TestEstimateEntryBytes(t *testing.T) {
	// One measurement with 1M series in the database; the shard holds 500k of
	// them. A 4-value tag key therefore covers 125k series per entry, and a
	// 5000-value key covers 100.
	sc := &Schema{
		Database:          "db",
		SeriesCardinality: map[string]int64{"cpu": 1_000_000},
		TagValueCount: map[string]map[string]int64{
			"cpu": {"region": 4, "host": 5000},
		},
	}

	worst, typical := EstimateEntryBytes(500_000, sc, 0)
	require.Equal(t, EntryBytes(125_000, 0), worst, "the lowest-cardinality key must set the worst case")
	require.Greater(t, worst, typical)
	require.Equal(t, EntryBytes(100, 0), typical, "median of the two pairs, nearest-rank, is the smaller")

	t.Run("no schema", func(t *testing.T) {
		w, ty := EstimateEntryBytes(500_000, nil, 0)
		require.Zero(t, w)
		require.Zero(t, ty)
	})

	t.Run("no series in shard", func(t *testing.T) {
		w, ty := EstimateEntryBytes(0, sc, 0)
		require.Zero(t, w)
		require.Zero(t, ty)
	})

	t.Run("zero tag values are skipped rather than dividing by zero", func(t *testing.T) {
		bad := &Schema{
			SeriesCardinality: map[string]int64{"cpu": 1000},
			TagValueCount:     map[string]map[string]int64{"cpu": {"k": 0}},
		}
		w, ty := EstimateEntryBytes(1000, bad, 0)
		require.Zero(t, w)
		require.Zero(t, ty)
	})
}

func TestRAMForInstanceType(t *testing.T) {
	ram, ok := RAMForInstanceType("r5.2xlarge")
	require.True(t, ok)
	require.Equal(t, 64*gib, ram)

	_, ok = RAMForInstanceType("m5.2xlarge")
	require.False(t, ok, "an unknown type must miss rather than guess")
}

// heapProfileFixture is a legacy text heap profile (the debug=1 format) with
// two records: one allocated under innerLockingPut and one that is not.
const heapProfileFixture = `heap profile: 3: 2097152 [5: 3145728] @ heap/2097152
2: 1048576 [3: 1572864] @ 0x1 0x2 0x3
#	0x1	github.com/influxdata/roaring.(*Bitmap).Clone+0x40	/go/roaring/roaring.go:100
#	0x2	github.com/influxdata/influxdb/tsdb.(*SeriesIDSet).Clone+0x20	/go/influxdb/tsdb/series_set.go:210
#	0x3	github.com/influxdata/influxdb/tsdb/index/tsi1.(*TagValueSeriesIDCache).innerLockingPut+0x1db	/go/influxdb/tsdb/index/tsi1/cache.go:395
1: 1048576 [2: 1572864] @ 0x4 0x5
#	0x4	github.com/influxdata/influxdb/tsdb.(*SeriesIDSet).Clone+0x20	/go/influxdb/tsdb/series_set.go:210
#	0x5	github.com/influxdata/influxdb/tsdb/index/tsi1.(*Index).TagValueSeriesIDIterator+0x88	/go/influxdb/tsdb/index/tsi1/index.go:1134

# runtime.MemStats
# Alloc = 123456
`

func TestParseHeapText(t *testing.T) {
	// The fixture header carries 2*MemProfileRate, so rate is 1 MiB. Each
	// record's average object is large relative to the rate, so the scale
	// factor is close to 1 but strictly above it.
	got, err := parseHeapText(strings.NewReader(heapProfileFixture), "innerLockingPut")
	require.NoError(t, err)

	_, want := scaleHeapSample(2, 1048576, 1048576)
	require.Equal(t, want, got, "only the innerLockingPut record may be counted")
	require.Greater(t, got, int64(1048576), "sampled bytes must be scaled up, not reported raw")

	t.Run("focus on the query path picks the other record", func(t *testing.T) {
		got, err := parseHeapText(strings.NewReader(heapProfileFixture), "TagValueSeriesIDIterator")
		require.NoError(t, err)
		_, want := scaleHeapSample(1, 1048576, 1048576)
		require.Equal(t, want, got)
	})

	t.Run("a focus matching nothing yields zero", func(t *testing.T) {
		got, err := parseHeapText(strings.NewReader(heapProfileFixture), "nosuchfunction")
		require.NoError(t, err)
		require.Zero(t, got)
	})

	t.Run("a missing header is an error rather than a silent zero", func(t *testing.T) {
		_, err := parseHeapText(strings.NewReader("not a profile\n"), "innerLockingPut")
		require.Error(t, err)
	})
}

func TestScaleHeapSample(t *testing.T) {
	tests := []struct {
		name          string
		count, size   int64
		rate          int64
		wantAtLeast   int64
		wantExactSize int64
	}{
		{name: "zero count", count: 0, size: 100, rate: 512 * 1024, wantExactSize: 0},
		{name: "zero size", count: 1, size: 0, rate: 512 * 1024, wantExactSize: 0},
		{
			// Rate <= 1 means every allocation was sampled; the values are
			// already exact and must be passed through untouched.
			name: "unsampled profile passes through", count: 7, size: 700, rate: 1,
			wantExactSize: 700,
		},
		{
			// A small object relative to the rate is sampled rarely, so its
			// scale factor is large.
			name: "small objects scale up sharply", count: 1, size: 64, rate: 512 * 1024,
			wantAtLeast: 64 * 1000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, size := scaleHeapSample(tt.count, tt.size, tt.rate)
			if tt.wantAtLeast > 0 {
				require.GreaterOrEqual(t, size, tt.wantAtLeast)
			} else {
				require.Equal(t, tt.wantExactSize, size)
			}
		})
	}
}

func TestRecommendTarget(t *testing.T) {
	const fallback = 0.90

	tests := []struct {
		name     string
		observed float64
		pinned   bool
		want     float64
		reason   string
	}{
		{
			// No evidence: the floor stands.
			name: "no observation", observed: 0, pinned: false, want: fallback,
			reason: "default",
		},
		{
			// Below the floor at the current size says nothing about the
			// ceiling — LRU can only do better with more room — so do not
			// lower the target on this evidence.
			name: "observed below the floor", observed: 0.60, pinned: false, want: fallback,
			reason: "default",
		},
		{
			// Within the margin of the floor: not enough to move it.
			name: "observed barely above the floor", observed: 0.915, pinned: false, want: fallback,
			reason: "default",
		},
		{
			// Demonstrated at the current size, so achievable at a larger one.
			name: "observed well above the floor", observed: 0.97, pinned: false, want: 0.95,
			reason: "achieved at the current cache size",
		},
		{
			// The margin alone holds the result at or below the cap, since a hit
			// rate cannot exceed 1.0. The clamp is defensive, not load-bearing.
			name: "a near-perfect hit rate lands just under the cap", observed: 0.999, pinned: false,
			want: 0.97, reason: "achieved at the current cache size",
		},
		{
			// Out-of-range input, the only way the clamp fires.
			name: "impossible hit rate is clamped", observed: 1.5, pinned: false,
			want: maxRecommendedTarget, reason: "achieved at the current cache size",
		},
		{
			// Pinned is authoritative and may push below the floor: this
			// workload cannot do better than 0.95, so aiming at 0.90 leaves
			// nothing on the table while aiming higher would run away.
			name: "pinned ceiling above the floor", observed: 0.95, pinned: true, want: 0.93,
			reason: "ceiling observed on shards pinned",
		},
		{
			name: "pinned ceiling below the floor", observed: 0.70, pinned: true, want: 0.68,
			reason: "ceiling observed on shards pinned",
		},
		{
			// A ceiling so low the margin would take it to zero.
			name: "pinned ceiling near zero", observed: 0.01, pinned: true, want: fallback,
			reason: "default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, why := RecommendTarget(tt.observed, tt.pinned, fallback)
			require.InDelta(t, tt.want, got, 1e-9)
			require.Contains(t, why, tt.reason)
		})
	}
}

func TestRecommendTargetNeverExceedsWhatIsDemonstrated(t *testing.T) {
	// The whole point of the pinned case: a target above the achievable ceiling
	// removes the growth policy's stopping signal. The recommendation must
	// always sit strictly below the ceiling it was derived from.
	for _, ceiling := range []float64{0.30, 0.55, 0.80, 0.9478, 0.99} {
		got, _ := RecommendTarget(ceiling, true, 0.90)
		require.Less(t, got, ceiling,
			"a target at or above the observed ceiling of %.4f would never be reached", ceiling)
	}
}

func TestRollUpTakesTheLowestTarget(t *testing.T) {
	// A configuration is shared, so the member with the lowest achievable
	// ceiling binds it — the same logic as the tightest memory budget binding
	// max-size.
	ok := func(name string, size int64, target float64) InstanceResult {
		return InstanceResult{
			Name: name, Config: "large", TargetHitRate: target,
			Rec: Recommendation{Verdict: VerdictOK, MaxSize: size},
		}
	}

	got := RollUp([]InstanceResult{
		ok("a1", 3200, 0.95),
		ok("a2", 800, 0.93),  // binds max-size
		ok("a3", 1600, 0.90), // binds the target
	})
	require.Len(t, got, 1)
	require.Equal(t, int64(800), got[0].MaxSize)
	require.Equal(t, "a2", got[0].Binding)
	require.InDelta(t, 0.90, got[0].TargetHitRate, 1e-9)
	require.Equal(t, "a3", got[0].TargetBinding,
		"the two can be bound by different members, and the report should say so")
}

func TestPinnedShards(t *testing.T) {
	cfg := CacheConfig{Floor: 100, MaxSize: 2000, TargetHitRate: 0.99}

	shard := func(capacity, hit, miss int64) CacheStat {
		return CacheStat{Capacity: capacity, Hit: hit, Miss: miss}
	}

	t.Run("at max and below target is pinned", func(t *testing.T) {
		snap := &VarsSnapshot{Cache: cfg, Caches: map[string]CacheStat{
			"1": shard(2000, 9500, 500), // 0.950 against a target of 0.99
			"2": shard(2000, 9400, 600), // 0.940
		}}
		pinned, observed := PinnedShards(snap, nil)
		require.Equal(t, []string{"1", "2"}, pinned)
		require.InDelta(t, 0.945, observed, 0.001,
			"the rate the pinned shards settled at is the workload's achievable ceiling")
	})

	t.Run("at max but meeting target is not pinned", func(t *testing.T) {
		// A large working set that genuinely needs the capacity and is being
		// served well is the feature working, not the failure mode.
		snap := &VarsSnapshot{Cache: cfg, Caches: map[string]CacheStat{
			"1": shard(2000, 9950, 50), // 0.995 >= target
		}}
		pinned, _ := PinnedShards(snap, nil)
		require.Empty(t, pinned)
	})

	t.Run("below max is not pinned however poor the hit rate", func(t *testing.T) {
		// Still climbing: a stopping signal is still reachable, it just has not
		// been reached.
		snap := &VarsSnapshot{Cache: cfg, Caches: map[string]CacheStat{
			"1": shard(800, 5000, 5000), // 0.50, but capacity < max
		}}
		pinned, _ := PinnedShards(snap, nil)
		require.Empty(t, pinned)
	})

	t.Run("mixed fleet reports only the pinned shards", func(t *testing.T) {
		snap := &VarsSnapshot{Cache: cfg, Caches: map[string]CacheStat{
			"1": shard(2000, 9000, 1000), // pinned
			"2": shard(400, 900, 100),    // still climbing
			"3": shard(2000, 9990, 10),   // at max, meeting target
		}}
		pinned, observed := PinnedShards(snap, nil)
		require.Equal(t, []string{"1"}, pinned)
		require.InDelta(t, 0.90, observed, 0.001,
			"only pinned shards contribute to the observed ceiling")
	})

	t.Run("silent when adaptive sizing is off", func(t *testing.T) {
		off := &VarsSnapshot{
			Cache:  CacheConfig{Floor: 100}, // max-size 0 => fixed cache
			Caches: map[string]CacheStat{"1": shard(100, 5000, 5000)},
		}
		pinned, _ := PinnedShards(off, nil)
		require.Empty(t, pinned, "a fixed cache sits at its size by definition, not pinned")
	})

	t.Run("shards with no traffic are ignored", func(t *testing.T) {
		snap := &VarsSnapshot{Cache: cfg, Caches: map[string]CacheStat{
			"1": shard(2000, 0, 0),
		}}
		pinned, _ := PinnedShards(snap, nil)
		require.Empty(t, pinned, "no gets means no evidence either way")
	})

	t.Run("nil input", func(t *testing.T) {
		pinned, observed := PinnedShards(nil, nil)
		require.Empty(t, pinned)
		require.Zero(t, observed)
	})
}

func TestAuditConfig(t *testing.T) {
	th := Defaults()
	budget := MemoryBudget(32*gib, 15*gib, th) // near the line: 512 MiB headroom, half of it
	model := PerEntryModel(251, 256)           // overhead-only entries, like the reference profile
	safeCap := LargestCapacity(budget, th.Floor, math.MaxInt64/4, model)
	require.Positive(t, safeCap)

	t.Run("a cap far above the budget is flagged with the multiples", func(t *testing.T) {
		cfg := CacheConfig{Floor: 100, MaxSize: 100_000, TargetHitRate: 0.95}
		act := CacheActivity{Hits: 9036, Misses: 964, Sampled: true}
		a, ok := AuditConfig(cfg, "flags", budget, safeCap, model, act)
		require.True(t, ok)
		require.True(t, a.OverBudget)
		require.Equal(t, model(100_000), a.WorstCaseBytes)
		require.Greater(t, a.BudgetMultiple, 10.0)
		require.InDelta(t, float64(100_000)/float64(safeCap), a.SafeCapMultiple, 1e-9)
		require.True(t, a.BelowTarget, "0.904 is below 0.95")
		require.Equal(t, "flags", a.Source)
	})

	t.Run("a cap inside the budget is not over", func(t *testing.T) {
		cfg := CacheConfig{Floor: 100, MaxSize: safeCap, TargetHitRate: 0.90}
		a, ok := AuditConfig(cfg, "instance", budget, safeCap, model, CacheActivity{})
		require.True(t, ok)
		require.False(t, a.OverBudget)
		require.InDelta(t, 1.0, a.SafeCapMultiple, 1e-9)
		require.False(t, a.BelowTarget, "no window sampled means no claim about the target")
	})

	t.Run("nothing to audit", func(t *testing.T) {
		_, ok := AuditConfig(CacheConfig{Floor: 100}, "instance", budget, safeCap, model, CacheActivity{})
		require.False(t, ok, "a fixed cache has no adaptive configuration to audit")
		_, ok = AuditConfig(CacheConfig{Floor: 100, MaxSize: 1600, TargetHitRate: 0.9}, "instance", budget, safeCap, nil, CacheActivity{})
		require.False(t, ok, "no model")
		_, ok = AuditConfig(CacheConfig{Floor: 100, MaxSize: 1600, TargetHitRate: 0.9}, "instance", 0, safeCap, model, CacheActivity{})
		require.False(t, ok, "no budget")
	})
}

func TestParseWindow(t *testing.T) {
	for _, tt := range []struct {
		in   string
		want time.Duration
	}{
		{"7d", 7 * 24 * time.Hour},
		{"36h", 36 * time.Hour},
		{"2w", 14 * 24 * time.Hour},
		{"30m", 30 * time.Minute},
		{" 45s ", 45 * time.Second},
	} {
		got, err := ParseWindow(tt.in)
		require.NoError(t, err, tt.in)
		require.Equal(t, tt.want, got, tt.in)
	}
	for _, bad := range []string{"", "d", "7", "7 d", "7days", "1.5h"} {
		_, err := ParseWindow(bad)
		require.Error(t, err, bad)
	}
}

func TestKnownConfigPrefersFlags(t *testing.T) {
	reported := CacheConfig{Floor: 100, MaxSize: 1600, TargetHitRate: 0.90}

	o := &options{}
	cfg, src := o.knownConfig(reported, "instance", 100)
	require.Equal(t, reported, cfg)
	require.Equal(t, "instance", src)

	o = &options{currentMaxSize: 100_000, currentTarget: 0.95}
	cfg, src = o.knownConfig(CacheConfig{}, "", 250)
	require.Equal(t, CacheConfig{Floor: 250, MaxSize: 100_000, TargetHitRate: 0.95}, cfg,
		"flags fill a build that published nothing, with the floor from -floor")
	require.Equal(t, "flags", src)

	o = &options{currentMaxSize: 100_000}
	cfg, _ = o.knownConfig(reported, "instance", 100)
	require.Equal(t, CacheConfig{Floor: 100, MaxSize: 100_000, TargetHitRate: 0.90}, cfg,
		"one flag overrides one field and keeps the rest of what was reported")

	o = &options{}
	_, src = o.knownConfig(reported, "flags", 100)
	require.Equal(t, "flags", src, "without flags the recorded provenance is kept")
}
