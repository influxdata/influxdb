package tsi1

import (
	"container/list"
	"math"
	"sync"
	"sync/atomic"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"go.uber.org/zap"
)

// statTagValueCacheMeasurement is the measurement name under which the tag
// value series ID cache reports its statistics.
const statTagValueCacheMeasurement = "tsi1_cache"

// logMsgCacheCapacityIncreased is the message logged when adaptive sizing
// grows the cache. Shared with tests so the assertion tracks the source.
const logMsgCacheCapacityIncreased = "tsi cache capacity increased"

// logMsgCacheCapacityDecreased is the message logged when adaptive sizing
// shrinks the cache. Shared with tests so the assertion tracks the source.
const logMsgCacheCapacityDecreased = "tsi cache capacity decreased"

// Statistic field names for the tag value series ID cache.
const (
	statTagValueCacheHit            = "hit"
	statTagValueCacheMiss           = "miss"
	statTagValueCacheEviction       = "eviction"
	statTagValueCacheShrinkEviction = "shrink_eviction"
	statTagValueCacheSize           = "size"
	statTagValueCacheCapacity       = "capacity"
)

// maxShrinkEvictPerEvent caps the number of LRU-tail entries shed in a single
// shrink event. The per-event bound limits how long the write lock is held
// during a shrink trim; further decay continues over subsequent windows. The
// other per-event bound is size/2 (applied in decideColdTail).
const maxShrinkEvictPerEvent = 1024

// TagValueSeriesIDCacheStatistics holds counters describing the behavior of a
// TagValueSeriesIDCache. Fields are atomic.Int64 so that Statistics can be
// sampled without acquiring the cache lock.
//
// Evictions counts entries forced out under write pressure (a Put on a full
// cache); ShrinkEvictions counts entries shed by the adaptive shrink policy.
// They are tracked separately because only Evictions is the binomial signal
// the shrink eviction-gate is derived against (Bernoulli "miss on full cache
// → forced eviction"), and operators often want to distinguish "the cache is
// under pressure" from "the cache is voluntarily releasing memory."
type TagValueSeriesIDCacheStatistics struct {
	Hits            atomic.Int64
	Misses          atomic.Int64
	Evictions       atomic.Int64
	ShrinkEvictions atomic.Int64
	Size            atomic.Int64
}

// resizeEvent describes a single capacity-change event (grow or shrink).
// Snapshot fields are captured under the cache write lock so the log line can be
// emitted by the caller after releasing the lock. evicted is set only for shrink
// events (entries shed to reach the new capacity); it is 0 for grow.
type resizeEvent struct {
	oldCap, newCap, gets, evicted int64
	rate                          float64
}

// TagValueSeriesIDCache is an LRU cache for series id sets associated with
// name -> key -> value mappings. The purpose of the cache is to provide
// efficient means to get sets of series ids that would otherwise involve merging
// many individual bitmaps at query time.
//
// When initialising a TagValueSeriesIDCache a capacity must be provided. When
// more than c items are added to the cache, the least recently used item is
// evicted from the cache.
//
// A TagValueSeriesIDCache comprises a linked list implementation to track the
// order by which items should be evicted from the cache, and a hashmap implementation
// to provide constant time retrievals of items from the cache.
type TagValueSeriesIDCache struct {
	// capacity is the current cache capacity. Read with capacity.Load()
	// (lockless reads from Statistics); written with capacity.Store() under
	// the cache write lock when the adaptive policy grows it. It is an
	// atomic.Int64, which aligns itself, so its placement is unconstrained.
	capacity atomic.Int64

	// stats holds the atomically-accessed counters.
	stats TagValueSeriesIDCacheStatistics

	sync.RWMutex
	cache   map[string]map[string]map[string]*list.Element
	evictor *list.List

	// Adaptive sizing fields. maxCapacity == 0 means adaptive sizing is
	// disabled and none of the other fields below are consulted. When
	// enabled, lastHits/lastMisses/evictionsSinceCheck are read and written
	// only under the write lock (checkEviction always holds it). None of
	// these int64 fields are accessed atomically, so their placement is
	// unconstrained.
	maxCapacity         int64
	targetHitRate       float64
	shrinkConservatism  float64 // sigmas below the at-target eviction mean for the shrink gate; >= 0.0
	minSamples          int64
	logger              *zap.Logger
	evictionsSinceCheck int64
	lastHits            int64
	lastMisses          int64

	// Shrink (capacity-decay) state. Written only under the write lock; not
	// accessed atomically, so placement is unconstrained. minCapacity is the
	// floor (the configured initial size); capacity never shrinks below it.
	// A window spans getsSinceShrinkCheck Gets up to shrinkWindowGets (0 means
	// no active window — recompute from current occupancy on the next Get). The
	// shrinkBase* fields snapshot the stats counters at window start so deltas
	// can be measured. deepestTouched marks the back of the warm front-prefix
	// observed this window (the LRU access footprint). cooldownGets is the number
	// of Gets for which shrink stays suppressed after any capacity change, sized
	// to the new capacity so a freshly-resized cache is exercised before the next
	// shrink decision.
	minCapacity          int64
	getsSinceShrinkCheck int64
	shrinkWindowGets     int64
	shrinkBaseHits       int64
	shrinkBaseMisses     int64
	shrinkBaseEvictions  int64
	cooldownGets         int64
	deepestTouched       *list.Element
}

// NewTagValueSeriesIDCache returns a TagValueSeriesIDCache with fixed
// capacity c. Adaptive sizing is disabled; the cache will evict at c.
func NewTagValueSeriesIDCache(c int) *TagValueSeriesIDCache {
	cache := &TagValueSeriesIDCache{
		cache:   map[string]map[string]map[string]*list.Element{},
		evictor: list.New(),
		logger:  zap.NewNop(),
	}
	cache.capacity.Store(int64(c))
	return cache
}

// NewAdaptiveTagValueSeriesIDCache returns a TagValueSeriesIDCache that sizes
// itself between initial and max as the workload changes; capacity is not a
// fixed high-water mark and callers must not rely on it staying at any value.
//
// It grows by doubling toward max when the observed Get hit rate falls below
// target while under eviction pressure (driven from the eviction path: after
// every `capacity` evictions it samples the windowed hit rate and grows once if
// the policy fires). It also shrinks: driven from the read path, after a
// self-tuning window of Gets, a cache that is quiet (few evictions) and serving
// at/above target trims capacity toward its observed working set, shedding only
// least-recently-used entries left untouched over the window, never below
// initial. A cooldown between resizes prevents grow/shrink oscillation.
//
// minSamples is a floor on the number of Get operations observed in a window
// before the grow policy is allowed to act; below this floor the rate is treated
// as too noisy to react to. shrinkConservatism (>= 0.0) is the number of standard
// deviations below the at-target eviction mean at which the shrink eviction gate
// sits; see the doc on SeriesIDSetCacheShrinkConservatism in tsdb.Config.
//
// Every argument is validated; each invalid one is logged separately (so a
// misconfiguration with several bad values surfaces all of them at once rather
// than one error per restart), and a fixed-size (non-adaptive) cache from
// NewTagValueSeriesIDCache is returned in place of the adaptive one, so
// misconfiguration degrades to a working cache rather than crashing the process.
// The fallback uses initial when it is itself valid (>0), otherwise
// tsdb.DefaultSeriesIDSetCacheSize. Invalid arguments are: initial <= 0; max <=
// initial (an adaptive cache that cannot grow is a misconfiguration — use
// NewTagValueSeriesIDCache directly for a fixed cache); target not in (0, 1)
// (1.0 is unachievable and is rejected by config validation; NaN is rejected);
// shrinkConservatism not in [0, +Inf) (NaN/±Inf rejected); minSamples < 0.
func NewAdaptiveTagValueSeriesIDCache(initial, max int, target, shrinkConservatism float64, minSamples int, logger *zap.Logger) *TagValueSeriesIDCache {
	if logger == nil {
		logger = zap.NewNop()
	}

	// Validate every argument before deciding, logging each problem and setting
	// useFallback, rather than returning on the first failure. A misconfiguration
	// with several bad values then reports all of them in one pass.
	const logPrefix = "NewAdaptiveTagValueSeriesIDCache: "
	useFallback := false

	if initial <= 0 {
		logger.Error(logPrefix+"initial must be > 0", zap.Int("initial", initial))
		useFallback = true
	}
	if max <= initial {
		logger.Error(logPrefix+"max must be > initial", zap.Int("initial", initial), zap.Int("max", max))
		useFallback = true
	}
	// Positive range test so NaN (for which both `<` and `>` are false) is
	// rejected rather than slipping through.
	if !(target > 0 && target < 1) {
		logger.Error(logPrefix+"target must be in (0, 1)", zap.Float64("target", target))
		useFallback = true
	}
	// Same positive-form trick rejects NaN and ±Inf for the conservatism.
	if !(shrinkConservatism >= 0 && shrinkConservatism < math.Inf(1)) {
		logger.Error(logPrefix+"shrinkConservatism must be a finite value >= 0", zap.Float64("shrink_conservatism", shrinkConservatism))
		useFallback = true
	}
	if minSamples < 0 {
		logger.Error(logPrefix+"minSamples must be >= 0", zap.Int("min_samples", minSamples))
		useFallback = true
	}

	if useFallback {
		// Build the non-adaptive replacement: a fixed-size cache sized to initial
		// when valid, otherwise tsdb.DefaultSeriesIDSetCacheSize, with the caller's
		// logger attached so the error log lines and the fallback share a
		// destination. Safe to set the logger directly because the cache has not
		// yet been shared with other goroutines.
		size := initial
		if size <= 0 {
			size = tsdb.DefaultSeriesIDSetCacheSize
		}
		logger.Error(logPrefix+"falling back to fixed-size cache", zap.Int("fallback_size", size))
		c := NewTagValueSeriesIDCache(size)
		c.SetLogger(logger)
		return c
	}

	cache := &TagValueSeriesIDCache{
		cache:              map[string]map[string]map[string]*list.Element{},
		evictor:            list.New(),
		minCapacity:        int64(initial),
		maxCapacity:        int64(max),
		targetHitRate:      target,
		shrinkConservatism: shrinkConservatism,
		minSamples:         int64(minSamples),
		logger:             logger,
	}
	cache.capacity.Store(int64(initial))
	return cache
}

// SetLogger replaces the cache's logger. Intended for callers that
// construct the cache before a logger is available (and for tests that
// need to observe resize log lines). Not safe for concurrent use; call
// before the cache is shared with other goroutines.
func (c *TagValueSeriesIDCache) SetLogger(logger *zap.Logger) {
	if logger == nil {
		logger = zap.NewNop()
	}
	c.logger = logger
}

// Statistics returns statistics for periodic monitoring.
func (c *TagValueSeriesIDCache) Statistics(tags map[string]string) []models.Statistic {
	return []models.Statistic{{
		Name: statTagValueCacheMeasurement,
		Tags: tags,
		Values: map[string]interface{}{
			statTagValueCacheHit:            c.stats.Hits.Load(),
			statTagValueCacheMiss:           c.stats.Misses.Load(),
			statTagValueCacheEviction:       c.stats.Evictions.Load(),
			statTagValueCacheShrinkEviction: c.stats.ShrinkEvictions.Load(),
			statTagValueCacheSize:           c.stats.Size.Load(),
			statTagValueCacheCapacity:       c.capacity.Load(),
		},
	}}
}

// Get returns the SeriesIDSet associated with the {name, key, value} tuple if it
// exists.
func (c *TagValueSeriesIDCache) Get(name, key, value []byte) *tsdb.SeriesIDSet {
	ss, event, shrank := func() (*tsdb.SeriesIDSet, resizeEvent, bool) {
		c.Lock()
		defer c.Unlock()
		ss, hit := c.get(name, key, value)
		// Drive the adaptive shrink check from the read path: the grow check is
		// eviction-driven and is blind when the cache is quiet (exactly when a
		// shrink may be warranted). Log after releasing the lock, like Put.
		event, shrank := c.checkShrink(hit)
		return ss, event, shrank
	}()

	if shrank {
		c.logResize(event)
	}
	return ss
}

func (c *TagValueSeriesIDCache) get(name, key, value []byte) (*tsdb.SeriesIDSet, bool) {
	if mmap, ok := c.cache[string(name)]; ok {
		if tkmap, ok := mmap[string(key)]; ok {
			if ele, ok := tkmap[string(value)]; ok {
				// Track the LRU access footprint for the shrink policy before
				// MoveToFront moves the element (the rule reads ele.Prev()).
				// Only bother when a shrink is actually reachable (capacity above
				// the floor); at the floor a shrink can never fire, so skip it.
				if c.maxCapacity != 0 && c.capacity.Load() > c.minCapacity {
					c.trackTouchLocked(ele)
				}
				c.evictor.MoveToFront(ele) // This now becomes most recently used.
				c.stats.Hits.Add(1)
				return ele.Value.(*seriesIDCacheElement).SeriesIDSet, true
			}
		}
	}
	c.stats.Misses.Add(1)
	return nil, false
}

// trackTouchLocked maintains deepestTouched — the back-most element of the warm
// front-prefix (the deepest element touched this window). Called on a hit before
// MoveToFront. Invariant: touched elements form a contiguous front-prefix, so the
// boundary only recedes (toward the front) when the boundary element is itself
// re-touched (it is about to jump to the front); touching a cold or non-deepest
// warm element leaves the boundary unchanged. Must be called under the write lock.
func (c *TagValueSeriesIDCache) trackTouchLocked(e *list.Element) {
	switch {
	case c.deepestTouched == nil:
		c.deepestTouched = e
	case e == c.deepestTouched:
		// Re-touching the boundary moves it to the front; its predecessor (still
		// warm) becomes the new boundary. Keep e if it is already at the front
		// (warm set is the single element {e}).
		if p := e.Prev(); p != nil {
			c.deepestTouched = p
		}
	}
}

// exists returns true if the item exists for the tuple {name, key, value}.
func (c *TagValueSeriesIDCache) exists(name, key, value string) bool {
	if mmap, ok := c.cache[name]; ok {
		if tkmap, ok := mmap[key]; ok {
			_, ok := tkmap[value]
			return ok
		}
	}
	return false
}

// addToSet adds x to the SeriesIDSet associated with the tuple {name, key, value}
// if it exists. This method takes a lock on the underlying SeriesIDSet.
//
// NB this does not count as an access on the set—therefore the set is not promoted
// within the LRU cache.
func (c *TagValueSeriesIDCache) addToSet(name, key, value []byte, x uint64) {
	if mmap, ok := c.cache[string(name)]; ok {
		if tkmap, ok := mmap[string(key)]; ok {
			if ele, ok := tkmap[string(value)]; ok {
				ss := ele.Value.(*seriesIDCacheElement).SeriesIDSet
				if ss == nil {
					ele.Value.(*seriesIDCacheElement).SeriesIDSet = tsdb.NewSeriesIDSet(x)
					return
				}
				ele.Value.(*seriesIDCacheElement).SeriesIDSet.Add(x)
			}
		}
	}
}

// measurementContainsSets returns true if there are sets cached for the provided measurement.
func (c *TagValueSeriesIDCache) measurementContainsSets(name []byte) bool {
	_, ok := c.cache[string(name)]
	return ok
}

// Put adds the SeriesIDSet to the cache under the tuple {name, key, value}. If
// the cache is at its limit, then the least recently used item is evicted.
func (c *TagValueSeriesIDCache) Put(name, key, value []byte, ss *tsdb.SeriesIDSet) {
	event, grew := c.innerLockingPut(name, key, value, ss)
	if grew {
		c.logResize(event)
	}
}

func (c *TagValueSeriesIDCache) innerLockingPut(name, key, value []byte, ss *tsdb.SeriesIDSet) (resizeEvent, bool) {
	c.Lock()
	defer c.Unlock()
	// Convert once; the same string backing array is shared between
	// the cache element (used during eviction) and the map keys.
	nameStr := string(name)
	keyStr := string(key)
	valueStr := string(value)
	// Check under the write lock if the relevant item is now in the cache.
	if c.exists(nameStr, keyStr, valueStr) {
		return resizeEvent{}, false
	}

	// Update stats.Size after the element added and any evictions.
	defer func() {
		c.stats.Size.Store(int64(c.evictor.Len()))
	}()
	// Ensure our SeriesIDSet is go heap backed.
	if ss != nil {
		ss = ss.Clone()
	}

	// Create list item, and add to the front of the eviction list.
	listElement := c.evictor.PushFront(&seriesIDCacheElement{
		name:        nameStr,
		key:         keyStr,
		value:       valueStr,
		SeriesIDSet: ss,
	})

	// Add the listElement to the set of items. The tuple cannot already
	// exist here: the exists() check at the top of Put returns early if it
	// does, and nothing inserts into c.cache between that check and here.
	if mmap, ok := c.cache[nameStr]; ok {
		if tkmap, ok := mmap[keyStr]; ok {
			// Add the set to the map
			tkmap[valueStr] = listElement
			goto EVICT
		}

		// No series set map for the tag key - first tag value for the tag key.
		mmap[keyStr] = map[string]*list.Element{valueStr: listElement}
		goto EVICT
	}

	// No map for the measurement - first tag key for the measurment.
	c.cache[nameStr] = map[string]map[string]*list.Element{
		keyStr: map[string]*list.Element{valueStr: listElement},
	}

EVICT:
	return c.checkEviction()
}

// Delete removes x from the tuple {name, key, value} if it exists.
// This method takes a lock on the underlying SeriesIDSet.
func (c *TagValueSeriesIDCache) Delete(name, key, value []byte, x uint64) {
	c.Lock()
	c.delete(name, key, value, x)
	c.Unlock()
}

// delete removes x from the tuple {name, key, value} if it exists.
func (c *TagValueSeriesIDCache) delete(name, key, value []byte, x uint64) {
	if mmap, ok := c.cache[string(name)]; ok {
		if tkmap, ok := mmap[string(key)]; ok {
			if ele, ok := tkmap[string(value)]; ok {
				if ss := ele.Value.(*seriesIDCacheElement).SeriesIDSet; ss != nil {
					ele.Value.(*seriesIDCacheElement).SeriesIDSet.Remove(x)
				}
			}
		}
	}
}

// checkEviction checks if the cache is too big, and evicts the least
// recently used item if it is. The eviction is a forced eviction (driven by
// write pressure) — distinct from the voluntary evictions performed by the
// shrink trim — so it bumps stats.Evictions. When adaptive sizing is enabled
// and the cache has just turned over (i.e. accumulated `capacity` evictions
// since the last policy firing), it samples the windowed hit rate and may
// grow the capacity. The optional resizeEvent return carries the snapshot
// the caller should use to emit a log line after releasing the write lock;
// the bool is true iff a resize occurred. Does not update stats.Size; its caller must
func (c *TagValueSeriesIDCache) checkEviction() (resizeEvent, bool) {
	if int64(c.evictor.Len()) <= c.capacity.Load() {
		return resizeEvent{}, false
	}
	if !c.evictLRULocked() {
		return resizeEvent{}, false
	}
	c.stats.Evictions.Add(1)

	// Adaptive sizing: skip entirely when disabled to keep the hot path
	// free of per-eviction tracking work.
	if c.maxCapacity == 0 {
		return resizeEvent{}, false
	}

	c.evictionsSinceCheck++
	if c.evictionsSinceCheck < c.capacity.Load() {
		return resizeEvent{}, false
	}
	c.evictionsSinceCheck = 0
	return c.maybeResizeLocked()
}

// evictLRULocked removes the least-recently-used entry (the back of the
// evictor list) from the eviction list and the cache maps
// Returns false if the cache is empty. Does NOT bump Evictions or
// ShrinkEvictions — callers do that with the appropriate counter, since they
// know whether the eviction was forced (Put under pressure) or voluntary
// (shrink trim). Must be called under the write lock.  It does not
// decrement Size; callers must do that.
func (c *TagValueSeriesIDCache) evictLRULocked() bool {
	e := c.evictor.Back() // Least recently used item.
	if e == nil {
		return false
	}
	listElement := e.Value.(*seriesIDCacheElement)
	name := listElement.name
	key := listElement.key
	value := listElement.value

	// If the boundary points at the element being evicted, recede it to the
	// new back. This case fires only via the Put path on an all-warm cache
	// (the LRU is the boundary iff every entry was touched this window). Put
	// is reached after a Get miss, so the new front element is itself warm;
	// the warm set is then the cache minus the evicted entry, and the new
	// back-most warm element is e.Prev() at the moment of the call. Using
	// Prev() (rather than nil-ing) preserves the touched-front-prefix
	// invariant the warm-count walk relies on. Prev() is nil only when the
	// cache held exactly one element; nil-ing is then correct (empty cache).
	if e == c.deepestTouched {
		c.deepestTouched = e.Prev()
	}

	c.evictor.Remove(e)               // Remove from evictor
	delete(c.cache[name][key], value) // Remove from hashmap of items.

	// Check if there are no more tag values for the tag key.
	if len(c.cache[name][key]) == 0 {
		delete(c.cache[name], key)
	}

	// Check there are no more tag keys for the measurement.
	if len(c.cache[name]) == 0 {
		delete(c.cache, name)
	}
	return true
}

// maybeResizeLocked samples the windowed hit rate, calls the pure policy
// helper, advances the per-window snapshot, and atomically applies the
// new capacity if growth is warranted. Must be called under the write
// lock.
func (c *TagValueSeriesIDCache) maybeResizeLocked() (resizeEvent, bool) {
	hits := c.stats.Hits.Load()
	misses := c.stats.Misses.Load()
	curCap := c.capacity.Load()

	newCap, gets, rate, grow := decideResize(
		hits, misses, c.lastHits, c.lastMisses,
		curCap, c.maxCapacity, c.minSamples, c.targetHitRate,
	)

	// Always advance the snapshot so each firing measures the next
	// window, regardless of whether we grew.
	c.lastHits = hits
	c.lastMisses = misses

	if !grow {
		return resizeEvent{}, false
	}

	c.capacity.Store(newCap)
	// Suppress shrink until the freshly-grown cache (occupancy ~50% of the new
	// capacity) has been exercised: a full observation window sized to the new
	// capacity, so the cooldown scales with the grown size rather than the
	// half-full occupancy.
	c.cooldownGets = adaptiveWindowLen(newCap, c.minSamples, c.targetHitRate)
	return resizeEvent{
		oldCap: curCap,
		newCap: newCap,
		gets:   gets,
		rate:   rate,
	}, true
}

// decideResize implements the adaptive-sizing policy as a pure function
// of the cache's counters and configuration. Kept as a free function so
// the policy table can be exercised with integer literals, without
// configuring a *TagValueSeriesIDCache instance for every row.
func decideResize(hits, misses, lastHits, lastMisses, capacity, maxCapacity, minSamples int64, target float64) (newCap, gets int64, rate float64, grow bool) {
	hitsW := hits - lastHits
	missesW := misses - lastMisses
	gets = hitsW + missesW

	// Compute rate up-front when there is at least one read so the
	// returned rate is always meaningful when gets > 0, regardless of
	// which branch the policy takes.
	if gets > 0 {
		rate = float64(hitsW) / float64(gets)
	}

	// Explicitly gate the zero-reads window first: with no Gets there is no
	// evidence to act on, and the rate would be the zero-init 0.0 (looking
	// like a 0% hit rate, triggering growth on a pure-write workload). The
	// minSamples floor below is a configurable noise floor for tiny windows;
	// it can be 0, so it does not subsume the gets == 0 check.
	if gets == 0 {
		return capacity, gets, rate, false
	}
	if gets < minSamples {
		return capacity, gets, rate, false
	}

	if rate >= target {
		return capacity, gets, rate, false
	}
	if capacity >= maxCapacity {
		return capacity, gets, rate, false
	}

	// Clamp before doubling so large valid int64 capacities cannot
	// overflow when applying the adaptive growth policy.
	if capacity > maxCapacity/2 {
		newCap = maxCapacity
	} else {
		newCap = capacity * 2
	}
	return newCap, gets, rate, true
}

// logResize emits a single INFO log line describing a capacity-change event,
// either a grow (driven from Put) or a shrink (driven from Get). The direction
// is taken from the event: newCap > oldCap is a grow, otherwise a shrink. Both
// callers invoke this only after an actual capacity change, and grow strictly
// increases capacity while shrink strictly decreases it, so the comparison is
// unambiguous and selects only the log message. Every metric is logged on both
// lines so the field set is uniform regardless of direction: evicted is 0 for a
// grow, and min_capacity/max_capacity/target are the configured bounds in either
// case. c.minCapacity, c.maxCapacity, and c.targetHitRate are read directly — all
// set at construction and never mutated. Called after releasing the cache write
// lock.
func (c *TagValueSeriesIDCache) logResize(e resizeEvent) {
	msg := logMsgCacheCapacityIncreased
	if e.newCap <= e.oldCap {
		msg = logMsgCacheCapacityDecreased
	}
	c.logger.Info(msg,
		zap.Int64("old_capacity", e.oldCap),
		zap.Int64("new_capacity", e.newCap),
		zap.Int64("min_capacity", c.minCapacity),
		zap.Int64("max_capacity", c.maxCapacity),
		zap.Int64("evicted", e.evicted),
		zap.Float64("hit_rate", e.rate),
		zap.Float64("target", c.targetHitRate),
		zap.Int64("gets_window", e.gets),
	)
}

// checkShrink advances the Get-driven shrink window and, when a window
// completes, evaluates whether to shrink the cache. It is called from Get under
// the write lock and returns a resizeEvent the caller should log after releasing
// the lock (the bool is true iff a shrink occurred). When adaptive sizing is
// disabled it does nothing, keeping the read path free of shrink bookkeeping.
// hit reports whether the Get that triggered this call was a cache hit, so the
// new-window baselines can snapshot the pre-Get counter state.
func (c *TagValueSeriesIDCache) checkShrink(hit bool) (resizeEvent, bool) {
	if c.maxCapacity == 0 {
		return resizeEvent{}, false
	}

	// At the floor a shrink can never fire: decideShrinkPre's slack branch floors
	// newCap at the current capacity, and decideColdTail's targetCap floors at
	// capacity == size, so both no-op. Skip all window bookkeeping while at the
	// floor. State is already clean here — a prior shrink-to-floor reset
	// shrinkWindowGets and deepestTouched, and a freshly constructed cache starts
	// at the floor with zero-valued state — so a fresh window simply starts on the
	// next Get once a Put grows the cache back above the floor. The cooldown need
	// not tick here: a grow resets it (maybeResizeLocked). Growth is driven from
	// the Put/eviction path and is unaffected by this early return.
	if c.capacity.Load() <= c.minCapacity {
		return resizeEvent{}, false
	}

	// One Get elapsed; tick down the post-resize cooldown.
	if c.cooldownGets > 0 {
		c.cooldownGets--
	}

	// Start a window if none is active (first ever, or just completed). The
	// window length is recomputed from current occupancy each time. The current
	// Get's hit was already folded into deepestTouched by get() and is also
	// counted in getsSinceShrinkCheck below; rewind the hit/miss baseline by one
	// so the same Get is folded into hitsW/missesW for the new window.
	if c.shrinkWindowGets == 0 {
		c.shrinkWindowGets = adaptiveWindowLen(int64(c.evictor.Len()), c.minSamples, c.targetHitRate)
		c.getsSinceShrinkCheck = 0
		c.shrinkBaseHits = c.stats.Hits.Load()
		c.shrinkBaseMisses = c.stats.Misses.Load()
		if hit {
			c.shrinkBaseHits--
		} else {
			c.shrinkBaseMisses--
		}
		c.shrinkBaseEvictions = c.stats.Evictions.Load()
		if c.shrinkWindowGets == 0 {
			// Cache too small (n < 2) to evaluate. Clear the boundary so a
			// warm-up touch can't carry into the first real window.
			c.deepestTouched = nil
			return resizeEvent{}, false
		}
	}

	c.getsSinceShrinkCheck++
	if c.getsSinceShrinkCheck < c.shrinkWindowGets {
		return resizeEvent{}, false
	}

	// Window complete. Evaluate unless a recent capacity change is cooling down.
	var event resizeEvent
	var shrank bool
	if c.cooldownGets == 0 {
		if e, ok := c.maybeShrinkLocked(); ok {
			event, shrank = e, true
			// Cool down for a window sized to the new (smaller) capacity.
			c.cooldownGets = adaptiveWindowLen(e.newCap, c.minSamples, c.targetHitRate)
		}
	}

	// End the window: a fresh one (with a recomputed length) starts on the next
	// Get, and the footprint resets so the next window measures cleanly.
	c.shrinkWindowGets = 0
	c.deepestTouched = nil
	return event, shrank
}

// maybeShrinkLocked measures the completed window, consults the shrink policy,
// and applies the result: it evicts up to the bounded number of LRU-tail entries
// and lowers capacity. The O(size) footprint walk (warmCountLocked) is performed
// only when the gates pass, the slack branch does not apply, and there is in fact
// an untouched tail — so gated-out and slack windows stay O(1). Must be called
// under the write lock.
func (c *TagValueSeriesIDCache) maybeShrinkLocked() (resizeEvent, bool) {
	hitsW := c.stats.Hits.Load() - c.shrinkBaseHits
	missesW := c.stats.Misses.Load() - c.shrinkBaseMisses
	evictionsW := c.stats.Evictions.Load() - c.shrinkBaseEvictions

	curCap := c.capacity.Load()
	size := int64(c.evictor.Len())

	newCap, shrink, coldTail := decideShrinkPre(hitsW, missesW, evictionsW, curCap, size, c.minCapacity, c.targetHitRate, c.shrinkConservatism)

	var evicted int64
	if coldTail {
		// The cache is full and the gates passed. Short-circuit the footprint
		// walk when the whole cache was touched this window (the boundary is at
		// the LRU tail) or nothing was touched: there is no cold tail to shed.
		if c.deepestTouched == nil || c.deepestTouched == c.evictor.Back() {
			return resizeEvent{}, false
		}
		var evict int64
		newCap, evict, shrink = decideColdTail(size, c.warmCountLocked(), c.minCapacity)
		if !shrink {
			return resizeEvent{}, false
		}
		for evicted < evict && c.evictLRULocked() {
			evicted++
		}
		c.stats.Size.Store(int64(c.evictor.Len()))
		c.stats.ShrinkEvictions.Add(evicted)
		// Derive the new capacity from what was actually evicted so the
		// invariant size <= capacity holds even if the list emptied early.
		newCap = size - evicted
	} else if !shrink {
		return resizeEvent{}, false
	}

	c.capacity.Store(newCap)

	// Capacity shrank: reset the grow policy's per-window state so the next
	// forced eviction measures a fresh post-shrink turnover. Without this,
	// the stale evictionsSinceCheck (potentially near or above the new
	// smaller capacity) could immediately fire maybeResizeLocked against
	// pre-shrink hit/miss baselines, mixing pre- and post-shrink samples.
	c.evictionsSinceCheck = 0
	c.lastHits = c.stats.Hits.Load()
	c.lastMisses = c.stats.Misses.Load()

	gets := hitsW + missesW
	var rate float64
	if gets > 0 {
		rate = float64(hitsW) / float64(gets)
	}
	return resizeEvent{oldCap: curCap, newCap: newCap, gets: gets, evicted: evicted, rate: rate}, true
}

// warmCountLocked returns the number of entries in the warm front-prefix
// (Front() through deepestTouched, inclusive) = the distinct entries touched
// this window. Must be called under the write lock.
func (c *TagValueSeriesIDCache) warmCountLocked() int64 {
	if c.deepestTouched == nil {
		return 0
	}
	var n int64
	for e := c.evictor.Front(); e != nil; e = e.Next() {
		n++
		if e == c.deepestTouched {
			break
		}
	}
	return n
}

// adaptiveWindowLen returns how many Gets to observe before making an adaptive
// sizing decision over a population of n items — either a shrink evaluation
// (n = current occupancy) or the quiet period after a resize (n = the new
// capacity). It is sized so that under ~uniform random access over n items the
// expected fraction left untouched is <= (1-target), so an item still untouched
// after a full window is genuinely cold, not merely unsampled. The result is
// floored at n (always sample at least as many times as there are items) and at
// minWindow (a noise floor for tiny caches), so it is ~max(3n at target 0.95, n,
// minWindow). Returns 0 when n < 2 (too small to evaluate). target must be in
// (0, 1) (enforced by config validation and the constructor); as defense, any
// out-of-range target — including NaN or a value rounding to 0 or 1 — is rejected
// up front and returns MaxInt64 ("effectively never"), so the logarithms below
// cannot produce NaN, ±Inf, or an overflowing window.
//
// Derivation. Model each Get as an independent draw landing uniformly on one of
// the n items. The chance a given item is not the one accessed by a single Get
// is (n-1)/n, so after k independent Gets the chance it is still untouched is
// ((n-1)/n)^k. By linearity of expectation that is also the expected fraction of
// all n items left untouched after k Gets. We want a window long enough that
// this expected untouched fraction is at most (1-target):
//
//	((n-1)/n)^k <= 1 - target
//
// Take natural logs. Since (n-1)/n < 1, ln((n-1)/n) is negative, so dividing by
// it flips the inequality:
//
//	k * ln((n-1)/n) <= ln(1 - target)
//	k >= ln(1 - target) / ln((n-1)/n)
//
// Both logs are negative, so the ratio is positive; take the ceiling for a whole
// number of Gets. For large n, ln((n-1)/n) = ln(1 - 1/n) ~= -1/n, so the window
// is ~ n * ln(1/(1-target)): linear in n with a multiplier set only by target
// (e.g. ln(20) ~= 3.0 at target 0.95, hence ~3n; ln(2) ~= 0.69 at target 0.5,
// which is below n and is why the result is floored at n).
func adaptiveWindowLen(n, minWindow int64, target float64) int64 {
	// Defensive argument range check: callers pass target in (0, 1) (enforced by
	// config validation and the constructor). Reject anything else up front —
	// including NaN and values rounding to 0 or 1 — so the logarithms below cannot
	// yield NaN, ±Inf, or an overflowing/negative window; an out-of-range target
	// disables sizing decisions, the safe choice for a bad input.
	if !(target > 0 && target < 1) {
		return math.MaxInt64
	}
	if n < 2 {
		return 0
	}
	// window = ceil( ln(1-target) / ln((n-1)/n) ); both logs are finite and negative.
	// The denominator is ln(1 - 1/n); compute it with Log1p rather than
	// Log(float64(n-1)/float64(n)) so the tiny 1/n term is not lost when the ratio
	// rounds to a double near 1.0 (catastrophic for very large n). Log1p is also
	// implemented in portable Go, so the result is identical on amd64 and arm64.
	wf := math.Ceil(math.Log(1-target) / math.Log1p(-1.0/float64(n)))
	if wf >= float64(math.MaxInt64) { // huge finite window (target very close to 1)
		return math.MaxInt64
	}
	w := int64(wf)
	if w < n { // sample at least as many times as there are items
		w = n
	}
	if w < minWindow {
		w = minWindow
	}
	return w
}

// atTargetEvictionGateLimit returns the shrink eviction-gate threshold using a
// lower confidence bound on the at-target eviction count distribution.
//
// Derivation. On a full cache a miss generally produces one eviction, so over a
// window of m Gets at hit rate T the eviction count is approximately
// Binomial(m, 1-T), with mean μ = m·(1-T) and variance σ² = m·T·(1-T). The gate
// admits shrink only when the observed evictions sit below μ − z·σ, the lower
// edge of the at-target distribution's z-sigma confidence band. Tying the
// threshold to the distribution itself yields a scale-adaptive margin: as the
// window m grows, σ grows like √m while μ grows like m, so the relative slack
// the gate demands shrinks naturally — a 1M-Get window admits a smaller relative
// margin than a 100-Get one because the rate estimate is correspondingly
// tighter. Higher z demands the cache outperform target by more standard
// deviations (more conservative); z = 0 admits at the median (μ). Floored at 1
// so a single stray eviction never blocks shrink. Out-of-range inputs (gets<1,
// target outside (0,1), z not in [0, +Inf)) return 1 (fail-safe: gates on any
// eviction).
//
// Note: the binomial assumes independent Bernoulli misses; real cache access
// is correlated (locality, Zipfian skew), so the true distribution is wider
// than σ here, and the bound is optimistic in that sense. Operators concerned
// about workload skew can compensate by raising z.
func atTargetEvictionGateLimit(gets int64, target, z float64) int64 {
	if gets < 1 || !(target > 0 && target < 1) || !(z >= 0 && z < math.Inf(1)) {
		return 1
	}
	m := float64(gets)
	mu := m * (1 - target)
	sigma := math.Sqrt(m * target * (1 - target))
	limit := mu - z*sigma
	// Clamp in float space before converting. Converting an out-of-range float to
	// int64 is implementation-defined in Go and differs by architecture: amd64
	// (CVTTSD2SI) yields MinInt64 for any out-of-range value, but arm64 (FCVTZS)
	// saturates, so a large-positive overflow becomes MaxInt64 — which would slip
	// past a post-conversion `< 1` check and invert the fail-safe. Guarding the
	// float first (the `!(limit >= 1)` form also rejects NaN and -Inf) keeps the value
	// fed to int64() provably in [1, 2^63), so the result is identical on both.
	if !(limit >= 1) {
		return 1
	} else if limit >= float64(math.MaxInt64) { // out-of-range high (only at unreachable gets)
		return math.MaxInt64
	} else {
		return int64(limit)
	}
}

// decideShrinkPre evaluates everything that does not require the access footprint:
// the gates and the slack branch. It returns the slack result (newCap/shrink), or
// coldTail=true to signal that the cache is full and the caller must measure the
// warm footprint and consult decideColdTail. When coldTail is true, newCap/shrink
// are not meaningful. The eviction gate threshold is produced by
// atTargetEvictionGateLimit (see its doc for derivation). The hit-rate gate is
// kept for defense.
func decideShrinkPre(hitsW, missesW, evictionsW, capacity, size, floor int64, target, conservatism float64) (newCap int64, shrink, coldTail bool) {
	gets := hitsW + missesW
	if gets <= 0 {
		return capacity, false, false
	}
	if evictionsW > atTargetEvictionGateLimit(gets, target, conservatism) {
		return capacity, false, false
	}
	if float64(hitsW)/float64(gets) < target {
		return capacity, false, false
	}

	// Slack branch: capacity above occupancy. Drop the unused headroom; no
	// eviction is needed because occupancy already fits the smaller capacity.
	if capacity > size {
		newCap = size
		if newCap < floor {
			newCap = floor
		}
		if newCap >= capacity {
			return capacity, false, false
		}
		return newCap, true, false
	}

	// Cache is full (capacity == size): the decision needs the footprint.
	return capacity, false, true
}

// decideColdTail trims a full cache to the warm footprint, clamped at floor
// and bounded per event so the write lock is not held long: both to half the
// cache (proportional) and to maxShrinkEvictPerEvent (absolute, so a single
// shrink on a multi-million-entry cache cannot park readers for an unbounded
// time). Decay continues over later windows. Returns the new capacity, the
// number of LRU-tail entries to evict to reach it, and whether a shrink
// should occur.
func decideColdTail(size, warmCount, floor int64) (newCap, evict int64, shrink bool) {
	targetCap := warmCount
	if targetCap < floor {
		targetCap = floor
	}
	if targetCap >= size {
		return size, 0, false // nothing cold to shed
	}
	evict = size - targetCap
	if maxEvict := size / 2; evict > maxEvict {
		evict = maxEvict
	}
	if evict > maxShrinkEvictPerEvent {
		evict = maxShrinkEvictPerEvent
	}
	return size - evict, evict, true
}

// seriesIDCacheElement is an item stored within a cache.
type seriesIDCacheElement struct {
	name        string
	key         string
	value       string
	SeriesIDSet *tsdb.SeriesIDSet
}
