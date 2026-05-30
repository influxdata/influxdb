package tsi1

import (
	"container/list"
	"fmt"
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
// TagValueSeriesIDCache. Fields are read and written with sync/atomic so that
// Statistics can be sampled without acquiring the cache lock.
//
// Evictions counts entries forced out under write pressure (a Put on a full
// cache); ShrinkEvictions counts entries shed by the adaptive shrink policy.
// They are tracked separately because only Evictions is the binomial signal
// the shrink eviction-gate is derived against (Bernoulli "miss on full cache
// → forced eviction"), and operators often want to distinguish "the cache is
// under pressure" from "the cache is voluntarily releasing memory."
type TagValueSeriesIDCacheStatistics struct {
	Hits            int64
	Misses          int64
	Evictions       int64
	ShrinkEvictions int64
	Size            int64
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
	// Due to a bug in atomic, 64-bit words accessed with sync/atomic must be
	// 64-bit aligned, and the first word of an allocated struct is the only
	// position guaranteed to be 8-byte aligned on 32-bit platforms. Keep all
	// atomically-accessed int64 fields (capacity and the stats counters)
	// contiguous at the top of the struct, ahead of every other field.
	// See: https://golang.org/pkg/sync/atomic/#pkg-note-BUG

	// capacity is the current cache capacity. Read with atomic.LoadInt64
	// (lockless reads from Statistics); written with atomic.StoreInt64 under
	// the cache write lock when the adaptive policy grows it.
	capacity int64

	// stats holds the atomically-accessed counters. Its int64 fields stay
	// 8-byte aligned because capacity above is exactly one 64-bit word.
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
	return &TagValueSeriesIDCache{
		cache:    map[string]map[string]map[string]*list.Element{},
		evictor:  list.New(),
		capacity: int64(c),
		logger:   zap.NewNop(),
	}
}

// NewAdaptiveTagValueSeriesIDCache returns a TagValueSeriesIDCache that sizes
// itself between initial and max as the workload changes; capacity is not a
// fixed high-water mark and callers must not rely on it staying at any value.
//
// It grows by doubling toward max when the observed Get hit rate falls below
// target while under eviction pressure (driven from the eviction path: after
// every `capacity` evictions it samples the windowed hit rate and grows once if
// the policy fires). It also shrinks: driven from the read path, after a
// self-tuning window of Gets, a cache that is quiet (no evictions) and serving
// at/above target trims capacity toward its observed working set, shedding only
// least-recently-used entries left untouched over the window, never below
// initial. A cooldown between resizes prevents grow/shrink oscillation.
//
// minSamples is a floor on the number of Get operations observed in a window
// before the grow policy is allowed to act; below this floor the rate is treated
// as too noisy to react to. shrinkConservatism (>= 0.0) is the number of standard
// deviations below the at-target eviction mean at which the shrink eviction gate
// sits; see the doc on SeriesIDSetCacheShrinkConservatism in tsdb.Config. Panics
// on invalid arguments: initial must be > 0, max > initial (an adaptive cache
// that cannot grow is a misconfiguration — use NewTagValueSeriesIDCache for a
// fixed cache), target in (0, 1) (1.0 is unachievable and is rejected by config
// validation; NaN is rejected), shrinkConservatism finite and >= 0.0 (NaN/±Inf
// rejected), minSamples >= 0.
func NewAdaptiveTagValueSeriesIDCache(initial, max int, target, shrinkConservatism float64, minSamples int, logger *zap.Logger) *TagValueSeriesIDCache {
	if initial <= 0 {
		panic(fmt.Sprintf("NewAdaptiveTagValueSeriesIDCache: initial must be > 0, got %d", initial))
	}
	if max <= initial {
		panic(fmt.Sprintf("NewAdaptiveTagValueSeriesIDCache: max (%d) must be > initial (%d)", max, initial))
	}
	// Positive range test so NaN (for which both `<` and `>` are false) is
	// rejected rather than slipping through.
	if !(target > 0 && target < 1) {
		panic(fmt.Sprintf("NewAdaptiveTagValueSeriesIDCache: target must be in (0, 1), got %v", target))
	}
	// Same positive-form trick rejects NaN and ±Inf for the conservatism.
	if !(shrinkConservatism >= 0 && shrinkConservatism < math.Inf(1)) {
		panic(fmt.Sprintf("NewAdaptiveTagValueSeriesIDCache: shrinkConservatism must be a finite value >= 0, got %v", shrinkConservatism))
	}
	if minSamples < 0 {
		panic(fmt.Sprintf("NewAdaptiveTagValueSeriesIDCache: minSamples must be >= 0, got %d", minSamples))
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &TagValueSeriesIDCache{
		cache:              map[string]map[string]map[string]*list.Element{},
		evictor:            list.New(),
		capacity:           int64(initial),
		minCapacity:        int64(initial),
		maxCapacity:        int64(max),
		targetHitRate:      target,
		shrinkConservatism: shrinkConservatism,
		minSamples:         int64(minSamples),
		logger:             logger,
	}
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
			statTagValueCacheHit:            atomic.LoadInt64(&c.stats.Hits),
			statTagValueCacheMiss:           atomic.LoadInt64(&c.stats.Misses),
			statTagValueCacheEviction:       atomic.LoadInt64(&c.stats.Evictions),
			statTagValueCacheShrinkEviction: atomic.LoadInt64(&c.stats.ShrinkEvictions),
			statTagValueCacheSize:           atomic.LoadInt64(&c.stats.Size),
			statTagValueCacheCapacity:       atomic.LoadInt64(&c.capacity),
		},
	}}
}

// Get returns the SeriesIDSet associated with the {name, key, value} tuple if it
// exists.
func (c *TagValueSeriesIDCache) Get(name, key, value []byte) *tsdb.SeriesIDSet {
	c.Lock()
	ss, hit := c.get(name, key, value)
	// Drive the adaptive shrink check from the read path: the grow check is
	// eviction-driven and is blind when the cache is quiet (exactly when a
	// shrink may be warranted). Log after releasing the lock, like Put.
	event, shrank := c.checkShrink(hit)
	c.Unlock()
	if shrank {
		c.logShrink(event)
	}
	return ss
}

func (c *TagValueSeriesIDCache) get(name, key, value []byte) (*tsdb.SeriesIDSet, bool) {
	if mmap, ok := c.cache[string(name)]; ok {
		if tkmap, ok := mmap[string(key)]; ok {
			if ele, ok := tkmap[string(value)]; ok {
				// Track the LRU access footprint for the shrink policy before
				// MoveToFront moves the element (the rule reads ele.Prev()).
				if c.maxCapacity != 0 {
					c.trackTouchLocked(ele)
				}
				c.evictor.MoveToFront(ele) // This now becomes most recently used.
				atomic.AddInt64(&c.stats.Hits, 1)
				return ele.Value.(*seriesIDCacheElement).SeriesIDSet, true
			}
		}
	}
	atomic.AddInt64(&c.stats.Misses, 1)
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
	c.Lock()
	// Convert once; the same string backing array is shared between
	// the cache element (used during eviction) and the map keys.
	nameStr := string(name)
	keyStr := string(key)
	valueStr := string(value)
	// Check under the write lock if the relevant item is now in the cache.
	if c.exists(nameStr, keyStr, valueStr) {
		c.Unlock()
		return
	}

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
	atomic.AddInt64(&c.stats.Size, 1)

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
	event, grew := c.checkEviction()
	c.Unlock()
	if grew {
		c.logResize(event)
	}
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
// the bool is true iff a resize occurred.
func (c *TagValueSeriesIDCache) checkEviction() (resizeEvent, bool) {
	if int64(c.evictor.Len()) <= atomic.LoadInt64(&c.capacity) {
		return resizeEvent{}, false
	}
	if !c.evictLRULocked() {
		return resizeEvent{}, false
	}
	atomic.AddInt64(&c.stats.Evictions, 1)

	// Adaptive sizing: skip entirely when disabled to keep the hot path
	// free of per-eviction tracking work.
	if c.maxCapacity == 0 {
		return resizeEvent{}, false
	}

	c.evictionsSinceCheck++
	if c.evictionsSinceCheck < atomic.LoadInt64(&c.capacity) {
		return resizeEvent{}, false
	}
	c.evictionsSinceCheck = 0
	return c.maybeResizeLocked()
}

// evictLRULocked removes the least-recently-used entry (the back of the
// evictor list) from the eviction list, the cache maps, and the size counter.
// Returns false if the cache is empty. Does NOT bump Evictions or
// ShrinkEvictions — callers do that with the appropriate counter, since they
// know whether the eviction was forced (Put under pressure) or voluntary
// (shrink trim). Must be called under the write lock.
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
	atomic.AddInt64(&c.stats.Size, -1)

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
	hits := atomic.LoadInt64(&c.stats.Hits)
	misses := atomic.LoadInt64(&c.stats.Misses)
	curCap := atomic.LoadInt64(&c.capacity)

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

	atomic.StoreInt64(&c.capacity, newCap)
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

// logResize emits a single INFO log line describing a capacity-growth
// event. Called by Put after releasing the cache write lock. Reads
// c.maxCapacity and c.targetHitRate directly — both are set at
// construction and never mutated.
func (c *TagValueSeriesIDCache) logResize(e resizeEvent) {
	c.logger.Info(logMsgCacheCapacityIncreased,
		zap.Int64("old_capacity", e.oldCap),
		zap.Int64("new_capacity", e.newCap),
		zap.Int64("max_capacity", c.maxCapacity),
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
		c.shrinkBaseHits = atomic.LoadInt64(&c.stats.Hits)
		c.shrinkBaseMisses = atomic.LoadInt64(&c.stats.Misses)
		if hit {
			c.shrinkBaseHits--
		} else {
			c.shrinkBaseMisses--
		}
		c.shrinkBaseEvictions = atomic.LoadInt64(&c.stats.Evictions)
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
	hitsW := atomic.LoadInt64(&c.stats.Hits) - c.shrinkBaseHits
	missesW := atomic.LoadInt64(&c.stats.Misses) - c.shrinkBaseMisses
	evictionsW := atomic.LoadInt64(&c.stats.Evictions) - c.shrinkBaseEvictions

	curCap := atomic.LoadInt64(&c.capacity)
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
		atomic.AddInt64(&c.stats.ShrinkEvictions, evicted)
		// Derive the new capacity from what was actually evicted so the
		// invariant size <= capacity holds even if the list emptied early.
		newCap = size - evicted
	} else if !shrink {
		return resizeEvent{}, false
	}

	atomic.StoreInt64(&c.capacity, newCap)

	// Capacity shrank: reset the grow policy's per-window state so the next
	// forced eviction measures a fresh post-shrink turnover. Without this,
	// the stale evictionsSinceCheck (potentially near or above the new
	// smaller capacity) could immediately fire maybeResizeLocked against
	// pre-shrink hit/miss baselines, mixing pre- and post-shrink samples.
	c.evictionsSinceCheck = 0
	c.lastHits = atomic.LoadInt64(&c.stats.Hits)
	c.lastMisses = atomic.LoadInt64(&c.stats.Misses)

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

// logShrink emits a single INFO log line describing a capacity-shrink event.
// Called by Get after releasing the cache write lock.
func (c *TagValueSeriesIDCache) logShrink(e resizeEvent) {
	c.logger.Info(logMsgCacheCapacityDecreased,
		zap.Int64("old_capacity", e.oldCap),
		zap.Int64("new_capacity", e.newCap),
		zap.Int64("min_capacity", c.minCapacity),
		zap.Int64("evicted", e.evicted),
		zap.Float64("hit_rate", e.rate),
		zap.Int64("gets_window", e.gets),
	)
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
	wf := math.Ceil(math.Log(1-target) / math.Log(float64(n-1)/float64(n)))
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
	limit := int64(mu - z*sigma)
	if limit < 1 {
		limit = 1
	}
	return limit
}

// decideShrink implements the full capacity-decay policy as a pure function of
// the window's counters, the current occupancy, and the observed access
// footprint. It mirrors decideResize so the policy can be exercised with integer
// literals. It composes decideShrinkPre and decideColdTail; production code calls
// those two directly so the O(size) footprint walk that produces warmCount is
// only performed when the cold-tail branch is actually reached (see
// maybeShrinkLocked).
//
// Gates (all required): at least one read; window evictions at or below the
// scaled at-target expectation (see atTargetEvictionGateLimit); and a windowed
// hit rate >= target (the cache is serving its working set well). When gated
// through, one of two branches fires:
//   - slack: capacity exceeds occupancy -> drop the unused headroom (no eviction).
//   - cold-tail: cache is full -> shed the LRU tail that went untouched this
//     window, down to the warm footprint (warmCount), clamped at floor, and
//     bounded to half the cache per event so the write lock is not held long.
//
// The evicted entries are always within the untouched cold tail, so warm entries
// are never shed.
func decideShrink(hitsW, missesW, evictionsW, capacity, size, warmCount, floor int64, target, conservatism float64) (newCap, evict int64, shrink bool) {
	newCap, shrink, coldTail := decideShrinkPre(hitsW, missesW, evictionsW, capacity, size, floor, target, conservatism)
	if coldTail {
		return decideColdTail(size, warmCount, floor)
	}
	return newCap, 0, shrink
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
