package tsi1

import (
	"container/list"
	"fmt"
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

// Statistic field names for the tag value series ID cache.
const (
	statTagValueCacheHit      = "hit"
	statTagValueCacheMiss     = "miss"
	statTagValueCacheEviction = "eviction"
	statTagValueCacheSize     = "size"
	statTagValueCacheCapacity = "capacity"
)

// TagValueSeriesIDCacheStatistics holds counters describing the behavior of a
// TagValueSeriesIDCache. Fields are read and written with sync/atomic so that
// Statistics can be sampled without acquiring the cache lock.
type TagValueSeriesIDCacheStatistics struct {
	Hits      int64
	Misses    int64
	Evictions int64
	Size      int64
}

// resizeEvent describes a single capacity-growth event. Snapshot fields are
// captured under the cache write lock so the log line can be emitted by the
// caller after releasing the lock.
type resizeEvent struct {
	oldCap, newCap, gets int64
	rate                 float64
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
	minSamples          int64
	logger              *zap.Logger
	evictionsSinceCheck int64
	lastHits            int64
	lastMisses          int64
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

// NewAdaptiveTagValueSeriesIDCache returns a TagValueSeriesIDCache that
// starts at the given initial capacity and grows (doubling) up to max as
// the observed Get hit rate falls below target. Resizes are driven from
// the eviction path: after every `capacity` evictions, the cache samples
// its windowed hit rate and grows once if the policy fires. The cache
// never shrinks. minSamples is a floor on the number of Get operations
// observed in a window before the policy is allowed to act; below this
// floor the rate is treated as too noisy to react to. Panics on invalid
// arguments: initial must be > 0, max > initial (an adaptive cache that
// cannot grow is a misconfiguration — use NewTagValueSeriesIDCache for a
// fixed cache), target in (0, 1], minSamples >= 0.
func NewAdaptiveTagValueSeriesIDCache(initial, max int, target float64, minSamples int, logger *zap.Logger) *TagValueSeriesIDCache {
	if initial <= 0 {
		panic(fmt.Sprintf("NewAdaptiveTagValueSeriesIDCache: initial must be > 0, got %d", initial))
	}
	if max <= initial {
		panic(fmt.Sprintf("NewAdaptiveTagValueSeriesIDCache: max (%d) must be > initial (%d)", max, initial))
	}
	if target <= 0 || target > 1 {
		panic(fmt.Sprintf("NewAdaptiveTagValueSeriesIDCache: target must be in (0, 1], got %v", target))
	}
	if minSamples < 0 {
		panic(fmt.Sprintf("NewAdaptiveTagValueSeriesIDCache: minSamples must be >= 0, got %d", minSamples))
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &TagValueSeriesIDCache{
		cache:         map[string]map[string]map[string]*list.Element{},
		evictor:       list.New(),
		capacity:      int64(initial),
		maxCapacity:   int64(max),
		targetHitRate: target,
		minSamples:    int64(minSamples),
		logger:        logger,
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
			statTagValueCacheHit:      atomic.LoadInt64(&c.stats.Hits),
			statTagValueCacheMiss:     atomic.LoadInt64(&c.stats.Misses),
			statTagValueCacheEviction: atomic.LoadInt64(&c.stats.Evictions),
			statTagValueCacheSize:     atomic.LoadInt64(&c.stats.Size),
			statTagValueCacheCapacity: atomic.LoadInt64(&c.capacity),
		},
	}}
}

// Get returns the SeriesIDSet associated with the {name, key, value} tuple if it
// exists.
func (c *TagValueSeriesIDCache) Get(name, key, value []byte) *tsdb.SeriesIDSet {
	c.Lock()
	defer c.Unlock()
	return c.get(name, key, value)
}

func (c *TagValueSeriesIDCache) get(name, key, value []byte) *tsdb.SeriesIDSet {
	if mmap, ok := c.cache[string(name)]; ok {
		if tkmap, ok := mmap[string(key)]; ok {
			if ele, ok := tkmap[string(value)]; ok {
				c.evictor.MoveToFront(ele) // This now becomes most recently used.
				atomic.AddInt64(&c.stats.Hits, 1)
				return ele.Value.(*seriesIDCacheElement).SeriesIDSet
			}
		}
	}
	atomic.AddInt64(&c.stats.Misses, 1)
	return nil
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
// recently used item if it is. When adaptive sizing is enabled and the
// cache has just turned over (i.e. accumulated `capacity` evictions since
// the last policy firing), it samples the windowed hit rate and may grow
// the capacity. The optional resizeEvent return carries the snapshot the
// caller should use to emit a log line after releasing the write lock;
// the bool is true iff a resize occurred.
func (c *TagValueSeriesIDCache) checkEviction() (resizeEvent, bool) {
	if int64(c.evictor.Len()) <= atomic.LoadInt64(&c.capacity) {
		return resizeEvent{}, false
	}

	e := c.evictor.Back() // Least recently used item.
	listElement := e.Value.(*seriesIDCacheElement)
	name := listElement.name
	key := listElement.key
	value := listElement.value

	c.evictor.Remove(e)                                       // Remove from evictor
	delete(c.cache[string(name)][string(key)], string(value)) // Remove from hashmap of items.
	atomic.AddInt64(&c.stats.Evictions, 1)
	atomic.AddInt64(&c.stats.Size, -1)

	// Check if there are no more tag values for the tag key.
	if len(c.cache[string(name)][string(key)]) == 0 {
		delete(c.cache[string(name)], string(key))
	}

	// Check there are no more tag keys for the measurement.
	if len(c.cache[string(name)]) == 0 {
		delete(c.cache, string(name))
	}

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

	// Floor on sample size: too few reads since the last firing makes
	// any rate observation noisy. Also covers pure-write workloads
	// (gets == 0).
	if gets < minSamples {
		return capacity, gets, rate, false
	}

	if rate >= target {
		return capacity, gets, rate, false
	}
	if capacity >= maxCapacity {
		return capacity, gets, rate, false
	}

	newCap = capacity * 2
	if newCap > maxCapacity {
		newCap = maxCapacity
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

// seriesIDCacheElement is an item stored within a cache.
type seriesIDCacheElement struct {
	name        string
	key         string
	value       string
	SeriesIDSet *tsdb.SeriesIDSet
}
