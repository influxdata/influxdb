package tsi1

import (
	"container/list"
	"sync"

	"github.com/influxdata/influxdb/tsdb"
	"github.com/prometheus/client_golang/prometheus"
)

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
	sync.RWMutex
	cache   map[string]map[string]map[string]*list.Element
	evictor *list.List

	tracker  *cacheTracker
	capacity uint64
}

// NewTagValueSeriesIDCache returns a TagValueSeriesIDCache with capacity c.
func NewTagValueSeriesIDCache(c uint64) *TagValueSeriesIDCache {
	return &TagValueSeriesIDCache{
		cache:    map[string]map[string]map[string]*list.Element{},
		evictor:  list.New(),
		tracker:  newCacheTracker(newCacheMetrics(nil), nil),
		capacity: c,
	}
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
				c.tracker.IncGetHit()
				c.evictor.MoveToFront(ele) // This now becomes most recently used.
				return ele.Value.(*seriesIDCacheElement).SeriesIDSet
			}
		}
	}
	c.tracker.IncGetMiss()
	return nil
}

// exists returns true if the an item exists for the tuple {name, key, value}.
func (c *TagValueSeriesIDCache) exists(name, key, value []byte) bool {
	if mmap, ok := c.cache[string(name)]; ok {
		if tkmap, ok := mmap[string(key)]; ok {
			_, ok := tkmap[string(value)]
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
func (c *TagValueSeriesIDCache) addToSet(name, key, value []byte, x tsdb.SeriesID) {
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
	// Check under the write lock if the relevant item is now in the cache.
	if c.exists(name, key, value) {
		c.Unlock()
		c.tracker.IncPutHit()
		return
	}
	defer c.Unlock()

	// Create list item, and add to the front of the eviction list.
	listElement := c.evictor.PushFront(&seriesIDCacheElement{
		name:        name,
		key:         key,
		value:       value,
		SeriesIDSet: ss,
	})

	// Add the listElement to the set of items.
	if mmap, ok := c.cache[string(name)]; ok {
		if tkmap, ok := mmap[string(key)]; ok {
			if _, ok := tkmap[string(value)]; ok {
				goto EVICT
			}

			// Add the set to the map
			tkmap[string(value)] = listElement
			goto EVICT
		}

		// No series set map for the tag key - first tag value for the tag key.
		mmap[string(key)] = map[string]*list.Element{string(value): listElement}
		goto EVICT
	}

	// No map for the measurement - first tag key for the measurment.
	c.cache[string(name)] = map[string]map[string]*list.Element{
		string(key): {string(value): listElement},
	}

EVICT:
	c.checkEviction()
	c.tracker.IncPutMiss()
}

// Delete removes x from the tuple {name, key, value} if it exists.
// This method takes a lock on the underlying SeriesIDSet.
func (c *TagValueSeriesIDCache) Delete(name, key, value []byte, x tsdb.SeriesID) {
	c.Lock()
	c.delete(name, key, value, x)
	c.Unlock()
}

// DeleteMeasurement removes all cached entries for the provided measurement name.
func (c *TagValueSeriesIDCache) DeleteMeasurement(name []byte) {
	c.Lock()
	delete(c.cache, string(name))
	c.Unlock()
}

// delete removes x from the tuple {name, key, value} if it exists.
func (c *TagValueSeriesIDCache) delete(name, key, value []byte, x tsdb.SeriesID) {
	if mmap, ok := c.cache[string(name)]; ok {
		if tkmap, ok := mmap[string(key)]; ok {
			if ele, ok := tkmap[string(value)]; ok {
				if ss := ele.Value.(*seriesIDCacheElement).SeriesIDSet; ss != nil {
					ele.Value.(*seriesIDCacheElement).SeriesIDSet.Remove(x)
					c.tracker.IncDeletesHit()
					return
				}
			}
		}
	}
	c.tracker.IncDeletesMiss()
}

// checkEviction checks if the cache is too big, and evicts the least recently used
// item if it is.
func (c *TagValueSeriesIDCache) checkEviction() {
	l := uint64(c.evictor.Len())
	c.tracker.SetSize(l)
	if l <= c.capacity {
		return
	}

	e := c.evictor.Back() // Least recently used item.
	listElement := e.Value.(*seriesIDCacheElement)
	name := listElement.name
	key := listElement.key
	value := listElement.value

	c.evictor.Remove(e)                                       // Remove from evictor
	delete(c.cache[string(name)][string(key)], string(value)) // Remove from hashmap of items.

	// Check if there are no more tag values for the tag key.
	if len(c.cache[string(name)][string(key)]) == 0 {
		delete(c.cache[string(name)], string(key))
	}

	// Check there are no more tag keys for the measurement.
	if len(c.cache[string(name)]) == 0 {
		delete(c.cache, string(name))
	}
	c.tracker.IncEvictions()
}

func (c *TagValueSeriesIDCache) PrometheusCollectors() []prometheus.Collector {
	var collectors []prometheus.Collector
	collectors = append(collectors, c.tracker.metrics.PrometheusCollectors()...)
	return collectors
}

// seriesIDCacheElement is an item stored within a cache.
type seriesIDCacheElement struct {
	name        []byte
	key         []byte
	value       []byte
	SeriesIDSet *tsdb.SeriesIDSet
}

type cacheTracker struct {
	metrics *cacheMetrics
	labels  prometheus.Labels
	enabled bool
}

func newCacheTracker(metrics *cacheMetrics, defaultLabels prometheus.Labels) *cacheTracker {
	return &cacheTracker{metrics: metrics, labels: defaultLabels, enabled: true}
}

// Labels returns a copy of labels for use with index cache metrics.
func (t *cacheTracker) Labels() prometheus.Labels {
	l := make(map[string]string, len(t.labels))
	for k, v := range t.labels {
		l[k] = v
	}
	return l
}

func (t *cacheTracker) SetSize(sz uint64) {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	t.metrics.Size.With(labels).Set(float64(sz))
}

func (t *cacheTracker) incGet(status string) {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	labels["status"] = status
	t.metrics.Gets.With(labels).Inc()
}

func (t *cacheTracker) IncGetHit()  { t.incGet("hit") }
func (t *cacheTracker) IncGetMiss() { t.incGet("miss") }

func (t *cacheTracker) incPut(status string) {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	labels["status"] = status
	t.metrics.Puts.With(labels).Inc()
}

func (t *cacheTracker) IncPutHit()  { t.incPut("hit") }
func (t *cacheTracker) IncPutMiss() { t.incPut("miss") }

func (t *cacheTracker) incDeletes(status string) {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	labels["status"] = status
	t.metrics.Deletes.With(labels).Inc()
}

func (t *cacheTracker) IncDeletesHit()  { t.incDeletes("hit") }
func (t *cacheTracker) IncDeletesMiss() { t.incDeletes("miss") }

func (t *cacheTracker) IncEvictions() {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	t.metrics.Evictions.With(labels).Inc()
}
