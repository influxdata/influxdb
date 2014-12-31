package influxdb

import (
	"regexp"
	"sort"
	"strings"
	"sync"
)

// Index is the in memory index structure for answering queries about measurements, tags
// and series within a database.
type Index struct {
	mu                  sync.RWMutex
	measurements        map[string]*Measurement // measurement name to object and index
	seriesToMeasurement map[uint32]*Measurement // map series id to its measurement
	series              map[uint32]*Series      // map series id to the Series object
	names               []string                // sorted list of the measurement names
}

func NewIndex() *Index {
	return &Index{
		measurements:        make(map[string]*Measurement),
		seriesToMeasurement: make(map[uint32]*Measurement),
		series:              make(map[uint32]*Series),
		names:               make([]string, 0),
	}
}

// Filter represents a tag filter when looking up other tags or measurements.
type Filter struct {
	Not   bool
	Key   string
	Value string
	Regex *regexp.Regexp
}

type Filters []*Filter

// SeriesIDs is a convenience type for sorting, checking equality, and doing union and
// intersection of collections of series ids.
type SeriesIDs []uint32

func (p SeriesIDs) Len() int           { return len(p) }
func (p SeriesIDs) Less(i, j int) bool { return p[i] < p[j] }
func (p SeriesIDs) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// Equals assumes that both are sorted. This is by design, no touchy!
func (a SeriesIDs) Equals(seriesIDs SeriesIDs) bool {
	if len(a) != len(seriesIDs) {
		return false
	}
	for i, s := range seriesIDs {
		if a[i] != s {
			return false
		}
	}
	return true
}

// Intersect returns a new collection of series ids in sorted order that is the intersection of the two.
// The two collections must already be sorted.
func (a SeriesIDs) Intersect(seriesIDs SeriesIDs) SeriesIDs {
	l := a
	r := seriesIDs

	// we want to iterate through the shortest one and stop
	if len(seriesIDs) < len(a) {
		l = seriesIDs
		r = a
	}

	// they're in sorted order so advance the counter as needed.
	// That is, don't run comparisons against lower values that we've already passed
	var i, j int

	ids := make([]uint32, 0, len(l))
	for i < len(l) {
		if l[i] == r[j] {
			ids = append(ids, l[i])
			i += 1
			j += 1
		} else if l[i] < r[j] {
			i += 1
		} else {
			j += 1
		}
	}

	return SeriesIDs(ids)
}

// Union returns a new collection of series ids in sorted order that is the union of the two.
// The two collections must already be sorted.
func (l SeriesIDs) Union(r SeriesIDs) SeriesIDs {
	ids := make([]uint32, 0, len(l)+len(r))
	var i, j int
	for i < len(l) && j < len(r) {
		if l[i] == r[j] {
			ids = append(ids, l[i])
			i += 1
			j += 1
		} else if l[i] < r[j] {
			ids = append(ids, l[i])
			i += 1
		} else {
			ids = append(ids, r[j])
			j += 1
		}
	}

	// now append the remainder
	if i < len(l) {
		ids = append(ids, l[i:]...)
	} else if j < len(r) {
		ids = append(ids, r[j:]...)
	}

	return ids
}

// Reject returns a new collection of series ids in sorted order with the passed in set removed from the original. This is useful for the NOT operator.
// The two collections must already be sorted.
func (l SeriesIDs) Reject(r SeriesIDs) SeriesIDs {
	var i, j int

	ids := make([]uint32, 0, len(l))
	for i < len(l) && j < len(r) {
		if l[i] == r[j] {
			i += 1
			j += 1
		} else if l[i] < r[j] {
			ids = append(ids, l[i])
			i += 1
		} else {
			j += 1
		}
	}

	// append the remainder
	if i < len(l) {
		ids = append(ids, l[i:]...)
	}

	return SeriesIDs(ids)
}

// AddSeries adds the series for the given measurement to the index. Returns false if already present
func (t *Index) AddSeries(name string, s *Series) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	// if there is a measurement for this id, it's already been added
	if t.seriesToMeasurement[s.ID] != nil {
		return false
	}

	// get or create the measurement index and index it globally and in the measurement
	idx := t.createMeasurementIfNotExists(name)

	t.seriesToMeasurement[s.ID] = idx
	t.series[s.ID] = s

	// TODO: add this series to the global tag index

	return idx.addSeries(s)
}

// createMeasurementIfNotExists will either add a measurement object to the index or return the existing one.
func (t *Index) createMeasurementIfNotExists(name string) *Measurement {
	idx := t.measurements[name]
	if idx == nil {
		idx = NewMeasurement(name)
		t.measurements[name] = idx
		t.names = append(t.names, name)
		sort.Strings(t.names)
	}
	return idx
}

// AddField adds a field to the measurement name. Returns false if already present
func (t *Index) AddField(name string, f *Field) bool {
	panic("not implemented")
	return false
}

// SeriesIDs returns an array of series ids for the given measurements and filters to be applied to all.
// Filters are equivalent to and AND operation. If you want to do an OR, get the series IDs for one set,
// then get the series IDs for another set and use the SeriesIDs.Union to combine the two.
func (t *Index) SeriesIDs(names []string, filters Filters) SeriesIDs {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// they want all ids if no filters are specified
	if len(filters) == 0 {
		ids := SeriesIDs(make([]uint32, 0))
		for _, idx := range t.measurements {
			ids = ids.Union(idx.ids)
		}
		return ids
	}

	ids := SeriesIDs(make([]uint32, 0))
	for _, n := range names {
		ids = ids.Union(t.seriesIDsForName(n, filters))
	}

	return ids
}

// TagKeys returns a sorted array of unique tag keys for the given measurements.
func (t *Index) TagKeys(names []string) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if len(names) == 0 {
		names = t.names
	}

	keys := make(map[string]bool)
	for _, n := range names {
		idx := t.measurements[n]
		if idx != nil {
			for k, _ := range idx.seriesByTagKeyValue {
				keys[k] = true
			}
		}
	}

	sortedKeys := make([]string, 0, len(keys))
	for k, _ := range keys {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	return sortedKeys
}

// TagValues returns a map of unique tag values for the given measurements and key with the given filters applied.
// Call .ToSlice() on the result to convert it into a sorted slice of strings.
// Filters are equivalent to and AND operation. If you want to do an OR, get the tag values for one set,
// then get the tag values for another set and do a union of the two.
func (t *Index) TagValues(names []string, key string, filters []*Filter) TagValues {
	t.mu.RLock()
	defer t.mu.RUnlock()

	values := TagValues(make(map[string]bool))

	// see if they just want all the tag values for this key
	if len(filters) == 0 {
		for _, n := range names {
			idx := t.measurements[n]
			if idx != nil {
				values.Union(idx.tagValues(key))
			}
		}
		return values
	}

	// they have filters so just get a set of series ids matching them and then get the tag values from those
	seriesIDs := t.SeriesIDs(names, filters)
	return t.tagValuesForSeries(key, seriesIDs)
}

// tagValuesForSeries will return a TagValues map of all the unique tag values for a collection of series.
func (t *Index) tagValuesForSeries(key string, seriesIDs SeriesIDs) TagValues {
	values := make(map[string]bool)
	for _, id := range seriesIDs {
		s := t.series[id]
		if s == nil {
			continue
		}
		if v, ok := s.Tags[key]; ok {
			values[v] = true
		}
	}
	return TagValues(values)
}

type TagValues map[string]bool

// ToSlice returns a sorted slice of the TagValues
func (t TagValues) ToSlice() []string {
	a := make([]string, 0, len(t))
	for v, _ := range t {
		a = append(a, v)
	}
	sort.Strings(a)
	return a
}

// Union will modify the receiver by merging in the passed in values.
func (l TagValues) Union(r TagValues) {
	for v, _ := range r {
		l[v] = true
	}
}

// Intersect will modify the receiver by keeping only the keys that exist in the passed in values
func (l TagValues) Intersect(r TagValues) {
	for v, _ := range l {
		if _, ok := r[v]; !ok {
			delete(l, v)
		}
	}
}

//seriesIDsForName is the same as SeriesIDs, but for a specific measurement.
func (t *Index) seriesIDsForName(name string, filters Filters) SeriesIDs {
	idx := t.measurements[name]
	if idx == nil {
		return nil
	}

	// process the filters one at a time to get the list of ids they return
	idsPerFilter := make([]SeriesIDs, len(filters), len(filters))
	for i, filter := range filters {
		idsPerFilter[i] = idx.seriesIDs(filter)
	}

	// collapse the set of ids
	allIDs := idsPerFilter[0]
	for i := 1; i < len(filters); i++ {
		allIDs = allIDs.Intersect(idsPerFilter[i])
	}

	return allIDs
}

// MeasurementBySeriesID returns the Measurement that is the parent of the given series id.
func (t *Index) MeasurementBySeriesID(id uint32) *Measurement {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.seriesToMeasurement[id]
}

// MeasurementAndSeries returns the Measurement and the Series for a given measurement name and tag set.
func (t *Index) MeasurementAndSeries(name string, tags map[string]string) (*Measurement, *Series) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	idx := t.measurements[name]
	if idx == nil {
		return nil, nil
	}
	return idx, idx.seriesByTags(tags)
}

// SereiesByID returns the Series that has the given id.
func (t *Index) SeriesByID(id uint32) *Series {
	return t.series[id]
}

// Measurements returns all measurements that match the given filters.
func (t *Index) Measurements(filters []*Filter) []*Measurement {
	measurements := make([]*Measurement, 0, len(t.measurements))
	for _, idx := range t.measurements {
		measurements = append(measurements, idx.measurement)
	}
	return measurements
}

// Names returns all measuremet names in sorted order.
func (t *Index) Names() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.names
}

// DropSeries will clear the index of all references to a series.
func (t *Index) DropSeries(id uint32) {
	panic("not implemented")
}

// DropMeasurement will clear the index of all references to a measurement and its child series.
func (t *Index) DropMeasurement(name string) {
	panic("not implemented")
}

// used to convert the tag set to bytes for use as a lookup key
func marshalTags(tags map[string]string) []byte {
	s := make([]string, 0, len(tags))
	// pull out keys to sort
	for k := range tags {
		s = append(s, k)
	}
	sort.Strings(s)

	// now append on the key values in key sorted order
	for _, k := range s {
		s = append(s, tags[k])
	}
	return []byte(strings.Join(s, "|"))
}
