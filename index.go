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
	measurementIndex    map[string]*measurementIndex // map measurement name to its tag index
	seriesToMeasurement map[uint32]*Measurement      // map series id to its measurement
	series              map[uint32]*Series           // map series id to the Series object
}

func NewIndex() *Index {
	return &Index{
		measurementIndex:    make(map[string]*measurementIndex),
		seriesToMeasurement: make(map[uint32]*Measurement),
		series:              make(map[uint32]*Series),
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

func (f Filters) String() string {
	return string(mustMarshalJSON(f))
}

// SeriesIDs is a convenience type for sorting, checking equality, and doing union and
// intersection of collections of series ids.
type SeriesIDs []uint32

func (p SeriesIDs) Len() int           { return len(p) }
func (p SeriesIDs) Less(i, j int) bool { return p[i] < p[j] }
func (p SeriesIDs) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p SeriesIDs) Sort()              { sort.Sort(p) }

// Equals assumes that both are sorted. This is by design, no touchy!
func (p SeriesIDs) Equals(s SeriesIDs) bool {
	if len(p) != len(s) {
		return false
	}
	for i, pp := range p {
		if s[i] != pp {
			return false
		}
	}
	return true
}

// Intersect returns a new collection of series ids in sorted order that is the intersection of the two.
// The two collections must already be sorted.
func (s SeriesIDs) Intersect(a SeriesIDs) SeriesIDs {
	l := s
	r := a

	// we want to iterate through the shortest one and stop
	if len(a) < len(s) {
		l = a
		r = s
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
func (s SeriesIDs) Union(a SeriesIDs) SeriesIDs {
	l := s
	r := a

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

// Convenience method to output something during tests
func (s SeriesIDs) String() string {
	return string(mustMarshalJSON(s))
}

// Keeps a mapping of the series in a measurement
type measurementIndex struct {
	series       map[string]*Series // sorted tag string to the series object
	measurement  *Measurement
	tagsToSeries map[string]map[string]SeriesIDs // map from tag key to value to sorted set of series ids
	ids          SeriesIDs                       // sorted list of series IDs in this measurement
}

// addSeries will add a series to the measurementIndex. Returns false if already present
func (m *measurementIndex) addSeries(s *Series) bool {
	id := string(tagsToBytes(s.Tags))
	if _, ok := m.series[id]; ok {
		return false
	}
	m.series[id] = s
	m.ids = append(m.ids, s.ID)
	// the series ID should always be higher than all others because it's a new
	// series. So don't do the sort if we don't have to.
	if len(m.ids) > 1 && m.ids[len(m.ids)-1] < m.ids[len(m.ids)-2] {
		m.ids.Sort()
	}

	// add this series id to the tag index on the measurement
	for k, v := range s.Tags {
		valueMap := m.tagsToSeries[k]
		if valueMap == nil {
			valueMap = make(map[string]SeriesIDs)
			m.tagsToSeries[k] = valueMap
		}
		ids := valueMap[v]
		if ids == nil {
			ids = make([]uint32, 0)
		}
		ids = append(ids, s.ID)

		// most of the time the series ID will be higher than all others because it's a new
		// series. So don't do the sort if we don't have to.
		if len(ids) > 1 && ids[len(ids)-1] < ids[len(ids)-2] {
			ids.Sort()
		}
		valueMap[v] = ids
	}

	return true
}

// seriesByTags returns the Series that matches the given tagset.
func (m measurementIndex) seriesByTags(tags map[string]string) *Series {
	return m.series[string(tagsToBytes(tags))]
}

// sereisIDs returns the series ids for a given filter
func (m measurementIndex) seriesIDs(filter *Filter) (ids SeriesIDs) {
	values := m.tagsToSeries[filter.Key]
	if values == nil {
		return
	}

	// hanlde regex filters
	if filter.Regex != nil {
		for k, v := range values {
			if filter.Regex.MatchString(k) {
				if ids == nil {
					ids = v
				} else {
					ids = ids.Union(v)
				}
			}
		}
		if filter.Not {
			ids = m.ids.Reject(ids)
		}
		return
	}

	// this is for the value is not null query
	if filter.Not && filter.Value == "" {
		for _, v := range values {
			if ids == nil {
				ids = v
			} else {
				ids.Intersect(v)
			}
		}
		return
	}

	// get the ids that have the given key/value tag pair
	ids = SeriesIDs(values[filter.Value])

	// filter out these ids from the entire set if it's a not query
	if filter.Not {
		ids = m.ids.Reject(ids)
	}

	return
}

// AddSeries adds the series for the given measurement to the index. Returns false if already present
func (t *Index) AddSeries(name string, s *Series) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	// if there is a measurement for this id, it's already been added
	if t.seriesToMeasurement[s.ID] != nil {
		return false
	}

	// get or create the measurement index
	idx := t.measurementIndex[name]
	if idx == nil {
		idx = &measurementIndex{
			series:       make(map[string]*Series),
			measurement:  NewMeasurement(name),
			tagsToSeries: make(map[string]map[string]SeriesIDs),
			ids:          SeriesIDs(make([]uint32, 0)),
		}
		t.measurementIndex[name] = idx
	}
	idx.measurement.Series = append(idx.measurement.Series, s)
	t.seriesToMeasurement[s.ID] = idx.measurement
	t.series[s.ID] = s

	// TODO: add this series to the global tag index

	b := idx.addSeries(s)
	return b
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
		for _, idx := range t.measurementIndex {
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

//seriesIDsForName is the same as SeriesIDs, but for a specific measurement.
func (t *Index) seriesIDsForName(name string, filters Filters) SeriesIDs {
	idx := t.measurementIndex[name]
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
	idx := t.measurementIndex[name]
	if idx == nil {
		return nil, nil
	}
	return idx.measurement, idx.seriesByTags(tags)
}

// SereiesByID returns the Series that has the given id.
func (t *Index) SeriesByID(id uint32) *Series {
	return t.series[id]
}

// Measurements returns all measurements that match the given filters.
func (t *Index) Measurements(filters []*Filter) []*Measurement {
	measurements := make([]*Measurement, 0, len(t.measurementIndex))
	for _, idx := range t.measurementIndex {
		measurements = append(measurements, idx.measurement)
	}
	return measurements
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
func tagsToBytes(tags map[string]string) []byte {
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
