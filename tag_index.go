package influxdb

import (
	"regexp"
	"sort"
	"strings"
	"sync"
)

// TagIndex is the in memory index structure for answering queries about measurements, tags
// and series within a database.
type TagIndex struct {
	mu                  sync.RWMutex
	measurementIndex    map[string]*measurementIndex
	seriesToMeasurement map[uint32]*Measurement
	series              map[uint32]*Series
}

func NewTagIndex() *TagIndex {
	return &TagIndex{
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
	Regex regexp.Regexp
}

type SeriesIDs []uint32

func (p SeriesIDs) Len() int           { return len(p) }
func (p SeriesIDs) Less(i, j int) bool { return p[i] < p[j] }
func (p SeriesIDs) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p SeriesIDs) Sort()              { sort.Sort(p) }
func (p SeriesIDs) Equals(s SeriesIDs) bool {
	if len(p) != len(s) {
		return false
	}
	p.Sort()
	s.Sort()
	for i, pp := range p {
		if s[i] != pp {
			return false
		}
	}
	return true
}

// Keeps a mapping of the series in a measurement
type measurementIndex struct {
	series      map[string]*Series // sorted tag string to the series object
	measurement *Measurement
}

// addSeries will add a series to the measurementIndex. Returns false if already present
func (m measurementIndex) addSeries(s *Series) bool {
	id := string(tagsToBytes(s.Tags))
	if _, ok := m.series[id]; ok {
		return false
	}
	m.series[id] = s
	return true
}

func (m measurementIndex) seriesByTags(tags map[string]string) *Series {
	return m.series[string(tagsToBytes(tags))]
}

// AddSeries adds the series for the given measurement to the index. Returns false if already present
func (t *TagIndex) AddSeries(name string, s *Series) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	// if there is a measurement for this id, it's already been added
	if t.seriesToMeasurement[s.ID] != nil {
		return false
	}

	// get or create the measurement index
	idx := t.measurementIndex[name]
	if idx == nil {
		idx = &measurementIndex{series: make(map[string]*Series), measurement: NewMeasurement(name)}
		t.measurementIndex[name] = idx
	}
	idx.measurement.Series = append(idx.measurement.Series, s)
	t.seriesToMeasurement[s.ID] = idx.measurement
	t.series[s.ID] = s

	return idx.addSeries(s)
}

// AddField adds a field to the measurement name. Returns false if already present
func (t *TagIndex) AddField(name string, f *Field) bool {
	return false
}

// SeriesIDs returns an array of series ids for the given measurements and filters to be applied to all
func (t *TagIndex) SeriesIDs(names []string, filters []*Filter) SeriesIDs {
	t.mu.RLock()
	defer t.mu.RUnlock()

	ids := make([]uint32, 0)
	for _, idx := range t.measurementIndex {
		for _, s := range idx.series {
			ids = append(ids, s.ID)
		}
	}
	return ids
}

func (t *TagIndex) MeasurementBySeriesID(id uint32) *Measurement {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.seriesToMeasurement[id]
}

func (t *TagIndex) MeasurementAndSeries(name string, tags map[string]string) (*Measurement, *Series) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	idx := t.measurementIndex[name]
	if idx == nil {
		return nil, nil
	}
	return idx.measurement, idx.seriesByTags(tags)
}

func (t *TagIndex) SeriesByID(id uint32) *Series {
	return t.series[id]
}

func (t *TagIndex) Measurements(filters []*Filter) []*Measurement {
	measurements := make([]*Measurement, 0, len(t.measurementIndex))
	for _, idx := range t.measurementIndex {
		measurements = append(measurements, idx.measurement)
	}
	return measurements
}

func (t *TagIndex) DropSeries(id uint32) {
	panic("not implemented")
}

func (t *TagIndex) DropMeasurement(name string) {
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
