package influxdb

import (
	"encoding/json"
	"regexp"
	"sort"
	"strings"
	"time"
)

// database is a collection of retention policies and shards. It also has methods
// for keeping an in memory index of all the measurements, series, and tags in the database.
// Methods on this struct aren't goroutine safe. They assume that the server is handling
// any locking to make things safe.
type database struct {
	name string

	policies map[string]*RetentionPolicy // retention policies by name
	shards   map[uint64]*Shard           // shards by id

	defaultRetentionPolicy string

	// in memory indexing structures
	measurements map[string]*Measurement // measurement name to object and index
	series       map[uint32]*Series      // map series id to the Series object
	names        []string                // sorted list of the measurement names
}

// newDatabase returns an instance of database.
func newDatabase() *database {
	return &database{
		policies:     make(map[string]*RetentionPolicy),
		shards:       make(map[uint64]*Shard),
		measurements: make(map[string]*Measurement),
		series:       make(map[uint32]*Series),
		names:        make([]string, 0),
	}
}

// shardByTimestamp returns a shard that owns a given timestamp.
func (db *database) shardByTimestamp(policy string, seriesID uint32, timestamp time.Time) (*Shard, error) {
	p := db.policies[policy]
	if p == nil {
		return nil, ErrRetentionPolicyNotFound
	}
	return p.shardByTimestamp(seriesID, timestamp), nil
}

// shardsByTimestamp returns all shards that own a given timestamp.
func (db *database) shardsByTimestamp(policy string, timestamp time.Time) ([]*Shard, error) {
	p := db.policies[policy]
	if p == nil {
		return nil, ErrRetentionPolicyNotFound
	}
	return p.shardsByTimestamp(timestamp), nil
}

// timeBetweenInclusive returns true if t is between min and max, inclusive.
func timeBetweenInclusive(t, min, max time.Time) bool {
	return (t.Equal(min) || t.After(min)) && (t.Equal(max) || t.Before(max))
}

// MarshalJSON encodes a database into a JSON-encoded byte slice.
func (db *database) MarshalJSON() ([]byte, error) {
	// Copy over properties to intermediate type.
	var o databaseJSON
	o.Name = db.name
	o.DefaultRetentionPolicy = db.defaultRetentionPolicy
	for _, rp := range db.policies {
		o.Policies = append(o.Policies, rp)
	}
	for _, s := range db.shards {
		o.Shards = append(o.Shards, s)
	}
	return json.Marshal(&o)
}

// UnmarshalJSON decodes a JSON-encoded byte slice to a database.
func (db *database) UnmarshalJSON(data []byte) error {
	// Decode into intermediate type.
	var o databaseJSON
	if err := json.Unmarshal(data, &o); err != nil {
		return err
	}

	// Copy over properties from intermediate type.
	db.name = o.Name
	db.defaultRetentionPolicy = o.DefaultRetentionPolicy

	// Copy shard policies.
	db.policies = make(map[string]*RetentionPolicy)
	for _, rp := range o.Policies {
		db.policies[rp.Name] = rp
	}

	// Copy shards.
	db.shards = make(map[uint64]*Shard)
	for _, s := range o.Shards {
		db.shards[s.ID] = s
	}

	return nil
}

// databaseJSON represents the JSON-serialization format for a database.
type databaseJSON struct {
	Name                   string             `json:"name,omitempty"`
	DefaultRetentionPolicy string             `json:"defaultRetentionPolicy,omitempty"`
	Policies               []*RetentionPolicy `json:"policies,omitempty"`
	Shards                 []*Shard           `json:"shards,omitempty"`
}

// Measurement represents a collection of time series in a database. It also contains in memory
// structures for indexing tags. These structures are accessed through private methods on the Measurement
// object. Generally these methods are only accessed from Index, which is responsible for ensuring
// go routine safe access.
type Measurement struct {
	Name   string    `json:"name,omitempty"`
	Fields []*Fields `json:"fields,omitempty"`

	// in memory index fields
	series              map[string]*Series // sorted tagset string to the series object
	seriesByID          map[uint32]*Series // lookup table for series by their id
	measurement         *Measurement
	seriesByTagKeyValue map[string]map[string]SeriesIDs // map from tag key to value to sorted set of series ids
	ids                 SeriesIDs                       // sorted list of series IDs in this measurement
}

func NewMeasurement(name string) *Measurement {
	return &Measurement{
		Name:   name,
		Fields: make([]*Fields, 0),

		series:              make(map[string]*Series),
		seriesByID:          make(map[uint32]*Series),
		seriesByTagKeyValue: make(map[string]map[string]SeriesIDs),
		ids:                 SeriesIDs(make([]uint32, 0)),
	}
}

// addSeries will add a series to the measurementIndex. Returns false if already present
func (m *Measurement) addSeries(s *Series) bool {
	if _, ok := m.seriesByID[s.ID]; ok {
		return false
	}
	m.seriesByID[s.ID] = s
	tagset := string(marshalTags(s.Tags))
	m.series[tagset] = s
	m.ids = append(m.ids, s.ID)
	// the series ID should always be higher than all others because it's a new
	// series. So don't do the sort if we don't have to.
	if len(m.ids) > 1 && m.ids[len(m.ids)-1] < m.ids[len(m.ids)-2] {
		sort.Sort(m.ids)
	}

	// add this series id to the tag index on the measurement
	for k, v := range s.Tags {
		valueMap := m.seriesByTagKeyValue[k]
		if valueMap == nil {
			valueMap = make(map[string]SeriesIDs)
			m.seriesByTagKeyValue[k] = valueMap
		}
		ids := valueMap[v]
		ids = append(ids, s.ID)

		// most of the time the series ID will be higher than all others because it's a new
		// series. So don't do the sort if we don't have to.
		if len(ids) > 1 && ids[len(ids)-1] < ids[len(ids)-2] {
			sort.Sort(ids)
		}
		valueMap[v] = ids
	}

	return true
}

// seriesByTags returns the Series that matches the given tagset.
func (m *Measurement) seriesByTags(tags map[string]string) *Series {
	return m.series[string(marshalTags(tags))]
}

// sereisIDs returns the series ids for a given filter
func (m *Measurement) seriesIDs(filter *TagFilter) (ids SeriesIDs) {
	values := m.seriesByTagKeyValue[filter.Key]
	if values == nil {
		return
	}

	// handle regex filters
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

// tagValues returns a map of unique tag values for the given key
func (m *Measurement) tagValues(key string) TagValues {
	tags := m.seriesByTagKeyValue[key]
	values := make(map[string]bool, len(tags))
	for k, _ := range tags {
		values[k] = true
	}
	return TagValues(values)
}

type Measurements []*Measurement

// Field represents a series field.
type Field struct {
	ID   uint8     `json:"id,omitempty"`
	Name string    `json:"name,omitempty"`
	Type FieldType `json:"field"`
}

type FieldType int

const (
	Int64 FieldType = iota
	Float64
	String
	Boolean
	Binary
)

// Fields represents a list of fields.
type Fields []*Field

// Series belong to a Measurement and represent unique time series in a database
type Series struct {
	ID   uint32
	Tags map[string]string

	measurement *Measurement
}

// RetentionPolicy represents a policy for creating new shards in a database and how long they're kept around for.
type RetentionPolicy struct {
	// Unique name within database. Required.
	Name string

	// Length of time to keep data around
	Duration time.Duration

	ReplicaN uint32
	SplitN   uint32

	Shards []*Shard
}

// NewRetentionPolicy returns a new instance of RetentionPolicy with defaults set.
func NewRetentionPolicy(name string) *RetentionPolicy {
	return &RetentionPolicy{
		Name:     name,
		ReplicaN: DefaultReplicaN,
		SplitN:   DefaultSplitN,
		Duration: DefaultShardRetention,
	}
}

// shardByTimestamp returns the shard in the space that owns a given timestamp for a given series id.
// Returns nil if the shard does not exist.
func (rp *RetentionPolicy) shardByTimestamp(seriesID uint32, timestamp time.Time) *Shard {
	shards := rp.shardsByTimestamp(timestamp)
	if len(shards) > 0 {
		return shards[int(seriesID)%len(shards)]
	}
	return nil
}

func (rp *RetentionPolicy) shardsByTimestamp(timestamp time.Time) []*Shard {
	shards := make([]*Shard, 0, rp.SplitN)
	for _, s := range rp.Shards {
		if timeBetweenInclusive(timestamp, s.StartTime, s.EndTime) {
			shards = append(shards, s)
		}
	}
	return shards
}

// MarshalJSON encodes a retention policy to a JSON-encoded byte slice.
func (rp *RetentionPolicy) MarshalJSON() ([]byte, error) {
	return json.Marshal(&retentionPolicyJSON{
		Name:     rp.Name,
		Duration: rp.Duration,
		ReplicaN: rp.ReplicaN,
		SplitN:   rp.SplitN,
	})
}

// UnmarshalJSON decodes a JSON-encoded byte slice to a retention policy.
func (rp *RetentionPolicy) UnmarshalJSON(data []byte) error {
	// Decode into intermediate type.
	var o retentionPolicyJSON
	if err := json.Unmarshal(data, &o); err != nil {
		return err
	}

	// Copy over properties from intermediate type.
	rp.Name = o.Name
	rp.ReplicaN = o.ReplicaN
	rp.SplitN = o.SplitN
	rp.Duration = o.Duration
	rp.Shards = o.Shards

	return nil
}

// retentionPolicyJSON represents an intermediate struct for JSON marshaling.
type retentionPolicyJSON struct {
	Name     string        `json:"name"`
	ReplicaN uint32        `json:"replicaN,omitempty"`
	SplitN   uint32        `json:"splitN,omitempty"`
	Duration time.Duration `json:"duration,omitempty"`
	Shards   []*Shard      `json:"shards,omitempty"`
}

// RetentionPolicies represents a list of shard policies.
type RetentionPolicies []*RetentionPolicy

// Shards returns a list of all shards for all policies.
func (rps RetentionPolicies) Shards() []*Shard {
	var shards []*Shard
	for _, rp := range rps {
		shards = append(shards, rp.Shards...)
	}
	return shards
}

// TagFilter represents a tag filter when looking up other tags or measurements.
type TagFilter struct {
	Not   bool
	Key   string
	Value string
	Regex *regexp.Regexp
}

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

// addSeriesToIndex adds the series for the given measurement to the index. Returns false if already present
func (d *database) addSeriesToIndex(measurementName string, s *Series) bool {
	// if there is a measurement for this id, it's already been added
	if d.series[s.ID] != nil {
		return false
	}

	// get or create the measurement index and index it globally and in the measurement
	idx := d.createMeasurementIfNotExists(measurementName)

	s.measurement = idx
	d.series[s.ID] = s

	// TODO: add this series to the global tag index

	return idx.addSeries(s)
}

// createMeasurementIfNotExists will either add a measurement object to the index or return the existing one.
func (d *database) createMeasurementIfNotExists(name string) *Measurement {
	idx := d.measurements[name]
	if idx == nil {
		idx = NewMeasurement(name)
		d.measurements[name] = idx
		d.names = append(d.names, name)
		sort.Strings(d.names)
	}
	return idx
}

// AddField adds a field to the measurement name. Returns false if already present
func (d *database) AddField(name string, f *Field) bool {
	if true { panic("not implemented") }
	return false
}

// MeasurementsBySeriesIDs returns a collection of unique Measurements for the passed in SeriesIDs.
func (d *database) MeasurementsBySeriesIDs(seriesIDs SeriesIDs) []*Measurement {
	measurements := make(map[*Measurement]bool)

	for _, id := range seriesIDs {
		m := d.series[id].measurement
		measurements[m] = true
	}

	values := make([]*Measurement, 0, len(measurements))
	for m, _ := range measurements {
		values = append(values, m)
	}

	return values
}

// SeriesIDs returns an array of series ids for the given measurements and filters to be applied to all.
// Filters are equivalent to an AND operation. If you want to do an OR, get the series IDs for one set,
// then get the series IDs for another set and use the SeriesIDs.Union to combine the two.
func (d *database) SeriesIDs(names []string, filters []*TagFilter) SeriesIDs {
	// they want all ids if no filters are specified
	if len(filters) == 0 {
		ids := SeriesIDs(make([]uint32, 0))
		for _, idx := range d.measurements {
			ids = ids.Union(idx.ids)
		}
		return ids
	}

	ids := SeriesIDs(make([]uint32, 0))
	for _, n := range names {
		ids = ids.Union(d.seriesIDsByName(n, filters))
	}

	return ids
}

// TagKeys returns a sorted array of unique tag keys for the given measurements.
// If an empty or nil slice is passed in, the tag keys for the entire database will be returned.
func (d *database) TagKeys(names []string) []string {
	if len(names) == 0 {
		names = d.names
	}

	keys := make(map[string]bool)
	for _, n := range names {
		idx := d.measurements[n]
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
func (d *database) TagValues(names []string, key string, filters []*TagFilter) TagValues {
	values := TagValues(make(map[string]bool))

	// see if they just want all the tag values for this key
	if len(filters) == 0 {
		for _, n := range names {
			idx := d.measurements[n]
			if idx != nil {
				values.Union(idx.tagValues(key))
			}
		}
		return values
	}

	// they have filters so just get a set of series ids matching them and then get the tag values from those
	seriesIDs := d.SeriesIDs(names, filters)
	return d.tagValuesBySeries(key, seriesIDs)
}

// tagValuesBySeries will return a TagValues map of all the unique tag values for a collection of series.
func (d *database) tagValuesBySeries(key string, seriesIDs SeriesIDs) TagValues {
	values := make(map[string]bool)
	for _, id := range seriesIDs {
		s := d.series[id]
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

//seriesIDsByName is the same as SeriesIDs, but for a specific measurement.
func (d *database) seriesIDsByName(name string, filters []*TagFilter) SeriesIDs {
	idx := d.measurements[name]
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
func (d *database) MeasurementBySeriesID(id uint32) *Measurement {
	if s, ok := d.series[id]; ok {
		return s.measurement
	}
	return nil
}

// MeasurementAndSeries returns the Measurement and the Series for a given measurement name and tag set.
func (d *database) MeasurementAndSeries(name string, tags map[string]string) (*Measurement, *Series) {
	idx := d.measurements[name]
	if idx == nil {
		return nil, nil
	}
	return idx, idx.seriesByTags(tags)
}

// SereiesByID returns the Series that has the given id.
func (d *database) SeriesByID(id uint32) *Series {
	return d.series[id]
}

// Measurements returns all measurements that match the given filters.
func (d *database) Measurements(filters []*TagFilter) []*Measurement {
	measurements := make([]*Measurement, 0, len(d.measurements))
	for _, idx := range d.measurements {
		measurements = append(measurements, idx.measurement)
	}
	return measurements
}

// Names returns all measurement names in sorted order.
func (d *database) Names() []string {
	return d.names
}

// DropSeries will clear the index of all references to a series.
func (d *database) DropSeries(id uint32) {
	panic("not implemented")
}

// DropMeasurement will clear the index of all references to a measurement and its child series.
func (d *database) DropMeasurement(name string) {
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
