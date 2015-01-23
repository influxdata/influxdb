package influxdb

import (
	"encoding/json"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/influxdb/influxdb/influxql"
)

// database is a collection of retention policies and shards. It also has methods
// for keeping an in memory index of all the measurements, series, and tags in the database.
// Methods on this struct aren't goroutine safe. They assume that the server is handling
// any locking to make things safe.
type database struct {
	name string

	policies map[string]*RetentionPolicy // retention policies by name

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
		measurements: make(map[string]*Measurement),
		series:       make(map[uint32]*Series),
		names:        make([]string, 0),
	}
}

// shardGroupByTimestamp returns a shard group that owns a given timestamp.
func (db *database) shardGroupByTimestamp(policy string, timestamp time.Time) (*ShardGroup, error) {
	p := db.policies[policy]
	if p == nil {
		return nil, ErrRetentionPolicyNotFound
	}
	return p.shardGroupByTimestamp(timestamp), nil
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

	return nil
}

// databaseJSON represents the JSON-serialization format for a database.
type databaseJSON struct {
	Name                   string             `json:"name,omitempty"`
	DefaultRetentionPolicy string             `json:"defaultRetentionPolicy,omitempty"`
	Policies               []*RetentionPolicy `json:"policies,omitempty"`
}

// Measurement represents a collection of time series in a database. It also contains in memory
// structures for indexing tags. These structures are accessed through private methods on the Measurement
// object. Generally these methods are only accessed from Index, which is responsible for ensuring
// go routine safe access.
type Measurement struct {
	Name   string   `json:"name,omitempty"`
	Fields []*Field `json:"fields,omitempty"`

	// in-memory index fields
	series              map[string]*Series // sorted tagset string to the series object
	seriesByID          map[uint32]*Series // lookup table for series by their id
	measurement         *Measurement
	seriesByTagKeyValue map[string]map[string]seriesIDs // map from tag key to value to sorted set of series ids
	ids                 seriesIDs                       // sorted list of series IDs in this measurement
}

func NewMeasurement(name string) *Measurement {
	return &Measurement{
		Name:   name,
		Fields: make([]*Field, 0),

		series:              make(map[string]*Series),
		seriesByID:          make(map[uint32]*Series),
		seriesByTagKeyValue: make(map[string]map[string]seriesIDs),
		ids:                 make(seriesIDs, 0),
	}
}

// createFieldIfNotExists creates a new field with an autoincrementing ID.
// Returns an error if 255 fields have already been created on the measurement.
func (m *Measurement) createFieldIfNotExists(name string, typ influxql.DataType) (*Field, error) {
	// Ignore if the field already exists.
	if f := m.FieldByName(name); f != nil {
		return f, nil
	}

	// Only 255 fields are allowed. If we go over that then return an error.
	if len(m.Fields)+1 > math.MaxUint8 {
		return nil, ErrFieldOverflow
	}

	// Create and append a new field.
	f := &Field{
		ID:   uint8(len(m.Fields) + 1),
		Name: name,
		Type: typ,
	}
	m.Fields = append(m.Fields, f)

	return f, nil
}

// Field returns a field by id.
func (m *Measurement) Field(id uint8) *Field {
	for _, f := range m.Fields {
		if f.ID == id {
			return f
		}
	}
	return nil
}

// FieldByName returns a field by name.
func (m *Measurement) FieldByName(name string) *Field {
	for _, f := range m.Fields {
		if f.Name == name {
			return f
		}
	}
	return nil
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
			valueMap = make(map[string]seriesIDs)
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

// mapValues converts a map of values with string keys to field id keys.
// Returns nil if any field doesn't exist.
func (m *Measurement) mapValues(values map[string]interface{}) map[uint8]interface{} {
	other := make(map[uint8]interface{}, len(values))
	for k, v := range values {
		// TODO: Cast value to original field type.

		f := m.FieldByName(k)
		if f == nil {
			return nil
		}
		other[f.ID] = v
	}
	return other
}

// expandExpr returns a list of expressions expanded by all possible tag combinations.
func (m *Measurement) expandExpr(expr influxql.Expr) []tagSetExpr {
	// Retrieve list of unique values for each tag.
	valuesByTagKey := m.uniqueTagValues(expr)

	// Convert keys to slices.
	keys := make([]string, 0, len(valuesByTagKey))
	for key := range valuesByTagKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Order uniques by key.
	uniques := make([][]string, len(keys))
	for i, key := range keys {
		uniques[i] = valuesByTagKey[key]
	}

	// Reduce a condition for each combination of tag values.
	return expandExprWithValues(expr, keys, []*string{}, uniques, 0)
}

func expandExprWithValues(expr influxql.Expr, keys []string, values []*string, uniques [][]string, index int) []tagSetExpr {
	// If we have no more keys left then execute the reduction and return.
	if index == len(keys) {
		// Create map.
		m := make(map[string]*string, len(keys))
		for i, key := range keys {
			if values[i] != nil {
				var value string
				value = *values[i]
				m[key] = &value
			} else {
				m[key] = nil
			}
		}

		// Reduce using the current tag key/value set.
		// Ignore it if reduces down to "false".
		e := influxql.Reduce(expr, &tagValuer{tags: m})
		if e, ok := e.(*influxql.BooleanLiteral); ok && e.Val == false {
			return nil
		}

		return []tagSetExpr{{tags: m, expr: e}}
	}

	// Otherwise expand for each possible value of the key.
	var exprs []tagSetExpr
	for _, v := range uniques[index] {
		exprs = append(exprs, expandExprWithValues(expr, keys, append(values, &v), uniques, index+1)...)
	}

	// Also expand for the nil value.
	exprs = append(exprs, expandExprWithValues(expr, keys, append(values, nil), uniques, index+1)...)

	return exprs
}

// tagValuer is used during expression expansion to evaluate all sets of tag values.
type tagValuer struct {
	tags map[string]*string
}

// Value returns the string value of a tag and true if it's listed in the tagset.
func (v *tagValuer) Value(name string) (interface{}, bool) {
	if value, ok := v.tags[name]; ok {
		if value == nil {
			return nil, true
		}
		return *value, true
	}
	return nil, false
}

// tagSetExpr represents a set of tag keys/values and associated expression.
type tagSetExpr struct {
	tags map[string]*string
	expr influxql.Expr
}

// uniqueTagValues returns a list of unique tag values used in an expression.
func (m *Measurement) uniqueTagValues(expr influxql.Expr) map[string][]string {
	// Track unique value per tag.
	tags := make(map[string]map[string]struct{})

	// Find all tag values referenced in the expression.
	influxql.WalkFunc(expr, func(n influxql.Node) {
		switch n := n.(type) {
		case *influxql.BinaryExpr:
			// Ignore non-equality operators.
			if n.Op != influxql.EQ {
				return
			}

			// Extract ref and string literal.
			var key, value string
			switch lhs := n.LHS.(type) {
			case *influxql.VarRef:
				if rhs, ok := n.RHS.(*influxql.StringLiteral); ok {
					key, value = lhs.Val, rhs.Val
				}
			case *influxql.StringLiteral:
				if rhs, ok := n.RHS.(*influxql.VarRef); ok {
					key, value = rhs.Val, lhs.Val
				}
			}
			if key == "" {
				return
			}

			// Add value to set.
			if tags[key] == nil {
				tags[key] = make(map[string]struct{})
			}
			tags[key][value] = struct{}{}
		}
	})

	// Convert to map of slices.
	out := make(map[string][]string)
	for k, values := range tags {
		out[k] = make([]string, 0, len(values))
		for v := range values {
			out[k] = append(out[k], v)
		}
		sort.Strings(out[k])
	}
	return out
}

type Measurements []*Measurement

// Field represents a series field.
type Field struct {
	ID   uint8             `json:"id,omitempty"`
	Name string            `json:"name,omitempty"`
	Type influxql.DataType `json:"type,omitempty"`
}

// Fields represents a list of fields.
type Fields []*Field

// Series belong to a Measurement and represent unique time series in a database
type Series struct {
	ID   uint32
	Tags map[string]string

	measurement *Measurement
}

// match returns true if all tags match the series' tags.
func (s *Series) match(tags map[string]string) bool {
	for k, v := range tags {
		if s.Tags[k] != v {
			return false
		}
	}
	return true
}

type seriesIDs []uint32

func (p seriesIDs) Len() int           { return len(p) }
func (p seriesIDs) Less(i, j int) bool { return p[i] < p[j] }
func (p seriesIDs) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// RetentionPolicy represents a policy for creating new shards in a database and how long they're kept around for.
type RetentionPolicy struct {
	// Unique name within database. Required.
	Name string

	// Length of time to keep data around
	Duration time.Duration

	// The number of copies to make of each shard.
	ReplicaN uint32

	shardGroups []*ShardGroup
}

// NewRetentionPolicy returns a new instance of RetentionPolicy with defaults set.
func NewRetentionPolicy(name string) *RetentionPolicy {
	return &RetentionPolicy{
		Name:     name,
		ReplicaN: DefaultReplicaN,
		Duration: DefaultShardRetention,
	}
}

// shardGroupByTimestamp returns the group in the policy that owns a timestamp.
// Returns nil group does not exist.
func (rp *RetentionPolicy) shardGroupByTimestamp(timestamp time.Time) *ShardGroup {
	for _, g := range rp.shardGroups {
		if timeBetweenInclusive(timestamp, g.StartTime, g.EndTime) {
			return g
		}
	}
	return nil
}

// MarshalJSON encodes a retention policy to a JSON-encoded byte slice.
func (rp *RetentionPolicy) MarshalJSON() ([]byte, error) {
	var o retentionPolicyJSON
	o.Name = rp.Name
	o.Duration = rp.Duration
	o.ReplicaN = rp.ReplicaN
	for _, g := range rp.shardGroups {
		o.ShardGroups = append(o.ShardGroups, g)
	}
	return json.Marshal(&o)
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
	rp.Duration = o.Duration
	rp.shardGroups = o.ShardGroups

	return nil
}

// retentionPolicyJSON represents an intermediate struct for JSON marshaling.
type retentionPolicyJSON struct {
	Name        string        `json:"name"`
	ReplicaN    uint32        `json:"replicaN,omitempty"`
	SplitN      uint32        `json:"splitN,omitempty"`
	Duration    time.Duration `json:"duration,omitempty"`
	ShardGroups []*ShardGroup `json:"shardGroups,omitempty"`
}

// addSeriesToIndex adds the series for the given measurement to the index.
// Returns true if not already present.
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

// MeasurementAndSeries returns the Measurement and the Series for a given measurement name and tag set.
func (d *database) MeasurementAndSeries(name string, tags map[string]string) (*Measurement, *Series) {
	idx := d.measurements[name]
	if idx == nil {
		return nil, nil
	}
	return idx, idx.seriesByTags(tags)
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

// timeBetweenInclusive returns true if t is between min and max, inclusive.
func timeBetweenInclusive(t, min, max time.Time) bool {
	return (t.Equal(min) || t.After(min)) && (t.Equal(max) || t.Before(max))
}
