package influxdb

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/influxdb/influxdb/influxql"
)

const (
	maxStringLength = 64 * 1024
)

// database is a collection of retention policies and shards. It also has methods
// for keeping an in memory index of all the measurements, series, and tags in the database.
// Methods on this struct aren't goroutine safe. They assume that the server is handling
// any locking to make things safe.
type database struct {
	name string

	policies          map[string]*RetentionPolicy // retention policies by name
	continuousQueries []*ContinuousQuery          // continuous queries

	defaultRetentionPolicy string

	// in memory indexing structures
	measurements map[string]*Measurement // measurement name to object and index
	series       map[uint32]*Series      // map series id to the Series object
	names        []string                // sorted list of the measurement names
}

// newDatabase returns an instance of database.
func newDatabase() *database {
	return &database{
		policies:          make(map[string]*RetentionPolicy),
		continuousQueries: make([]*ContinuousQuery, 0),
		measurements:      make(map[string]*Measurement),
		series:            make(map[uint32]*Series),
		names:             make([]string, 0),
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

// Series takes a series ID and returns a series.
func (db *database) Series(id uint32) *Series {
	return db.series[id]
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
	o.ContinuousQueries = db.continuousQueries
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

	// we need the parsed continuous queries to be in the in memory index
	db.continuousQueries = make([]*ContinuousQuery, 0, len(o.ContinuousQueries))
	for _, cq := range o.ContinuousQueries {
		c, _ := NewContinuousQuery(cq.Query)
		db.continuousQueries = append(db.continuousQueries, c)
	}

	return nil
}

// databaseJSON represents the JSON-serialization format for a database.
type databaseJSON struct {
	Name                   string             `json:"name,omitempty"`
	DefaultRetentionPolicy string             `json:"defaultRetentionPolicy,omitempty"`
	Policies               []*RetentionPolicy `json:"policies,omitempty"`
	ContinuousQueries      []*ContinuousQuery `json:"continuousQueries,omitempty"`
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
	seriesIDs           seriesIDs                       // sorted list of series IDs in this measurement
}

// NewMeasurement allocates and initializes a new Measurement.
func NewMeasurement(name string) *Measurement {
	return &Measurement{
		Name:   name,
		Fields: make([]*Field, 0),

		series:              make(map[string]*Series),
		seriesByID:          make(map[uint32]*Series),
		seriesByTagKeyValue: make(map[string]map[string]seriesIDs),
		seriesIDs:           make(seriesIDs, 0),
	}
}

// HasTagKey returns true if at least one eries in this measurement has written a value for the passed in tag key
func (m *Measurement) HasTagKey(k string) bool {
	return m.seriesByTagKeyValue[k] != nil
}

// createFieldIfNotExists creates a new field with an autoincrementing ID.
// Returns an error if 255 fields have already been created on the measurement or
// the fields already exists with a different type.
func (m *Measurement) createFieldIfNotExists(name string, typ influxql.DataType) error {
	// Ignore if the field already exists.
	if f := m.FieldByName(name); f != nil {
		if f.Type != typ {
			return ErrFieldTypeConflict
		}
		return nil
	}

	// Only 255 fields are allowed. If we go over that then return an error.
	if len(m.Fields)+1 > math.MaxUint8 {
		return ErrFieldOverflow
	}

	// Create and append a new field.
	f := &Field{
		ID:   uint8(len(m.Fields) + 1),
		Name: name,
		Type: typ,
	}
	m.Fields = append(m.Fields, f)

	return nil
}

// Field returns a field by id.
func (m *Measurement) Field(id uint8) *Field {
	if int(id) > len(m.Fields) {
		return nil
	}
	return m.Fields[id-1]
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
	m.seriesIDs = append(m.seriesIDs, s.ID)

	// the series ID should always be higher than all others because it's a new
	// series. So don't do the sort if we don't have to.
	if len(m.seriesIDs) > 1 && m.seriesIDs[len(m.seriesIDs)-1] < m.seriesIDs[len(m.seriesIDs)-2] {
		sort.Sort(m.seriesIDs)
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

// dropSeries will remove a series from the measurementIndex. Returns true if already removed
func (m *Measurement) dropSeries(seriesID uint32) bool {
	if _, ok := m.seriesByID[seriesID]; !ok {
		return true
	}
	s := m.seriesByID[seriesID]
	tagset := string(marshalTags(s.Tags))

	delete(m.series, tagset)
	delete(m.seriesByID, seriesID)

	var ids []uint32
	for _, id := range m.seriesIDs {
		if id != seriesID {
			ids = append(ids, id)
		}
	}
	m.seriesIDs = ids

	// remove this series id to the tag index on the measurement
	// s.seriesByTagKeyValue is defined as map[string]map[string]seriesIDs
	for k, v := range m.seriesByTagKeyValue {
		values := v
		for kk, vv := range values {
			var ids []uint32
			for _, id := range vv {
				if id != seriesID {
					ids = append(ids, id)
				}
			}
			// Check to see if we have any ids, if not, remove the key
			if len(ids) == 0 {
				delete(values, kk)
			} else {
				values[kk] = ids
			}
		}
		// If we have no values, then we delete the key
		if len(values) == 0 {
			delete(m.seriesByTagKeyValue, k)
		} else {
			m.seriesByTagKeyValue[k] = values
		}
	}

	return true
}

// seriesByTags returns the Series that matches the given tagset.
func (m *Measurement) seriesByTags(tags map[string]string) *Series {
	return m.series[string(marshalTags(tags))]
}

// filters walks the where clause of a select statement and returns a map with all series ids
// matching the where clause and any filter expression that should be applied to each
func (m *Measurement) filters(stmt *influxql.SelectStatement) map[uint32]influxql.Expr {
	seriesIdsToExpr := make(map[uint32]influxql.Expr)
	if stmt.Condition == nil || stmt.OnlyTimeDimensions() {
		for _, id := range m.seriesIDs {
			seriesIdsToExpr[id] = nil
		}
		return seriesIdsToExpr
	}
	ids, _, _ := m.walkWhereForSeriesIds(stmt.Condition, seriesIdsToExpr)

	// ensure every id is in the map
	for _, id := range ids {
		if _, ok := seriesIdsToExpr[id]; !ok {
			seriesIdsToExpr[id] = nil
		}
	}

	return seriesIdsToExpr
}

// tagSets returns the unique tag sets that exist for the given tag keys. This is used to determine
// what composite series will be created by a group by. i.e. "group by region" should return:
// {"region":"uswest"}, {"region":"useast"}
// or region, service returns
// {"region": "uswest", "service": "redis"}, {"region": "uswest", "service": "mysql"}, etc...
// This will also populate the TagSet objects with the series IDs that match each tagset and any
// influx filter expression that goes with the series
func (m *Measurement) tagSets(stmt *influxql.SelectStatement, dimensions []string) []*influxql.TagSet {
	// get the unique set of series ids and the filters that should be applied to each
	filters := m.filters(stmt)

	// build the tag sets
	var tagStrings []string
	tagSets := make(map[string]*influxql.TagSet)
	for id, filter := range filters {
		// get the series and set the tag values for the dimensions we care about
		s := m.seriesByID[id]
		tags := make([]string, len(dimensions))
		for i, dim := range dimensions {
			tags[i] = s.Tags[dim]
		}

		// marshal it into a string and put this series and its expr into the tagSets map
		t := strings.Join(tags, "")
		set, ok := tagSets[t]
		if !ok {
			tagStrings = append(tagStrings, t)
			set = &influxql.TagSet{}
			// set the tags for this set
			tagsForSet := make(map[string]string)
			for i, dim := range dimensions {
				tagsForSet[dim] = tags[i]
			}
			set.Tags = tagsForSet
			set.Key = marshalTags(tagsForSet)
		}
		set.AddFilter(id, filter)
		tagSets[t] = set
	}

	// return the tag sets in sorted order
	a := make([]*influxql.TagSet, 0, len(tagSets))
	sort.Strings(tagStrings)
	for _, s := range tagStrings {
		a = append(a, tagSets[s])
	}

	return a
}

// idsForExpr will return a collection of series ids, a bool indicating if the result should be
// used (it'll be false if it's a time expr) and a field expression if the passed in expression is against a field.
func (m *Measurement) idsForExpr(n *influxql.BinaryExpr) (seriesIDs, bool, influxql.Expr) {
	name, ok := n.LHS.(*influxql.VarRef)
	value := n.RHS
	if !ok {
		name, _ = n.RHS.(*influxql.VarRef)
		value = n.LHS
	}

	// ignore time literals
	if _, ok := value.(*influxql.TimeLiteral); ok || name.Val == "time" {
		return nil, false, nil
	}

	// if it's a field we can't collapse it so we have to look at all series ids for this
	if m.FieldByName(name.Val) != nil {
		return m.seriesIDs, true, n
	}

	tagVals, ok := m.seriesByTagKeyValue[name.Val]
	if !ok {
		return nil, true, nil
	}

	// if we're looking for series with a specific tag value
	if str, ok := value.(*influxql.StringLiteral); ok {
		return tagVals[str.Val], true, nil
	}

	// if we're looking for series with tag values that match a regex
	if re, ok := value.(*influxql.RegexLiteral); ok {
		var ids seriesIDs
		for k := range tagVals {
			match := re.Val.MatchString(k)

			if (match && n.Op == influxql.EQREGEX) || (!match && n.Op == influxql.NEQREGEX) {
				ids = ids.union(tagVals[k])
			}
		}
		return ids, true, nil
	}

	return nil, true, nil
}

// walkWhereForSeriesIds will recursively walk the where clause and return a collection of series ids, a boolean indicating if this return
// value should be included in the resulting set, and an expression if the return is a field expression.
// The map that it takes maps each series id to the field expression that should be used to evaluate it when iterating over its cursor.
// Series that have no field expressions won't be in the map
func (m *Measurement) walkWhereForSeriesIds(expr influxql.Expr, filters map[uint32]influxql.Expr) (seriesIDs, bool, influxql.Expr) {
	switch n := expr.(type) {
	case *influxql.BinaryExpr:
		switch n.Op {
		case influxql.EQ, influxql.NEQ, influxql.LT, influxql.LTE, influxql.GT, influxql.GTE, influxql.EQREGEX, influxql.NEQREGEX:
			// if it's a compare, then it's either a field expression or against a tag. we can return this
			ids, shouldInclude, expr := m.idsForExpr(n)
			for _, id := range ids {
				filters[id] = expr
			}
			return ids, shouldInclude, expr
		case influxql.AND, influxql.OR:
			// if it's an AND or OR we need to union or intersect the results
			var ids seriesIDs
			l, il, lexpr := m.walkWhereForSeriesIds(n.LHS, filters)
			r, ir, rexpr := m.walkWhereForSeriesIds(n.RHS, filters)

			if il && ir { // we should include both the LHS and RHS of the BinaryExpr in the return
				if n.Op == influxql.AND {
					ids = l.intersect(r)
				} else if n.Op == influxql.OR {
					ids = l.union(r)
				}
			} else if !il && !ir { // we don't need to include either so return nothing
				return nil, false, nil
			} else if il { // just include the left side
				ids = l
			} else { // just include the right side
				ids = r
			}

			if n.Op == influxql.OR && il && ir && (lexpr == nil || rexpr == nil) {
				// if it's an OR and we're going to include both sides and one of those expression is nil,
				// we need to clear out restrictive filters on series that don't need them anymore
				idsToClear := l.intersect(r)
				for _, id := range idsToClear {
					delete(filters, id)
				}
			} else {
				// put the LHS field expression into the filters
				if lexpr != nil {
					for _, id := range ids {
						f := filters[id]
						if f == nil {
							filters[id] = lexpr
						} else {
							filters[id] = &influxql.BinaryExpr{LHS: f, RHS: lexpr, Op: n.Op}
						}
					}
				}

				// put the RHS field expression into the filters
				if rexpr != nil {
					for _, id := range ids {
						f := filters[id]
						if f == nil {
							filters[id] = rexpr
						} else {
							filters[id] = &influxql.BinaryExpr{LHS: f, RHS: rexpr, Op: n.Op}
						}
					}
				}

				// if the op is AND and we include both, clear out any of the non-intersecting ids.
				// that is, filters that are no longer part of the end result set
				if n.Op == influxql.AND && il && ir {
					filtersToClear := l.union(r).reject(ids)
					for _, id := range filtersToClear {
						delete(filters, id)
					}
				}
			}

			// finally return the ids and say that we should include them
			return ids, true, nil
		}

		return m.idsForExpr(n)
	case *influxql.ParenExpr:
		// walk down the tree
		return m.walkWhereForSeriesIds(n.Expr, filters)
	default:
		return nil, false, nil
	}
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
	return expandExprWithValues(expr, keys, []tagExpr{}, uniques, 0)
}

func expandExprWithValues(expr influxql.Expr, keys []string, tagExprs []tagExpr, uniques [][]string, index int) []tagSetExpr {
	// If we have no more keys left then execute the reduction and return.
	if index == len(keys) {
		// Create a map of tag key/values.
		m := make(map[string]*string, len(keys))
		for i, key := range keys {
			if tagExprs[i].op == influxql.EQ {
				m[key] = &tagExprs[i].values[0]
			} else {
				m[key] = nil
			}
		}

		// TODO: Rewrite full expressions instead of VarRef replacement.

		// Reduce using the current tag key/value set.
		// Ignore it if reduces down to "false".
		e := influxql.Reduce(expr, &tagValuer{tags: m})
		if e, ok := e.(*influxql.BooleanLiteral); ok && e.Val == false {
			return nil
		}

		return []tagSetExpr{{values: copyTagExprs(tagExprs), expr: e}}
	}

	// Otherwise expand for each possible equality value of the key.
	var exprs []tagSetExpr
	for _, v := range uniques[index] {
		exprs = append(exprs, expandExprWithValues(expr, keys, append(tagExprs, tagExpr{keys[index], []string{v}, influxql.EQ}), uniques, index+1)...)
	}
	exprs = append(exprs, expandExprWithValues(expr, keys, append(tagExprs, tagExpr{keys[index], uniques[index], influxql.NEQ}), uniques, index+1)...)

	return exprs
}

// seriesIDsAllOrByExpr walks an expressions for matching series IDs
// or, if no expressions is given, returns all series IDs for the measurement.
func (m *Measurement) seriesIDsAllOrByExpr(expr influxql.Expr) (seriesIDs, error) {
	// If no expression given or the measurement has no series,
	// we can take just return the ids or nil accordingly.
	if expr == nil {
		return m.seriesIDs, nil
	} else if len(m.seriesIDs) == 0 {
		return nil, nil
	}

	// Get series IDs that match the WHERE clause.
	filters := map[uint32]influxql.Expr{}
	ids, _, _ := m.walkWhereForSeriesIds(expr, filters)

	return ids, nil
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
	values []tagExpr
	expr   influxql.Expr
}

// tagExpr represents one or more values assigned to a given tag.
type tagExpr struct {
	key    string
	values []string
	op     influxql.Token // EQ or NEQ
}

func copyTagExprs(a []tagExpr) []tagExpr {
	other := make([]tagExpr, len(a))
	copy(other, a)
	return other
}

// uniqueTagValues returns a list of unique tag values used in an expression.
func (m *Measurement) uniqueTagValues(expr influxql.Expr) map[string][]string {
	// Track unique value per tag.
	tags := make(map[string]map[string]struct{})

	// Find all tag values referenced in the expression.
	influxql.WalkFunc(expr, func(n influxql.Node) {
		switch n := n.(type) {
		case *influxql.BinaryExpr:
			// Ignore operators that are not equality.
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

// Measurements represents a list of *Measurement.
type Measurements []*Measurement

func (a Measurements) Len() int           { return len(a) }
func (a Measurements) Less(i, j int) bool { return a[i].Name < a[j].Name }
func (a Measurements) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func (a Measurements) intersect(other Measurements) Measurements {
	l := a
	r := other

	// we want to iterate through the shortest one and stop
	if len(other) < len(a) {
		l = other
		r = a
	}

	// they're in sorted order so advance the counter as needed.
	// That is, don't run comparisons against lower values that we've already passed
	var i, j int

	result := make(Measurements, 0, len(l))
	for i < len(l) && j < len(r) {
		if l[i].Name == r[j].Name {
			result = append(result, l[i])
			i++
			j++
		} else if l[i].Name < r[j].Name {
			i++
		} else {
			j++
		}
	}

	return result
}

func (a Measurements) union(other Measurements) Measurements {
	result := make(Measurements, 0, len(a)+len(other))
	var i, j int
	for i < len(a) && j < len(other) {
		if a[i].Name == other[j].Name {
			result = append(result, a[i])
			i++
			j++
		} else if a[i].Name < other[j].Name {
			result = append(result, a[i])
			i++
		} else {
			result = append(result, other[j])
			j++
		}
	}

	// now append the remainder
	if i < len(a) {
		result = append(result, a[i:]...)
	} else if j < len(other) {
		result = append(result, other[j:]...)
	}

	return result
}

// Field represents a series field.
type Field struct {
	ID   uint8             `json:"id,omitempty"`
	Name string            `json:"name,omitempty"`
	Type influxql.DataType `json:"type,omitempty"`
}

// Fields represents a list of fields.
type Fields []*Field

// FieldCodec providecs encoding and decoding functionality for the fields of a given
// Measurement. It is a distinct type to avoid locking writes on this node while
// potentially long-running queries are executing.
//
// It is not affected by changes to the Measurement object after codec creation.
type FieldCodec struct {
	fieldsByID   map[uint8]*Field
	fieldsByName map[string]*Field
}

// NewFieldCodec returns a FieldCodec for the given Measurement. Must be called with
// a RLock that protects the Measurement.
func NewFieldCodec(m *Measurement) *FieldCodec {
	fieldsByID := make(map[uint8]*Field, len(m.Fields))
	fieldsByName := make(map[string]*Field, len(m.Fields))
	for _, f := range m.Fields {
		fieldsByID[f.ID] = f
		fieldsByName[f.Name] = f
	}
	return &FieldCodec{fieldsByID: fieldsByID, fieldsByName: fieldsByName}
}

// EncodeFields converts a map of values with string keys to a byte slice of field
// IDs and values.
//
// If a field exists in the codec, but its type is different, an error is returned. If
// a field is not present in the codec, the system panics.
func (f *FieldCodec) EncodeFields(values map[string]interface{}) ([]byte, error) {
	// Allocate byte slice
	b := make([]byte, 0, 10)

	for k, v := range values {
		field := f.fieldsByName[k]
		if field == nil {
			panic(fmt.Sprintf("field does not exist for %s", k))
		} else if influxql.InspectDataType(v) != field.Type {
			return nil, fmt.Errorf("field \"%s\" is type %T, mapped as type %s", k, k, field.Type)
		}

		var buf []byte

		switch field.Type {
		case influxql.Number:
			var value float64
			// Convert integers to floats.
			if intval, ok := v.(int); ok {
				value = float64(intval)
			} else {
				value = v.(float64)
			}

			buf = make([]byte, 9)
			binary.BigEndian.PutUint64(buf[1:9], math.Float64bits(value))
		case influxql.Boolean:
			value := v.(bool)

			// Only 1 byte need for a boolean.
			buf = make([]byte, 2)
			if value {
				buf[1] = byte(1)
			}
		case influxql.String:
			value := v.(string)
			if len(value) > maxStringLength {
				value = value[:maxStringLength]
			}
			// Make a buffer for field ID (1 bytes), the string length (2 bytes), and the string.
			buf = make([]byte, len(value)+3)

			// Set the string length, then copy the string itself.
			binary.BigEndian.PutUint16(buf[1:3], uint16(len(value)))
			for i, c := range []byte(value) {
				buf[i+3] = byte(c)
			}
		default:
			panic(fmt.Sprintf("unsupported value type: %T", v))
		}

		// Always set the field ID as the leading byte.
		buf[0] = field.ID

		// Append temp buffer to the end.
		b = append(b, buf...)
	}

	return b, nil
}

// DecodeByID scans a byte slice for a field with the given ID, converts it to its
// expected type, and return that value.
func (f *FieldCodec) DecodeByID(targetID uint8, b []byte) (interface{}, error) {
	if len(b) == 0 {
		return 0, ErrFieldNotFound
	}

	for {
		if len(b) < 1 {
			// No more bytes.
			break
		}
		field, ok := f.fieldsByID[b[0]]
		if !ok {
			panic(fmt.Sprintf("field ID %d has no mapping", b[0]))
		}

		var value interface{}
		switch field.Type {
		case influxql.Number:
			// Move bytes forward.
			value = math.Float64frombits(binary.BigEndian.Uint64(b[1:9]))
			b = b[9:]
		case influxql.Boolean:
			if b[1] == 1 {
				value = true
			} else {
				value = false
			}
			// Move bytes forward.
			b = b[2:]
		case influxql.String:
			size := binary.BigEndian.Uint16(b[1:3])
			value = string(b[3 : 3+size])
			// Move bytes forward.
			b = b[size+3:]
		default:
			panic(fmt.Sprintf("unsupported value type: %T", field.Type))
		}

		if field.ID == targetID {
			return value, nil
		}
	}

	return 0, ErrFieldNotFound
}

// DecodeFields decodes a byte slice into a set of field ids and values.
func (f *FieldCodec) DecodeFields(b []byte) map[uint8]interface{} {
	if len(b) == 0 {
		return nil
	}

	// Create a map to hold the decoded data.
	values := make(map[uint8]interface{}, 0)

	for {
		if len(b) < 1 {
			// No more bytes.
			break
		}

		// First byte is the field identifier.
		fieldID := b[0]
		field := f.fieldsByID[fieldID]
		if field == nil {
			panic(fmt.Sprintf("field ID %d has no mapping", fieldID))
		}

		var value interface{}
		switch field.Type {
		case influxql.Number:
			value = math.Float64frombits(binary.BigEndian.Uint64(b[1:9]))
			// Move bytes forward.
			b = b[9:]
		case influxql.Boolean:
			if b[1] == 1 {
				value = true
			} else {
				value = false
			}
			// Move bytes forward.
			b = b[2:]
		case influxql.String:
			size := binary.BigEndian.Uint16(b[1:3])
			value = string(b[3 : size+3])
			// Move bytes forward.
			b = b[size+3:]
		default:
			panic(fmt.Sprintf("unsupported value type: %T", f.fieldsByID[fieldID]))
		}

		values[fieldID] = value

	}

	return values
}

// DecodeFieldsWithNames decodes a byte slice into a set of field names and values
func (f *FieldCodec) DecodeFieldsWithNames(b []byte) map[string]interface{} {
	fields := f.DecodeFields(b)
	m := make(map[string]interface{})
	for id, v := range fields {
		field := f.fieldsByID[id]
		if field != nil {
			m[field.Name] = v
		}
	}
	return m
}

// FieldByName returns the field by its name. It will return a nil if not found
func (f *FieldCodec) FieldByName(name string) *Field {
	return f.fieldsByName[name]
}

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

// seriesIDs is a convenience type for sorting, checking equality, and doing
// union and intersection of collections of series ids.
type seriesIDs []uint32

func (a seriesIDs) Len() int           { return len(a) }
func (a seriesIDs) Less(i, j int) bool { return a[i] < a[j] }
func (a seriesIDs) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// equals assumes that both are sorted.
func (a seriesIDs) equals(other seriesIDs) bool {
	if len(a) != len(other) {
		return false
	}
	for i, s := range other {
		if a[i] != s {
			return false
		}
	}
	return true
}

// intersect returns a new collection of series ids in sorted order that is the intersection of the two.
// The two collections must already be sorted.
func (a seriesIDs) intersect(other seriesIDs) seriesIDs {
	l := a
	r := other

	// we want to iterate through the shortest one and stop
	if len(other) < len(a) {
		l = other
		r = a
	}

	// they're in sorted order so advance the counter as needed.
	// That is, don't run comparisons against lower values that we've already passed
	var i, j int

	ids := make([]uint32, 0, len(l))
	for i < len(l) && j < len(r) {
		if l[i] == r[j] {
			ids = append(ids, l[i])
			i++
			j++
		} else if l[i] < r[j] {
			i++
		} else {
			j++
		}
	}

	return seriesIDs(ids)
}

// union returns a new collection of series ids in sorted order that is the union of the two.
// The two collections must already be sorted.
func (a seriesIDs) union(other seriesIDs) seriesIDs {
	l := a
	r := other
	ids := make([]uint32, 0, len(l)+len(r))
	var i, j int
	for i < len(l) && j < len(r) {
		if l[i] == r[j] {
			ids = append(ids, l[i])
			i++
			j++
		} else if l[i] < r[j] {
			ids = append(ids, l[i])
			i++
		} else {
			ids = append(ids, r[j])
			j++
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

// reject returns a new collection of series ids in sorted order with the passed in set removed from the original.
// This is useful for the NOT operator. The two collections must already be sorted.
func (a seriesIDs) reject(other seriesIDs) seriesIDs {
	l := a
	r := other
	var i, j int

	ids := make([]uint32, 0, len(l))
	for i < len(l) && j < len(r) {
		if l[i] == r[j] {
			i++
			j++
		} else if l[i] < r[j] {
			ids = append(ids, l[i])
			i++
		} else {
			j++
		}
	}

	// Append the remainder
	if i < len(l) {
		ids = append(ids, l[i:]...)
	}

	return seriesIDs(ids)
}

// RetentionPolicy represents a policy for creating new shards in a database and how long they're kept around for.
type RetentionPolicy struct {
	// Unique name within database. Required.
	Name string `json:"name"`

	// Length of time to keep data around. A zero duration means keep the data forever.
	Duration time.Duration `json:"duration"`

	// Length of time to create shard groups in.
	ShardGroupDuration time.Duration `json:"shardGroupDuration"`

	// The number of copies to make of each shard.
	ReplicaN uint32 `json:"replicaN"`

	shardGroups []*ShardGroup
}

// RetentionPolicies represents a list of retention policies.
type RetentionPolicies []*RetentionPolicy

func (a RetentionPolicies) Len() int           { return len(a) }
func (a RetentionPolicies) Less(i, j int) bool { return a[i].Name < a[j].Name }
func (a RetentionPolicies) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

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

// shardGroupByID returns the group in the policy for the given ID.
// Returns nil if group does not exist.
func (rp *RetentionPolicy) shardGroupByID(shardID uint64) *ShardGroup {
	for _, g := range rp.shardGroups {
		if g.ID == shardID {
			return g
		}
	}
	return nil
}

// dropMeasurement will remove a measurement from:
//    In memory index.
//    Series data from the shards.
func (db *database) dropMeasurement(name string) error {
	if _, ok := db.measurements[name]; !ok {
		return nil
	}

	// remove measurement from in memory index
	delete(db.measurements, name)

	// collect the series ids to remove
	var ids []uint32

	// remove series from in memory map
	for id, series := range db.series {
		if series.measurement.Name == name {
			ids = append(ids, id)
			delete(db.series, id)
		}
	}

	// remove series data from shards
	for _, rp := range db.policies {
		if err := rp.dropSeries(ids...); err != nil {
			return err
		}
	}

	return nil
}

// dropSeries will delete all data with the seriesID
func (rp *RetentionPolicy) dropSeries(seriesIDs ...uint32) error {
	for _, g := range rp.shardGroups {
		err := g.dropSeries(seriesIDs...)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rp *RetentionPolicy) removeShardGroupByID(shardID uint64) {
	for i, g := range rp.shardGroups {
		if g.ID == shardID {
			rp.shardGroups[i] = nil
			rp.shardGroups = append(rp.shardGroups[:i], rp.shardGroups[i+1:]...)
		}
	}
}

// MarshalJSON encodes a retention policy to a JSON-encoded byte slice.
func (rp *RetentionPolicy) MarshalJSON() ([]byte, error) {
	var o retentionPolicyJSON
	o.Name = rp.Name
	o.Duration = rp.Duration
	o.ShardGroupDuration = rp.ShardGroupDuration
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
	rp.ShardGroupDuration = o.ShardGroupDuration
	rp.shardGroups = o.ShardGroups

	return nil
}

// retentionPolicyJSON represents an intermediate struct for JSON marshaling.
type retentionPolicyJSON struct {
	Name               string        `json:"name"`
	ReplicaN           uint32        `json:"replicaN,omitempty"`
	Duration           time.Duration `json:"duration,omitempty"`
	ShardGroupDuration time.Duration `json:"shardGroupDuration"`
	ShardGroups        []*ShardGroup `json:"shardGroups,omitempty"`
}

// TagFilter represents a tag filter when looking up other tags or measurements.
type TagFilter struct {
	Op    influxql.Token
	Key   string
	Value string
	Regex *regexp.Regexp
}

// addSeriesToIndex adds the series for the given measurement to the index. Returns false if already present
func (db *database) addSeriesToIndex(measurementName string, s *Series) bool {
	// if there is a measurement for this id, it's already been added
	if db.series[s.ID] != nil {
		return false
	}

	// get or create the measurement index and index it globally and in the measurement
	idx := db.createMeasurementIfNotExists(measurementName)

	s.measurement = idx
	db.series[s.ID] = s

	// TODO: add this series to the global tag index

	return idx.addSeries(s)
}

// dropSeries removes the series from the in memory references
func (db *database) dropSeries(seriesByMeasurement map[string][]uint32) error {
	for measurement, ids := range seriesByMeasurement {
		for _, id := range ids {
			// if the series is already gone, return
			if db.series[id] == nil {
				continue
			}

			delete(db.series, id)

			// Remove series information from measurements
			db.measurements[measurement].dropSeries(id)

			// Remove shard data
			for _, rp := range db.policies {
				if err := rp.dropSeries(id); err != nil {
					return fmt.Errorf("database.retentionPolicies.dropSeries: %s", err)
				}
			}
		}
	}

	return nil
}

// createMeasurementIfNotExists will either add a measurement object to the index or return the existing one.
func (db *database) createMeasurementIfNotExists(name string) *Measurement {
	idx := db.measurements[name]
	if idx == nil {
		idx = NewMeasurement(name)
		db.measurements[name] = idx
		db.names = append(db.names, name)
		sort.Strings(db.names)
	}
	return idx
}

// MeasurementAndSeries returns the Measurement and the Series for a given measurement name and tag set.
func (db *database) MeasurementAndSeries(name string, tags map[string]string) (*Measurement, *Series) {
	idx := db.measurements[name]
	if idx == nil {
		return nil, nil
	}
	return idx, idx.seriesByTags(tags)
}

// SeriesByID returns the Series that has the given id.
func (db *database) SeriesByID(id uint32) *Series {
	return db.series[id]
}

// Names returns all measurement names in sorted order.
func (db *database) MeasurementNames() []string {
	return db.names
}

// DropSeries will clear the index of all references to a series.
func (db *database) DropSeries(id uint32) {
	panic("not implemented")
}

// DropMeasurement will clear the index of all references to a measurement and its child series.
func (db *database) DropMeasurement(name string) {
	panic("not implemented")
}

func (db *database) continuousQueryByName(name string) *ContinuousQuery {
	for _, cq := range db.continuousQueries {
		if cq.cq.Name == name {
			return cq
		}
	}
	return nil
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

// measurementsByExpr takes and expression containing only tags and returns
// a list of matching *Measurement.
func (db *database) measurementsByExpr(expr influxql.Expr) (Measurements, error) {
	switch e := expr.(type) {
	case *influxql.BinaryExpr:
		switch e.Op {
		case influxql.EQ, influxql.NEQ, influxql.EQREGEX, influxql.NEQREGEX:
			tag, ok := e.LHS.(*influxql.VarRef)
			if !ok {
				return nil, fmt.Errorf("left side of '%s' must be a tag name", e.Op.String())
			}

			tf := &TagFilter{
				Op:  e.Op,
				Key: tag.Val,
			}

			if influxql.IsRegexOp(e.Op) {
				re, ok := e.RHS.(*influxql.RegexLiteral)
				if !ok {
					return nil, fmt.Errorf("right side of '%s' must be a regular expression", e.Op.String())
				}
				tf.Regex = re.Val
			} else {
				s, ok := e.RHS.(*influxql.StringLiteral)
				if !ok {
					return nil, fmt.Errorf("right side of '%s' must be a tag value string", e.Op.String())
				}
				tf.Value = s.Val
			}

			return db.measurementsByTagFilters([]*TagFilter{tf}), nil
		case influxql.OR, influxql.AND:
			lhsIDs, err := db.measurementsByExpr(e.LHS)
			if err != nil {
				return nil, err
			}

			rhsIDs, err := db.measurementsByExpr(e.RHS)
			if err != nil {
				return nil, err
			}

			if e.Op == influxql.OR {
				return lhsIDs.union(rhsIDs), nil
			}

			return lhsIDs.intersect(rhsIDs), nil
		default:
			return nil, fmt.Errorf("invalid operator")
		}
	case *influxql.ParenExpr:
		return db.measurementsByExpr(e.Expr)
	}
	return nil, fmt.Errorf("%#v", expr)
}

func (db *database) measurementsByTagFilters(filters []*TagFilter) Measurements {
	// If no filters, then return all measurements.
	if len(filters) == 0 {
		measurements := make(Measurements, 0, len(db.measurements))
		for _, m := range db.measurements {
			measurements = append(measurements, m)
		}
		return measurements
	}

	// Build a list of measurements matching the filters.
	var measurements Measurements
	var tagMatch bool

	// Iterate through all measurements in the database.
	for _, m := range db.measurements {
		// Iterate filters seeing if the measurement has a matching tag.
		for _, f := range filters {
			tagVals, ok := m.seriesByTagKeyValue[f.Key]
			if !ok {
				continue
			}

			tagMatch = false

			// If the operator is non-regex, only check the specified value.
			if f.Op == influxql.EQ || f.Op == influxql.NEQ {
				if _, ok := tagVals[f.Value]; ok {
					tagMatch = true
				}
			} else {
				// Else, the operator is regex and we have to check all tag
				// values against the regular expression.
				for tagVal := range tagVals {
					if f.Regex.MatchString(tagVal) {
						tagMatch = true
						break
					}
				}
			}

			isEQ := (f.Op == influxql.EQ || f.Op == influxql.EQREGEX)

			// tags match | operation is EQ | measurement matches
			// --------------------------------------------------
			//     True   |       True      |      True
			//     True   |       False     |      False
			//     False  |       True      |      False
			//     False  |       False     |      True

			if tagMatch == isEQ {
				measurements = append(measurements, m)
				break
			}
		}
	}

	return measurements
}

// measurementsByRegex returns the measurements that match the regex.
func (db *database) measurementsByRegex(re *regexp.Regexp) Measurements {
	var matches Measurements
	for _, m := range db.measurements {
		if re.MatchString(m.Name) {
			matches = append(matches, m)
		}
	}
	return matches
}

// Measurements returns a list of all measurements.
func (db *database) Measurements() Measurements {
	measurements := make(Measurements, 0, len(db.measurements))
	for _, m := range db.measurements {
		measurements = append(measurements, m)
	}
	return measurements
}

// tagKeys returns a list of the measurement's tag names.
func (m *Measurement) tagKeys() []string {
	keys := make([]string, 0, len(m.seriesByTagKeyValue))
	for k := range m.seriesByTagKeyValue {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (m *Measurement) tagValuesByKeyAndSeriesID(tagKeys []string, ids seriesIDs) map[string]stringSet {
	// If no tag keys were passed, get all tag keys for the measurement.
	if len(tagKeys) == 0 {
		for k := range m.seriesByTagKeyValue {
			tagKeys = append(tagKeys, k)
		}
	}

	// Mapping between tag keys to all existing tag values.
	tagValues := make(map[string]stringSet, 0)

	// Iterate all series to collect tag values.
	for _, id := range ids {
		s, ok := m.seriesByID[id]
		if !ok {
			continue
		}

		// Iterate the tag keys we're interested in and collect values
		// from this series, if they exist.
		for _, tagKey := range tagKeys {
			tagKey = strings.Trim(tagKey, `"`)
			if tagVal, ok := s.Tags[tagKey]; ok {
				if _, ok = tagValues[tagKey]; !ok {
					tagValues[tagKey] = newStringSet()
				}
				tagValues[tagKey].add(tagVal)
			}
		}
	}

	return tagValues
}

type stringSet map[string]struct{}

func newStringSet() stringSet {
	return make(map[string]struct{})
}

func (s stringSet) add(ss string) {
	s[ss] = struct{}{}
}

func (s stringSet) contains(ss string) bool {
	_, ok := s[ss]
	return ok
}

func (s stringSet) list() []string {
	l := make([]string, 0, len(s))
	for k := range s {
		l = append(l, k)
	}
	return l
}

func (s stringSet) union(o stringSet) stringSet {
	ns := newStringSet()
	for k := range s {
		ns[k] = struct{}{}
	}
	for k := range o {
		ns[k] = struct{}{}
	}
	return ns
}

func (s stringSet) intersect(o stringSet) stringSet {
	ns := newStringSet()
	for k := range s {
		if _, ok := o[k]; ok {
			ns[k] = struct{}{}
		}
	}
	for k := range o {
		if _, ok := s[k]; ok {
			ns[k] = struct{}{}
		}
	}
	return ns
}
