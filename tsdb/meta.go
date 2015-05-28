package tsdb

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/influxdb/influxdb/influxql"
)

const (
	maxStringLength = 64 * 1024
)

// DatabaseIndex is the in memory index of a collection of measurements, time series, and their tags.
type DatabaseIndex struct {
	// in memory metadata index, built on load and updated when new series come in
	mu           sync.RWMutex
	measurements map[string]*Measurement // measurement name to object and index
	series       map[string]*Series      // map series key to the Series object
	names        []string                // sorted list of the measurement names
	lastID       uint64                  // last used series ID. They're in memory only for this shard
}

func NewDatabaseIndex() *DatabaseIndex {
	return &DatabaseIndex{
		measurements: make(map[string]*Measurement),
		series:       make(map[string]*Series),
		names:        make([]string, 0),
	}
}

// createSeriesIndexIfNotExists adds the series for the given measurement to the index and sets its ID or returns the existing series object
func (s *DatabaseIndex) createSeriesIndexIfNotExists(measurementName string, series *Series) *Series {
	// if there is a measurement for this id, it's already been added
	ss := s.series[series.Key]
	if ss != nil {
		return ss
	}

	// get or create the measurement index
	m := s.createMeasurementIndexIfNotExists(measurementName)

	// set the in memory ID for query processing on this shard
	series.id = s.lastID + 1
	s.lastID += 1

	series.measurement = m
	s.series[series.Key] = series

	m.addSeries(series)

	return series
}

// addMeasurementToIndexIfNotExists creates or retrieves an in memory index object for the measurement
func (s *DatabaseIndex) createMeasurementIndexIfNotExists(name string) *Measurement {
	m := s.measurements[name]
	if m == nil {
		m = NewMeasurement(name, s)
		s.measurements[name] = m
		s.names = append(s.names, name)
		sort.Strings(s.names)
	}
	return m
}

// measurementsByExpr takes and expression containing only tags and returns
// a list of matching *Measurement.
func (db *DatabaseIndex) measurementsByExpr(expr influxql.Expr) (Measurements, error) {
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

func (db *DatabaseIndex) measurementsByTagFilters(filters []*TagFilter) Measurements {
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
func (db *DatabaseIndex) measurementsByRegex(re *regexp.Regexp) Measurements {
	var matches Measurements
	for _, m := range db.measurements {
		if re.MatchString(m.Name) {
			matches = append(matches, m)
		}
	}
	return matches
}

// Measurements returns a list of all measurements.
func (db *DatabaseIndex) Measurements() Measurements {
	measurements := make(Measurements, 0, len(db.measurements))
	for _, m := range db.measurements {
		measurements = append(measurements, m)
	}
	return measurements
}

// Measurement represents a collection of time series in a database. It also contains in memory
// structures for indexing tags. These structures are accessed through private methods on the Measurement
// object. Generally these methods are only accessed from Index, which is responsible for ensuring
// go routine safe access.
type Measurement struct {
	Name       string              `json:"name,omitempty"`
	FieldNames map[string]struct{} `json:"fieldNames,omitempty"`
	index      *DatabaseIndex

	// in-memory index fields
	series              map[string]*Series // sorted tagset string to the series object
	seriesByID          map[uint64]*Series // lookup table for series by their id
	measurement         *Measurement
	seriesByTagKeyValue map[string]map[string]seriesIDs // map from tag key to value to sorted set of series ids
	seriesIDs           seriesIDs                       // sorted list of series IDs in this measurement
}

// NewMeasurement allocates and initializes a new Measurement.
func NewMeasurement(name string, idx *DatabaseIndex) *Measurement {
	return &Measurement{
		Name:       name,
		FieldNames: make(map[string]struct{}),
		index:      idx,

		series:              make(map[string]*Series),
		seriesByID:          make(map[uint64]*Series),
		seriesByTagKeyValue: make(map[string]map[string]seriesIDs),
		seriesIDs:           make(seriesIDs, 0),
	}
}

// HasTagKey returns true if at least one eries in this measurement has written a value for the passed in tag key
func (m *Measurement) HasTagKey(k string) bool {
	return m.seriesByTagKeyValue[k] != nil
}

// addSeries will add a series to the measurementIndex. Returns false if already present
func (m *Measurement) addSeries(s *Series) bool {
	if _, ok := m.seriesByID[s.id]; ok {
		return false
	}
	m.seriesByID[s.id] = s
	tagset := string(marshalTags(s.Tags))
	m.series[tagset] = s
	m.seriesIDs = append(m.seriesIDs, s.id)

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
		ids = append(ids, s.id)

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
func (m *Measurement) dropSeries(seriesID uint64) bool {
	if _, ok := m.seriesByID[seriesID]; !ok {
		return true
	}
	s := m.seriesByID[seriesID]
	tagset := string(marshalTags(s.Tags))

	delete(m.series, tagset)
	delete(m.seriesByID, seriesID)

	var ids []uint64
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
			var ids []uint64
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
func (m *Measurement) filters(stmt *influxql.SelectStatement) (map[uint64]influxql.Expr, error) {
	seriesIdsToExpr := make(map[uint64]influxql.Expr)

	if stmt.Condition == nil || stmt.OnlyTimeDimensions() {
		for _, id := range m.seriesIDs {
			seriesIdsToExpr[id] = nil
		}
		return seriesIdsToExpr, nil
	}

	ids, _, _, err := m.walkWhereForSeriesIds(stmt.Condition, seriesIdsToExpr)
	if err != nil {
		return nil, err
	}

	// ensure every id is in the map
	for _, id := range ids {
		if _, ok := seriesIdsToExpr[id]; !ok {
			seriesIdsToExpr[id] = nil
		}
	}

	return seriesIdsToExpr, nil
}

// tagSets returns the unique tag sets that exist for the given tag keys. This is used to determine
// what composite series will be created by a group by. i.e. "group by region" should return:
// {"region":"uswest"}, {"region":"useast"}
// or region, service returns
// {"region": "uswest", "service": "redis"}, {"region": "uswest", "service": "mysql"}, etc...
// This will also populate the TagSet objects with the series IDs that match each tagset and any
// influx filter expression that goes with the series
// TODO: this shouldn't be exported. However, until tx.go and the engine get refactored into tsdb, we need it.
func (m *Measurement) TagSets(stmt *influxql.SelectStatement, dimensions []string) ([]*influxql.TagSet, error) {
	m.index.mu.RLock()
	defer m.index.mu.RUnlock()

	// get the unique set of series ids and the filters that should be applied to each
	filters, err := m.filters(stmt)
	if err != nil {
		return nil, err
	}

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
		set.AddFilter(m.seriesByID[id].Key, filter)
		tagSets[t] = set
	}

	// return the tag sets in sorted order
	a := make([]*influxql.TagSet, 0, len(tagSets))
	sort.Strings(tagStrings)
	for _, s := range tagStrings {
		a = append(a, tagSets[s])
	}

	return a, nil
}

// idsForExpr will return a collection of series ids, a bool indicating if the result should be
// used (it'll be false if it's a time expr) and a field expression if the passed in expression is against a field.
func (m *Measurement) idsForExpr(n *influxql.BinaryExpr) (seriesIDs, bool, influxql.Expr, error) {
	name, ok := n.LHS.(*influxql.VarRef)
	value := n.RHS
	if !ok {
		name, ok = n.RHS.(*influxql.VarRef)
		if !ok {
			return nil, false, nil, fmt.Errorf("invalid expression: %s", n.String())
		}
		value = n.LHS
	}

	// ignore time literals
	if _, ok := value.(*influxql.TimeLiteral); ok || name.Val == "time" {
		return nil, false, nil, nil
	}

	// if it's a field we can't collapse it so we have to look at all series ids for this
	if _, present := m.FieldNames[name.Val]; present {
		return m.seriesIDs, true, n, nil
	}

	tagVals, ok := m.seriesByTagKeyValue[name.Val]
	if !ok {
		return nil, true, nil, nil
	}

	// if we're looking for series with specific tag values
	if str, ok := value.(*influxql.StringLiteral); ok {
		var ids seriesIDs

		if n.Op == influxql.EQ {
			// return series that have a tag of specific value.
			ids = tagVals[str.Val]
		} else if n.Op == influxql.NEQ {
			ids = m.seriesIDs.reject(tagVals[str.Val])
		}
		return ids, true, nil, nil
	}

	// if we're looking for series with tag values that match a regex
	if re, ok := value.(*influxql.RegexLiteral); ok {
		var ids seriesIDs

		// The operation is a NEQREGEX, code must start by assuming all match, even
		// series without any tags.
		if n.Op == influxql.NEQREGEX {
			ids = m.seriesIDs
		}

		for k := range tagVals {
			match := re.Val.MatchString(k)

			if match && n.Op == influxql.EQREGEX {
				ids = ids.union(tagVals[k])
			} else if match && n.Op == influxql.NEQREGEX {
				ids = ids.reject(tagVals[k])
			}
		}
		return ids, true, nil, nil
	}

	return nil, true, nil, nil
}

// walkWhereForSeriesIds will recursively walk the where clause and return a collection of series ids, a boolean indicating if this return
// value should be included in the resulting set, and an expression if the return is a field expression.
// The map that it takes maps each series id to the field expression that should be used to evaluate it when iterating over its cursor.
// Series that have no field expressions won't be in the map
func (m *Measurement) walkWhereForSeriesIds(expr influxql.Expr, filters map[uint64]influxql.Expr) (seriesIDs, bool, influxql.Expr, error) {
	switch n := expr.(type) {
	case *influxql.BinaryExpr:
		switch n.Op {
		case influxql.EQ, influxql.NEQ, influxql.LT, influxql.LTE, influxql.GT, influxql.GTE, influxql.EQREGEX, influxql.NEQREGEX:
			// if it's a compare, then it's either a field expression or against a tag. we can return this
			ids, shouldInclude, expr, err := m.idsForExpr(n)
			if err != nil {
				return nil, false, nil, err
			}

			for _, id := range ids {
				filters[id] = expr
			}

			return ids, shouldInclude, expr, nil
		case influxql.AND, influxql.OR:
			// if it's an AND or OR we need to union or intersect the results
			var ids seriesIDs
			l, il, lexpr, err := m.walkWhereForSeriesIds(n.LHS, filters)
			if err != nil {
				return nil, false, nil, err
			}

			r, ir, rexpr, err := m.walkWhereForSeriesIds(n.RHS, filters)
			if err != nil {
				return nil, false, nil, err
			}

			if il && ir { // we should include both the LHS and RHS of the BinaryExpr in the return
				if n.Op == influxql.AND {
					ids = l.intersect(r)
				} else if n.Op == influxql.OR {
					ids = l.union(r)
				}
			} else if !il && !ir { // we don't need to include either so return nothing
				return nil, false, nil, nil
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
			return ids, true, nil, nil
		}

		return m.idsForExpr(n)
	case *influxql.ParenExpr:
		// walk down the tree
		return m.walkWhereForSeriesIds(n.Expr, filters)
	default:
		return nil, false, nil, nil
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
	filters := map[uint64]influxql.Expr{}
	ids, _, _, err := m.walkWhereForSeriesIds(expr, filters)
	if err != nil {
		return nil, err
	}

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

// Series belong to a Measurement and represent unique time series in a database
type Series struct {
	Key  string
	Tags map[string]string

	id          uint64
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
type seriesIDs []uint64

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

	ids := make([]uint64, 0, len(l))
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
	ids := make([]uint64, 0, len(l)+len(r))
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

	ids := make([]uint64, 0, len(l))
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

// TagFilter represents a tag filter when looking up other tags or measurements.
type TagFilter struct {
	Op    influxql.Token
	Key   string
	Value string
	Regex *regexp.Regexp
}

// used to convert the tag set to bytes for use as a lookup key
func marshalTags(tags map[string]string) []byte {
	// Empty maps marshal to empty bytes.
	if len(tags) == 0 {
		return nil
	}

	// Extract keys and determine final size.
	sz := (len(tags) * 2) - 1 // separators
	keys := make([]string, 0, len(tags))
	for k, v := range tags {
		keys = append(keys, k)
		sz += len(k) + len(v)
	}
	sort.Strings(keys)

	// Generate marshaled bytes.
	b := make([]byte, sz)
	buf := b
	for _, k := range keys {
		copy(buf, k)
		buf[len(k)] = '|'
		buf = buf[len(k)+1:]
	}
	for i, k := range keys {
		v := tags[k]
		copy(buf, v)
		if i < len(keys)-1 {
			buf[len(v)] = '|'
			buf = buf[len(v)+1:]
		}
	}
	return b
}

// timeBetweenInclusive returns true if t is between min and max, inclusive.
func timeBetweenInclusive(t, min, max time.Time) bool {
	return (t.Equal(min) || t.After(min)) && (t.Equal(max) || t.Before(max))
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

func measurementFromSeriesKey(key string) string {
	return key[:strings.Index(key, ",")]
}
