/*
Package inmem implements a shared, in-memory index for each database.

The in-memory index is the original index implementation and provides fast
access to index data. However, it also forces high memory usage for large
datasets and can cause OOM errors.

Index is the shared index structure that provides most of the functionality.
However, ShardIndex is a light per-shard wrapper that adapts this original
shared index format to the new per-shard format.
*/
package inmem

import (
	"fmt"
	"regexp"
	"sort"
	"sync"
	// "sync/atomic"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/escape"
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/pkg/estimator/hll"
	"github.com/influxdata/influxdb/tsdb"
)

func init() {
	// Inject shared index constructor into tsdb to avoid circular dependency.
	tsdb.NewInmemIndex = func(name string) (interface{}, error) { return NewIndex(name) }

	// Register index format.
	tsdb.RegisterIndex("inmem", NewShardIndex)
}

// Index is the in memory index of a collection of measurements, time
// series, and their tags. Exported functions are goroutine safe while
// un-exported functions assume the caller will use the appropriate locks.
type Index struct {
	// In-memory metadata index, built on load and updated when new series come in
	mu           sync.RWMutex
	measurements map[string]*tsdb.Measurement // measurement name to object and index
	series       map[string]*tsdb.Series      // map series key to the Series object
	lastID       uint64                       // last used series ID. They're in memory only for this shard

	seriesSketch, seriesTSSketch             *hll.Plus
	measurementsSketch, measurementsTSSketch *hll.Plus

	name string // name of the database represented by this index
}

// NewIndex returns a new initialized Index.
func NewIndex(name string) (index *Index, err error) {
	index = &Index{
		measurements: make(map[string]*tsdb.Measurement),
		series:       make(map[string]*tsdb.Series),
		name:         name,
	}

	if index.seriesSketch, err = hll.NewPlus(16); err != nil {
		return nil, err
	} else if index.seriesTSSketch, err = hll.NewPlus(16); err != nil {
		return nil, err
	} else if index.measurementsSketch, err = hll.NewPlus(16); err != nil {
		return nil, err
	} else if index.measurementsTSSketch, err = hll.NewPlus(16); err != nil {
		return nil, err
	}

	return index, nil
}

func (i *Index) Open() (err error) { return nil }
func (i *Index) Close() error      { return nil }

// Series returns a series by key.
func (i *Index) Series(key []byte) (*tsdb.Series, error) {
	i.mu.RLock()
	s := i.series[string(key)]
	i.mu.RUnlock()
	return s, nil
}

// SeriesN returns the exact number of series in the index.
func (i *Index) SeriesN() (uint64, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return uint64(len(i.series)), nil
}

// SeriesSketch returns the sketch for the series.
func (i *Index) SeriesSketches() (estimator.Sketch, estimator.Sketch, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.seriesSketch, i.seriesTSSketch, nil
}

// Measurement returns the measurement object from the index by the name
func (i *Index) Measurement(name []byte) (*tsdb.Measurement, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.measurements[string(name)], nil
}

// MeasurementsSketch returns the sketch for the series.
func (i *Index) MeasurementsSketches() (estimator.Sketch, estimator.Sketch, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.measurementsSketch, i.measurementsTSSketch, nil
}

// MeasurementsByName returns a list of measurements.
func (i *Index) MeasurementsByName(names [][]byte) ([]*tsdb.Measurement, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	a := make([]*tsdb.Measurement, 0, len(names))
	for _, name := range names {
		if m := i.measurements[string(name)]; m != nil {
			a = append(a, m)
		}
	}
	return a, nil
}

// CreateSeriesIfNotExists adds the series for the given measurement to the
// index and sets its ID or returns the existing series object
func (i *Index) CreateSeriesIfNotExists(shardID uint64, name []byte, tags models.Tags, opt *tsdb.EngineOptions) error {
	key := models.MakeKey(name, tags)

	i.mu.RLock()
	// if there is a measurement for this id, it's already been added
	ss := i.series[string(key)]
	if ss != nil {
		ss.AssignShard(shardID)
		i.mu.RUnlock()
		return nil
	}
	i.mu.RUnlock()

	// Check for series count.
	n, err := i.SeriesN()
	if err != nil {
		return err
	}
	if opt.Config.MaxSeriesPerDatabase > 0 && n+1 > uint64(opt.Config.MaxSeriesPerDatabase) {
		return &tsdb.LimitError{
			Reason: fmt.Sprintf("max-series-per-database limit exceeded: (%d/%d)", n, opt.Config.MaxSeriesPerDatabase),
		}
	}

	// get or create the measurement index
	m := i.CreateMeasurementIndexIfNotExists(string(name))

	i.mu.Lock()
	// Check for the series again under a write lock
	ss = i.series[string(key)]
	if ss != nil {
		ss.AssignShard(shardID)
		i.mu.Unlock()
		return nil
	}

	// set the in memory ID for query processing on this shard
	series := tsdb.NewSeries(key, tags)
	series.ID = i.lastID + 1
	i.lastID++

	series.SetMeasurement(m)
	i.series[string(key)] = series

	m.AddSeries(series)
	series.AssignShard(shardID)

	// Add the series to the series sketch.
	i.seriesSketch.Add(key)
	i.mu.Unlock()

	return nil
}

// CreateMeasurementIndexIfNotExists creates or retrieves an in memory index
// object for the measurement
func (i *Index) CreateMeasurementIndexIfNotExists(name string) *tsdb.Measurement {
	name = escape.UnescapeString(name)

	// See if the measurement exists using a read-lock
	i.mu.RLock()
	m := i.measurements[name]
	if m != nil {
		i.mu.RUnlock()
		return m
	}
	i.mu.RUnlock()

	// Doesn't exist, so lock the index to create it
	i.mu.Lock()
	defer i.mu.Unlock()

	// Make sure it was created in between the time we released our read-lock
	// and acquire the write lock
	m = i.measurements[name]
	if m == nil {
		m = tsdb.NewMeasurement(name)
		i.measurements[name] = m

		// Add the measurement to the measurements sketch.
		i.measurementsSketch.Add([]byte(name))
	}
	return m
}

// TagsForSeries returns the tag map for the passed in series
func (i *Index) TagsForSeries(key string) (models.Tags, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	ss := i.series[key]
	if ss == nil {
		return nil, nil
	}
	return ss.Tags, nil
}

// MeasurementsByExpr takes an expression containing only tags and returns a
// list of matching *tsdb.Measurement. The bool return argument returns if the
// expression was a measurement expression. It is used to differentiate a list
// of no measurements because all measurements were filtered out (when the bool
// is true) against when there are no measurements because the expression
// wasn't evaluated (when the bool is false).
func (i *Index) MeasurementsByExpr(expr influxql.Expr) (tsdb.Measurements, bool, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.measurementsByExpr(expr)
}

func (i *Index) measurementsByExpr(expr influxql.Expr) (tsdb.Measurements, bool, error) {
	if expr == nil {
		return nil, false, nil
	}

	switch e := expr.(type) {
	case *influxql.BinaryExpr:
		switch e.Op {
		case influxql.EQ, influxql.NEQ, influxql.EQREGEX, influxql.NEQREGEX:
			tag, ok := e.LHS.(*influxql.VarRef)
			if !ok {
				return nil, false, fmt.Errorf("left side of '%s' must be a tag key", e.Op.String())
			}

			tf := &tsdb.TagFilter{
				Op:  e.Op,
				Key: tag.Val,
			}

			if influxql.IsRegexOp(e.Op) {
				re, ok := e.RHS.(*influxql.RegexLiteral)
				if !ok {
					return nil, false, fmt.Errorf("right side of '%s' must be a regular expression", e.Op.String())
				}
				tf.Regex = re.Val
			} else {
				s, ok := e.RHS.(*influxql.StringLiteral)
				if !ok {
					return nil, false, fmt.Errorf("right side of '%s' must be a tag value string", e.Op.String())
				}
				tf.Value = s.Val
			}

			// Match on name, if specified.
			if tag.Val == "_name" {
				return i.measurementsByNameFilter(tf.Op, tf.Value, tf.Regex), true, nil
			} else if influxql.IsSystemName(tag.Val) {
				return nil, false, nil
			}

			return i.measurementsByTagFilters([]*tsdb.TagFilter{tf}), true, nil
		case influxql.OR, influxql.AND:
			lhsIDs, lhsOk, err := i.measurementsByExpr(e.LHS)
			if err != nil {
				return nil, false, err
			}

			rhsIDs, rhsOk, err := i.measurementsByExpr(e.RHS)
			if err != nil {
				return nil, false, err
			}

			if lhsOk && rhsOk {
				if e.Op == influxql.OR {
					return lhsIDs.Union(rhsIDs), true, nil
				}

				return lhsIDs.Intersect(rhsIDs), true, nil
			} else if lhsOk {
				return lhsIDs, true, nil
			} else if rhsOk {
				return rhsIDs, true, nil
			}
			return nil, false, nil
		default:
			return nil, false, fmt.Errorf("invalid tag comparison operator")
		}
	case *influxql.ParenExpr:
		return i.measurementsByExpr(e.Expr)
	}
	return nil, false, fmt.Errorf("%#v", expr)
}

// measurementsByNameFilter returns the sorted measurements matching a name.
func (i *Index) measurementsByNameFilter(op influxql.Token, val string, regex *regexp.Regexp) tsdb.Measurements {
	var measurements tsdb.Measurements
	for _, m := range i.measurements {
		var matched bool
		switch op {
		case influxql.EQ:
			matched = m.Name == val
		case influxql.NEQ:
			matched = m.Name != val
		case influxql.EQREGEX:
			matched = regex.MatchString(m.Name)
		case influxql.NEQREGEX:
			matched = !regex.MatchString(m.Name)
		}

		if !matched {
			continue
		}
		measurements = append(measurements, m)
	}
	sort.Sort(measurements)
	return measurements
}

// measurementsByTagFilters returns the sorted measurements matching the filters on tag values.
func (i *Index) measurementsByTagFilters(filters []*tsdb.TagFilter) tsdb.Measurements {
	// If no filters, then return all measurements.
	if len(filters) == 0 {
		measurements := make(tsdb.Measurements, 0, len(i.measurements))
		for _, m := range i.measurements {
			measurements = append(measurements, m)
		}
		return measurements
	}

	// Build a list of measurements matching the filters.
	var measurements tsdb.Measurements
	var tagMatch bool

	// Iterate through all measurements in the database.
	for _, m := range i.measurements {
		// Iterate filters seeing if the measurement has a matching tag.
		for _, f := range filters {
			tagVals := m.SeriesByTagKeyValue(f.Key)
			if tagVals == nil {
				continue
			}

			tagMatch = false

			// If the operator is non-regex, only check the specified value.
			if f.Op == influxql.EQ || f.Op == influxql.NEQ {
				if _, ok := tagVals[f.Value]; ok {
					tagMatch = true
				}
			} else {
				// Else, the operator is a regex and we have to check all tag
				// values against the regular expression.
				for tagVal := range tagVals {
					if f.Regex.MatchString(tagVal) {
						tagMatch = true
						break
					}
				}
			}

			isEQ := (f.Op == influxql.EQ || f.Op == influxql.EQREGEX)

			//
			// XNOR gate
			//
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

	sort.Sort(measurements)
	return measurements
}

// MeasurementNamesByRegex returns the measurements that match the regex.
func (i *Index) MeasurementNamesByRegex(re *regexp.Regexp) ([][]byte, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	var matches [][]byte
	for _, m := range i.measurements {
		if re.MatchString(m.Name) {
			matches = append(matches, []byte(m.Name))
		}
	}
	return matches, nil
}

// Measurements returns a list of all measurements.
func (i *Index) Measurements() (tsdb.Measurements, error) {
	i.mu.RLock()
	measurements := make(tsdb.Measurements, 0, len(i.measurements))
	for _, m := range i.measurements {
		measurements = append(measurements, m)
	}
	i.mu.RUnlock()

	return measurements, nil
}

// DropMeasurement removes the measurement and all of its underlying
// series from the database index
func (i *Index) DropMeasurement(name []byte) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.dropMeasurement(string(name))
}

func (i *Index) dropMeasurement(name string) error {
	// Update the tombstone sketch.
	i.measurementsTSSketch.Add([]byte(name))

	m := i.measurements[name]
	if m == nil {
		return nil
	}

	delete(i.measurements, name)
	for _, s := range m.SeriesByIDMap() {
		delete(i.series, s.Key)
	}
	return nil
}

// DropSeries removes the series keys and their tags from the index
func (i *Index) DropSeries(keys [][]byte) error {
	if len(keys) == 0 {
		return nil
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	var (
		mToDelete = map[string]struct{}{}
		nDeleted  int64
	)

	for _, k := range keys {
		// Update the tombstone sketch.
		i.seriesTSSketch.Add(k)

		series := i.series[string(k)]
		if series == nil {
			continue
		}
		series.Measurement().DropSeries(series)
		delete(i.series, string(k))
		nDeleted++

		// If there are no more series in the measurement then we'll
		// remove it.
		if len(series.Measurement().SeriesByIDMap()) == 0 {
			mToDelete[series.Measurement().Name] = struct{}{}
		}
	}

	for mname := range mToDelete {
		i.dropMeasurement(mname)
	}
	return nil
}

// Dereference removes all references to data within b and moves them to the heap.
func (i *Index) Dereference(b []byte) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	for _, s := range i.series {
		s.Dereference(b)
	}
}

// TagSets returns a list of tag sets.
func (i *Index) TagSets(shardID uint64, name []byte, dimensions []string, condition influxql.Expr) ([]*influxql.TagSet, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	mm := i.measurements[string(name)]
	if mm == nil {
		return nil, nil
	}

	tagSets, err := mm.TagSets(shardID, dimensions, condition)
	if err != nil {
		return nil, err
	}

	return tagSets, nil
}

// AssignShard update the index to indicate that series k exists in the given shardID.
func (i *Index) AssignShard(k string, shardID uint64) {
	ss, _ := i.Series([]byte(k))
	if ss != nil {
		ss.AssignShard(shardID)
	}
}

// UnassignShard updates the index to indicate that series k does not exist in
// the given shardID.
func (i *Index) UnassignShard(k string, shardID uint64) {
	ss, _ := i.Series([]byte(k))
	if ss != nil {
		if ss.Assigned(shardID) {
			// Remove the shard from any series
			ss.UnassignShard(shardID)

			// If this series no longer has shards assigned, remove the series
			if ss.ShardN() == 0 {
				// Remove the series the measurements
				ss.Measurement().DropSeries(ss)

				// If the measurement no longer has any series, remove it as well
				if !ss.Measurement().HasSeries() {
					i.mu.Lock()
					i.dropMeasurement(ss.Measurement().Name)
					i.mu.Unlock()
				}

				// Remove the series key from the series index
				i.mu.Lock()
				delete(i.series, k)
				// atomic.AddInt64(&i.stats.NumSeries, -1)
				i.mu.Unlock()
			}
		}
	}
}

func (i *Index) SeriesKeys() []string {
	i.mu.RLock()
	s := make([]string, 0, len(i.series))
	for k := range i.series {
		s = append(s, k)
	}
	i.mu.RUnlock()
	return s
}

// SetFieldName adds a field name to a measurement.
func (i *Index) SetFieldName(measurement, name string) {
	m := i.CreateMeasurementIndexIfNotExists(measurement)
	m.SetFieldName(name)
}

// RemoveShard removes all references to shardID from any series or measurements
// in the index.  If the shard was the only owner of data for the series, the series
// is removed from the index.
func (i *Index) RemoveShard(shardID uint64) {
	for _, k := range i.SeriesKeys() {
		i.UnassignShard(k, shardID)
	}
}

// Ensure index implements interface.
var _ tsdb.Index = &ShardIndex{}

// ShardIndex represents a wrapper around the shared Index.
type ShardIndex struct {
	*Index
	id  uint64
	opt tsdb.EngineOptions
}

func (i *ShardIndex) CreateSeriesIfNotExists(name []byte, tags models.Tags) error {
	return i.Index.CreateSeriesIfNotExists(i.id, name, tags, &i.opt)
}

// TagSets returns a list of tag sets based on series filtering.
func (i *ShardIndex) TagSets(name []byte, dimensions []string, condition influxql.Expr) ([]*influxql.TagSet, error) {
	return i.Index.TagSets(i.id, name, dimensions, condition)
}

// NewShardIndex returns a new index for a shard.
func NewShardIndex(id uint64, path string, opt tsdb.EngineOptions) tsdb.Index {
	return &ShardIndex{
		Index: opt.InmemIndex.(*Index),
		id:    id,
		opt:   opt,
	}
}
