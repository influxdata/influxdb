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
	"errors"
	"fmt"
	"regexp"
	"sort"
	"sync"
	// "sync/atomic"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/bytesutil"
	"github.com/influxdata/influxdb/pkg/escape"
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/pkg/estimator/hll"
	"github.com/influxdata/influxdb/tsdb"
)

// IndexName is the name of this index.
const IndexName = "inmem"

func init() {
	tsdb.NewInmemIndex = func(name string) (interface{}, error) { return NewIndex(), nil }

	tsdb.RegisterIndex(IndexName, func(id uint64, path string, opt tsdb.EngineOptions) tsdb.Index {
		return NewShardIndex(id, path, opt)
	})
}

// Index is the in memory index of a collection of measurements, time
// series, and their tags. Exported functions are goroutine safe while
// un-exported functions assume the caller will use the appropriate locks.
type Index struct {
	mu sync.RWMutex

	// In-memory metadata index, built on load and updated when new series come in
	measurements map[string]*tsdb.Measurement // measurement name to object and index
	series       map[string]*tsdb.Series      // map series key to the Series object
	lastID       uint64                       // last used series ID. They're in memory only for this shard

	seriesSketch, seriesTSSketch             *hll.Plus
	measurementsSketch, measurementsTSSketch *hll.Plus
}

// NewIndex returns a new initialized Index.
func NewIndex() *Index {
	index := &Index{
		measurements: make(map[string]*tsdb.Measurement),
		series:       make(map[string]*tsdb.Series),
	}

	index.seriesSketch = hll.NewDefaultPlus()
	index.seriesTSSketch = hll.NewDefaultPlus()
	index.measurementsSketch = hll.NewDefaultPlus()
	index.measurementsTSSketch = hll.NewDefaultPlus()

	return index
}

func (i *Index) Type() string      { return IndexName }
func (i *Index) Open() (err error) { return nil }
func (i *Index) Close() error      { return nil }

// Series returns a series by key.
func (i *Index) Series(key []byte) (*tsdb.Series, error) {
	i.mu.RLock()
	s := i.series[string(key)]
	i.mu.RUnlock()
	return s, nil
}

// SeriesSketches returns the sketches for the series.
func (i *Index) SeriesSketches() (estimator.Sketch, estimator.Sketch, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.seriesSketch.Clone(), i.seriesTSSketch.Clone(), nil
}

// SeriesN returns the number of unique non-tombstoned series in the index.
// Since indexes are not shared across shards, the count returned by SeriesN
// cannot be combined with other shards' counts.
func (i *Index) SeriesN() int64 {
	i.mu.RLock()
	n := int64(len(i.series))
	i.mu.RUnlock()
	return n
}

// Measurement returns the measurement object from the index by the name
func (i *Index) Measurement(name []byte) (*tsdb.Measurement, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.measurements[string(name)], nil
}

// MeasurementExists returns true if the measurement exists.
func (i *Index) MeasurementExists(name []byte) (bool, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.measurements[string(name)] != nil, nil
}

// MeasurementsSketches returns the sketches for the measurements.
func (i *Index) MeasurementsSketches() (estimator.Sketch, estimator.Sketch, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.measurementsSketch.Clone(), i.measurementsTSSketch.Clone(), nil
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
func (i *Index) CreateSeriesIfNotExists(shardID uint64, key, name []byte, tags models.Tags, opt *tsdb.EngineOptions, ignoreLimits bool) error {
	i.mu.RLock()
	// if there is a series for this id, it's already been added
	ss := i.series[string(key)]
	i.mu.RUnlock()

	if ss != nil {
		ss.AssignShard(shardID)
		return nil
	}

	// get or create the measurement index
	m := i.CreateMeasurementIndexIfNotExists(name)

	i.mu.Lock()
	// Check for the series again under a write lock
	ss = i.series[string(key)]
	if ss != nil {
		i.mu.Unlock()
		ss.AssignShard(shardID)
		return nil
	}

	// Verify that the series will not exceed limit.
	if !ignoreLimits {
		if max := opt.Config.MaxSeriesPerDatabase; max > 0 && len(i.series)+1 > max {
			i.mu.Unlock()
			return errMaxSeriesPerDatabaseExceeded
		}
	}

	// set the in memory ID for query processing on this shard
	// The series key and tags are clone to prevent a memory leak
	series := tsdb.NewSeries([]byte(string(key)), tags.Clone())
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
func (i *Index) CreateMeasurementIndexIfNotExists(name []byte) *tsdb.Measurement {
	name = escape.Unescape(name)

	// See if the measurement exists using a read-lock
	i.mu.RLock()
	m := i.measurements[string(name)]
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
	m = i.measurements[string(name)]
	if m == nil {
		m = tsdb.NewMeasurement(string(name))
		i.measurements[string(name)] = m

		// Add the measurement to the measurements sketch.
		i.measurementsSketch.Add([]byte(name))
	}
	return m
}

// HasTagKey returns true if tag key exists.
func (i *Index) HasTagKey(name, key []byte) (bool, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	mm := i.measurements[string(name)]
	if mm == nil {
		return false, nil
	}
	return mm.HasTagKey(string(key)), nil
}

// HasTagValue returns true if tag value exists.
func (i *Index) HasTagValue(name, key, value []byte) bool {
	i.mu.RLock()
	mm := i.measurements[string(name)]
	i.mu.RUnlock()

	if mm == nil {
		return false
	}
	return mm.HasTagKeyValue(key, value)
}

// TagValueN returns the cardinality of a tag value.
func (i *Index) TagValueN(name, key []byte) int {
	i.mu.RLock()
	defer i.mu.RUnlock()

	mm := i.measurements[string(name)]
	if mm == nil {
		return 0
	}
	return mm.CardinalityBytes(key)
}

// MeasurementTagKeysByExpr returns an ordered set of tag keys filtered by an expression.
func (i *Index) MeasurementTagKeysByExpr(name []byte, expr influxql.Expr) (map[string]struct{}, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	mm := i.measurements[string(name)]
	if mm == nil {
		return nil, nil
	}
	return mm.TagKeysByExpr(expr)
}

// ForEachMeasurementTagKey iterates over all tag keys for a measurement.
func (i *Index) ForEachMeasurementTagKey(name []byte, fn func(key []byte) error) error {
	// Ensure we do not hold a lock on the index while fn executes in case fn tries
	// to acquire a lock on the index again.  If another goroutine has Lock, this will
	// deadlock.
	i.mu.RLock()
	mm := i.measurements[string(name)]
	i.mu.RUnlock()

	if mm == nil {
		return nil
	}

	for _, key := range mm.TagKeys() {
		if err := fn([]byte(key)); err != nil {
			return err
		}
	}

	return nil
}

// TagKeyCardinality returns the number of values for a measurement/tag key.
func (i *Index) TagKeyCardinality(name, key []byte) int {
	i.mu.RLock()
	defer i.mu.RUnlock()

	mm := i.measurements[string(name)]
	if mm == nil {
		return 0
	}
	return mm.CardinalityBytes(key)
}

// TagsForSeries returns the tag map for the passed in series
func (i *Index) TagsForSeries(key string) (models.Tags, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	ss := i.series[key]
	if ss == nil {
		return nil, nil
	}
	return ss.Tags(), nil
}

// MeasurementNamesByExpr takes an expression containing only tags and returns a
// list of matching meaurement names.
func (i *Index) MeasurementNamesByExpr(expr influxql.Expr) ([][]byte, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	// Return all measurement names if no expression is provided.
	if expr == nil {
		a := make([][]byte, 0, len(i.measurements))
		for name := range i.measurements {
			a = append(a, []byte(name))
		}
		bytesutil.Sort(a)
		return a, nil
	}

	return i.measurementNamesByExpr(expr)
}

func (i *Index) measurementNamesByExpr(expr influxql.Expr) ([][]byte, error) {
	if expr == nil {
		return nil, nil
	}

	switch e := expr.(type) {
	case *influxql.BinaryExpr:
		switch e.Op {
		case influxql.EQ, influxql.NEQ, influxql.EQREGEX, influxql.NEQREGEX:
			tag, ok := e.LHS.(*influxql.VarRef)
			if !ok {
				return nil, fmt.Errorf("left side of '%s' must be a tag key", e.Op.String())
			}

			tf := &tsdb.TagFilter{
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

			// Match on name, if specified.
			if tag.Val == "_name" {
				return i.measurementNamesByNameFilter(tf.Op, tf.Value, tf.Regex), nil
			} else if influxql.IsSystemName(tag.Val) {
				return nil, nil
			}

			return i.measurementNamesByTagFilters(tf), nil
		case influxql.OR, influxql.AND:
			lhs, err := i.measurementNamesByExpr(e.LHS)
			if err != nil {
				return nil, err
			}

			rhs, err := i.measurementNamesByExpr(e.RHS)
			if err != nil {
				return nil, err
			}

			if e.Op == influxql.OR {
				return bytesutil.Union(lhs, rhs), nil
			}
			return bytesutil.Intersect(lhs, rhs), nil
		default:
			return nil, fmt.Errorf("invalid tag comparison operator")
		}
	case *influxql.ParenExpr:
		return i.measurementNamesByExpr(e.Expr)
	}
	return nil, fmt.Errorf("%#v", expr)
}

// measurementNamesByNameFilter returns the sorted measurements matching a name.
func (i *Index) measurementNamesByNameFilter(op influxql.Token, val string, regex *regexp.Regexp) [][]byte {
	var names [][]byte
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
		names = append(names, []byte(m.Name))
	}
	bytesutil.Sort(names)
	return names
}

// measurementNamesByTagFilters returns the sorted measurements matching the filters on tag values.
func (i *Index) measurementNamesByTagFilters(filter *tsdb.TagFilter) [][]byte {
	// Build a list of measurements matching the filters.
	var names [][]byte
	var tagMatch bool

	// Iterate through all measurements in the database.
	for _, m := range i.measurements {
		tagVals := m.SeriesByTagKeyValue(filter.Key)
		if tagVals == nil {
			continue
		}

		tagMatch = false

		// If the operator is non-regex, only check the specified value.
		if filter.Op == influxql.EQ || filter.Op == influxql.NEQ {
			if _, ok := tagVals[filter.Value]; ok {
				tagMatch = true
			}
		} else {
			// Else, the operator is a regex and we have to check all tag
			// values against the regular expression.
			for tagVal := range tagVals {
				if filter.Regex.MatchString(tagVal) {
					tagMatch = true
					continue
				}
			}
		}

		//
		// XNOR gate
		//
		// tags match | operation is EQ | measurement matches
		// --------------------------------------------------
		//     True   |       True      |      True
		//     True   |       False     |      False
		//     False  |       True      |      False
		//     False  |       False     |      True
		if tagMatch == (filter.Op == influxql.EQ || filter.Op == influxql.EQREGEX) {
			names = append(names, []byte(m.Name))
			continue
		}
	}

	bytesutil.Sort(names)
	return names
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
		i.seriesTSSketch.Add([]byte(s.Key))
	}
	return nil
}

// DropSeries removes the series key and its tags from the index.
func (i *Index) DropSeries(key []byte) error {
	if key == nil {
		return nil
	}

	i.mu.Lock()
	k := string(key)
	series := i.series[k]
	if series == nil {
		i.mu.Unlock()
		return nil
	}

	// Update the tombstone sketch.
	i.seriesTSSketch.Add([]byte(k))

	// Remove from the index.
	delete(i.series, k)

	// Remove the measurement's reference.
	series.Measurement().DropSeries(series)

	// If the measurement no longer has any series, remove it as well.
	if !series.Measurement().HasSeries() {
		i.dropMeasurement(series.Measurement().Name)
	}
	i.mu.Unlock()

	return nil
}

// ForEachMeasurementSeriesByExpr iterates over all series in a measurement filtered by an expression.
func (i *Index) ForEachMeasurementSeriesByExpr(name []byte, expr influxql.Expr, fn func(tags models.Tags) error) error {
	i.mu.RLock()
	mm := i.measurements[string(name)]
	i.mu.RUnlock()

	if mm == nil {
		return nil
	}

	if err := mm.ForEachSeriesByExpr(expr, fn); err != nil {
		return err
	}

	return nil
}

// TagSets returns a list of tag sets.
func (i *Index) TagSets(shardID uint64, name []byte, opt influxql.IteratorOptions) ([]*influxql.TagSet, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	mm := i.measurements[string(name)]
	if mm == nil {
		return nil, nil
	}

	tagSets, err := mm.TagSets(shardID, opt)
	if err != nil {
		return nil, err
	}

	return tagSets, nil
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

// SetFieldSet sets a shared field set from the engine.
func (i *Index) SetFieldSet(*tsdb.MeasurementFieldSet) {}

// SetFieldName adds a field name to a measurement.
func (i *Index) SetFieldName(measurement []byte, name string) {
	m := i.CreateMeasurementIndexIfNotExists(measurement)
	m.SetFieldName(name)
}

// ForEachMeasurementName iterates over each measurement name.
func (i *Index) ForEachMeasurementName(fn func(name []byte) error) error {
	i.mu.RLock()
	defer i.mu.RUnlock()

	mms := make(tsdb.Measurements, 0, len(i.measurements))
	for _, m := range i.measurements {
		mms = append(mms, m)
	}
	sort.Sort(mms)

	for _, m := range mms {
		if err := fn([]byte(m.Name)); err != nil {
			return err
		}
	}
	return nil
}

func (i *Index) MeasurementSeriesKeysByExpr(name []byte, condition influxql.Expr) ([][]byte, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	m := i.measurements[string(name)]
	if m == nil {
		return nil, nil
	}

	// Return all series if no condition specified.
	if condition == nil {
		return m.SeriesKeys(), nil
	}

	// Get series IDs that match the WHERE clause.
	ids, filters, err := m.WalkWhereForSeriesIds(condition)
	if err != nil {
		return nil, err
	}

	// Delete boolean literal true filter expressions.
	// These are returned for `WHERE tagKey = 'tagVal'` type expressions and are okay.
	filters.DeleteBoolLiteralTrues()

	// Check for unsupported field filters.
	// Any remaining filters means there were fields (e.g., `WHERE value = 1.2`).
	if filters.Len() > 0 {
		return nil, errors.New("fields not supported in WHERE clause during deletion")
	}

	return m.SeriesKeysByID(ids), nil
}

// SeriesPointIterator returns an influxql iterator over all series.
func (i *Index) SeriesPointIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {
	// Read and sort all measurements.
	mms := make(tsdb.Measurements, 0, len(i.measurements))
	for _, mm := range i.measurements {
		mms = append(mms, mm)
	}
	sort.Sort(mms)

	return &seriesPointIterator{
		mms: mms,
		point: influxql.FloatPoint{
			Aux: make([]interface{}, len(opt.Aux)),
		},
		opt: opt,
	}, nil
}

// SnapshotTo is a no-op since this is an in-memory index.
func (i *Index) SnapshotTo(path string) error { return nil }

// AssignShard update the index to indicate that series k exists in the given shardID.
func (i *Index) AssignShard(k string, shardID uint64) {
	ss, _ := i.Series([]byte(k))
	if ss != nil {
		ss.AssignShard(shardID)
	}
}

// UnassignShard updates the index to indicate that series k does not exist in
// the given shardID.
func (i *Index) UnassignShard(k string, shardID uint64) error {
	ss, _ := i.Series([]byte(k))
	if ss != nil {
		if ss.Assigned(shardID) {
			// Remove the shard from any series
			ss.UnassignShard(shardID)

			// If this series no longer has shards assigned, remove the series
			if ss.ShardN() == 0 {
				// Remove the series key from the index.
				return i.DropSeries([]byte(k))
			}
		}
	}
	return nil
}

// RemoveShard removes all references to shardID from any series or measurements
// in the index.  If the shard was the only owner of data for the series, the series
// is removed from the index.
func (i *Index) RemoveShard(shardID uint64) {
	for _, k := range i.SeriesKeys() {
		i.UnassignShard(k, shardID)
	}
}

// assignExistingSeries assigns the existings series to shardID and returns the series, names and tags that
// do not exists yet.
func (i *Index) assignExistingSeries(shardID uint64, keys, names [][]byte, tagsSlice []models.Tags) ([][]byte, [][]byte, []models.Tags) {
	i.mu.RLock()
	var n int
	for j, key := range keys {
		if ss, ok := i.series[string(key)]; !ok {
			keys[n] = keys[j]
			names[n] = names[j]
			tagsSlice[n] = tagsSlice[j]
			n++
		} else {
			ss.AssignShard(shardID)
		}
	}
	i.mu.RUnlock()
	return keys[:n], names[:n], tagsSlice[:n]
}

// Ensure index implements interface.
var _ tsdb.Index = &ShardIndex{}

// ShardIndex represents a shim between the TSDB index interface and the shared
// in-memory index. This is required because per-shard in-memory indexes will
// grow the heap size too large.
type ShardIndex struct {
	*Index

	id  uint64 // shard id
	opt tsdb.EngineOptions
}

// CreateSeriesListIfNotExists creates a list of series if they doesn't exist in bulk.
func (idx *ShardIndex) CreateSeriesListIfNotExists(keys, names [][]byte, tagsSlice []models.Tags) error {
	keys, names, tagsSlice = idx.assignExistingSeries(idx.id, keys, names, tagsSlice)
	if len(keys) == 0 {
		return nil
	}

	var reason string
	var dropped int
	var droppedKeys map[string]struct{}

	// Ensure that no tags go over the maximum cardinality.
	if maxValuesPerTag := idx.opt.Config.MaxValuesPerTag; maxValuesPerTag > 0 {
		var n int

	outer:
		for i, name := range names {
			tags := tagsSlice[i]
			for _, tag := range tags {
				// Skip if the tag value already exists.
				if idx.HasTagValue(name, tag.Key, tag.Value) {
					continue
				}

				// Read cardinality. Skip if we're below the threshold.
				n := idx.TagValueN(name, tag.Key)
				if n < maxValuesPerTag {
					continue
				}

				dropped++
				reason = fmt.Sprintf("max-values-per-tag limit exceeded (%d/%d): measurement=%q tag=%q value=%q",
					n, maxValuesPerTag, name, string(tag.Key), string(tag.Value))

				if droppedKeys == nil {
					droppedKeys = make(map[string]struct{})
				}
				droppedKeys[string(keys[i])] = struct{}{}
				continue outer
			}

			// Increment success count if all checks complete.
			keys[n], names[n], tagsSlice[n] = keys[i], names[i], tagsSlice[i]
			n++
		}

		// Slice to only include successful points.
		keys, names, tagsSlice = keys[:n], names[:n], tagsSlice[:n]
	}

	// Write
	for i := range keys {
		if err := idx.CreateSeriesIfNotExists(keys[i], names[i], tagsSlice[i]); err == errMaxSeriesPerDatabaseExceeded {
			dropped++
			reason = fmt.Sprintf("max-series-per-database limit exceeded: (%d)", idx.opt.Config.MaxSeriesPerDatabase)
			if droppedKeys == nil {
				droppedKeys = make(map[string]struct{})
			}
			droppedKeys[string(keys[i])] = struct{}{}
			continue
		} else if err != nil {
			return err
		}
	}

	// Report partial writes back to shard.
	if dropped > 0 {
		return &tsdb.PartialWriteError{
			Reason:      reason,
			Dropped:     dropped,
			DroppedKeys: droppedKeys,
		}
	}

	return nil
}

// InitializeSeries is called during startup.
// This works the same as CreateSeriesIfNotExists except it ignore limit errors.
func (i *ShardIndex) InitializeSeries(key, name []byte, tags models.Tags) error {
	return i.Index.CreateSeriesIfNotExists(i.id, key, name, tags, &i.opt, true)
}

func (i *ShardIndex) CreateSeriesIfNotExists(key, name []byte, tags models.Tags) error {
	return i.Index.CreateSeriesIfNotExists(i.id, key, name, tags, &i.opt, false)
}

// TagSets returns a list of tag sets based on series filtering.
func (i *ShardIndex) TagSets(name []byte, opt influxql.IteratorOptions) ([]*influxql.TagSet, error) {
	return i.Index.TagSets(i.id, name, opt)
}

// NewShardIndex returns a new index for a shard.
func NewShardIndex(id uint64, path string, opt tsdb.EngineOptions) tsdb.Index {
	return &ShardIndex{
		Index: opt.InmemIndex.(*Index),
		id:    id,
		opt:   opt,
	}
}

// seriesPointIterator emits series as influxql points.
type seriesPointIterator struct {
	mms  tsdb.Measurements
	keys struct {
		buf []string
		i   int
	}

	point influxql.FloatPoint // reusable point
	opt   influxql.IteratorOptions
}

// Stats returns stats about the points processed.
func (itr *seriesPointIterator) Stats() influxql.IteratorStats { return influxql.IteratorStats{} }

// Close closes the iterator.
func (itr *seriesPointIterator) Close() error { return nil }

// Next emits the next point in the iterator.
func (itr *seriesPointIterator) Next() (*influxql.FloatPoint, error) {
	for {
		// Load next measurement's keys if there are no more remaining.
		if itr.keys.i >= len(itr.keys.buf) {
			if err := itr.nextKeys(); err != nil {
				return nil, err
			}
			if len(itr.keys.buf) == 0 {
				return nil, nil
			}
		}

		// Read the next key.
		key := itr.keys.buf[itr.keys.i]
		itr.keys.i++

		// Write auxiliary fields.
		for i, f := range itr.opt.Aux {
			switch f.Val {
			case "key":
				itr.point.Aux[i] = key
			}
		}
		return &itr.point, nil
	}
}

// nextKeys reads all keys for the next measurement.
func (itr *seriesPointIterator) nextKeys() error {
	for {
		// Ensure previous keys are cleared out.
		itr.keys.i, itr.keys.buf = 0, itr.keys.buf[:0]

		// Read next measurement.
		if len(itr.mms) == 0 {
			return nil
		}
		mm := itr.mms[0]
		itr.mms = itr.mms[1:]

		// Read all series keys.
		ids, err := mm.SeriesIDsAllOrByExpr(itr.opt.Condition)
		if err != nil {
			return err
		} else if len(ids) == 0 {
			continue
		}
		itr.keys.buf = mm.AppendSeriesKeysByID(itr.keys.buf, ids)
		sort.Strings(itr.keys.buf)

		return nil
	}
}

// errMaxSeriesPerDatabaseExceeded is a marker error returned during series creation
// to indicate that a new series would exceed the limits of the database.
var errMaxSeriesPerDatabaseExceeded = errors.New("max series per database exceeded")
