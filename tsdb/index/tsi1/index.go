package tsi1

import (
	"errors"
	"regexp"
	"sync"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	"github.com/uber-go/zap"
)

// IndexName is the name of the index.
const IndexName = "tsi1"

// An IndexOption is a functional option for changing the configuration of
// an Index.
type IndexOption func(i *Index)

// WithPath sets the root path of the Index
var WithPath = func(path string) IndexOption {
	return func(i *Index) {
		i.path = path
	}
}

// WithCompactions enables or disabled compactions on the Index.
var WithCompactions = func(enabled bool) IndexOption {
	return func(i *Index) {
		i.CompactionEnabled = enabled
	}
}

// WithLogger sets the logger for the Index.
var WithLogger = func(l zap.Logger) IndexOption {
	return func(i *Index) {
		i.logger = l.With(zap.String("index", "tsi"))
	}
}

// Index represents a collection of layered index files and WAL.
type Index struct {
	mu     sync.RWMutex
	opened bool

	// The following can be set when initialising an Index.
	path              string     // Root directory of the index files.
	CompactionEnabled bool       // Frequency of compaction checks.
	logger            zap.Logger // Index's logger.

	// Close management.
	once    sync.Once
	closing chan struct{}

	// Index's version.
	version int
}

// NewIndex returns a new instance of Index.
func NewIndex(options ...IndexOption) *Index {
	i := &Index{
		closing: make(chan struct{}),

		logger:  zap.New(zap.NullEncoder()),
		version: Version,
	}

	for _, option := range options {
		option(i)
	}
	return i
}

// Type returns the type of Index this is.
func (i *Index) Type() string { return IndexName }

// Open opens the index.
func (i *Index) Open() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.opened {
		return errors.New("index already open")
	}

	// TODO(edd): Open all the Partitions.

	// Mark opened.
	i.opened = true

	return nil
}

// Wait returns once outstanding compactions have finished.
func (i *Index) Wait() {
	// TODO(edd): Wait on each partition.
}

// Close closes the index.
func (i *Index) Close() error {
	// Wait for goroutines to finish.
	i.once.Do(func() { close(i.closing) })
	i.Wait()

	// Lock index and close partitions.
	i.mu.Lock()
	defer i.mu.Unlock()

	// TODO(edd): Close Partitions.

	return nil
}

// RetainFileSet returns the current fileset for all partitions in the Index.
func (i *Index) RetainFileSet() *FileSet {
	// TODO(edd): Merge all FileSets for all partitions. For the moment we will
	// just append them from each partition.
	return nil
}

// SetFieldSet sets a shared field set from the engine.
func (i *Index) SetFieldSet(fs *tsdb.MeasurementFieldSet) {
	i.mu.Lock()
	// TODO(edd): set the field set on all the Partitions?
	i.mu.Unlock()
}

// ForEachMeasurementName iterates over all measurement names in the index.
func (i *Index) ForEachMeasurementName(fn func(name []byte) error) error {
	// TODO(edd): Call on each partition. Could be done in parallel?
	return nil
}

// MeasurementExists returns true if a measurement exists.
func (i *Index) MeasurementExists(name []byte) (bool, error) {
	// TODO(edd): Call on each Partition. In parallel?
	return false, nil
}

// MeasurementNamesByExpr returns measurement names for the provided expression.
func (i *Index) MeasurementNamesByExpr(expr influxql.Expr) ([][]byte, error) {
	// TODO(edd): Call on each partition. Merge results.
	return nil, nil
}

// MeasurementNamesByRegex returns measurement names for the provided regex.
func (i *Index) MeasurementNamesByRegex(re *regexp.Regexp) ([][]byte, error) {
	// TODO(edd): Call on each Partition. Merge results.
	return nil, nil
}

// DropMeasurement deletes a measurement from the index.
func (i *Index) DropMeasurement(name []byte) error {
	// TODO(edd): Call on each Partition. In parallel?
	return nil
}

// CreateSeriesListIfNotExists creates a list of series if they doesn't exist in bulk.
func (i *Index) CreateSeriesListIfNotExists(_, names [][]byte, tagsSlice []models.Tags) error {
	// TODO(edd): Call on correct Partition.
	return nil
}

// InitializeSeries is a no-op. This only applies to the in-memory index.
func (i *Index) InitializeSeries(key, name []byte, tags models.Tags) error {
	return nil
}

// CreateSeriesIfNotExists creates a series if it doesn't exist or is deleted.
func (i *Index) CreateSeriesIfNotExists(key, name []byte, tags models.Tags) error {
	// TODO(edd): Call on correct Partition.
	return nil
}

// DropSeries drops the provided series from the index.
func (i *Index) DropSeries(key []byte) error {
	// TODO(edd): Call on correct Partition.
	return nil
}

// MeasurementsSketches returns the two sketches for the index by merging all
// instances of the type sketch types in all the index files.
func (i *Index) MeasurementsSketches() (estimator.Sketch, estimator.Sketch, error) {
	// TODO(edd): Merge sketches for each partition.
	return nil, nil, nil
}

// SeriesN returns the number of unique non-tombstoned series in the index.
// Since indexes are not shared across shards, the count returned by SeriesN
// cannot be combined with other shard's results. If you need to count series
// across indexes then use SeriesSketches and merge the results from other
// indexes.
func (i *Index) SeriesN() int64 {
	// TODO(edd): Sum over all Partitions.
	return 0
}

// HasTagKey returns true if tag key exists.
func (i *Index) HasTagKey(name, key []byte) (bool, error) {
	// TODO(edd): Check on each Partition? In parallel?
	return false, nil
}

// MeasurementTagKeysByExpr extracts the tag keys wanted by the expression.
func (i *Index) MeasurementTagKeysByExpr(name []byte, expr influxql.Expr) (map[string]struct{}, error) {
	// TODO(edd): Call on each partition. Merge results.
	return nil, nil
}

// MeasurementTagKeyValuesByExpr returns a set of tag values filtered by an expression.
//
// See tsm1.Engine.MeasurementTagKeyValuesByExpr for a fuller description of this
// method.
func (i *Index) MeasurementTagKeyValuesByExpr(name []byte, keys []string, expr influxql.Expr, keysSorted bool) ([][]string, error) {
	// TODO(edd): Merge results from each Partition.
	return nil, nil
}

// ForEachMeasurementTagKey iterates over all tag keys in a measurement and applies
// the provided function.
func (i *Index) ForEachMeasurementTagKey(name []byte, fn func(key []byte) error) error {
	// TODO(edd): Apply fn on each Partition. In parallel?
	return nil
}

// TagKeyCardinality always returns zero.
// It is not possible to determine cardinality of tags across index files, and
// thus it cannot be done across partitions.
func (i *Index) TagKeyCardinality(name, key []byte) int {
	return 0
}

// MeasurementSeriesKeysByExpr returns a list of series keys matching expr.
func (i *Index) MeasurementSeriesKeysByExpr(name []byte, expr influxql.Expr) ([][]byte, error) {
	// TODO(edd): Merge results from each Partition.
	return nil, nil
}

// TagSets returns an ordered list of tag sets for a measurement by dimension
// and filtered by an optional conditional expression.
func (i *Index) TagSets(name []byte, opt query.IteratorOptions) ([]*query.TagSet, error) {
	// TODO(edd): Merge results from each Partition.
	return nil, nil
}

// SnapshotTo creates hard links to the file set into path.
func (i *Index) SnapshotTo(path string) error {
	// TODO(edd): Call on each Partition.
	return nil
}

// SetFieldName is a no-op on tsi1.
func (i *Index) SetFieldName(measurement []byte, name string) {}

// RemoveShard is a no-op on tsi1.
func (i *Index) RemoveShard(shardID uint64) {}

// AssignShard is a no-op on tsi1.
func (i *Index) AssignShard(k string, shardID uint64) {}

// UnassignShard simply calls into DropSeries.
func (i *Index) UnassignShard(k string, shardID uint64) error {
	// This can be called directly once inmem is gone.
	return i.DropSeries([]byte(k))
}

// SeriesPointIterator returns an influxql iterator over all series.
func (i *Index) SeriesPointIterator(opt query.IteratorOptions) (query.Iterator, error) {
	// TODO(edd): Create iterators for each Partition and return a merged
	// iterator.
	return nil, nil
}

// Compact requests a compaction of log files.
func (i *Index) Compact() {
	// TODO(edd): Request compactions on each Partition?
}

// Rebuild is a no-op on tsi1.
func (i *Index) Rebuild() {}
