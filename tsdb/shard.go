package tsdb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/influxdata/influxdb/query"
	internal "github.com/influxdata/influxdb/tsdb/internal"
	"github.com/uber-go/zap"
)

// monitorStatInterval is the interval at which the shard is inspected
// for the purpose of determining certain monitoring statistics.
const monitorStatInterval = 30 * time.Second

const (
	statWriteReq           = "writeReq"
	statWriteReqOK         = "writeReqOk"
	statWriteReqErr        = "writeReqErr"
	statSeriesCreate       = "seriesCreate"
	statFieldsCreate       = "fieldsCreate"
	statWritePointsErr     = "writePointsErr"
	statWritePointsDropped = "writePointsDropped"
	statWritePointsOK      = "writePointsOk"
	statWriteBytes         = "writeBytes"
	statDiskBytes          = "diskBytes"
)

var (
	// ErrFieldOverflow is returned when too many fields are created on a measurement.
	ErrFieldOverflow = errors.New("field overflow")

	// ErrFieldTypeConflict is returned when a new field already exists with a different type.
	ErrFieldTypeConflict = errors.New("field type conflict")

	// ErrFieldNotFound is returned when a field cannot be found.
	ErrFieldNotFound = errors.New("field not found")

	// ErrFieldUnmappedID is returned when the system is presented, during decode, with a field ID
	// there is no mapping for.
	ErrFieldUnmappedID = errors.New("field ID not mapped")

	// ErrEngineClosed is returned when a caller attempts indirectly to
	// access the shard's underlying engine.
	ErrEngineClosed = errors.New("engine is closed")

	// ErrShardDisabled is returned when a the shard is not available for
	// queries or writes.
	ErrShardDisabled = errors.New("shard is disabled")
)

var (
	// Static objects to prevent small allocs.
	timeBytes = []byte("time")
)

// A ShardError implements the error interface, and contains extra
// context about the shard that generated the error.
type ShardError struct {
	id  uint64
	Err error
}

// NewShardError returns a new ShardError.
func NewShardError(id uint64, err error) error {
	if err == nil {
		return nil
	}
	return ShardError{id: id, Err: err}
}

// Error returns the string representation of the error, to satisfy the error interface.
func (e ShardError) Error() string {
	return fmt.Sprintf("[shard %d] %s", e.id, e.Err)
}

// PartialWriteError indicates a write request could only write a portion of the
// requested values.
type PartialWriteError struct {
	Reason  string
	Dropped int

	// The set of series keys that were dropped. Can be nil.
	DroppedKeys map[string]struct{}
}

func (e PartialWriteError) Error() string {
	return fmt.Sprintf("partial write: %s dropped=%d", e.Reason, e.Dropped)
}

// Shard represents a self-contained time series database. An inverted index of
// the measurement and tag data is kept along with the raw time series data.
// Data can be split across many shards. The query engine in TSDB is responsible
// for combining the output of many shards into a single query result.
type Shard struct {
	path    string
	walPath string
	id      uint64

	database        string
	retentionPolicy string

	options EngineOptions

	mu     sync.RWMutex
	engine Engine
	index  Index

	closing chan struct{}
	enabled bool

	// expvar-based stats.
	stats       *ShardStatistics
	defaultTags models.StatisticTags

	baseLogger zap.Logger
	logger     zap.Logger

	EnableOnOpen bool
}

// NewShard returns a new initialized Shard. walPath doesn't apply to the b1 type index
func NewShard(id uint64, path string, walPath string, opt EngineOptions) *Shard {
	db, rp := decodeStorePath(path)
	logger := zap.New(zap.NullEncoder())

	s := &Shard{
		id:      id,
		path:    path,
		walPath: walPath,
		options: opt,
		closing: make(chan struct{}),

		stats: &ShardStatistics{},
		defaultTags: models.StatisticTags{
			"path":            path,
			"walPath":         walPath,
			"id":              fmt.Sprintf("%d", id),
			"database":        db,
			"retentionPolicy": rp,
			"engine":          opt.EngineVersion,
		},

		database:        db,
		retentionPolicy: rp,

		logger:       logger,
		baseLogger:   logger,
		EnableOnOpen: true,
	}
	return s
}

// WithLogger sets the logger on the shard.
func (s *Shard) WithLogger(log zap.Logger) {
	s.baseLogger = log
	if err := s.ready(); err == nil {
		s.engine.WithLogger(s.baseLogger)
		s.index.WithLogger(s.baseLogger)
	}
	s.logger = s.baseLogger.With(zap.String("service", "shard"))
}

// SetEnabled enables the shard for queries and write.  When disabled, all
// writes and queries return an error and compactions are stopped for the shard.
func (s *Shard) SetEnabled(enabled bool) {
	s.mu.Lock()
	// Prevent writes and queries
	s.enabled = enabled
	if s.engine != nil {
		// Disable background compactions and snapshotting
		s.engine.SetEnabled(enabled)
	}
	s.mu.Unlock()
}

// ID returns the shards ID.
func (s *Shard) ID() uint64 {
	return s.id
}

// Database returns the database of the shard.
func (s *Shard) Database() string {
	return s.database
}

// RetentionPolicy returns the retention policy of the shard.
func (s *Shard) RetentionPolicy() string {
	return s.retentionPolicy
}

// ShardStatistics maintains statistics for a shard.
type ShardStatistics struct {
	WriteReq           int64
	WriteReqOK         int64
	WriteReqErr        int64
	FieldsCreated      int64
	WritePointsErr     int64
	WritePointsDropped int64
	WritePointsOK      int64
	BytesWritten       int64
	DiskBytes          int64
}

// Statistics returns statistics for periodic monitoring.
func (s *Shard) Statistics(tags map[string]string) []models.Statistic {
	if err := s.ready(); err != nil {
		return nil
	}

	// Refresh our disk size stat
	if _, err := s.DiskSize(); err != nil {
		return nil
	}
	seriesN := s.engine.SeriesN()

	tags = s.defaultTags.Merge(tags)
	statistics := []models.Statistic{{
		Name: "shard",
		Tags: tags,
		Values: map[string]interface{}{
			statWriteReq:           atomic.LoadInt64(&s.stats.WriteReq),
			statWriteReqOK:         atomic.LoadInt64(&s.stats.WriteReqOK),
			statWriteReqErr:        atomic.LoadInt64(&s.stats.WriteReqErr),
			statSeriesCreate:       seriesN,
			statFieldsCreate:       atomic.LoadInt64(&s.stats.FieldsCreated),
			statWritePointsErr:     atomic.LoadInt64(&s.stats.WritePointsErr),
			statWritePointsDropped: atomic.LoadInt64(&s.stats.WritePointsDropped),
			statWritePointsOK:      atomic.LoadInt64(&s.stats.WritePointsOK),
			statWriteBytes:         atomic.LoadInt64(&s.stats.BytesWritten),
			statDiskBytes:          atomic.LoadInt64(&s.stats.DiskBytes),
		},
	}}

	// Add the index and engine statistics.
	statistics = append(statistics, s.engine.Statistics(tags)...)
	return statistics
}

// Path returns the path set on the shard when it was created.
func (s *Shard) Path() string { return s.path }

// Open initializes and opens the shard's store.
func (s *Shard) Open() error {
	if err := func() error {
		s.mu.Lock()
		defer s.mu.Unlock()

		// Return if the shard is already open
		if s.engine != nil {
			return nil
		}

		// Initialize underlying index.
		ipath := filepath.Join(s.path, "index")
		idx, err := NewIndex(s.id, s.database, ipath, s.options)
		if err != nil {
			return err
		}

		// Open index.
		if err := idx.Open(); err != nil {
			return err
		}
		s.index = idx
		idx.WithLogger(s.baseLogger)

		// Initialize underlying engine.
		e, err := NewEngine(s.id, idx, s.database, s.path, s.walPath, s.options)
		if err != nil {
			return err
		}

		// Set log output on the engine.
		e.WithLogger(s.baseLogger)

		// Disable compactions while loading the index
		e.SetEnabled(false)

		// Open engine.
		if err := e.Open(); err != nil {
			return err
		}

		// Load metadata index for the inmem index only.
		if err := e.LoadMetadataIndex(s.id, s.index); err != nil {
			return err
		}
		s.engine = e

		return nil
	}(); err != nil {
		s.close(true)
		return NewShardError(s.id, err)
	}

	if s.EnableOnOpen {
		// enable writes, queries and compactions
		s.SetEnabled(true)
	}

	return nil
}

// Close shuts down the shard's store.
func (s *Shard) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.close(true)
}

// CloseFast closes the shard without cleaning up the shard ID or any of the
// shard's series keys from the index it belongs to.
//
// CloseFast can be called when the entire index is being removed, e.g., when
// the database the shard belongs to is being dropped.
func (s *Shard) CloseFast() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.close(false)
}

// close closes the shard an removes reference to the shard from associated
// indexes, unless clean is false.
func (s *Shard) close(clean bool) error {
	if s.engine == nil {
		return nil
	}

	// Close the closing channel at most once.
	select {
	case <-s.closing:
	default:
		close(s.closing)
	}

	if clean {
		// Don't leak our shard ID and series keys in the index
		s.unloadIndex()
	}

	err := s.engine.Close()
	if err == nil {
		s.engine = nil
	}

	if e := s.index.Close(); e == nil {
		s.index = nil
	}
	return err
}

func (s *Shard) IndexType() string {
	if err := s.ready(); err != nil {
		return ""
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index.Type()
}

// ready determines if the Shard is ready for queries or writes.
// It returns nil if ready, otherwise ErrShardClosed or ErrShardDiabled
func (s *Shard) ready() error {
	var err error

	s.mu.RLock()
	if s.engine == nil {
		err = ErrEngineClosed
	} else if !s.enabled {
		err = ErrShardDisabled
	}
	s.mu.RUnlock()
	return err
}

// LastModified returns the time when this shard was last modified.
func (s *Shard) LastModified() time.Time {
	if err := s.ready(); err != nil {
		return time.Time{}
	}
	return s.engine.LastModified()
}

// UnloadIndex removes all references to this shard from the DatabaseIndex
func (s *Shard) UnloadIndex() {
	if err := s.ready(); err != nil {
		return
	}
	s.unloadIndex()
}

func (s *Shard) unloadIndex() {
	s.index.RemoveShard(s.id)
}

// Index returns a reference to the underlying index.
// This should only be used by utilities and not directly accessed by the database.
func (s *Shard) Index() Index {
	return s.index
}

// IsIdle return true if the shard is not receiving writes and is fully compacted.
func (s *Shard) IsIdle() bool {
	if err := s.ready(); err != nil {
		return true
	}

	return s.engine.IsIdle()
}

func (s *Shard) Free() error {
	if err := s.ready(); err != nil {
		return err
	}

	// Disable compactions to stop background goroutines
	s.SetCompactionsEnabled(false)

	return s.engine.Free()
}

// SetCompactionsEnabled enables or disable shard background compactions.
func (s *Shard) SetCompactionsEnabled(enabled bool) {
	if err := s.ready(); err != nil {
		return
	}
	s.engine.SetCompactionsEnabled(enabled)
}

// DiskSize returns the size on disk of this shard
func (s *Shard) DiskSize() (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.engine == nil {
		return 0, ErrEngineClosed
	}
	size := s.engine.DiskSize()
	atomic.StoreInt64(&s.stats.DiskBytes, size)
	return size, nil
}

// FieldCreate holds information for a field to create on a measurement.
type FieldCreate struct {
	Measurement []byte
	Field       *Field
}

// WritePoints will write the raw data points and any new metadata to the index in the shard.
func (s *Shard) WritePoints(points []models.Point) error {
	if err := s.ready(); err != nil {
		return err
	}

	var writeError error

	s.mu.RLock()
	defer s.mu.RUnlock()

	atomic.AddInt64(&s.stats.WriteReq, 1)

	points, fieldsToCreate, err := s.validateSeriesAndFields(points)
	if err != nil {
		if _, ok := err.(PartialWriteError); !ok {
			return err
		}
		// There was a partial write (points dropped), hold onto the error to return
		// to the caller, but continue on writing the remaining points.
		writeError = err
	}
	atomic.AddInt64(&s.stats.FieldsCreated, int64(len(fieldsToCreate)))

	// add any new fields and keep track of what needs to be saved
	if err := s.createFieldsAndMeasurements(fieldsToCreate); err != nil {
		return err
	}

	// Write to the engine.
	if err := s.engine.WritePoints(points); err != nil {
		atomic.AddInt64(&s.stats.WritePointsErr, int64(len(points)))
		atomic.AddInt64(&s.stats.WriteReqErr, 1)
		return fmt.Errorf("engine: %s", err)
	}
	atomic.AddInt64(&s.stats.WritePointsOK, int64(len(points)))
	atomic.AddInt64(&s.stats.WriteReqOK, 1)

	return writeError
}

// DeleteSeries deletes a list of series.
func (s *Shard) DeleteSeries(seriesKeys [][]byte) error {
	return s.DeleteSeriesRange(seriesKeys, math.MinInt64, math.MaxInt64)
}

// DeleteSeriesRange deletes all values from for seriesKeys between min and max (inclusive)
func (s *Shard) DeleteSeriesRange(seriesKeys [][]byte, min, max int64) error {
	if err := s.ready(); err != nil {
		return err
	}

	if err := s.engine.DeleteSeriesRange(seriesKeys, min, max); err != nil {
		return err
	}

	return nil
}

// DeleteMeasurement deletes a measurement and all underlying series.
func (s *Shard) DeleteMeasurement(name []byte) error {
	if err := s.ready(); err != nil {
		return err
	}
	return s.engine.DeleteMeasurement(name)
}

// SeriesN returns the unique number of series in the shard.
func (s *Shard) SeriesN() int64 {
	return s.engine.SeriesN()
}

// SeriesSketches returns the series sketches for the shard.
func (s *Shard) SeriesSketches() (estimator.Sketch, estimator.Sketch, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.engine == nil {
		return nil, nil, nil
	}
	return s.engine.SeriesSketches()
}

// MeasurementsSketches returns the measurement sketches for the shard.
func (s *Shard) MeasurementsSketches() (estimator.Sketch, estimator.Sketch, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.engine == nil {
		return nil, nil, nil
	}
	return s.engine.MeasurementsSketches()
}

func (s *Shard) createFieldsAndMeasurements(fieldsToCreate []*FieldCreate) error {
	if len(fieldsToCreate) == 0 {
		return nil
	}

	// add fields
	for _, f := range fieldsToCreate {
		mf := s.engine.MeasurementFields(f.Measurement)
		if err := mf.CreateFieldIfNotExists([]byte(f.Field.Name), f.Field.Type, false); err != nil {
			return err
		}

		s.index.SetFieldName(f.Measurement, f.Field.Name)
	}

	return nil
}

// validateSeriesAndFields checks which series and fields are new and whose metadata should be saved and indexed.
func (s *Shard) validateSeriesAndFields(points []models.Point) ([]models.Point, []*FieldCreate, error) {
	var (
		fieldsToCreate []*FieldCreate
		err            error
		dropped        int
		reason         string // only first error reason is set unless returned from CreateSeriesListIfNotExists
	)

	// Create all series against the index in bulk.
	keys := make([][]byte, len(points))
	names := make([][]byte, len(points))
	tagsSlice := make([]models.Tags, len(points))

	// Drop any series w/ a "time" tag, these are illegal
	var j int
	for i, p := range points {
		tags := p.Tags()
		if v := tags.Get(timeBytes); v != nil {
			dropped++
			if reason == "" {
				reason = fmt.Sprintf("invalid tag key: input tag \"%s\" on measurement \"%s\" is invalid", "time", string(p.Name()))
			}
			continue
		}
		keys[j] = p.Key()
		names[j] = p.Name()
		tagsSlice[j] = tags
		points[j] = points[i]
		j++
	}
	points, keys, names, tagsSlice = points[:j], keys[:j], names[:j], tagsSlice[:j]

	// Add new series. Check for partial writes.
	var droppedKeys map[string]struct{}
	if err := s.engine.CreateSeriesListIfNotExists(keys, names, tagsSlice); err != nil {
		switch err := err.(type) {
		case *PartialWriteError:
			reason = err.Reason
			dropped += err.Dropped
			droppedKeys = err.DroppedKeys
			atomic.AddInt64(&s.stats.WritePointsDropped, int64(err.Dropped))
		default:
			return nil, nil, err
		}
	}

	// get the shard mutex for locally defined fields
	n := 0

	// mfCache is a local cache of MeasurementFields to reduce lock contention when validating
	// field types.
	mfCache := make(map[string]*MeasurementFields, 16)
	for i, p := range points {
		var skip bool
		var validField bool
		iter := p.FieldIterator()
		for iter.Next() {
			if bytes.Equal(iter.FieldKey(), timeBytes) {
				continue
			}
			validField = true
			break
		}

		if !validField {
			dropped++
			if reason == "" {
				reason = fmt.Sprintf("invalid field name: input field \"%s\" on measurement \"%s\" is invalid", "time", string(p.Name()))
			}
			continue
		}

		iter.Reset()

		// Skip points if keys have been dropped.
		// The drop count has already been incremented during series creation.
		if droppedKeys != nil {
			if _, ok := droppedKeys[string(keys[i])]; ok {
				continue
			}
		}

		name := p.Name()
		// see if the field definitions need to be saved to the shard
		mf := mfCache[string(name)]
		if mf == nil {
			mf = s.engine.MeasurementFields(name).Clone()
			mfCache[string(name)] = mf
		}
		iter.Reset()

		// validate field types and encode data
		for iter.Next() {

			// Skip fields name "time", they are illegal
			if bytes.Equal(iter.FieldKey(), timeBytes) {
				continue
			}

			var fieldType influxql.DataType
			switch iter.Type() {
			case models.Float:
				fieldType = influxql.Float
			case models.Integer:
				fieldType = influxql.Integer
			case models.Unsigned:
				fieldType = influxql.Unsigned
			case models.Boolean:
				fieldType = influxql.Boolean
			case models.String:
				fieldType = influxql.String
			default:
				continue
			}

			if f := mf.FieldBytes(iter.FieldKey()); f != nil {
				// Field present in shard metadata, make sure there is no type conflict.
				if f.Type != fieldType {
					atomic.AddInt64(&s.stats.WritePointsDropped, 1)
					dropped++
					if reason == "" {
						reason = fmt.Sprintf("%s: input field \"%s\" on measurement \"%s\" is type %s, already exists as type %s", ErrFieldTypeConflict, iter.FieldKey(), name, fieldType, f.Type)
					}
					skip = true
				} else {
					continue // Field is present, and it's of the same type. Nothing more to do.
				}
			}

			if !skip {
				fieldsToCreate = append(fieldsToCreate, &FieldCreate{p.Name(), &Field{Name: string(iter.FieldKey()), Type: fieldType}})
			}
		}

		if !skip {
			points[n] = points[i]
			n++
		}
	}
	points = points[:n]

	if dropped > 0 {
		err = PartialWriteError{Reason: reason, Dropped: dropped}
	}

	return points, fieldsToCreate, err
}

// MeasurementNamesByExpr returns names of measurements matching the condition.
// If cond is nil then all measurement names are returned.
func (s *Shard) MeasurementNamesByExpr(cond influxql.Expr) ([][]byte, error) {
	return s.engine.MeasurementNamesByExpr(cond)
}

// MeasurementFields returns fields for a measurement.
func (s *Shard) MeasurementFields(name []byte) *MeasurementFields {
	return s.engine.MeasurementFields(name)
}

func (s *Shard) MeasurementExists(name []byte) (bool, error) {
	return s.engine.MeasurementExists(name)
}

// WriteTo writes the shard's data to w.
func (s *Shard) WriteTo(w io.Writer) (int64, error) {
	if err := s.ready(); err != nil {
		return 0, err
	}
	n, err := s.engine.WriteTo(w)
	atomic.AddInt64(&s.stats.BytesWritten, int64(n))
	return n, err
}

// CreateIterator returns an iterator for the data in the shard.
func (s *Shard) CreateIterator(measurement string, opt query.IteratorOptions) (query.Iterator, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}

	if strings.HasPrefix(measurement, "_") {
		if itr, ok, err := s.createSystemIterator(measurement, opt); ok {
			return itr, err
		}
		// Unknown system source so pass this to the engine.
	}
	return s.engine.CreateIterator(measurement, opt)
}

// createSystemIterator returns an iterator for a field of system source.
func (s *Shard) createSystemIterator(measurement string, opt query.IteratorOptions) (query.Iterator, bool, error) {
	switch measurement {
	case "_fieldKeys":
		itr, err := NewFieldKeysIterator(s, opt)
		return itr, true, err
	case "_series":
		itr, err := s.createSeriesIterator(opt)
		return itr, true, err
	case "_tagKeys":
		itr, err := NewTagKeysIterator(s, opt)
		return itr, true, err
	default:
		return nil, false, nil
	}
}

// createSeriesIterator returns a new instance of SeriesIterator.
func (s *Shard) createSeriesIterator(opt query.IteratorOptions) (query.Iterator, error) {
	// Only equality operators are allowed.
	var err error
	influxql.WalkFunc(opt.Condition, func(n influxql.Node) {
		switch n := n.(type) {
		case *influxql.BinaryExpr:
			switch n.Op {
			case influxql.EQ, influxql.NEQ, influxql.EQREGEX, influxql.NEQREGEX,
				influxql.OR, influxql.AND:
			default:
				err = errors.New("invalid tag comparison operator")
			}
		}
	})
	if err != nil {
		return nil, err
	}

	return s.engine.SeriesPointIterator(opt)
}

// IteratorCost returns the estimated cost of constructing and reading an iterator.
func (s *Shard) IteratorCost(measurement string, opt query.IteratorOptions) (query.IteratorCost, error) {
	if err := s.ready(); err != nil {
		return query.IteratorCost{}, err
	}
	return s.engine.IteratorCost(measurement, opt)
}

// FieldDimensions returns unique sets of fields and dimensions across a list of sources.
func (s *Shard) FieldDimensions(measurements []string) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	if err := s.ready(); err != nil {
		return nil, nil, err
	}

	fields = make(map[string]influxql.DataType)
	dimensions = make(map[string]struct{})

	for _, name := range measurements {
		// Handle system sources.
		if strings.HasPrefix(name, "_") {
			var keys []string
			switch name {
			case "_fieldKeys":
				keys = []string{"fieldKey", "fieldType"}
			case "_series":
				keys = []string{"key"}
			case "_tagKeys":
				keys = []string{"tagKey"}
			}

			if len(keys) > 0 {
				for _, k := range keys {
					if _, ok := fields[k]; !ok || influxql.String < fields[k] {
						fields[k] = influxql.String
					}
				}
				continue
			}
			// Unknown system source so default to looking for a measurement.
		}

		// Retrieve measurement.
		if exists, err := s.engine.MeasurementExists([]byte(name)); err != nil {
			return nil, nil, err
		} else if !exists {
			continue
		}

		// Append fields and dimensions.
		mf := s.engine.MeasurementFields([]byte(name))
		if mf != nil {
			for k, typ := range mf.FieldSet() {
				if _, ok := fields[k]; !ok || typ < fields[k] {
					fields[k] = typ
				}
			}
		}

		if err := s.engine.ForEachMeasurementTagKey([]byte(name), func(key []byte) error {
			dimensions[string(key)] = struct{}{}
			return nil
		}); err != nil {
			return nil, nil, err
		}
	}

	return fields, dimensions, nil
}

func (s *Shard) MeasurementsByRegex(re *regexp.Regexp) []string {
	a, _ := s.engine.MeasurementNamesByRegex(re)

	other := make([]string, len(a))
	for i := range a {
		other[i] = string(a[i])
	}
	return other
}

// MapType returns the data type for the field within the measurement.
func (s *Shard) MapType(measurement, field string) influxql.DataType {
	switch field {
	case "_name", "_tagKey", "_tagValue", "_seriesKey":
		return influxql.String
	}

	// Process system measurements.
	if strings.HasPrefix(measurement, "_") {
		switch measurement {
		case "_fieldKeys":
			if field == "fieldKey" || field == "fieldType" {
				return influxql.String
			}
			return influxql.Unknown
		case "_series":
			if field == "key" {
				return influxql.String
			}
			return influxql.Unknown
		case "_tagKeys":
			if field == "tagKey" {
				return influxql.String
			}
			return influxql.Unknown
		}
		// Unknown system source so default to looking for a measurement.
	}

	if exists, _ := s.engine.MeasurementExists([]byte(measurement)); !exists {
		return influxql.Unknown
	}

	mf := s.engine.MeasurementFields([]byte(measurement))
	if mf != nil {
		f := mf.Field(field)
		if f != nil {
			return f.Type
		}
	}

	if exists, _ := s.engine.HasTagKey([]byte(measurement), []byte(field)); exists {
		return influxql.Tag
	}

	return influxql.Unknown
}

// ExpandSources expands regex sources and removes duplicates.
// NOTE: sources must be normalized (db and rp set) before calling this function.
func (s *Shard) ExpandSources(sources influxql.Sources) (influxql.Sources, error) {
	// Use a map as a set to prevent duplicates.
	set := map[string]influxql.Source{}

	// Iterate all sources, expanding regexes when they're found.
	for _, source := range sources {
		switch src := source.(type) {
		case *influxql.Measurement:
			// Add non-regex measurements directly to the set.
			if src.Regex == nil {
				set[src.String()] = src
				continue
			}

			// Loop over matching measurements.
			names, err := s.engine.MeasurementNamesByRegex(src.Regex.Val)
			if err != nil {
				return nil, err
			}

			for _, name := range names {
				other := &influxql.Measurement{
					Database:        src.Database,
					RetentionPolicy: src.RetentionPolicy,
					Name:            string(name),
				}
				set[other.String()] = other
			}

		default:
			return nil, fmt.Errorf("expandSources: unsupported source type: %T", source)
		}
	}

	// Convert set to sorted slice.
	names := make([]string, 0, len(set))
	for name := range set {
		names = append(names, name)
	}
	sort.Strings(names)

	// Convert set to a list of Sources.
	expanded := make(influxql.Sources, 0, len(set))
	for _, name := range names {
		expanded = append(expanded, set[name])
	}

	return expanded, nil
}

// Restore restores data to the underlying engine for the shard.
// The shard is reopened after restore.
func (s *Shard) Restore(r io.Reader, basePath string) error {
	s.mu.Lock()

	// Restore to engine.
	if err := s.engine.Restore(r, basePath); err != nil {
		s.mu.Unlock()
		return err
	}

	s.mu.Unlock()

	// Close shard.
	if err := s.Close(); err != nil {
		return err
	}

	// Reopen engine.
	return s.Open()
}

// Import imports data to the underlying engine for the shard. r should
// be a reader from a backup created by Backup.
func (s *Shard) Import(r io.Reader, basePath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Restore to engine.
	return s.engine.Import(r, basePath)
}

// CreateSnapshot will return a path to a temp directory
// containing hard links to the underlying shard files.
func (s *Shard) CreateSnapshot() (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.engine == nil {
		return "", ErrEngineClosed
	}
	return s.engine.CreateSnapshot()
}

func (s *Shard) ForEachMeasurementTagKey(name []byte, fn func(key []byte) error) error {
	if err := s.ready(); err != nil {
		return nil
	}

	return s.engine.ForEachMeasurementTagKey(name, fn)
}

func (s *Shard) TagKeyCardinality(name, key []byte) int {
	if err := s.ready(); err != nil {
		return 0
	}

	return s.engine.TagKeyCardinality(name, key)
}

type ShardGroup interface {
	MeasurementsByRegex(re *regexp.Regexp) []string
	FieldDimensions(measurements []string) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error)
	MapType(measurement, field string) influxql.DataType
	CreateIterator(measurement string, opt query.IteratorOptions) (query.Iterator, error)
	IteratorCost(measurement string, opt query.IteratorOptions) (query.IteratorCost, error)
	ExpandSources(sources influxql.Sources) (influxql.Sources, error)
}

// Shards represents a sortable list of shards.
type Shards []*Shard

// Len implements sort.Interface.
func (a Shards) Len() int { return len(a) }

// Less implements sort.Interface.
func (a Shards) Less(i, j int) bool { return a[i].id < a[j].id }

// Swap implements sort.Interface.
func (a Shards) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

func (a Shards) MeasurementsByRegex(re *regexp.Regexp) []string {
	m := make(map[string]struct{})
	for _, sh := range a {
		names := sh.MeasurementsByRegex(re)
		for _, name := range names {
			m[name] = struct{}{}
		}
	}

	if len(m) == 0 {
		return nil
	}

	names := make([]string, 0, len(m))
	for key := range m {
		names = append(names, key)
	}
	sort.Strings(names)
	return names
}

func (a Shards) FieldDimensions(measurements []string) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	fields = make(map[string]influxql.DataType)
	dimensions = make(map[string]struct{})

	for _, sh := range a {
		f, d, err := sh.FieldDimensions(measurements)
		if err != nil {
			return nil, nil, err
		}
		for k, typ := range f {
			if _, ok := fields[k]; typ != influxql.Unknown && (!ok || typ < fields[k]) {
				fields[k] = typ
			}
		}
		for k := range d {
			dimensions[k] = struct{}{}
		}
	}
	return
}

func (a Shards) MapType(measurement, field string) influxql.DataType {
	var typ influxql.DataType
	for _, sh := range a {
		if t := sh.MapType(measurement, field); typ.LessThan(t) {
			typ = t
		}
	}
	return typ
}

func (a Shards) CreateIterator(measurement string, opt query.IteratorOptions) (query.Iterator, error) {
	itrs := make([]query.Iterator, 0, len(a))
	for _, sh := range a {
		itr, err := sh.CreateIterator(measurement, opt)
		if err != nil {
			query.Iterators(itrs).Close()
			return nil, err
		} else if itr == nil {
			continue
		}
		itrs = append(itrs, itr)

		select {
		case <-opt.InterruptCh:
			query.Iterators(itrs).Close()
			return nil, err
		default:
		}

		// Enforce series limit at creation time.
		if opt.MaxSeriesN > 0 {
			stats := itr.Stats()
			if stats.SeriesN > opt.MaxSeriesN {
				query.Iterators(itrs).Close()
				return nil, fmt.Errorf("max-select-series limit exceeded: (%d/%d)", stats.SeriesN, opt.MaxSeriesN)
			}
		}
	}
	return query.Iterators(itrs).Merge(opt)
}

func (a Shards) IteratorCost(measurement string, opt query.IteratorOptions) (query.IteratorCost, error) {
	var costs query.IteratorCost
	var costerr error
	var mu sync.RWMutex

	limit := limiter.NewFixed(runtime.GOMAXPROCS(0))
	var wg sync.WaitGroup
	for _, sh := range a {
		limit.Take()
		wg.Add(1)

		mu.RLock()
		if costerr != nil {
			mu.RUnlock()
			break
		}
		mu.RUnlock()

		go func(sh *Shard) {
			defer limit.Release()
			defer wg.Done()

			cost, err := sh.IteratorCost(measurement, opt)

			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				if costerr == nil {
					costerr = err
				}
				return
			}
			costs = costs.Combine(cost)
		}(sh)
	}
	wg.Wait()
	return costs, costerr
}

func (a Shards) ExpandSources(sources influxql.Sources) (influxql.Sources, error) {
	// Use a map as a set to prevent duplicates.
	set := map[string]influxql.Source{}

	// Iterate through every shard and expand the sources.
	for _, sh := range a {
		expanded, err := sh.ExpandSources(sources)
		if err != nil {
			return nil, err
		}

		for _, src := range expanded {
			switch src := src.(type) {
			case *influxql.Measurement:
				set[src.String()] = src
			default:
				return nil, fmt.Errorf("Store.ExpandSources: unsupported source type: %T", src)
			}
		}
	}

	// Convert set to sorted slice.
	names := make([]string, 0, len(set))
	for name := range set {
		names = append(names, name)
	}
	sort.Strings(names)

	// Convert set to a list of Sources.
	sorted := make([]influxql.Source, 0, len(set))
	for _, name := range names {
		sorted = append(sorted, set[name])
	}
	return sorted, nil
}

// MeasurementFields holds the fields of a measurement and their codec.
type MeasurementFields struct {
	mu sync.RWMutex

	fields map[string]*Field
}

// NewMeasurementFields returns an initialised *MeasurementFields value.
func NewMeasurementFields() *MeasurementFields {
	return &MeasurementFields{fields: make(map[string]*Field)}
}

func (m *MeasurementFields) FieldKeys() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	a := make([]string, 0, len(m.fields))
	for key := range m.fields {
		a = append(a, key)
	}
	sort.Strings(a)
	return a
}

// MarshalBinary encodes the object to a binary format.
func (m *MeasurementFields) MarshalBinary() ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var pb internal.MeasurementFields
	for _, f := range m.fields {
		id := int32(f.ID)
		name := f.Name
		t := int32(f.Type)
		pb.Fields = append(pb.Fields, &internal.Field{ID: &id, Name: &name, Type: &t})
	}
	return proto.Marshal(&pb)
}

// UnmarshalBinary decodes the object from a binary format.
func (m *MeasurementFields) UnmarshalBinary(buf []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var pb internal.MeasurementFields
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}
	m.fields = make(map[string]*Field, len(pb.Fields))
	for _, f := range pb.Fields {
		m.fields[f.GetName()] = &Field{ID: uint8(f.GetID()), Name: f.GetName(), Type: influxql.DataType(f.GetType())}
	}
	return nil
}

// CreateFieldIfNotExists creates a new field with an autoincrementing ID.
// Returns an error if 255 fields have already been created on the measurement or
// the fields already exists with a different type.
func (m *MeasurementFields) CreateFieldIfNotExists(name []byte, typ influxql.DataType, limitCount bool) error {
	m.mu.RLock()

	// Ignore if the field already exists.
	if f := m.fields[string(name)]; f != nil {
		if f.Type != typ {
			m.mu.RUnlock()
			return ErrFieldTypeConflict
		}
		m.mu.RUnlock()
		return nil
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	// Re-check field and type under write lock.
	if f := m.fields[string(name)]; f != nil {
		if f.Type != typ {
			return ErrFieldTypeConflict
		}
		return nil
	}

	// Create and append a new field.
	f := &Field{
		ID:   uint8(len(m.fields) + 1),
		Name: string(name),
		Type: typ,
	}
	m.fields[string(name)] = f

	return nil
}

func (m *MeasurementFields) FieldN() int {
	m.mu.RLock()
	n := len(m.fields)
	m.mu.RUnlock()
	return n
}

// Field returns the field for name, or nil if there is no field for name.
func (m *MeasurementFields) Field(name string) *Field {
	m.mu.RLock()
	f := m.fields[name]
	m.mu.RUnlock()
	return f
}

func (m *MeasurementFields) HasField(name string) bool {
	m.mu.RLock()
	f := m.fields[name]
	m.mu.RUnlock()
	return f != nil
}

// FieldBytes returns the field for name, or nil if there is no field for name.
// FieldBytes should be preferred to Field when the caller has a []byte, because
// it avoids a string allocation, which can't be avoided if the caller converts
// the []byte to a string and calls Field.
func (m *MeasurementFields) FieldBytes(name []byte) *Field {
	m.mu.RLock()
	f := m.fields[string(name)]
	m.mu.RUnlock()
	return f
}

// FieldSet returns the set of fields and their types for the measurement.
func (m *MeasurementFields) FieldSet() map[string]influxql.DataType {
	m.mu.RLock()
	defer m.mu.RUnlock()

	fields := make(map[string]influxql.DataType)
	for name, f := range m.fields {
		fields[name] = f.Type
	}
	return fields
}

// Clone returns copy of the MeasurementFields
func (m *MeasurementFields) Clone() *MeasurementFields {
	m.mu.RLock()
	defer m.mu.RUnlock()
	fields := make(map[string]*Field, len(m.fields))
	for key, field := range m.fields {
		fields[key] = field
	}
	return &MeasurementFields{
		fields: fields,
	}
}

// MeasurementFieldSet represents a collection of fields by measurement.
// This safe for concurrent use.
type MeasurementFieldSet struct {
	mu     sync.RWMutex
	fields map[string]*MeasurementFields
}

// NewMeasurementFieldSet returns a new instance of MeasurementFieldSet.
func NewMeasurementFieldSet() *MeasurementFieldSet {
	return &MeasurementFieldSet{
		fields: make(map[string]*MeasurementFields),
	}
}

// Fields returns fields for a measurement by name.
func (fs *MeasurementFieldSet) Fields(name string) *MeasurementFields {
	fs.mu.RLock()
	mf := fs.fields[name]
	fs.mu.RUnlock()
	return mf
}

// CreateFieldsIfNotExists returns fields for a measurement by name.
func (fs *MeasurementFieldSet) CreateFieldsIfNotExists(name []byte) *MeasurementFields {
	fs.mu.RLock()
	mf := fs.fields[string(name)]
	fs.mu.RUnlock()

	if mf != nil {
		return mf
	}

	fs.mu.Lock()
	mf = fs.fields[string(name)]
	if mf == nil {
		mf = NewMeasurementFields()
		fs.fields[string(name)] = mf
	}
	fs.mu.Unlock()
	return mf
}

// Delete removes a field set for a measurement.
func (fs *MeasurementFieldSet) Delete(name string) {
	fs.mu.Lock()
	delete(fs.fields, name)
	fs.mu.Unlock()
}

// DeleteWithLock executes fn and removes a field set from a measurement under lock.
func (fs *MeasurementFieldSet) DeleteWithLock(name string, fn func() error) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if err := fn(); err != nil {
		return err
	}

	delete(fs.fields, name)
	return nil
}

// Field represents a series field.
type Field struct {
	ID   uint8             `json:"id,omitempty"`
	Name string            `json:"name,omitempty"`
	Type influxql.DataType `json:"type,omitempty"`
}

// NewFieldKeysIterator returns an iterator that can be iterated over to
// retrieve field keys.
func NewFieldKeysIterator(sh *Shard, opt query.IteratorOptions) (query.Iterator, error) {
	itr := &fieldKeysIterator{sh: sh}

	// Retrieve measurements from shard. Filter if condition specified.
	names, err := sh.engine.MeasurementNamesByExpr(opt.Condition)
	if err != nil {
		return nil, err
	}
	itr.names = names

	return itr, nil
}

// fieldKeysIterator iterates over measurements and gets field keys from each measurement.
type fieldKeysIterator struct {
	sh    *Shard
	names [][]byte // remaining measurement names
	buf   struct {
		name   []byte  // current measurement name
		fields []Field // current measurement's fields
	}
}

// Stats returns stats about the points processed.
func (itr *fieldKeysIterator) Stats() query.IteratorStats { return query.IteratorStats{} }

// Close closes the iterator.
func (itr *fieldKeysIterator) Close() error { return nil }

// Next emits the next tag key name.
func (itr *fieldKeysIterator) Next() (*query.FloatPoint, error) {
	for {
		// If there are no more keys then move to the next measurements.
		if len(itr.buf.fields) == 0 {
			if len(itr.names) == 0 {
				return nil, nil
			}

			itr.buf.name = itr.names[0]
			mf := itr.sh.engine.MeasurementFields(itr.buf.name)
			if mf != nil {
				fset := mf.FieldSet()
				if len(fset) == 0 {
					itr.names = itr.names[1:]
					continue
				}

				keys := make([]string, 0, len(fset))
				for k := range fset {
					keys = append(keys, k)
				}
				sort.Strings(keys)

				itr.buf.fields = make([]Field, len(keys))
				for i, name := range keys {
					itr.buf.fields[i] = Field{Name: name, Type: fset[name]}
				}
			}
			itr.names = itr.names[1:]
			continue
		}

		// Return next key.
		field := itr.buf.fields[0]
		p := &query.FloatPoint{
			Name: string(itr.buf.name),
			Aux:  []interface{}{field.Name, field.Type.String()},
		}
		itr.buf.fields = itr.buf.fields[1:]

		return p, nil
	}
}

// NewTagKeysIterator returns a new instance of TagKeysIterator.
func NewTagKeysIterator(sh *Shard, opt query.IteratorOptions) (query.Iterator, error) {
	fn := func(name []byte) ([][]byte, error) {
		var keys [][]byte
		if err := sh.engine.ForEachMeasurementTagKey(name, func(key []byte) error {
			keys = append(keys, key)
			return nil
		}); err != nil {
			return nil, err
		}
		return keys, nil
	}
	return newMeasurementKeysIterator(sh, fn, opt)
}

// measurementKeyFunc is the function called by measurementKeysIterator.
type measurementKeyFunc func(name []byte) ([][]byte, error)

func newMeasurementKeysIterator(sh *Shard, fn measurementKeyFunc, opt query.IteratorOptions) (*measurementKeysIterator, error) {
	itr := &measurementKeysIterator{fn: fn}

	names, err := sh.engine.MeasurementNamesByExpr(opt.Condition)
	if err != nil {
		return nil, err
	}
	itr.names = names

	return itr, nil
}

// measurementKeysIterator iterates over measurements and gets keys from each measurement.
type measurementKeysIterator struct {
	names [][]byte // remaining measurement names
	buf   struct {
		name []byte   // current measurement name
		keys [][]byte // current measurement's keys
	}
	fn measurementKeyFunc
}

// Stats returns stats about the points processed.
func (itr *measurementKeysIterator) Stats() query.IteratorStats { return query.IteratorStats{} }

// Close closes the iterator.
func (itr *measurementKeysIterator) Close() error { return nil }

// Next emits the next tag key name.
func (itr *measurementKeysIterator) Next() (*query.FloatPoint, error) {
	for {
		// If there are no more keys then move to the next measurements.
		if len(itr.buf.keys) == 0 {
			if len(itr.names) == 0 {
				return nil, nil
			}

			itr.buf.name, itr.names = itr.names[0], itr.names[1:]

			keys, err := itr.fn(itr.buf.name)
			if err != nil {
				return nil, err
			}
			itr.buf.keys = keys
			continue
		}

		// Return next key.
		p := &query.FloatPoint{
			Name: string(itr.buf.name),
			Aux:  []interface{}{string(itr.buf.keys[0])},
		}
		itr.buf.keys = itr.buf.keys[1:]

		return p, nil
	}
}

// LimitError represents an error caused by a configurable limit.
type LimitError struct {
	Reason string
}

func (e *LimitError) Error() string { return e.Reason }
