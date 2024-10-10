package tsdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unsafe"

	"github.com/influxdata/influxdb/v2/influxql/query"
	"github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/pkg/bytesutil"
	errors2 "github.com/influxdata/influxdb/v2/pkg/errors"
	"github.com/influxdata/influxdb/v2/pkg/estimator"
	"github.com/influxdata/influxdb/v2/pkg/file"
	"github.com/influxdata/influxdb/v2/pkg/limiter"
	"github.com/influxdata/influxdb/v2/pkg/slices"
	internal "github.com/influxdata/influxdb/v2/tsdb/internal"
	"github.com/influxdata/influxql"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const (
	measurementKey        = "_name"
	DefaultMetricInterval = 10 * time.Second
	FieldsChangeFile      = "fields.idxl"
	bytesInInt64          = 8
)

var (
	// ErrFieldTypeConflict is returned when a new field already exists with a different type.
	ErrFieldTypeConflict = errors.New("field type conflict")

	// ErrEngineClosed is returned when a caller attempts indirectly to
	// access the shard's underlying engine.
	ErrEngineClosed = errors.New("engine is closed")

	// ErrShardDisabled is returned when a the shard is not available for
	// queries or writes.
	ErrShardDisabled = errors.New("shard is disabled")

	// ErrUnknownFieldsFormat is returned when the fields index file is not identifiable by
	// the file's magic number.
	ErrUnknownFieldsFormat = errors.New("unknown field index format")

	// ErrUnknownFieldType is returned when the type of a field cannot be determined.
	ErrUnknownFieldType = errors.New("unknown field type")

	// ErrShardNotIdle is returned when an operation requiring the shard to be idle/cold is
	// attempted on a hot shard.
	ErrShardNotIdle = errors.New("shard not idle")

	// fieldsIndexMagicNumber is the file magic number for the fields index file.
	fieldsIndexMagicNumber = []byte{0, 6, 1, 3}
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

// Unwrap returns the underlying error.
func (e ShardError) Unwrap() error {
	return e.Err
}

// PartialWriteError indicates a write request could only write a portion of the
// requested values.
type PartialWriteError struct {
	Reason  string
	Dropped int

	// A sorted slice of series keys that were dropped.
	DroppedKeys [][]byte
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

	sfile   *SeriesFile
	options EngineOptions

	mu      sync.RWMutex
	_engine Engine
	index   Index
	enabled bool

	stats *ShardMetrics

	baseLogger *zap.Logger
	logger     *zap.Logger

	metricUpdater *ticker

	EnableOnOpen bool

	// CompactionDisabled specifies the shard should not schedule compactions.
	// This option is intended for offline tooling.
	CompactionDisabled bool
}

// NewShard returns a new initialized Shard. walPath doesn't apply to the b1 type index
func NewShard(id uint64, path string, walPath string, sfile *SeriesFile, opt EngineOptions) *Shard {
	db, rp := decodeStorePath(path)
	logger := zap.NewNop()

	engineTags := EngineTags{
		Path:          path,
		WalPath:       walPath,
		Id:            fmt.Sprintf("%d", id),
		Bucket:        db,
		EngineVersion: opt.EngineVersion,
	}

	s := &Shard{
		id:              id,
		path:            path,
		walPath:         walPath,
		sfile:           sfile,
		options:         opt,
		stats:           newShardMetrics(engineTags),
		database:        db,
		retentionPolicy: rp,
		logger:          logger,
		baseLogger:      logger,
		EnableOnOpen:    true,
	}
	return s
}

// WithLogger sets the logger on the shard. It must be called before Open.
func (s *Shard) WithLogger(log *zap.Logger) {
	s.baseLogger = log
	engine, err := s.Engine()
	if err == nil {
		engine.WithLogger(s.baseLogger)
		s.index.WithLogger(s.baseLogger)
	}
	s.logger = s.baseLogger.With(zap.String("service", "shard"))
}

// SetEnabled enables the shard for queries and write.  When disabled, all
// writes and queries return an error and compactions are stopped for the shard.
func (s *Shard) SetEnabled(enabled bool) {
	s.mu.Lock()
	s.setEnabledNoLock(enabled)
	s.mu.Unlock()
}

// ! setEnabledNoLock performs actual work of SetEnabled. Must hold s.mu before calling.
func (s *Shard) setEnabledNoLock(enabled bool) {
	// Prevent writes and queries
	s.enabled = enabled
	if s._engine != nil && !s.CompactionDisabled {
		// Disable background compactions and snapshotting
		s._engine.SetEnabled(enabled)
	}
}

// SetNewReadersBlocked sets if new readers can access the shard. If blocked
// is true, the number of reader blocks is incremented and new readers will
// receive an error instead of shard access. If blocked is false, the number
// of reader blocks is decremented. If the reader blocks drops to 0, then
// new readers will be granted access to the shard.
func (s *Shard) SetNewReadersBlocked(blocked bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s._engine.SetNewReadersBlocked(blocked)
}

// InUse returns true if this shard is in-use.
func (s *Shard) InUse() (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s._engine.InUse()
}

// ScheduleFullCompaction forces a full compaction to be schedule on the shard.
func (s *Shard) ScheduleFullCompaction() error {
	engine, err := s.Engine()
	if err != nil {
		return err
	}
	return engine.ScheduleFullCompaction()
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

var globalShardMetrics = newAllShardMetrics()

type twoCounterObserver struct {
	count prometheus.Counter
	sum   prometheus.Counter
}

func (t twoCounterObserver) Observe(f float64) {
	t.sum.Inc()
	t.count.Add(f)
}

var _ prometheus.Observer = twoCounterObserver{}

type allShardMetrics struct {
	writes        *prometheus.CounterVec
	writesSum     *prometheus.CounterVec
	writesErr     *prometheus.CounterVec
	writesErrSum  *prometheus.CounterVec
	writesDropped *prometheus.CounterVec
	fieldsCreated *prometheus.CounterVec
	diskSize      *prometheus.GaugeVec
	series        *prometheus.GaugeVec
}

type ShardMetrics struct {
	writes        prometheus.Observer
	writesErr     prometheus.Observer
	writesDropped prometheus.Counter
	fieldsCreated prometheus.Counter
	diskSize      prometheus.Gauge
	series        prometheus.Gauge
}

const storageNamespace = "storage"
const shardSubsystem = "shard"

func newAllShardMetrics() *allShardMetrics {
	labels := EngineLabelNames()
	return &allShardMetrics{
		writes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: storageNamespace,
			Subsystem: shardSubsystem,
			Name:      "write_count",
			Help:      "Count of the number of write requests",
		}, labels),
		writesSum: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: storageNamespace,
			Subsystem: shardSubsystem,
			Name:      "write_sum",
			Help:      "Counter of the number of points for write requests",
		}, labels),
		writesErr: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: storageNamespace,
			Subsystem: shardSubsystem,
			Name:      "write_err_count",
			Help:      "Count of the number of write requests with errors",
		}, labels),
		writesErrSum: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: storageNamespace,
			Subsystem: shardSubsystem,
			Name:      "write_err_sum",
			Help:      "Counter of the number of points for write requests with errors",
		}, labels),
		writesDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: storageNamespace,
			Subsystem: shardSubsystem,
			Name:      "write_dropped_sum",
			Help:      "Counter of the number of points droppped",
		}, labels),
		fieldsCreated: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: storageNamespace,
			Subsystem: shardSubsystem,
			Name:      "fields_created",
			Help:      "Counter of the number of fields created",
		}, labels),
		diskSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: storageNamespace,
			Subsystem: shardSubsystem,
			Name:      "disk_size",
			Help:      "Gauge of the disk size for the shard",
		}, labels),
		series: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: storageNamespace,
			Subsystem: shardSubsystem,
			Name:      "series",
			Help:      "Gauge of the number of series in the shard index",
		}, labels),
	}
}

func ShardCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		globalShardMetrics.writes,
		globalShardMetrics.writesSum,
		globalShardMetrics.writesErr,
		globalShardMetrics.writesErrSum,
		globalShardMetrics.writesDropped,
		globalShardMetrics.fieldsCreated,
		globalShardMetrics.diskSize,
		globalShardMetrics.series,
	}
}

func newShardMetrics(tags EngineTags) *ShardMetrics {
	labels := tags.GetLabels()
	return &ShardMetrics{
		writes: twoCounterObserver{
			count: globalShardMetrics.writes.With(labels),
			sum:   globalShardMetrics.writesSum.With(labels),
		},
		writesErr: twoCounterObserver{
			count: globalShardMetrics.writesErr.With(labels),
			sum:   globalShardMetrics.writesErrSum.With(labels),
		},
		writesDropped: globalShardMetrics.writesDropped.With(labels),
		fieldsCreated: globalShardMetrics.fieldsCreated.With(labels),
		diskSize:      globalShardMetrics.diskSize.With(labels),
		series:        globalShardMetrics.series.With(labels),
	}
}

// ticker runs fn periodically, and stops when Stop() is called
//
// Stop waits for the last function run to finish if already running
type ticker struct {
	wg      sync.WaitGroup
	closing chan struct{}
}

// Stops the ticker and waits for the function to complete
func (t *ticker) Stop() {
	close(t.closing)
	t.wg.Wait()
}

// Path returns the path set on the shard when it was created.
func (s *Shard) Path() string { return s.path }

// Open initializes and opens the shard's store.
func (s *Shard) Open(ctx context.Context) error {
	s.mu.Lock()
	closeWaitNeeded, err := s.openNoLock(ctx)
	s.mu.Unlock()
	if closeWaitNeeded {
		werr := s.closeWait()
		// We want the first error we get returned to the caller
		if err == nil {
			err = werr
		}
	}
	return err
}

// openNoLock performs work of Open. Must hold s.mu before calling. The first return
// value is true if the caller should call closeWait after unlocking s.mu in order
// to clean up a failed open operation.
func (s *Shard) openNoLock(ctx context.Context) (bool, error) {
	if err := func() error {
		// Return if the shard is already open
		if s._engine != nil {
			return nil
		}

		seriesIDSet := NewSeriesIDSet()

		// Initialize underlying index.
		ipath := filepath.Join(s.path, "index")
		idx, err := NewIndex(s.id, s.database, ipath, seriesIDSet, s.sfile, s.options)
		if err != nil {
			return err
		}
		idx.WithLogger(s.baseLogger)

		// Check if the index needs to be rebuilt before Open() initializes
		// its file system layout.
		var shouldReindex bool
		if _, err := os.Stat(ipath); os.IsNotExist(err) {
			shouldReindex = true
		}

		// Open index.
		if err := idx.Open(); err != nil {
			return err
		}
		s.index = idx

		// Initialize underlying engine.
		e, err := NewEngine(s.id, idx, s.path, s.walPath, s.sfile, s.options)
		if err != nil {
			return err
		}

		// Set log output on the engine.
		e.WithLogger(s.baseLogger)

		// Disable compactions while loading the index
		e.SetEnabled(false)

		// Open engine.
		if err := e.Open(ctx); err != nil {
			return err
		}
		if shouldReindex {
			if err := e.Reindex(); err != nil {
				return err
			}
		}

		if err := e.LoadMetadataIndex(s.id, s.index); err != nil {
			return err
		}
		s._engine = e

		// Set up metric collection
		metricUpdater := &ticker{
			closing: make(chan struct{}),
		}

		// We want a way to turn off the series and disk size metrics if they are suspected to cause issues
		// This corresponds to the top-level MetricsDisabled argument
		if !s.options.MetricsDisabled {
			metricUpdater.wg.Add(1)
			go func() {
				tick := time.NewTicker(DefaultMetricInterval)
				defer metricUpdater.wg.Done()
				defer tick.Stop()
				for {
					select {
					case <-tick.C:
						// Note this takes the engine lock, so we have to be careful not
						// to close metricUpdater.closing while holding the engine lock
						e, err := s.Engine()
						if err != nil {
							continue
						}
						s.stats.series.Set(float64(e.SeriesN()))
						s.stats.diskSize.Set(float64(e.DiskSize()))
					case <-metricUpdater.closing:
						return
					}
				}
			}()
		}

		s.metricUpdater = metricUpdater

		return nil
	}(); err != nil {
		s.closeNoLock(false)
		return true, NewShardError(s.id, err)
	}

	if s.EnableOnOpen {
		// enable writes, queries and compactions
		s.setEnabledNoLock(true)
	}

	return false, nil
}

// FlushAndClose flushes the shard's WAL and then closes down the shard's store.
func (s *Shard) FlushAndClose() error {
	return s.closeAndWait(true)
}

// closeAndWait shuts down the shard's store and waits for the close to complete.
// If flush is true, the WAL is flushed and cleared before closing.
func (s *Shard) closeAndWait(flush bool) error {
	err := func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.closeNoLock(flush)
	}()
	// make sure not to hold a lock while waiting for close to finish
	werr := s.closeWait()

	if err != nil {
		return err
	}
	return werr

}

// Close shuts down the shard's store.
func (s *Shard) Close() error {
	return s.closeAndWait(false)
}

// closeNoLock closes the shard an removes reference to the shard from associated
// indexes. If flush is true, the WAL is flushed and cleared before closing.
// The s.mu mutex must be held before calling closeNoLock. closeWait should always
// be called after calling closeNoLock.
func (s *Shard) closeNoLock(flush bool) error {
	if s._engine == nil {
		return nil
	}

	if s.metricUpdater != nil {
		close(s.metricUpdater.closing)
	}

	err := s._engine.Close(flush)
	if err == nil {
		s._engine = nil
	}

	if e := s.index.Close(); e == nil {
		s.index = nil
	}
	return err
}

// closeWait waits for goroutines and other background operations associated with this
// shard to complete after closeNoLock is called. Must only be called after calling
// closeNoLock. closeWait should always be called after calling closeNoLock.
// Public methods which close the shard should call closeWait after closeNoLock before
// returning. Must be called without holding shard locks to avoid deadlocking.
func (s *Shard) closeWait() error {
	if s.metricUpdater != nil {
		s.metricUpdater.wg.Wait()
	}
	return nil
}

// IndexType returns the index version being used for this shard.
//
// IndexType returns the empty string if it is called before the shard is opened,
// since it is only that point that the underlying index type is known.
func (s *Shard) IndexType() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s._engine == nil || s.index == nil { // Shard not open yet.
		return ""
	}
	return s.index.Type()
}

// ready determines if the Shard is ready for queries or writes.
// It returns nil if ready, otherwise ErrShardClosed or ErrShardDisabled
func (s *Shard) ready() error {
	var err error
	if s._engine == nil {
		err = ErrEngineClosed
	} else if !s.enabled {
		err = ErrShardDisabled
	}
	return err
}

// LastModified returns the time when this shard was last modified.
func (s *Shard) LastModified() time.Time {
	engine, err := s.Engine()
	if err != nil {
		return time.Time{}
	}
	return engine.LastModified()
}

// Index returns a reference to the underlying index. It returns an error if
// the index is nil.
func (s *Shard) Index() (Index, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.ready(); err != nil {
		return nil, err
	}
	return s.index, nil
}

// SeriesFile returns a reference the underlying series file. If return an error
// if the series file is nil.
func (s *Shard) SeriesFile() (*SeriesFile, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.ready(); err != nil {
		return nil, err
	}
	return s.sfile, nil
}

// IsIdle return true if the shard is not receiving writes and is fully compacted.
func (s *Shard) IsIdle() (state bool, reason string) {
	engine, err := s.Engine()
	if err != nil {
		return true, ""
	}
	return engine.IsIdle()
}

func (s *Shard) Free() error {
	engine, err := s.Engine()
	if err != nil {
		return err
	}

	// Disable compactions to stop background goroutines
	s.SetCompactionsEnabled(false)

	return engine.Free()
}

// SetCompactionsEnabled enables or disable shard background compactions.
func (s *Shard) SetCompactionsEnabled(enabled bool) {
	engine, err := s.Engine()
	if err != nil {
		return
	}
	engine.SetCompactionsEnabled(enabled)
}

// DiskSize returns the size on disk of this shard.
func (s *Shard) DiskSize() (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// We don't use engine() because we still want to report the shard's disk
	// size even if the shard has been disabled.
	if s._engine == nil {
		return 0, ErrEngineClosed
	}
	size := s._engine.DiskSize()
	return size, nil
}

// FieldCreate holds information for a field to create on a measurement.
type FieldCreate struct {
	Measurement []byte
	Field       *Field
}

// WritePoints will write the raw data points and any new metadata to the index in the shard.
func (s *Shard) WritePoints(ctx context.Context, points []models.Point) (rErr error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	engine, err := s.engineNoLock()
	if err != nil {
		return err
	}

	var writeError error
	s.stats.writes.Observe(float64(len(points)))
	defer func() {
		if rErr != nil {
			s.stats.writesErr.Observe(float64(len(points)))
		}
	}()

	points, fieldsToCreate, err := s.validateSeriesAndFields(points)
	if err != nil {
		if _, ok := err.(PartialWriteError); !ok {
			return err
		}
		// There was a partial write (points dropped), hold onto the error to return
		// to the caller, but continue on writing the remaining points.
		writeError = err
	}
	s.stats.fieldsCreated.Add(float64(len(fieldsToCreate)))

	// add any new fields and keep track of what needs to be saved
	if err := s.createFieldsAndMeasurements(fieldsToCreate); err != nil {
		return err
	}

	// Write to the engine.
	if err := engine.WritePoints(ctx, points); err != nil {
		return fmt.Errorf("engine: %s", err)
	}

	return writeError
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

	// Check if keys should be unicode validated.
	validateKeys := s.options.Config.ValidateKeys

	var j int
	for i, p := range points {
		tags := p.Tags()

		// Drop any series w/ a "time" tag, these are illegal
		if v := tags.Get(timeBytes); v != nil {
			dropped++
			if reason == "" {
				reason = fmt.Sprintf(
					"invalid tag key: input tag \"%s\" on measurement \"%s\" is invalid",
					"time", string(p.Name()))
			}
			continue
		}

		// Drop any series with invalid unicode characters in the key.
		if validateKeys && !models.ValidKeyTokens(string(p.Name()), tags) {
			dropped++
			if reason == "" {
				reason = fmt.Sprintf("key contains invalid unicode: %q", makePrintable(string(p.Key())))
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

	engine, err := s.engineNoLock()
	if err != nil {
		return nil, nil, err
	}

	// Add new series. Check for partial writes.
	var droppedKeys [][]byte
	if err := engine.CreateSeriesListIfNotExists(keys, names, tagsSlice); err != nil {
		switch err := err.(type) {
		// (DSB) This was previously *PartialWriteError. Now catch pointer and value types.
		case *PartialWriteError:
			reason = err.Reason
			dropped += err.Dropped
			droppedKeys = err.DroppedKeys
			s.stats.writesDropped.Add(float64(err.Dropped))
		case PartialWriteError:
			reason = err.Reason
			dropped += err.Dropped
			droppedKeys = err.DroppedKeys
			s.stats.writesDropped.Add(float64(err.Dropped))
		default:
			return nil, nil, err
		}
	}

	j = 0
	for i, p := range points {
		// Skip any points with only invalid fields.
		iter := p.FieldIterator()
		validField := false
		for iter.Next() {
			if bytes.Equal(iter.FieldKey(), timeBytes) {
				continue
			}
			validField = true
			break
		}
		if !validField {
			if reason == "" {
				reason = fmt.Sprintf(
					"invalid field name: input field \"%s\" on measurement \"%s\" is invalid",
					"time", string(p.Name()))
			}
			dropped++
			continue
		}

		// Skip any points whos keys have been dropped. Dropped has already been incremented for them.
		if len(droppedKeys) > 0 && bytesutil.Contains(droppedKeys, keys[i]) {
			continue
		}

		name := p.Name()
		mf := engine.MeasurementFields(name)

		// Check with the field validator.
		if err := ValidateFields(mf, p, s.options.Config.SkipFieldSizeValidation); err != nil {
			switch err := err.(type) {
			case PartialWriteError:
				if reason == "" {
					reason = err.Reason
				}
				dropped += err.Dropped
				s.stats.writesDropped.Add(float64(err.Dropped))
			default:
				return nil, nil, err
			}
			continue
		}

		points[j] = points[i]
		j++

		// Create any fields that are missing.
		iter.Reset()
		for iter.Next() {
			fieldKey := iter.FieldKey()

			// Skip fields named "time". They are illegal.
			if bytes.Equal(fieldKey, timeBytes) {
				continue
			}

			if mf.FieldBytes(fieldKey) != nil {
				continue
			}

			dataType := dataTypeFromModelsFieldType(iter.Type())
			if dataType == influxql.Unknown {
				continue
			}

			fieldsToCreate = append(fieldsToCreate, &FieldCreate{
				Measurement: name,
				Field: &Field{
					Name: string(fieldKey),
					Type: dataType,
				},
			})
		}
	}

	if dropped > 0 {
		err = PartialWriteError{Reason: reason, Dropped: dropped}
	}

	return points[:j], fieldsToCreate, err
}

const unPrintReplRune = '?'
const unPrintMaxReplRune = 3

// makePrintable - replace invalid and non-printable unicode characters with a few '?' runes
func makePrintable(s string) string {
	b := strings.Builder{}
	b.Grow(len(s))
	c := 0
	for _, r := range strings.ToValidUTF8(s, string(unicode.ReplacementChar)) {
		if !unicode.IsPrint(r) || r == unicode.ReplacementChar {
			if c < unPrintMaxReplRune {
				b.WriteRune(unPrintReplRune)
			}
			c++
		} else {
			b.WriteRune(r)
			c = 0
		}
	}
	return b.String()
}

func (s *Shard) createFieldsAndMeasurements(fieldsToCreate []*FieldCreate) error {
	if len(fieldsToCreate) == 0 {
		return nil
	}

	engine, err := s.engineNoLock()
	if err != nil {
		return err
	}

	// add fields
	changes := make([]*FieldChange, 0, len(fieldsToCreate))
	for _, f := range fieldsToCreate {
		mf := engine.MeasurementFields(f.Measurement)
		if err := mf.CreateFieldIfNotExists([]byte(f.Field.Name), f.Field.Type); err != nil {
			return err
		}
		changes = append(changes, &FieldChange{
			FieldCreate: *f,
			ChangeType:  AddMeasurementField,
		})
	}

	return engine.MeasurementFieldSet().Save(changes)
}

// DeleteSeriesRange deletes all values from for seriesKeys between min and max (inclusive)
func (s *Shard) DeleteSeriesRange(ctx context.Context, itr SeriesIterator, min, max int64) error {
	engine, err := s.Engine()
	if err != nil {
		return err
	}
	return engine.DeleteSeriesRange(ctx, itr, min, max)
}

// DeleteSeriesRangeWithPredicate deletes all values from for seriesKeys between min and max (inclusive)
// for which predicate() returns true. If predicate() is nil, then all values in range are deleted.
func (s *Shard) DeleteSeriesRangeWithPredicate(
	ctx context.Context,
	itr SeriesIterator,
	predicate func(name []byte, tags models.Tags) (int64, int64, bool),
) error {
	engine, err := s.Engine()
	if err != nil {
		return err
	}
	return engine.DeleteSeriesRangeWithPredicate(ctx, itr, predicate)
}

// DeleteMeasurement deletes a measurement and all underlying series.
func (s *Shard) DeleteMeasurement(ctx context.Context, name []byte) error {
	engine, err := s.Engine()
	if err != nil {
		return err
	}
	return engine.DeleteMeasurement(ctx, name)
}

// SeriesN returns the unique number of series in the shard.
func (s *Shard) SeriesN() int64 {
	engine, err := s.Engine()
	if err != nil {
		return 0
	}
	return engine.SeriesN()
}

// SeriesSketches returns the measurement sketches for the shard.
func (s *Shard) SeriesSketches() (estimator.Sketch, estimator.Sketch, error) {
	engine, err := s.Engine()
	if err != nil {
		return nil, nil, err
	}
	return engine.SeriesSketches()
}

// MeasurementsSketches returns the measurement sketches for the shard.
func (s *Shard) MeasurementsSketches() (estimator.Sketch, estimator.Sketch, error) {
	engine, err := s.Engine()
	if err != nil {
		return nil, nil, err
	}
	return engine.MeasurementsSketches()
}

// MeasurementNamesByRegex returns names of measurements matching the regular expression.
func (s *Shard) MeasurementNamesByRegex(re *regexp.Regexp) ([][]byte, error) {
	engine, err := s.Engine()
	if err != nil {
		return nil, err
	}
	return engine.MeasurementNamesByRegex(re)
}

// MeasurementNamesByPredicate returns fields for a measurement filtered by an expression.
func (s *Shard) MeasurementNamesByPredicate(expr influxql.Expr) ([][]byte, error) {
	index, err := s.Index()
	if err != nil {
		return nil, err
	}
	indexSet := IndexSet{Indexes: []Index{index}, SeriesFile: s.sfile}
	return indexSet.MeasurementNamesByPredicate(query.OpenAuthorizer, expr)
}

// MeasurementFields returns fields for a measurement.
func (s *Shard) MeasurementFields(name []byte) *MeasurementFields {
	engine, err := s.Engine()
	if err != nil {
		return nil
	}
	return engine.MeasurementFields(name)
}

// MeasurementExists returns true if the shard contains name.
// TODO(edd): This method is currently only being called from tests; do we
// really need it?
func (s *Shard) MeasurementExists(name []byte) (bool, error) {
	engine, err := s.Engine()
	if err != nil {
		return false, err
	}
	return engine.MeasurementExists(name)
}

// CreateIterator returns an iterator for the data in the shard.
func (s *Shard) CreateIterator(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
	engine, err := s.Engine()
	if err != nil {
		return nil, err
	}
	switch m.SystemIterator {
	case "_fieldKeys":
		return NewFieldKeysIterator(s, opt)
	case "_series":
		// TODO(benbjohnson): Move up to the Shards.CreateIterator().
		index, err := s.Index()
		if err != nil {
			return nil, err
		}
		indexSet := IndexSet{Indexes: []Index{index}, SeriesFile: s.sfile}

		itr, err := NewSeriesPointIterator(indexSet, opt)
		if err != nil {
			return nil, err
		}

		return query.NewInterruptIterator(itr, opt.InterruptCh), nil
	case "_tagKeys":
		return NewTagKeysIterator(s, opt)
	}
	return engine.CreateIterator(ctx, m.Name, opt)
}

func (s *Shard) CreateSeriesCursor(ctx context.Context, req SeriesCursorRequest, cond influxql.Expr) (SeriesCursor, error) {
	index, err := s.Index()
	if err != nil {
		return nil, err
	}
	return newSeriesCursor(req, IndexSet{Indexes: []Index{index}, SeriesFile: s.sfile}, cond)
}

func (s *Shard) CreateCursorIterator(ctx context.Context) (CursorIterator, error) {
	engine, err := s.Engine()
	if err != nil {
		return nil, err
	}
	return engine.CreateCursorIterator(ctx)
}

// FieldDimensions returns unique sets of fields and dimensions across a list of sources.
func (s *Shard) FieldDimensions(measurements []string) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	engine, err := s.Engine()
	if err != nil {
		return nil, nil, err
	}

	fields = make(map[string]influxql.DataType)
	dimensions = make(map[string]struct{})

	index, err := s.Index()
	if err != nil {
		return nil, nil, err
	}
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
					if fields[k].LessThan(influxql.String) {
						fields[k] = influxql.String
					}
				}
				continue
			}
			// Unknown system source so default to looking for a measurement.
		}

		// Retrieve measurement.
		if exists, err := engine.MeasurementExists([]byte(name)); err != nil {
			return nil, nil, err
		} else if !exists {
			continue
		}

		// Append fields and dimensions.
		mf := engine.MeasurementFields([]byte(name))
		if mf != nil {
			for k, typ := range mf.FieldSet() {
				if fields[k].LessThan(typ) {
					fields[k] = typ
				}
			}
		}

		indexSet := IndexSet{Indexes: []Index{index}, SeriesFile: s.sfile}
		if err := indexSet.ForEachMeasurementTagKey([]byte(name), func(key []byte) error {
			dimensions[string(key)] = struct{}{}
			return nil
		}); err != nil {
			return nil, nil, err
		}
	}

	return fields, dimensions, nil
}

// mapType returns the data type for the field within the measurement.
func (s *Shard) mapType(measurement, field string) (influxql.DataType, error) {
	engine, err := s.engineNoLock()
	if err != nil {
		return 0, err
	}

	switch field {
	case "_name", "_tagKey", "_tagValue", "_seriesKey":
		return influxql.String, nil
	}

	// Process system measurements.
	switch measurement {
	case "_fieldKeys":
		if field == "fieldKey" || field == "fieldType" {
			return influxql.String, nil
		}
		return influxql.Unknown, nil
	case "_series":
		if field == "key" {
			return influxql.String, nil
		}
		return influxql.Unknown, nil
	case "_tagKeys":
		if field == "tagKey" {
			return influxql.String, nil
		}
		return influxql.Unknown, nil
	}
	// Unknown system source so default to looking for a measurement.

	if exists, _ := engine.MeasurementExists([]byte(measurement)); !exists {
		return influxql.Unknown, nil
	}

	mf := engine.MeasurementFields([]byte(measurement))
	if mf != nil {
		f := mf.Field(field)
		if f != nil {
			return f.Type, nil
		}
	}

	if exists, _ := engine.HasTagKey([]byte(measurement), []byte(field)); exists {
		return influxql.Tag, nil
	}

	return influxql.Unknown, nil
}

// expandSources expands regex sources and removes duplicates.
// NOTE: sources must be normalized (db and rp set) before calling this function.
func (s *Shard) expandSources(sources influxql.Sources) (influxql.Sources, error) {
	engine, err := s.engineNoLock()
	if err != nil {
		return nil, err
	}

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
			names, err := engine.MeasurementNamesByRegex(src.Regex.Val)
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

// Backup backs up the shard by creating a tar archive of all TSM files that
// have been modified since the provided time. See Engine.Backup for more details.
func (s *Shard) Backup(w io.Writer, basePath string, since time.Time) error {
	engine, err := s.Engine()
	if err != nil {
		return err
	}
	return engine.Backup(w, basePath, since)
}

func (s *Shard) Export(w io.Writer, basePath string, start time.Time, end time.Time) error {
	engine, err := s.Engine()
	if err != nil {
		return err
	}
	return engine.Export(w, basePath, start, end)
}

// Restore restores data to the underlying engine for the shard.
// The shard is reopened after restore.
func (s *Shard) Restore(ctx context.Context, r io.Reader, basePath string) error {
	closeWaitNeeded, err := func() (bool, error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		closeWaitNeeded := false

		// Special case - we can still restore to a disabled shard, so we should
		// only check if the engine is closed and not care if the shard is
		// disabled.
		if s._engine == nil {
			return closeWaitNeeded, ErrEngineClosed
		}

		// Restore to engine.
		if err := s._engine.Restore(r, basePath); err != nil {
			return closeWaitNeeded, nil
		}

		// Close shard.
		closeWaitNeeded = true // about to call closeNoLock, closeWait will be needed
		if err := s.closeNoLock(false); err != nil {
			return closeWaitNeeded, err
		}
		return closeWaitNeeded, nil
	}()

	// Now that we've unlocked, we can call closeWait if needed
	if closeWaitNeeded {
		werr := s.closeWait()
		// Return the first error encountered to the caller
		if err == nil {
			err = werr
		}
	}
	if err != nil {
		return err
	}

	// Reopen engine. Need locked method since we had to unlock for closeWait.
	return s.Open(ctx)
}

// Import imports data to the underlying engine for the shard. r should
// be a reader from a backup created by Backup.
func (s *Shard) Import(r io.Reader, basePath string) error {
	// Special case - we can still import to a disabled shard, so we should
	// only check if the engine is closed and not care if the shard is
	// disabled.
	s.mu.Lock()
	defer s.mu.Unlock()
	if s._engine == nil {
		return ErrEngineClosed
	}

	// Import to engine.
	return s._engine.Import(r, basePath)
}

// CreateSnapshot will return a path to a temp directory
// containing hard links to the underlying shard files.
func (s *Shard) CreateSnapshot(skipCacheOk bool) (string, error) {
	engine, err := s.Engine()
	if err != nil {
		return "", err
	}
	return engine.CreateSnapshot(skipCacheOk)
}

// ForEachMeasurementName iterates over each measurement in the shard.
func (s *Shard) ForEachMeasurementName(fn func(name []byte) error) error {
	engine, err := s.Engine()
	if err != nil {
		return err
	}
	return engine.ForEachMeasurementName(fn)
}

func (s *Shard) TagKeyCardinality(name, key []byte) int {
	engine, err := s.Engine()
	if err != nil {
		return 0
	}
	return engine.TagKeyCardinality(name, key)
}

// Digest returns a digest of the shard.
func (s *Shard) Digest() (io.ReadCloser, int64, string, error) {
	engine, err := s.Engine()
	if err != nil {
		return nil, 0, "", err
	}

	// Make sure the shard is idle/cold. (No use creating a digest of a
	// hot shard that is rapidly changing.)
	if isIdle, reason := engine.IsIdle(); !isIdle {
		return nil, 0, reason, ErrShardNotIdle
	}

	readCloser, size, err := engine.Digest()
	return readCloser, size, "", err
}

// engine safely (under an RLock) returns a reference to the shard's Engine, or
// an error if the Engine is closed, or the shard is currently disabled.
//
// The shard's Engine should always be accessed via a call to engine(), rather
// than directly referencing Shard.engine.
//
// If a caller needs an Engine reference but is already under a lock, then they
// should use engineNoLock().
func (s *Shard) Engine() (Engine, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.engineNoLock()
}

// engineNoLock is similar to calling engine(), but the caller must guarantee
// that they already hold an appropriate lock.
func (s *Shard) engineNoLock() (Engine, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	return s._engine, nil
}

type ShardGroup interface {
	MeasurementsByRegex(re *regexp.Regexp) []string
	FieldKeysByMeasurement(name []byte) []string
	FieldDimensions(measurements []string) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error)
	MapType(measurement, field string) influxql.DataType
	CreateIterator(ctx context.Context, measurement *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error)
	IteratorCost(ctx context.Context, measurement string, opt query.IteratorOptions) (query.IteratorCost, error)
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

// MeasurementsByRegex returns the unique set of measurements matching the
// provided regex, for all the shards.
func (a Shards) MeasurementsByRegex(re *regexp.Regexp) []string {
	var m map[string]struct{}
	for _, sh := range a {
		names, err := sh.MeasurementNamesByRegex(re)
		if err != nil {
			continue // Skip this shard's results—previous behaviour.
		}

		if m == nil {
			m = make(map[string]struct{}, len(names))
		}

		for _, name := range names {
			m[string(name)] = struct{}{}
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

// FieldKeysByMeasurement returns a de-duplicated, sorted, set of field keys for
// the provided measurement name.
func (a Shards) FieldKeysByMeasurement(name []byte) []string {
	if len(a) == 1 {
		mf := a[0].MeasurementFields(name)
		if mf == nil {
			return nil
		}
		return mf.FieldKeys()
	}

	all := make([][]string, 0, len(a))
	for _, shard := range a {
		mf := shard.MeasurementFields(name)
		if mf == nil {
			continue
		}
		all = append(all, mf.FieldKeys())
	}
	return slices.MergeSortedStrings(all...)
}

// MeasurementNamesByPredicate returns the measurements that match the given predicate.
func (a Shards) MeasurementNamesByPredicate(expr influxql.Expr) ([][]byte, error) {
	if len(a) == 1 {
		return a[0].MeasurementNamesByPredicate(expr)
	}

	all := make([][][]byte, len(a))
	for i, shard := range a {
		names, err := shard.MeasurementNamesByPredicate(expr)
		if err != nil {
			return nil, err
		}
		all[i] = names
	}
	return slices.MergeSortedBytes(all...), nil
}

// FieldKeysByPredicate returns the field keys for series that match
// the given predicate.
func (a Shards) FieldKeysByPredicate(expr influxql.Expr) (map[string][]string, error) {
	names, ok := measurementOptimization(expr, measurementKey)
	if !ok {
		var err error
		if names, err = a.MeasurementNamesByPredicate(expr); err != nil {
			return nil, err
		}
	}

	all := make(map[string][]string, len(names))
	for _, name := range names {
		all[string(name)] = a.FieldKeysByMeasurement(name)
	}
	return all, nil
}

// consecutiveAndChildren finds all child nodes of consecutive
// influxql.BinaryExpr with AND operator nodes ("AND nodes") which are not
// themselves AND nodes. This may be the root of the tree if the root of the
// tree is not an AND node.
type consecutiveAndChildren struct {
	children []influxql.Node
}

func (v *consecutiveAndChildren) Visit(node influxql.Node) influxql.Visitor {
	switch n := node.(type) {
	case *influxql.BinaryExpr:
		if n.Op == influxql.AND {
			return v
		}
	case *influxql.ParenExpr:
		// Parens are essentially a no-op and can be traversed through.
		return v
	}

	// If this wasn't a BinaryExpr with an AND operator or a Paren, record this
	// child node and stop the search for this branch.
	v.children = append(v.children, node)
	return nil
}

// orMeasurementTree determines if a tree (or subtree) represents a grouping of
// exclusively measurement names OR'd together with EQ operators for the
// measurements themselves. It collects the list of measurement names
// encountered and records the validity of the tree.
type orMeasurementTree struct {
	measurementKey   string
	measurementNames []string
	valid            bool
}

func (v *orMeasurementTree) Visit(node influxql.Node) influxql.Visitor {
	// Return early if this tree has already been invalidated - no reason to
	// continue evaluating at that point.
	if !v.valid {
		return nil
	}

	switch n := node.(type) {
	case *influxql.BinaryExpr:
		// A BinaryExpr must have an operation of OR or EQ in a valid tree
		if n.Op == influxql.OR {
			return v
		} else if n.Op == influxql.EQ {
			// An EQ must be in the form of "v.measurementKey == measurementName" in a
			// valid tree
			if name, ok := measurementNameFromEqBinary(n, v.measurementKey); ok {
				v.measurementNames = append(v.measurementNames, name)
				// If a valid measurement key/value was found, there is no need to
				// continue evaluating the VarRef/StringLiteral child nodes of this
				// node.
				return nil
			}
		}
	case *influxql.ParenExpr:
		// Parens are essentially a no-op and can be traversed through.
		return v
	}

	// The the type switch didn't already return, this tree is invalid.
	v.valid = false
	return nil
}

func measurementOptimization(expr influxql.Expr, key string) ([][]byte, bool) {
	// A measurement optimization is possible if the query contains a single group
	// of one or more measurements (in the form of _measurement = measName,
	// equality operator only) grouped together by OR operators, with the subtree
	// containing the OR'd measurements accessible from root of the tree either
	// directly (tree contains nothing but OR'd measurements) or by traversing AND
	// binary expression nodes.

	// Get a list of "candidate" measurement subtrees.
	v := consecutiveAndChildren{}
	influxql.Walk(&v, expr)
	possibleSubtrees := v.children

	// Evaluate the candidate subtrees to determine which measurement names they
	// contain, and to see if they are valid for the optimization.
	validSubtrees := []orMeasurementTree{}
	for _, h := range possibleSubtrees {
		t := orMeasurementTree{
			measurementKey: key,
			valid:          true,
		}
		influxql.Walk(&t, h)
		if t.valid {
			validSubtrees = append(validSubtrees, t)
		}
	}

	// There must be exactly one valid measurement subtree for this optimization
	// to be applied. Note: It may also be possible to have measurements in
	// multiple subtrees, as long as there are no measurements in invalid
	// subtrees, by determining an intersection of the measurement names across
	// all valid subtrees - this is not currently implemented.
	if len(validSubtrees) != 1 {
		return nil, false
	}

	return slices.StringsToBytes(validSubtrees[0].measurementNames...), true
}

// measurementNameFromEqBinary returns the name of a measurement from a binary
// expression if possible, and a boolean status indicating if the binary
// expression contained a measurement name. A meausurement name will only be
// returned if the operator for the binary is EQ, and the measurement key is on
// the LHS with the measurement name on the RHS.
func measurementNameFromEqBinary(be *influxql.BinaryExpr, key string) (string, bool) {
	lhs, ok := be.LHS.(*influxql.VarRef)
	if !ok {
		return "", false
	} else if lhs.Val != key {
		return "", false
	}

	rhs, ok := be.RHS.(*influxql.StringLiteral)
	if !ok {
		return "", false
	}

	return rhs.Val, true
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
			if fields[k].LessThan(typ) {
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
		sh.mu.RLock()
		if t, err := sh.mapType(measurement, field); err == nil && typ.LessThan(t) {
			typ = t
		}
		sh.mu.RUnlock()
	}
	return typ
}

func (a Shards) CallType(name string, args []influxql.DataType) (influxql.DataType, error) {
	typmap := query.CallTypeMapper{}
	return typmap.CallType(name, args)
}

func (a Shards) CreateIterator(ctx context.Context, measurement *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
	switch measurement.SystemIterator {
	case "_series":
		return a.createSeriesIterator(ctx, opt)
	}

	itrs := make([]query.Iterator, 0, len(a))
	for _, sh := range a {
		itr, err := sh.CreateIterator(ctx, measurement, opt)
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
			return nil, query.ErrQueryInterrupted
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

func (a Shards) createSeriesIterator(ctx context.Context, opt query.IteratorOptions) (_ query.Iterator, err error) {
	var (
		idxs  = make([]Index, 0, len(a))
		sfile *SeriesFile
	)
	for _, sh := range a {
		var idx Index
		if idx, err = sh.Index(); err == nil {
			idxs = append(idxs, idx)
		}
		if sfile == nil {
			sfile, _ = sh.SeriesFile()
		}
	}

	if sfile == nil {
		return nil, nil
	}

	return NewSeriesPointIterator(IndexSet{Indexes: idxs, SeriesFile: sfile}, opt)
}

func (a Shards) IteratorCost(ctx context.Context, measurement string, opt query.IteratorOptions) (query.IteratorCost, error) {
	var costs query.IteratorCost
	var costerr error
	var mu sync.RWMutex

	setErr := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if costerr == nil {
			costerr = err
		}
	}

	limit := limiter.NewFixed(runtime.GOMAXPROCS(0))
	var wg sync.WaitGroup
	for _, sh := range a {
		costerr = limit.Take(ctx)
		wg.Add(1)

		mu.RLock()
		if costerr != nil {
			limit.Release()
			mu.RUnlock()
			break
		}
		mu.RUnlock()

		go func(sh *Shard) {
			defer limit.Release()
			defer wg.Done()

			engine, err := sh.Engine()
			if err != nil {
				setErr(err)
				return
			}

			cost, err := engine.IteratorCost(measurement, opt)
			if err != nil {
				setErr(err)
				return
			}

			mu.Lock()
			costs = costs.Combine(cost)
			mu.Unlock()
		}(sh)
	}
	wg.Wait()
	return costs, costerr
}

func (a Shards) CreateSeriesCursor(ctx context.Context, req SeriesCursorRequest, cond influxql.Expr) (_ SeriesCursor, err error) {
	var (
		idxs  []Index
		sfile *SeriesFile
	)
	for _, sh := range a {
		var idx Index
		if idx, err = sh.Index(); err == nil {
			idxs = append(idxs, idx)
		}
		if sfile == nil {
			sfile, _ = sh.SeriesFile()
		}
	}

	if sfile == nil {
		return nil, errors.New("CreateSeriesCursor: no series file")
	}

	return newSeriesCursor(req, IndexSet{Indexes: idxs, SeriesFile: sfile}, cond)
}

func (a Shards) ExpandSources(sources influxql.Sources) (influxql.Sources, error) {
	// Use a map as a set to prevent duplicates.
	set := map[string]influxql.Source{}

	// Iterate through every shard and expand the sources.
	for _, sh := range a {
		sh.mu.RLock()
		expanded, err := sh.expandSources(sources)
		sh.mu.RUnlock()
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
	mu sync.Mutex

	fields atomic.Value // map[string]*Field
}

// NewMeasurementFields returns an initialised *MeasurementFields value.
func NewMeasurementFields() *MeasurementFields {
	fields := make(map[string]*Field)
	mf := &MeasurementFields{}
	mf.fields.Store(fields)
	return mf
}

func (m *MeasurementFields) FieldKeys() []string {
	fields := m.fields.Load().(map[string]*Field)
	a := make([]string, 0, len(fields))
	for key := range fields {
		a = append(a, key)
	}
	sort.Strings(a)
	return a
}

// bytes estimates the memory footprint of this MeasurementFields, in bytes.
func (m *MeasurementFields) bytes() int {
	var b int
	b += 24 // mu RWMutex is 24 bytes
	fields := m.fields.Load().(map[string]*Field)
	b += int(unsafe.Sizeof(fields))
	for k, v := range fields {
		b += int(unsafe.Sizeof(k)) + len(k)
		b += int(unsafe.Sizeof(v)+unsafe.Sizeof(*v)) + len(v.Name)
	}
	return b
}

// CreateFieldIfNotExists creates a new field with an autoincrementing ID.
// Returns an error if 255 fields have already been created on the measurement or
// the fields already exists with a different type.
func (m *MeasurementFields) CreateFieldIfNotExists(name []byte, typ influxql.DataType) error {
	fields := m.fields.Load().(map[string]*Field)

	// Ignore if the field already exists.
	if f := fields[string(name)]; f != nil {
		if f.Type != typ {
			return ErrFieldTypeConflict
		}
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	fields = m.fields.Load().(map[string]*Field)
	// Re-check field and type under write lock.
	if f := fields[string(name)]; f != nil {
		if f.Type != typ {
			return ErrFieldTypeConflict
		}
		return nil
	}

	fieldsUpdate := make(map[string]*Field, len(fields)+1)
	for k, v := range fields {
		fieldsUpdate[k] = v
	}
	// Create and append a new field.
	f := &Field{
		ID:   uint8(len(fields) + 1),
		Name: string(name),
		Type: typ,
	}
	fieldsUpdate[string(name)] = f
	m.fields.Store(fieldsUpdate)

	return nil
}

func (m *MeasurementFields) FieldN() int {
	n := len(m.fields.Load().(map[string]*Field))
	return n
}

// Field returns the field for name, or nil if there is no field for name.
func (m *MeasurementFields) Field(name string) *Field {
	f := m.fields.Load().(map[string]*Field)[name]
	return f
}

func (m *MeasurementFields) HasField(name string) bool {
	if m == nil {
		return false
	}
	f := m.fields.Load().(map[string]*Field)[name]
	return f != nil
}

// FieldBytes returns the field for name, or nil if there is no field for name.
// FieldBytes should be preferred to Field when the caller has a []byte, because
// it avoids a string allocation, which can't be avoided if the caller converts
// the []byte to a string and calls Field.
func (m *MeasurementFields) FieldBytes(name []byte) *Field {
	f := m.fields.Load().(map[string]*Field)[string(name)]
	return f
}

// FieldSet returns the set of fields and their types for the measurement.
func (m *MeasurementFields) FieldSet() map[string]influxql.DataType {
	fields := m.fields.Load().(map[string]*Field)
	fieldTypes := make(map[string]influxql.DataType)
	for name, f := range fields {
		fieldTypes[name] = f.Type
	}
	return fieldTypes
}

func (m *MeasurementFields) ForEachField(fn func(name string, typ influxql.DataType) bool) {
	fields := m.fields.Load().(map[string]*Field)
	for name, f := range fields {
		if !fn(name, f.Type) {
			return
		}
	}
}

type FieldChanges []*FieldChange

func MeasurementsToFieldChangeDeletions(measurements []string) FieldChanges {
	fcs := make([]*FieldChange, 0, len(measurements))
	for _, m := range measurements {
		fcs = append(fcs, &FieldChange{
			FieldCreate: FieldCreate{
				Measurement: []byte(m),
				Field:       nil,
			},
			ChangeType: DeleteMeasurement,
		})
	}
	return fcs
}

// MeasurementFieldSet represents a collection of fields by measurement.
// This safe for concurrent use.
type MeasurementFieldSet struct {
	mu     sync.RWMutex
	fields map[string]*MeasurementFields
	// path is the location to persist field sets
	path      string
	changeMgr *measurementFieldSetChangeMgr
}

// NewMeasurementFieldSet returns a new instance of MeasurementFieldSet.
func NewMeasurementFieldSet(path string, logger *zap.Logger) (*MeasurementFieldSet, error) {
	const MaxCombinedWrites = 100
	fs := &MeasurementFieldSet{
		fields: make(map[string]*MeasurementFields),
		path:   path,
	}
	if nil == logger {
		logger = zap.NewNop()
	}
	fs.SetMeasurementFieldSetWriter(MaxCombinedWrites, logger)
	// If there is a load error, return the error and an empty set so
	// it can be rebuild manually.
	return fs, fs.load()
}

func (fs *MeasurementFieldSet) Close() error {
	if fs != nil && fs.changeMgr != nil {
		fs.changeMgr.Close()
		// If there is a change log file, save the in-memory version
		if _, err := os.Stat(fs.changeMgr.changeFilePath); err == nil {
			return fs.WriteToFile()
		} else if os.IsNotExist(err) {
			return nil
		} else {
			return fmt.Errorf("cannot get file information for %s: %w", fs.changeMgr.changeFilePath, err)
		}
	}
	return nil
}

func (fs *MeasurementFieldSet) ChangesPath() string {
	return fs.changeMgr.changeFilePath
}

// Bytes estimates the memory footprint of this MeasurementFieldSet, in bytes.
func (fs *MeasurementFieldSet) Bytes() int {
	var b int
	fs.mu.RLock()
	b += 24 // mu RWMutex is 24 bytes
	for k, v := range fs.fields {
		b += int(unsafe.Sizeof(k)) + len(k)
		b += int(unsafe.Sizeof(v)) + v.bytes()
	}
	b += int(unsafe.Sizeof(fs.fields))
	b += int(unsafe.Sizeof(fs.path)) + len(fs.path)
	fs.mu.RUnlock()
	return b
}

// MeasurementNames returns the names of all of the measurements in the field set in
// lexographical order.
func (fs *MeasurementFieldSet) MeasurementNames() []string {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	names := make([]string, 0, len(fs.fields))
	for name := range fs.fields {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Fields returns fields for a measurement by name.
func (fs *MeasurementFieldSet) Fields(name []byte) *MeasurementFields {
	fs.mu.RLock()
	mf := fs.fields[string(name)]
	fs.mu.RUnlock()
	return mf
}

// FieldsByString returns fields for a measurement by name.
func (fs *MeasurementFieldSet) FieldsByString(name string) *MeasurementFields {
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
	fs.deleteNoLock(name)
	fs.mu.Unlock()
}

// DeleteWithLock executes fn and removes a field set from a measurement under lock.
func (fs *MeasurementFieldSet) DeleteWithLock(name string, fn func() error) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if err := fn(); err != nil {
		return err
	}

	fs.deleteNoLock(name)
	return nil
}

// deleteNoLock removes a field set for a measurement
func (fs *MeasurementFieldSet) deleteNoLock(name string) {
	delete(fs.fields, name)
}

func (fs *MeasurementFieldSet) IsEmpty() bool {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return len(fs.fields) == 0
}

type errorChannel chan<- error

type writeRequest struct {
	errorReturn chan<- error
	changes     FieldChanges
}

type measurementFieldSetChangeMgr struct {
	mu             sync.Mutex
	wg             sync.WaitGroup
	writeRequests  chan writeRequest
	changeFilePath string
	logger         *zap.Logger
	changeFileSize int64
}

// SetMeasurementFieldSetWriter - initialize the queue for write requests
// and start the background write process
func (fs *MeasurementFieldSet) SetMeasurementFieldSetWriter(queueLength int, logger *zap.Logger) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.changeMgr = &measurementFieldSetChangeMgr{
		writeRequests:  make(chan writeRequest, queueLength),
		changeFilePath: filepath.Join(filepath.Dir(fs.path), FieldsChangeFile),
		logger:         logger,
		changeFileSize: int64(0),
	}
	fs.changeMgr.wg.Add(1)
	go fs.changeMgr.SaveWriter()
}

func (fscm *measurementFieldSetChangeMgr) Close() {
	if fscm != nil {
		close(fscm.writeRequests)
		fscm.wg.Wait()
	}
}

func (fs *MeasurementFieldSet) Save(changes FieldChanges) error {
	return fs.changeMgr.RequestSave(changes)
}

func (fscm *measurementFieldSetChangeMgr) RequestSave(changes FieldChanges) error {
	done := make(chan error)
	fscm.writeRequests <- writeRequest{errorReturn: done, changes: changes}
	return <-done
}

func (fscm *measurementFieldSetChangeMgr) SaveWriter() {
	defer fscm.wg.Done()
	// Block until someone modifies the MeasurementFieldSet, and
	// it needs to be written to disk. Exit when the channel is closed
	for wr, ok := <-fscm.writeRequests; ok; wr, ok = <-fscm.writeRequests {
		fscm.appendToChangesFile(wr)
	}
}

// WriteToFile: Write the new index to a temp file and rename when it's sync'd
// This locks the MeasurementFieldSet during the marshaling, the write, and the rename.
func (fs *MeasurementFieldSet) WriteToFile() error {
	path := fs.path + ".tmp"

	// Open the temp file
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL|os.O_SYNC, 0666)
	if err != nil {
		return fmt.Errorf("failed opening %s: %w", fs.path, err)
	}
	// Ensure temp file is cleaned up
	defer func() {
		if e := os.RemoveAll(path); err == nil && e != nil {
			err = fmt.Errorf("failed removing temporary file %s: %w", path, e)
		}
		if e := os.RemoveAll(fs.changeMgr.changeFilePath); err == nil && e != nil {
			err = fmt.Errorf("failed removing saved field changes - %s: %w", fs.changeMgr.changeFilePath, e)
		}
	}()
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	isEmpty, err := func() (isEmpty bool, err error) {
		// ensure temp file closed before rename (for Windows)
		defer func() {
			if e := fd.Close(); err == nil && e != nil {
				err = fmt.Errorf("closing %s: %w", path, e)
			}
		}()
		if _, err = fd.Write(fieldsIndexMagicNumber); err != nil {
			return true, fmt.Errorf("failed writing magic number for %s: %w", path, err)
		}

		// Lock, copy, and marshal the in-memory index
		b, err := fs.marshalMeasurementFieldSetNoLock()
		if err != nil {
			return true, fmt.Errorf("failed marshaling fields for %s: %w", fs.path, err)
		}
		if b == nil {
			// No fields, file removed, all done
			return true, nil
		}
		if _, err := fd.Write(b); err != nil {
			return true, fmt.Errorf("failed saving fields to %s: %w", path, err)
		}
		return false, nil
	}()
	if err != nil {
		return err
	} else if isEmpty {
		// remove empty file
		if err = os.RemoveAll(fs.path); err != nil {
			return fmt.Errorf("cannot remove %s: %w", fs.path, err)
		} else {
			return nil
		}
	}

	return fs.renameFileNoLock(path)
}

// appendToChangesFile: Write a change file for fields.idx
// Only called in one Go proc, so does not need locking.
func (fscm *measurementFieldSetChangeMgr) appendToChangesFile(first writeRequest) {
	var err error = nil
	// Put the errorChannel on which we blocked into a slice to allow more invocations
	// to share the return code from the file write
	errorChannels := []errorChannel{first.errorReturn}
	changes := []FieldChanges{first.changes}
	// On return, send the error to every go proc that send changes
	defer func() {
		for _, c := range errorChannels {
			c <- err
			close(c)
		}
	}()
	log, end := logger.NewOperation(context.TODO(), fscm.logger, "saving field index changes", "MeasurementFieldSet")
	defer end()
	// Do some blocking IO operations before marshalling the changes,
	// to allow other changes to be queued up and be captured in one
	// write operation, in case we are under heavy field creation load
	fscm.mu.Lock()
	defer fscm.mu.Unlock()
	fd, err := os.OpenFile(fscm.changeFilePath, os.O_CREATE|os.O_APPEND|os.O_SYNC|os.O_WRONLY, 0666)
	if err != nil {
		err = fmt.Errorf("opening %s: %w", fscm.changeFilePath, err)
		log.Error("failed", zap.Error(err))
		return
	}

	// ensure file closed
	defer errors2.Capture(&err, func() error {
		if e := fd.Close(); e != nil {
			e = fmt.Errorf("closing %s: %w", fd.Name(), e)
			log.Error("failed", zap.Error(e))
			return e
		} else {
			return nil
		}
	})()

	var fi os.FileInfo
	if fi, err = fd.Stat(); err != nil {
		err = fmt.Errorf("unable to get size of %s: %w", fd.Name(), err)
		log.Error("failed", zap.Error(err))
		return
	} else if fi.Size() > fscm.changeFileSize {
		// If we had a partial write last time, truncate the file to remove it.
		if err = fd.Truncate(fscm.changeFileSize); err != nil {
			err = fmt.Errorf("cannot truncate %s to last known good size of %d after incomplete write: %w", fd.Name(), fscm.changeFileSize, err)
			log.Error("failed", zap.Error(err))
			return
		}
	}

	// Read all the pending field and measurement write or delete
	// requests
	for {
		select {
		case wr, ok := <-fscm.writeRequests:
			if ok {
				changes = append(changes, wr.changes)
				errorChannels = append(errorChannels, wr.errorReturn)
				continue
			}
		default:
		}
		break
	}
	// marshal the slice of slices of field changes in size-prefixed protobuf
	var b []byte
	b, err = marshalFieldChanges(changes...)
	if err != nil {
		err = fmt.Errorf("error marshaling changes for %s: %w", fd.Name(), err)
		log.Error("failed", zap.Error(err))
		return
	}

	if _, err = fd.Write(b); err != nil {
		err = fmt.Errorf("failed writing to %s: %w", fd.Name(), err)
		log.Error("failed", zap.Error(err))
		return
	} else if fi, err = fd.Stat(); err != nil {
		err = fmt.Errorf("unable to get final size of %s after appendation: %w", fd.Name(), err)
		log.Error("failed", zap.Error(err))
		return
	} else {
		fscm.changeFileSize = fi.Size()
	}
}

func readSizePlusBuffer(r io.Reader, b []byte) ([]byte, error) {
	var numBuf [bytesInInt64]byte

	if _, err := r.Read(numBuf[:]); err != nil {
		return nil, err
	}
	size := int(binary.LittleEndian.Uint64(numBuf[:]))
	if cap(b) < size {
		b = make([]byte, size)
	}
	_, err := io.ReadAtLeast(r, b, size)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (fs *MeasurementFieldSet) renameFileNoLock(path string) error {
	if err := file.RenameFile(path, fs.path); err != nil {
		return fmt.Errorf("cannot rename %s to %s: %w", path, fs.path, err)
	}

	dir := filepath.Dir(fs.path)
	if err := file.SyncDir(dir); err != nil {
		return fmt.Errorf("cannot sync directory %s: %w", dir, err)
	}

	return nil
}

// marshalMeasurementFieldSetNoLock: remove the fields.idx file if no fields
// otherwise, copy the in-memory version into a protobuf to write to
// disk
func (fs *MeasurementFieldSet) marshalMeasurementFieldSetNoLock() (marshalled []byte, err error) {
	if len(fs.fields) == 0 {
		// If no fields left, remove the fields index file
		return nil, nil
	}

	pb := internal.MeasurementFieldSet{
		Measurements: make([]*internal.MeasurementFields, 0, len(fs.fields)),
	}

	for name, mf := range fs.fields {
		imf := &internal.MeasurementFields{
			Name:   []byte(name),
			Fields: make([]*internal.Field, 0, mf.FieldN()),
		}

		mf.ForEachField(func(field string, typ influxql.DataType) bool {
			imf.Fields = append(imf.Fields, &internal.Field{Name: []byte(field), Type: int32(typ)})
			return true
		})

		pb.Measurements = append(pb.Measurements, imf)
	}
	b, err := proto.Marshal(&pb)
	if err != nil {
		return nil, err
	} else {
		return b, nil
	}
}

func marshalFieldChanges(changeSet ...FieldChanges) ([]byte, error) {
	fcs := internal.FieldChangeSet{
		Changes: nil,
	}
	for _, fc := range changeSet {
		for _, f := range fc {
			mfc := &internal.MeasurementFieldChange{
				Measurement: f.Measurement,
				Change:      internal.ChangeType(f.ChangeType),
			}
			if f.Field != nil {
				mfc.Field = &internal.Field{
					Name: []byte(f.Field.Name),
					Type: int32(f.Field.Type),
				}
				fcs.Changes = append(fcs.Changes, mfc)
			}
		}
	}
	mo := proto.MarshalOptions{}
	var numBuf [bytesInInt64]byte

	b, err := mo.MarshalAppend(numBuf[:], &fcs)
	binary.LittleEndian.PutUint64(b[0:bytesInInt64], uint64(len(b)-bytesInInt64))

	if err != nil {
		fields := make([]string, 0, len(fcs.Changes))
		for _, fc := range changeSet {
			for _, f := range fc {
				fields = append(fields, fmt.Sprintf("%q.%q", f.Measurement, f.Field.Name))
			}
		}
		return nil, fmt.Errorf("failed marshaling new fields - %s: %w", strings.Join(fields, ", "), err)
	}
	return b, nil
}

func (fs *MeasurementFieldSet) load() (rErr error) {
	err := func() error {
		fs.mu.Lock()
		defer fs.mu.Unlock()

		pb, err := fs.loadParseFieldIndexPB()
		if err != nil {
			return err
		}
		fs.fields = make(map[string]*MeasurementFields, len(pb.GetMeasurements()))
		for _, measurement := range pb.GetMeasurements() {
			fields := make(map[string]*Field, len(measurement.GetFields()))
			for _, field := range measurement.GetFields() {
				fields[string(field.GetName())] = &Field{Name: string(field.GetName()), Type: influxql.DataType(field.GetType())}
			}
			set := &MeasurementFields{}
			set.fields.Store(fields)
			fs.fields[string(measurement.GetName())] = set
		}
		return nil
	}()

	if err != nil {
		return fmt.Errorf("failed loading field indices: %w", err)
	}
	return fs.ApplyChanges()
}

func (fs *MeasurementFieldSet) loadParseFieldIndexPB() (pb *internal.MeasurementFieldSet, rErr error) {
	pb = &internal.MeasurementFieldSet{}

	fd, err := os.Open(fs.path)
	if os.IsNotExist(err) {
		return pb, nil
	} else if err != nil {
		err = fmt.Errorf("failed opening %s: %w", fs.path, err)
		return nil, err
	}

	defer errors2.Capture(&rErr, func() error {
		if e := fd.Close(); e != nil {
			return fmt.Errorf("failed closing %s: %w", fd.Name(), e)
		} else {
			return nil
		}
	})()

	var magic [4]byte
	if _, err := fd.Read(magic[:]); err != nil {
		err = fmt.Errorf("failed reading %s: %w", fs.path, err)
		return nil, err
	}

	if !bytes.Equal(magic[:], fieldsIndexMagicNumber) {
		return nil, fmt.Errorf("%q: %w", fs.path, ErrUnknownFieldsFormat)
	}

	b, err := io.ReadAll(fd)
	if err != nil {
		err = fmt.Errorf("failed reading %s: %w", fs.path, err)
		return nil, err
	}
	if err = proto.Unmarshal(b, pb); err != nil {
		err = fmt.Errorf("failed unmarshaling %s: %w", fs.path, err)
		return nil, err
	}
	return pb, err
}

func (fscm *measurementFieldSetChangeMgr) loadAllFieldChanges(log *zap.Logger) (changes []FieldChanges, rErr error) {
	var fcs FieldChanges

	fscm.mu.Lock()
	defer fscm.mu.Unlock()
	fd, err := os.Open(fscm.changeFilePath)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		err = fmt.Errorf("failed opening %s: %w", fscm.changeFilePath, err)
		log.Error("field index file of changes", zap.Error(err))
		return nil, err
	}
	defer errors2.Capture(&rErr, func() error {
		if e := fd.Close(); e != nil {
			return fmt.Errorf("failed closing %s: %w", fd.Name(), e)
		} else {
			return nil
		}
	})()
	changesetCount := 0
	totalChanges := 0
	for fcs, err = fscm.loadFieldChangeSet(fd); err == nil; fcs, err = fscm.loadFieldChangeSet(fd) {
		totalChanges += len(fcs)
		changesetCount++
		log.Debug("loading field change set",
			zap.Int("set", changesetCount),
			zap.Int("changes", len(fcs)),
			zap.Int("total_changes", totalChanges))
		changes = append(changes, fcs)
	}
	if errors.Is(err, io.EOF) {
		return changes, nil
	} else if errors.Is(err, io.ErrUnexpectedEOF) {
		log.Warn("last entry was an incomplete write", zap.Error(err))
		return changes, nil
	} else {
		log.Error("field index file of changes", zap.Error(err))
		return nil, err
	}
}

func (fscm *measurementFieldSetChangeMgr) loadFieldChangeSet(r io.Reader) (FieldChanges, error) {
	var pb internal.FieldChangeSet

	b, err := readSizePlusBuffer(r, nil)
	if err != nil {
		return nil, fmt.Errorf("failed reading %s: %w", fscm.changeFilePath, err)
	}
	if err := proto.Unmarshal(b, &pb); err != nil {
		return nil, fmt.Errorf("failed unmarshalling %s: %w", fscm.changeFilePath, err)
	}

	fcs := make([]*FieldChange, 0, len(pb.Changes))

	for _, fc := range pb.Changes {
		fcs = append(fcs, &FieldChange{
			FieldCreate: FieldCreate{
				Measurement: fc.Measurement,
				Field: &Field{
					ID:   0,
					Name: string(fc.Field.Name),
					Type: influxql.DataType(fc.Field.Type),
				},
			},
			ChangeType: ChangeType(fc.Change),
		})
	}
	return fcs, nil
}

func (fs *MeasurementFieldSet) ApplyChanges() error {
	log, end := logger.NewOperation(context.TODO(), fs.changeMgr.logger, "loading changes", "field indices", zap.String("path", fs.changeMgr.changeFilePath))
	defer end()
	changes, err := fs.changeMgr.loadAllFieldChanges(log)
	if err != nil {
		return err
	}
	if len(changes) <= 0 {
		return os.RemoveAll(fs.changeMgr.changeFilePath)
	}

	for _, fcs := range changes {
		for _, fc := range fcs {
			if fc.ChangeType == DeleteMeasurement {
				fs.Delete(string(fc.Measurement))
			} else {
				mf := fs.CreateFieldsIfNotExists(fc.Measurement)
				if err := mf.CreateFieldIfNotExists([]byte(fc.Field.Name), fc.Field.Type); err != nil {
					err = fmt.Errorf("failed creating %q.%q: %w", fc.Measurement, fc.Field.Name, err)
					log.Error("field creation", zap.Error(err))
					return err
				}
			}
		}
	}
	return fs.WriteToFile()
}

// Field represents a series field. All of the fields must be hashable.
type Field struct {
	ID   uint8             `json:"id,omitempty"`
	Name string            `json:"name,omitempty"`
	Type influxql.DataType `json:"type,omitempty"`
}

type FieldChange struct {
	FieldCreate
	ChangeType ChangeType
}

type ChangeType int

const (
	AddMeasurementField = ChangeType(internal.ChangeType_AddMeasurementField)
	DeleteMeasurement   = ChangeType(internal.ChangeType_DeleteMeasurement)
)

// NewFieldKeysIterator returns an iterator that can be iterated over to
// retrieve field keys.
func NewFieldKeysIterator(sh *Shard, opt query.IteratorOptions) (query.Iterator, error) {
	itr := &fieldKeysIterator{shard: sh}

	index, err := sh.Index()
	if err != nil {
		return nil, err
	}

	// Retrieve measurements from shard. Filter if condition specified.
	//
	// FGA is currently not supported when retrieving field keys.
	indexSet := IndexSet{Indexes: []Index{index}, SeriesFile: sh.sfile}
	names, err := indexSet.MeasurementNamesByExpr(query.OpenAuthorizer, opt.Condition)
	if err != nil {
		return nil, err
	}
	itr.names = names

	return itr, nil
}

// fieldKeysIterator iterates over measurements and gets field keys from each measurement.
type fieldKeysIterator struct {
	shard *Shard
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
			mf := itr.shard.MeasurementFields(itr.buf.name)
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
		index, err := sh.Index()
		if err != nil {
			return nil, err
		}

		indexSet := IndexSet{Indexes: []Index{index}, SeriesFile: sh.sfile}
		var keys [][]byte
		if err := indexSet.ForEachMeasurementTagKey(name, func(key []byte) error {
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
	index, err := sh.Index()
	if err != nil {
		return nil, err
	}

	indexSet := IndexSet{Indexes: []Index{index}, SeriesFile: sh.sfile}
	itr := &measurementKeysIterator{fn: fn}
	names, err := indexSet.MeasurementNamesByExpr(opt.Authorizer, opt.Condition)
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
