package tsdb

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/influxdata/platform/models"
	"go.uber.org/zap"
)

// SeriesFileDirectory is the name of the directory containing series files for
// a database.
const SeriesFileDirectory = "_series"

var (
	// Static objects to prevent small allocs.
	timeBytes = []byte("time")
)

// Shard represents a self-contained time series database. An inverted index of
// the measurement and tag data is kept along with the raw time series data.
// Data can be split across many shards. The query engine in TSDB is responsible
// for combining the output of many shards into a single query result.
type Shard struct {
	path string
	id   uint64

	sfile   *SeriesFile
	options EngineOptions

	mu      sync.RWMutex
	_engine Engine
	index   Index
	enabled bool

	baseLogger *zap.Logger
	logger     *zap.Logger

	EnableOnOpen bool

	// CompactionDisabled specifies the shard should not schedule compactions.
	// This option is intended for offline tooling.
	CompactionDisabled bool
}

// NewShard returns a new initialized Shard.
func NewShard(id uint64, path string, sfile *SeriesFile, opt EngineOptions) *Shard {
	logger := zap.NewNop()
	if opt.FieldValidator == nil {
		opt.FieldValidator = defaultFieldValidator{}
	}

	s := &Shard{
		id:      id,
		path:    path,
		sfile:   sfile,
		options: opt,

		logger:       logger,
		baseLogger:   logger,
		EnableOnOpen: true,
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
	// Prevent writes and queries
	s.enabled = enabled
	if s._engine != nil && !s.CompactionDisabled {
		// Disable background compactions and snapshotting
		s._engine.SetEnabled(enabled)
	}
	s.mu.Unlock()
}

// ID returns the shards ID.
func (s *Shard) ID() uint64 {
	return s.id
}

// Path returns the path set on the shard when it was created.
func (s *Shard) Path() string { return s.path }

// Open initializes and opens the shard's store.
func (s *Shard) Open() error {
	if err := func() error {
		s.mu.Lock()
		defer s.mu.Unlock()

		// Return if the shard is already open
		if s._engine != nil {
			return nil
		}

		seriesIDSet := NewSeriesIDSet()

		// Initialize underlying index.
		ipath := filepath.Join(s.path, "index")
		idx, err := NewIndex(s.id, "remove-me", ipath, seriesIDSet, s.sfile, s.options)
		if err != nil {
			return err
		}

		idx.WithLogger(s.baseLogger)

		// Open index.
		if err := idx.Open(); err != nil {
			return err
		}
		s.index = idx

		// Initialize underlying engine.
		e, err := NewEngine(s.id, idx, s.path, s.sfile, s.options)
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

		s._engine = e

		return nil
	}(); err != nil {
		s.close()
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
	return s.close()
}

// close closes the shard an removes reference to the shard from associated
// indexes, unless clean is false.
func (s *Shard) close() error {
	if s._engine == nil {
		return nil
	}

	err := s._engine.Close()
	if err == nil {
		s._engine = nil
	}

	if e := s.index.Close(); e == nil {
		s.index = nil
	}
	return err
}

// ready determines if the Shard is ready for queries or writes.
// It returns nil if ready, otherwise ErrShardClosed or ErrShardDisabled
func (s *Shard) ready() error {
	return nil // TODO(edd)remove
}

// Engine returns a reference to the currently loaded engine.
func (s *Shard) Engine() (Engine, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.engineNoLock()
}

// engineNoLock is similar to calling Engine(), but the caller must guarantee
// that they already hold an appropriate lock.
func (s *Shard) engineNoLock() (Engine, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	return s._engine, nil
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

// WritePoints will write the raw data points and any new metadata to the index in the shard.
func (s *Shard) WritePoints(points []models.Point) error {
	collection := NewSeriesCollection(points)

	j := 0
	for iter := collection.Iterator(); iter.Next(); {
		tags := iter.Tags()

		// Filter out any tags with key equal to "time": they are invalid.
		if tags.Get(timeBytes) != nil {
			if collection.Reason == "" {
				collection.Reason = fmt.Sprintf(
					"invalid tag key: input tag %q on measurement %q is invalid",
					timeBytes, iter.Name())
			}
			collection.Dropped++
			collection.DroppedKeys = append(collection.DroppedKeys, iter.Key())
			continue
		}

		// Drop any series with invalid unicode characters in the key.
		if s.options.Config.ValidateKeys && !models.ValidKeyTokens(string(iter.Name()), tags) {
			if collection.Reason == "" {
				collection.Reason = fmt.Sprintf(
					"key contains invalid unicode: %q",
					iter.Key())
			}
			collection.Dropped++
			collection.DroppedKeys = append(collection.DroppedKeys, iter.Key())
			continue
		}

		collection.Copy(j, iter.Index())
		j++
	}
	collection.Truncate(j)

	s.mu.RLock()
	defer s.mu.RUnlock()

	engine, err := s.engineNoLock()
	if err != nil {
		return err
	}

	// validate and create any new fields
	fieldsToCreate, err := s.validateSeriesAndFields(engine, collection)
	if err != nil {
		return err
	}
	if err := s.createFieldsAndMeasurements(engine, fieldsToCreate); err != nil {
		return err
	}

	// Write to the engine.
	if err := engine.WritePoints(collection.Points); err != nil {
		return fmt.Errorf("engine: %s", err)
	}

	return collection.PartialWriteError()
}

// DeleteSeriesRangeWithPredicate deletes all values from for seriesKeys between min and max (inclusive)
// for which predicate() returns true. If predicate() is nil, then all values in range are deleted.
func (s *Shard) DeleteSeriesRangeWithPredicate(itr SeriesIterator, predicate func(name []byte, tags models.Tags) (int64, int64, bool)) error {
	engine, err := s.Engine()
	if err != nil {
		return err
	}
	return engine.DeleteSeriesRangeWithPredicate(itr, predicate)
}

// SeriesN returns the unique number of series in the shard.
func (s *Shard) SeriesN() int64 {
	engine, err := s.Engine()
	if err != nil {
		return 0
	}
	return engine.SeriesN()
}

// CreateCursorIterator creates a CursorIterator for the shard.
func (s *Shard) CreateCursorIterator(ctx context.Context) (CursorIterator, error) {
	engine, err := s.Engine()
	if err != nil {
		return nil, err
	}
	return engine.CreateCursorIterator(ctx)
}
