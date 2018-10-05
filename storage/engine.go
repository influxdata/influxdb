package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/influxdata/influxql"
	"github.com/influxdata/platform/models"
	"github.com/influxdata/platform/tsdb"
	"github.com/influxdata/platform/tsdb/index/tsi1"
	"github.com/influxdata/platform/tsdb/tsm1"
	"go.uber.org/zap"
)

// Static objects to prevent small allocs.
var timeBytes = []byte("time")

// ErrEngineClosed is returned when a caller attempts to use the engine while
// it's closed.
var ErrEngineClosed = errors.New("engine is closed")

type Engine struct {
	config Config
	path   string

	mu     sync.RWMutex
	open   bool
	index  *tsi1.Index
	sfile  *tsdb.SeriesFile
	engine *tsm1.Engine

	logger *zap.Logger
}

// Option provides a set
type Option func(*Engine)

// WithTSMFilenameFormatter sets a function on the underlying tsm1.Engine to specify
// how TSM files are named.
var WithTSMFilenameFormatter = func(fn tsm1.FormatFileNameFunc) Option {
	return func(e *Engine) {
		e.engine.WithFormatFileNameFunc(fn)
	}
}

// NewEngine initialises a new storage engine, including a series file, index and
// TSM engine.
func NewEngine(path string, c Config, options ...Option) *Engine {
	e := &Engine{
		path:   path,
		sfile:  tsdb.NewSeriesFile(filepath.Join(path, tsdb.SeriesFileDirectory)),
		logger: zap.NewNop(),
	}

	// Initialise index.
	index := tsi1.NewIndex(e.sfile, "remove me", c.Index,
		tsi1.WithPath(filepath.Join(path, tsi1.DefaultIndexDirectoryName)),
	)
	e.index = index

	// Initialise Engine
	// TODO(edd): should just be able to use the config values for data/wal.
	engine := tsm1.NewEngine(0, tsdb.Index(e.index), filepath.Join(path, "data"), filepath.Join(path, "wal"), e.sfile, c.EngineOptions)

	// TODO(edd): Once the tsdb.Engine abstraction is gone, this won't be needed.
	e.engine = engine.(*tsm1.Engine)

	// Apply options.
	for _, option := range options {
		option(e)
	}
	return e
}

// WithLogger sets the logger on the Store. It must be called before Open.
func (e *Engine) WithLogger(log *zap.Logger) {
	e.logger = log.With(zap.String("service", "storage-engine"))
	e.sfile.WithLogger(e.logger)
	e.index.WithLogger(e.logger)
	e.engine.WithLogger(e.logger)
}

// Open opens the store and all underlying resources. It returns an error if
// any of the underlying systems fail to open.
func (e *Engine) Open() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.open {
		return nil // no-op
	}

	if err := e.sfile.Open(); err != nil {
		return err
	}

	if err := e.index.Open(); err != nil {
		return err
	}

	if err := e.engine.Open(); err != nil {
		return err
	}
	e.engine.SetCompactionsEnabled(true) // TODO(edd):is this needed?
	e.open = true
	return nil
}

// Close closes the store and all underlying resources. It returns an error if
// any of the underlying systems fail to close.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.open {
		return nil // no-op
	}
	e.open = false
	if err := e.sfile.Close(); err != nil {
		return err
	}

	if err := e.index.Close(); err != nil {
		return err
	}

	return e.engine.Close()
}

func (e *Engine) CreateSeriesCursor(ctx context.Context, req SeriesCursorRequest, cond influxql.Expr) (SeriesCursor, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if !e.open {
		return nil, ErrEngineClosed
	}
	// TODO(edd): remove IndexSet
	return newSeriesCursor(req, tsdb.IndexSet{Indexes: []tsdb.Index{e.index}, SeriesFile: e.sfile}, cond)
}

func (e *Engine) CreateCursorIterator(ctx context.Context) (tsdb.CursorIterator, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if !e.open {
		return nil, ErrEngineClosed
	}
	return e.engine.CreateCursorIterator(ctx)
}

// WritePoints writes the provided points to the engine.
//
// The Engine expects all points to have been correctly validated by the caller.
// WritePoints will however determine if there are any field type conflicts, and
// return an appropriate error in that case.
func (e *Engine) WritePoints(points []models.Point) error {
	collection := tsdb.NewSeriesCollection(points)

	j := 0
	for iter := collection.Iterator(); iter.Next(); {
		tags := iter.Tags()

		if tags.Len() > 0 && bytes.Equal(tags[0].Key, tsdb.FieldKeyTagKeyBytes) && bytes.Equal(tags[0].Value, timeBytes) {
			// Field key "time" is invalid
			if collection.Reason == "" {
				collection.Reason = fmt.Sprintf("invalid field key: input field %q is invalid", timeBytes)
			}
			collection.Dropped++
			collection.DroppedKeys = append(collection.DroppedKeys, iter.Key())
		}

		// Filter out any tags with key equal to "time": they are invalid.
		if tags.Get(timeBytes) != nil {
			if collection.Reason == "" {
				collection.Reason = fmt.Sprintf("invalid tag key: input tag %q on measurement %q is invalid", timeBytes, iter.Name())
			}
			collection.Dropped++
			collection.DroppedKeys = append(collection.DroppedKeys, iter.Key())
			continue
		}

		// Drop any series with invalid unicode characters in the key.
		if e.config.ValidateKeys && !models.ValidKeyTokens(string(iter.Name()), tags) {
			if collection.Reason == "" {
				collection.Reason = fmt.Sprintf("key contains invalid unicode: %q", iter.Key())
			}
			collection.Dropped++
			collection.DroppedKeys = append(collection.DroppedKeys, iter.Key())
			continue
		}

		collection.Copy(j, iter.Index())
		j++
	}
	collection.Truncate(j)

	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.open {
		return ErrEngineClosed
	}

	// Add new series to the index and series file. Check for partial writes.
	if err := e.index.CreateSeriesListIfNotExists(collection); err != nil {
		// ignore PartialWriteErrors. The collection captures it.
		// TODO(edd/jeff): should we just remove PartialWriteError from the index then?
		if _, ok := err.(tsdb.PartialWriteError); !ok {
			return err
		}
	}

	// Write the points to the cache and WAL.
	if err := e.engine.WritePoints(collection.Points); err != nil {
		return err
	}
	return collection.PartialWriteError()
}

// DeleteSeriesRangeWithPredicate deletes all series data iterated over if fn returns
// true for that series.
func (e *Engine) DeleteSeriesRangeWithPredicate(itr tsdb.SeriesIterator, fn func([]byte, models.Tags) (int64, int64, bool)) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if !e.open {
		return ErrEngineClosed
	}
	return e.engine.DeleteSeriesRangeWithPredicate(itr, fn)
}

// SeriesCardinality returns the number of series in the engine.
func (e *Engine) SeriesCardinality() int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if !e.open {
		return 0
	}
	return e.index.SeriesN()
}

// Path returns the path of the engine's base directory.
func (e *Engine) Path() string {
	return e.path
}

// ApplyFnToSeriesIDSet allows the caller to apply fn to the SeriesIDSet held
// within the engine's index.
func (e *Engine) ApplyFnToSeriesIDSet(fn func(*tsdb.SeriesIDSet)) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if !e.open {
		return
	}
	fn(e.index.SeriesIDSet())
}
