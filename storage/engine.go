package storage

import (
	"path/filepath"

	"github.com/influxdata/platform/tsdb"
	"github.com/influxdata/platform/tsdb/index/tsi1"
	"github.com/influxdata/platform/tsdb/tsm1"
	"go.uber.org/zap"
)

type Engine struct {
	config Config

	index  *tsi1.Index
	sfile  *tsdb.SeriesFile
	engine *tsm1.Engine

	logger *zap.Logger
}

func NewEngine(path string, c Config) *Engine {
	e := &Engine{
		sfile:  tsdb.NewSeriesFile(filepath.Join(path, tsdb.SeriesFileDirectory)),
		logger: zap.NewNop(),
	}

	// Initialise index.
	index := tsi1.NewIndex(e.sfile, "remove me", c.Index,
		tsi1.WithPath(filepath.Join(path, tsi1.DefaultIndexDirectoryName)),
	)
	e.index = index

	// Initialise Engine
	engine := tsm1.NewEngine(0, tsdb.Index(e.index), filepath.Join(path, "data"), "remove-me-wal", e.sfile, c.EngineOptions)
	// TODO(edd): Once the tsdb.Engine abstraction is gone, this won't be needed.
	e.engine = engine.(*tsm1.Engine)

	return e
}

// WithLogger sets the logger on the Store. It must be called before Open.
func (e *Engine) WithLogger(log *zap.Logger) {
	e.logger = log.With(zap.String("service", "storage"))
	e.sfile.WithLogger(e.logger)
	e.index.WithLogger(e.logger)
	e.engine.WithLogger(e.logger)
}

// Open opens the store and all underlying resources. It returns an error if
// any of the underlying systems fail to open.
func (e *Engine) Open() error {
	return nil
}

// Close closes the store and all underlying resources. It returns an error if
// any of the underlying systems fail to close.
func (e *Engine) Close() error {
	return nil
}
