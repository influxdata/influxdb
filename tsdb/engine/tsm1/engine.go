package tsm1

import (
	"io"
	"log"
	"os"
	"sync"

	"github.com/influxdb/influxdb/models"
	"github.com/influxdb/influxdb/tsdb"
)

func init() {
	tsdb.RegisterEngine("tsm1dev", NewDevEngine)
}

// Ensure Engine implements the interface.
var _ tsdb.Engine = &DevEngine{}

// Engine represents a storage engine with compressed blocks.
type DevEngine struct {
	mu sync.RWMutex

	path   string
	logger *log.Logger

	WAL *WAL

	RotateFileSize    uint32
	MaxFileSize       uint32
	MaxPointsPerBlock int
}

// NewDevEngine returns a new instance of Engine.
func NewDevEngine(path string, walPath string, opt tsdb.EngineOptions) tsdb.Engine {
	w := NewWAL(walPath)
	w.LoggingEnabled = opt.Config.WALLoggingEnabled

	e := &DevEngine{
		path:   path,
		logger: log.New(os.Stderr, "[tsm1] ", log.LstdFlags),

		WAL:               w,
		RotateFileSize:    DefaultRotateFileSize,
		MaxFileSize:       MaxDataFileSize,
		MaxPointsPerBlock: DefaultMaxPointsPerBlock,
	}

	return e
}

// Path returns the path the engine was opened with.
func (e *DevEngine) Path() string { return e.path }

// PerformMaintenance is for periodic maintenance of the store. A no-op for b1
func (e *DevEngine) PerformMaintenance() {
}

// Format returns the format type of this engine
func (e *DevEngine) Format() tsdb.EngineFormat {
	return tsdb.TSM1Format
}

// Open opens and initializes the engine.
func (e *DevEngine) Open() error {
	if err := e.WAL.Open(); err != nil {
		return err
	}

	return nil
}

// Close closes the engine.
func (e *DevEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.WAL.Close()

	return nil
}

// SetLogOutput is a no-op.
func (e *DevEngine) SetLogOutput(w io.Writer) {}

// LoadMetadataIndex loads the shard metadata into memory.
func (e *DevEngine) LoadMetadataIndex(shard *tsdb.Shard, index *tsdb.DatabaseIndex, measurementFields map[string]*tsdb.MeasurementFields) error {
	return nil
}

// WritePoints writes metadata and point data into the engine.
// Returns an error if new points are added to an existing key.
func (e *DevEngine) WritePoints(points []models.Point, measurementFieldsToSave map[string]*tsdb.MeasurementFields, seriesToCreate []*tsdb.SeriesCreate) error {
	return e.WAL.WritePoints(points)
}

// DeleteSeries deletes the series from the engine.
func (e *DevEngine) DeleteSeries(seriesKeys []string) error {
	panic("not implemented")
}

// DeleteMeasurement deletes a measurement and all related series.
func (e *DevEngine) DeleteMeasurement(name string, seriesKeys []string) error {
	panic("not implemented")
}

// SeriesCount returns the number of series buckets on the shard.
func (e *DevEngine) SeriesCount() (n int, err error) {
	return 0, nil
}

// Begin starts a new transaction on the engine.
func (e *DevEngine) Begin(writable bool) (tsdb.Tx, error) {
	panic("not implemented")
}

func (e *DevEngine) WriteTo(w io.Writer) (n int64, err error) { panic("not implemented") }
