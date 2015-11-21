package tsm1

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/influxdb/influxdb/models"
	"github.com/influxdb/influxdb/tsdb"
)

// minCompactionSegments is the number of WAL segements that must be
// closed in order for a compaction to run.  A lower value would shorten
// compaction times and memory requirements, but produce more TSM files
// with lower compression ratios.  A higher value increases compaction times
// and memory usage but produces more dense TSM files.
const minCompactionSegments = 10

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

	WAL       *WAL
	Compactor *Compactor

	RotateFileSize    uint32
	MaxFileSize       uint32
	MaxPointsPerBlock int
}

// NewDevEngine returns a new instance of Engine.
func NewDevEngine(path string, walPath string, opt tsdb.EngineOptions) tsdb.Engine {
	w := NewWAL(walPath)
	w.LoggingEnabled = opt.Config.WALLoggingEnabled

	c := &Compactor{
		Dir: path,
	}

	e := &DevEngine{
		path:   path,
		logger: log.New(os.Stderr, "[tsm1dev] ", log.LstdFlags),

		WAL:               w,
		Compactor:         c,
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
	return tsdb.TSM1DevFormat
}

// Open opens and initializes the engine.
func (e *DevEngine) Open() error {
	if err := os.MkdirAll(e.path, 0777); err != nil {
		return err
	}

	if err := e.WAL.Open(); err != nil {
		return err
	}

	go e.compact()

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
	values := map[string][]Value{}
	for _, p := range points {
		for k, v := range p.Fields() {
			key := fmt.Sprintf("%s%s%s", p.Key(), keyFieldSeparator, k)
			values[key] = append(values[key], NewValue(p.Time(), v))
		}
	}

	return e.WAL.WritePoints(values)
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

func (e *DevEngine) compact() {
	for {
		// Grab the closed segments that a no longer being written to
		segments, err := e.WAL.ClosedSegments()
		if err != nil {
			e.logger.Printf("error retrieving closed WAL segments: %v", err)
			time.Sleep(time.Second)
			continue
		}

		// NOTE: This logic is temporary.  Only compact the closed segments if we
		// have at least 10 of them.
		n := minCompactionSegments
		if len(segments) == 0 || len(segments) < n {
			time.Sleep(time.Second)
			continue
		}

		// If we have more than 10, just compact 10 to keep compactions times bounded.
		var compact []string
		if len(segments) >= n {
			compact = segments[:n]
		} else {
			compact = segments
		}

		start := time.Now()
		files, err := e.Compactor.Compact(compact)
		if err != nil {
			e.logger.Printf("error compacting WAL segments: %v", err)
		}

		// TODO: this is stubbed out but would be the place to replace files in the
		// file store with the new compacted versions.
		e.replaceFiles(files, compact)

		// TODO: if replacement succeeds, we'd update the cache with the latest checkpoint.
		// e.Cache.SetCheckpoint(...)

		e.logger.Printf("compacted %d segments into %d files in %s", len(compact), len(files), time.Since(start))
	}
}

func (e *DevEngine) replaceFiles(tsm, segments []string) {
	// TODO: this is temporary, this func should replace the files in the file store

	// The new TSM files are have a tmp extension.  First rename them.
	for _, f := range tsm {
		os.Rename(f, f[:len(f)-4])
	}

	// The segments are fully compacted, delete them.
	for _, f := range segments {
		os.RemoveAll(f)
	}
}
