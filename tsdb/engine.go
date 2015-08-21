package tsdb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/boltdb/bolt"
)

var (
	// ErrFormatNotFound is returned when no format can be determined from a path.
	ErrFormatNotFound = errors.New("format not found")
)

// DefaultEngine is the default engine used by the shard when initializing.
const DefaultEngine = "bz1"

// Engine represents a swappable storage engine for the shard.
type Engine interface {
	Open() error
	Close() error

	SetLogOutput(io.Writer)
	LoadMetadataIndex(index *DatabaseIndex, measurementFields map[string]*MeasurementFields) error

	Begin(writable bool) (Tx, error)
	WritePoints(points []Point, measurementFieldsToSave map[string]*MeasurementFields, seriesToCreate []*SeriesCreate) error
	DeleteSeries(keys []string) error
	DeleteMeasurement(name string, seriesKeys []string) error
	SeriesCount() (n int, err error)
}

// NewEngineFunc creates a new engine.
type NewEngineFunc func(path string, walPath string, options EngineOptions) Engine

// newEngineFuncs is a lookup of engine constructors by name.
var newEngineFuncs = make(map[string]NewEngineFunc)

// RegisterEngine registers a storage engine initializer by name.
func RegisterEngine(name string, fn NewEngineFunc) {
	if _, ok := newEngineFuncs[name]; ok {
		panic("engine already registered: " + name)
	}
	newEngineFuncs[name] = fn
}

// NewEngine returns an instance of an engine based on its format.
// If the path does not exist then the DefaultFormat is used.
func NewEngine(path string, walPath string, options EngineOptions) (Engine, error) {
	// Create a new engine
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return newEngineFuncs[options.EngineVersion](path, walPath, options), nil
	}

	// Only bolt-based backends are currently supported so open it and check the format.
	var format string
	if err := func() error {
		db, err := bolt.Open(path, 0666, &bolt.Options{Timeout: 1 * time.Second})
		if err != nil {
			return err
		}
		defer db.Close()

		return db.View(func(tx *bolt.Tx) error {
			// Retrieve the meta bucket.
			b := tx.Bucket([]byte("meta"))

			// If no format is specified then it must be an original b1 database.
			if b == nil {
				format = "b1"
				return nil
			}

			// Save the format.
			format = string(b.Get([]byte("format")))
			if format == "v1" {
				format = "b1"
			}
			return nil
		})
	}(); err != nil {
		return nil, err
	}

	// Lookup engine by format.
	fn := newEngineFuncs[format]
	if fn == nil {
		return nil, fmt.Errorf("invalid engine format: %q", format)
	}

	return fn(path, walPath, options), nil
}

// EngineOptions represents the options used to initialize the engine.
type EngineOptions struct {
	EngineVersion          string
	MaxWALSize             int
	WALFlushInterval       time.Duration
	WALPartitionFlushDelay time.Duration

	Config Config
}

// NewEngineOptions returns the default options.
func NewEngineOptions() EngineOptions {
	return EngineOptions{
		EngineVersion:          DefaultEngine,
		MaxWALSize:             DefaultMaxWALSize,
		WALFlushInterval:       DefaultWALFlushInterval,
		WALPartitionFlushDelay: DefaultWALPartitionFlushDelay,
		Config:                 NewConfig(),
	}
}

// Tx represents a transaction.
type Tx interface {
	io.WriterTo

	Cursor(series string) Cursor
	Size() int64
	Commit() error
	Rollback() error
}

// Cursor represents an iterator over a series.
type Cursor interface {
	Seek(seek []byte) (key, value []byte)
	Next() (key, value []byte)
}

// DedupeEntries returns slices with unique keys (the first 8 bytes).
func DedupeEntries(a [][]byte) [][]byte {
	// Convert to a map where the last slice is used.
	m := make(map[string][]byte)
	for _, b := range a {
		m[string(b[0:8])] = b
	}

	// Convert map back to a slice of byte slices.
	other := make([][]byte, 0, len(m))
	for _, v := range m {
		other = append(other, v)
	}

	// Sort entries.
	sort.Sort(ByteSlices(other))

	return other
}

type ByteSlices [][]byte

func (a ByteSlices) Len() int           { return len(a) }
func (a ByteSlices) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByteSlices) Less(i, j int) bool { return bytes.Compare(a[i], a[j]) == -1 }
