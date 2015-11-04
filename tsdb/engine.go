package tsdb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/models"
)

var (
	// ErrFormatNotFound is returned when no format can be determined from a path.
	ErrFormatNotFound = errors.New("format not found")
)

// Engine represents a swappable storage engine for the shard.
type Engine interface {
	Open() error
	Close() error

	SetLogOutput(io.Writer)
	LoadMetadataIndex(shard *Shard, index *DatabaseIndex, measurementFields map[string]*MeasurementFields) error

	Begin(writable bool) (Tx, error)
	WritePoints(points []models.Point, measurementFieldsToSave map[string]*MeasurementFields, seriesToCreate []*SeriesCreate) error
	DeleteSeries(keys []string) error
	DeleteMeasurement(name string, seriesKeys []string) error
	SeriesCount() (n int, err error)

	// PerformMaintenance will get called periodically by the store
	PerformMaintenance()

	// Format will return the format for the engine
	Format() EngineFormat

	io.WriterTo

	Backup(w io.Writer, basePath string, since time.Time) error
}

type EngineFormat int

const (
	TSM1Format EngineFormat = 2
)

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

// RegisteredEngines returns the slice of currently registered engines.
func RegisteredEngines() []string {
	a := make([]string, 0, len(newEngineFuncs))
	for k, _ := range newEngineFuncs {
		a = append(a, k)
	}
	sort.Strings(a)
	return a
}

// NewEngine returns an instance of an engine based on its format.
// If the path does not exist then the DefaultFormat is used.
func NewEngine(path string, walPath string, options EngineOptions) (Engine, error) {
	// Create a new engine
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return newEngineFuncs[options.EngineVersion](path, walPath, options), nil
	}

	// If it's a dir then it's a tsm1 engine
	var format string
	if fi, err := os.Stat(path); err != nil {
		return nil, err
	} else if !fi.Mode().IsDir() {
		return nil, errors.New("unknown engine type")
	} else {
		format = "tsm1"
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

	Size() int64
	Commit() error
	Rollback() error

	Cursor(series string, fields []string, dec *FieldCodec, ascending bool) Cursor
}

func newTxIterator(tx Tx, sh *Shard, opt influxql.IteratorOptions, dimensions map[string]struct{}) (influxql.Iterator, error) {
	// If there's no expression then it's just an auxilary field selection.
	if opt.Expr == nil {
		return newTxVarRefIterator(tx, sh, opt, dimensions)
	}

	// If raw data is being read then read it directly.
	// Otherwise wrap it in a call iterator.
	switch expr := opt.Expr.(type) {
	case *influxql.VarRef:
		return newTxVarRefIterator(tx, sh, opt, dimensions)
	case *influxql.Call:
		refOpt := opt
		refOpt.Expr = expr.Args[0].(*influxql.VarRef)
		input, err := newTxVarRefIterator(tx, sh, refOpt, dimensions)
		if err != nil {
			return nil, err
		}
		return influxql.NewCallIterator(input, opt), nil
	default:
		panic(fmt.Sprintf("unsupported tx iterator expr: %T", expr))
	}
}

// newTxVarRefIterator creates an tx iterator for a variable reference.
func newTxVarRefIterator(tx Tx, sh *Shard, opt influxql.IteratorOptions, dimensions map[string]struct{}) (influxql.Iterator, error) {
	ref, _ := opt.Expr.(*influxql.VarRef)

	var itrs []influxql.Iterator
	if err := func() error {
		mms := Measurements(sh.index.MeasurementsByName(influxql.Sources(opt.Sources).Names()))
		for _, mm := range mms {
			// Determine tagsets for this measurement based on dimensions and filters.
			tagSets, err := mm.TagSets(opt.Dimensions, opt.Condition)
			if err != nil {
				return err
			}

			// FIXME(benbjohnson): Calculate tag sets and apply SLIMIT/SOFFSET.
			// tagSets = m.stmt.LimitTagSets(tagSets)

			for _, t := range tagSets {
				for i, seriesKey := range t.SeriesKeys {
					// Create field list for cursor.
					fields := make([]string, 0, len(opt.Aux)+1)
					if ref != nil {
						fields = append(fields, ref.Val)
					}
					fields = append(fields, opt.Aux...)

					// Create cursor.
					cur := tx.Cursor(seriesKey, fields, sh.FieldCodec(mm.Name), opt.Ascending)
					if cur == nil {
						continue
					}

					// Create options specific for this series.
					curOpt := opt
					curOpt.Condition = t.Filters[i]

					itr := NewFloatCursorIterator(mm.Name, sh.index.TagsForSeries(seriesKey), cur, curOpt)
					itrs = append(itrs, itr)
				}
			}
		}
		return nil
	}(); err != nil {
		influxql.Iterators(itrs).Close()
		return nil, err
	}

	// Merge iterators into one.
	itr := influxql.NewMergeIterator(itrs, opt)
	switch itr := itr.(type) {
	case influxql.FloatIterator:
		return &txFloatIterator{tx: tx, itr: itr}, nil
	default:
		panic(fmt.Sprintf("unsupported tx iterator input type: %T", itr))
	}
}

// txFloatIterator represents an iterator with an attached transaction.
// It is used to track the Tx so it can be closed.
type txFloatIterator struct {
	tx  Tx
	itr influxql.FloatIterator
}

// Close closes the underlying iterator and rolls back the transaction.
func (itr *txFloatIterator) Close() error {
	defer itr.tx.Rollback()
	return itr.itr.Close()
}

// Next returns the next point.
func (itr *txFloatIterator) Next() *influxql.FloatPoint { return itr.itr.Next() }

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
