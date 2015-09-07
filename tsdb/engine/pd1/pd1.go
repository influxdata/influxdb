package pd1

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/golang/snappy"
	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/tsdb"
)

const (
	// Format is the file format name of this engine.
	Format = "pd1"

	// FieldsFileExtension is the extension for the file that stores compressed field
	// encoding data for this db
	FieldsFileExtension = "fld"

	// SeriesFileExtension is the extension for the file that stores the compressed
	// series metadata for series in this db
	SeriesFileExtension = "srs"

	CollisionsFileExtension = "col"
)

type TimePrecision uint8

const (
	Seconds TimePrecision = iota
	Milliseconds
	Microseconds
	Nanoseconds
)

func init() {
	tsdb.RegisterEngine(Format, NewEngine)
}

const (
	// DefaultBlockSize is the default size of uncompressed points blocks.
	DefaultBlockSize = 512 * 1024 // 512KB

	DefaultMaxFileSize = 5 * 1024 * 1024 // 5MB

	DefaultMaxPointsPerBlock = 1000

	// MAP_POPULATE is for the mmap syscall. For some reason this isn't defined in golang's syscall
	MAP_POPULATE = 0x8000
)

// Ensure Engine implements the interface.
var _ tsdb.Engine = &Engine{}

// Engine represents a storage engine with compressed blocks.
type Engine struct {
	mu   sync.Mutex
	path string

	// HashSeriesField is a function that takes a series key and a field name
	// and returns a hash identifier. It's not guaranteed to be unique.
	HashSeriesField func(key string) uint64

	// Shard is an interface that can pull back field type information based on measurement name
	Shard interface {
		FieldCodec(measurementName string) *tsdb.FieldCodec
	}

	filesLock     sync.RWMutex
	files         dataFiles
	currentFileID int
	queryLock     sync.RWMutex
}

// NewEngine returns a new instance of Engine.
func NewEngine(path string, walPath string, opt tsdb.EngineOptions) tsdb.Engine {
	e := &Engine{
		path: path,

		HashSeriesField: hashSeriesField,
	}

	return e
}

// Path returns the path the engine was opened with.
func (e *Engine) Path() string { return e.path }

// Open opens and initializes the engine.
func (e *Engine) Open() error {
	if err := os.MkdirAll(e.path, 0777); err != nil {
		return err
	}

	// TODO: clean up previous series write
	// TODO: clean up previous fields write
	// TODO: clean up previous names write
	// TODO: clean up any data files that didn't get cleaned up

	files, err := filepath.Glob(filepath.Join(e.path, fmt.Sprintf("*.%s", Format)))
	if err != nil {
		return err
	}
	for _, fn := range files {
		f, err := os.OpenFile(fn, os.O_RDONLY, 0666)
		if err != nil {
			return fmt.Errorf("error opening file %s: %s", fn, err.Error())
		}
		df, err := NewDataFile(f)
		if err != nil {
			return fmt.Errorf("error opening memory map for file %s: %s", fn, err.Error())
		}
		e.files = append(e.files, df)
	}
	sort.Sort(e.files)

	return nil
}

// Close closes the engine.
func (e *Engine) Close() error {
	e.queryLock.Lock()
	defer e.queryLock.Unlock()

	for _, df := range e.files {
		_ = df.Close()
	}
	return nil
}

// SetLogOutput is a no-op.
func (e *Engine) SetLogOutput(w io.Writer) {}

// LoadMetadataIndex loads the shard metadata into memory.
func (e *Engine) LoadMetadataIndex(shard *tsdb.Shard, index *tsdb.DatabaseIndex, measurementFields map[string]*tsdb.MeasurementFields) error {
	e.Shard = shard
	// TODO: write the metadata from the WAL

	// Load measurement metadata
	fields, err := e.readFields()
	if err != nil {
		return err
	}
	for k, mf := range fields {
		m := index.CreateMeasurementIndexIfNotExists(string(k))
		for name, _ := range mf.Fields {
			m.SetFieldName(name)
		}
		mf.Codec = tsdb.NewFieldCodec(mf.Fields)
		measurementFields[m.Name] = mf
	}

	// Load series metadata
	series, err := e.readSeries()
	if err != nil {
		return err
	}

	// Load the series into the in-memory index in sorted order to ensure
	// it's always consistent for testing purposes
	a := make([]string, 0, len(series))
	for k, _ := range series {
		a = append(a, k)
	}
	sort.Strings(a)
	for _, key := range a {
		s := series[key]
		s.InitializeShards()
		index.CreateSeriesIndexIfNotExists(tsdb.MeasurementFromSeriesKey(string(key)), s)
	}

	return nil
}

// WritePoints writes metadata and point data into the engine.
// Returns an error if new points are added to an existing key.
func (e *Engine) WritePoints(points []tsdb.Point, measurementFieldsToSave map[string]*tsdb.MeasurementFields, seriesToCreate []*tsdb.SeriesCreate) error {
	// TODO: Write points to the WAL

	return e.WriteAndCompact(points, measurementFieldsToSave, seriesToCreate)
}

func (e *Engine) WriteAndCompact(points []tsdb.Point, measurementFieldsToSave map[string]*tsdb.MeasurementFields, seriesToCreate []*tsdb.SeriesCreate) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.writeNewFields(measurementFieldsToSave); err != nil {
		return err
	}
	if err := e.writeNewSeries(seriesToCreate); err != nil {
		return err
	}

	if len(points) == 0 {
		return nil
	}

	b, err := e.readCompressedFile("names")
	if err != nil {
		return err
	}
	ids := make(map[uint64]string)

	var names []string
	if b != nil {
		if err := json.Unmarshal(b, &names); err != nil {
			return err
		}

		for _, n := range names {
			ids[e.HashSeriesField(n)] = n
		}
	}

	// these are values that are newer than anything stored in the shard
	valuesByID := make(map[uint64]*valueCollection)
	// map the points to the data file they belong to if they overlap
	dataFileToValues := make(map[*dataFile]map[uint64]*valueCollection)

	// we keep track of the newest data file and if it should be
	// rewritten with new data.
	var newestDataFile *dataFile
	overwriteNewestFile := false
	if len(e.files) > 0 {
		newestDataFile = e.files[len(e.files)-1]
		overwriteNewestFile = newestDataFile.size < DefaultMaxFileSize
	}

	// compute ids of new keys and arrange for insertion
	for _, p := range points {
		for fn, val := range p.Fields() {
			n := seriesFieldKey(string(p.Key()), fn)
			id := e.HashSeriesField(n)
			if series, ok := ids[id]; !ok {
				names = append(names, n)
			} else { // possible collision?
				if n != series {
					// TODO: implement collision detection
					panic("name collision!")
				}
			}

			ids[id] = n

			vals := valuesByID[id]
			if vals == nil {
				// TODO: deal with situation where there are already files,
				//       but the user is inserting a bunch of data that predates
				//       any of them. It's ok to rewrite the first file, but
				//       only to max size. Then we should create a new one

				// points always come in time increasing order. This is
				// the first point we've seen for this key. So it might
				// need to get put into an older file instead of a new
				// one. Check and set accordingly
				var df *dataFile
				for i := len(e.files) - 1; i >= 0; i-- {
					if p.UnixNano() > e.files[i].MaxTime() {
						break
					}
					df = e.files[i]
				}
				vals = &valueCollection{}

				if df == nil || (df == newestDataFile && overwriteNewestFile) {
					// this point is newer than anything we have stored
					// or it belongs in the most recent file, which should get
					// rewritten
					valuesByID[id] = vals
				} else {
					// it overlaps with another file so mark it and it can be compacted
					dfm := dataFileToValues[df]
					if dfm == nil {
						dfm = make(map[uint64]*valueCollection)
						dataFileToValues[df] = dfm
					}

					if vc := dfm[id]; vc == nil {
						dfm[id] = vals
					} else {
						vals = vc
					}
				}
			}

			switch t := val.(type) {
			case float64:
				vals.floatValues = append(vals.floatValues, FloatValue{Time: p.Time(), Value: t})
			case int64:
				vals.intValues = append(vals.intValues, Int64Value{Time: p.Time(), Value: t})
			case bool:
				vals.boolValues = append(vals.boolValues, BoolValue{Time: p.Time(), Value: t})
			case string:
				vals.stringValues = append(vals.stringValues, StringValue{Time: p.Time(), Value: t})
			default:
				panic("unsupported type")
			}
		}
	}

	b, err = json.Marshal(names)
	if err != nil {
		return err
	}

	if err := e.replaceCompressedFile("names", b); err != nil {
		return err
	}

	// flush values by id to either a new file or rewrite the old one
	if overwriteNewestFile {
		if err := e.rewriteFile(newestDataFile, valuesByID); err != nil {
			return err
		}
	} else if err := e.rewriteFile(nil, valuesByID); err != nil {
		return err
	}

	// flush each of the old ones
	for df, vals := range dataFileToValues {
		if err := e.rewriteFile(df, vals); err != nil {
			return err
		}
	}

	return nil
}

func (e *Engine) rewriteFile(oldDF *dataFile, values map[uint64]*valueCollection) error {
	// we need the values in sorted order so that we can merge them into the
	// new file as we read the old file
	ids := make([]uint64, 0, len(values))
	for id, _ := range values {
		ids = append(ids, id)
	}

	minTime := int64(math.MaxInt64)
	maxTime := int64(math.MinInt64)

	// read header of ids to starting positions and times
	oldIDToPosition := make(map[uint64]uint32)
	if oldDF != nil {
		oldIDToPosition = oldDF.IDToPosition()
		minTime = oldDF.MinTime()
		maxTime = oldDF.MaxTime()
	}
	for _, v := range values {
		if minTime > v.MinTime().UnixNano() {
			minTime = v.MinTime().UnixNano()
		}
		if maxTime < v.MaxTime().UnixNano() {
			maxTime = v.MaxTime().UnixNano()
		}
	}

	// add any ids that are in the file that aren't getting flushed here
	for id, _ := range oldIDToPosition {
		if _, ok := values[id]; !ok {
			ids = append(ids, id)
		}
	}

	// always write in order by ID
	sort.Sort(uint64slice(ids))

	// TODO: add checkpoint file that indicates if this completed or not
	f, err := os.OpenFile(e.nextFileName(), os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}

	// write the header of the file and keep track of the current file position
	currentPosition := uint32(4)
	// series count
	if _, err := f.Write(u32tob(uint32(len(ids)))); err != nil {
		f.Close()
		return err
	}
	// empty min time and max time
	currentPosition += 16
	if _, err := f.Write([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}); err != nil {
		f.Close()
		return nil
	}

	// write the series ids and empty starting positions
	for _, id := range ids {
		if _, err := f.Write(append(u64tob(id), []byte{0x00, 0x00, 0x00, 0x00}...)); err != nil {
			f.Close()
			return err
		}
		currentPosition += 12
	}

	// now combine the old file data with the new values, keeping track of
	// their positions
	newPositions := make([]uint32, len(ids))
	buf := make([]byte, DefaultMaxPointsPerBlock*20)
	for i, id := range ids {
		// mark the position for this ID
		newPositions[i] = currentPosition

		newVals := values[id]

		// if this id is only in the file and not in the new values, just copy over from old file
		if newVals == nil {
			fpos := oldIDToPosition[id]

			// write the blocks until we hit whatever the next id is
			for {
				fid := btou64(oldDF.mmap[fpos : fpos+8])
				if fid != id {
					break
				}
				length := btou32(oldDF.mmap[fpos+8 : fpos+12])
				if _, err := f.Write(oldDF.mmap[fpos : fpos+12+length]); err != nil {
					f.Close()
					return err
				}
				fpos += (12 + length)
				currentPosition += (12 + length)

				// make sure we're not at the end of the file
				if fpos >= oldDF.size {
					break
				}
			}

			continue
		}

		// if the values are not in the file, just write the new ones
		fpos, ok := oldIDToPosition[id]
		if !ok {
			// TODO: ensure we encode only the amount in a block
			block := newVals.Encode(buf)
			if _, err := f.Write(append(u64tob(id), u32tob(uint32(len(block)))...)); err != nil {
				f.Close()
				return err
			}
			if _, err := f.Write(block); err != nil {
				f.Close()
				return err
			}
			currentPosition += uint32(12 + len(block))

			continue
		}

		// it's in the file and the new values, combine them and write out
		for {
			fid := btou64(oldDF.mmap[fpos : fpos+8])
			if fid != id {
				break
			}
			length := btou32(oldDF.mmap[fpos+8 : fpos+12])
			block := oldDF.mmap[fpos+12 : fpos+12+length]
			fpos += (12 + length)

			// determine if there's a block after this with the same id and get its time
			hasFutureBlock := false
			nextTime := int64(0)
			if fpos < oldDF.size {
				nextID := btou64(oldDF.mmap[fpos : fpos+8])
				if nextID == id {
					hasFutureBlock = true
					nextTime = int64(btou64(oldDF.mmap[fpos+12 : fpos+20]))
				}
			}

			newBlock, err := newVals.DecodeAndCombine(block, buf[:0], nextTime, hasFutureBlock)
			if err != nil {
				return err
			}
			if _, err := f.Write(append(u64tob(id), u32tob(uint32(len(newBlock)))...)); err != nil {
				f.Close()
				return err
			}
			if _, err := f.Write(newBlock); err != nil {
				f.Close()
				return err
			}

			currentPosition += uint32(12 + len(newBlock))

			if fpos >= oldDF.size {
				break
			}
		}

		// TODO: ensure we encode only the amount in a block, refactor this wil line 450 into func
		if len(newVals.floatValues) > 0 {
			// TODO: ensure we encode only the amount in a block
			block := newVals.Encode(buf)
			if _, err := f.Write(append(u64tob(id), u32tob(uint32(len(block)))...)); err != nil {
				f.Close()
				return err
			}
			if _, err := f.Write(block); err != nil {
				f.Close()
				return err
			}
			currentPosition += uint32(12 + len(block))
		}
	}

	// write out the times and positions
	if _, err := f.Seek(4, 0); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(append(u64tob(uint64(minTime)), u64tob(uint64(maxTime))...)); err != nil {
		f.Close()
		return err
	}
	for _, pos := range newPositions {
		if _, err := f.Seek(8, 1); err != nil {
			f.Close()
			return err
		}

		if _, err := f.Write(u32tob(pos)); err != nil {
			return err
		}
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if _, err := f.Seek(0, 0); err != nil {
		f.Close()
		return err
	}

	// now open it as a memory mapped data file
	newDF, err := NewDataFile(f)
	if err != nil {
		return err
	}

	// update the engine to point at the new dataFiles
	e.queryLock.Lock()
	var files dataFiles
	for _, df := range e.files {
		if df != oldDF {
			files = append(files, df)
		}
	}
	files = append(files, newDF)
	sort.Sort(files)
	e.files = files
	e.queryLock.Unlock()

	// remove the old data file. no need to block returning the write,
	// but we need to let any running queries finish before deleting it
	if oldDF != nil {
		go func(df *dataFile) {
			if err := oldDF.Delete(); err != nil {
				// TODO: log this error
			}
		}(oldDF)
	}

	return nil
}

func (e *Engine) nextFileName() string {
	e.currentFileID++
	return filepath.Join(e.path, fmt.Sprintf("%07d.%s", e.currentFileID, Format))
}

func (e *Engine) readCompressedFile(name string) ([]byte, error) {
	f, err := os.OpenFile(filepath.Join(e.path, name), os.O_RDONLY, 0666)
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	b, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	data, err := snappy.Decode(nil, b)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (e *Engine) replaceCompressedFile(name string, data []byte) error {
	tmpName := filepath.Join(e.path, name+"tmp")
	f, err := os.OpenFile(tmpName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	b := snappy.Encode(nil, data)
	if _, err := f.Write(b); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Remove(name); err != nil && !os.IsNotExist(err) {
		return err
	}
	return os.Rename(tmpName, filepath.Join(e.path, name))
}

// DeleteSeries deletes the series from the engine.
func (e *Engine) DeleteSeries(keys []string) error {
	return nil
}

// DeleteMeasurement deletes a measurement and all related series.
func (e *Engine) DeleteMeasurement(name string, seriesKeys []string) error {
	return nil
}

// SeriesCount returns the number of series buckets on the shard.
func (e *Engine) SeriesCount() (n int, err error) {
	return 0, nil
}

// Begin starts a new transaction on the engine.
func (e *Engine) Begin(writable bool) (tsdb.Tx, error) {
	return e, nil
}

// TODO: make the cursor take a field name
func (e *Engine) Cursor(series string, direction tsdb.Direction) tsdb.Cursor {
	measurementName := tsdb.MeasurementFromSeriesKey(series)
	codec := e.Shard.FieldCodec(measurementName)
	if codec == nil {
		return &cursor{}
	}
	field := codec.FieldByName("value")
	if field == nil {
		panic("pd1 engine only supports one field with name of value")
	}

	// TODO: ensure we map the collisions
	id := hashSeriesField(seriesFieldKey(series, field.Name))
	return newCursor(id, field.Type, e.copyFilesCollection(), direction)
}

func (e *Engine) copyFilesCollection() []*dataFile {
	e.filesLock.RLock()
	defer e.filesLock.RUnlock()
	a := make([]*dataFile, len(e.files))
	copy(a, e.files)
	return a
}

func (e *Engine) Size() int64                              { return 0 }
func (e *Engine) Commit() error                            { return nil }
func (e *Engine) Rollback() error                          { return nil }
func (e *Engine) WriteTo(w io.Writer) (n int64, err error) { return 0, nil }

func (e *Engine) writeNewFields(measurementFieldsToSave map[string]*tsdb.MeasurementFields) error {
	if len(measurementFieldsToSave) == 0 {
		return nil
	}

	// read in all the previously saved fields
	fields, err := e.readFields()
	if err != nil {
		return err
	}

	// add the new ones or overwrite old ones
	for name, mf := range measurementFieldsToSave {
		fields[name] = mf
	}

	return e.writeFields(fields)
}

func (e *Engine) writeFields(fields map[string]*tsdb.MeasurementFields) error {
	// compress and save everything
	data, err := json.Marshal(fields)
	if err != nil {
		return err
	}

	fn := filepath.Join(e.path, FieldsFileExtension+"tmp")
	ff, err := os.OpenFile(fn, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	_, err = ff.Write(snappy.Encode(nil, data))
	if err != nil {
		return err
	}
	if err := ff.Close(); err != nil {
		return err
	}
	fieldsFileName := filepath.Join(e.path, FieldsFileExtension)

	if _, err := os.Stat(fieldsFileName); !os.IsNotExist(err) {
		if err := os.Remove(fieldsFileName); err != nil {
			return err
		}
	}

	return os.Rename(fn, fieldsFileName)
}

func (e *Engine) readFields() (map[string]*tsdb.MeasurementFields, error) {
	fields := make(map[string]*tsdb.MeasurementFields)

	f, err := os.OpenFile(filepath.Join(e.path, FieldsFileExtension), os.O_RDONLY, 0666)
	if os.IsNotExist(err) {
		return fields, nil
	} else if err != nil {
		return nil, err
	}
	b, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	data, err := snappy.Decode(nil, b)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}

	return fields, nil
}

func (e *Engine) writeNewSeries(seriesToCreate []*tsdb.SeriesCreate) error {
	if len(seriesToCreate) == 0 {
		return nil
	}

	// read in previously saved series
	series, err := e.readSeries()
	if err != nil {
		return err
	}

	// add new ones, compress and save
	for _, s := range seriesToCreate {
		series[s.Series.Key] = s.Series
	}

	return e.writeSeries(series)
}

func (e *Engine) writeSeries(series map[string]*tsdb.Series) error {
	data, err := json.Marshal(series)
	if err != nil {
		return err
	}

	fn := filepath.Join(e.path, SeriesFileExtension+"tmp")
	ff, err := os.OpenFile(fn, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	_, err = ff.Write(snappy.Encode(nil, data))
	if err != nil {
		return err
	}
	if err := ff.Close(); err != nil {
		return err
	}
	seriesFileName := filepath.Join(e.path, SeriesFileExtension)

	if _, err := os.Stat(seriesFileName); !os.IsNotExist(err) {
		if err := os.Remove(seriesFileName); err != nil && err != os.ErrNotExist {
			return err
		}
	}

	return os.Rename(fn, seriesFileName)
}

func (e *Engine) readSeries() (map[string]*tsdb.Series, error) {
	series := make(map[string]*tsdb.Series)

	f, err := os.OpenFile(filepath.Join(e.path, SeriesFileExtension), os.O_RDONLY, 0666)
	if os.IsNotExist(err) {
		return series, nil
	} else if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	data, err := snappy.Decode(nil, b)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(data, &series); err != nil {
		return nil, err
	}

	return series, nil
}

type valueCollection struct {
	floatValues  []FloatValue
	boolValues   []BoolValue
	intValues    []Int64Value
	stringValues []StringValue
}

func (v *valueCollection) MinTime() time.Time {
	if v.floatValues != nil {
		return v.floatValues[0].Time
	} else if v.boolValues != nil {
		return v.boolValues[0].Time
	} else if v.intValues != nil {
		return v.intValues[0].Time
	} else if v.stringValues != nil {
		return v.stringValues[0].Time
	}

	return time.Unix(0, 0)
}

func (v *valueCollection) MaxTime() time.Time {
	if v.floatValues != nil {
		return v.floatValues[len(v.floatValues)-1].Time
	} else if v.boolValues != nil {
		return v.boolValues[len(v.boolValues)-1].Time
	} else if v.intValues != nil {
		return v.intValues[len(v.intValues)-1].Time
	} else if v.stringValues != nil {
		return v.stringValues[len(v.stringValues)-1].Time
	}

	return time.Unix(0, 0)
}

func (v *valueCollection) Encode(buf []byte) []byte {
	if v.floatValues != nil {
		return EncodeFloatBlock(buf, v.floatValues)
	} else if v.boolValues != nil {
		return EncodeBoolBlock(buf, v.boolValues)
	} else if v.intValues != nil {
		return EncodeInt64Block(buf, v.intValues)
	} else if v.stringValues != nil {
		return EncodeStringBlock(buf, v.stringValues)
	}

	return nil
}

// DecodeAndCombine take an encoded block from a file, decodes it and interleaves the file
// values with the values in this collection. nextTime and hasNext refer to if the file
// has future encoded blocks so that this method can know how much of its values can be
// combined and output in the resulting encoded block.
func (v *valueCollection) DecodeAndCombine(block, buf []byte, nextTime int64, hasFutureBlock bool) ([]byte, error) {
	if v.floatValues != nil {
		values, err := DecodeFloatBlock(block)
		if err != nil {
			return nil, err
		}

		if hasFutureBlock {
			// take all values that have times less than the future block and update the vals array
			pos := sort.Search(len(v.floatValues), func(i int) bool {
				return v.floatValues[i].Time.UnixNano() >= nextTime
			})
			values = append(values, v.floatValues[:pos]...)
			v.floatValues = v.floatValues[pos:]
		} else {
			values = append(values, v.floatValues...)
			v.floatValues = nil
		}
		sort.Sort(FloatValues(values))
		// TODO: deduplicate values

		if len(values) > DefaultMaxPointsPerBlock {
			v.floatValues = values[DefaultMaxPointsPerBlock:]
			values = values[:DefaultMaxPointsPerBlock]
		}

		return EncodeFloatBlock(buf, values), nil
	} else if v.boolValues != nil {
		// TODO: wire up the other value types
		return nil, fmt.Errorf("not implemented")
	} else if v.intValues != nil {
		return nil, fmt.Errorf("not implemented")
	} else if v.stringValues != nil {
		return nil, fmt.Errorf("not implemented")
	}

	return nil, nil
}

type dataFile struct {
	f    *os.File
	mu   sync.RWMutex
	size uint32
	mmap []byte
}

// byte size constants for the data file
const (
	seriesCountSize    = 4
	timeSize           = 8
	fileHeaderSize     = seriesCountSize + (2 * timeSize)
	seriesIDSize       = 8
	seriesPositionSize = 4
	seriesHeaderSize   = seriesIDSize + seriesPositionSize
)

func NewDataFile(f *os.File) (*dataFile, error) {
	fInfo, err := f.Stat()
	if err != nil {
		return nil, err
	}
	mmap, err := syscall.Mmap(int(f.Fd()), 0, int(fInfo.Size()), syscall.PROT_READ, syscall.MAP_SHARED|MAP_POPULATE)
	if err != nil {
		f.Close()
		return nil, err
	}

	return &dataFile{
		f:    f,
		mmap: mmap,
		size: uint32(fInfo.Size()),
	}, nil
}

func (d *dataFile) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.close()
}

func (d *dataFile) Delete() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.close(); err != nil {
		return err
	}
	return os.Remove(d.f.Name())
}

func (d *dataFile) close() error {
	if d.mmap == nil {
		return nil
	}
	err := syscall.Munmap(d.mmap)
	if err != nil {
		return err
	}

	d.mmap = nil
	return d.f.Close()
}

func (d *dataFile) MinTime() int64 {
	return int64(btou64(d.mmap[4:12]))
}

func (d *dataFile) MaxTime() int64 {
	return int64(btou64(d.mmap[12:20]))
}

func (d *dataFile) SeriesCount() uint32 {
	return btou32(d.mmap[:4])
}

func (d *dataFile) IDToPosition() map[uint64]uint32 {
	count := int(d.SeriesCount())
	m := make(map[uint64]uint32)

	for i := 0; i < count; i++ {
		offset := 20 + (i * 12)
		id := btou64(d.mmap[offset : offset+8])
		pos := btou32(d.mmap[offset+8 : offset+12])
		m[id] = pos
	}

	return m
}

// StartingPositionForID returns the position in the file of the
// first block for the given ID. If zero is returned the ID doesn't
// have any data in this file.
func (d *dataFile) StartingPositionForID(id uint64) uint32 {
	seriesCount := d.SeriesCount()

	min := 0
	max := int(seriesCount)

	for min < max {
		mid := (max-min)/2 + min

		offset := mid*seriesHeaderSize + fileHeaderSize
		checkID := btou64(d.mmap[offset : offset+8])

		if checkID == id {
			return btou32(d.mmap[offset+8 : offset+12])
		} else if checkID < id {
			min = mid + 1
		} else {
			max = mid
		}
	}

	return uint32(0)
}

type dataFiles []*dataFile

func (a dataFiles) Len() int           { return len(a) }
func (a dataFiles) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a dataFiles) Less(i, j int) bool { return a[i].MinTime() < a[j].MinTime() }

type cursor struct {
	id       uint64
	dataType influxql.DataType
	f        *dataFile
	filesPos int // the index in the files slice we're looking at
	pos      uint32
	vals     FloatValues

	direction tsdb.Direction

	// time acending list of data files
	files []*dataFile
}

func newCursor(id uint64, dataType influxql.DataType, files []*dataFile, direction tsdb.Direction) *cursor {
	return &cursor{
		id:        id,
		dataType:  dataType,
		direction: direction,
		files:     files,
	}
}

func (c *cursor) Seek(seek []byte) (key, value []byte) {
	t := int64(btou64(seek))

	if t < c.files[0].MinTime() {
		c.filesPos = 0
		c.f = c.files[0]
	} else {
		for i, f := range c.files {
			if t >= f.MinTime() && t <= f.MaxTime() {
				c.filesPos = i
				c.f = f
				break
			}
		}
	}

	if c.f == nil {
		return nil, nil
	}

	// TODO: make this for the reverse direction cursor

	// now find the spot in the file we need to go
	for {
		pos := c.f.StartingPositionForID(c.id)

		// if this id isn't in this file, move to next one or return
		if pos == 0 {
			c.filesPos++
			if c.filesPos >= len(c.files) {
				return nil, nil
			}
			c.f = c.files[c.filesPos]
			continue
		}

		// seek to the block and values we're looking for
		for {
			// if the time is between this block and the next,
			// decode this block and go, otherwise seek to next block
			length := btou32(c.f.mmap[pos+8 : pos+12])

			// if the next block has a time less than what we're seeking to,
			// skip decoding this block and continue on
			nextBlockPos := pos + 12 + length
			if nextBlockPos < c.f.size {
				nextBlockID := btou64(c.f.mmap[nextBlockPos : nextBlockPos+8])
				if nextBlockID == c.id {
					nextBlockTime := int64(btou64(c.f.mmap[nextBlockPos+12 : nextBlockPos+20]))
					if nextBlockTime <= t {
						pos = nextBlockPos
						continue
					}
				}
			}

			// it must be in this block or not at all
			tb, vb := c.decodeBlockAndGetValues(pos)
			if int64(btou64(tb)) >= t {
				return tb, vb
			}

			// wasn't in the first value popped out of the block, check the rest
			for i, v := range c.vals {
				if v.Time.UnixNano() >= t {
					c.vals = c.vals[i+1:]
					return v.TimeBytes(), v.ValueBytes()
				}
			}

			// not in this one, let the top loop look for it in the next file
			break
		}
	}
}

func (c *cursor) Next() (key, value []byte) {
	if len(c.vals) == 0 {
		// if we have a file set, see if the next block is for this ID
		if c.f != nil && c.pos < c.f.size {
			nextBlockID := btou64(c.f.mmap[c.pos : c.pos+8])
			if nextBlockID == c.id {
				return c.decodeBlockAndGetValues(c.pos)
			}
		}

		// if the file is nil we hit the end of the previous file, advance the file cursor
		if c.f != nil {
			c.filesPos++
		}

		// loop until we find a file with some data
		for c.filesPos < len(c.files) {
			f := c.files[c.filesPos]

			startingPos := f.StartingPositionForID(c.id)
			if startingPos == 0 {
				continue
			}
			c.f = f
			return c.decodeBlockAndGetValues(startingPos)
		}

		// we didn't get to a file that had a next value
		return nil, nil
	}

	v := c.vals[0]
	c.vals = c.vals[1:]

	return v.TimeBytes(), v.ValueBytes()
}

func (c *cursor) decodeBlockAndGetValues(position uint32) ([]byte, []byte) {
	length := btou32(c.f.mmap[position+8 : position+12])
	block := c.f.mmap[position+12 : position+12+length]
	c.vals, _ = DecodeFloatBlock(block)
	c.pos = position + 12 + length

	v := c.vals[0]
	c.vals = c.vals[1:]
	return v.TimeBytes(), v.ValueBytes()
}

func (c *cursor) Direction() tsdb.Direction { return c.direction }

// u64tob converts a uint64 into an 8-byte slice.
func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func btou64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func u32tob(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

func btou32(b []byte) uint32 {
	return uint32(binary.BigEndian.Uint32(b))
}

func hashSeriesField(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}

// seriesFieldKey combine a series key and field name for a unique string to be hashed to a numeric ID
func seriesFieldKey(seriesKey, field string) string {
	return seriesKey + "#" + field
}

type uint64slice []uint64

func (a uint64slice) Len() int           { return len(a) }
func (a uint64slice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a uint64slice) Less(i, j int) bool { return a[i] < a[j] }

/* TODO: REMOVE THIS STUFF */
func (e *Engine) pointsToBlocks(points [][]byte) []byte {
	var b bytes.Buffer
	block := make([]byte, 0)
	for _, p := range points {
		block = append(block, p[0:8]...)
		block = append(block, u32tob(uint32(len(p)-8))...)
		block = append(block, p[8:]...)
		if len(block) > DefaultBlockSize {
			e.writeBlockToBuffer(block, &b)
			block = make([]byte, 0)
		}
	}
	if len(block) > 0 {
		e.writeBlockToBuffer(block, &b)
	}

	return b.Bytes()
}

func (e *Engine) writeBlockToBuffer(block []byte, b *bytes.Buffer) {
	// write the min time
	if _, err := b.Write(block[0:8]); err != nil {
		panic(err)
	}

	// write the length of the compressed data
	data := snappy.Encode(nil, block)
	if _, err := b.Write(u32tob(uint32(len(data)))); err != nil {
		panic(err)
	}

	// write the compressed data
	if _, err := b.Write(data); err != nil {
		panic(err)
	}
}

func (e *Engine) readPointsFromFile(f *os.File) (map[uint64][][]byte, error) {
	buf := make([]byte, 8)
	if _, err := io.ReadFull(f, buf); err != nil {
		return nil, err
	}
	seriesCount := btou64(buf)
	positions := make([]uint64, seriesCount, seriesCount)
	ids := make([]uint64, seriesCount, seriesCount)

	// read the series index file header
	position := uint64(8)
	for i := 0; uint64(i) < seriesCount; i++ {
		// read the id of the series
		if _, err := io.ReadFull(f, buf); err != nil {
			return nil, err
		}
		ids[i] = btou64(buf)

		// read the min time and ignore
		if _, err := io.ReadFull(f, buf); err != nil {
			return nil, err
		}
		if _, err := io.ReadFull(f, buf); err != nil {
			return nil, err
		}

		// read the starting position of this id
		if _, err := io.ReadFull(f, buf); err != nil {
			return nil, err
		}
		positions[i] = btou64(buf)
		position += 32
	}

	if position != positions[0] {
		panic("we aren't at the right place")
	}

	// read the raw data
	seriesData := make(map[uint64][][]byte)
	compressedBuff := make([]byte, DefaultBlockSize)
	seriesPosition := 0
	for {
		// read the min time and ignore
		if _, err := io.ReadFull(f, buf); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		// read the length of the compressed block
		if _, err := io.ReadFull(f, buf[:4]); err != nil {
			return nil, err
		}
		length := btou32(buf)

		if length > uint32(len(compressedBuff)) {
			compressedBuff = make([]byte, length)
		}
		if _, err := io.ReadFull(f, compressedBuff[:length]); err != nil {
			return nil, err
		}

		data, err := snappy.Decode(nil, compressedBuff[:length])
		if err != nil {
			return nil, err
		}
		id := ids[seriesPosition]
		seriesData[id] = append(seriesData[id], e.pointsFromDataBlock(data)...)
		position += uint64(12 + length)

		if seriesPosition+1 >= len(positions) {
			continue
		}
		if positions[seriesPosition+1] == position {
			seriesPosition += 1
		}
	}

	return seriesData, nil
}

func (e *Engine) pointsFromDataBlock(data []byte) [][]byte {
	a := make([][]byte, 0)
	for {
		length := entryDataSize(data)
		p := append(data[:8], data[12:12+length]...)
		a = append(a, p)
		data = data[12+length:]
		if len(data) == 0 {
			break
		}
	}
	return a
}

func entryDataSize(v []byte) int { return int(binary.BigEndian.Uint32(v[8:12])) }

func (e *Engine) lastFileAndNewFile() (*os.File, *os.File, error) {
	files, err := filepath.Glob(filepath.Join(e.path, fmt.Sprintf("*.%s", Format)))
	if err != nil {
		return nil, nil, err
	}

	if len(files) == 0 {
		newFile, err := os.OpenFile(filepath.Join(e.path, fmt.Sprintf("%07d.%s", 1, Format)), os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return nil, nil, err
		}
		return nil, newFile, nil
	}

	oldFile, err := os.OpenFile(files[len(files)-1], os.O_RDONLY, 0666)
	if err != nil {
		return nil, nil, err
	}

	info, err := oldFile.Stat()
	if err != nil {
		_ = oldFile.Close()
		return nil, nil, err
	}

	num := strings.Split(filepath.Base(files[len(files)-1]), ".")[0]
	n, err := strconv.ParseUint(num, 10, 32)
	if err != nil {
		return nil, nil, err
	}
	newFile, err := os.OpenFile(filepath.Join(e.path, fmt.Sprintf("%07d.%s", n+1, Format)), os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, nil, err
	}
	if info.Size() >= DefaultMaxFileSize {
		oldFile.Close()
		return nil, newFile, nil
	}
	return oldFile, newFile, nil
}
