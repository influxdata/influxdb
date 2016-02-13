package tsdb

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/internal"
)

const (
	statWriteReq        = "writeReq"
	statSeriesCreate    = "seriesCreate"
	statFieldsCreate    = "fieldsCreate"
	statWritePointsFail = "writePointsFail"
	statWritePointsOK   = "writePointsOk"
	statWriteBytes      = "writeBytes"
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
)

// Shard represents a self-contained time series database. An inverted index of
// the measurement and tag data is kept along with the raw time series data.
// Data can be split across many shards. The query engine in TSDB is responsible
// for combining the output of many shards into a single query result.
type Shard struct {
	index   *DatabaseIndex
	path    string
	walPath string
	id      uint64

	engine  Engine
	options EngineOptions

	mu                sync.RWMutex
	measurementFields map[string]*MeasurementFields // measurement name to their fields

	// expvar-based stats.
	statMap *expvar.Map

	// The writer used by the logger.
	LogOutput io.Writer
}

// NewShard returns a new initialized Shard. walPath doesn't apply to the b1 type index
func NewShard(id uint64, index *DatabaseIndex, path string, walPath string, options EngineOptions) *Shard {
	// Configure statistics collection.
	key := fmt.Sprintf("shard:%s:%d", path, id)
	tags := map[string]string{"path": path, "id": fmt.Sprintf("%d", id), "engine": options.EngineVersion}
	statMap := influxdb.NewStatistics(key, "shard", tags)

	return &Shard{
		index:             index,
		path:              path,
		walPath:           walPath,
		id:                id,
		options:           options,
		measurementFields: make(map[string]*MeasurementFields),

		statMap:   statMap,
		LogOutput: os.Stderr,
	}
}

// Path returns the path set on the shard when it was created.
func (s *Shard) Path() string { return s.path }

// PerformMaintenance gets called periodically to have the engine perform
// any maintenance tasks like WAL flushing and compaction
func (s *Shard) PerformMaintenance() {
	s.engine.PerformMaintenance()
}

// Open initializes and opens the shard's store.
func (s *Shard) Open() error {
	if err := func() error {
		s.mu.Lock()
		defer s.mu.Unlock()

		s.index.mu.Lock()
		defer s.index.mu.Unlock()

		// Return if the shard is already open
		if s.engine != nil {
			return nil
		}

		// Initialize underlying engine.
		e, err := NewEngine(s.path, s.walPath, s.options)
		if err != nil {
			return fmt.Errorf("new engine: %s", err)
		}
		s.engine = e

		// Set log output on the engine.
		s.engine.SetLogOutput(s.LogOutput)

		// Open engine.
		if err := s.engine.Open(); err != nil {
			return fmt.Errorf("open engine: %s", err)
		}

		// Load metadata index.
		if err := s.engine.LoadMetadataIndex(s, s.index, s.measurementFields); err != nil {
			return fmt.Errorf("load metadata index: %s", err)
		}

		return nil
	}(); err != nil {
		s.close()
		return err
	}

	return nil
}

// Close shuts down the shard's store.
func (s *Shard) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.close()
}

func (s *Shard) close() error {
	if s.engine != nil {
		return s.engine.Close()
	}
	return nil
}

// DiskSize returns the size on disk of this shard
func (s *Shard) DiskSize() (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	stats, err := os.Stat(s.path)
	if err != nil {
		return 0, err
	}
	return stats.Size(), nil
}

// FieldCodec returns the field encoding for a measurement.
// TODO: this is temporarily exported to make tx.go work. When the query engine gets refactored
// into the tsdb package this should be removed. No one outside tsdb should know the underlying field encoding scheme.
func (s *Shard) FieldCodec(measurementName string) *FieldCodec {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.measurementFields[measurementName]
	if m == nil {
		return NewFieldCodec(nil)
	}
	return m.Codec
}

// FieldCreate holds information for a field to create on a measurement
type FieldCreate struct {
	Measurement string
	Field       *Field
}

// SeriesCreate holds information for a series to create
type SeriesCreate struct {
	Measurement string
	Series      *Series
}

// WritePoints will write the raw data points and any new metadata to the index in the shard
func (s *Shard) WritePoints(points []models.Point) error {
	s.statMap.Add(statWriteReq, 1)

	seriesToCreate, fieldsToCreate, seriesToAddShardTo, err := s.validateSeriesAndFields(points)
	if err != nil {
		return err
	}
	s.statMap.Add(statSeriesCreate, int64(len(seriesToCreate)))
	s.statMap.Add(statFieldsCreate, int64(len(fieldsToCreate)))

	// add any new series to the in-memory index
	if len(seriesToCreate) > 0 {
		s.index.mu.Lock()
		for _, ss := range seriesToCreate {
			s.index.CreateSeriesIndexIfNotExists(ss.Measurement, ss.Series)
		}
		s.index.mu.Unlock()
	}

	if len(seriesToAddShardTo) > 0 {
		s.index.mu.Lock()
		for _, k := range seriesToAddShardTo {
			ss := s.index.series[k]
			if ss != nil {
				ss.shardIDs[s.id] = true
			}
		}
		s.index.mu.Unlock()
	}

	// add any new fields and keep track of what needs to be saved
	measurementFieldsToSave, err := s.createFieldsAndMeasurements(fieldsToCreate)
	if err != nil {
		return err
	}

	// make sure all data is encoded before attempting to save to bolt
	// only required for the b1 and bz1 formats
	if s.engine.Format() != TSM1Format {
		for _, p := range points {
			// Ignore if raw data has already been marshaled.
			if p.Data() != nil {
				continue
			}

			// This was populated earlier, don't need to validate that it's there.
			s.mu.RLock()
			mf := s.measurementFields[p.Name()]
			s.mu.RUnlock()

			// If a measurement is dropped while writes for it are in progress, this could be nil
			if mf == nil {
				return ErrFieldNotFound
			}

			data, err := mf.Codec.EncodeFields(p.Fields())
			if err != nil {
				return err
			}
			p.SetData(data)
		}
	}

	// Write to the engine.
	if err := s.engine.WritePoints(points, measurementFieldsToSave, seriesToCreate); err != nil {
		s.statMap.Add(statWritePointsFail, 1)
		return fmt.Errorf("engine: %s", err)
	}
	s.statMap.Add(statWritePointsOK, int64(len(points)))

	return nil
}

// DeleteSeries deletes a list of series.
func (s *Shard) DeleteSeries(seriesKeys []string) error {
	return s.engine.DeleteSeries(seriesKeys)
}

// DeleteMeasurement deletes a measurement and all underlying series.
func (s *Shard) DeleteMeasurement(name string, seriesKeys []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.engine.DeleteMeasurement(name, seriesKeys); err != nil {
		return err
	}

	// Remove entry from shard index.
	delete(s.measurementFields, name)

	return nil
}

func (s *Shard) createFieldsAndMeasurements(fieldsToCreate []*FieldCreate) (map[string]*MeasurementFields, error) {
	if len(fieldsToCreate) == 0 {
		return nil, nil
	}

	s.index.mu.Lock()
	s.mu.Lock()
	defer s.index.mu.Unlock()
	defer s.mu.Unlock()

	// add fields
	measurementsToSave := make(map[string]*MeasurementFields)
	for _, f := range fieldsToCreate {
		m := s.measurementFields[f.Measurement]
		if m == nil {
			m = measurementsToSave[f.Measurement]
			if m == nil {
				m = &MeasurementFields{Fields: make(map[string]*Field)}
			}
			s.measurementFields[f.Measurement] = m
		}

		measurementsToSave[f.Measurement] = m

		// Add the field to the in memory index
		if err := m.CreateFieldIfNotExists(f.Field.Name, f.Field.Type, false); err != nil {
			return nil, err
		}

		// ensure the measurement is in the index and the field is there
		measurement := s.index.CreateMeasurementIndexIfNotExists(f.Measurement)
		measurement.SetFieldName(f.Field.Name)
	}

	return measurementsToSave, nil
}

// validateSeriesAndFields checks which series and fields are new and whose metadata should be saved and indexed
func (s *Shard) validateSeriesAndFields(points []models.Point) ([]*SeriesCreate, []*FieldCreate, []string, error) {
	var seriesToCreate []*SeriesCreate
	var fieldsToCreate []*FieldCreate
	var seriesToAddShardTo []string

	// get the mutex for the in memory index, which is shared across shards
	s.index.mu.RLock()
	defer s.index.mu.RUnlock()

	// get the shard mutex for locally defined fields
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, p := range points {
		// see if the series should be added to the index
		if ss := s.index.series[string(p.Key())]; ss == nil {
			series := NewSeries(string(p.Key()), p.Tags())
			seriesToCreate = append(seriesToCreate, &SeriesCreate{p.Name(), series})
			seriesToAddShardTo = append(seriesToAddShardTo, series.Key)
		} else if !ss.shardIDs[s.id] {
			// this is the first time this series is being written into this shard, persist it
			seriesToCreate = append(seriesToCreate, &SeriesCreate{p.Name(), ss})
			seriesToAddShardTo = append(seriesToAddShardTo, ss.Key)
		}

		// see if the field definitions need to be saved to the shard
		mf := s.measurementFields[p.Name()]
		if mf == nil {
			for name, value := range p.Fields() {
				fieldsToCreate = append(fieldsToCreate, &FieldCreate{p.Name(), &Field{Name: name, Type: influxql.InspectDataType(value)}})
			}
			continue // skip validation since all fields are new
		}

		// validate field types and encode data
		for name, value := range p.Fields() {
			if f := mf.Fields[name]; f != nil {
				// Field present in shard metadata, make sure there is no type conflict.
				if f.Type != influxql.InspectDataType(value) {
					return nil, nil, nil, fmt.Errorf("field type conflict: input field \"%s\" on measurement \"%s\" is type %T, already exists as type %s", name, p.Name(), value, f.Type)
				}

				continue // Field is present, and it's of the same type. Nothing more to do.
			}

			fieldsToCreate = append(fieldsToCreate, &FieldCreate{p.Name(), &Field{Name: name, Type: influxql.InspectDataType(value)}})
		}
	}

	return seriesToCreate, fieldsToCreate, seriesToAddShardTo, nil
}

// SeriesCount returns the number of series buckets on the shard.
func (s *Shard) SeriesCount() (int, error) { return s.engine.SeriesCount() }

// WriteTo writes the shard's data to w.
func (s *Shard) WriteTo(w io.Writer) (int64, error) {
	n, err := s.engine.WriteTo(w)
	s.statMap.Add(statWriteBytes, int64(n))
	return n, err
}

// CreateIterator returns an iterator for the data in the shard.
func (s *Shard) CreateIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {
	return s.engine.CreateIterator(opt)
}

// FieldDimensions returns unique sets of fields and dimensions across a list of sources.
func (s *Shard) FieldDimensions(sources influxql.Sources) (fields, dimensions map[string]struct{}, err error) {
	fields = make(map[string]struct{})
	dimensions = make(map[string]struct{})

	for _, src := range sources {
		switch m := src.(type) {
		case *influxql.Measurement:
			// Retrieve measurement.
			mm := s.index.Measurement(m.Name)
			if mm == nil {
				continue
			}

			// Append fields and dimensions.
			for _, name := range mm.FieldNames() {
				fields[name] = struct{}{}
			}
			for _, key := range mm.TagKeys() {
				dimensions[key] = struct{}{}
			}
		}
	}

	return
}

// SeriesKeys returns a list of series in the shard.
func (s *Shard) SeriesKeys(opt influxql.IteratorOptions) (influxql.SeriesList, error) {
	return s.engine.SeriesKeys(opt)
}

// Shards represents a sortable list of shards.
type Shards []*Shard

func (a Shards) Len() int           { return len(a) }
func (a Shards) Less(i, j int) bool { return a[i].id < a[j].id }
func (a Shards) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// CreateIterator returns a single combined iterator for the shards.
func (a Shards) CreateIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {
	if influxql.Sources(opt.Sources).HasSystemSource() {
		return a.createSystemIterator(opt)
	}

	// Create iterators for each shard.
	// Ensure that they are closed if an error occurs.
	itrs := make([]influxql.Iterator, 0, len(a))
	if err := func() error {
		for _, sh := range a {
			itr, err := sh.CreateIterator(opt)
			if err != nil {
				return err
			}
			itrs = append(itrs, itr)
		}
		return nil
	}(); err != nil {
		influxql.Iterators(itrs).Close()
		return nil, err
	}

	// Merge into a single iterator.
	if opt.MergeSorted() {
		return influxql.NewSortedMergeIterator(itrs, opt), nil
	}

	itr := influxql.NewMergeIterator(itrs, opt)
	if opt.Expr != nil {
		if expr, ok := opt.Expr.(*influxql.Call); ok && expr.Name == "count" {
			opt.Expr = &influxql.Call{
				Name: "sum",
				Args: expr.Args,
			}
		}
	}
	return influxql.NewCallIterator(itr, opt), nil
}

// createSystemIterator returns an iterator for a system source.
func (a Shards) createSystemIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {
	// Only support a single system source.
	if len(opt.Sources) > 1 {
		return nil, errors.New("cannot select from multiple system sources")
	}

	m := opt.Sources[0].(*influxql.Measurement)
	switch m.Name {
	case "_measurements":
		return a.createMeasurementsIterator(opt)
	case "_tagKeys":
		return a.createTagKeysIterator(opt)
	default:
		return nil, fmt.Errorf("unknown system source: %s", m.Name)
	}
}

// SeriesKeys returns a list of series in in all shards in a. If a series
// exists in multiple shards in a, all instances will be combined into a single
// Series by calling Combine on it.
func (a Shards) SeriesKeys(opt influxql.IteratorOptions) (influxql.SeriesList, error) {
	if influxql.Sources(opt.Sources).HasSystemSource() {
		// Only support a single system source.
		if len(opt.Sources) > 1 {
			return nil, errors.New("cannot select from multiple system sources")
		}
		// Meta queries don't need to know the series name and always have a single string.
		return []influxql.Series{{Aux: []influxql.DataType{influxql.String}}}, nil
	}

	seriesMap := make(map[string]influxql.Series)
	for _, sh := range a {
		series, err := sh.SeriesKeys(opt)
		if err != nil {
			return nil, err
		}

		for _, s := range series {
			cur, ok := seriesMap[s.ID()]
			if ok {
				cur.Combine(&s)
			} else {
				seriesMap[s.ID()] = s
			}
		}
	}

	seriesList := make([]influxql.Series, 0, len(seriesMap))
	for _, s := range seriesMap {
		seriesList = append(seriesList, s)
	}
	sort.Sort(influxql.SeriesList(seriesList))
	return influxql.SeriesList(seriesList), nil
}

// createMeasurementsIterator returns an iterator for all measurement names.
func (a Shards) createMeasurementsIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {
	itrs := make([]influxql.Iterator, 0, len(a))
	if err := func() error {
		for _, sh := range a {
			itr, err := NewMeasurementIterator(sh, opt)
			if err != nil {
				return err
			}
			itrs = append(itrs, itr)
		}
		return nil
	}(); err != nil {
		influxql.Iterators(itrs).Close()
		return nil, err
	}
	return influxql.NewMergeIterator(itrs, opt), nil
}

// createTagKeysIterator returns an iterator for all tag keys across measurements.
func (a Shards) createTagKeysIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {
	itrs := make([]influxql.Iterator, 0, len(a))
	if err := func() error {
		for _, sh := range a {
			itr, err := NewTagKeysIterator(sh, opt)
			if err != nil {
				return err
			}
			itrs = append(itrs, itr)
		}
		return nil
	}(); err != nil {
		influxql.Iterators(itrs).Close()
		return nil, err
	}
	return influxql.NewMergeIterator(itrs, opt), nil
}

// FieldDimensions returns the unique fields and dimensions across a list of sources.
func (a Shards) FieldDimensions(sources influxql.Sources) (fields, dimensions map[string]struct{}, err error) {
	fields = make(map[string]struct{})
	dimensions = make(map[string]struct{})

	for _, sh := range a {
		f, d, err := sh.FieldDimensions(sources)
		if err != nil {
			return nil, nil, err
		}
		for k := range f {
			fields[k] = struct{}{}
		}
		for k := range d {
			dimensions[k] = struct{}{}
		}
	}
	return
}

// MeasurementFields holds the fields of a measurement and their codec.
type MeasurementFields struct {
	Fields map[string]*Field `json:"fields"`
	Codec  *FieldCodec
}

// MarshalBinary encodes the object to a binary format.
func (m *MeasurementFields) MarshalBinary() ([]byte, error) {
	var pb internal.MeasurementFields
	for _, f := range m.Fields {
		id := int32(f.ID)
		name := f.Name
		t := int32(f.Type)
		pb.Fields = append(pb.Fields, &internal.Field{ID: &id, Name: &name, Type: &t})
	}
	return proto.Marshal(&pb)
}

// UnmarshalBinary decodes the object from a binary format.
func (m *MeasurementFields) UnmarshalBinary(buf []byte) error {
	var pb internal.MeasurementFields
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}
	m.Fields = make(map[string]*Field, len(pb.Fields))
	for _, f := range pb.Fields {
		m.Fields[f.GetName()] = &Field{ID: uint8(f.GetID()), Name: f.GetName(), Type: influxql.DataType(f.GetType())}
	}
	return nil
}

// CreateFieldIfNotExists creates a new field with an autoincrementing ID.
// Returns an error if 255 fields have already been created on the measurement or
// the fields already exists with a different type.
func (m *MeasurementFields) CreateFieldIfNotExists(name string, typ influxql.DataType, limitCount bool) error {
	// Ignore if the field already exists.
	if f := m.Fields[name]; f != nil {
		if f.Type != typ {
			return ErrFieldTypeConflict
		}
		return nil
	}

	// If we're supposed to limit the number of fields, only 255 are allowed. If we go over that then return an error.
	if len(m.Fields)+1 > math.MaxUint8 && limitCount {
		return ErrFieldOverflow
	}

	// Create and append a new field.
	f := &Field{
		ID:   uint8(len(m.Fields) + 1),
		Name: name,
		Type: typ,
	}
	m.Fields[name] = f
	m.Codec = NewFieldCodec(m.Fields)

	return nil
}

// Field represents a series field.
type Field struct {
	ID   uint8             `json:"id,omitempty"`
	Name string            `json:"name,omitempty"`
	Type influxql.DataType `json:"type,omitempty"`
}

// FieldCodec provides encoding and decoding functionality for the fields of a given
// Measurement. It is a distinct type to avoid locking writes on this node while
// potentially long-running queries are executing.
//
// It is not affected by changes to the Measurement object after codec creation.
// TODO: this shouldn't be exported. nothing outside the shard should know about field encodings.
//       However, this is here until tx.go and the engine get refactored into tsdb.
type FieldCodec struct {
	fieldsByID   map[uint8]*Field
	fieldsByName map[string]*Field
}

// NewFieldCodec returns a FieldCodec for the given Measurement. Must be called with
// a RLock that protects the Measurement.
func NewFieldCodec(fields map[string]*Field) *FieldCodec {
	fieldsByID := make(map[uint8]*Field, len(fields))
	fieldsByName := make(map[string]*Field, len(fields))
	for _, f := range fields {
		fieldsByID[f.ID] = f
		fieldsByName[f.Name] = f
	}
	return &FieldCodec{fieldsByID: fieldsByID, fieldsByName: fieldsByName}
}

// EncodeFields converts a map of values with string keys to a byte slice of field
// IDs and values.
//
// If a field exists in the codec, but its type is different, an error is returned. If
// a field is not present in the codec, the system panics.
func (f *FieldCodec) EncodeFields(values map[string]interface{}) ([]byte, error) {
	// Allocate byte slice
	b := make([]byte, 0, 10)

	for k, v := range values {
		field := f.fieldsByName[k]
		if field == nil {
			panic(fmt.Sprintf("field does not exist for %s", k))
		} else if influxql.InspectDataType(v) != field.Type {
			return nil, fmt.Errorf("field \"%s\" is type %T, mapped as type %s", k, v, field.Type)
		}

		var buf []byte

		switch field.Type {
		case influxql.Float:
			value := v.(float64)
			buf = make([]byte, 9)
			binary.BigEndian.PutUint64(buf[1:9], math.Float64bits(value))
		case influxql.Integer:
			var value uint64
			switch v.(type) {
			case int:
				value = uint64(v.(int))
			case int32:
				value = uint64(v.(int32))
			case int64:
				value = uint64(v.(int64))
			default:
				panic(fmt.Sprintf("invalid integer type: %T", v))
			}
			buf = make([]byte, 9)
			binary.BigEndian.PutUint64(buf[1:9], value)
		case influxql.Boolean:
			value := v.(bool)

			// Only 1 byte need for a boolean.
			buf = make([]byte, 2)
			if value {
				buf[1] = byte(1)
			}
		case influxql.String:
			value := v.(string)
			if len(value) > maxStringLength {
				value = value[:maxStringLength]
			}
			// Make a buffer for field ID (1 bytes), the string length (2 bytes), and the string.
			buf = make([]byte, len(value)+3)

			// Set the string length, then copy the string itself.
			binary.BigEndian.PutUint16(buf[1:3], uint16(len(value)))
			for i, c := range []byte(value) {
				buf[i+3] = byte(c)
			}
		default:
			panic(fmt.Sprintf("unsupported value type during encode fields: %T", v))
		}

		// Always set the field ID as the leading byte.
		buf[0] = field.ID

		// Append temp buffer to the end.
		b = append(b, buf...)
	}

	return b, nil
}

// FieldIDByName returns the ID of the field with the given name s.
// TODO: this shouldn't be exported. remove when tx.go and engine.go get refactored into tsdb
func (f *FieldCodec) FieldIDByName(s string) (uint8, error) {
	fi := f.fieldsByName[s]
	if fi == nil {
		return 0, ErrFieldNotFound
	}
	return fi.ID, nil
}

// DecodeFields decodes a byte slice into a set of field ids and values.
func (f *FieldCodec) DecodeFields(b []byte) (map[uint8]interface{}, error) {
	if len(b) == 0 {
		return nil, nil
	}

	// Create a map to hold the decoded data.
	values := make(map[uint8]interface{}, 0)

	for {
		if len(b) < 1 {
			// No more bytes.
			break
		}

		// First byte is the field identifier.
		fieldID := b[0]
		field := f.fieldsByID[fieldID]
		if field == nil {
			// See note in DecodeByID() regarding field-mapping failures.
			return nil, ErrFieldUnmappedID
		}

		var value interface{}
		switch field.Type {
		case influxql.Float:
			value = math.Float64frombits(binary.BigEndian.Uint64(b[1:9]))
			// Move bytes forward.
			b = b[9:]
		case influxql.Integer:
			value = int64(binary.BigEndian.Uint64(b[1:9]))
			// Move bytes forward.
			b = b[9:]
		case influxql.Boolean:
			if b[1] == 1 {
				value = true
			} else {
				value = false
			}
			// Move bytes forward.
			b = b[2:]
		case influxql.String:
			size := binary.BigEndian.Uint16(b[1:3])
			value = string(b[3 : size+3])
			// Move bytes forward.
			b = b[size+3:]
		default:
			panic(fmt.Sprintf("unsupported value type during decode fields: %T", f.fieldsByID[fieldID]))
		}

		values[fieldID] = value

	}

	return values, nil
}

// DecodeFieldsWithNames decodes a byte slice into a set of field names and values
// TODO: shouldn't be exported. refactor engine
func (f *FieldCodec) DecodeFieldsWithNames(b []byte) (map[string]interface{}, error) {
	fields, err := f.DecodeFields(b)
	if err != nil {
		return nil, err
	}
	m := make(map[string]interface{})
	for id, v := range fields {
		field := f.fieldsByID[id]
		if field != nil {
			m[field.Name] = v
		}
	}
	return m, nil
}

// DecodeByID scans a byte slice for a field with the given ID, converts it to its
// expected type, and return that value.
// TODO: shouldn't be exported. refactor engine
func (f *FieldCodec) DecodeByID(targetID uint8, b []byte) (interface{}, error) {
	if len(b) == 0 {
		return 0, ErrFieldNotFound
	}

	for {
		if len(b) < 1 {
			// No more bytes.
			break
		}
		field, ok := f.fieldsByID[b[0]]
		if !ok {
			// This can happen, though is very unlikely. If this node receives encoded data, to be written
			// to disk, and is queried for that data before its metastore is updated, there will be no field
			// mapping for the data during decode. All this can happen because data is encoded by the node
			// that first received the write request, not the node that actually writes the data to disk.
			// So if this happens, the read must be aborted.
			return 0, ErrFieldUnmappedID
		}

		var value interface{}
		switch field.Type {
		case influxql.Float:
			// Move bytes forward.
			value = math.Float64frombits(binary.BigEndian.Uint64(b[1:9]))
			b = b[9:]
		case influxql.Integer:
			value = int64(binary.BigEndian.Uint64(b[1:9]))
			b = b[9:]
		case influxql.Boolean:
			if b[1] == 1 {
				value = true
			} else {
				value = false
			}
			// Move bytes forward.
			b = b[2:]
		case influxql.String:
			size := binary.BigEndian.Uint16(b[1:3])
			value = string(b[3 : 3+size])
			// Move bytes forward.
			b = b[size+3:]
		default:
			panic(fmt.Sprintf("unsupported value type during decode by id: %T", field.Type))
		}

		if field.ID == targetID {
			return value, nil
		}
	}

	return 0, ErrFieldNotFound
}

// DecodeByName scans a byte slice for a field with the given name, converts it to its
// expected type, and return that value.
func (f *FieldCodec) DecodeByName(name string, b []byte) (interface{}, error) {
	fi := f.FieldByName(name)
	if fi == nil {
		return 0, ErrFieldNotFound
	}
	return f.DecodeByID(fi.ID, b)
}

// Fields returns a unsorted list of the codecs fields.
func (f *FieldCodec) Fields() []*Field {
	a := make([]*Field, 0, len(f.fieldsByID))
	for _, f := range f.fieldsByID {
		a = append(a, f)
	}
	return a
}

// FieldByName returns the field by its name. It will return a nil if not found
func (f *FieldCodec) FieldByName(name string) *Field {
	return f.fieldsByName[name]
}

// MeasurementIterator represents a string iterator that emits all measurement names in a shard.
type MeasurementIterator struct {
	mms    Measurements
	source *influxql.Measurement
}

// NewMeasurementIterator returns a new instance of MeasurementIterator.
func NewMeasurementIterator(sh *Shard, opt influxql.IteratorOptions) (*MeasurementIterator, error) {
	itr := &MeasurementIterator{}

	// Extract source.
	if len(opt.Sources) > 0 {
		itr.source, _ = opt.Sources[0].(*influxql.Measurement)
	}

	// Retrieve measurements from shard. Filter if condition specified.
	if opt.Condition == nil {
		itr.mms = sh.index.Measurements()
	} else {
		mms, err := sh.index.measurementsByExpr(opt.Condition)
		if err != nil {
			return nil, err
		}
		itr.mms = mms
	}

	// Sort measurements by name.
	sort.Sort(itr.mms)

	return itr, nil
}

// Close closes the iterator.
func (itr *MeasurementIterator) Close() error { return nil }

// Next emits the next measurement name.
func (itr *MeasurementIterator) Next() *influxql.FloatPoint {
	if len(itr.mms) == 0 {
		return nil
	}
	mm := itr.mms[0]
	itr.mms = itr.mms[1:]
	return &influxql.FloatPoint{
		Name: "measurements",
		Aux:  []interface{}{mm.Name},
	}
}

// TagKeysIterator represents a string iterator that emits all tag keys in a shard.
type TagKeysIterator struct {
	mms Measurements // remaining measurements
	buf struct {
		mm   *Measurement // current measurement
		keys []string     // current measurement's keys
	}
}

// NewTagKeysIterator returns a new instance of TagKeysIterator.
func NewTagKeysIterator(sh *Shard, opt influxql.IteratorOptions) (*TagKeysIterator, error) {
	itr := &TagKeysIterator{}

	// Retrieve measurements from shard. Filter if condition specified.
	if opt.Condition == nil {
		itr.mms = sh.index.Measurements()
	} else {
		mms, err := sh.index.measurementsByExpr(opt.Condition)
		if err != nil {
			return nil, err
		}
		itr.mms = mms
	}

	// Sort measurements by name.
	sort.Sort(itr.mms)

	return itr, nil
}

// Close closes the iterator.
func (itr *TagKeysIterator) Close() error { return nil }

// Next emits the next tag key name.
func (itr *TagKeysIterator) Next() *influxql.FloatPoint {
	for {
		// If there are no more keys then move to the next measurements.
		if len(itr.buf.keys) == 0 {
			if len(itr.mms) == 0 {
				return nil
			}

			itr.buf.mm = itr.mms[0]
			itr.buf.keys = itr.buf.mm.TagKeys()
			itr.mms = itr.mms[1:]
			continue
		}

		// Return next key.
		p := &influxql.FloatPoint{
			Name: itr.buf.mm.Name,
			Aux:  []interface{}{itr.buf.keys[0]},
		}
		itr.buf.keys = itr.buf.keys[1:]

		return p
	}
}

// IsNumeric returns whether a given aggregate can only be run on numeric fields.
func IsNumeric(c *influxql.Call) bool {
	switch c.Name {
	case "count", "first", "last", "distinct":
		return false
	default:
		return true
	}
}

// mustMarshal encodes a value to JSON.
// This will panic if an error occurs. This should only be used internally when
// an invalid marshal will cause corruption and a panic is appropriate.
func mustMarshalJSON(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic("marshal: " + err.Error())
	}
	return b
}

// mustUnmarshalJSON decodes a value from JSON.
// This will panic if an error occurs. This should only be used internally when
// an invalid unmarshal will cause corruption and a panic is appropriate.
func mustUnmarshalJSON(b []byte, v interface{}) {
	if err := json.Unmarshal(b, v); err != nil {
		panic("unmarshal: " + err.Error())
	}
}

// u64tob converts a uint64 into an 8-byte slice.
func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}
