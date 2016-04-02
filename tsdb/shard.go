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
	"strings"
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

	// ErrEngineClosed is returned when a caller attempts indirectly to
	// access the shard's underlying engine.
	ErrEngineClosed = errors.New("engine is closed")
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

func (e ShardError) Error() string {
	return fmt.Sprintf("[shard %d] %s", e.id, e.Err)
}

// Shard represents a self-contained time series database. An inverted index of
// the measurement and tag data is kept along with the raw time series data.
// Data can be split across many shards. The query engine in TSDB is responsible
// for combining the output of many shards into a single query result.
type Shard struct {
	index   *DatabaseIndex
	path    string
	walPath string
	id      uint64

	database        string
	retentionPolicy string

	options EngineOptions

	mu     sync.RWMutex
	engine Engine

	// expvar-based stats.
	statMap *expvar.Map

	// The writer used by the logger.
	LogOutput io.Writer
}

// NewShard returns a new initialized Shard. walPath doesn't apply to the b1 type index
func NewShard(id uint64, index *DatabaseIndex, path string, walPath string, options EngineOptions) *Shard {
	// Configure statistics collection.
	key := fmt.Sprintf("shard:%s:%d", path, id)
	db, rp := DecodeStorePath(path)
	tags := map[string]string{
		"path":            path,
		"id":              fmt.Sprintf("%d", id),
		"engine":          options.EngineVersion,
		"database":        db,
		"retentionPolicy": rp,
	}
	statMap := influxdb.NewStatistics(key, "shard", tags)

	return &Shard{
		index:   index,
		id:      id,
		path:    path,
		walPath: walPath,
		options: options,

		database:        db,
		retentionPolicy: rp,

		statMap:   statMap,
		LogOutput: os.Stderr,
	}
}

// Path returns the path set on the shard when it was created.
func (s *Shard) Path() string { return s.path }

// Open initializes and opens the shard's store.
func (s *Shard) Open() error {
	if err := func() error {
		s.mu.Lock()
		defer s.mu.Unlock()

		// Return if the shard is already open
		if s.engine != nil {
			return nil
		}

		// Initialize underlying engine.
		e, err := NewEngine(s.path, s.walPath, s.options)
		if err != nil {
			return err
		}
		s.engine = e

		// Set log output on the engine.
		s.engine.SetLogOutput(s.LogOutput)

		// Open engine.
		if err := s.engine.Open(); err != nil {
			return err
		}

		// Load metadata index.
		if err := s.engine.LoadMetadataIndex(s, s.index); err != nil {
			return err
		}

		return nil
	}(); err != nil {
		s.close()
		return NewShardError(s.id, err)
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
	if s.engine == nil {
		return nil
	}

	err := s.engine.Close()
	if err == nil {
		s.engine = nil
	}
	return err
}

// closed determines if the Shard is closed.
func (s *Shard) closed() bool {
	s.mu.RLock()
	closed := s.engine == nil
	s.mu.RUnlock()
	return closed
}

// DiskSize returns the size on disk of this shard
func (s *Shard) DiskSize() (int64, error) {
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
	m := s.engine.MeasurementFields(measurementName)
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
	if s.closed() {
		return ErrEngineClosed
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	s.statMap.Add(statWriteReq, 1)

	fieldsToCreate, err := s.validateSeriesAndFields(points)
	if err != nil {
		return err
	}
	s.statMap.Add(statFieldsCreate, int64(len(fieldsToCreate)))

	// add any new fields and keep track of what needs to be saved
	if err := s.createFieldsAndMeasurements(fieldsToCreate); err != nil {
		return err
	}

	// Write to the engine.
	if err := s.engine.WritePoints(points); err != nil {
		s.statMap.Add(statWritePointsFail, 1)
		return fmt.Errorf("engine: %s", err)
	}
	s.statMap.Add(statWritePointsOK, int64(len(points)))

	return nil
}

// DeleteSeries deletes a list of series.
func (s *Shard) DeleteSeries(seriesKeys []string) error {
	if s.closed() {
		return ErrEngineClosed
	}
	return s.engine.DeleteSeries(seriesKeys)
}

// DeleteMeasurement deletes a measurement and all underlying series.
func (s *Shard) DeleteMeasurement(name string, seriesKeys []string) error {
	if s.closed() {
		return ErrEngineClosed
	}

	if err := s.engine.DeleteMeasurement(name, seriesKeys); err != nil {
		return err
	}

	return nil
}

func (s *Shard) createFieldsAndMeasurements(fieldsToCreate []*FieldCreate) error {
	if len(fieldsToCreate) == 0 {
		return nil
	}

	// add fields
	for _, f := range fieldsToCreate {
		m := s.engine.MeasurementFields(f.Measurement)

		// Add the field to the in memory index
		if err := m.CreateFieldIfNotExists(f.Field.Name, f.Field.Type, false); err != nil {
			return err
		}

		// ensure the measurement is in the index and the field is there
		measurement := s.index.CreateMeasurementIndexIfNotExists(f.Measurement)
		measurement.SetFieldName(f.Field.Name)
	}

	return nil
}

// validateSeriesAndFields checks which series and fields are new and whose metadata should be saved and indexed
func (s *Shard) validateSeriesAndFields(points []models.Point) ([]*FieldCreate, error) {
	var fieldsToCreate []*FieldCreate

	// get the shard mutex for locally defined fields
	for _, p := range points {
		// see if the series should be added to the index
		ss := s.index.Series(string(p.Key()))
		if ss == nil {
			ss = NewSeries(string(p.Key()), p.Tags())
			s.statMap.Add(statSeriesCreate, 1)
		}

		ss = s.index.CreateSeriesIndexIfNotExists(p.Name(), ss)
		s.index.AssignShard(ss.Key, ss.id)

		// see if the field definitions need to be saved to the shard
		mf := s.engine.MeasurementFields(p.Name())

		if mf == nil {
			for name, value := range p.Fields() {
				fieldsToCreate = append(fieldsToCreate, &FieldCreate{p.Name(), &Field{Name: name, Type: influxql.InspectDataType(value)}})
			}
			continue // skip validation since all fields are new
		}

		// validate field types and encode data
		for name, value := range p.Fields() {
			if f := mf.Field(name); f != nil {
				// Field present in shard metadata, make sure there is no type conflict.
				if f.Type != influxql.InspectDataType(value) {
					return nil, fmt.Errorf("field type conflict: input field \"%s\" on measurement \"%s\" is type %T, already exists as type %s", name, p.Name(), value, f.Type)
				}

				continue // Field is present, and it's of the same type. Nothing more to do.
			}

			fieldsToCreate = append(fieldsToCreate, &FieldCreate{p.Name(), &Field{Name: name, Type: influxql.InspectDataType(value)}})
		}
	}

	return fieldsToCreate, nil
}

// SeriesCount returns the number of series buckets on the shard.
func (s *Shard) SeriesCount() (int, error) {
	if s.closed() {
		return 0, ErrEngineClosed
	}
	return s.engine.SeriesCount()
}

// WriteTo writes the shard's data to w.
func (s *Shard) WriteTo(w io.Writer) (int64, error) {
	if s.closed() {
		return 0, ErrEngineClosed
	}
	n, err := s.engine.WriteTo(w)
	s.statMap.Add(statWriteBytes, int64(n))
	return n, err
}

// CreateIterator returns an iterator for the data in the shard.
func (s *Shard) CreateIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {
	if s.closed() {
		return nil, ErrEngineClosed
	}

	if influxql.Sources(opt.Sources).HasSystemSource() {
		return s.createSystemIterator(opt)
	}
	return s.engine.CreateIterator(opt)
}

// createSystemIterator returns an iterator for a system source.
func (s *Shard) createSystemIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {
	// Only support a single system source.
	if len(opt.Sources) > 1 {
		return nil, errors.New("cannot select from multiple system sources")
	}

	m := opt.Sources[0].(*influxql.Measurement)
	switch m.Name {
	case "_fieldKeys":
		return NewFieldKeysIterator(s, opt)
	case "_measurements":
		return NewMeasurementIterator(s, opt)
	case "_series":
		return NewSeriesIterator(s, opt)
	case "_tagKeys":
		return NewTagKeysIterator(s, opt)
	case "_tags":
		return NewTagValuesIterator(s, opt)
	default:
		return nil, fmt.Errorf("unknown system source: %s", m.Name)
	}
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
	if s.closed() {
		return nil, ErrEngineClosed
	}

	if influxql.Sources(opt.Sources).HasSystemSource() {
		// Only support a single system source.
		if len(opt.Sources) > 1 {
			return nil, errors.New("cannot select from multiple system sources")
		}

		// Meta queries don't need to know the series name and
		// always have a single series of strings.
		auxFields := make([]influxql.DataType, len(opt.Aux))
		for i := range auxFields {
			auxFields[i] = influxql.String
		}
		return []influxql.Series{{Aux: auxFields}}, nil
	}

	return s.engine.SeriesKeys(opt)
}

// ExpandSources expands regex sources and removes duplicates.
// NOTE: sources must be normalized (db and rp set) before calling this function.
func (s *Shard) ExpandSources(sources influxql.Sources) (influxql.Sources, error) {
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
			for _, m := range s.index.MeasurementsByRegex(src.Regex.Val) {
				other := &influxql.Measurement{
					Database:        src.Database,
					RetentionPolicy: src.RetentionPolicy,
					Name:            m.Name,
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

// Shards represents a sortable list of shards.
type Shards []*Shard

func (a Shards) Len() int           { return len(a) }
func (a Shards) Less(i, j int) bool { return a[i].id < a[j].id }
func (a Shards) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// MeasurementFields holds the fields of a measurement and their codec.
type MeasurementFields struct {
	mu sync.RWMutex

	fields map[string]*Field `json:"fields"`
	Codec  *FieldCodec
}

func NewMeasurementFields() *MeasurementFields {
	return &MeasurementFields{fields: make(map[string]*Field)}
}

// MarshalBinary encodes the object to a binary format.
func (m *MeasurementFields) MarshalBinary() ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var pb internal.MeasurementFields
	for _, f := range m.fields {
		id := int32(f.ID)
		name := f.Name
		t := int32(f.Type)
		pb.Fields = append(pb.Fields, &internal.Field{ID: &id, Name: &name, Type: &t})
	}
	return proto.Marshal(&pb)
}

// UnmarshalBinary decodes the object from a binary format.
func (m *MeasurementFields) UnmarshalBinary(buf []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var pb internal.MeasurementFields
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}
	m.fields = make(map[string]*Field, len(pb.Fields))
	for _, f := range pb.Fields {
		m.fields[f.GetName()] = &Field{ID: uint8(f.GetID()), Name: f.GetName(), Type: influxql.DataType(f.GetType())}
	}
	return nil
}

// CreateFieldIfNotExists creates a new field with an autoincrementing ID.
// Returns an error if 255 fields have already been created on the measurement or
// the fields already exists with a different type.
func (m *MeasurementFields) CreateFieldIfNotExists(name string, typ influxql.DataType, limitCount bool) error {
	m.mu.RLock()

	// Ignore if the field already exists.
	if f := m.fields[name]; f != nil {
		if f.Type != typ {
			m.mu.RUnlock()
			return ErrFieldTypeConflict
		}
		m.mu.RUnlock()
		return nil
	}
	m.mu.RUnlock()

	m.mu.Lock()
	if f := m.fields[name]; f != nil {
		return nil
	}

	// Create and append a new field.
	f := &Field{
		ID:   uint8(len(m.fields) + 1),
		Name: name,
		Type: typ,
	}
	m.fields[name] = f
	m.Codec = NewFieldCodec(m.fields)
	m.mu.Unlock()

	return nil
}

func (m *MeasurementFields) Field(name string) *Field {
	m.mu.RLock()
	f := m.fields[name]
	m.mu.RUnlock()
	return f
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

// shardIteratorCreator creates iterators for a local shard.
// This simply wraps the shard so that Close() does not close the underlying shard.
type shardIteratorCreator struct {
	sh *Shard
}

func (ic *shardIteratorCreator) Close() error { return nil }

func (ic *shardIteratorCreator) CreateIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {
	return ic.sh.CreateIterator(opt)
}
func (ic *shardIteratorCreator) FieldDimensions(sources influxql.Sources) (fields, dimensions map[string]struct{}, err error) {
	return ic.sh.FieldDimensions(sources)
}
func (ic *shardIteratorCreator) SeriesKeys(opt influxql.IteratorOptions) (influxql.SeriesList, error) {
	return ic.sh.SeriesKeys(opt)
}
func (ic *shardIteratorCreator) ExpandSources(sources influxql.Sources) (influxql.Sources, error) {
	return ic.sh.ExpandSources(sources)
}

func NewFieldKeysIterator(sh *Shard, opt influxql.IteratorOptions) (influxql.Iterator, error) {
	fn := func(m *Measurement) []string {
		keys := m.FieldNames()
		sort.Strings(keys)
		return keys
	}
	return newMeasurementKeysIterator(sh, fn, opt)
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
		mms, _, err := sh.index.measurementsByExpr(opt.Condition)
		if err != nil {
			return nil, err
		}
		itr.mms = mms
	}

	// Sort measurements by name.
	sort.Sort(itr.mms)

	return itr, nil
}

// Stats returns stats about the points processed.
func (itr *MeasurementIterator) Stats() influxql.IteratorStats { return influxql.IteratorStats{} }

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

// seriesIterator emits series ids.
type seriesIterator struct {
	keys   []string // remaining series
	fields []string // fields to emit (key)
}

// NewSeriesIterator returns a new instance of SeriesIterator.
func NewSeriesIterator(sh *Shard, opt influxql.IteratorOptions) (influxql.Iterator, error) {
	// Retrieve a list of all measurements.
	mms := sh.index.Measurements()
	sort.Sort(mms)

	// Only equality operators are allowed.
	var err error
	influxql.WalkFunc(opt.Condition, func(n influxql.Node) {
		switch n := n.(type) {
		case *influxql.BinaryExpr:
			switch n.Op {
			case influxql.EQ, influxql.NEQ, influxql.EQREGEX, influxql.NEQREGEX,
				influxql.OR, influxql.AND:
			default:
				err = errors.New("invalid tag comparison operator")
			}
		}
	})
	if err != nil {
		return nil, err
	}

	// Generate a list of all series keys.
	keys := newStringSet()
	for _, mm := range mms {
		ids, err := mm.seriesIDsAllOrByExpr(opt.Condition)
		if err != nil {
			return nil, err
		}

		for _, id := range ids {
			keys.add(mm.SeriesByID(id).Key)
		}
	}

	return &seriesIterator{
		keys:   keys.list(),
		fields: opt.Aux,
	}, nil
}

// Stats returns stats about the points processed.
func (itr *seriesIterator) Stats() influxql.IteratorStats { return influxql.IteratorStats{} }

// Close closes the iterator.
func (itr *seriesIterator) Close() error { return nil }

// Next emits the next point in the iterator.
func (itr *seriesIterator) Next() *influxql.FloatPoint {
	// If there are no more keys then return nil.
	if len(itr.keys) == 0 {
		return nil
	}

	// Prepare auxiliary fields.
	aux := make([]interface{}, len(itr.fields))
	for i, f := range itr.fields {
		switch f {
		case "key":
			aux[i] = itr.keys[0]
		}
	}

	// Return next key.
	p := &influxql.FloatPoint{
		Aux: aux,
	}
	itr.keys = itr.keys[1:]

	return p
}

// NewTagKeysIterator returns a new instance of TagKeysIterator.
func NewTagKeysIterator(sh *Shard, opt influxql.IteratorOptions) (influxql.Iterator, error) {
	fn := func(m *Measurement) []string {
		return m.TagKeys()
	}
	return newMeasurementKeysIterator(sh, fn, opt)
}

// tagValuesIterator emits key/tag values
type tagValuesIterator struct {
	series []*Series // remaining series
	keys   []string  // tag keys to select from a series
	fields []string  // fields to emit (key or value)
	buf    struct {
		s    *Series  // current series
		keys []string // current tag's keys
	}
}

// NewTagValuesIterator returns a new instance of TagValuesIterator.
func NewTagValuesIterator(sh *Shard, opt influxql.IteratorOptions) (influxql.Iterator, error) {
	if opt.Condition == nil {
		return nil, errors.New("a condition is required")
	}

	mms, ok, err := sh.index.measurementsByExpr(opt.Condition)
	if err != nil {
		return nil, err
	} else if !ok {
		mms = sh.index.Measurements()
		sort.Sort(mms)
	}

	filterExpr := influxql.CloneExpr(opt.Condition)
	filterExpr = influxql.RewriteExpr(filterExpr, func(e influxql.Expr) influxql.Expr {
		switch e := e.(type) {
		case *influxql.BinaryExpr:
			switch e.Op {
			case influxql.EQ, influxql.NEQ, influxql.EQREGEX, influxql.NEQREGEX:
				tag, ok := e.LHS.(*influxql.VarRef)
				if !ok || tag.Val == "name" || strings.HasPrefix(tag.Val, "_") {
					return nil
				}
			}
		}
		return e
	})

	var series []*Series
	keys := newStringSet()
	for _, mm := range mms {
		ss, ok, err := mm.tagKeysByExpr(opt.Condition)
		if err != nil {
			return nil, err
		} else if !ok {
			keys.add(mm.TagKeys()...)
		} else {
			keys = keys.union(ss)
		}

		ids, err := mm.seriesIDsAllOrByExpr(filterExpr)
		if err != nil {
			return nil, err
		}

		for _, id := range ids {
			series = append(series, mm.SeriesByID(id))
		}
	}

	return &tagValuesIterator{
		series: series,
		keys:   keys.list(),
		fields: opt.Aux,
	}, nil
}

// Stats returns stats about the points processed.
func (itr *tagValuesIterator) Stats() influxql.IteratorStats { return influxql.IteratorStats{} }

// Close closes the iterator.
func (itr *tagValuesIterator) Close() error { return nil }

// Next emits the next point in the iterator.
func (itr *tagValuesIterator) Next() *influxql.FloatPoint {
	for {
		// If there are no more values then move to the next key.
		if len(itr.buf.keys) == 0 {
			if len(itr.series) == 0 {
				return nil
			}

			itr.buf.s = itr.series[0]
			itr.buf.keys = itr.keys
			itr.series = itr.series[1:]
			continue
		}

		key := itr.buf.keys[0]
		value, ok := itr.buf.s.Tags[key]
		if !ok {
			itr.buf.keys = itr.buf.keys[1:]
			continue
		}

		// Prepare auxiliary fields.
		auxFields := make([]interface{}, len(itr.fields))
		for i, f := range itr.fields {
			switch f {
			case "_tagKey":
				auxFields[i] = key
			case "value":
				auxFields[i] = value
			}
		}

		// Return next key.
		p := &influxql.FloatPoint{
			Name: itr.buf.s.measurement.Name,
			Aux:  auxFields,
		}
		itr.buf.keys = itr.buf.keys[1:]

		return p
	}
}

// measurementKeyFunc is the function called by measurementKeysIterator.
type measurementKeyFunc func(m *Measurement) []string

func newMeasurementKeysIterator(sh *Shard, fn measurementKeyFunc, opt influxql.IteratorOptions) (*measurementKeysIterator, error) {
	itr := &measurementKeysIterator{fn: fn}

	// Retrieve measurements from shard. Filter if condition specified.
	if opt.Condition == nil {
		itr.mms = sh.index.Measurements()
	} else {
		mms, _, err := sh.index.measurementsByExpr(opt.Condition)
		if err != nil {
			return nil, err
		}
		itr.mms = mms
	}

	// Sort measurements by name.
	sort.Sort(itr.mms)

	return itr, nil
}

// measurementKeysIterator iterates over measurements and gets keys from each measurement.
type measurementKeysIterator struct {
	mms Measurements // remaining measurements
	buf struct {
		mm   *Measurement // current measurement
		keys []string     // current measurement's keys
	}
	fn measurementKeyFunc
}

// Stats returns stats about the points processed.
func (itr *measurementKeysIterator) Stats() influxql.IteratorStats { return influxql.IteratorStats{} }

// Close closes the iterator.
func (itr *measurementKeysIterator) Close() error { return nil }

// Next emits the next tag key name.
func (itr *measurementKeysIterator) Next() *influxql.FloatPoint {
	for {
		// If there are no more keys then move to the next measurements.
		if len(itr.buf.keys) == 0 {
			if len(itr.mms) == 0 {
				return nil
			}

			itr.buf.mm = itr.mms[0]
			itr.buf.keys = itr.fn(itr.buf.mm)
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
