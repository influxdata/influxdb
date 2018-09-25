package tsdb

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"unsafe"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/influxdb/pkg/file"
	"github.com/influxdata/influxql"
	"github.com/influxdata/platform/tsdb/internal"
)

//
// this file contains stuff related to measurement fields, including some methods on the shard
// it is localized to this one file so that it's easy to remove later.
//

// FieldCreate holds information for a field to create on a measurement.
type FieldCreate struct {
	Measurement []byte
	Field       *Field
}

// validateSeriesAndFields checks which series and fields are new and whose metadata should be saved and indexed.
func (s *Shard) validateSeriesAndFields(engine Engine, collection *SeriesCollection) ([]*FieldCreate, error) {
	var fieldsToCreate []*FieldCreate

	// Add new series. Check for partial writes.
	if err := engine.CreateSeriesListIfNotExists(collection); err != nil {
		// ignore PartialWriteErrors. The collection captures it.
		if _, ok := err.(PartialWriteError); !ok {
			return nil, err
		}
	}

	// Create a MeasurementFields cache.
	mfCache := make(map[string]*MeasurementFields, 16)
	j := 0
	for iter := collection.Iterator(); iter.Next(); {
		// Skip any points with only invalid fields.
		point := iter.Point()

		fieldIter := point.FieldIterator()
		validField := false
		for fieldIter.Next() {
			if bytes.Equal(fieldIter.FieldKey(), timeBytes) {
				continue
			}
			validField = true
			break
		}

		if !validField {
			if collection.Reason == "" {
				collection.Reason = fmt.Sprintf(
					"invalid field name: input field %q on measurement %q is invalid",
					timeBytes, iter.Name())
			}
			collection.Dropped++
			collection.DroppedKeys = append(collection.DroppedKeys, iter.Key())
			continue
		}

		// Grab the MeasurementFields checking the local cache to avoid lock contention.
		name := iter.Name()
		mf := mfCache[string(name)]
		if mf == nil {
			mf = engine.MeasurementFields(name).Clone()
			mfCache[string(name)] = mf
		}

		// Check with the field validator.
		if err := s.options.FieldValidator.Validate(mf, point); err != nil {
			switch err := err.(type) { // combine in any partial write error
			case PartialWriteError:
				if collection.Reason == "" {
					collection.Reason = err.Reason
				}
				collection.Dropped += uint64(err.Dropped)
				collection.DroppedKeys = append(collection.DroppedKeys, err.DroppedKeys...)
			default:
				return nil, err
			}
			continue
		}

		collection.Copy(j, iter.Index())
		j++

		// Create any fields that are missing.
		fieldIter.Reset()
		for fieldIter.Next() {
			fieldKey := fieldIter.FieldKey()

			// Skip fields named "time". They are illegal.
			if bytes.Equal(fieldKey, timeBytes) {
				continue
			}

			if mf.FieldBytes(fieldKey) != nil {
				continue
			}

			dataType := dataTypeFromModelsFieldType(fieldIter.Type())
			if dataType == influxql.Unknown {
				continue
			}

			fieldsToCreate = append(fieldsToCreate, &FieldCreate{
				Measurement: name,
				Field: &Field{
					Name: string(fieldKey),
					Type: dataType,
				},
			})
		}
	}

	collection.Truncate(j)
	return fieldsToCreate, nil
}

func (s *Shard) createFieldsAndMeasurements(engine Engine, fieldsToCreate []*FieldCreate) error {
	if len(fieldsToCreate) == 0 {
		return nil
	}

	// add fields
	for _, f := range fieldsToCreate {
		mf := engine.MeasurementFields(f.Measurement)
		if err := mf.CreateFieldIfNotExists([]byte(f.Field.Name), f.Field.Type); err != nil {
			return err
		}

		s.index.SetFieldName(f.Measurement, f.Field.Name)
	}

	if len(fieldsToCreate) > 0 {
		return engine.MeasurementFieldSet().Save()
	}

	return nil
}

// MeasurementFields holds the fields of a measurement and their codec.
type MeasurementFields struct {
	mu sync.RWMutex

	fields map[string]*Field
}

// NewMeasurementFields returns an initialised *MeasurementFields value.
func NewMeasurementFields() *MeasurementFields {
	return &MeasurementFields{fields: make(map[string]*Field)}
}

func (m *MeasurementFields) FieldKeys() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	a := make([]string, 0, len(m.fields))
	for key := range m.fields {
		a = append(a, key)
	}
	sort.Strings(a)
	return a
}

// bytes estimates the memory footprint of this MeasurementFields, in bytes.
func (m *MeasurementFields) bytes() int {
	var b int
	m.mu.RLock()
	b += 24 // mu RWMutex is 24 bytes
	b += int(unsafe.Sizeof(m.fields))
	for k, v := range m.fields {
		b += int(unsafe.Sizeof(k)) + len(k)
		b += int(unsafe.Sizeof(v)+unsafe.Sizeof(*v)) + len(v.Name)
	}
	m.mu.RUnlock()
	return b
}

// CreateFieldIfNotExists creates a new field with an autoincrementing ID.
// Returns an error if 255 fields have already been created on the measurement or
// the fields already exists with a different type.
func (m *MeasurementFields) CreateFieldIfNotExists(name []byte, typ influxql.DataType) error {
	m.mu.RLock()

	// Ignore if the field already exists.
	if f := m.fields[string(name)]; f != nil {
		if f.Type != typ {
			m.mu.RUnlock()
			return ErrFieldTypeConflict
		}
		m.mu.RUnlock()
		return nil
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	// Re-check field and type under write lock.
	if f := m.fields[string(name)]; f != nil {
		if f.Type != typ {
			return ErrFieldTypeConflict
		}
		return nil
	}

	// Create and append a new field.
	f := &Field{
		ID:   uint8(len(m.fields) + 1),
		Name: string(name),
		Type: typ,
	}
	m.fields[string(name)] = f

	return nil
}

func (m *MeasurementFields) FieldN() int {
	m.mu.RLock()
	n := len(m.fields)
	m.mu.RUnlock()
	return n
}

// Field returns the field for name, or nil if there is no field for name.
func (m *MeasurementFields) Field(name string) *Field {
	m.mu.RLock()
	f := m.fields[name]
	m.mu.RUnlock()
	return f
}

func (m *MeasurementFields) HasField(name string) bool {
	if m == nil {
		return false
	}
	m.mu.RLock()
	f := m.fields[name]
	m.mu.RUnlock()
	return f != nil
}

// FieldBytes returns the field for name, or nil if there is no field for name.
// FieldBytes should be preferred to Field when the caller has a []byte, because
// it avoids a string allocation, which can't be avoided if the caller converts
// the []byte to a string and calls Field.
func (m *MeasurementFields) FieldBytes(name []byte) *Field {
	m.mu.RLock()
	f := m.fields[string(name)]
	m.mu.RUnlock()
	return f
}

// FieldSet returns the set of fields and their types for the measurement.
func (m *MeasurementFields) FieldSet() map[string]influxql.DataType {
	m.mu.RLock()
	defer m.mu.RUnlock()

	fields := make(map[string]influxql.DataType)
	for name, f := range m.fields {
		fields[name] = f.Type
	}
	return fields
}

func (m *MeasurementFields) ForEachField(fn func(name string, typ influxql.DataType) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for name, f := range m.fields {
		if !fn(name, f.Type) {
			return
		}
	}
}

// Clone returns copy of the MeasurementFields
func (m *MeasurementFields) Clone() *MeasurementFields {
	m.mu.RLock()
	defer m.mu.RUnlock()
	fields := make(map[string]*Field, len(m.fields))
	for key, field := range m.fields {
		fields[key] = field
	}
	return &MeasurementFields{
		fields: fields,
	}
}

// MeasurementFieldSet represents a collection of fields by measurement.
// This safe for concurrent use.
type MeasurementFieldSet struct {
	mu     sync.RWMutex
	fields map[string]*MeasurementFields

	// path is the location to persist field sets
	path string
}

// NewMeasurementFieldSet returns a new instance of MeasurementFieldSet.
func NewMeasurementFieldSet(path string) (*MeasurementFieldSet, error) {
	fs := &MeasurementFieldSet{
		fields: make(map[string]*MeasurementFields),
		path:   path,
	}

	// If there is a load error, return the error and an empty set so
	// it can be rebuild manually.
	return fs, fs.load()
}

// Bytes estimates the memory footprint of this MeasurementFieldSet, in bytes.
func (fs *MeasurementFieldSet) Bytes() int {
	var b int
	fs.mu.RLock()
	b += 24 // mu RWMutex is 24 bytes
	for k, v := range fs.fields {
		b += int(unsafe.Sizeof(k)) + len(k)
		b += int(unsafe.Sizeof(v)) + v.bytes()
	}
	b += int(unsafe.Sizeof(fs.fields))
	b += int(unsafe.Sizeof(fs.path)) + len(fs.path)
	fs.mu.RUnlock()
	return b
}

// Fields returns fields for a measurement by name.
func (fs *MeasurementFieldSet) Fields(name []byte) *MeasurementFields {
	fs.mu.RLock()
	mf := fs.fields[string(name)]
	fs.mu.RUnlock()
	return mf
}

// FieldsByString returns fields for a measurment by name.
func (fs *MeasurementFieldSet) FieldsByString(name string) *MeasurementFields {
	fs.mu.RLock()
	mf := fs.fields[name]
	fs.mu.RUnlock()
	return mf
}

// CreateFieldsIfNotExists returns fields for a measurement by name.
func (fs *MeasurementFieldSet) CreateFieldsIfNotExists(name []byte) *MeasurementFields {
	fs.mu.RLock()
	mf := fs.fields[string(name)]
	fs.mu.RUnlock()

	if mf != nil {
		return mf
	}

	fs.mu.Lock()
	mf = fs.fields[string(name)]
	if mf == nil {
		mf = NewMeasurementFields()
		fs.fields[string(name)] = mf
	}
	fs.mu.Unlock()
	return mf
}

// Delete removes a field set for a measurement.
func (fs *MeasurementFieldSet) Delete(name string) {
	fs.mu.Lock()
	delete(fs.fields, name)
	fs.mu.Unlock()
}

// DeleteWithLock executes fn and removes a field set from a measurement under lock.
func (fs *MeasurementFieldSet) DeleteWithLock(name string, fn func() error) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if err := fn(); err != nil {
		return err
	}

	delete(fs.fields, name)
	return nil
}

func (fs *MeasurementFieldSet) IsEmpty() bool {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return len(fs.fields) == 0
}

func (fs *MeasurementFieldSet) Save() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	return fs.saveNoLock()
}

func (fs *MeasurementFieldSet) saveNoLock() error {
	// No fields left, remove the fields index file
	if len(fs.fields) == 0 {
		return os.RemoveAll(fs.path)
	}

	// Write the new index to a temp file and rename when it's sync'd
	path := fs.path + ".tmp"
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL|os.O_SYNC, 0666)
	if err != nil {
		return err
	}
	defer os.RemoveAll(path)

	if _, err := fd.Write(fieldsIndexMagicNumber); err != nil {
		return err
	}

	pb := internal.MeasurementFieldSet{
		Measurements: make([]*internal.MeasurementFields, 0, len(fs.fields)),
	}
	for name, mf := range fs.fields {
		fs := &internal.MeasurementFields{
			Name:   name,
			Fields: make([]*internal.Field, 0, mf.FieldN()),
		}

		mf.ForEachField(func(field string, typ influxql.DataType) bool {
			fs.Fields = append(fs.Fields, &internal.Field{Name: field, Type: int32(typ)})
			return true
		})

		pb.Measurements = append(pb.Measurements, fs)
	}

	b, err := proto.Marshal(&pb)
	if err != nil {
		return err
	}

	if _, err := fd.Write(b); err != nil {
		return err
	}

	if err = fd.Sync(); err != nil {
		return err
	}

	//close file handle before renaming to support Windows
	if err = fd.Close(); err != nil {
		return err
	}

	if err := file.RenameFile(path, fs.path); err != nil {
		return err
	}

	return file.SyncDir(filepath.Dir(fs.path))
}

func (fs *MeasurementFieldSet) load() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	fd, err := os.Open(fs.path)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	defer fd.Close()

	var magic [4]byte
	if _, err := fd.Read(magic[:]); err != nil {
		return err
	}

	if !bytes.Equal(magic[:], fieldsIndexMagicNumber) {
		return ErrUnknownFieldsFormat
	}

	var pb internal.MeasurementFieldSet
	b, err := ioutil.ReadAll(fd)
	if err != nil {
		return err
	}

	if err := proto.Unmarshal(b, &pb); err != nil {
		return err
	}

	fs.fields = make(map[string]*MeasurementFields, len(pb.GetMeasurements()))
	for _, measurement := range pb.GetMeasurements() {
		set := &MeasurementFields{
			fields: make(map[string]*Field, len(measurement.GetFields())),
		}
		for _, field := range measurement.GetFields() {
			set.fields[field.GetName()] = &Field{Name: field.GetName(), Type: influxql.DataType(field.GetType())}
		}
		fs.fields[measurement.GetName()] = set
	}
	return nil
}

// Field represents a series field.
type Field struct {
	ID   uint8             `json:"id,omitempty"`
	Name string            `json:"name,omitempty"`
	Type influxql.DataType `json:"type,omitempty"`
}
