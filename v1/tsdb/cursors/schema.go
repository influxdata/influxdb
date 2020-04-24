package cursors

import (
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxql"
)

// FieldType represents the primitive field data types available in tsm.
type FieldType int

const (
	Float     FieldType = iota // means the data type is a float
	Integer                    // means the data type is an integer
	Unsigned                   // means the data type is an unsigned integer
	String                     // means the data type is a string of text
	Boolean                    // means the data type is a boolean
	Undefined                  // means the data type in unknown or undefined
)

var (
	fieldTypeToDataTypeMapping = [8]influxql.DataType{
		Float:     influxql.Float,
		Integer:   influxql.Integer,
		Unsigned:  influxql.Unsigned,
		String:    influxql.String,
		Boolean:   influxql.Boolean,
		Undefined: influxql.Unknown,
		6:         influxql.Unknown,
		7:         influxql.Unknown,
	}
)

// FieldTypeToDataType returns the equivalent influxql DataType for the field type ft.
// If ft is an invalid FieldType, the results are undefined.
func FieldTypeToDataType(ft FieldType) influxql.DataType {
	return fieldTypeToDataTypeMapping[ft&7]
}

// IsLower returns true if the other FieldType has greater precedence than the
// current value. Undefined has the lowest precedence.
func (ft FieldType) IsLower(other FieldType) bool { return other < ft }

var (
	modelsFieldTypeToFieldTypeMapping = [8]FieldType{
		models.Integer:  Integer,
		models.Float:    Float,
		models.Boolean:  Boolean,
		models.String:   String,
		models.Empty:    Undefined,
		models.Unsigned: Unsigned,
		6:               Undefined,
		7:               Undefined,
	}
)

// ModelsFieldTypeToFieldType returns the equivalent FieldType for ft.
// If ft is an invalid FieldType, the results are undefined.
func ModelsFieldTypeToFieldType(ft models.FieldType) FieldType {
	return modelsFieldTypeToFieldTypeMapping[ft&7]
}

type MeasurementField struct {
	Key       string    // Key is the name of the field
	Type      FieldType // Type is field type
	Timestamp int64     // Timestamp refers to the maximum timestamp observed for the given field
}

// MeasurementFieldSlice implements sort.Interface and sorts
// the slice from lowest to highest precedence. Use sort.Reverse
// to sort from highest to lowest.
type MeasurementFieldSlice []MeasurementField

func (m MeasurementFieldSlice) Len() int {
	return len(m)
}

func (m MeasurementFieldSlice) Less(i, j int) bool {
	ii, jj := &m[i], &m[j]
	return ii.Key < jj.Key ||
		(ii.Key == jj.Key &&
			(ii.Timestamp < jj.Timestamp ||
				(ii.Timestamp == jj.Timestamp && ii.Type.IsLower(jj.Type))))
}

func (m MeasurementFieldSlice) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}

// UniqueByKey performs an in-place update of m, removing duplicate elements
// by Key, keeping the first occurrence of each. If the slice is not sorted,
// the behavior of UniqueByKey is undefined.
func (m *MeasurementFieldSlice) UniqueByKey() {
	mm := *m
	if len(mm) < 2 {
		return
	}

	j := 0
	for i := 1; i < len(mm); i++ {
		if mm[j].Key != mm[i].Key {
			j++
			if j != i {
				// optimization: skip copy if j == i
				mm[j] = mm[i]
			}
		}
	}

	*m = mm[:j+1]
}

type MeasurementFields struct {
	Fields []MeasurementField
}

type MeasurementFieldsIterator interface {
	// Next advances the iterator to the next value. It returns false
	// when there are no more values.
	Next() bool

	// Value returns the current value.
	Value() MeasurementFields

	Stats() CursorStats
}

// EmptyMeasurementFieldsIterator is an implementation of MeasurementFieldsIterator that returns
// no values.
var EmptyMeasurementFieldsIterator = &measurementFieldsIterator{}

type measurementFieldsIterator struct{}

func (m *measurementFieldsIterator) Next() bool               { return false }
func (m *measurementFieldsIterator) Value() MeasurementFields { return MeasurementFields{} }
func (m *measurementFieldsIterator) Stats() CursorStats       { return CursorStats{} }

type MeasurementFieldsSliceIterator struct {
	f     []MeasurementFields
	v     MeasurementFields
	i     int
	stats CursorStats
}

func NewMeasurementFieldsSliceIterator(f []MeasurementFields) *MeasurementFieldsSliceIterator {
	return &MeasurementFieldsSliceIterator{f: f}
}

func NewMeasurementFieldsSliceIteratorWithStats(f []MeasurementFields, stats CursorStats) *MeasurementFieldsSliceIterator {
	return &MeasurementFieldsSliceIterator{f: f, stats: stats}
}

func (s *MeasurementFieldsSliceIterator) Next() bool {
	if s.i < len(s.f) {
		s.v = s.f[s.i]
		s.i++
		return true
	}
	s.v = MeasurementFields{}
	return false
}

func (s *MeasurementFieldsSliceIterator) Value() MeasurementFields {
	return s.v
}

func (s *MeasurementFieldsSliceIterator) Stats() CursorStats {
	return s.stats
}

func (s *MeasurementFieldsSliceIterator) toSlice() []MeasurementFields {
	if s.i < len(s.f) {
		return s.f[s.i:]
	}
	return nil
}

// MeasurementFieldsIteratorFlatMap reads the remainder of i, flattening the results
// to a single slice.
func MeasurementFieldsIteratorFlatMap(i MeasurementFieldsIterator) []MeasurementField {
	if i == nil {
		return nil
	}

	var res []MeasurementField
	if si, ok := i.(*MeasurementFieldsSliceIterator); ok {
		s := si.toSlice()
		sz := 0
		for i := range s {
			sz += len(s[i].Fields)
		}
		res = make([]MeasurementField, 0, sz)
		for i := range s {
			res = append(res, s[i].Fields...)
		}
	} else {
		for i.Next() {
			res = append(res, i.Value().Fields...)
		}
	}
	return res
}
