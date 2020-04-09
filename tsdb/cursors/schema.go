package cursors

// FieldType represents the primitive field data types available in tsm.
type FieldType int

const (
	Float     FieldType = iota // means the data type is a float
	Integer                    // means the data type is an integer
	Unsigned                   // means the data type is an unsigned integer
	Boolean                    // means the data type is a boolean
	String                     // means the data type is a string of text
	Undefined                  // means the data type in unknown or undefined
)

type MeasurementField struct {
	Key  string
	Type FieldType
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
