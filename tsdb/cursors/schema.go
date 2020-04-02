package cursors

// FieldType represents the primitive field data types available in tsm.
type FieldType int

const (
	Float    FieldType = iota // means the data type is a float
	Integer                   // means the data type is an integer
	Unsigned                  // means the data type is an unsigned integer
	Boolean                   // means the data type is a boolean
	String                    // means the data type is a string of text
)

type MeasurementField struct {
	Key  string
	Type FieldType
}

type MeasurementFields struct {
	Fields []MeasurementField
}

type MeasurementFieldsCursor interface {
	// Next advances the MeasurementFieldsCursor to the next value. It returns false
	// when there are no more values.
	Next() bool

	// Value returns the current value.
	Value() MeasurementFields

	Stats() CursorStats
}
