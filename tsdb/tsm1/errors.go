package tsm1

import "errors"

var (
	// errFieldTypeConflict is returned when a new field already exists with a different type.
	errFieldTypeConflict = errors.New("field type conflict")

	// errUnknownFieldType is returned when the type of a field cannot be determined.
	errUnknownFieldType = errors.New("unknown field type")
)
