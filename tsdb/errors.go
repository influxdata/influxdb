package tsdb

import (
	"errors"
	"fmt"
)

var (
	// ErrFieldTypeConflict is returned when a new field already exists with a different type.
	ErrFieldTypeConflict = errors.New("field type conflict")

	// ErrUnknownFieldType is returned when the type of a field cannot be determined.
	ErrUnknownFieldType = errors.New("unknown field type")
)

// PartialWriteError indicates a write request could only write a portion of the
// requested values.
type PartialWriteError struct {
	Reason  string
	Dropped int

	// A sorted slice of series keys that were dropped.
	DroppedKeys [][]byte
}

func (e PartialWriteError) Error() string {
	return fmt.Sprintf("partial write: %s dropped=%d", e.Reason, e.Dropped)
}
