package tsdb

import (
	"errors"
	"fmt"
)

var (
	// ErrFieldTypeConflict is returned when a new field already exists with a different type.
	ErrFieldTypeConflict = errors.New("field type conflict")

	// ErrShardDisabled is returned when a the shard is not available for
	// queries or writes.
	ErrShardDisabled = errors.New("shard is disabled")

	// ErrUnknownFieldType is returned when the type of a field cannot be determined.
	ErrUnknownFieldType = errors.New("unknown field type")
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

// Error returns the string representation of the error, to satisfy the error interface.
func (e ShardError) Error() string {
	return fmt.Sprintf("[shard %d] %s", e.id, e.Err)
}

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
