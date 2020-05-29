package tsdb

import (
	"fmt"
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
