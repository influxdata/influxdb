package tsm1

import (
	"sync"
	"sync/atomic"

	"github.com/influxdata/influxql"
)

// entry is a set of values and some metadata.
type entry struct {
	// Tracks the number of values in the entry. Must always be accessed via
	// atomic; must be 8b aligned.
	n int64

	mu     sync.RWMutex
	values Values // All stored values.

	// The type of values stored. Read only so doesn't need to be protected by mu.
	vtype byte
}

// newEntryValues returns a new instance of entry with the given values.  If the
// values are not valid, an error is returned.
func newEntryValues(values []Value) (*entry, error) {
	e := &entry{
		values: make(Values, 0, len(values)),
		n:      int64(len(values)),
	}
	e.values = append(e.values, values...)

	// No values, don't check types and ordering
	if len(values) == 0 {
		return e, nil
	}

	et := valueType(values[0])
	for _, v := range values {
		// Make sure all the values are the same type
		if et != valueType(v) {
			return nil, errFieldTypeConflict
		}
	}

	// Set the type of values stored.
	e.vtype = et

	return e, nil
}

// add adds the given values to the entry.
func (e *entry) add(values []Value) error {
	if len(values) == 0 {
		return nil // Nothing to do.
	}

	// Are any of the new values the wrong type?
	if e.vtype != 0 {
		for _, v := range values {
			if e.vtype != valueType(v) {
				return errFieldTypeConflict
			}
		}
	}

	// entry currently has no values, so add the new ones and we're done.
	e.mu.Lock()
	if len(e.values) == 0 {
		e.values = values
		atomic.StoreInt64(&e.n, int64(len(e.values)))
		e.vtype = valueType(values[0])
		e.mu.Unlock()
		return nil
	}

	// Append the new values to the existing ones...
	e.values = append(e.values, values...)
	atomic.StoreInt64(&e.n, int64(len(e.values)))
	e.mu.Unlock()
	return nil
}

// deduplicate sorts and orders the entry's values. If values are already deduped and sorted,
// the function does no work and simply returns.
func (e *entry) deduplicate() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.values) <= 1 {
		return
	}
	e.values = e.values.Deduplicate()
	atomic.StoreInt64(&e.n, int64(len(e.values)))
}

// count returns the number of values in this entry.
func (e *entry) count() int {
	return int(atomic.LoadInt64(&e.n))
}

// filter removes all values with timestamps between min and max inclusive.
func (e *entry) filter(min, max int64) {
	e.mu.Lock()
	if len(e.values) > 1 {
		e.values = e.values.Deduplicate()
	}
	e.values = e.values.Exclude(min, max)
	atomic.StoreInt64(&e.n, int64(len(e.values)))
	e.mu.Unlock()
}

// size returns the size of this entry in bytes.
func (e *entry) size() int {
	e.mu.RLock()
	sz := e.values.Size()
	e.mu.RUnlock()
	return sz
}

// InfluxQLType returns for the entry the data type of its values.
func (e *entry) InfluxQLType() (influxql.DataType, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.values.InfluxQLType()
}

// BlockType returns the data type for the entry as a block type.
func (e *entry) BlockType() byte {
	// This value is mutated on create and does not need to be
	// protected by a mutex.
	return valueTypeToBlockType(e.vtype)
}
