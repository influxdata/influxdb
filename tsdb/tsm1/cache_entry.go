package tsm1

import (
	"sync"

	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
)

// entry is a set of values and some metadata.
type entry struct {
	mu     sync.RWMutex
	values Values // All stored values.

	// The type of values stored. Read only so doesn't need to be protected by mu.
	vtype byte
}

// newEntryValues returns a new instance of entry with the given values.  If the
// values are not valid, an error is returned.
func newEntryValues(values []Value) (*entry, error) {
	e := &entry{}
	e.values = make(Values, 0, len(values))
	e.values = append(e.values, values...)

	// No values, don't check types and ordering
	if len(values) == 0 {
		return e, nil
	}

	et := valueType(values[0])
	for _, v := range values {
		// Make sure all the values are the same type
		if et != valueType(v) {
			return nil, tsdb.ErrFieldTypeConflict
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
				return tsdb.ErrFieldTypeConflict
			}
		}
	}

	// entry currently has no values, so add the new ones and we're done.
	e.mu.Lock()
	if len(e.values) == 0 {
		e.values = values
		e.vtype = valueType(values[0])
		e.mu.Unlock()
		return nil
	}

	// Append the new values to the existing ones...
	e.values = append(e.values, values...)
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
}

// count returns the number of values in this entry.
func (e *entry) count() int {
	e.mu.RLock()
	n := len(e.values)
	e.mu.RUnlock()
	return n
}

// filter removes all values with timestamps between min and max inclusive.
func (e *entry) filter(min, max int64) {
	e.mu.Lock()
	if len(e.values) > 1 {
		e.values = e.values.Deduplicate()
	}
	e.values = e.values.Exclude(min, max)
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
