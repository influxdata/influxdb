package influxql

import (
	"fmt"
	"math"
	"time"

	"github.com/influxdb/influxdb/models"
)

// Emitter groups values together by name,
type Emitter struct {
	itrs []Iterator

	tags Tags
	row  *models.Row

	// The columns to attach to each row.
	Columns []string
}

// NewEmitter returns a new instance of Emitter that pulls from itrs.
// Iterators are joined together by the emitter.
func NewEmitter(itrs []Iterator) *Emitter {
	return &Emitter{
		itrs: Join(itrs),
	}
}

// Close closes the underlying iterators.
func (e *Emitter) Close() error {
	return Iterators(e.itrs).Close()
}

// Emit returns the next row from the iterators.
func (e *Emitter) Emit() *models.Row {
	// Immediately end emission if there are no iterators.
	if len(e.itrs) == 0 {
		return nil
	}

	// Continually read from iterators until they are exhausted.
	// Top-level iterators will return an equal number of items.
	for {
		// Read next set of values from all iterators.
		// If no values are returned then return row.
		name, tags, values := e.read()
		if values == nil {
			row := e.row
			e.row = nil
			return row
		}

		// If there's no row yet then create one.
		// If the name and tags match the existing row, append to that row.
		// Otherwise return existing row and add values to next emitted row.
		if e.row == nil {
			e.createRow(name, tags, values)
		} else if e.row.Name == name && e.tags.Equals(&tags) {
			e.row.Values = append(e.row.Values, values)
		} else {
			row := e.row
			e.createRow(name, tags, values)
			return row
		}
	}
}

// createRow creates a new row attached to the emitter.
func (e *Emitter) createRow(name string, tags Tags, values []interface{}) {
	e.tags = tags
	e.row = &models.Row{
		Name:    name,
		Tags:    tags.KeyValues(),
		Columns: e.Columns,
		Values:  [][]interface{}{values},
	}
}

// read returns the next slice of values from the iterators.
// Returns nil values once the iterators are exhausted.
func (e *Emitter) read() (name string, tags Tags, values []interface{}) {
	values = make([]interface{}, len(e.itrs)+1)

	for i, itr := range e.itrs {
		// Read iterator's point. Exit if any iterator returns nil.
		p := e.readIterator(itr)
		if p == nil {
			return "", Tags{}, nil
		}

		// Use the first iterator to populate name, tags, and timestamp.
		if i == 0 {
			name, tags = p.name(), p.tags()
			values[0] = time.Unix(0, p.time()).UTC()
		}

		// Set value.
		// Replace non-JSON encodable values with nil.
		switch p := p.(type) {
		case *FloatPoint:
			if math.IsNaN(p.Value) {
				values[i+1] = nil
			} else {
				values[i+1] = p.Value
			}
		default:
			values[i+1] = p.value()
		}
	}

	return
}

// readIterator reads the next point from itr.
func (e *Emitter) readIterator(itr Iterator) Point {
	switch itr := itr.(type) {
	case FloatIterator:
		if p := itr.Next(); p != nil {
			return p
		}
	case StringIterator:
		if p := itr.Next(); p != nil {
			return p
		}
	case BooleanIterator:
		if p := itr.Next(); p != nil {
			return p
		}
	default:
		panic(fmt.Sprintf("unsupported iterator: %T", itr))
	}
	return nil
}
