package query

import (
	"fmt"
)

// Emitter groups values together by name, tags, and time.
type Emitter struct {
	buf       []Point
	itrs      []Iterator
	ascending bool
}

// NewEmitter returns a new instance of Emitter that pulls from itrs.
func NewEmitter(itrs []Iterator, ascending bool) *Emitter {
	return &Emitter{
		buf:       make([]Point, len(itrs)),
		itrs:      itrs,
		ascending: ascending,
	}
}

// Close closes the underlying iterators.
func (e *Emitter) Close() error {
	return Iterators(e.itrs).Close()
}

// LoadBuf reads in points into empty buffer slots.
// Returns the next time/name/tags to emit for.
func (e *Emitter) LoadBuf() (t int64, name string, tags Tags, err error) {
	t = ZeroTime

	for i := range e.itrs {
		// Load buffer, if empty.
		if e.buf[i] == nil {
			e.buf[i], err = e.readIterator(e.itrs[i])
			if err != nil {
				break
			}
		}

		// Skip if buffer is empty.
		p := e.buf[i]
		if p == nil {
			continue
		}
		itrTime, itrName, itrTags := p.time(), p.name(), p.tags()

		// Initialize range values if not set.
		if t == ZeroTime {
			t, name, tags = itrTime, itrName, itrTags
			continue
		}

		// Update range values if lower and emitter is in time ascending order.
		if e.ascending {
			if (itrName < name) || (itrName == name && itrTags.ID() < tags.ID()) || (itrName == name && itrTags.ID() == tags.ID() && itrTime < t) {
				t, name, tags = itrTime, itrName, itrTags
			}
			continue
		}

		// Update range values if higher and emitter is in time descending order.
		if (itrName > name) || (itrName == name && itrTags.ID() > tags.ID()) || (itrName == name && itrTags.ID() == tags.ID() && itrTime > t) {
			t, name, tags = itrTime, itrName, itrTags
		}
	}
	return
}

// ReadInto reads the values at time, name, and tags into values.
func (e *Emitter) ReadInto(t int64, name string, tags Tags, values []interface{}) {
	for i, p := range e.buf {
		// Skip if buffer is empty.
		if p == nil {
			values[i] = nil
			continue
		}

		// Skip point if it doesn't match time/name/tags.
		pTags := p.tags()
		if p.time() != t || p.name() != name || !pTags.Equals(&tags) {
			values[i] = nil
			continue
		}

		// Read point value.
		values[i] = p.value()

		// Clear buffer.
		e.buf[i] = nil
	}
}

// readIterator reads the next point from itr.
func (e *Emitter) readIterator(itr Iterator) (Point, error) {
	if itr == nil {
		return nil, nil
	}

	switch itr := itr.(type) {
	case FloatIterator:
		if p, err := itr.Next(); err != nil {
			return nil, err
		} else if p != nil {
			return p, nil
		}
	case IntegerIterator:
		if p, err := itr.Next(); err != nil {
			return nil, err
		} else if p != nil {
			return p, nil
		}
	case UnsignedIterator:
		if p, err := itr.Next(); err != nil {
			return nil, err
		} else if p != nil {
			return p, nil
		}
	case StringIterator:
		if p, err := itr.Next(); err != nil {
			return nil, err
		} else if p != nil {
			return p, nil
		}
	case BooleanIterator:
		if p, err := itr.Next(); err != nil {
			return nil, err
		} else if p != nil {
			return p, nil
		}
	default:
		panic(fmt.Sprintf("unsupported iterator: %T", itr))
	}
	return nil, nil
}
