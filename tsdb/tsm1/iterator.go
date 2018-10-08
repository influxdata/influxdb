package tsm1

import (
	"fmt"

	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/platform/tsdb"
	"go.uber.org/zap"
)

// literalValueCursor represents a cursor that always returns a single value.
// It doesn't not have a time value so it can only be used with nextAt().
type literalValueCursor struct {
	value interface{}
}

func (c *literalValueCursor) close() error                   { return nil }
func (c *literalValueCursor) peek() (t int64, v interface{}) { return tsdb.EOF, c.value }
func (c *literalValueCursor) next() (t int64, v interface{}) { return tsdb.EOF, c.value }
func (c *literalValueCursor) nextAt(seek int64) interface{}  { return c.value }

type cursorsAt []cursorAt

func (c cursorsAt) close() {
	for _, cur := range c {
		cur.close()
	}
}

// newFinalizerIterator creates a new iterator that installs a runtime finalizer
// to ensure close is eventually called if the iterator is garbage collected.
// This additional guard attempts to protect against clients of CreateIterator not
// correctly closing them and leaking cursors.
func newFinalizerIterator(itr query.Iterator, log *zap.Logger) query.Iterator {
	if itr == nil {
		return nil
	}

	switch inner := itr.(type) {
	case query.FloatIterator:
		return newFloatFinalizerIterator(inner, log)
	case query.IntegerIterator:
		return newIntegerFinalizerIterator(inner, log)
	case query.UnsignedIterator:
		return newUnsignedFinalizerIterator(inner, log)
	case query.StringIterator:
		return newStringFinalizerIterator(inner, log)
	case query.BooleanIterator:
		return newBooleanFinalizerIterator(inner, log)
	default:
		panic(fmt.Sprintf("unsupported finalizer iterator type: %T", itr))
	}
}
