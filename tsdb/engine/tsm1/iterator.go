package tsm1

import (
	"fmt"

	"github.com/influxdata/influxdb/influxql"
	"github.com/uber-go/zap"
)

func newLimitIterator(input influxql.Iterator, opt influxql.IteratorOptions) influxql.Iterator {
	switch input := input.(type) {
	case influxql.FloatIterator:
		return newFloatLimitIterator(input, opt)
	case influxql.IntegerIterator:
		return newIntegerLimitIterator(input, opt)
	case influxql.StringIterator:
		return newStringLimitIterator(input, opt)
	case influxql.BooleanIterator:
		return newBooleanLimitIterator(input, opt)
	default:
		panic(fmt.Sprintf("unsupported limit iterator type: %T", input))
	}
}

type floatCastIntegerCursor struct {
	cursor integerCursor
}

func (c *floatCastIntegerCursor) close() error { return c.cursor.close() }

func (c *floatCastIntegerCursor) next() (t int64, v interface{}) { return c.nextFloat() }

func (c *floatCastIntegerCursor) nextFloat() (int64, float64) {
	t, v := c.cursor.nextInteger()
	return t, float64(v)
}

type integerCastFloatCursor struct {
	cursor floatCursor
}

func (c *integerCastFloatCursor) close() error { return c.cursor.close() }

func (c *integerCastFloatCursor) next() (t int64, v interface{}) { return c.nextInteger() }

func (c *integerCastFloatCursor) nextInteger() (int64, int64) {
	t, v := c.cursor.nextFloat()
	return t, int64(v)
}

type cursorsAt []cursorAt

func (c cursorsAt) close() {
	for _, cur := range c {
		cur.close()
	}
}

// newMergeFinalizerIterator creates a new Merge iterator from the inputs. If the call to Merge succeeds,
// the resulting Iterator will be wrapped in a finalizer iterator.
// If Merge returns an error, the inputs will be closed.
func newMergeFinalizerIterator(inputs []influxql.Iterator, opt influxql.IteratorOptions, log zap.Logger) (influxql.Iterator, error) {
	itr, err := influxql.Iterators(inputs).Merge(opt)
	if err != nil {
		influxql.Iterators(inputs).Close()
		return nil, err
	}
	return newFinalizerIterator(itr, log), nil
}

// newFinalizerIterator creates a new iterator that installs a runtime finalizer
// to ensure close is eventually called if the iterator is garbage collected.
// This additional guard attempts to protect against clients of CreateIterator not
// correctly closing them and leaking cursors.
func newFinalizerIterator(itr influxql.Iterator, log zap.Logger) influxql.Iterator {
	if itr == nil {
		return nil
	}

	switch inner := itr.(type) {
	case influxql.FloatIterator:
		return newFloatFinalizerIterator(inner, log)
	case influxql.IntegerIterator:
		return newIntegerFinalizerIterator(inner, log)
	case influxql.StringIterator:
		return newStringFinalizerIterator(inner, log)
	case influxql.BooleanIterator:
		return newBooleanFinalizerIterator(inner, log)
	default:
		panic(fmt.Sprintf("unsupported finalizer iterator type: %T", itr))
	}
}
