package tsm1

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/pkg/metrics"
	"github.com/influxdata/influxdb/pkg/tracing"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
	"go.uber.org/zap"
)

func newLimitIterator(input query.Iterator, opt query.IteratorOptions) query.Iterator {
	switch input := input.(type) {
	case query.FloatIterator:
		return newFloatLimitIterator(input, opt)
	case query.IntegerIterator:
		return newIntegerLimitIterator(input, opt)
	case query.UnsignedIterator:
		return newUnsignedLimitIterator(input, opt)
	case query.StringIterator:
		return newStringLimitIterator(input, opt)
	case query.BooleanIterator:
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

type floatCastUnsignedCursor struct {
	cursor unsignedCursor
}

func (c *floatCastUnsignedCursor) close() error { return c.cursor.close() }

func (c *floatCastUnsignedCursor) next() (t int64, v interface{}) { return c.nextFloat() }

func (c *floatCastUnsignedCursor) nextFloat() (int64, float64) {
	t, v := c.cursor.nextUnsigned()
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

type integerCastUnsignedCursor struct {
	cursor unsignedCursor
}

func (c *integerCastUnsignedCursor) close() error { return c.cursor.close() }

func (c *integerCastUnsignedCursor) next() (t int64, v interface{}) { return c.nextInteger() }

func (c *integerCastUnsignedCursor) nextInteger() (int64, int64) {
	t, v := c.cursor.nextUnsigned()
	return t, int64(v)
}

type unsignedCastFloatCursor struct {
	cursor floatCursor
}

func (c *unsignedCastFloatCursor) close() error { return c.cursor.close() }

func (c *unsignedCastFloatCursor) next() (t int64, v interface{}) { return c.nextUnsigned() }

func (c *unsignedCastFloatCursor) nextUnsigned() (int64, uint64) {
	t, v := c.cursor.nextFloat()
	return t, uint64(v)
}

type unsignedCastIntegerCursor struct {
	cursor integerCursor
}

func (c *unsignedCastIntegerCursor) close() error { return c.cursor.close() }

func (c *unsignedCastIntegerCursor) next() (t int64, v interface{}) { return c.nextUnsigned() }

func (c *unsignedCastIntegerCursor) nextUnsigned() (int64, uint64) {
	t, v := c.cursor.nextInteger()
	return t, uint64(v)
}

// literalValueCursor represents a cursor that always returns a single value.
// It doesn't not have a time value so it can only be used with nextAt().
type literalValueCursor struct {
	value interface{}
}

func (c *literalValueCursor) close() error                   { return nil }
func (c *literalValueCursor) peek() (t int64, v interface{}) { return tsdb.EOF, c.value }
func (c *literalValueCursor) next() (t int64, v interface{}) { return tsdb.EOF, c.value }
func (c *literalValueCursor) nextAt(seek int64) interface{}  { return c.value }

// preallocate and cast to cursorAt to avoid allocations
var (
	nilFloatLiteralValueCursor    cursorAt = &literalValueCursor{value: (*float64)(nil)}
	nilIntegerLiteralValueCursor  cursorAt = &literalValueCursor{value: (*int64)(nil)}
	nilUnsignedLiteralValueCursor cursorAt = &literalValueCursor{value: (*uint64)(nil)}
	nilStringLiteralValueCursor   cursorAt = &literalValueCursor{value: (*string)(nil)}
	nilBooleanLiteralValueCursor  cursorAt = &literalValueCursor{value: (*bool)(nil)}
)

// stringSliceCursor is a cursor that outputs a slice of string values.
type stringSliceCursor struct {
	values []string
}

func (c *stringSliceCursor) close() error { return nil }

func (c *stringSliceCursor) next() (int64, interface{}) { return c.nextString() }

func (c *stringSliceCursor) nextString() (int64, string) {
	if len(c.values) == 0 {
		return tsdb.EOF, ""
	}

	value := c.values[0]
	c.values = c.values[1:]
	return 0, value
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
func newMergeFinalizerIterator(ctx context.Context, inputs []query.Iterator, opt query.IteratorOptions, log *zap.Logger) (query.Iterator, error) {
	itr, err := query.Iterators(inputs).Merge(opt)
	if err != nil {
		query.Iterators(inputs).Close()
		return nil, err
	}
	return newInstrumentedIterator(ctx, newFinalizerIterator(itr, log)), nil
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

func newInstrumentedIterator(ctx context.Context, itr query.Iterator) query.Iterator {
	if itr == nil {
		return nil
	}

	span := tracing.SpanFromContext(ctx)
	grp := metrics.GroupFromContext(ctx)
	if span == nil || grp == nil {
		return itr
	}

	switch inner := itr.(type) {
	case query.FloatIterator:
		return newFloatInstrumentedIterator(inner, span, grp)
	case query.IntegerIterator:
		return newIntegerInstrumentedIterator(inner, span, grp)
	case query.UnsignedIterator:
		return newUnsignedInstrumentedIterator(inner, span, grp)
	case query.StringIterator:
		return newStringInstrumentedIterator(inner, span, grp)
	case query.BooleanIterator:
		return newBooleanInstrumentedIterator(inner, span, grp)
	default:
		panic(fmt.Sprintf("unsupported instrumented iterator type: %T", itr))
	}
}
