package tsm1

import (
	"fmt"

	"github.com/influxdata/influxdb/influxql"
)

func newLimitIterator(input influxql.Iterator, opt influxql.IteratorOptions) influxql.Iterator {
	switch input := input.(type) {
	case influxql.FloatIterator:
		return newFloatLimitIterator(input, opt)
	case influxql.IntegerIterator:
		return newIntegerLimitIterator(input, opt)
	case influxql.UnsignedIterator:
		return newUnsignedLimitIterator(input, opt)
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
