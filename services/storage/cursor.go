package storage

import (
	"github.com/influxdata/influxdb/tsdb"
)

func newFilterCursor(cur tsdb.Cursor, cond expression) tsdb.Cursor {
	switch cur := cur.(type) {
	case tsdb.FloatCursor:
		return newFloatFilterCursor(cur, cond)

	case tsdb.IntegerCursor:
		return newIntegerFilterCursor(cur, cond)

	case nil:
		return nil

	default:
		panic("invalid cursor type")
	}
}

type integerFilterCursor struct {
	tsdb.IntegerCursor
	cond expression
	m    *singleValue
}

type singleValue struct {
	v interface{}
}

func (v *singleValue) Value(key string) (interface{}, bool) {
	return v.v, true
}

func newIntegerFilterCursor(cur tsdb.IntegerCursor, cond expression) *integerFilterCursor {
	return &integerFilterCursor{IntegerCursor: cur, cond: cond, m: &singleValue{}}
}

func (c *integerFilterCursor) Next() (key int64, value int64) {
	for {
		k, v := c.IntegerCursor.Next()
		if k == tsdb.EOF {
			return k, v
		}

		c.m.v = v
		if !c.cond.EvalBool(c.m) {
			continue
		}
		return k, v
	}
}

type floatFilterCursor struct {
	tsdb.FloatCursor
	cond expression
	m    *singleValue
}

func newFloatFilterCursor(cur tsdb.FloatCursor, cond expression) *floatFilterCursor {
	return &floatFilterCursor{FloatCursor: cur, cond: cond, m: &singleValue{}}
}

func (c *floatFilterCursor) Next() (key int64, value float64) {
	for {
		k, v := c.FloatCursor.Next()
		if k == tsdb.EOF {
			return k, v
		}

		c.m.v = v
		if !c.cond.EvalBool(c.m) {
			continue
		}
		return k, v
	}
}
