package storage

import (
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/tsdb"
)

func newFilterCursor(cur tsdb.Cursor, cond influxql.Expr) tsdb.Cursor {
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
	cond influxql.Expr
	m    map[string]interface{}
}

func newIntegerFilterCursor(cur tsdb.IntegerCursor, cond influxql.Expr) *integerFilterCursor {
	return &integerFilterCursor{IntegerCursor: cur, cond: cond, m: map[string]interface{}{"$": nil}}
}

func (c *integerFilterCursor) Next() (key int64, value int64) {
	for {
		k, v := c.IntegerCursor.Next()
		if k == tsdb.EOF {
			return k, v
		}

		c.m["$"] = v
		if !influxql.EvalBool(c.cond, c.m) {
			continue
		}
		return k, v
	}
}

type floatFilterCursor struct {
	tsdb.FloatCursor
	cond influxql.Expr
	m    map[string]interface{}
}

func newFloatFilterCursor(cur tsdb.FloatCursor, cond influxql.Expr) *floatFilterCursor {
	return &floatFilterCursor{FloatCursor: cur, cond: cond, m: map[string]interface{}{"$": nil}}
}

func (c *floatFilterCursor) Next() (key int64, value float64) {
	for {
		k, v := c.FloatCursor.Next()
		if k == tsdb.EOF {
			return k, v
		}

		c.m["$"] = v
		if !influxql.EvalBool(c.cond, c.m) {
			continue
		}
		return k, v
	}
}
