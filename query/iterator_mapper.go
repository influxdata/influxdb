package query

import (
	"fmt"
	"math"

	"github.com/influxdata/influxql"
)

type IteratorMap interface {
	Value(row *Row) interface{}
}

type FieldMap struct {
	Index int
	Type  influxql.DataType
}

func (f FieldMap) Value(row *Row) interface{} {
	v := castToType(row.Values[f.Index], f.Type)
	if v == NullFloat {
		// If the value is a null float, then convert it back to NaN
		// so it is treated as a float for eval.
		v = math.NaN()
	}
	return v
}

type TagMap string

func (s TagMap) Value(row *Row) interface{} { return row.Series.Tags.Value(string(s)) }

type NullMap struct{}

func (NullMap) Value(row *Row) interface{} { return nil }

func NewIteratorMapper(cur Cursor, driver IteratorMap, fields []IteratorMap, opt IteratorOptions) Iterator {
	if driver != nil {
		switch driver := driver.(type) {
		case FieldMap:
			switch driver.Type {
			case influxql.Float:
				return newFloatIteratorMapper(cur, driver, fields, opt)
			case influxql.Integer:
				return newIntegerIteratorMapper(cur, driver, fields, opt)
			case influxql.Unsigned:
				return newUnsignedIteratorMapper(cur, driver, fields, opt)
			case influxql.String, influxql.Tag:
				return newStringIteratorMapper(cur, driver, fields, opt)
			case influxql.Boolean:
				return newBooleanIteratorMapper(cur, driver, fields, opt)
			default:
				// The driver doesn't appear to to have a valid driver type.
				// We should close the cursor and return a blank iterator.
				// We close the cursor because we own it and have a responsibility
				// to close it once it is passed into this function.
				cur.Close()
				return &nilFloatIterator{}
			}
		case TagMap:
			return newStringIteratorMapper(cur, driver, fields, opt)
		default:
			panic(fmt.Sprintf("unable to create iterator mapper with driver expression type: %T", driver))
		}
	}
	return newFloatIteratorMapper(cur, nil, fields, opt)
}
