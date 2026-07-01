package query

import (
	"fmt"
	"math"
	"time"

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

// datePartMap computes a GROUP BY date_part dimension value from a row's
// timestamp. It is used when the source is a subquery: the date_part dimension
// is not a real field of the subquery, so its int64 aux value must be derived
// here from row.Time, mirroring what the TSM iterator appends for a measurement
// source. The result feeds the existing DimensionGrouper / reduce path unchanged.
type datePartMap struct {
	expr DatePartExpr
	loc  *time.Location
}

func (m datePartMap) Value(row *Row) interface{} {
	v, _ := ExtractDatePartExpr(time.Unix(0, row.Time).In(LocationOrUTC(m.loc)), m.expr)
	return v
}

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
