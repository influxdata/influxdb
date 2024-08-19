package parquet

import (
	"fmt"

	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/cursors"
)

type values struct {
	values any
}

func (v *values) String() string {
	return fmt.Sprintf("type: %T number of values: %d", v.Type(), v.Len())
}

func (v *values) Type() interface{} {
	switch v.values.(type) {
	case *cursors.IntegerArray:
		return int64(0)
	case *cursors.FloatArray:
		return float64(0)
	case *cursors.UnsignedArray:
		return uint64(0)
	case *cursors.BooleanArray:
		return true
	case *cursors.StringArray:
		return ""
	default:
		panic(fmt.Sprintf("unsupported type %T", v.values))
	}
}

func (v *values) Timestamps() []int64 {
	switch val := v.values.(type) {
	case *cursors.IntegerArray:
		return val.Timestamps
	case *cursors.FloatArray:
		return val.Timestamps
	case *cursors.UnsignedArray:
		return val.Timestamps
	case *cursors.BooleanArray:
		return val.Timestamps
	case *cursors.StringArray:
		return val.Timestamps
	default:
		panic(fmt.Sprintf("unsupported type %T", val))
	}
}

func (v *values) Values() any {
	switch val := v.values.(type) {
	case *cursors.IntegerArray:
		return val.Values
	case *cursors.FloatArray:
		return val.Values
	case *cursors.UnsignedArray:
		return val.Values
	case *cursors.BooleanArray:
		return val.Values
	case *cursors.StringArray:
		return val.Values
	default:
		panic(fmt.Sprintf("unsupported type %T", val))
	}
}

func (v *values) Len() int {
	switch val := v.values.(type) {
	case *cursors.IntegerArray:
		return val.Len()
	case *cursors.FloatArray:
		return val.Len()
	case *cursors.UnsignedArray:
		return val.Len()
	case *cursors.BooleanArray:
		return val.Len()
	case *cursors.StringArray:
		return val.Len()
	default:
		panic(fmt.Sprintf("unsupported type %T", val))
	}
}

func (v *values) append(cur tsdb.Cursor) {
	switch c := cur.(type) {
	case tsdb.IntegerArrayCursor:
		for {
			a := c.Next()
			if a.Len() == 0 {
				break
			}
			if v.values == nil {
				v.values = cursors.NewIntegerArrayLen(0)
			}
			v.values.(*cursors.IntegerArray).Merge(a)
		}
		//bw.WriteIntegerCursor(c)
	case tsdb.FloatArrayCursor:
		for {
			a := c.Next()
			if a.Len() == 0 {
				break
			}
			if v.values == nil {
				v.values = cursors.NewFloatArrayLen(0)
			}
			v.values.(*cursors.FloatArray).Merge(a)
		}
		//bw.WriteFloatCursor(c)
	case tsdb.UnsignedArrayCursor:
		for {
			a := c.Next()
			if a.Len() == 0 {
				break
			}
			if v.values == nil {
				v.values = cursors.NewUnsignedArrayLen(0)
			}
			v.values.(*cursors.UnsignedArray).Merge(a)
		}
		//bw.WriteUnsignedCursor(c)
	case tsdb.BooleanArrayCursor:
		for {
			a := c.Next()
			if a.Len() == 0 {
				break
			}
			if v.values == nil {
				v.values = cursors.NewBooleanArrayLen(0)
			}
			v.values.(*cursors.BooleanArray).Merge(a)
		}
		//bw.WriteBooleanCursor(c)
	case tsdb.StringArrayCursor:
		for {
			a := c.Next()
			if a.Len() == 0 {
				break
			}
			if v.values == nil {
				v.values = cursors.NewStringArrayLen(0)
			}
			v.values.(*cursors.StringArray).Merge(a)
		}
		//bw.WriteStringCursor(c)
	case nil:
		// no data for series key + field combination in this shard
	default:
		panic(fmt.Sprintf("unreachable: %T", c))
	}
}
