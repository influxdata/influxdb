package tsdb

import (
	"fmt"
	"time"

	tsm "github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

type FloatValue struct {
	T time.Time
	V float64
}

func (f *FloatValue) Time() time.Time {
	return f.T
}

func (f *FloatValue) UnixNano() int64 {
	return f.T.UnixNano()
}

func (f *FloatValue) Value() interface{} {
	return f.V
}

func (f *FloatValue) Size() int {
	return 16
}

func (f *FloatValue) String() string {
	return fmt.Sprintf("%v %v", f.Time(), f.Value())
}

type BoolValue struct {
	T time.Time
	V bool
}

func (b *BoolValue) Time() time.Time {
	return b.T
}

func (b *BoolValue) Size() int {
	return 9
}

func (b *BoolValue) UnixNano() int64 {
	return b.T.UnixNano()
}

func (b *BoolValue) Value() interface{} {
	return b.V
}

func (f *BoolValue) String() string {
	return fmt.Sprintf("%v %v", f.Time(), f.Value())
}

type Int64Value struct {
	T time.Time
	V int64
}

func (v *Int64Value) Time() time.Time {
	return v.T
}

func (v *Int64Value) Value() interface{} {
	return v.V
}

func (v *Int64Value) UnixNano() int64 {
	return v.T.UnixNano()
}

func (v *Int64Value) Size() int {
	return 16
}

func (f *Int64Value) String() string {
	return fmt.Sprintf("%v %v", f.Time(), f.Value())
}

type StringValue struct {
	T time.Time
	V string
}

func (v *StringValue) Time() time.Time {
	return v.T
}

func (v *StringValue) Value() interface{} {
	return v.V
}

func (v *StringValue) UnixNano() int64 {
	return v.T.UnixNano()
}

func (v *StringValue) Size() int {
	return 8 + len(v.V)
}

func (f *StringValue) String() string {
	return fmt.Sprintf("%v %v", f.Time(), f.Value())
}

func ConvertToValue(k int64, v interface{}) tsm.Value {
	var value tsm.Value

	switch v := v.(type) {
	case int64:
		value = &Int64Value{
			T: time.Unix(0, k),
			V: v,
		}
	case float64:
		value = &FloatValue{
			T: time.Unix(0, k),
			V: v,
		}
	case bool:
		value = &BoolValue{
			T: time.Unix(0, k),
			V: v,
		}
	case string:
		value = &StringValue{
			T: time.Unix(0, k),
			V: v,
		}
	default:
		panic(fmt.Sprintf("value type %T unsupported for conversion", v))
	}

	return value
}
