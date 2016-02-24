package tsdb

import (
	"fmt"
	"time"

	tsm "github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// FloatValue holds float64 values
type FloatValue struct {
	T time.Time
	V float64
}

// Time returns the time associated with the FloatValue
func (f *FloatValue) Time() time.Time {
	return f.T
}

// UnixNano returns the Unix time in nanoseconds associated with the FloatValue
func (f *FloatValue) UnixNano() int64 {
	return f.T.UnixNano()
}

// Value returns the float64 value
func (f *FloatValue) Value() interface{} {
	return f.V
}

// Size returns the size of the FloatValue. It is always 16
func (f *FloatValue) Size() int {
	return 16
}

// String returns the formatted string. Implements the Stringer interface
func (f *FloatValue) String() string {
	return fmt.Sprintf("%v %v", f.Time(), f.Value())
}

// BoolValue holds bool values
type BoolValue struct {
	T time.Time
	V bool
}

// Time returns the time associated with the BoolValue
func (b *BoolValue) Time() time.Time {
	return b.T
}

// Size returns the size of the BoolValue. It is always 9
func (b *BoolValue) Size() int {
	return 9
}

// UnixNano returns the Unix time in nanoseconds associated with the BoolValue
func (b *BoolValue) UnixNano() int64 {
	return b.T.UnixNano()
}

// Value returns the boolean stored
func (b *BoolValue) Value() interface{} {
	return b.V
}

// String returns the formatted string. Implements the Stringer interface
func (b *BoolValue) String() string {
	return fmt.Sprintf("%v %v", b.Time(), b.Value())
}

// Int64Value holds int64 values
type Int64Value struct {
	T time.Time
	V int64
}

// Time returns the time associated with the Int64Value
func (v *Int64Value) Time() time.Time {
	return v.T
}

// Value returns the int64 stored
func (v *Int64Value) Value() interface{} {
	return v.V
}

// UnixNano returns the Unix time in nanoseconds associated with the Int64Value
func (v *Int64Value) UnixNano() int64 {
	return v.T.UnixNano()
}

// Size returns the size of the Int64Value. It is always 16
func (v *Int64Value) Size() int {
	return 16
}

// String returns the formatted string. Implements the Stringer interface
func (v *Int64Value) String() string {
	return fmt.Sprintf("%v %v", v.Time(), v.Value())
}

// StringValue holds string values
type StringValue struct {
	T time.Time
	V string
}

// Time returns the time associated with the StringValue
func (v *StringValue) Time() time.Time {
	return v.T
}

// Value returns the float stored
func (v *StringValue) Value() interface{} {
	return v.V
}

// UnixNano returns the Unix time in nanoseconds associated with the StringValue
func (v *StringValue) UnixNano() int64 {
	return v.T.UnixNano()
}

// Size returns the size of the StringValue
func (v *StringValue) Size() int {
	return 8 + len(v.V)
}

// String returns the formatted string. Implements the Stringer interface
func (v *StringValue) String() string {
	return fmt.Sprintf("%v %v", v.Time(), v.Value())
}

// ConvertToValue converts the data from other engines to TSM
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
