package value

import (
	"fmt"
	"time"

	"github.com/influxdata/influxdb/query"
)

// Value represents a TSM-encoded value.
type Value interface {
	// UnixNano returns the timestamp of the value in nanoseconds since unix epoch.
	UnixNano() int64

	// Value returns the underlying value.
	Value() interface{}

	// Size returns the number of bytes necessary to represent the value and its timestamp.
	Size() int

	// String returns the string representation of the value and its timestamp.
	String() string

	// internalOnly is unexported to ensure implementations of Value
	// can only originate in this package.
	internalOnly()
}

// NewValue returns a new Value with the underlying type dependent on value.
func NewValue(t int64, value interface{}) Value {
	switch v := value.(type) {
	case int64:
		return IntegerValue{unixnano: t, value: v}
	case uint64:
		return UnsignedValue{unixnano: t, value: v}
	case float64:
		return FloatValue{unixnano: t, value: v}
	case bool:
		return BooleanValue{unixnano: t, value: v}
	case string:
		return StringValue{unixnano: t, value: v}
	}
	return EmptyValue{}
}

// NewRawIntegerValue returns a new integer value.
func NewRawIntegerValue(t int64, v int64) IntegerValue { return IntegerValue{unixnano: t, value: v} }

// NewRawUnsignedValue returns a new unsigned integer value.
func NewRawUnsignedValue(t int64, v uint64) UnsignedValue { return UnsignedValue{unixnano: t, value: v} }

// NewRawFloatValue returns a new float value.
func NewRawFloatValue(t int64, v float64) FloatValue { return FloatValue{unixnano: t, value: v} }

// NewRawBooleanValue returns a new boolean value.
func NewRawBooleanValue(t int64, v bool) BooleanValue { return BooleanValue{unixnano: t, value: v} }

// NewRawStringValue returns a new string value.
func NewRawStringValue(t int64, v string) StringValue { return StringValue{unixnano: t, value: v} }

// NewIntegerValue returns a new integer value.
func NewIntegerValue(t int64, v int64) Value { return NewRawIntegerValue(t, v) }

// NewUnsignedValue returns a new unsigned integer value.
func NewUnsignedValue(t int64, v uint64) Value { return NewRawUnsignedValue(t, v) }

// NewFloatValue returns a new float value.
func NewFloatValue(t int64, v float64) Value { return NewRawFloatValue(t, v) }

// NewBooleanValue returns a new boolean value.
func NewBooleanValue(t int64, v bool) Value { return NewRawBooleanValue(t, v) }

// NewStringValue returns a new string value.
func NewStringValue(t int64, v string) Value { return NewRawStringValue(t, v) }

// EmptyValue is used when there is no appropriate other value.
type EmptyValue struct{}

// UnixNano returns query.ZeroTime.
func (e EmptyValue) UnixNano() int64 { return query.ZeroTime }

// Value returns nil.
func (e EmptyValue) Value() interface{} { return nil }

// Size returns 0.
func (e EmptyValue) Size() int { return 0 }

// String returns the empty string.
func (e EmptyValue) String() string { return "" }

func (EmptyValue) internalOnly()    {}
func (StringValue) internalOnly()   {}
func (IntegerValue) internalOnly()  {}
func (UnsignedValue) internalOnly() {}
func (BooleanValue) internalOnly()  {}
func (FloatValue) internalOnly()    {}

// IntegerValue represents an int64 value.
type IntegerValue struct {
	unixnano int64
	value    int64
}

// Value returns the underlying int64 value.
func (v IntegerValue) Value() interface{} {
	return v.value
}

// UnixNano returns the timestamp of the value.
func (v IntegerValue) UnixNano() int64 {
	return v.unixnano
}

// Size returns the number of bytes necessary to represent the value and its timestamp.
func (v IntegerValue) Size() int {
	return 16
}

// String returns the string representation of the value and its timestamp.
func (v IntegerValue) String() string {
	return fmt.Sprintf("%v %v", time.Unix(0, v.unixnano), v.Value())
}

func (v IntegerValue) RawValue() int64 { return v.value }

// UnsignedValue represents an int64 value.
type UnsignedValue struct {
	unixnano int64
	value    uint64
}

// Value returns the underlying int64 value.
func (v UnsignedValue) Value() interface{} {
	return v.value
}

// UnixNano returns the timestamp of the value.
func (v UnsignedValue) UnixNano() int64 {
	return v.unixnano
}

// Size returns the number of bytes necessary to represent the value and its timestamp.
func (v UnsignedValue) Size() int {
	return 16
}

// String returns the string representation of the value and its timestamp.
func (v UnsignedValue) String() string {
	return fmt.Sprintf("%v %v", time.Unix(0, v.unixnano), v.Value())
}

func (v UnsignedValue) RawValue() uint64 { return v.value }

// FloatValue represents a float64 value.
type FloatValue struct {
	unixnano int64
	value    float64
}

// UnixNano returns the timestamp of the value.
func (v FloatValue) UnixNano() int64 {
	return v.unixnano
}

// Value returns the underlying float64 value.
func (v FloatValue) Value() interface{} {
	return v.value
}

// Size returns the number of bytes necessary to represent the value and its timestamp.
func (v FloatValue) Size() int {
	return 16
}

// String returns the string representation of the value and its timestamp.
func (v FloatValue) String() string {
	return fmt.Sprintf("%v %v", time.Unix(0, v.unixnano), v.value)
}

func (v FloatValue) RawValue() float64 { return v.value }

// BooleanValue represents a boolean value.
type BooleanValue struct {
	unixnano int64
	value    bool
}

// Size returns the number of bytes necessary to represent the value and its timestamp.
func (v BooleanValue) Size() int {
	return 9
}

// UnixNano returns the timestamp of the value in nanoseconds since unix epoch.
func (v BooleanValue) UnixNano() int64 {
	return v.unixnano
}

// Value returns the underlying boolean value.
func (v BooleanValue) Value() interface{} {
	return v.value
}

// String returns the string representation of the value and its timestamp.
func (v BooleanValue) String() string {
	return fmt.Sprintf("%v %v", time.Unix(0, v.unixnano), v.Value())
}

func (v BooleanValue) RawValue() bool { return v.value }

// StringValue represents a string value.
type StringValue struct {
	unixnano int64
	value    string
}

// Value returns the underlying string value.
func (v StringValue) Value() interface{} {
	return v.value
}

// UnixNano returns the timestamp of the value.
func (v StringValue) UnixNano() int64 {
	return v.unixnano
}

// Size returns the number of bytes necessary to represent the value and its timestamp.
func (v StringValue) Size() int {
	return 8 + len(v.value)
}

// String returns the string representation of the value and its timestamp.
func (v StringValue) String() string {
	return fmt.Sprintf("%v %v", time.Unix(0, v.unixnano), v.Value())
}

func (v StringValue) RawValue() string { return v.value }
