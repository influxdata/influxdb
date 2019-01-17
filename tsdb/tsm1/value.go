package tsm1

import "github.com/influxdata/influxdb/tsdb/value"

type (
	Value         = value.Value
	IntegerValue  = value.IntegerValue
	UnsignedValue = value.UnsignedValue
	FloatValue    = value.FloatValue
	BooleanValue  = value.BooleanValue
	StringValue   = value.StringValue
)

// NewValue returns a new Value with the underlying type dependent on value.
func NewValue(t int64, v interface{}) Value { return value.NewValue(t, v) }

// NewRawIntegerValue returns a new integer value.
func NewRawIntegerValue(t int64, v int64) IntegerValue { return value.NewRawIntegerValue(t, v) }

// NewRawUnsignedValue returns a new unsigned integer value.
func NewRawUnsignedValue(t int64, v uint64) UnsignedValue { return value.NewRawUnsignedValue(t, v) }

// NewRawFloatValue returns a new float value.
func NewRawFloatValue(t int64, v float64) FloatValue { return value.NewRawFloatValue(t, v) }

// NewRawBooleanValue returns a new boolean value.
func NewRawBooleanValue(t int64, v bool) BooleanValue { return value.NewRawBooleanValue(t, v) }

// NewRawStringValue returns a new string value.
func NewRawStringValue(t int64, v string) StringValue { return value.NewRawStringValue(t, v) }

// NewIntegerValue returns a new integer value.
func NewIntegerValue(t int64, v int64) Value { return value.NewIntegerValue(t, v) }

// NewUnsignedValue returns a new unsigned integer value.
func NewUnsignedValue(t int64, v uint64) Value { return value.NewUnsignedValue(t, v) }

// NewFloatValue returns a new float value.
func NewFloatValue(t int64, v float64) Value { return value.NewFloatValue(t, v) }

// NewBooleanValue returns a new boolean value.
func NewBooleanValue(t int64, v bool) Value { return value.NewBooleanValue(t, v) }

// NewStringValue returns a new string value.
func NewStringValue(t int64, v string) Value { return value.NewStringValue(t, v) }
