package main

import (
	"fmt"
	"time"
)

type FloatValue struct {
	time  time.Time
	value float64
}

func (f *FloatValue) Time() time.Time {
	return f.time
}

func (f *FloatValue) UnixNano() int64 {
	return f.time.UnixNano()
}

func (f *FloatValue) Value() interface{} {
	return f.value
}

func (f *FloatValue) Size() int {
	return 16
}

func (f *FloatValue) String() string {
	return fmt.Sprintf("%v %v", f.Time(), f.Value())
}

type BoolValue struct {
	time  time.Time
	value bool
}

func (b *BoolValue) Time() time.Time {
	return b.time
}

func (b *BoolValue) Size() int {
	return 9
}

func (b *BoolValue) UnixNano() int64 {
	return b.time.UnixNano()
}

func (b *BoolValue) Value() interface{} {
	return b.value
}

func (f *BoolValue) String() string {
	return fmt.Sprintf("%v %v", f.Time(), f.Value())
}

type Int64Value struct {
	time  time.Time
	value int64
}

func (v *Int64Value) Time() time.Time {
	return v.time
}

func (v *Int64Value) Value() interface{} {
	return v.value
}

func (v *Int64Value) UnixNano() int64 {
	return v.time.UnixNano()
}

func (v *Int64Value) Size() int {
	return 16
}

func (f *Int64Value) String() string {
	return fmt.Sprintf("%v %v", f.Time(), f.Value())
}

type StringValue struct {
	time  time.Time
	value string
}

func (v *StringValue) Time() time.Time {
	return v.time
}

func (v *StringValue) Value() interface{} {
	return v.value
}

func (v *StringValue) UnixNano() int64 {
	return v.time.UnixNano()
}

func (v *StringValue) Size() int {
	return 8 + len(v.value)
}

func (f *StringValue) String() string {
	return fmt.Sprintf("%v %v", f.Time(), f.Value())
}
