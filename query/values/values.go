package values

import (
	"fmt"
	"regexp"

	"github.com/influxdata/platform/query/semantic"
)

type Typer interface {
	Type() semantic.Type
}

type Value interface {
	Typer
	Str() string
	Int() int64
	UInt() uint64
	Float() float64
	Bool() bool
	Time() Time
	Duration() Duration
	Regexp() *regexp.Regexp
	Array() Array
	Object() Object
	Function() Function
}

// Function represents a callable type
type Function interface {
	Value
	Call(args Object) (Value, error)
}

type value struct {
	t semantic.Type
	v interface{}
}

func (v value) Type() semantic.Type {
	return v.t
}
func (v value) Str() string {
	CheckKind(v.t.Kind(), semantic.String)
	return v.v.(string)
}
func (v value) Int() int64 {
	CheckKind(v.t.Kind(), semantic.Int)
	return v.v.(int64)
}
func (v value) UInt() uint64 {
	CheckKind(v.t.Kind(), semantic.UInt)
	return v.v.(uint64)
}
func (v value) Float() float64 {
	CheckKind(v.t.Kind(), semantic.Float)
	return v.v.(float64)
}
func (v value) Bool() bool {
	CheckKind(v.t.Kind(), semantic.Bool)
	return v.v.(bool)
}
func (v value) Time() Time {
	CheckKind(v.t.Kind(), semantic.Time)
	return v.v.(Time)
}
func (v value) Duration() Duration {
	CheckKind(v.t.Kind(), semantic.Duration)
	return v.v.(Duration)
}
func (v value) Regexp() *regexp.Regexp {
	CheckKind(v.t.Kind(), semantic.Regexp)
	return v.v.(*regexp.Regexp)
}
func (v value) Array() Array {
	CheckKind(v.t.Kind(), semantic.Array)
	return v.v.(Array)
}
func (v value) Object() Object {
	CheckKind(v.t.Kind(), semantic.Object)
	return v.v.(Object)
}
func (v value) Function() Function {
	CheckKind(v.t.Kind(), semantic.Function)
	return v.v.(Function)
}

// InvalidValue is a non nil value who's type is semantic.Invalid
var InvalidValue = value{t: semantic.Invalid}

func NewStringValue(v string) Value {
	return value{
		t: semantic.String,
		v: v,
	}
}
func NewIntValue(v int64) Value {
	return value{
		t: semantic.Int,
		v: v,
	}
}
func NewUIntValue(v uint64) Value {
	return value{
		t: semantic.UInt,
		v: v,
	}
}
func NewFloatValue(v float64) Value {
	return value{
		t: semantic.Float,
		v: v,
	}
}
func NewBoolValue(v bool) Value {
	return value{
		t: semantic.Bool,
		v: v,
	}
}
func NewTimeValue(v Time) Value {
	return value{
		t: semantic.Time,
		v: v,
	}
}
func NewDurationValue(v Duration) Value {
	return value{
		t: semantic.Duration,
		v: v,
	}
}
func NewRegexpValue(v *regexp.Regexp) Value {
	return value{
		t: semantic.Regexp,
		v: v,
	}
}

func UnexpectedKind(got, exp semantic.Kind) error {
	return fmt.Errorf("unexpected kind: got %q expected %q", got, exp)
}

// CheckKind panics if got != exp.
func CheckKind(got, exp semantic.Kind) {
	if got != exp {
		panic(UnexpectedKind(got, exp))
	}
}
