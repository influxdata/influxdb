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
	Equal(Value) bool
}

// Function represents a callable type
type Function interface {
	Value
	HasSideEffect() bool
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
func (v value) Equal(r Value) bool {
	if v.Type() != r.Type() {
		return false
	}
	switch k := v.Type().Kind(); k {
	case semantic.Bool:
		return v.Bool() == r.Bool()
	case semantic.UInt:
		return v.UInt() == r.UInt()
	case semantic.Int:
		return v.Int() == r.Int()
	case semantic.Float:
		return v.Float() == r.Float()
	case semantic.String:
		return v.Str() == r.Str()
	case semantic.Time:
		return v.Time() == r.Time()
	case semantic.Duration:
		return v.Duration() == r.Duration()
	case semantic.Regexp:
		return v.Regexp().String() == r.Regexp().String()
	case semantic.Object:
		return v.Object().Equal(r.Object())
	case semantic.Array:
		return v.Array().Equal(r.Array())
	case semantic.Function:
		return v.Function().Equal(r.Function())
	default:
		return false
	}
}

func (v value) String() string {
	return fmt.Sprintf("%v", v.v)
}

// InvalidValue is a non nil value who's type is semantic.Invalid
var InvalidValue = value{t: semantic.Invalid}

func NewValue(v interface{}, k semantic.Kind) (Value, error) {
	switch k {
	case semantic.String:
		_, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("string value must have type string, got %T", v)
		}
	case semantic.Int:
		_, ok := v.(int64)
		if !ok {
			return nil, fmt.Errorf("int value must have type int64, got %T", v)
		}
	case semantic.UInt:
		_, ok := v.(uint64)
		if !ok {
			return nil, fmt.Errorf("uint value must have type uint64, got %T", v)
		}
	case semantic.Float:
		_, ok := v.(float64)
		if !ok {
			return nil, fmt.Errorf("float value must have type float64, got %T", v)
		}
	case semantic.Bool:
		_, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("bool value must have type bool, got %T", v)
		}
	case semantic.Time:
		_, ok := v.(Time)
		if !ok {
			return nil, fmt.Errorf("time value must have type Time, got %T", v)
		}
	case semantic.Duration:
		_, ok := v.(Duration)
		if !ok {
			return nil, fmt.Errorf("duration value must have type Duration, got %T", v)
		}
	case semantic.Regexp:
		_, ok := v.(*regexp.Regexp)
		if !ok {
			return nil, fmt.Errorf("regexp value must have type *regexp.Regexp, got %T", v)
		}
	default:
		return nil, fmt.Errorf("unsupported value kind %v", k)
	}
	return value{
		t: k,
		v: v,
	}, nil
}

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
