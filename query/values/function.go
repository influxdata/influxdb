package values

import (
	"regexp"

	"github.com/influxdata/platform/query/semantic"
)

// Function represents a callable type
type Function interface {
	Value
	HasSideEffect() bool
	Call(args Object) (Value, error)
}

// NewFunction returns a new function value
func NewFunction(name string, typ semantic.Type, call func(args Object) (Value, error), sideEffect bool) Function {
	return &function{
		name:          name,
		t:             typ,
		call:          call,
		hasSideEffect: sideEffect,
	}
}

// function implements Value interface and more specifically the Function interface
type function struct {
	name          string
	t             semantic.Type
	call          func(args Object) (Value, error)
	hasSideEffect bool
}

func (f *function) Type() semantic.Type {
	return f.t
}

func (f *function) Str() string {
	panic(UnexpectedKind(semantic.Object, semantic.String))
}

func (f *function) Int() int64 {
	panic(UnexpectedKind(semantic.Object, semantic.Int))
}

func (f *function) UInt() uint64 {
	panic(UnexpectedKind(semantic.Object, semantic.UInt))
}

func (f *function) Float() float64 {
	panic(UnexpectedKind(semantic.Object, semantic.Float))
}

func (f *function) Bool() bool {
	panic(UnexpectedKind(semantic.Object, semantic.Bool))
}

func (f *function) Time() Time {
	panic(UnexpectedKind(semantic.Object, semantic.Time))
}

func (f *function) Duration() Duration {
	panic(UnexpectedKind(semantic.Object, semantic.Duration))
}

func (f *function) Regexp() *regexp.Regexp {
	panic(UnexpectedKind(semantic.Object, semantic.Regexp))
}

func (f *function) Array() Array {
	panic(UnexpectedKind(semantic.Object, semantic.Function))
}

func (f *function) Object() Object {
	panic(UnexpectedKind(semantic.Object, semantic.Object))
}

func (f *function) Function() Function {
	return f
}

func (f *function) Equal(rhs Value) bool {
	if f.Type() != rhs.Type() {
		return false
	}
	v, ok := rhs.(*function)
	return ok && (f == v)
}

func (f *function) HasSideEffect() bool {
	return f.hasSideEffect
}

func (f *function) Call(args Object) (Value, error) {
	return f.call(args)
}
