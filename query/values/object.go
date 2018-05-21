package values

import (
	"regexp"

	"github.com/influxdata/ifql/semantic"
)

type Object interface {
	Value
	Get(name string) (Value, bool)
	Set(name string, v Value)
	Len() int
	Range(func(name string, v Value))
}

type object struct {
	values        map[string]Value
	propertyTypes map[string]semantic.Type
	typ           semantic.Type
}

func NewObject() *object {
	return &object{
		values:        make(map[string]Value),
		propertyTypes: make(map[string]semantic.Type),
	}
}

func (o *object) Type() semantic.Type {
	if o.typ == nil {
		o.typ = semantic.NewObjectType(o.propertyTypes)
	}
	return o.typ
}

func (o *object) Set(name string, v Value) {
	o.values[name] = v
	if o.propertyTypes[name] != v.Type() {
		o.setPropertyType(name, v.Type())
	}
}
func (o *object) Get(name string) (Value, bool) {
	v, ok := o.values[name]
	return v, ok
}
func (o *object) Len() int {
	return len(o.values)
}

func (o *object) setPropertyType(name string, t semantic.Type) {
	o.propertyTypes[name] = t
	o.typ = nil
}

func (o *object) Range(f func(name string, v Value)) {
	for k, v := range o.values {
		f(k, v)
	}
}

func (o *object) Str() string {
	panic(UnexpectedKind(semantic.Object, semantic.String))
}
func (o *object) Int() int64 {
	panic(UnexpectedKind(semantic.Object, semantic.Int))
}
func (o *object) UInt() uint64 {
	panic(UnexpectedKind(semantic.Object, semantic.UInt))
}
func (o *object) Float() float64 {
	panic(UnexpectedKind(semantic.Object, semantic.Float))
}
func (o *object) Bool() bool {
	panic(UnexpectedKind(semantic.Object, semantic.Bool))
}
func (o *object) Time() Time {
	panic(UnexpectedKind(semantic.Object, semantic.Time))
}
func (o *object) Duration() Duration {
	panic(UnexpectedKind(semantic.Object, semantic.Duration))
}
func (o *object) Regexp() *regexp.Regexp {
	panic(UnexpectedKind(semantic.Object, semantic.Regexp))
}
func (o *object) Array() Array {
	panic(UnexpectedKind(semantic.Object, semantic.Array))
}
func (o *object) Object() Object {
	return o
}
func (o *object) Function() Function {
	panic(UnexpectedKind(semantic.Object, semantic.Function))
}
