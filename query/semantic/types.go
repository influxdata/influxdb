package semantic

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

// Type is the representation of an IFQL type.
//
// Type values are comparable and as such can be used as map keys and directly comparison using the == operator.
// Two types are equal if they represent identical types.
//
// DO NOT embed this type into other interfaces or structs as that will invalidate the comparison properties of the interface.
type Type interface {
	// Kind returns the specific kind of this type.
	Kind() Kind

	// PropertyType returns the type of a given property.
	// It panics if the type's Kind is not Object
	PropertyType(name string) Type

	// Properties returns a map of all property types.
	// It panics if the type's Kind is not Object
	Properties() map[string]Type

	// ElementType return the type of elements in the array.
	// It panics if the type's Kind is not Array.
	ElementType() Type

	// PipeArgument reports the name of the argument that can be pipe into.
	// It panics if the type's Kind is not Function.
	PipeArgument() string

	// ReturnType reports the return type of the function
	// It panics if the type's Kind is not Function.
	ReturnType() Type

	// Types cannot be created outside of the semantic package
	// This is needed so that we can cache type definitions.
	typ()
}

type Kind int

const (
	Invalid Kind = iota
	Nil
	String
	Int
	UInt
	Float
	Bool
	Time
	Duration
	Regexp
	Array
	Object
	Function
)

var kindNames = []string{
	Invalid:  "invalid",
	Nil:      "nil",
	String:   "string",
	Int:      "int",
	UInt:     "uint",
	Float:    "float",
	Bool:     "bool",
	Time:     "time",
	Duration: "duration",
	Regexp:   "regexp",
	Array:    "array",
	Object:   "object",
	Function: "function",
}

func (k Kind) String() string {
	if int(k) < len(kindNames) {
		return kindNames[k]
	}
	return "kind" + strconv.Itoa(int(k))
}

func (k Kind) Kind() Kind {
	return k
}
func (k Kind) PropertyType(name string) Type {
	panic(fmt.Errorf("cannot get type of property %q, from kind %q", name, k))
}
func (k Kind) Properties() map[string]Type {
	panic(fmt.Errorf("cannot get properties from kind %s", k))
}
func (k Kind) ElementType() Type {
	panic(fmt.Errorf("cannot get element type from kind %s", k))
}
func (k Kind) PipeArgument() string {
	panic(fmt.Errorf("cannot get pipe argument name from kind %s", k))
}
func (k Kind) ReturnType() Type {
	panic(fmt.Errorf("cannot get return type from kind %s", k))
}
func (k Kind) typ() {}

type arrayType struct {
	elementType Type
}

func (t *arrayType) String() string {
	return fmt.Sprintf("[%v]", t.elementType)
}

func (t *arrayType) Kind() Kind {
	return Array
}
func (t *arrayType) PropertyType(name string) Type {
	panic(fmt.Errorf("cannot get property type of kind %s", t.Kind()))
}
func (t *arrayType) Properties() map[string]Type {
	panic(fmt.Errorf("cannot get properties type of kind %s", t.Kind()))
}
func (t *arrayType) ElementType() Type {
	return t.elementType
}
func (t *arrayType) PipeArgument() string {
	panic(fmt.Errorf("cannot get pipe argument name from kind %s", t.Kind()))
}
func (t *arrayType) ReturnType() Type {
	panic(fmt.Errorf("cannot get return type of kind %s", t.Kind()))
}

func (t *arrayType) typ() {}

// arrayTypeCache caches *arrayType values.
//
// Since arrayTypes only have a single field elementType we can key
// all arrayTypes by their elementType.
var arrayTypeCache struct {
	sync.Mutex // Guards stores (but not loads) on m.

	// m is a map[Type]*arrayType keyed by the elementType of the array.
	// Elements in m are append-only and thus safe for concurrent reading.
	m sync.Map
}

// arrayTypeOf returns the Type for the given ArrayExpression.
func arrayTypeOf(e *ArrayExpression) Type {
	if len(e.Elements) == 0 {
		return EmptyArrayType
	}
	et := e.Elements[0].Type()
	return NewArrayType(et)
}

var EmptyArrayType = NewArrayType(Nil)

func NewArrayType(elementType Type) Type {
	// Lookup arrayType in cache by elementType
	if t, ok := arrayTypeCache.m.Load(elementType); ok {
		return t.(*arrayType)
	}

	// Type not found in cache, lock and retry.
	arrayTypeCache.Lock()
	defer arrayTypeCache.Unlock()

	// First read again while holding the lock.
	if t, ok := arrayTypeCache.m.Load(elementType); ok {
		return t.(*arrayType)
	}

	// Still no cache entry, add it.
	at := &arrayType{
		elementType: elementType,
	}
	arrayTypeCache.m.Store(elementType, at)

	return at
}

type objectType struct {
	properties map[string]Type
}

func (t *objectType) String() string {
	var buf bytes.Buffer
	buf.Write([]byte("{"))
	for k, prop := range t.properties {
		fmt.Fprintf(&buf, "%s:%v,", k, prop)
	}
	buf.WriteRune('}')

	return buf.String()
}

func (t *objectType) Kind() Kind {
	return Object
}
func (t *objectType) PropertyType(name string) Type {
	return t.properties[name]
}
func (t *objectType) Properties() map[string]Type {
	return t.properties
}
func (t *objectType) ElementType() Type {
	panic(fmt.Errorf("cannot get element type of kind %s", t.Kind()))
}
func (t *objectType) PipeArgument() string {
	panic(fmt.Errorf("cannot get pipe argument name from kind %s", t.Kind()))
}
func (t *objectType) ReturnType() Type {
	panic(fmt.Errorf("cannot get return type of kind %s", t.Kind()))
}
func (t *objectType) typ() {}

func (t *objectType) equal(o *objectType) bool {
	if t == o {
		return true
	}

	if len(t.properties) != len(o.properties) {
		return false
	}

	for k, vtyp := range t.properties {
		ovtyp, ok := o.properties[k]
		if !ok {
			return false
		}
		if ovtyp != vtyp {
			return false
		}
	}
	return true
}

// objectTypeCache caches all *objectTypes.
//
// Since objectTypes are identified by their properties,
// a hash is computed of the property names and kinds to reduce the search space.
var objectTypeCache struct {
	sync.Mutex // Guards stores (but not loads) on m.

	// m is a map[uint32][]*objectType keyed by the hash calculated of the object's properties' name and kind.
	// Elements in m are append-only and thus safe for concurrent reading.
	m sync.Map
}

// objectTypeOf returns the Type for the given ObjectExpression.
func objectTypeOf(e *ObjectExpression) Type {
	propertyTypes := make(map[string]Type, len(e.Properties))
	for _, p := range e.Properties {
		propertyTypes[p.Key.Name] = p.Value.Type()
	}

	return NewObjectType(propertyTypes)
}

var EmptyObject = NewObjectType(nil)

func NewObjectType(propertyTypes map[string]Type) Type {
	propertyNames := make([]string, 0, len(propertyTypes))
	for name := range propertyTypes {
		propertyNames = append(propertyNames, name)
	}
	sort.Strings(propertyNames)

	sum := fnv.New32a()
	for _, p := range propertyNames {
		t := propertyTypes[p]

		// track hash of property names and kinds
		sum.Write([]byte(p))
		binary.Write(sum, binary.LittleEndian, t.Kind())
	}

	// Create new object type
	ot := &objectType{
		properties: propertyTypes,
	}

	// Simple linear search after hash lookup
	h := sum.Sum32()
	if ts, ok := objectTypeCache.m.Load(h); ok {
		for _, t := range ts.([]*objectType) {
			if t.equal(ot) {
				return t
			}
		}
	}

	// Type not found in cache, lock and retry.
	objectTypeCache.Lock()
	defer objectTypeCache.Unlock()

	// First read again while holding the lock.
	var types []*objectType
	if ts, ok := objectTypeCache.m.Load(h); ok {
		types = ts.([]*objectType)
		for _, t := range types {
			if t.equal(ot) {
				return t
			}
		}
	}

	// Make copy of properties since we can't trust that the source will not be modified
	properties := make(map[string]Type)
	for k, v := range ot.properties {
		properties[k] = v
	}
	ot.properties = properties

	// Still no cache entry, add it.
	objectTypeCache.m.Store(h, append(types, ot))

	return ot
}

type functionType struct {
	params       map[string]Type
	returnType   Type
	pipeArgument string
}

func (t *functionType) String() string {
	var buf bytes.Buffer
	buf.Write([]byte("function("))
	for k, param := range t.params {
		fmt.Fprintf(&buf, "%s:%v,", k, param)
	}
	fmt.Fprintf(&buf, ") %v", t.returnType)

	return buf.String()
}

func (t *functionType) Kind() Kind {
	return Function
}
func (t *functionType) PropertyType(name string) Type {
	panic(fmt.Errorf("cannot get property type of kind %s", t.Kind()))
}
func (t *functionType) Properties() map[string]Type {
	panic(fmt.Errorf("cannot get properties type of kind %s", t.Kind()))
}
func (t *functionType) ElementType() Type {
	panic(fmt.Errorf("cannot get element type of kind %s", t.Kind()))
}
func (t *functionType) PipeArgument() string {
	return t.pipeArgument
}
func (t *functionType) ReturnType() Type {
	return t.returnType
}
func (t *functionType) typ() {}

func (t *functionType) Params() map[string]Type {
	return t.params
}

func (t *functionType) equal(o *functionType) bool {
	if t == o {
		return true
	}

	if t.returnType != o.returnType {
		return false
	}

	if len(t.params) != len(o.params) {
		return false
	}

	for k, pt := range t.params {
		opt, ok := o.params[k]
		if !ok {
			return false
		}
		if opt != pt {
			return false
		}
	}

	return true
}

// functionTypeCache caches all *functionTypes.
//
// Since functionTypes are identified by their parameters and returnType,
// a hash is computed of the param names and kinds to reduce the search space.
var functionTypeCache struct {
	sync.Mutex // Guards stores (but not loads) on m.

	// m is a map[uint32][]*functionType keyed by the hash calculated.
	// Elements in m are append-only and thus safe for concurrent reading.
	m sync.Map
}

// functionTypeOf returns the Type for the given ObjectExpression.
func functionTypeOf(e *FunctionExpression) Type {
	sig := FunctionSignature{}
	sig.Params = make(map[string]Type, len(e.Params))
	for _, p := range e.Params {
		sig.Params[p.Key.Name] = p.Type()
	}
	// Determine returnType
	switch b := e.Body.(type) {
	case Expression:
		sig.ReturnType = b.Type()
	case *BlockStatement:
		rs := b.ReturnStatement()
		sig.ReturnType = rs.Argument.Type()
	}
	for _, p := range e.Params {
		if p.Piped {
			sig.PipeArgument = p.Key.Name
			break
		}
	}
	return NewFunctionType(sig)
}

type FunctionSignature struct {
	Params       map[string]Type
	ReturnType   Type
	PipeArgument string
}

func NewFunctionType(sig FunctionSignature) Type {
	paramNames := make([]string, 0, len(sig.Params))
	for k := range sig.Params {
		paramNames = append(paramNames, k)
	}
	sort.Strings(paramNames)

	sum := fnv.New32a()
	sum.Write([]byte(sig.PipeArgument))
	for _, p := range paramNames {
		// track hash of parameter names and kinds
		sum.Write([]byte(p))
		// TODO(nathanielc): Include parameter type information
		binary.Write(sum, binary.LittleEndian, sig.Params[p].Kind())
	}

	// Create new object type
	ft := &functionType{
		params:       sig.Params,
		returnType:   sig.ReturnType,
		pipeArgument: sig.PipeArgument,
	}

	// Simple linear search after hash lookup
	h := sum.Sum32()
	if ts, ok := functionTypeCache.m.Load(h); ok {
		for _, t := range ts.([]*functionType) {
			if t.equal(ft) {
				return t
			}
		}
	}

	// Type not found in cache, lock and retry.
	functionTypeCache.Lock()
	defer functionTypeCache.Unlock()

	// First read again while holding the lock.
	var types []*functionType
	if ts, ok := functionTypeCache.m.Load(h); ok {
		types = ts.([]*functionType)
		for _, t := range types {
			if t.equal(ft) {
				return t
			}
		}
	}

	// Make copy of Params since we can't trust the source is not modified
	params := make(map[string]Type, len(ft.params))
	for k, v := range ft.params {
		params[k] = v
	}
	ft.params = params

	// Still no cache entry, add it.
	functionTypeCache.m.Store(h, append(types, ft))

	return ft
}
