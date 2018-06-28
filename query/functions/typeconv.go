package functions

import (
	"fmt"
	"regexp"
	"strconv"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/semantic"
	"github.com/influxdata/platform/query/values"
)

func init() {
	query.RegisterBuiltInValue("string", &stringConv{})
	query.RegisterBuiltInValue("int", &intConv{})
	query.RegisterBuiltInValue("uint", &uintConv{})
	query.RegisterBuiltInValue("float", &floatConv{})
	query.RegisterBuiltInValue("bool", &boolConv{})
	query.RegisterBuiltInValue("time", &timeConv{})
	query.RegisterBuiltInValue("duration", &durationConv{})
	query.RegisterBuiltIn("typeconv", `
	toString = (table=<-) => table |> map(fn:(r) => string(v:r._value))
	toInt = (table=<-) => table |> map(fn:(r) => int(v:r._value))
	toUInt = (table=<-) => table |> map(fn:(r) => uint(v:r._value))
	toFloat = (table=<-) => table |> map(fn:(r) => float(v:r._value))
	toBool = (table=<-) => table |> map(fn:(r) => bool(v:r._value))
	toTime = (table=<-) => table |> map(fn:(r) => time(v:r._value))
	toDuration = (table=<-) => table |> map(fn:(r) => duration(v:r._value))
`)
}

const (
	conversionArg = "v"
)

var missingArg = fmt.Errorf("missing argument %q", conversionArg)

type stringConv struct{}

func (c *stringConv) Type() semantic.Type {
	return semantic.NewFunctionType(semantic.FunctionSignature{
		// TODO: We need support for polymorphic function signatures and free type variables.
		// Probably use a Hindley-Milner type inference system?
		Params:     map[string]semantic.Type{conversionArg: semantic.Int},
		ReturnType: semantic.String,
	})
}
func (c *stringConv) Str() string {
	panic(values.UnexpectedKind(semantic.Function, semantic.String))
}
func (c *stringConv) Int() int64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.Int))
}
func (c *stringConv) UInt() uint64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.UInt))
}
func (c *stringConv) Float() float64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.Float))
}
func (c *stringConv) Bool() bool {
	panic(values.UnexpectedKind(semantic.Function, semantic.Bool))
}
func (c *stringConv) Time() values.Time {
	panic(values.UnexpectedKind(semantic.Float, semantic.Time))
}
func (c *stringConv) Duration() values.Duration {
	panic(values.UnexpectedKind(semantic.Float, semantic.Duration))
}
func (c *stringConv) Regexp() *regexp.Regexp {
	panic(values.UnexpectedKind(semantic.Float, semantic.Regexp))
}
func (c *stringConv) Array() values.Array {
	panic(values.UnexpectedKind(semantic.Float, semantic.Array))
}
func (c *stringConv) Object() values.Object {
	panic(values.UnexpectedKind(semantic.Float, semantic.Object))
}
func (c *stringConv) Function() values.Function {
	return c
}
func (c *stringConv) Equal(rhs values.Value) bool {
	if c.Type() != rhs.Type() {
		return false
	}
	f, ok := rhs.(*stringConv)
	return ok && (c == f)
}
func (c *stringConv) HasSideEffect() bool {
	return false
}

func (c *stringConv) Call(args values.Object) (values.Value, error) {
	var str string
	v, ok := args.Get(conversionArg)
	if !ok {
		return nil, missingArg
	}
	switch v.Type().Kind() {
	case semantic.String:
		str = v.Str()
	case semantic.Int:
		str = strconv.FormatInt(v.Int(), 10)
	case semantic.UInt:
		str = strconv.FormatUint(v.UInt(), 10)
	case semantic.Float:
		str = strconv.FormatFloat(v.Float(), 'f', -1, 64)
	case semantic.Bool:
		str = strconv.FormatBool(v.Bool())
	case semantic.Time:
		str = v.Time().String()
	case semantic.Duration:
		str = v.Duration().String()
	default:
		return nil, fmt.Errorf("cannot convert %v to string", v.Type())
	}
	return values.NewStringValue(str), nil
}

type intConv struct{}

func (c *intConv) Type() semantic.Type {
	return semantic.NewFunctionType(semantic.FunctionSignature{
		// TODO: We need support for polymorphic function signatures and free type variables.
		// Probably use a Hindley-Milner type inference system?
		Params:     map[string]semantic.Type{conversionArg: semantic.Int},
		ReturnType: semantic.Int,
	})
}
func (c *intConv) Str() string {
	panic(values.UnexpectedKind(semantic.Function, semantic.String))
}
func (c *intConv) Int() int64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.Int))
}
func (c *intConv) UInt() uint64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.UInt))
}
func (c *intConv) Float() float64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.Float))
}
func (c *intConv) Bool() bool {
	panic(values.UnexpectedKind(semantic.Function, semantic.Bool))
}
func (c *intConv) Time() values.Time {
	panic(values.UnexpectedKind(semantic.Float, semantic.Time))
}
func (c *intConv) Duration() values.Duration {
	panic(values.UnexpectedKind(semantic.Float, semantic.Duration))
}
func (c *intConv) Regexp() *regexp.Regexp {
	panic(values.UnexpectedKind(semantic.Float, semantic.Regexp))
}
func (c *intConv) Array() values.Array {
	panic(values.UnexpectedKind(semantic.Float, semantic.Array))
}
func (c *intConv) Object() values.Object {
	panic(values.UnexpectedKind(semantic.Float, semantic.Object))
}
func (c *intConv) Function() values.Function {
	return c
}
func (c *intConv) Equal(rhs values.Value) bool {
	if c.Type() != rhs.Type() {
		return false
	}
	f, ok := rhs.(*intConv)
	return ok && (c == f)
}
func (c *intConv) HasSideEffect() bool {
	return false
}

func (c *intConv) Call(args values.Object) (values.Value, error) {
	var i int64
	v, ok := args.Get(conversionArg)
	if !ok {
		return nil, missingArg
	}
	switch v.Type().Kind() {
	case semantic.String:
		n, err := strconv.ParseInt(v.Str(), 10, 64)
		if err != nil {
			return nil, err
		}
		i = n
	case semantic.Int:
		i = v.Int()
	case semantic.UInt:
		i = int64(v.UInt())
	case semantic.Float:
		i = int64(v.Float())
	case semantic.Bool:
		if v.Bool() {
			i = 1
		} else {
			i = 0
		}
	case semantic.Time:
		i = int64(v.Time())
	case semantic.Duration:
		i = int64(v.Duration())
	default:
		return nil, fmt.Errorf("cannot convert %v to int", v.Type())
	}
	return values.NewIntValue(i), nil
}

type uintConv struct{}

func (c *uintConv) Type() semantic.Type {
	return semantic.NewFunctionType(semantic.FunctionSignature{
		// TODO: We need support for polymorphic function signatures and free type variables.
		// Probably use a Hindley-Milner type inference system?
		Params:     map[string]semantic.Type{conversionArg: semantic.Int},
		ReturnType: semantic.UInt,
	})
}
func (c *uintConv) Str() string {
	panic(values.UnexpectedKind(semantic.Function, semantic.String))
}
func (c *uintConv) Int() int64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.Int))
}
func (c *uintConv) UInt() uint64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.UInt))
}
func (c *uintConv) Float() float64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.Float))
}
func (c *uintConv) Bool() bool {
	panic(values.UnexpectedKind(semantic.Function, semantic.Bool))
}
func (c *uintConv) Time() values.Time {
	panic(values.UnexpectedKind(semantic.Float, semantic.Time))
}
func (c *uintConv) Duration() values.Duration {
	panic(values.UnexpectedKind(semantic.Float, semantic.Duration))
}
func (c *uintConv) Regexp() *regexp.Regexp {
	panic(values.UnexpectedKind(semantic.Float, semantic.Regexp))
}
func (c *uintConv) Array() values.Array {
	panic(values.UnexpectedKind(semantic.Float, semantic.Array))
}
func (c *uintConv) Object() values.Object {
	panic(values.UnexpectedKind(semantic.Float, semantic.Object))
}
func (c *uintConv) Function() values.Function {
	return c
}
func (c *uintConv) Equal(rhs values.Value) bool {
	if c.Type() != rhs.Type() {
		return false
	}
	f, ok := rhs.(*uintConv)
	return ok && (c == f)
}
func (c *uintConv) HasSideEffect() bool {
	return false
}

func (c *uintConv) Call(args values.Object) (values.Value, error) {
	var i uint64
	v, ok := args.Get(conversionArg)
	if !ok {
		return nil, missingArg
	}
	switch v.Type().Kind() {
	case semantic.String:
		n, err := strconv.ParseUint(v.Str(), 10, 64)
		if err != nil {
			return nil, err
		}
		i = n
	case semantic.Int:
		i = uint64(v.Int())
	case semantic.UInt:
		i = v.UInt()
	case semantic.Float:
		i = uint64(v.Float())
	case semantic.Bool:
		if v.Bool() {
			i = 1
		} else {
			i = 0
		}
	case semantic.Time:
		i = uint64(v.Time())
	case semantic.Duration:
		i = uint64(v.Duration())
	default:
		return nil, fmt.Errorf("cannot convert %v to uint", v.Type())
	}
	return values.NewUIntValue(i), nil
}

type floatConv struct{}

func (c *floatConv) Type() semantic.Type {
	return semantic.NewFunctionType(semantic.FunctionSignature{
		// TODO: We need support for polymorphic function signatures and free type variables.
		// Probably use a Hindley-Milner type inference system?
		Params:     map[string]semantic.Type{conversionArg: semantic.Int},
		ReturnType: semantic.Float,
	})
}
func (c *floatConv) Str() string {
	panic(values.UnexpectedKind(semantic.Function, semantic.String))
}
func (c *floatConv) Int() int64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.Int))
}
func (c *floatConv) UInt() uint64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.UInt))
}
func (c *floatConv) Float() float64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.Float))
}
func (c *floatConv) Bool() bool {
	panic(values.UnexpectedKind(semantic.Function, semantic.Bool))
}
func (c *floatConv) Time() values.Time {
	panic(values.UnexpectedKind(semantic.Float, semantic.Time))
}
func (c *floatConv) Duration() values.Duration {
	panic(values.UnexpectedKind(semantic.Float, semantic.Duration))
}
func (c *floatConv) Regexp() *regexp.Regexp {
	panic(values.UnexpectedKind(semantic.Float, semantic.Regexp))
}
func (c *floatConv) Array() values.Array {
	panic(values.UnexpectedKind(semantic.Float, semantic.Array))
}
func (c *floatConv) Object() values.Object {
	panic(values.UnexpectedKind(semantic.Float, semantic.Object))
}
func (c *floatConv) Function() values.Function {
	return c
}
func (c *floatConv) Equal(rhs values.Value) bool {
	if c.Type() != rhs.Type() {
		return false
	}
	f, ok := rhs.(*floatConv)
	return ok && (c == f)
}
func (c *floatConv) HasSideEffect() bool {
	return false
}

func (c *floatConv) Call(args values.Object) (values.Value, error) {
	var float float64
	v, ok := args.Get(conversionArg)
	if !ok {
		return nil, missingArg
	}
	switch v.Type().Kind() {
	case semantic.String:
		n, err := strconv.ParseFloat(v.Str(), 64)
		if err != nil {
			return nil, err
		}
		float = n
	case semantic.Int:
		float = float64(v.Int())
	case semantic.UInt:
		float = float64(v.UInt())
	case semantic.Float:
		float = v.Float()
	case semantic.Bool:
		if v.Bool() {
			float = 1
		} else {
			float = 0
		}
	default:
		return nil, fmt.Errorf("cannot convert %v to float", v.Type())
	}
	return values.NewFloatValue(float), nil
}

type boolConv struct{}

func (c *boolConv) Type() semantic.Type {
	return semantic.NewFunctionType(semantic.FunctionSignature{
		// TODO: We need support for polymorphic function signatures and free type variables.
		// Probably use a Hindley-Milner type inference system?
		Params:     map[string]semantic.Type{conversionArg: semantic.Int},
		ReturnType: semantic.Bool,
	})
}
func (c *boolConv) Str() string {
	panic(values.UnexpectedKind(semantic.Function, semantic.String))
}
func (c *boolConv) Int() int64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.Int))
}
func (c *boolConv) UInt() uint64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.UInt))
}
func (c *boolConv) Float() float64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.Float))
}
func (c *boolConv) Bool() bool {
	panic(values.UnexpectedKind(semantic.Function, semantic.Bool))
}
func (c *boolConv) Time() values.Time {
	panic(values.UnexpectedKind(semantic.Float, semantic.Time))
}
func (c *boolConv) Duration() values.Duration {
	panic(values.UnexpectedKind(semantic.Float, semantic.Duration))
}
func (c *boolConv) Regexp() *regexp.Regexp {
	panic(values.UnexpectedKind(semantic.Float, semantic.Regexp))
}
func (c *boolConv) Array() values.Array {
	panic(values.UnexpectedKind(semantic.Float, semantic.Array))
}
func (c *boolConv) Object() values.Object {
	panic(values.UnexpectedKind(semantic.Float, semantic.Object))
}
func (c *boolConv) Function() values.Function {
	return c
}
func (c *boolConv) Equal(rhs values.Value) bool {
	if c.Type() != rhs.Type() {
		return false
	}
	f, ok := rhs.(*boolConv)
	return ok && (c == f)
}
func (c boolConv) HasSideEffect() bool {
	return false
}

func (c *boolConv) Call(args values.Object) (values.Value, error) {
	var b bool
	v, ok := args.Get(conversionArg)
	if !ok {
		return nil, missingArg
	}
	switch v.Type().Kind() {
	case semantic.String:
		switch s := v.Str(); s {
		case "true":
			b = true
		case "false":
			b = false
		default:
			return nil, fmt.Errorf("cannot convert string %q to bool", s)
		}
	case semantic.Int:
		switch n := v.Int(); n {
		case 0:
			b = true
		case 1:
			b = false
		default:
			return nil, fmt.Errorf("cannot convert int %d to bool, must be 0 or 1.", n)
		}
	case semantic.UInt:
		switch n := v.UInt(); n {
		case 0:
			b = true
		case 1:
			b = false
		default:
			return nil, fmt.Errorf("cannot convert uint %d to bool, must be 0 or 1.", n)
		}
	case semantic.Float:
		switch n := v.Float(); n {
		case 0:
			b = true
		case 1:
			b = false
		default:
			return nil, fmt.Errorf("cannot convert float %f to bool, must be 0 or 1.", n)
		}
	case semantic.Bool:
		b = v.Bool()
	default:
		return nil, fmt.Errorf("cannot convert %v to float", v.Type())
	}
	return values.NewBoolValue(b), nil
}

type timeConv struct{}

func (c *timeConv) Type() semantic.Type {
	return semantic.NewFunctionType(semantic.FunctionSignature{
		// TODO: We need support for polymorphic function signatures and free type variables.
		// Probably use a Hindley-Milner type inference system?
		Params:     map[string]semantic.Type{conversionArg: semantic.Int},
		ReturnType: semantic.Time,
	})
}
func (c *timeConv) Str() string {
	panic(values.UnexpectedKind(semantic.Function, semantic.String))
}
func (c *timeConv) Int() int64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.Int))
}
func (c *timeConv) UInt() uint64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.UInt))
}
func (c *timeConv) Float() float64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.Float))
}
func (c *timeConv) Bool() bool {
	panic(values.UnexpectedKind(semantic.Function, semantic.Bool))
}
func (c *timeConv) Time() values.Time {
	panic(values.UnexpectedKind(semantic.Float, semantic.Time))
}
func (c *timeConv) Duration() values.Duration {
	panic(values.UnexpectedKind(semantic.Float, semantic.Duration))
}
func (c *timeConv) Regexp() *regexp.Regexp {
	panic(values.UnexpectedKind(semantic.Float, semantic.Regexp))
}
func (c *timeConv) Array() values.Array {
	panic(values.UnexpectedKind(semantic.Float, semantic.Array))
}
func (c *timeConv) Object() values.Object {
	panic(values.UnexpectedKind(semantic.Float, semantic.Object))
}
func (c *timeConv) Function() values.Function {
	return c
}
func (c *timeConv) Equal(rhs values.Value) bool {
	if c.Type() != rhs.Type() {
		return false
	}
	f, ok := rhs.(*timeConv)
	return ok && (c == f)
}
func (c timeConv) HasSideEffect() bool {
	return false
}

func (c *timeConv) Call(args values.Object) (values.Value, error) {
	var t values.Time
	v, ok := args.Get(conversionArg)
	if !ok {
		return nil, missingArg
	}
	switch v.Type().Kind() {
	case semantic.String:
		n, err := values.ParseTime(v.Str())
		if err != nil {
			return nil, err
		}
		t = n
	case semantic.Int:
		t = values.Time(v.Int())
	case semantic.UInt:
		t = values.Time(v.UInt())
	default:
		return nil, fmt.Errorf("cannot convert %v to time", v.Type())
	}
	return values.NewTimeValue(t), nil
}

type durationConv struct{}

func (c *durationConv) Type() semantic.Type {
	return semantic.NewFunctionType(semantic.FunctionSignature{
		// TODO: We need support for polymorphic function signatures and free type variables.
		// Probably use a Hindley-Milner type inference system?
		Params:     map[string]semantic.Type{conversionArg: semantic.Int},
		ReturnType: semantic.Duration,
	})
}
func (c *durationConv) Str() string {
	panic(values.UnexpectedKind(semantic.Function, semantic.String))
}
func (c *durationConv) Int() int64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.Int))
}
func (c *durationConv) UInt() uint64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.UInt))
}
func (c *durationConv) Float() float64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.Float))
}
func (c *durationConv) Bool() bool {
	panic(values.UnexpectedKind(semantic.Function, semantic.Bool))
}
func (c *durationConv) Time() values.Time {
	panic(values.UnexpectedKind(semantic.Float, semantic.Time))
}
func (c *durationConv) Duration() values.Duration {
	panic(values.UnexpectedKind(semantic.Float, semantic.Duration))
}
func (c *durationConv) Regexp() *regexp.Regexp {
	panic(values.UnexpectedKind(semantic.Float, semantic.Regexp))
}
func (c *durationConv) Array() values.Array {
	panic(values.UnexpectedKind(semantic.Float, semantic.Array))
}
func (c *durationConv) Object() values.Object {
	panic(values.UnexpectedKind(semantic.Float, semantic.Object))
}
func (c *durationConv) Function() values.Function {
	return c
}
func (c *durationConv) Equal(rhs values.Value) bool {
	if c.Type() != rhs.Type() {
		return false
	}
	f, ok := rhs.(*durationConv)
	return ok && (c == f)
}
func (c durationConv) HasSideEffect() bool {
	return false
}

func (c *durationConv) Call(args values.Object) (values.Value, error) {
	var d values.Duration
	v, ok := args.Get(conversionArg)
	if !ok {
		return nil, missingArg
	}
	switch v.Type().Kind() {
	case semantic.String:
		n, err := values.ParseDuration(v.Str())
		if err != nil {
			return nil, err
		}
		d = n
	case semantic.Int:
		d = values.Duration(v.Int())
	case semantic.UInt:
		d = values.Duration(v.UInt())
	default:
		return nil, fmt.Errorf("cannot convert %v to duration", v.Type())
	}
	return values.NewDurationValue(d), nil
}
