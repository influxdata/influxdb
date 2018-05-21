package compiler

import (
	"fmt"
	"regexp"

	"github.com/influxdata/ifql/ast"
	"github.com/influxdata/ifql/semantic"
	"github.com/influxdata/ifql/values"
)

type Evaluator interface {
	Type() semantic.Type
	EvalString(scope Scope) string
	EvalInt(scope Scope) int64
	EvalUInt(scope Scope) uint64
	EvalFloat(scope Scope) float64
	EvalBool(scope Scope) bool
	EvalTime(scope Scope) values.Time
	EvalDuration(scope Scope) values.Duration
	EvalRegexp(scope Scope) *regexp.Regexp
	EvalArray(scope Scope) values.Array
	EvalObject(scope Scope) values.Object
	EvalFunction(scope Scope) values.Function
}

type Func interface {
	Type() semantic.Type
	EvalString(scope Scope) (string, error)
	Eval(scope Scope) (values.Value, error)
	EvalInt(scope Scope) (int64, error)
	EvalUInt(scope Scope) (uint64, error)
	EvalFloat(scope Scope) (float64, error)
	EvalBool(scope Scope) (bool, error)
	EvalTime(scope Scope) (values.Time, error)
	EvalDuration(scope Scope) (values.Duration, error)
	EvalRegexp(scope Scope) (*regexp.Regexp, error)
	EvalArray(scope Scope) (values.Array, error)
	EvalObject(scope Scope) (values.Object, error)
	EvalFunction(scope Scope) (values.Function, error)
}

type compiledFn struct {
	root    Evaluator
	inTypes map[string]semantic.Type
}

func (c compiledFn) validate(scope Scope) error {
	// Validate scope
	for k, t := range c.inTypes {
		if scope.Type(k) != t {
			return fmt.Errorf("missing or incorrectly typed value found in scope for name %q", k)
		}
	}
	return nil
}

func (c compiledFn) Type() semantic.Type {
	return c.root.Type()
}

func (c compiledFn) Eval(scope Scope) (values.Value, error) {
	if err := c.validate(scope); err != nil {
		return nil, err
	}
	switch c.Type().Kind() {
	case semantic.String:
		return values.NewStringValue(c.root.EvalString(scope)), nil
	case semantic.Int:
		return values.NewIntValue(c.root.EvalInt(scope)), nil
	case semantic.UInt:
		return values.NewUIntValue(c.root.EvalUInt(scope)), nil
	case semantic.Float:
		return values.NewFloatValue(c.root.EvalFloat(scope)), nil
	case semantic.Bool:
		return values.NewBoolValue(c.root.EvalBool(scope)), nil
	case semantic.Time:
		return values.NewTimeValue(c.root.EvalTime(scope)), nil
	case semantic.Duration:
		return values.NewDurationValue(c.root.EvalDuration(scope)), nil
	case semantic.Regexp:
		return values.NewRegexpValue(c.root.EvalRegexp(scope)), nil
	case semantic.Array:
		return c.root.EvalArray(scope), nil
	case semantic.Object:
		return c.root.EvalObject(scope), nil
	case semantic.Function:
		return c.root.EvalFunction(scope), nil
	default:
		return nil, fmt.Errorf("unsupported kind %s", c.Type().Kind())
	}
}

func (c compiledFn) EvalString(scope Scope) (string, error) {
	if err := c.validate(scope); err != nil {
		return "", err
	}
	return c.root.EvalString(scope), nil
}
func (c compiledFn) EvalBool(scope Scope) (bool, error) {
	if err := c.validate(scope); err != nil {
		return false, err
	}
	return c.root.EvalBool(scope), nil
}
func (c compiledFn) EvalInt(scope Scope) (int64, error) {
	if err := c.validate(scope); err != nil {
		return 0, err
	}
	return c.root.EvalInt(scope), nil
}
func (c compiledFn) EvalUInt(scope Scope) (uint64, error) {
	if err := c.validate(scope); err != nil {
		return 0, err
	}
	return c.root.EvalUInt(scope), nil
}
func (c compiledFn) EvalFloat(scope Scope) (float64, error) {
	if err := c.validate(scope); err != nil {
		return 0, err
	}
	return c.root.EvalFloat(scope), nil
}
func (c compiledFn) EvalTime(scope Scope) (values.Time, error) {
	if err := c.validate(scope); err != nil {
		return 0, err
	}
	return c.root.EvalTime(scope), nil
}
func (c compiledFn) EvalDuration(scope Scope) (values.Duration, error) {
	if err := c.validate(scope); err != nil {
		return 0, err
	}
	return c.root.EvalDuration(scope), nil
}
func (c compiledFn) EvalRegexp(scope Scope) (*regexp.Regexp, error) {
	if err := c.validate(scope); err != nil {
		return nil, err
	}
	return c.root.EvalRegexp(scope), nil
}
func (c compiledFn) EvalArray(scope Scope) (values.Array, error) {
	if err := c.validate(scope); err != nil {
		return nil, err
	}
	return c.root.EvalArray(scope), nil
}
func (c compiledFn) EvalObject(scope Scope) (values.Object, error) {
	if err := c.validate(scope); err != nil {
		return nil, err
	}
	return c.root.EvalObject(scope), nil
}
func (c compiledFn) EvalFunction(scope Scope) (values.Function, error) {
	if err := c.validate(scope); err != nil {
		return nil, err
	}
	return c.root.EvalFunction(scope), nil
}

type Scope map[string]values.Value

func (s Scope) Type(name string) semantic.Type {
	return s[name].Type()
}
func (s Scope) Set(name string, v values.Value) {
	s[name] = v
}

func (s Scope) GetString(name string) string {
	return s[name].Str()
}
func (s Scope) GetInt(name string) int64 {
	return s[name].Int()
}
func (s Scope) GetUInt(name string) uint64 {
	return s[name].UInt()
}
func (s Scope) GetFloat(name string) float64 {
	return s[name].Float()
}
func (s Scope) GetBool(name string) bool {
	return s[name].Bool()
}
func (s Scope) GetTime(name string) values.Time {
	return s[name].Time()
}
func (s Scope) GetDuration(name string) values.Duration {
	return s[name].Duration()
}
func (s Scope) GetRegexp(name string) *regexp.Regexp {
	return s[name].Regexp()
}
func (s Scope) GetArray(name string) values.Array {
	return s[name].Array()
}
func (s Scope) GetObject(name string) values.Object {
	return s[name].Object()
}
func (s Scope) GetFunction(name string) values.Function {
	return s[name].Function()
}

func (s Scope) Copy() Scope {
	n := make(Scope, len(s))
	for k, v := range s {
		n[k] = v
	}
	return n
}

func eval(e Evaluator, scope Scope) values.Value {
	switch e.Type().Kind() {
	case semantic.String:
		return values.NewStringValue(e.EvalString(scope))
	case semantic.Int:
		return values.NewIntValue(e.EvalInt(scope))
	case semantic.UInt:
		return values.NewUIntValue(e.EvalUInt(scope))
	case semantic.Float:
		return values.NewFloatValue(e.EvalFloat(scope))
	case semantic.Bool:
		return values.NewBoolValue(e.EvalBool(scope))
	case semantic.Time:
		return values.NewTimeValue(e.EvalTime(scope))
	case semantic.Duration:
		return values.NewDurationValue(e.EvalDuration(scope))
	case semantic.Regexp:
		return values.NewRegexpValue(e.EvalRegexp(scope))
	case semantic.Array:
		return e.EvalArray(scope)
	case semantic.Object:
		return e.EvalObject(scope)
	case semantic.Function:
		return e.EvalFunction(scope)
	default:
		return nil
	}
}

type blockEvaluator struct {
	t     semantic.Type
	body  []Evaluator
	value values.Value
}

func (e *blockEvaluator) Type() semantic.Type {
	return e.t
}

func (e *blockEvaluator) eval(scope Scope) {
	for _, b := range e.body {
		e.value = eval(b, scope)
	}
}

func (e *blockEvaluator) EvalString(scope Scope) string {
	values.CheckKind(e.t.Kind(), semantic.String)
	e.eval(scope)
	return e.value.Str()
}
func (e *blockEvaluator) EvalInt(scope Scope) int64 {
	values.CheckKind(e.t.Kind(), semantic.Int)
	e.eval(scope)
	return e.value.Int()
}
func (e *blockEvaluator) EvalUInt(scope Scope) uint64 {
	values.CheckKind(e.t.Kind(), semantic.UInt)
	e.eval(scope)
	return e.value.UInt()
}
func (e *blockEvaluator) EvalFloat(scope Scope) float64 {
	values.CheckKind(e.t.Kind(), semantic.Float)
	e.eval(scope)
	return e.value.Float()
}
func (e *blockEvaluator) EvalBool(scope Scope) bool {
	values.CheckKind(e.t.Kind(), semantic.Bool)
	e.eval(scope)
	return e.value.Bool()
}
func (e *blockEvaluator) EvalTime(scope Scope) values.Time {
	values.CheckKind(e.t.Kind(), semantic.Time)
	e.eval(scope)
	return e.value.Time()
}
func (e *blockEvaluator) EvalDuration(scope Scope) values.Duration {
	values.CheckKind(e.t.Kind(), semantic.Duration)
	e.eval(scope)
	return e.value.Duration()
}
func (e *blockEvaluator) EvalRegexp(scope Scope) *regexp.Regexp {
	values.CheckKind(e.t.Kind(), semantic.Regexp)
	e.eval(scope)
	return e.value.Regexp()
}
func (e *blockEvaluator) EvalArray(scope Scope) values.Array {
	values.CheckKind(e.t.Kind(), semantic.Object)
	e.eval(scope)
	return e.value.Array()
}
func (e *blockEvaluator) EvalObject(scope Scope) values.Object {
	values.CheckKind(e.t.Kind(), semantic.Object)
	e.eval(scope)
	return e.value.Object()
}
func (e *blockEvaluator) EvalFunction(scope Scope) values.Function {
	values.CheckKind(e.t.Kind(), semantic.Object)
	e.eval(scope)
	return e.value.Function()
}

type returnEvaluator struct {
	Evaluator
}

type declarationEvaluator struct {
	t    semantic.Type
	id   string
	init Evaluator
}

func (e *declarationEvaluator) Type() semantic.Type {
	return e.t
}

func (e *declarationEvaluator) eval(scope Scope) {
	scope.Set(e.id, eval(e.init, scope))
}

func (e *declarationEvaluator) EvalString(scope Scope) string {
	e.eval(scope)
	return scope.GetString(e.id)
}
func (e *declarationEvaluator) EvalInt(scope Scope) int64 {
	e.eval(scope)
	return scope.GetInt(e.id)
}
func (e *declarationEvaluator) EvalUInt(scope Scope) uint64 {
	e.eval(scope)
	return scope.GetUInt(e.id)
}
func (e *declarationEvaluator) EvalFloat(scope Scope) float64 {
	e.eval(scope)
	return scope.GetFloat(e.id)
}
func (e *declarationEvaluator) EvalBool(scope Scope) bool {
	e.eval(scope)
	return scope.GetBool(e.id)
}
func (e *declarationEvaluator) EvalTime(scope Scope) values.Time {
	e.eval(scope)
	return scope.GetTime(e.id)
}
func (e *declarationEvaluator) EvalDuration(scope Scope) values.Duration {
	e.eval(scope)
	return scope.GetDuration(e.id)
}
func (e *declarationEvaluator) EvalRegexp(scope Scope) *regexp.Regexp {
	e.eval(scope)
	return scope.GetRegexp(e.id)
}
func (e *declarationEvaluator) EvalArray(scope Scope) values.Array {
	e.eval(scope)
	return scope.GetArray(e.id)
}
func (e *declarationEvaluator) EvalObject(scope Scope) values.Object {
	e.eval(scope)
	return scope.GetObject(e.id)
}
func (e *declarationEvaluator) EvalFunction(scope Scope) values.Function {
	e.eval(scope)
	return scope.GetFunction(e.id)
}

type objEvaluator struct {
	t          semantic.Type
	properties map[string]Evaluator
}

func (e *objEvaluator) Type() semantic.Type {
	return e.t
}

func (e *objEvaluator) EvalString(scope Scope) string {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.String))
}
func (e *objEvaluator) EvalInt(scope Scope) int64 {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Int))
}
func (e *objEvaluator) EvalUInt(scope Scope) uint64 {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.UInt))
}
func (e *objEvaluator) EvalFloat(scope Scope) float64 {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Float))
}
func (e *objEvaluator) EvalBool(scope Scope) bool {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Bool))
}
func (e *objEvaluator) EvalTime(scope Scope) values.Time {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Time))
}
func (e *objEvaluator) EvalDuration(scope Scope) values.Duration {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Duration))
}
func (e *objEvaluator) EvalRegexp(scope Scope) *regexp.Regexp {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Regexp))
}
func (e *objEvaluator) EvalArray(scope Scope) values.Array {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Array))
}
func (e *objEvaluator) EvalObject(scope Scope) values.Object {
	obj := values.NewObject()
	for k, node := range e.properties {
		v := eval(node, scope)
		obj.Set(k, v)
	}
	return obj
}
func (e *objEvaluator) EvalFunction(scope Scope) values.Function {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Function))
}

type logicalEvaluator struct {
	t           semantic.Type
	operator    ast.LogicalOperatorKind
	left, right Evaluator
}

func (e *logicalEvaluator) Type() semantic.Type {
	return e.t
}

func (e *logicalEvaluator) EvalString(scope Scope) string {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.String))
}
func (e *logicalEvaluator) EvalInt(scope Scope) int64 {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Int))
}
func (e *logicalEvaluator) EvalUInt(scope Scope) uint64 {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.UInt))
}
func (e *logicalEvaluator) EvalFloat(scope Scope) float64 {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Float))
}
func (e *logicalEvaluator) EvalBool(scope Scope) bool {
	switch e.operator {
	case ast.AndOperator:
		return e.left.EvalBool(scope) && e.right.EvalBool(scope)
	case ast.OrOperator:
		return e.left.EvalBool(scope) || e.right.EvalBool(scope)
	default:
		panic(fmt.Errorf("unknown logical operator %v", e.operator))
	}
}
func (e *logicalEvaluator) EvalTime(scope Scope) values.Time {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Time))
}
func (e *logicalEvaluator) EvalDuration(scope Scope) values.Duration {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Duration))
}
func (e *logicalEvaluator) EvalRegexp(scope Scope) *regexp.Regexp {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Regexp))
}
func (e *logicalEvaluator) EvalArray(scope Scope) values.Array {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Array))
}
func (e *logicalEvaluator) EvalObject(scope Scope) values.Object {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Object))
}
func (e *logicalEvaluator) EvalFunction(scope Scope) values.Function {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Function))
}

type binaryFunc func(scope Scope, left, right Evaluator) values.Value

type binarySignature struct {
	Operator    ast.OperatorKind
	Left, Right semantic.Type
}

type binaryEvaluator struct {
	t           semantic.Type
	left, right Evaluator
	f           values.BinaryFunction
}

func (e *binaryEvaluator) Type() semantic.Type {
	return e.t
}

func (e *binaryEvaluator) eval(scope Scope) (l, r values.Value) {
	return eval(e.left, scope), eval(e.right, scope)
}

func (e *binaryEvaluator) EvalString(scope Scope) string {
	return e.f(e.eval(scope)).Str()
}
func (e *binaryEvaluator) EvalInt(scope Scope) int64 {
	return e.f(e.eval(scope)).Int()
}
func (e *binaryEvaluator) EvalUInt(scope Scope) uint64 {
	return e.f(e.eval(scope)).UInt()
}
func (e *binaryEvaluator) EvalFloat(scope Scope) float64 {
	return e.f(e.eval(scope)).Float()
}
func (e *binaryEvaluator) EvalBool(scope Scope) bool {
	return e.f(e.eval(scope)).Bool()
}
func (e *binaryEvaluator) EvalTime(scope Scope) values.Time {
	return e.f(e.eval(scope)).Time()
}
func (e *binaryEvaluator) EvalDuration(scope Scope) values.Duration {
	return e.f(e.eval(scope)).Duration()
}
func (e *binaryEvaluator) EvalRegexp(scope Scope) *regexp.Regexp {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Regexp))
}
func (e *binaryEvaluator) EvalArray(scope Scope) values.Array {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Array))
}
func (e *binaryEvaluator) EvalObject(scope Scope) values.Object {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Object))
}
func (e *binaryEvaluator) EvalFunction(scope Scope) values.Function {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Function))
}

type unaryEvaluator struct {
	t    semantic.Type
	node Evaluator
}

func (e *unaryEvaluator) Type() semantic.Type {
	return e.t
}

func (e *unaryEvaluator) EvalString(scope Scope) string {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.String))
}
func (e *unaryEvaluator) EvalInt(scope Scope) int64 {
	// There is only one integer unary operator
	return -e.node.EvalInt(scope)
}
func (e *unaryEvaluator) EvalUInt(scope Scope) uint64 {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.UInt))
}
func (e *unaryEvaluator) EvalFloat(scope Scope) float64 {
	// There is only one float unary operator
	return -e.node.EvalFloat(scope)
}
func (e *unaryEvaluator) EvalBool(scope Scope) bool {
	// There is only one boolean unary operator
	return !e.node.EvalBool(scope)
}
func (e *unaryEvaluator) EvalTime(scope Scope) values.Time {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Time))
}
func (e *unaryEvaluator) EvalDuration(scope Scope) values.Duration {
	// There is only one duration unary operator
	return -e.node.EvalDuration(scope)
}
func (e *unaryEvaluator) EvalRegexp(scope Scope) *regexp.Regexp {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Regexp))
}
func (e *unaryEvaluator) EvalArray(scope Scope) values.Array {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Array))
}
func (e *unaryEvaluator) EvalObject(scope Scope) values.Object {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Object))
}
func (e *unaryEvaluator) EvalFunction(scope Scope) values.Function {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Function))
}

type integerEvaluator struct {
	t semantic.Type
	i int64
}

func (e *integerEvaluator) Type() semantic.Type {
	return e.t
}

func (e *integerEvaluator) EvalString(scope Scope) string {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.String))
}
func (e *integerEvaluator) EvalInt(scope Scope) int64 {
	return e.i
}
func (e *integerEvaluator) EvalUInt(scope Scope) uint64 {
	return uint64(e.i)
}
func (e *integerEvaluator) EvalFloat(scope Scope) float64 {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Float))
}
func (e *integerEvaluator) EvalBool(scope Scope) bool {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Bool))
}
func (e *integerEvaluator) EvalTime(scope Scope) values.Time {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Time))
}
func (e *integerEvaluator) EvalDuration(scope Scope) values.Duration {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Duration))
}
func (e *integerEvaluator) EvalRegexp(scope Scope) *regexp.Regexp {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Regexp))
}
func (e *integerEvaluator) EvalArray(scope Scope) values.Array {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Array))
}
func (e *integerEvaluator) EvalObject(scope Scope) values.Object {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Object))
}
func (e *integerEvaluator) EvalFunction(scope Scope) values.Function {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Function))
}

type stringEvaluator struct {
	t semantic.Type
	s string
}

func (e *stringEvaluator) Type() semantic.Type {
	return e.t
}

func (e *stringEvaluator) EvalString(scope Scope) string {
	return e.s
}
func (e *stringEvaluator) EvalInt(scope Scope) int64 {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Int))
}
func (e *stringEvaluator) EvalUInt(scope Scope) uint64 {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.UInt))
}
func (e *stringEvaluator) EvalFloat(scope Scope) float64 {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Float))
}
func (e *stringEvaluator) EvalBool(scope Scope) bool {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Bool))
}
func (e *stringEvaluator) EvalTime(scope Scope) values.Time {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Time))
}
func (e *stringEvaluator) EvalDuration(scope Scope) values.Duration {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Duration))
}
func (e *stringEvaluator) EvalRegexp(scope Scope) *regexp.Regexp {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Regexp))
}
func (e *stringEvaluator) EvalArray(scope Scope) values.Array {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Array))
}
func (e *stringEvaluator) EvalObject(scope Scope) values.Object {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Object))
}
func (e *stringEvaluator) EvalFunction(scope Scope) values.Function {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Function))
}

type regexpEvaluator struct {
	t semantic.Type
	r *regexp.Regexp
}

func (e *regexpEvaluator) Type() semantic.Type {
	return e.t
}

func (e *regexpEvaluator) EvalString(scope Scope) string {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.String))
}
func (e *regexpEvaluator) EvalInt(scope Scope) int64 {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Int))
}
func (e *regexpEvaluator) EvalUInt(scope Scope) uint64 {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.UInt))
}
func (e *regexpEvaluator) EvalFloat(scope Scope) float64 {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Float))
}
func (e *regexpEvaluator) EvalBool(scope Scope) bool {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Bool))
}
func (e *regexpEvaluator) EvalTime(scope Scope) values.Time {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Time))
}
func (e *regexpEvaluator) EvalDuration(scope Scope) values.Duration {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Duration))
}
func (e *regexpEvaluator) EvalRegexp(scope Scope) *regexp.Regexp {
	return e.r
}
func (e *regexpEvaluator) EvalArray(scope Scope) values.Array {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Array))
}
func (e *regexpEvaluator) EvalObject(scope Scope) values.Object {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Object))
}
func (e *regexpEvaluator) EvalFunction(scope Scope) values.Function {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Function))
}

type booleanEvaluator struct {
	t semantic.Type
	b bool
}

func (e *booleanEvaluator) Type() semantic.Type {
	return e.t
}

func (e *booleanEvaluator) EvalString(scope Scope) string {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.String))
}
func (e *booleanEvaluator) EvalInt(scope Scope) int64 {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Int))
}
func (e *booleanEvaluator) EvalUInt(scope Scope) uint64 {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.UInt))
}
func (e *booleanEvaluator) EvalFloat(scope Scope) float64 {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Float))
}
func (e *booleanEvaluator) EvalBool(scope Scope) bool {
	return e.b
}
func (e *booleanEvaluator) EvalTime(scope Scope) values.Time {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Time))
}
func (e *booleanEvaluator) EvalDuration(scope Scope) values.Duration {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Duration))
}
func (e *booleanEvaluator) EvalRegexp(scope Scope) *regexp.Regexp {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Regexp))
}
func (e *booleanEvaluator) EvalArray(scope Scope) values.Array {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Array))
}
func (e *booleanEvaluator) EvalObject(scope Scope) values.Object {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Object))
}
func (e *booleanEvaluator) EvalFunction(scope Scope) values.Function {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Function))
}

type floatEvaluator struct {
	t semantic.Type
	f float64
}

func (e *floatEvaluator) Type() semantic.Type {
	return e.t
}

func (e *floatEvaluator) EvalString(scope Scope) string {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.String))
}
func (e *floatEvaluator) EvalInt(scope Scope) int64 {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Int))
}
func (e *floatEvaluator) EvalUInt(scope Scope) uint64 {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.UInt))
}
func (e *floatEvaluator) EvalFloat(scope Scope) float64 {
	return e.f
}
func (e *floatEvaluator) EvalBool(scope Scope) bool {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Bool))
}
func (e *floatEvaluator) EvalTime(scope Scope) values.Time {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Time))
}
func (e *floatEvaluator) EvalDuration(scope Scope) values.Duration {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Duration))
}
func (e *floatEvaluator) EvalRegexp(scope Scope) *regexp.Regexp {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Regexp))
}
func (e *floatEvaluator) EvalArray(scope Scope) values.Array {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Array))
}
func (e *floatEvaluator) EvalObject(scope Scope) values.Object {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Object))
}
func (e *floatEvaluator) EvalFunction(scope Scope) values.Function {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Function))
}

type timeEvaluator struct {
	t    semantic.Type
	time values.Time
}

func (e *timeEvaluator) Type() semantic.Type {
	return e.t
}

func (e *timeEvaluator) EvalString(scope Scope) string {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.String))
}
func (e *timeEvaluator) EvalInt(scope Scope) int64 {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Int))
}
func (e *timeEvaluator) EvalUInt(scope Scope) uint64 {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.UInt))
}
func (e *timeEvaluator) EvalFloat(scope Scope) float64 {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Float))
}
func (e *timeEvaluator) EvalBool(scope Scope) bool {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Bool))
}
func (e *timeEvaluator) EvalTime(scope Scope) values.Time {
	return e.time
}
func (e *timeEvaluator) EvalDuration(scope Scope) values.Duration {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Duration))
}
func (e *timeEvaluator) EvalRegexp(scope Scope) *regexp.Regexp {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Regexp))
}
func (e *timeEvaluator) EvalArray(scope Scope) values.Array {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Array))
}
func (e *timeEvaluator) EvalObject(scope Scope) values.Object {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Object))
}
func (e *timeEvaluator) EvalFunction(scope Scope) values.Function {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Function))
}

type identifierEvaluator struct {
	t    semantic.Type
	name string
}

func (e *identifierEvaluator) Type() semantic.Type {
	return e.t
}

func (e *identifierEvaluator) EvalString(scope Scope) string {
	return scope.GetString(e.name)
}
func (e *identifierEvaluator) EvalInt(scope Scope) int64 {
	return scope.GetInt(e.name)
}
func (e *identifierEvaluator) EvalUInt(scope Scope) uint64 {
	return scope.GetUInt(e.name)
}
func (e *identifierEvaluator) EvalFloat(scope Scope) float64 {
	return scope.GetFloat(e.name)
}
func (e *identifierEvaluator) EvalBool(scope Scope) bool {
	return scope.GetBool(e.name)
}
func (e *identifierEvaluator) EvalTime(scope Scope) values.Time {
	return scope.GetTime(e.name)
}
func (e *identifierEvaluator) EvalDuration(scope Scope) values.Duration {
	return scope.GetDuration(e.name)
}
func (e *identifierEvaluator) EvalRegexp(scope Scope) *regexp.Regexp {
	return scope.GetRegexp(e.name)
}
func (e *identifierEvaluator) EvalArray(scope Scope) values.Array {
	return scope.GetArray(e.name)
}
func (e *identifierEvaluator) EvalObject(scope Scope) values.Object {
	return scope.GetObject(e.name)
}
func (e *identifierEvaluator) EvalFunction(scope Scope) values.Function {
	return scope.GetFunction(e.name)
}

type valueEvaluator struct {
	value values.Value
}

func (e *valueEvaluator) Type() semantic.Type {
	return e.value.Type()
}

func (e *valueEvaluator) EvalString(scope Scope) string {
	return e.value.Str()
}
func (e *valueEvaluator) EvalInt(scope Scope) int64 {
	return e.value.Int()
}
func (e *valueEvaluator) EvalUInt(scope Scope) uint64 {
	return e.value.UInt()
}
func (e *valueEvaluator) EvalFloat(scope Scope) float64 {
	return e.value.Float()
}
func (e *valueEvaluator) EvalBool(scope Scope) bool {
	return e.value.Bool()
}
func (e *valueEvaluator) EvalTime(scope Scope) values.Time {
	return e.value.Time()
}
func (e *valueEvaluator) EvalDuration(scope Scope) values.Duration {
	return e.value.Duration()
}
func (e *valueEvaluator) EvalRegexp(scope Scope) *regexp.Regexp {
	return e.value.Regexp()
}
func (e *valueEvaluator) EvalArray(scope Scope) values.Array {
	return e.value.Array()
}
func (e *valueEvaluator) EvalObject(scope Scope) values.Object {
	return e.value.Object()
}
func (e *valueEvaluator) EvalFunction(scope Scope) values.Function {
	return e.value.Function()
}

type memberEvaluator struct {
	t        semantic.Type
	object   Evaluator
	property string
}

func (e *memberEvaluator) Type() semantic.Type {
	return e.t
}

func (e *memberEvaluator) EvalString(scope Scope) string {
	v, _ := e.object.EvalObject(scope).Get(e.property)
	return v.Str()
}
func (e *memberEvaluator) EvalInt(scope Scope) int64 {
	v, _ := e.object.EvalObject(scope).Get(e.property)
	return v.Int()
}
func (e *memberEvaluator) EvalUInt(scope Scope) uint64 {
	v, _ := e.object.EvalObject(scope).Get(e.property)
	return v.UInt()
}
func (e *memberEvaluator) EvalFloat(scope Scope) float64 {
	v, _ := e.object.EvalObject(scope).Get(e.property)
	return v.Float()
}
func (e *memberEvaluator) EvalBool(scope Scope) bool {
	v, _ := e.object.EvalObject(scope).Get(e.property)
	return v.Bool()
}
func (e *memberEvaluator) EvalTime(scope Scope) values.Time {
	v, _ := e.object.EvalObject(scope).Get(e.property)
	return v.Time()
}
func (e *memberEvaluator) EvalDuration(scope Scope) values.Duration {
	v, _ := e.object.EvalObject(scope).Get(e.property)
	return v.Duration()
}
func (e *memberEvaluator) EvalRegexp(scope Scope) *regexp.Regexp {
	v, _ := e.object.EvalObject(scope).Get(e.property)
	return v.Regexp()
}
func (e *memberEvaluator) EvalArray(scope Scope) values.Array {
	v, _ := e.object.EvalObject(scope).Get(e.property)
	return v.Array()
}
func (e *memberEvaluator) EvalObject(scope Scope) values.Object {
	v, _ := e.object.EvalObject(scope).Get(e.property)
	return v.Object()
}
func (e *memberEvaluator) EvalFunction(scope Scope) values.Function {
	v, _ := e.object.EvalObject(scope).Get(e.property)
	return v.Function()
}

type callEvaluator struct {
	t      semantic.Type
	callee Evaluator
	args   Evaluator
}

func (e *callEvaluator) Type() semantic.Type {
	return e.t
}

func (e *callEvaluator) eval(scope Scope) values.Value {
	args := e.args.EvalObject(scope)
	f := e.callee.EvalFunction(scope)
	//TODO(nathanielc): What to do about error when calling functions?
	v, _ := f.Call(args)
	return v
}

func (e *callEvaluator) EvalString(scope Scope) string {
	return e.eval(scope).Str()
}
func (e *callEvaluator) EvalInt(scope Scope) int64 {
	return e.eval(scope).Int()
}
func (e *callEvaluator) EvalUInt(scope Scope) uint64 {
	return e.eval(scope).UInt()
}
func (e *callEvaluator) EvalFloat(scope Scope) float64 {
	return e.eval(scope).Float()
}
func (e *callEvaluator) EvalBool(scope Scope) bool {
	return e.eval(scope).Bool()
}
func (e *callEvaluator) EvalTime(scope Scope) values.Time {
	return e.eval(scope).Time()
}
func (e *callEvaluator) EvalDuration(scope Scope) values.Duration {
	return e.eval(scope).Duration()
}
func (e *callEvaluator) EvalRegexp(scope Scope) *regexp.Regexp {
	return e.eval(scope).Regexp()
}
func (e *callEvaluator) EvalArray(scope Scope) values.Array {
	return e.eval(scope).Array()
}
func (e *callEvaluator) EvalObject(scope Scope) values.Object {
	return e.eval(scope).Object()
}
func (e *callEvaluator) EvalFunction(scope Scope) values.Function {
	return e.eval(scope).Function()
}

type functionEvaluator struct {
	t      semantic.Type
	body   Evaluator
	params []functionParam
}

func (e *functionEvaluator) Type() semantic.Type {
	return e.t
}

func (e *functionEvaluator) EvalString(scope Scope) string {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.String))
}
func (e *functionEvaluator) EvalInt(scope Scope) int64 {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Int))
}
func (e *functionEvaluator) EvalUInt(scope Scope) uint64 {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.UInt))
}
func (e *functionEvaluator) EvalFloat(scope Scope) float64 {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Float))
}
func (e *functionEvaluator) EvalBool(scope Scope) bool {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Bool))
}
func (e *functionEvaluator) EvalTime(scope Scope) values.Time {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Time))
}
func (e *functionEvaluator) EvalDuration(scope Scope) values.Duration {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Duration))
}
func (e *functionEvaluator) EvalRegexp(scope Scope) *regexp.Regexp {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Regexp))
}
func (e *functionEvaluator) EvalArray(scope Scope) values.Array {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Array))
}
func (e *functionEvaluator) EvalObject(scope Scope) values.Object {
	panic(values.UnexpectedKind(e.t.Kind(), semantic.Object))
}
func (e *functionEvaluator) EvalFunction(scope Scope) values.Function {
	return functionValue{
		t:      e.t,
		body:   e.body,
		params: e.params,
		scope:  scope.Copy(),
	}
}

type functionValue struct {
	t      semantic.Type
	body   Evaluator
	params []functionParam
	scope  Scope
}

type functionParam struct {
	Key     string
	Default Evaluator
	Type    semantic.Type
}

func (f functionValue) Type() semantic.Type {
	return f.t
}

func (f functionValue) Str() string {
	panic(values.UnexpectedKind(semantic.Function, semantic.String))
}
func (f functionValue) Int() int64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.Int))
}
func (f functionValue) UInt() uint64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.UInt))
}
func (f functionValue) Float() float64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.Float))
}
func (f functionValue) Bool() bool {
	panic(values.UnexpectedKind(semantic.Function, semantic.Bool))
}
func (f functionValue) Time() values.Time {
	panic(values.UnexpectedKind(semantic.Function, semantic.Time))
}
func (f functionValue) Duration() values.Duration {
	panic(values.UnexpectedKind(semantic.Function, semantic.Duration))
}
func (f functionValue) Regexp() *regexp.Regexp {
	panic(values.UnexpectedKind(semantic.Function, semantic.Regexp))
}
func (f functionValue) Array() values.Array {
	panic(values.UnexpectedKind(semantic.Function, semantic.Array))
}
func (f functionValue) Object() values.Object {
	panic(values.UnexpectedKind(semantic.Function, semantic.Object))
}
func (f functionValue) Function() values.Function {
	return f
}

func (f functionValue) Call(args values.Object) (values.Value, error) {
	scope := f.scope.Copy()
	for _, p := range f.params {
		v, ok := args.Get(p.Key)
		if !ok && p.Default != nil {
			v = eval(p.Default, f.scope)
		}
		scope.Set(p.Key, v)
	}
	return eval(f.body, scope), nil
}
