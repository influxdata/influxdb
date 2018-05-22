package query

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"time"

	"github.com/influxdata/platform/query/interpreter"
	"github.com/influxdata/platform/query/parser"
	"github.com/influxdata/platform/query/semantic"
	"github.com/influxdata/platform/query/values"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
)

const (
	TableParameter = "table"

	tableIDKey      = "id"
	tableKindKey    = "kind"
	tableParentsKey = "parents"
	//tableSpecKey    = "spec"
)

type Option func(*options)

func Verbose(v bool) Option {
	return func(o *options) {
		o.verbose = v
	}
}

type options struct {
	verbose bool
}

// Compile evaluates an IFQL script producing a query Spec.
func Compile(ctx context.Context, q string, opts ...Option) (*Spec, error) {
	o := new(options)
	for _, opt := range opts {
		opt(o)
	}
	s, _ := opentracing.StartSpanFromContext(ctx, "parse")
	astProg, err := parser.NewAST(q)
	if err != nil {
		return nil, err
	}
	s.Finish()
	s, _ = opentracing.StartSpanFromContext(ctx, "compile")
	defer s.Finish()

	qd := new(queryDomain)
	scope, decls := builtIns(qd)
	interpScope := interpreter.NewScopeWithValues(scope)

	// Convert AST program to a semantic program
	semProg, err := semantic.New(astProg, decls)
	if err != nil {
		return nil, err
	}

	if err := interpreter.Eval(semProg, interpScope); err != nil {
		return nil, err
	}
	spec := qd.ToSpec()

	if o.verbose {
		log.Println("Query Spec: ", Formatted(spec, FmtJSON))
	}
	return spec, nil
}

type CreateOperationSpec func(args Arguments, a *Administration) (OperationSpec, error)

var builtinScope = make(map[string]values.Value)
var builtinDeclarations = make(semantic.DeclarationScope)

// list of builtin scripts
var builtins = make(map[string]string)
var finalized bool

// RegisterBuiltIn adds any variable declarations in the script to the builtin scope.
func RegisterBuiltIn(name, script string) {
	if finalized {
		panic(errors.New("already finalized, cannot register builtin"))
	}
	builtins[name] = script
}

// RegisterFunction adds a new builtin top level function.
func RegisterFunction(name string, c CreateOperationSpec, sig semantic.FunctionSignature) {
	f := function{
		t:            semantic.NewFunctionType(sig),
		name:         name,
		createOpSpec: c,
	}
	RegisterBuiltInValue(name, f)
}

// RegisterBuiltInValue adds the value to the builtin scope.
func RegisterBuiltInValue(name string, v values.Value) {
	if finalized {
		panic(errors.New("already finalized, cannot register builtin"))
	}
	if _, ok := builtinScope[name]; ok {
		panic(fmt.Errorf("duplicate registration for builtin %q", name))
	}
	builtinDeclarations[name] = semantic.NewExternalVariableDeclaration(name, v.Type())
	builtinScope[name] = v
}

// FinalizeRegistration must be called to complete registration.
// Future calls to RegisterFunction, RegisterBuiltIn or RegisterBuiltInValue will panic.
func FinalizeRegistration() {
	if finalized {
		panic("already finalized")
	}
	finalized = true
	//for name, script := range builtins {
	//	astProg, err := parser.NewAST(script)
	//	if err != nil {
	//		panic(errors.Wrapf(err, "failed to parse builtin %q", name))
	//	}
	//	semProg, err := semantic.New(astProg, builtinDeclarations)
	//	if err != nil {
	//		panic(errors.Wrapf(err, "failed to create semantic graph for builtin %q", name))
	//	}

	//	if err := interpreter.Eval(semProg, builtinScope); err != nil {
	//		panic(errors.Wrapf(err, "failed to evaluate builtin %q", name))
	//	}
	//}
	//// free builtins list
	//builtins = nil
}

var TableObjectType = semantic.NewObjectType(map[string]semantic.Type{
	tableIDKey:   semantic.String,
	tableKindKey: semantic.String,
	// TODO(nathanielc): The spec types vary significantly making type comparisons impossible, for now the solution is to state the type as an empty object.
	//tableSpecKey: semantic.EmptyObject,
	// TODO(nathanielc): Support recursive types, for now we state that the array has empty objects.
	tableParentsKey: semantic.NewArrayType(semantic.EmptyObject),
})

type TableObject struct {
	ID      OperationID
	Kind    OperationKind
	Spec    OperationSpec
	Parents values.Array
}

func (t TableObject) Operation() *Operation {
	return &Operation{
		ID:   t.ID,
		Spec: t.Spec,
	}
}

func (t TableObject) String() string {
	return fmt.Sprintf("{id: %q, kind: %q}", t.ID, t.Kind)
}

func (t TableObject) ToSpec() *Spec {
	visited := make(map[OperationID]bool)
	spec := new(Spec)
	t.buildSpec(spec, visited)
	return spec
}

func (t TableObject) buildSpec(spec *Spec, visited map[OperationID]bool) {
	id := t.ID
	t.Parents.Range(func(i int, v values.Value) {
		p := v.(TableObject)
		if !visited[p.ID] {
			// rescurse up parents
			p.buildSpec(spec, visited)
		}

		spec.Edges = append(spec.Edges, Edge{
			Parent: p.ID,
			Child:  id,
		})
	})

	visited[id] = true
	spec.Operations = append(spec.Operations, t.Operation())
}

func (t TableObject) Type() semantic.Type {
	return TableObjectType
}

func (t TableObject) Str() string {
	panic(values.UnexpectedKind(semantic.Object, semantic.String))
}
func (t TableObject) Int() int64 {
	panic(values.UnexpectedKind(semantic.Object, semantic.Int))
}
func (t TableObject) UInt() uint64 {
	panic(values.UnexpectedKind(semantic.Object, semantic.UInt))
}
func (t TableObject) Float() float64 {
	panic(values.UnexpectedKind(semantic.Object, semantic.Float))
}
func (t TableObject) Bool() bool {
	panic(values.UnexpectedKind(semantic.Object, semantic.Bool))
}
func (t TableObject) Time() values.Time {
	panic(values.UnexpectedKind(semantic.Object, semantic.Time))
}
func (t TableObject) Duration() values.Duration {
	panic(values.UnexpectedKind(semantic.Object, semantic.Duration))
}
func (t TableObject) Regexp() *regexp.Regexp {
	panic(values.UnexpectedKind(semantic.Object, semantic.Regexp))
}
func (t TableObject) Array() values.Array {
	panic(values.UnexpectedKind(semantic.Object, semantic.Array))
}
func (t TableObject) Object() values.Object {
	return t
}
func (t TableObject) Function() values.Function {
	panic(values.UnexpectedKind(semantic.Object, semantic.Function))
}

func (t TableObject) Get(name string) (values.Value, bool) {
	switch name {
	case tableIDKey:
		return values.NewStringValue(string(t.ID)), true
	case tableKindKey:
		return values.NewStringValue(string(t.Kind)), true
	case tableParentsKey:
		return t.Parents, true
	default:
		return nil, false
	}
}

func (t TableObject) Set(name string, v values.Value) {
	//TableObject is immutable
}

func (t TableObject) Len() int {
	return 3
}

func (t TableObject) Range(f func(name string, v values.Value)) {
	f(tableIDKey, values.NewStringValue(string(t.ID)))
	f(tableKindKey, values.NewStringValue(string(t.Kind)))
	f(tableParentsKey, t.Parents)
}

// DefaultFunctionSignature returns a FunctionSignature for standard functions which accept a table piped argument.
// It is safe to modify the returned signature.
func DefaultFunctionSignature() semantic.FunctionSignature {
	return semantic.FunctionSignature{
		Params: map[string]semantic.Type{
			TableParameter: TableObjectType,
		},
		ReturnType:   TableObjectType,
		PipeArgument: TableParameter,
	}
}

func BuiltIns() (map[string]values.Value, semantic.DeclarationScope) {
	qd := new(queryDomain)
	return builtIns(qd)
}

func builtIns(qd *queryDomain) (map[string]values.Value, semantic.DeclarationScope) {
	decls := builtinDeclarations.Copy()
	scope := make(map[string]values.Value, len(builtinScope))
	for k, v := range builtinScope {
		if v.Type().Kind() == semantic.Function {
			if f, ok := v.Function().(function); ok {
				f.qd = qd
				v = f
			}
		}
		scope[k] = v
	}
	interpScope := interpreter.NewScopeWithValues(scope)
	for name, script := range builtins {
		astProg, err := parser.NewAST(script)
		if err != nil {
			panic(errors.Wrapf(err, "failed to parse builtin %q", name))
		}
		semProg, err := semantic.New(astProg, decls)
		if err != nil {
			panic(errors.Wrapf(err, "failed to create semantic graph for builtin %q", name))
		}

		if err := interpreter.Eval(semProg, interpScope); err != nil {
			panic(errors.Wrapf(err, "failed to evaluate builtin %q", name))
		}
	}
	return scope, decls
}

type Administration struct {
	id      OperationID
	parents values.Array
}

func newAdministration(id OperationID) *Administration {
	return &Administration{
		id: id,
		// TODO(nathanielc): Once we can support recursive types change this to,
		// interpreter.NewArray(TableObjectType)
		parents: values.NewArray(semantic.EmptyObject),
	}
}

// AddParentFromArgs reads the args for the `table` argument and adds the value as a parent.
func (a *Administration) AddParentFromArgs(args Arguments) error {
	parent, err := args.GetRequiredObject(TableParameter)
	if err != nil {
		return err
	}
	p, ok := parent.(TableObject)
	if !ok {
		return fmt.Errorf("argument is not a table object: got %T", parent)
	}
	a.AddParent(p)
	return nil
}

// AddParent instructs the evaluation Context that a new edge should be created from the parent to the current operation.
// Duplicate parents will be removed, so the caller need not concern itself with which parents have already been added.
func (a *Administration) AddParent(np TableObject) {
	// Check for duplicates
	found := false
	a.parents.Range(func(i int, p values.Value) {
		if p.(TableObject).ID == np.ID {
			found = true
		}
	})
	if !found {
		a.parents.Append(np)
	}
}

type Domain interface {
	ToSpec() *Spec
}

func NewDomain() Domain {
	return new(queryDomain)
}

type queryDomain struct {
	id int

	operations []TableObject
}

func (d *queryDomain) NewID(name string) OperationID {
	return OperationID(fmt.Sprintf("%s%d", name, d.nextID()))
}

func (d *queryDomain) nextID() int {
	id := d.id
	d.id++
	return id
}

func (d *queryDomain) ToSpec() *Spec {
	spec := new(Spec)
	visited := make(map[OperationID]bool)
	for _, t := range d.operations {
		t.buildSpec(spec, visited)
	}
	return spec
}

type function struct {
	name         string
	t            semantic.Type
	createOpSpec CreateOperationSpec
	qd           *queryDomain
}

func (f function) Type() semantic.Type {
	return f.t
}

func (f function) Str() string {
	panic(values.UnexpectedKind(semantic.Function, semantic.String))
}
func (f function) Int() int64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.Int))
}
func (f function) UInt() uint64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.UInt))
}
func (f function) Float() float64 {
	panic(values.UnexpectedKind(semantic.Function, semantic.Float))
}
func (f function) Bool() bool {
	panic(values.UnexpectedKind(semantic.Function, semantic.Bool))
}
func (f function) Time() values.Time {
	panic(values.UnexpectedKind(semantic.Function, semantic.Time))
}
func (f function) Duration() values.Duration {
	panic(values.UnexpectedKind(semantic.Function, semantic.Duration))
}
func (f function) Regexp() *regexp.Regexp {
	panic(values.UnexpectedKind(semantic.Function, semantic.Regexp))
}
func (f function) Array() values.Array {
	panic(values.UnexpectedKind(semantic.Function, semantic.Array))
}
func (f function) Object() values.Object {
	panic(values.UnexpectedKind(semantic.Function, semantic.Object))
}
func (f function) Function() values.Function {
	return f
}

func (f function) Call(argsObj values.Object) (values.Value, error) {
	return interpreter.DoFunctionCall(f.call, argsObj)
}

func (f function) call(args interpreter.Arguments) (values.Value, error) {
	id := f.qd.NewID(f.name)

	a := newAdministration(id)

	spec, err := f.createOpSpec(Arguments{Arguments: args}, a)
	if err != nil {
		return nil, err
	}

	if a.parents.Len() > 1 {
		// Always add parents in a consistent order
		a.parents.Sort(func(i, j values.Value) bool {
			return i.(TableObject).ID < j.(TableObject).ID
		})
	}

	t := TableObject{
		ID:      id,
		Kind:    spec.Kind(),
		Spec:    spec,
		Parents: a.parents,
	}
	f.qd.operations = append(f.qd.operations, t)
	return t, nil
}

type specValue struct {
	spec OperationSpec
}

func (v specValue) Type() semantic.Type {
	return semantic.EmptyObject
}

func (v specValue) Value() interface{} {
	return v.spec
}

func (v specValue) Property(name string) (interpreter.Value, error) {
	return nil, errors.New("spec does not have properties")
}

type Arguments struct {
	interpreter.Arguments
}

func (a Arguments) GetTime(name string) (Time, bool, error) {
	v, ok := a.Get(name)
	if !ok {
		return Time{}, false, nil
	}
	qt, err := ToQueryTime(v)
	if err != nil {
		return Time{}, ok, err
	}
	return qt, ok, nil
}

func (a Arguments) GetRequiredTime(name string) (Time, error) {
	qt, ok, err := a.GetTime(name)
	if err != nil {
		return Time{}, err
	}
	if !ok {
		return Time{}, fmt.Errorf("missing required keyword argument %q", name)
	}
	return qt, nil
}

func (a Arguments) GetDuration(name string) (Duration, bool, error) {
	v, ok := a.Get(name)
	if !ok {
		return 0, false, nil
	}
	return Duration(v.Duration()), true, nil
}

func (a Arguments) GetRequiredDuration(name string) (Duration, error) {
	d, ok, err := a.GetDuration(name)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("missing required keyword argument %q", name)
	}
	return d, nil
}

func ToQueryTime(value values.Value) (Time, error) {
	switch value.Type().Kind() {
	case semantic.Time:
		return Time{
			Absolute: value.Time().Time(),
		}, nil
	case semantic.Duration:
		return Time{
			Relative:   value.Duration().Duration(),
			IsRelative: true,
		}, nil
	case semantic.Int:
		return Time{
			Absolute: time.Unix(value.Int(), 0),
		}, nil
	default:
		return Time{}, fmt.Errorf("value is not a time, got %v", value.Type())
	}
}
