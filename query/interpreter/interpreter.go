package interpreter

import (
	"fmt"
	"regexp"

	"github.com/influxdata/platform/query/ast"
	"github.com/influxdata/platform/query/semantic"
	"github.com/influxdata/platform/query/values"
	"github.com/pkg/errors"
)

// Interpreter used to interpret a Flux program
type Interpreter struct {
	values  []values.Value
	options *Scope
	globals *Scope
}

// NewInterpreter instantiates a new Flux Interpreter whose builtin values are not mutable.
// Options are always mutable.
func NewInterpreter(options, builtins map[string]values.Value) *Interpreter {
	optionScope := NewScopeWithValues(options)
	globalScope := optionScope.NestWithValues(builtins)
	interpreter := &Interpreter{
		options: optionScope,
		globals: globalScope.Nest(),
	}
	return interpreter
}

// NewMutableInterpreter instantiates a new Flux Interpreter whose builtin values are mutable.
// Options are always mutable.
func NewMutableInterpreter(options, builtins map[string]values.Value) *Interpreter {
	optionScope := NewScopeWithValues(options)
	globalScope := optionScope.NestWithValues(builtins)
	interpreter := &Interpreter{
		options: optionScope,
		globals: globalScope,
	}
	return interpreter
}

// Return gives the return value from the block
func (itrp *Interpreter) Return() values.Value {
	return itrp.globals.Return()
}

// GlobalScope returns a pointer to the global scope of the program.
// That is the scope nested directly below the options scope.
func (itrp *Interpreter) GlobalScope() *Scope {
	return itrp.globals
}

// SetVar adds a variable binding to the global scope
func (itrp *Interpreter) SetVar(name string, val values.Value) {
	itrp.globals.Set(name, val)
}

// SideEffects returns the evaluated expressions of a Flux program
func (itrp *Interpreter) SideEffects() []values.Value {
	return itrp.values
}

// Option returns a Flux option by name
func (itrp *Interpreter) Option(name string) values.Value {
	return itrp.options.Get(name)
}

// SetOption sets a new option binding
func (itrp *Interpreter) SetOption(name string, val values.Value) {
	itrp.options.Set(name, val)
}

// Eval evaluates the expressions composing a Flux program.
func (itrp *Interpreter) Eval(program *semantic.Program) error {
	return itrp.eval(program)
}

func (itrp *Interpreter) eval(program *semantic.Program) error {
	topLevelScope := itrp.globals
	for _, stmt := range program.Body {
		val, err := itrp.doStatement(stmt, topLevelScope)
		if err != nil {
			return err
		}
		if val != nil {
			itrp.values = append(itrp.values, val)
		}
	}
	return nil
}

// doStatement returns the resolved value of a top-level statement
func (itrp *Interpreter) doStatement(stmt semantic.Statement, scope *Scope) (values.Value, error) {
	scope.SetReturn(values.InvalidValue)
	switch s := stmt.(type) {
	case *semantic.OptionStatement:
		return itrp.doOptionStatement(s.Declaration.(*semantic.NativeVariableDeclaration), scope)
	case *semantic.NativeVariableDeclaration:
		return itrp.doVariableDeclaration(s, scope)
	case *semantic.ExpressionStatement:
		v, err := itrp.doExpression(s.Expression, scope)
		if err != nil {
			return nil, err
		}
		scope.SetReturn(v)
		return v, nil
	case *semantic.BlockStatement:
		nested := scope.Nest()
		for i, stmt := range s.Body {
			_, err := itrp.doStatement(stmt, nested)
			if err != nil {
				return nil, err
			}
			// Validate a return statement is the last statement
			if _, ok := stmt.(*semantic.ReturnStatement); ok {
				if i != len(s.Body)-1 {
					return nil, errors.New("return statement is not the last statement in the block")
				}
			}
		}
		// Propgate any return value from the nested scope out. Since a return statement is
		// always last we do not have to worry about overriding an existing return value.
		scope.SetReturn(nested.Return())
	case *semantic.ReturnStatement:
		v, err := itrp.doExpression(s.Argument, scope)
		if err != nil {
			return nil, err
		}
		scope.SetReturn(v)
	default:
		return nil, fmt.Errorf("unsupported statement type %T", stmt)
	}
	return nil, nil
}

func (itrp *Interpreter) doOptionStatement(declaration *semantic.NativeVariableDeclaration, scope *Scope) (values.Value, error) {
	value, err := itrp.doExpression(declaration.Init, scope)
	if err != nil {
		return nil, err
	}
	itrp.options.Set(declaration.Identifier.Name, value)
	return value, nil
}

func (itrp *Interpreter) doVariableDeclaration(declaration *semantic.NativeVariableDeclaration, scope *Scope) (values.Value, error) {
	value, err := itrp.doExpression(declaration.Init, scope)
	if err != nil {
		return nil, err
	}
	scope.Set(declaration.Identifier.Name, value)
	return value, nil
}

func (itrp *Interpreter) doExpression(expr semantic.Expression, scope *Scope) (values.Value, error) {
	switch e := expr.(type) {
	case semantic.Literal:
		return itrp.doLiteral(e)
	case *semantic.ArrayExpression:
		return itrp.doArray(e, scope)
	case *semantic.IdentifierExpression:
		value, ok := scope.Lookup(e.Name)
		if !ok {
			return nil, fmt.Errorf("undefined identifier %q", e.Name)
		}
		return value, nil
	case *semantic.CallExpression:
		v, err := itrp.doCall(e, scope)
		if err != nil {
			// Determine function name
			return nil, errors.Wrapf(err, "error calling function %q", functionName(e))
		}
		return v, nil
	case *semantic.MemberExpression:
		obj, err := itrp.doExpression(e.Object, scope)
		if err != nil {
			return nil, err
		}
		v, ok := obj.Object().Get(e.Property)
		if !ok {
			return nil, fmt.Errorf("object has no property %q", e.Property)
		}
		return v, nil
	case *semantic.ObjectExpression:
		return itrp.doObject(e, scope)
	case *semantic.UnaryExpression:
		v, err := itrp.doExpression(e.Argument, scope)
		if err != nil {
			return nil, err
		}
		switch e.Operator {
		case ast.NotOperator:
			if v.Type() != semantic.Bool {
				return nil, fmt.Errorf("operand to unary expression is not a boolean value, got %v", v.Type())
			}
			return values.NewBoolValue(!v.Bool()), nil
		case ast.SubtractionOperator:
			switch t := v.Type(); t {
			case semantic.Int:
				return values.NewIntValue(-v.Int()), nil
			case semantic.Float:
				return values.NewFloatValue(-v.Float()), nil
			case semantic.Duration:
				return values.NewDurationValue(-v.Duration()), nil
			default:
				return nil, fmt.Errorf("operand to unary expression is not a number value, got %v", v.Type())
			}
		default:
			return nil, fmt.Errorf("unsupported operator %q to unary expression", e.Operator)
		}

	case *semantic.BinaryExpression:
		l, err := itrp.doExpression(e.Left, scope)
		if err != nil {
			return nil, err
		}

		r, err := itrp.doExpression(e.Right, scope)
		if err != nil {
			return nil, err
		}

		bf, err := values.LookupBinaryFunction(values.BinaryFuncSignature{
			Operator: e.Operator,
			Left:     l.Type(),
			Right:    r.Type(),
		})
		if err != nil {
			return nil, err
		}
		return bf(l, r), nil
	case *semantic.LogicalExpression:
		l, err := itrp.doExpression(e.Left, scope)
		if err != nil {
			return nil, err
		}
		if l.Type() != semantic.Bool {
			return nil, fmt.Errorf("left operand to logcial expression is not a boolean value, got %v", l.Type())
		}
		left := l.Bool()

		if e.Operator == ast.AndOperator && !left {
			// Early return
			return values.NewBoolValue(false), nil
		} else if e.Operator == ast.OrOperator && left {
			// Early return
			return values.NewBoolValue(true), nil
		}

		r, err := itrp.doExpression(e.Right, scope)
		if err != nil {
			return nil, err
		}
		if r.Type() != semantic.Bool {
			return nil, errors.New("right operand to logcial expression is not a boolean value")
		}
		right := r.Bool()

		switch e.Operator {
		case ast.AndOperator:
			return values.NewBoolValue(left && right), nil
		case ast.OrOperator:
			return values.NewBoolValue(left || right), nil
		default:
			return nil, fmt.Errorf("invalid logical operator %v", e.Operator)
		}
	case *semantic.FunctionExpression:
		return &function{
			e:     e,
			scope: scope.Nest(),
		}, nil
	default:
		return nil, fmt.Errorf("unsupported expression %T", expr)
	}
}

func (itrp *Interpreter) doArray(a *semantic.ArrayExpression, scope *Scope) (values.Value, error) {
	elements := make([]values.Value, len(a.Elements))
	elementType := semantic.EmptyArrayType.ElementType()
	for i, el := range a.Elements {
		v, err := itrp.doExpression(el, scope)
		if err != nil {
			return nil, err
		}
		if i == 0 {
			elementType = v.Type()
		}
		if elementType != v.Type() {
			return nil, fmt.Errorf("cannot mix types in an array, found both %v and %v", elementType, v.Type())
		}
		elements[i] = v
	}
	return values.NewArrayWithBacking(elementType, elements), nil
}

func (itrp *Interpreter) doObject(m *semantic.ObjectExpression, scope *Scope) (values.Value, error) {
	obj := values.NewObject()
	for _, p := range m.Properties {
		v, err := itrp.doExpression(p.Value, scope)
		if err != nil {
			return nil, err
		}
		if _, ok := obj.Get(p.Key.Name); ok {
			return nil, fmt.Errorf("duplicate key in object: %q", p.Key.Name)
		}
		obj.Set(p.Key.Name, v)
	}
	return obj, nil
}

func (itrp *Interpreter) doLiteral(lit semantic.Literal) (values.Value, error) {
	switch l := lit.(type) {
	case *semantic.DateTimeLiteral:
		return values.NewTimeValue(values.Time(l.Value.UnixNano())), nil
	case *semantic.DurationLiteral:
		return values.NewDurationValue(values.Duration(l.Value)), nil
	case *semantic.FloatLiteral:
		return values.NewFloatValue(l.Value), nil
	case *semantic.IntegerLiteral:
		return values.NewIntValue(l.Value), nil
	case *semantic.UnsignedIntegerLiteral:
		return values.NewUIntValue(l.Value), nil
	case *semantic.StringLiteral:
		return values.NewStringValue(l.Value), nil
	case *semantic.RegexpLiteral:
		return values.NewRegexpValue(l.Value), nil
	case *semantic.BooleanLiteral:
		return values.NewBoolValue(l.Value), nil
	default:
		return nil, fmt.Errorf("unknown literal type %T", lit)
	}
}

func functionName(call *semantic.CallExpression) string {
	switch callee := call.Callee.(type) {
	case *semantic.IdentifierExpression:
		return callee.Name
	case *semantic.MemberExpression:
		return callee.Property
	default:
		return "<anonymous function>"
	}
}

func DoFunctionCall(f func(args Arguments) (values.Value, error), argsObj values.Object) (values.Value, error) {
	args := NewArguments(argsObj)
	v, err := f(args)
	if err != nil {
		return nil, err
	}
	if unused := args.listUnused(); len(unused) > 0 {
		return nil, fmt.Errorf("unused arguments %v", unused)
	}
	return v, nil
}

func (itrp *Interpreter) doCall(call *semantic.CallExpression, scope *Scope) (values.Value, error) {
	callee, err := itrp.doExpression(call.Callee, scope)
	if err != nil {
		return nil, err
	}
	if callee.Type().Kind() != semantic.Function {
		return nil, fmt.Errorf("cannot call function, value is of type %v", callee.Type())
	}
	f := callee.Function()
	argObj, err := itrp.doArguments(call.Arguments, scope)
	if err != nil {
		return nil, err
	}

	// Check if the function is an interpFunction and rebind it.
	if af, ok := f.(*function); ok {
		af.itrp = itrp
		f = af
	}

	// Call the function
	value, err := f.Call(argObj)
	if err != nil {
		return nil, err
	}
	if f.HasSideEffect() {
		itrp.values = append(itrp.values, value)
	}
	return value, nil
}

func (itrp *Interpreter) doArguments(args *semantic.ObjectExpression, scope *Scope) (values.Object, error) {
	obj := values.NewObject()
	if args == nil || len(args.Properties) == 0 {
		return obj, nil
	}
	for _, p := range args.Properties {
		value, err := itrp.doExpression(p.Value, scope)
		if err != nil {
			return nil, err
		}
		if _, ok := obj.Get(p.Key.Name); ok {
			return nil, fmt.Errorf("duplicate keyword parameter specified: %q", p.Key.Name)
		}

		obj.Set(p.Key.Name, value)
	}
	return obj, nil
}

// TODO(Josh): Scope methods should be private
type Scope struct {
	parent      *Scope
	values      map[string]values.Value
	returnValue values.Value
}

func NewScope() *Scope {
	return &Scope{
		values: make(map[string]values.Value),
	}
}

// NewScopeWithValues creates a new scope with the initial set of values.
// The vals map will be mutated.
func NewScopeWithValues(vals map[string]values.Value) *Scope {
	return &Scope{
		values: vals,
	}
}

func (s *Scope) Get(name string) values.Value {
	return s.values[name]
}

func (s *Scope) Set(name string, value values.Value) {
	s.values[name] = value
}

func (s *Scope) Values() map[string]values.Value {
	cp := make(map[string]values.Value, len(s.values))
	for k, v := range s.values {
		cp[k] = v
	}
	return cp
}

func (s *Scope) SetValues(vals map[string]values.Value) {
	for k, v := range vals {
		s.values[k] = v
	}
}

func (s *Scope) Lookup(name string) (values.Value, bool) {
	if s == nil {
		return nil, false
	}
	v, ok := s.values[name]
	if !ok {
		return s.parent.Lookup(name)
	}
	return v, ok
}

// SetReturn sets the return value of this scope.
func (s *Scope) SetReturn(value values.Value) {
	s.returnValue = value
}

// Return reports the return value for this scope. If no return value has been set a value with type semantic.TInvalid is returned.
func (s *Scope) Return() values.Value {
	return s.returnValue
}

func (s *Scope) Names() []string {
	if s == nil {
		return nil
	}
	names := s.parent.Names()
	for k := range s.values {
		names = append(names, k)
	}
	return names
}

// Nest returns a new nested scope.
func (s *Scope) Nest() *Scope {
	c := NewScope()
	c.parent = s
	return c
}

func (s *Scope) NestWithValues(values map[string]values.Value) *Scope {
	c := NewScopeWithValues(values)
	c.parent = s
	return c
}

// Copy returns a copy of the scope and its parents.
func (s *Scope) Copy() *Scope {
	c := NewScope()

	// copy parent values into new scope
	curr := s
	for curr != nil {
		// copy values
		for k, v := range curr.values {
			c.values[k] = v
		}
		curr = curr.parent
	}
	return c
}

func (s *Scope) Range(f func(k string, v values.Value)) {
	for k, v := range s.values {
		f(k, v)
	}
	if s.parent != nil {
		s.parent.Range(f)
	}
}

// Value represents any value that can be the result of evaluating any expression.
type Value interface {
	// Type reports the type of value
	Type() semantic.Type
	// Value returns the actual value represented.
	Value() interface{}
	// Property returns a new value which is a property of this value.
	Property(name string) (values.Value, error)
}

type value struct {
	t semantic.Type
	v interface{}
}

func (v value) Type() semantic.Type {
	return v.t
}
func (v value) Value() interface{} {
	return v.v
}
func (v value) Property(name string) (values.Value, error) {
	return nil, fmt.Errorf("property %q does not exist", name)
}
func (v value) String() string {
	return fmt.Sprintf("%v", v.v)
}

type function struct {
	e     *semantic.FunctionExpression
	scope *Scope
	call  func(Arguments) (values.Value, error)

	itrp *Interpreter
}

func (f *function) Type() semantic.Type {
	return f.e.Type()
}

func (f *function) Str() string {
	panic(values.UnexpectedKind(semantic.Object, semantic.String))
}
func (f *function) Int() int64 {
	panic(values.UnexpectedKind(semantic.Object, semantic.Int))
}
func (f *function) UInt() uint64 {
	panic(values.UnexpectedKind(semantic.Object, semantic.UInt))
}
func (f *function) Float() float64 {
	panic(values.UnexpectedKind(semantic.Object, semantic.Float))
}
func (f *function) Bool() bool {
	panic(values.UnexpectedKind(semantic.Object, semantic.Bool))
}
func (f *function) Time() values.Time {
	panic(values.UnexpectedKind(semantic.Object, semantic.Time))
}
func (f *function) Duration() values.Duration {
	panic(values.UnexpectedKind(semantic.Object, semantic.Duration))
}
func (f *function) Regexp() *regexp.Regexp {
	panic(values.UnexpectedKind(semantic.Object, semantic.Regexp))
}
func (f *function) Array() values.Array {
	panic(values.UnexpectedKind(semantic.Object, semantic.Function))
}
func (f *function) Object() values.Object {
	panic(values.UnexpectedKind(semantic.Object, semantic.Object))
}
func (f *function) Function() values.Function {
	return f
}
func (f *function) Equal(rhs values.Value) bool {
	if f.Type() != rhs.Type() {
		return false
	}
	v, ok := rhs.(*function)
	return ok && (f == v)
}
func (f *function) HasSideEffect() bool {
	// Function definitions do not produce side effects.
	// Only a function call expression can produce side effects.
	return false
}

func (f *function) Call(argsObj values.Object) (values.Value, error) {
	args := newArguments(argsObj)
	v, err := f.doCall(args)
	if err != nil {
		return nil, err
	}
	if unused := args.listUnused(); len(unused) > 0 {
		return nil, fmt.Errorf("unused arguments %s", unused)
	}
	return v, nil
}
func (f *function) doCall(args Arguments) (values.Value, error) {
	for _, p := range f.e.Params {
		if p.Default == nil {
			v, err := args.GetRequired(p.Key.Name)
			if err != nil {
				return nil, err
			}
			f.scope.Set(p.Key.Name, v)
		} else {
			v, ok := args.Get(p.Key.Name)
			if !ok {
				// Use default value
				var err error
				v, err = f.itrp.doExpression(p.Default, f.scope)
				if err != nil {
					return nil, err
				}
			}
			f.scope.Set(p.Key.Name, v)
		}
	}
	switch n := f.e.Body.(type) {
	case semantic.Expression:
		return f.itrp.doExpression(n, f.scope)
	case semantic.Statement:
		_, err := f.itrp.doStatement(n, f.scope)
		if err != nil {
			return nil, err
		}
		v := f.scope.Return()
		if v.Type() == semantic.Invalid {
			return nil, errors.New("function has no return value")
		}
		return v, nil
	default:
		return nil, fmt.Errorf("unsupported function body type %T", f.e.Body)
	}
}

// Resolver represents a value that can resolve itself
type Resolver interface {
	Resolve() (semantic.Node, error)
}

func ResolveFunction(f values.Function) (*semantic.FunctionExpression, error) {
	resolver, ok := f.(Resolver)
	if !ok {
		return nil, errors.New("function is not resolvable")
	}
	resolved, err := resolver.Resolve()
	if err != nil {
		return nil, err
	}
	fn, ok := resolved.(*semantic.FunctionExpression)
	if !ok {
		return nil, errors.New("resolved function is not a function")
	}
	return fn, nil
}

// Resolve rewrites the function resolving any identifiers not listed in the function params.
func (f *function) Resolve() (semantic.Node, error) {
	n := f.e.Copy()
	node, err := f.resolveIdentifiers(n)
	if err != nil {
		return nil, err
	}
	return node, nil
}

func (f function) resolveIdentifiers(n semantic.Node) (semantic.Node, error) {
	switch n := n.(type) {
	case *semantic.IdentifierExpression:
		for _, p := range f.e.Params {
			if n.Name == p.Key.Name {
				// Identifier is a parameter do not resolve
				return n, nil
			}
		}
		v, ok := f.scope.Lookup(n.Name)
		if !ok {
			return nil, fmt.Errorf("name %q does not exist in scope", n.Name)
		}
		return resolveValue(v)
	case *semantic.BlockStatement:
		for i, s := range n.Body {
			node, err := f.resolveIdentifiers(s)
			if err != nil {
				return nil, err
			}
			n.Body[i] = node.(semantic.Statement)
		}
	case *semantic.OptionStatement:
		node, err := f.resolveIdentifiers(n.Declaration)
		if err != nil {
			return nil, err
		}
		n.Declaration = node.(semantic.VariableDeclaration)
	case *semantic.ExpressionStatement:
		node, err := f.resolveIdentifiers(n.Expression)
		if err != nil {
			return nil, err
		}
		n.Expression = node.(semantic.Expression)
	case *semantic.ReturnStatement:
		node, err := f.resolveIdentifiers(n.Argument)
		if err != nil {
			return nil, err
		}
		n.Argument = node.(semantic.Expression)
	case *semantic.NativeVariableDeclaration:
		node, err := f.resolveIdentifiers(n.Init)
		if err != nil {
			return nil, err
		}
		n.Init = node.(semantic.Expression)
	case *semantic.CallExpression:
		node, err := f.resolveIdentifiers(n.Arguments)
		if err != nil {
			return nil, err
		}
		n.Arguments = node.(*semantic.ObjectExpression)
	case *semantic.FunctionExpression:
		node, err := f.resolveIdentifiers(n.Body)
		if err != nil {
			return nil, err
		}
		n.Body = node
	case *semantic.BinaryExpression:
		node, err := f.resolveIdentifiers(n.Left)
		if err != nil {
			return nil, err
		}
		n.Left = node.(semantic.Expression)

		node, err = f.resolveIdentifiers(n.Right)
		if err != nil {
			return nil, err
		}
		n.Right = node.(semantic.Expression)
	case *semantic.UnaryExpression:
		node, err := f.resolveIdentifiers(n.Argument)
		if err != nil {
			return nil, err
		}
		n.Argument = node.(semantic.Expression)
	case *semantic.LogicalExpression:
		node, err := f.resolveIdentifiers(n.Left)
		if err != nil {
			return nil, err
		}
		n.Left = node.(semantic.Expression)
		node, err = f.resolveIdentifiers(n.Right)
		if err != nil {
			return nil, err
		}
		n.Right = node.(semantic.Expression)
	case *semantic.ArrayExpression:
		for i, el := range n.Elements {
			node, err := f.resolveIdentifiers(el)
			if err != nil {
				return nil, err
			}
			n.Elements[i] = node.(semantic.Expression)
		}
	case *semantic.ObjectExpression:
		for i, p := range n.Properties {
			node, err := f.resolveIdentifiers(p)
			if err != nil {
				return nil, err
			}
			n.Properties[i] = node.(*semantic.Property)
		}
	case *semantic.ConditionalExpression:
		node, err := f.resolveIdentifiers(n.Test)
		if err != nil {
			return nil, err
		}
		n.Test = node.(semantic.Expression)

		node, err = f.resolveIdentifiers(n.Alternate)
		if err != nil {
			return nil, err
		}
		n.Alternate = node.(semantic.Expression)

		node, err = f.resolveIdentifiers(n.Consequent)
		if err != nil {
			return nil, err
		}
		n.Consequent = node.(semantic.Expression)
	case *semantic.Property:
		node, err := f.resolveIdentifiers(n.Value)
		if err != nil {
			return nil, err
		}
		n.Value = node.(semantic.Expression)
	}
	return n, nil
}

func resolveValue(v values.Value) (semantic.Node, error) {
	switch k := v.Type().Kind(); k {
	case semantic.String:
		return &semantic.StringLiteral{
			Value: v.Str(),
		}, nil
	case semantic.Int:
		return &semantic.IntegerLiteral{
			Value: v.Int(),
		}, nil
	case semantic.UInt:
		return &semantic.UnsignedIntegerLiteral{
			Value: v.UInt(),
		}, nil
	case semantic.Float:
		return &semantic.FloatLiteral{
			Value: v.Float(),
		}, nil
	case semantic.Bool:
		return &semantic.BooleanLiteral{
			Value: v.Bool(),
		}, nil
	case semantic.Time:
		return &semantic.DateTimeLiteral{
			Value: v.Time().Time(),
		}, nil
	case semantic.Regexp:
		return &semantic.RegexpLiteral{
			Value: v.Regexp(),
		}, nil
	case semantic.Duration:
		return &semantic.DurationLiteral{
			Value: v.Duration().Duration(),
		}, nil
	case semantic.Function:
		resolver, ok := v.Function().(Resolver)
		if !ok {
			return nil, fmt.Errorf("function is not resolvable %T", v.Function())
		}
		return resolver.Resolve()
	case semantic.Array:
		arr := v.Array()
		node := new(semantic.ArrayExpression)
		node.Elements = make([]semantic.Expression, arr.Len())
		var err error
		arr.Range(func(i int, el values.Value) {
			if err != nil {
				return
			}
			var n semantic.Node
			n, err = resolveValue(el)
			if err != nil {
				return
			}
			node.Elements[i] = n.(semantic.Expression)
		})
		if err != nil {
			return nil, err
		}
		return node, nil
	case semantic.Object:
		obj := v.Object()
		node := new(semantic.ObjectExpression)
		node.Properties = make([]*semantic.Property, 0, obj.Len())
		var err error
		obj.Range(func(k string, v values.Value) {
			if err != nil {
				return
			}
			var n semantic.Node
			n, err = resolveValue(v)
			if err != nil {
				return
			}
			node.Properties = append(node.Properties, &semantic.Property{
				Key:   &semantic.Identifier{Name: k},
				Value: n.(semantic.Expression),
			})
		})
		if err != nil {
			return nil, err
		}
		return node, nil
	default:
		return nil, fmt.Errorf("cannot resove value of type %v", k)
	}
}

func ToStringArray(a values.Array) ([]string, error) {
	if a.Type().ElementType() != semantic.String {
		return nil, fmt.Errorf("cannot convert array of %v to an array of strings", a.Type().ElementType())
	}
	strs := make([]string, a.Len())
	a.Range(func(i int, v values.Value) {
		strs[i] = v.Str()
	})
	return strs, nil
}

// Arguments provides access to the keyword arguments passed to a function.
// semantic.The Get{Type} methods return three values: the typed value of the arg,
// whether the argument was specified and any errors about the argument type.
// semantic.The GetRequired{Type} methods return only two values, the typed value of the arg and any errors, a missing argument is considered an error in this case.
type Arguments interface {
	GetAll() []string
	Get(name string) (values.Value, bool)
	GetRequired(name string) (values.Value, error)

	GetString(name string) (string, bool, error)
	GetInt(name string) (int64, bool, error)
	GetFloat(name string) (float64, bool, error)
	GetBool(name string) (bool, bool, error)
	GetFunction(name string) (values.Function, bool, error)
	GetArray(name string, t semantic.Kind) (values.Array, bool, error)
	GetObject(name string) (values.Object, bool, error)

	GetRequiredString(name string) (string, error)
	GetRequiredInt(name string) (int64, error)
	GetRequiredFloat(name string) (float64, error)
	GetRequiredBool(name string) (bool, error)
	GetRequiredFunction(name string) (values.Function, error)
	GetRequiredArray(name string, t semantic.Kind) (values.Array, error)
	GetRequiredObject(name string) (values.Object, error)

	// listUnused returns the list of provided arguments that were not used by the function.
	listUnused() []string
}

type arguments struct {
	obj  values.Object
	used map[string]bool
}

func newArguments(obj values.Object) *arguments {
	if obj == nil {
		return new(arguments)
	}
	return &arguments{
		obj:  obj,
		used: make(map[string]bool, obj.Len()),
	}
}
func NewArguments(obj values.Object) Arguments {
	return newArguments(obj)
}

func (a *arguments) GetAll() []string {
	args := make([]string, 0, a.obj.Len())
	a.obj.Range(func(name string, v values.Value) {
		args = append(args, name)
	})
	return args
}

func (a *arguments) Get(name string) (values.Value, bool) {
	a.used[name] = true
	v, ok := a.obj.Get(name)
	return v, ok
}

func (a *arguments) GetRequired(name string) (values.Value, error) {
	a.used[name] = true
	v, ok := a.obj.Get(name)
	if !ok {
		return nil, fmt.Errorf("missing required keyword argument %q", name)
	}
	return v, nil
}

func (a *arguments) GetString(name string) (string, bool, error) {
	v, ok, err := a.get(name, semantic.String, false)
	if err != nil || !ok {
		return "", ok, err
	}
	return v.Str(), ok, nil
}
func (a *arguments) GetRequiredString(name string) (string, error) {
	v, _, err := a.get(name, semantic.String, true)
	if err != nil {
		return "", err
	}
	return v.Str(), nil
}
func (a *arguments) GetInt(name string) (int64, bool, error) {
	v, ok, err := a.get(name, semantic.Int, false)
	if err != nil || !ok {
		return 0, ok, err
	}
	return v.Int(), ok, nil
}
func (a *arguments) GetRequiredInt(name string) (int64, error) {
	v, _, err := a.get(name, semantic.Int, true)
	if err != nil {
		return 0, err
	}
	return v.Int(), nil
}
func (a *arguments) GetFloat(name string) (float64, bool, error) {
	v, ok, err := a.get(name, semantic.Float, false)
	if err != nil || !ok {
		return 0, ok, err
	}
	return v.Float(), ok, nil
}
func (a *arguments) GetRequiredFloat(name string) (float64, error) {
	v, _, err := a.get(name, semantic.Float, true)
	if err != nil {
		return 0, err
	}
	return v.Float(), nil
}
func (a *arguments) GetBool(name string) (bool, bool, error) {
	v, ok, err := a.get(name, semantic.Bool, false)
	if err != nil || !ok {
		return false, ok, err
	}
	return v.Bool(), ok, nil
}
func (a *arguments) GetRequiredBool(name string) (bool, error) {
	v, _, err := a.get(name, semantic.Bool, true)
	if err != nil {
		return false, err
	}
	return v.Bool(), nil
}

func (a *arguments) GetArray(name string, t semantic.Kind) (values.Array, bool, error) {
	v, ok, err := a.get(name, semantic.Array, false)
	if err != nil || !ok {
		return nil, ok, err
	}
	arr := v.Array()
	if arr.Type().ElementType() != t {
		return nil, true, fmt.Errorf("keyword argument %q should be of an array of type %v, but got an array of type %v", name, t, arr.Type())
	}
	return v.Array(), ok, nil
}
func (a *arguments) GetRequiredArray(name string, t semantic.Kind) (values.Array, error) {
	v, _, err := a.get(name, semantic.Array, true)
	if err != nil {
		return nil, err
	}
	arr := v.Array()
	if arr.Type().ElementType() != t {
		return nil, fmt.Errorf("keyword argument %q should be of an array of type %v, but got an array of type %v", name, t, arr.Type())
	}
	return arr, nil
}
func (a *arguments) GetFunction(name string) (values.Function, bool, error) {
	v, ok, err := a.get(name, semantic.Function, false)
	if err != nil || !ok {
		return nil, ok, err
	}
	return v.Function(), ok, nil
}
func (a *arguments) GetRequiredFunction(name string) (values.Function, error) {
	v, _, err := a.get(name, semantic.Function, true)
	if err != nil {
		return nil, err
	}
	return v.Function(), nil
}

func (a *arguments) GetObject(name string) (values.Object, bool, error) {
	v, ok, err := a.get(name, semantic.Object, false)
	if err != nil || !ok {
		return nil, ok, err
	}
	return v.Object(), ok, nil
}
func (a *arguments) GetRequiredObject(name string) (values.Object, error) {
	v, _, err := a.get(name, semantic.Object, true)
	if err != nil {
		return nil, err
	}
	return v.Object(), nil
}

func (a *arguments) get(name string, kind semantic.Kind, required bool) (values.Value, bool, error) {
	a.used[name] = true
	v, ok := a.obj.Get(name)
	if !ok {
		if required {
			return nil, false, fmt.Errorf("missing required keyword argument %q", name)
		}
		return nil, false, nil
	}
	if v.Type().Kind() != kind {
		return nil, true, fmt.Errorf("keyword argument %q should be of kind %v, but got %v", name, kind, v.Type().Kind())
	}
	return v, true, nil
}

func (a *arguments) listUnused() []string {
	var unused []string
	if a.obj != nil {
		a.obj.Range(func(k string, v values.Value) {
			if !a.used[k] {
				unused = append(unused, k)
			}
		})
	}
	return unused
}
