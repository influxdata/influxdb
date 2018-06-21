package semantic

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/influxdata/platform/query/ast"
)

type Node interface {
	node()
	NodeType() string
	Copy() Node

	json.Marshaler
}

func (*Program) node() {}

func (*BlockStatement) node()              {}
func (*OptionStatement) node()             {}
func (*ExpressionStatement) node()         {}
func (*ReturnStatement) node()             {}
func (*NativeVariableDeclaration) node()   {}
func (*ExternalVariableDeclaration) node() {}

func (*ArrayExpression) node()       {}
func (*FunctionExpression) node()    {}
func (*BinaryExpression) node()      {}
func (*CallExpression) node()        {}
func (*ConditionalExpression) node() {}
func (*IdentifierExpression) node()  {}
func (*LogicalExpression) node()     {}
func (*MemberExpression) node()      {}
func (*ObjectExpression) node()      {}
func (*UnaryExpression) node()       {}

func (*Identifier) node()    {}
func (*Property) node()      {}
func (*FunctionParam) node() {}

func (*BooleanLiteral) node()         {}
func (*DateTimeLiteral) node()        {}
func (*DurationLiteral) node()        {}
func (*FloatLiteral) node()           {}
func (*IntegerLiteral) node()         {}
func (*StringLiteral) node()          {}
func (*RegexpLiteral) node()          {}
func (*UnsignedIntegerLiteral) node() {}

type Statement interface {
	Node
	stmt()
}

func (*BlockStatement) stmt()              {}
func (*OptionStatement) stmt()             {}
func (*ExpressionStatement) stmt()         {}
func (*ReturnStatement) stmt()             {}
func (*NativeVariableDeclaration) stmt()   {}
func (*ExternalVariableDeclaration) stmt() {}

type Expression interface {
	Node
	Type() Type
	expression()
}

func (*ArrayExpression) expression()        {}
func (*BinaryExpression) expression()       {}
func (*BooleanLiteral) expression()         {}
func (*CallExpression) expression()         {}
func (*ConditionalExpression) expression()  {}
func (*DateTimeLiteral) expression()        {}
func (*DurationLiteral) expression()        {}
func (*FloatLiteral) expression()           {}
func (*FunctionExpression) expression()     {}
func (*IdentifierExpression) expression()   {}
func (*IntegerLiteral) expression()         {}
func (*LogicalExpression) expression()      {}
func (*MemberExpression) expression()       {}
func (*ObjectExpression) expression()       {}
func (*RegexpLiteral) expression()          {}
func (*StringLiteral) expression()          {}
func (*UnaryExpression) expression()        {}
func (*UnsignedIntegerLiteral) expression() {}

type Literal interface {
	Expression
	literal()
}

func (*BooleanLiteral) literal()         {}
func (*DateTimeLiteral) literal()        {}
func (*DurationLiteral) literal()        {}
func (*FloatLiteral) literal()           {}
func (*IntegerLiteral) literal()         {}
func (*RegexpLiteral) literal()          {}
func (*StringLiteral) literal()          {}
func (*UnsignedIntegerLiteral) literal() {}

type Program struct {
	Body []Statement `json:"body"`
}

func (*Program) NodeType() string { return "Program" }

func (p *Program) Copy() Node {
	if p == nil {
		return p
	}
	np := new(Program)
	*np = *p

	if len(p.Body) > 0 {
		np.Body = make([]Statement, len(p.Body))
		for i, s := range p.Body {
			np.Body[i] = s.Copy().(Statement)
		}
	}

	return np
}

type BlockStatement struct {
	Body []Statement `json:"body"`
}

func (*BlockStatement) NodeType() string { return "BlockStatement" }

func (s *BlockStatement) ReturnStatement() *ReturnStatement {
	return s.Body[len(s.Body)-1].(*ReturnStatement)
}

func (s *BlockStatement) Copy() Node {
	if s == nil {
		return s
	}
	ns := new(BlockStatement)
	*ns = *s

	if len(s.Body) > 0 {
		ns.Body = make([]Statement, len(s.Body))
		for i, stmt := range s.Body {
			ns.Body[i] = stmt.Copy().(Statement)
		}
	}

	return ns
}

type OptionStatement struct {
	Declaration VariableDeclaration `json:"declaration"`
}

func (s *OptionStatement) NodeType() string { return "OptionStatement" }

func (s *OptionStatement) Copy() Node {
	if s == nil {
		return s
	}
	ns := new(OptionStatement)
	*ns = *s

	ns.Declaration = s.Declaration.Copy().(VariableDeclaration)

	return ns
}

type ExpressionStatement struct {
	Expression Expression `json:"expression"`
}

func (*ExpressionStatement) NodeType() string { return "ExpressionStatement" }

func (s *ExpressionStatement) Copy() Node {
	if s == nil {
		return s
	}
	ns := new(ExpressionStatement)
	*ns = *s

	ns.Expression = s.Expression.Copy().(Expression)

	return ns
}

type ReturnStatement struct {
	Argument Expression `json:"argument"`
}

func (*ReturnStatement) NodeType() string { return "ReturnStatement" }

func (s *ReturnStatement) Copy() Node {
	if s == nil {
		return s
	}
	ns := new(ReturnStatement)
	*ns = *s

	ns.Argument = s.Argument.Copy().(Expression)

	return ns
}

type VariableDeclaration interface {
	Statement
	ID() *Identifier
	InitType() Type
}

type NativeVariableDeclaration struct {
	Identifier *Identifier `json:"identifier"`
	Init       Expression  `json:"init"`
}

func (d *NativeVariableDeclaration) ID() *Identifier {
	return d.Identifier
}
func (d *NativeVariableDeclaration) InitType() Type {
	return d.Init.Type()
}

func (*NativeVariableDeclaration) NodeType() string { return "NativeVariableDeclaration" }

func (s *NativeVariableDeclaration) Copy() Node {
	if s == nil {
		return s
	}
	ns := new(NativeVariableDeclaration)
	*ns = *s

	ns.Identifier = s.Identifier.Copy().(*Identifier)

	if s.Init != nil {
		ns.Init = s.Init.Copy().(Expression)
	}

	return ns
}

type ExternalVariableDeclaration struct {
	Identifier *Identifier `json:"identifier"`
	Type       Type        `json:"type"`
}

func NewExternalVariableDeclaration(name string, typ Type) *ExternalVariableDeclaration {
	return &ExternalVariableDeclaration{
		Identifier: &Identifier{Name: name},
		Type:       typ,
	}
}

func (d *ExternalVariableDeclaration) ID() *Identifier {
	return d.Identifier
}
func (d *ExternalVariableDeclaration) InitType() Type {
	return d.Type
}

func (*ExternalVariableDeclaration) NodeType() string { return "ExternalVariableDeclaration" }

func (s *ExternalVariableDeclaration) Copy() Node {
	if s == nil {
		return s
	}
	ns := new(ExternalVariableDeclaration)
	*ns = *s

	ns.Identifier = s.Identifier.Copy().(*Identifier)

	return ns
}

type ArrayExpression struct {
	Elements []Expression `json:"elements"`
	typ      atomic.Value //    Type
}

func (*ArrayExpression) NodeType() string { return "ArrayExpression" }
func (e *ArrayExpression) Type() Type {
	t := e.typ.Load()
	if t != nil {
		return t.(Type)
	}
	typ := arrayTypeOf(e)
	e.typ.Store(typ)
	return typ
}

func (e *ArrayExpression) Copy() Node {
	if e == nil {
		return e
	}
	ne := new(ArrayExpression)
	*ne = *e

	if len(e.Elements) > 0 {
		ne.Elements = make([]Expression, len(e.Elements))
		for i, elem := range e.Elements {
			ne.Elements[i] = elem.Copy().(Expression)
		}
	}

	return ne
}

type FunctionExpression struct {
	Params []*FunctionParam `json:"params"`
	Body   Node             `json:"body"`
	typ    atomic.Value     //Type
}

func (*FunctionExpression) NodeType() string { return "ArrowFunctionExpression" }
func (e *FunctionExpression) Type() Type {
	t := e.typ.Load()
	if t != nil {
		return t.(Type)
	}
	typ := functionTypeOf(e)
	e.typ.Store(typ)
	return typ
}

func (e *FunctionExpression) Copy() Node {
	if e == nil {
		return e
	}
	ne := new(FunctionExpression)
	*ne = *e

	if len(e.Params) > 0 {
		ne.Params = make([]*FunctionParam, len(e.Params))
		for i, p := range e.Params {
			ne.Params[i] = p.Copy().(*FunctionParam)
		}
	}
	ne.Body = e.Body.Copy()

	return ne
}

type FunctionParam struct {
	Key         *Identifier `json:"key"`
	Default     Expression  `json:"default"`
	Piped       bool        `json:"piped,omitempty"`
	declaration VariableDeclaration
}

func (*FunctionParam) NodeType() string { return "FunctionParam" }

func (f *FunctionParam) Type() Type {
	if f.declaration == nil {
		if f.Default != nil {
			f.declaration = &NativeVariableDeclaration{
				Identifier: f.Key,
				Init:       f.Default,
			}
		} else {
			return Invalid
		}
	}
	return f.declaration.InitType()
}

func (p *FunctionParam) Copy() Node {
	if p == nil {
		return p
	}
	np := new(FunctionParam)
	*np = *p

	np.Key = p.Key.Copy().(*Identifier)
	if np.Default != nil {
		np.Default = p.Default.Copy().(Expression)
	}

	return np
}

type BinaryExpression struct {
	Operator ast.OperatorKind `json:"operator"`
	Left     Expression       `json:"left"`
	Right    Expression       `json:"right"`
}

func (*BinaryExpression) NodeType() string { return "BinaryExpression" }
func (e *BinaryExpression) Type() Type {
	return binaryTypesLookup[binarySignature{
		operator: e.Operator,
		left:     e.Left.Type().Kind(),
		right:    e.Right.Type().Kind(),
	}]
}

func (e *BinaryExpression) Copy() Node {
	if e == nil {
		return e
	}
	ne := new(BinaryExpression)
	*ne = *e

	ne.Left = e.Left.Copy().(Expression)
	ne.Right = e.Right.Copy().(Expression)

	return ne
}

type CallExpression struct {
	Callee    Expression        `json:"callee"`
	Arguments *ObjectExpression `json:"arguments"`
}

func (*CallExpression) NodeType() string { return "CallExpression" }
func (e *CallExpression) Type() Type {
	return e.Callee.Type().ReturnType()
}

func (e *CallExpression) Copy() Node {
	if e == nil {
		return e
	}
	ne := new(CallExpression)
	*ne = *e

	ne.Callee = e.Callee.Copy().(Expression)
	ne.Arguments = e.Arguments.Copy().(*ObjectExpression)

	return ne
}

type ConditionalExpression struct {
	Test       Expression `json:"test"`
	Alternate  Expression `json:"alternate"`
	Consequent Expression `json:"consequent"`
}

func (*ConditionalExpression) NodeType() string { return "ConditionalExpression" }

func (e *ConditionalExpression) Copy() Node {
	if e == nil {
		return e
	}
	ne := new(ConditionalExpression)
	*ne = *e

	ne.Test = e.Test.Copy().(Expression)
	ne.Alternate = e.Alternate.Copy().(Expression)
	ne.Consequent = e.Consequent.Copy().(Expression)

	return ne
}

type LogicalExpression struct {
	Operator ast.LogicalOperatorKind `json:"operator"`
	Left     Expression              `json:"left"`
	Right    Expression              `json:"right"`
}

func (*LogicalExpression) NodeType() string { return "LogicalExpression" }
func (*LogicalExpression) Type() Type       { return Bool }

func (e *LogicalExpression) Copy() Node {
	if e == nil {
		return e
	}
	ne := new(LogicalExpression)
	*ne = *e

	ne.Left = e.Left.Copy().(Expression)
	ne.Right = e.Right.Copy().(Expression)

	return ne
}

type MemberExpression struct {
	Object   Expression `json:"object"`
	Property string     `json:"property"`
}

func (*MemberExpression) NodeType() string { return "MemberExpression" }

func (e *MemberExpression) Type() Type {
	t := e.Object.Type()
	if t.Kind() != Object {
		return Invalid
	}
	return e.Object.Type().PropertyType(e.Property)
}

func (e *MemberExpression) Copy() Node {
	if e == nil {
		return e
	}
	ne := new(MemberExpression)
	*ne = *e

	ne.Object = e.Object.Copy().(Expression)

	return ne
}

type ObjectExpression struct {
	Properties []*Property  `json:"properties"`
	typ        atomic.Value //Type
}

func (*ObjectExpression) NodeType() string { return "ObjectExpression" }
func (e *ObjectExpression) Type() Type {
	t := e.typ.Load()
	if t != nil {
		return t.(Type)
	}
	typ := objectTypeOf(e)
	e.typ.Store(typ)
	return typ
}

func (e *ObjectExpression) Copy() Node {
	if e == nil {
		return e
	}
	ne := new(ObjectExpression)
	*ne = *e

	if len(e.Properties) > 0 {
		ne.Properties = make([]*Property, len(e.Properties))
		for i, prop := range e.Properties {
			ne.Properties[i] = prop.Copy().(*Property)
		}
	}

	return ne
}

type UnaryExpression struct {
	Operator ast.OperatorKind `json:"operator"`
	Argument Expression       `json:"argument"`
}

func (*UnaryExpression) NodeType() string { return "UnaryExpression" }
func (e *UnaryExpression) Type() Type {
	return e.Argument.Type()
}

func (e *UnaryExpression) Copy() Node {
	if e == nil {
		return e
	}
	ne := new(UnaryExpression)
	*ne = *e

	ne.Argument = e.Argument.Copy().(Expression)

	return ne
}

type Property struct {
	Key   *Identifier `json:"key"`
	Value Expression  `json:"value"`
}

func (*Property) NodeType() string { return "Property" }

func (p *Property) Copy() Node {
	if p == nil {
		return p
	}
	np := new(Property)
	*np = *p

	np.Value = p.Value.Copy().(Expression)

	return np
}

type IdentifierExpression struct {
	Name string `json:"name"`
	// declaration is the node that declares this identifier
	declaration VariableDeclaration
}

func (*IdentifierExpression) NodeType() string { return "IdentifierExpression" }

func (e *IdentifierExpression) Type() Type {
	if e.declaration == nil {
		return Invalid
	}
	return e.declaration.InitType()
}

func (e *IdentifierExpression) Copy() Node {
	if e == nil {
		return e
	}
	ne := new(IdentifierExpression)
	*ne = *e

	if ne.declaration != nil {
		ne.declaration = e.declaration.Copy().(VariableDeclaration)
	}

	return ne
}

type Identifier struct {
	Name string `json:"name"`
}

func (*Identifier) NodeType() string { return "Identifier" }

func (i *Identifier) Copy() Node {
	if i == nil {
		return i
	}
	ni := new(Identifier)
	*ni = *i

	return ni
}

type BooleanLiteral struct {
	Value bool `json:"value"`
}

func (*BooleanLiteral) NodeType() string { return "BooleanLiteral" }
func (*BooleanLiteral) Type() Type       { return Bool }

func (l *BooleanLiteral) Copy() Node {
	if l == nil {
		return l
	}
	nl := new(BooleanLiteral)
	*nl = *l

	return nl
}

type DateTimeLiteral struct {
	Value time.Time `json:"value"`
}

func (*DateTimeLiteral) NodeType() string { return "DateTimeLiteral" }
func (*DateTimeLiteral) Type() Type       { return Time }

func (l *DateTimeLiteral) Copy() Node {
	if l == nil {
		return l
	}
	nl := new(DateTimeLiteral)
	*nl = *l

	return nl
}

type DurationLiteral struct {
	Value time.Duration `json:"value"`
}

func (*DurationLiteral) NodeType() string { return "DurationLiteral" }
func (*DurationLiteral) Type() Type       { return Duration }

func (l *DurationLiteral) Copy() Node {
	if l == nil {
		return l
	}
	nl := new(DurationLiteral)
	*nl = *l

	return nl
}

type IntegerLiteral struct {
	Value int64 `json:"value"`
}

func (*IntegerLiteral) NodeType() string { return "IntegerLiteral" }
func (*IntegerLiteral) Type() Type       { return Int }

func (l *IntegerLiteral) Copy() Node {
	if l == nil {
		return l
	}
	nl := new(IntegerLiteral)
	*nl = *l

	return nl
}

type FloatLiteral struct {
	Value float64 `json:"value"`
}

func (*FloatLiteral) NodeType() string { return "FloatLiteral" }
func (*FloatLiteral) Type() Type       { return Float }

func (l *FloatLiteral) Copy() Node {
	if l == nil {
		return l
	}
	nl := new(FloatLiteral)
	*nl = *l

	return nl
}

type RegexpLiteral struct {
	Value *regexp.Regexp `json:"value"`
}

func (*RegexpLiteral) NodeType() string { return "RegexpLiteral" }
func (*RegexpLiteral) Type() Type       { return Regexp }

func (l *RegexpLiteral) Copy() Node {
	if l == nil {
		return l
	}
	nl := new(RegexpLiteral)
	*nl = *l

	nl.Value = l.Value.Copy()

	return nl
}

type StringLiteral struct {
	Value string `json:"value"`
}

func (*StringLiteral) NodeType() string { return "StringLiteral" }
func (*StringLiteral) Type() Type       { return String }

func (l *StringLiteral) Copy() Node {
	if l == nil {
		return l
	}
	nl := new(StringLiteral)
	*nl = *l

	return nl
}

type UnsignedIntegerLiteral struct {
	Value uint64 `json:"value"`
}

func (*UnsignedIntegerLiteral) NodeType() string { return "UnsignedIntegerLiteral" }
func (*UnsignedIntegerLiteral) Type() Type       { return UInt }

func (l *UnsignedIntegerLiteral) Copy() Node {
	if l == nil {
		return l
	}
	nl := new(UnsignedIntegerLiteral)
	*nl = *l

	return nl
}

// New creates a semantic graph from the provided AST and builtin declarations
// The declarations will be modified for any variable declaration found in the program.
func New(prog *ast.Program, declarations map[string]VariableDeclaration) (*Program, error) {
	if declarations == nil {
		// NOTE: Calls to New may expect modifications to declarations to persist outside the function.
		// The check is against nil instead of len(declarations) == 0 for this reason.
		declarations = make(map[string]VariableDeclaration)
	}
	return analyzeProgram(prog, DeclarationScope(declarations))
}

// SolveTypes inspects the expression and ensures that all sub expressions konw their type
func SolveTypes(n Node, declarations DeclarationScope) {
	if declarations == nil {
		declarations = make(DeclarationScope)
	}
	// TODO(nathanielc): Use a formal type inference system like Hindley-Milner
	// TODO(nathanielc): The current implementation only implements the code paths that the current tests and common use cases need.
	// The implementation is by no means complete for all possible expressions.
	v := solverVisitor{
		declarations: declarations,
	}
	Walk(v, n)
}

type solverVisitor struct {
	declarations DeclarationScope
}

func (v solverVisitor) Done() {}
func (v solverVisitor) Visit(n Node) Visitor {
	switch n := n.(type) {
	case *NativeVariableDeclaration:
		v.declarations[n.Identifier.Name] = n
	case *FunctionExpression:
		funcDeclarations := v.declarations.Copy()
		nv := solverVisitor{
			declarations: funcDeclarations,
		}
		return nv
	case *FunctionParam:
		n.Type()
		if n.declaration != nil {
			v.declarations[n.Key.Name] = n.declaration
		}
	case *IdentifierExpression:
		declaration, ok := v.declarations[n.Name]
		if ok {
			n.declaration = declaration
		}
	}
	return v
}

type DeclarationScope map[string]VariableDeclaration

func (s DeclarationScope) Copy() DeclarationScope {
	cpy := make(DeclarationScope, len(s))
	for k, v := range s {
		cpy[k] = v
	}
	return cpy
}

func analyzeProgram(prog *ast.Program, declarations DeclarationScope) (*Program, error) {
	p := &Program{
		Body: make([]Statement, len(prog.Body)),
	}
	for i, s := range prog.Body {
		n, err := analyzeStatment(s, declarations)
		if err != nil {
			return nil, err
		}
		p.Body[i] = n
	}
	return p, nil
}

func analyzeNode(n ast.Node, declarations DeclarationScope) (Node, error) {
	switch n := n.(type) {
	case ast.Statement:
		return analyzeStatment(n, declarations)
	case ast.Expression:
		return analyzeExpression(n, declarations)
	default:
		return nil, fmt.Errorf("unsupported node %T", n)
	}
}

func analyzeStatment(s ast.Statement, declarations DeclarationScope) (Statement, error) {
	switch s := s.(type) {
	case *ast.BlockStatement:
		return analyzeBlockStatement(s, declarations)
	case *ast.OptionStatement:
		return analyzeOptionStatement(s, declarations)
	case *ast.ExpressionStatement:
		return analyzeExpressionStatement(s, declarations)
	case *ast.ReturnStatement:
		return analyzeReturnStatement(s, declarations)
	case *ast.VariableDeclaration:
		// Expect a single declaration
		if len(s.Declarations) != 1 {
			return nil, fmt.Errorf("only single variable declarations are supported, found %d declarations", len(s.Declarations))
		}
		return analyzeVariableDeclaration(s.Declarations[0], declarations)
	default:
		return nil, fmt.Errorf("unsupported statement %T", s)
	}
}

func analyzeBlockStatement(block *ast.BlockStatement, declarations DeclarationScope) (*BlockStatement, error) {
	declarations = declarations.Copy()
	b := &BlockStatement{
		Body: make([]Statement, len(block.Body)),
	}
	for i, s := range block.Body {
		n, err := analyzeStatment(s, declarations)
		if err != nil {
			return nil, err
		}
		b.Body[i] = n
	}
	last := len(b.Body) - 1
	if _, ok := b.Body[last].(*ReturnStatement); !ok {
		return nil, errors.New("missing return statement in block")
	}
	return b, nil
}

func analyzeOptionStatement(option *ast.OptionStatement, declarations DeclarationScope) (*OptionStatement, error) {
	declaration, err := analyzeVariableDeclaration(option.Declaration, declarations)
	if err != nil {
		return nil, err
	}
	return &OptionStatement{
		Declaration: declaration,
	}, nil
}

func analyzeExpressionStatement(expr *ast.ExpressionStatement, declarations DeclarationScope) (*ExpressionStatement, error) {
	e, err := analyzeExpression(expr.Expression, declarations)
	if err != nil {
		return nil, err
	}
	return &ExpressionStatement{
		Expression: e,
	}, nil
}

func analyzeReturnStatement(ret *ast.ReturnStatement, declarations DeclarationScope) (*ReturnStatement, error) {
	arg, err := analyzeExpression(ret.Argument, declarations)
	if err != nil {
		return nil, err
	}
	return &ReturnStatement{
		Argument: arg,
	}, nil
}

func analyzeVariableDeclaration(decl *ast.VariableDeclarator, declarations DeclarationScope) (*NativeVariableDeclaration, error) {
	id, err := analyzeIdentifier(decl.ID, declarations)
	if err != nil {
		return nil, err
	}
	init, err := analyzeExpression(decl.Init, declarations)
	if err != nil {
		return nil, err
	}
	vd := &NativeVariableDeclaration{
		Identifier: id,
		Init:       init,
	}
	declarations[vd.Identifier.Name] = vd
	return vd, nil
}

func analyzeExpression(expr ast.Expression, declarations DeclarationScope) (Expression, error) {
	switch expr := expr.(type) {
	case *ast.ArrowFunctionExpression:
		return analyzeArrowFunctionExpression(expr, declarations)
	case *ast.CallExpression:
		return analyzeCallExpression(expr, declarations)
	case *ast.MemberExpression:
		return analyzeMemberExpression(expr, declarations)
	case *ast.PipeExpression:
		return analyzePipeExpression(expr, declarations)
	case *ast.BinaryExpression:
		return analyzeBinaryExpression(expr, declarations)
	case *ast.UnaryExpression:
		return analyzeUnaryExpression(expr, declarations)
	case *ast.LogicalExpression:
		return analyzeLogicalExpression(expr, declarations)
	case *ast.ObjectExpression:
		return analyzeObjectExpression(expr, declarations)
	case *ast.ArrayExpression:
		return analyzeArrayExpression(expr, declarations)
	case *ast.Identifier:
		return analyzeIdentifierExpression(expr, declarations)
	case ast.Literal:
		return analyzeLiteral(expr, declarations)
	default:
		return nil, fmt.Errorf("unsupported expression %T", expr)
	}
}

func analyzeLiteral(lit ast.Literal, declarations DeclarationScope) (Literal, error) {
	switch lit := lit.(type) {
	case *ast.StringLiteral:
		return analyzeStringLiteral(lit, declarations)
	case *ast.BooleanLiteral:
		return analyzeBooleanLiteral(lit, declarations)
	case *ast.FloatLiteral:
		return analyzeFloatLiteral(lit, declarations)
	case *ast.IntegerLiteral:
		return analyzeIntegerLiteral(lit, declarations)
	case *ast.UnsignedIntegerLiteral:
		return analyzeUnsignedIntegerLiteral(lit, declarations)
	case *ast.RegexpLiteral:
		return analyzeRegexpLiteral(lit, declarations)
	case *ast.DurationLiteral:
		return analyzeDurationLiteral(lit, declarations)
	case *ast.DateTimeLiteral:
		return analyzeDateTimeLiteral(lit, declarations)
	case *ast.PipeLiteral:
		return nil, errors.New("a pipe literal may only be used as a default value for an argument in a function definition")
	default:
		return nil, fmt.Errorf("unsupported literal %T", lit)
	}
}

func analyzeArrowFunctionExpression(arrow *ast.ArrowFunctionExpression, declarations DeclarationScope) (*FunctionExpression, error) {
	declarations = declarations.Copy()
	f := &FunctionExpression{
		Params: make([]*FunctionParam, len(arrow.Params)),
	}
	pipedCount := 0
	for i, p := range arrow.Params {
		key, err := analyzeIdentifier(p.Key, declarations)
		if err != nil {
			return nil, err
		}

		var (
			def         Expression
			declaration VariableDeclaration
			piped       bool
		)
		if p.Value != nil {
			if _, ok := p.Value.(*ast.PipeLiteral); ok {
				// Special case the PipeLiteral
				piped = true
				pipedCount++
				if pipedCount > 1 {
					return nil, errors.New("only a single argument may be piped")
				}
			} else {
				d, err := analyzeExpression(p.Value, declarations)
				if err != nil {
					return nil, err
				}
				def = d
				declaration = &NativeVariableDeclaration{
					Identifier: key,
					Init:       def,
				}
				declarations[key.Name] = declaration
			}
		}

		f.Params[i] = &FunctionParam{
			Key:         key,
			Default:     def,
			Piped:       piped,
			declaration: declaration,
		}

	}

	b, err := analyzeNode(arrow.Body, declarations)
	if err != nil {
		return nil, err
	}
	f.Body = b

	return f, nil
}

func analyzeCallExpression(call *ast.CallExpression, declarations DeclarationScope) (*CallExpression, error) {
	callee, err := analyzeExpression(call.Callee, declarations)
	if err != nil {
		return nil, err
	}
	var args *ObjectExpression
	if l := len(call.Arguments); l > 1 {
		return nil, fmt.Errorf("arguments are not a single object expression %v", args)
	} else if l == 1 {
		obj, ok := call.Arguments[0].(*ast.ObjectExpression)
		if !ok {
			return nil, fmt.Errorf("arguments not an object expression")
		}
		var err error
		args, err = analyzeObjectExpression(obj, declarations)
		if err != nil {
			return nil, err
		}
	} else {
		args = new(ObjectExpression)
	}

	expr := &CallExpression{
		Callee:    callee,
		Arguments: args,
	}

	declarations = declarations.Copy()
	for _, arg := range args.Properties {
		declarations[arg.Key.Name] = &NativeVariableDeclaration{
			Identifier: arg.Key,
			Init:       arg.Value,
		}
	}

	ApplyNewDeclarations(expr.Callee, declarations)
	return expr, nil
}

func ApplyNewDeclarations(n Node, declarations map[string]VariableDeclaration) {
	v := &applyDeclarationsVisitor{
		declarations: declarations,
	}
	Walk(v, n)
}

type applyDeclarationsVisitor struct {
	declarations DeclarationScope
}

func (v *applyDeclarationsVisitor) Visit(n Node) Visitor {
	switch n := n.(type) {
	case *IdentifierExpression:
		if n.declaration == nil {
			n.declaration = v.declarations[n.Name]
		}
		// No need to walk further down this branch
		return nil
	// TODO(nathanielc): Support polymorphic function arguments
	//case *FunctionExpression:
	//	// Remove type information since we may have changed it.
	//	n.typ.Store((Type)(Invalid))
	case *FunctionParam:
		if n.declaration == nil {
			n.declaration = v.declarations[n.Key.Name]
		}
		// No need to walk further down this branch
		return nil
	}
	return v
}
func (v *applyDeclarationsVisitor) Done() {}

func analyzeMemberExpression(member *ast.MemberExpression, declarations DeclarationScope) (*MemberExpression, error) {
	obj, err := analyzeExpression(member.Object, declarations)
	if err != nil {
		return nil, err
	}

	var propertyName string
	switch p := member.Property.(type) {
	case *ast.Identifier:
		propertyName = p.Name
	case *ast.StringLiteral:
		propertyName = p.Value
	case *ast.IntegerLiteral:
		propertyName = strconv.FormatInt(p.Value, 10)
	default:
		return nil, fmt.Errorf("unsupported member property expression of type %T", member.Property)
	}

	return &MemberExpression{
		Object:   obj,
		Property: propertyName,
	}, nil
}

func analyzePipeExpression(pipe *ast.PipeExpression, declarations DeclarationScope) (*CallExpression, error) {
	call, err := analyzeCallExpression(pipe.Call, declarations)
	if err != nil {
		return nil, err
	}

	decl, err := resolveDeclaration(call.Callee)
	if err != nil {
		return nil, err
	}
	fnTyp := decl.InitType()
	if fnTyp.Kind() != Function {
		return nil, fmt.Errorf("cannot pipe into non function %q", fnTyp.Kind())
	}
	key := fnTyp.PipeArgument()
	if key == "" {
		return nil, fmt.Errorf("function %q does not have a pipe argument", decl.ID().Name)
	}

	value, err := analyzeExpression(pipe.Argument, declarations)
	if err != nil {
		return nil, err
	}
	property := &Property{
		Key:   &Identifier{Name: key},
		Value: value,
	}

	found := false
	for i, p := range call.Arguments.Properties {
		if key == p.Key.Name {
			found = true
			call.Arguments.Properties[i] = property
			break
		}
	}
	if !found {
		call.Arguments.Properties = append(call.Arguments.Properties, property)
	}
	return call, nil
}

// resolveDeclaration traverse the expression until a variable declaration is found for the expression.
func resolveDeclaration(n Node) (VariableDeclaration, error) {
	switch n := n.(type) {
	case *IdentifierExpression:
		if n.declaration == nil {
			return nil, fmt.Errorf("identifier expression %q has no declaration", n.Name)
		}
		return resolveDeclaration(n.declaration)
	case *ExternalVariableDeclaration:
		return n, nil
	case *NativeVariableDeclaration:
		if n.Init == nil {
			return nil, fmt.Errorf("variable declaration %v has no init", n.Identifier)
		}
		if i, ok := n.Init.(*IdentifierExpression); ok {
			return resolveDeclaration(i)
		}
		return n, nil
	}
	return nil, errors.New("no declaration found")
}

func analyzeBinaryExpression(binary *ast.BinaryExpression, declarations DeclarationScope) (*BinaryExpression, error) {
	left, err := analyzeExpression(binary.Left, declarations)
	if err != nil {
		return nil, err
	}
	right, err := analyzeExpression(binary.Right, declarations)
	if err != nil {
		return nil, err
	}
	return &BinaryExpression{
		Operator: binary.Operator,
		Left:     left,
		Right:    right,
	}, nil
}

func analyzeUnaryExpression(unary *ast.UnaryExpression, declarations DeclarationScope) (*UnaryExpression, error) {
	arg, err := analyzeExpression(unary.Argument, declarations)
	if err != nil {
		return nil, err
	}
	// TODO(nathanielc): validate operand type once we have type inference working with functions.
	//k := arg.Type().Kind()
	//if k != Bool && k != Int && k != Float && k != Duration {
	//	return nil, fmt.Errorf("invalid unary operator %v on type %v", unary.Operator, k)
	//}
	return &UnaryExpression{
		Operator: unary.Operator,
		Argument: arg,
	}, nil
}
func analyzeLogicalExpression(logical *ast.LogicalExpression, declarations DeclarationScope) (*LogicalExpression, error) {
	left, err := analyzeExpression(logical.Left, declarations)
	if err != nil {
		return nil, err
	}
	// TODO(nathanielc): Validate operand types once we have type inference working with functions.
	//if k := left.Type().Kind(); k != Bool {
	//	return nil, fmt.Errorf("left operand to logical expression is not a boolean, got kind %v", k)
	//}
	right, err := analyzeExpression(logical.Right, declarations)
	if err != nil {
		return nil, err
	}
	//if k := right.Type().Kind(); k != Bool {
	//	return nil, fmt.Errorf("right operand to logical expression is not a boolean, got kind %v", k)
	//}
	return &LogicalExpression{
		Operator: logical.Operator,
		Left:     left,
		Right:    right,
	}, nil
}
func analyzeObjectExpression(obj *ast.ObjectExpression, declarations DeclarationScope) (*ObjectExpression, error) {
	o := &ObjectExpression{
		Properties: make([]*Property, len(obj.Properties)),
	}
	for i, p := range obj.Properties {
		n, err := analyzeProperty(p, declarations)
		if err != nil {
			return nil, err
		}
		o.Properties[i] = n
	}
	return o, nil
}
func analyzeArrayExpression(array *ast.ArrayExpression, declarations DeclarationScope) (*ArrayExpression, error) {
	a := &ArrayExpression{
		Elements: make([]Expression, len(array.Elements)),
	}
	for i, e := range array.Elements {
		n, err := analyzeExpression(e, declarations)
		if err != nil {
			return nil, err
		}
		a.Elements[i] = n
	}
	return a, nil
}

func analyzeIdentifier(ident *ast.Identifier, declarations DeclarationScope) (*Identifier, error) {
	return &Identifier{
		Name: ident.Name,
	}, nil
}

func analyzeIdentifierExpression(ident *ast.Identifier, declarations DeclarationScope) (*IdentifierExpression, error) {
	return &IdentifierExpression{
		Name:        ident.Name,
		declaration: declarations[ident.Name],
	}, nil
}

func analyzeProperty(property *ast.Property, declarations DeclarationScope) (*Property, error) {
	key, err := analyzeIdentifier(property.Key, declarations)
	if err != nil {
		return nil, err
	}
	value, err := analyzeExpression(property.Value, declarations)
	if err != nil {
		return nil, err
	}
	return &Property{
		Key:   key,
		Value: value,
	}, nil
}

func analyzeDateTimeLiteral(lit *ast.DateTimeLiteral, declarations DeclarationScope) (*DateTimeLiteral, error) {
	return &DateTimeLiteral{
		Value: lit.Value,
	}, nil
}
func analyzeDurationLiteral(lit *ast.DurationLiteral, declarations DeclarationScope) (*DurationLiteral, error) {
	return &DurationLiteral{
		Value: lit.Value,
	}, nil
}
func analyzeFloatLiteral(lit *ast.FloatLiteral, declarations DeclarationScope) (*FloatLiteral, error) {
	return &FloatLiteral{
		Value: lit.Value,
	}, nil
}
func analyzeIntegerLiteral(lit *ast.IntegerLiteral, declarations DeclarationScope) (*IntegerLiteral, error) {
	return &IntegerLiteral{
		Value: lit.Value,
	}, nil
}
func analyzeUnsignedIntegerLiteral(lit *ast.UnsignedIntegerLiteral, declarations DeclarationScope) (*UnsignedIntegerLiteral, error) {
	return &UnsignedIntegerLiteral{
		Value: lit.Value,
	}, nil
}
func analyzeStringLiteral(lit *ast.StringLiteral, declarations DeclarationScope) (*StringLiteral, error) {
	return &StringLiteral{
		Value: lit.Value,
	}, nil
}
func analyzeBooleanLiteral(lit *ast.BooleanLiteral, declarations DeclarationScope) (*BooleanLiteral, error) {
	return &BooleanLiteral{
		Value: lit.Value,
	}, nil
}
func analyzeRegexpLiteral(lit *ast.RegexpLiteral, declarations DeclarationScope) (*RegexpLiteral, error) {
	return &RegexpLiteral{
		Value: lit.Value,
	}, nil
}
