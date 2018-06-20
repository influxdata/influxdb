package ast

import (
	"encoding/json"
	"fmt"
	"regexp"
	"time"
)

// Position represents a specific location in the source
type Position struct {
	Line   int `json:"line"`   // Line is the line in the source marked by this position
	Column int `json:"column"` // Column is the column in the source marked by this position
}

// SourceLocation represents the location of a node in the AST
type SourceLocation struct {
	Start  Position `json:"start"`            // Start is the location in the source the node starts
	End    Position `json:"end"`              // End is the location in the source the node ends
	Source *string  `json:"source,omitempty"` // Source is optional raw source
}

// Node represents a node in the InfluxDB abstract syntax tree.
type Node interface {
	node()
	Type() string // Type property is a string that contains the variant type of the node
	Location() *SourceLocation
	Copy() Node

	// All node must support json marshalling
	json.Marshaler
}

func (*Program) node() {}

func (*BlockStatement) node()      {}
func (*ExpressionStatement) node() {}
func (*ReturnStatement) node()     {}
func (*OptionStatement) node()     {}
func (*VariableDeclaration) node() {}
func (*VariableDeclarator) node()  {}

func (*ArrayExpression) node()         {}
func (*ArrowFunctionExpression) node() {}
func (*BinaryExpression) node()        {}
func (*CallExpression) node()          {}
func (*ConditionalExpression) node()   {}
func (*LogicalExpression) node()       {}
func (*MemberExpression) node()        {}
func (*PipeExpression) node()          {}
func (*ObjectExpression) node()        {}
func (*UnaryExpression) node()         {}

func (*Property) node()   {}
func (*Identifier) node() {}

func (*BooleanLiteral) node()         {}
func (*DateTimeLiteral) node()        {}
func (*DurationLiteral) node()        {}
func (*FloatLiteral) node()           {}
func (*IntegerLiteral) node()         {}
func (*PipeLiteral) node()            {}
func (*RegexpLiteral) node()          {}
func (*StringLiteral) node()          {}
func (*UnsignedIntegerLiteral) node() {}

// BaseNode holds the attributes every expression or statement should have
type BaseNode struct {
	Loc *SourceLocation `json:"location,omitempty"`
}

// Location is the source location of the Node
func (b *BaseNode) Location() *SourceLocation { return b.Loc }

// Program represents a complete program source tree
type Program struct {
	*BaseNode
	Body []Statement `json:"body"`
}

// Type is the abstract type
func (*Program) Type() string { return "Program" }

func (p *Program) Copy() Node {
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

// Statement Perhaps we don't even want statements nor expression statements
type Statement interface {
	Node
	stmt()
}

func (*BlockStatement) stmt()      {}
func (*ExpressionStatement) stmt() {}
func (*ReturnStatement) stmt()     {}
func (*VariableDeclaration) stmt() {}
func (*OptionStatement) stmt()     {}

// BlockStatement is a set of statements
type BlockStatement struct {
	*BaseNode
	Body []Statement `json:"body"`
}

// Type is the abstract type
func (*BlockStatement) Type() string { return "BlockStatement" }

func (s *BlockStatement) Copy() Node {
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

// ExpressionStatement may consist of an expression that does not return a value and is executed solely for its side-effects.
type ExpressionStatement struct {
	*BaseNode
	Expression Expression `json:"expression"`
}

// Type is the abstract type
func (*ExpressionStatement) Type() string { return "ExpressionStatement" }

func (s *ExpressionStatement) Copy() Node {
	if s == nil {
		return s
	}
	ns := new(ExpressionStatement)
	*ns = *s

	ns.Expression = s.Expression.Copy().(Expression)

	return ns
}

// ReturnStatement defines an Expression to return
type ReturnStatement struct {
	*BaseNode
	Argument Expression `json:"argument"`
}

// Type is the abstract type
func (*ReturnStatement) Type() string { return "ReturnStatement" }
func (s *ReturnStatement) Copy() Node {
	if s == nil {
		return s
	}
	ns := new(ReturnStatement)
	*ns = *s

	ns.Argument = s.Argument.Copy().(Expression)

	return ns
}

// OptionStatement syntactically is a single variable declaration
type OptionStatement struct {
	*BaseNode
	Declaration *VariableDeclarator `json:"declaration"`
}

// Type is the abstract type
func (*OptionStatement) Type() string { return "OptionStatement" }

// Copy returns a deep copy of an OptionStatement Node
func (s *OptionStatement) Copy() Node {
	if s == nil {
		return s
	}
	ns := new(OptionStatement)
	*ns = *s

	ns.Declaration = s.Declaration.Copy().(*VariableDeclarator)

	return ns
}

// VariableDeclaration declares one or more variables using assignment
type VariableDeclaration struct {
	*BaseNode
	Declarations []*VariableDeclarator `json:"declarations"`
}

// Type is the abstract type
func (*VariableDeclaration) Type() string { return "VariableDeclaration" }

func (d *VariableDeclaration) Copy() Node {
	if d == nil {
		return d
	}
	nd := new(VariableDeclaration)
	*nd = *d

	if len(d.Declarations) > 0 {
		nd.Declarations = make([]*VariableDeclarator, len(d.Declarations))
		for i, decl := range d.Declarations {
			nd.Declarations[i] = decl.Copy().(*VariableDeclarator)
		}
	}

	return nd
}

// VariableDeclarator represents the declaration of a variable
type VariableDeclarator struct {
	*BaseNode
	ID   *Identifier `json:"id"`
	Init Expression  `json:"init"`
}

// Type is the abstract type
func (*VariableDeclarator) Type() string { return "VariableDeclarator" }

func (d *VariableDeclarator) Copy() Node {
	if d == nil {
		return d
	}
	nd := new(VariableDeclarator)
	*nd = *d

	nd.Init = d.Init.Copy().(Expression)

	return nd
}

// Expression represents an action that can be performed by InfluxDB that can be evaluated to a value.
type Expression interface {
	Node
	expression()
}

func (*ArrayExpression) expression()         {}
func (*ArrowFunctionExpression) expression() {}
func (*BinaryExpression) expression()        {}
func (*BooleanLiteral) expression()          {}
func (*CallExpression) expression()          {}
func (*ConditionalExpression) expression()   {}
func (*DateTimeLiteral) expression()         {}
func (*DurationLiteral) expression()         {}
func (*FloatLiteral) expression()            {}
func (*Identifier) expression()              {}
func (*IntegerLiteral) expression()          {}
func (*LogicalExpression) expression()       {}
func (*MemberExpression) expression()        {}
func (*ObjectExpression) expression()        {}
func (*PipeExpression) expression()          {}
func (*PipeLiteral) expression()             {}
func (*RegexpLiteral) expression()           {}
func (*StringLiteral) expression()           {}
func (*UnaryExpression) expression()         {}
func (*UnsignedIntegerLiteral) expression()  {}

// CallExpression represents a function all whose callee may be an Identifier or MemberExpression
type CallExpression struct {
	*BaseNode
	Callee    Expression   `json:"callee"`
	Arguments []Expression `json:"arguments,omitempty"`
}

// Type is the abstract type
func (*CallExpression) Type() string { return "CallExpression" }

func (e *CallExpression) Copy() Node {
	if e == nil {
		return e
	}
	ne := new(CallExpression)
	*ne = *e

	ne.Callee = e.Callee.Copy().(Expression)

	if len(e.Arguments) > 0 {
		ne.Arguments = make([]Expression, len(e.Arguments))
		for i, arg := range e.Arguments {
			ne.Arguments[i] = arg.Copy().(Expression)
		}
	}

	return ne
}

type PipeExpression struct {
	*BaseNode
	Argument Expression      `json:"argument"`
	Call     *CallExpression `json:"call"`
}

// Type is the abstract type
func (*PipeExpression) Type() string { return "PipeExpression" }

func (e *PipeExpression) Copy() Node {
	if e == nil {
		return e
	}
	ne := new(PipeExpression)
	*ne = *e

	ne.Argument = e.Argument.Copy().(Expression)
	ne.Call = e.Call.Copy().(*CallExpression)

	return ne
}

// MemberExpression represents calling a property of a CallExpression
type MemberExpression struct {
	*BaseNode
	Object   Expression `json:"object"`
	Property Expression `json:"property"`
}

// Type is the abstract type
func (*MemberExpression) Type() string { return "MemberExpression" }

func (e *MemberExpression) Copy() Node {
	if e == nil {
		return e
	}
	ne := new(MemberExpression)
	*ne = *e

	ne.Object = e.Object.Copy().(Expression)
	ne.Property = e.Property.Copy().(Expression)

	return ne
}

type ArrowFunctionExpression struct {
	*BaseNode
	Params []*Property `json:"params"`
	Body   Node        `json:"body"`
}

// Type is the abstract type
func (*ArrowFunctionExpression) Type() string { return "ArrowFunctionExpression" }

func (e *ArrowFunctionExpression) Copy() Node {
	if e == nil {
		return e
	}
	ne := new(ArrowFunctionExpression)
	*ne = *e

	if len(e.Params) > 0 {
		ne.Params = make([]*Property, len(e.Params))
		for i, param := range e.Params {
			ne.Params[i] = param.Copy().(*Property)
		}
	}

	ne.Body = e.Body.Copy()

	return ne
}

// OperatorKind are Equality and Arithmatic operators.
// Result of evaluating an equality operator is always of type Boolean based on whether the
// comparison is true
// Arithmetic operators take numerical values (either literals or variables) as their operands
//  and return a single numerical value.
type OperatorKind int

const (
	opBegin OperatorKind = iota
	MultiplicationOperator
	DivisionOperator
	AdditionOperator
	SubtractionOperator
	LessThanEqualOperator
	LessThanOperator
	GreaterThanEqualOperator
	GreaterThanOperator
	StartsWithOperator
	InOperator
	NotOperator
	NotEmptyOperator
	EmptyOperator
	EqualOperator
	NotEqualOperator
	RegexpMatchOperator
	NotRegexpMatchOperator
	opEnd
)

func (o OperatorKind) String() string {
	return OperatorTokens[o]
}

// OperatorLookup converts the operators to OperatorKind
func OperatorLookup(op string) OperatorKind {
	return operators[op]
}

func (o OperatorKind) MarshalText() ([]byte, error) {
	text, ok := OperatorTokens[o]
	if !ok {
		return nil, fmt.Errorf("unknown operator %d", int(o))
	}
	return []byte(text), nil
}
func (o *OperatorKind) UnmarshalText(data []byte) error {
	var ok bool
	*o, ok = operators[string(data)]
	if !ok {
		return fmt.Errorf("unknown operator %q", string(data))
	}
	return nil
}

// BinaryExpression use binary operators act on two operands in an expression.
// BinaryExpression includes relational and arithmatic operators
type BinaryExpression struct {
	*BaseNode
	Operator OperatorKind `json:"operator"`
	Left     Expression   `json:"left"`
	Right    Expression   `json:"right"`
}

// Type is the abstract type
func (*BinaryExpression) Type() string { return "BinaryExpression" }

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

// UnaryExpression use operators act on a single operand in an expression.
type UnaryExpression struct {
	*BaseNode
	Operator OperatorKind `json:"operator"`
	Argument Expression   `json:"argument"`
}

// Type is the abstract type
func (*UnaryExpression) Type() string { return "UnaryExpression" }

func (e *UnaryExpression) Copy() Node {
	if e == nil {
		return e
	}
	ne := new(UnaryExpression)
	*ne = *e

	ne.Argument = e.Argument.Copy().(Expression)

	return ne
}

// LogicalOperatorKind are used with boolean (logical) values
type LogicalOperatorKind int

const (
	logOpBegin LogicalOperatorKind = iota
	AndOperator
	OrOperator
	logOpEnd
)

func (o LogicalOperatorKind) String() string {
	return LogicalOperatorTokens[o]
}

// LogicalOperatorLookup converts the operators to LogicalOperatorKind
func LogicalOperatorLookup(op string) LogicalOperatorKind {
	return logOperators[op]
}

func (o LogicalOperatorKind) MarshalText() ([]byte, error) {
	text, ok := LogicalOperatorTokens[o]
	if !ok {
		return nil, fmt.Errorf("unknown logical operator %d", int(o))
	}
	return []byte(text), nil
}
func (o *LogicalOperatorKind) UnmarshalText(data []byte) error {
	var ok bool
	*o, ok = logOperators[string(data)]
	if !ok {
		return fmt.Errorf("unknown logical operator %q", string(data))
	}
	return nil
}

// LogicalExpression represent the rule conditions that collectively evaluate to either true or false.
// `or` expressions compute the disjunction of two boolean expressions and return boolean values.
// `and`` expressions compute the conjunction of two boolean expressions and return boolean values.
type LogicalExpression struct {
	*BaseNode
	Operator LogicalOperatorKind `json:"operator"`
	Left     Expression          `json:"left"`
	Right    Expression          `json:"right"`
}

// Type is the abstract type
func (*LogicalExpression) Type() string { return "LogicalExpression" }

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

// ArrayExpression is used to create and directly specify the elements of an array object
type ArrayExpression struct {
	*BaseNode
	Elements []Expression `json:"elements"`
}

// Type is the abstract type
func (*ArrayExpression) Type() string { return "ArrayExpression" }

func (e *ArrayExpression) Copy() Node {
	if e == nil {
		return e
	}
	ne := new(ArrayExpression)
	*ne = *e

	if len(e.Elements) > 0 {
		ne.Elements = make([]Expression, len(e.Elements))
		for i, el := range e.Elements {
			ne.Elements[i] = el.Copy().(Expression)
		}
	}

	return ne
}

// ObjectExpression allows the declaration of an anonymous object within a declaration.
type ObjectExpression struct {
	*BaseNode
	Properties []*Property `json:"properties"`
}

// Type is the abstract type
func (*ObjectExpression) Type() string { return "ObjectExpression" }

func (e *ObjectExpression) Copy() Node {
	if e == nil {
		return e
	}
	ne := new(ObjectExpression)
	*ne = *e

	if len(e.Properties) > 0 {
		ne.Properties = make([]*Property, len(e.Properties))
		for i, p := range e.Properties {
			ne.Properties[i] = p.Copy().(*Property)
		}
	}

	return ne
}

// ConditionalExpression selects one of two expressions, `Alternate` or `Consequent`
// depending on a third, boolean, expression, `Test`.
type ConditionalExpression struct {
	*BaseNode
	Test       Expression `json:"test"`
	Alternate  Expression `json:"alternate"`
	Consequent Expression `json:"consequent"`
}

// Type is the abstract type
func (*ConditionalExpression) Type() string { return "ConditionalExpression" }

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

// Property is the value associated with a key
type Property struct {
	*BaseNode
	Key   *Identifier `json:"key"`
	Value Expression  `json:"value"`
}

func (p *Property) Copy() Node {
	if p == nil {
		return p
	}
	np := new(Property)
	*np = *p

	if p.Value != nil {
		np.Value = p.Value.Copy().(Expression)
	}

	return np
}

// Type is the abstract type
func (*Property) Type() string { return "Property" }

// Identifier represents a name that identifies a unique Node
type Identifier struct {
	*BaseNode
	Name string `json:"name"`
}

// Type is the abstract type
func (*Identifier) Type() string { return "Identifier" }

func (i *Identifier) Copy() Node {
	if i == nil {
		return i
	}
	ni := new(Identifier)
	*ni = *i
	return ni
}

// Literal are thelexical forms for literal expressions which define
// boolean, string, integer, number, duration, datetime and field values.
// Literals must be coerced explicitly.
type Literal interface {
	Expression
	literal()
}

func (*BooleanLiteral) literal()         {}
func (*DateTimeLiteral) literal()        {}
func (*DurationLiteral) literal()        {}
func (*FloatLiteral) literal()           {}
func (*IntegerLiteral) literal()         {}
func (*PipeLiteral) literal()            {}
func (*RegexpLiteral) literal()          {}
func (*StringLiteral) literal()          {}
func (*UnsignedIntegerLiteral) literal() {}

// PipeLiteral represents an specialized literal value, indicating the left hand value of a pipe expression.
type PipeLiteral struct {
	*BaseNode
}

// Type is the abstract type
func (*PipeLiteral) Type() string { return "PipeLiteral" }

func (i *PipeLiteral) Copy() Node {
	if i == nil {
		return i
	}
	ni := new(PipeLiteral)
	*ni = *i
	return ni
}

// StringLiteral expressions begin and end with double quote marks.
type StringLiteral struct {
	*BaseNode
	Value string `json:"value"`
}

func (*StringLiteral) Type() string { return "StringLiteral" }

func (l *StringLiteral) Copy() Node {
	if l == nil {
		return l
	}
	nl := new(StringLiteral)
	*nl = *l
	return nl
}

// BooleanLiteral represent boolean values
type BooleanLiteral struct {
	*BaseNode
	Value bool `json:"value"`
}

// Type is the abstract type
func (*BooleanLiteral) Type() string { return "BooleanLiteral" }

func (l *BooleanLiteral) Copy() Node {
	if l == nil {
		return l
	}
	nl := new(BooleanLiteral)
	*nl = *l
	return nl
}

// FloatLiteral  represent floating point numbers according to the double representations defined by the IEEE-754-1985
type FloatLiteral struct {
	*BaseNode
	Value float64 `json:"value"`
}

// Type is the abstract type
func (*FloatLiteral) Type() string { return "FloatLiteral" }

func (l *FloatLiteral) Copy() Node {
	if l == nil {
		return l
	}
	nl := new(FloatLiteral)
	*nl = *l
	return nl
}

// IntegerLiteral represent integer numbers.
type IntegerLiteral struct {
	*BaseNode
	Value int64 `json:"value"`
}

// Type is the abstract type
func (*IntegerLiteral) Type() string { return "IntegerLiteral" }

func (l *IntegerLiteral) Copy() Node {
	if l == nil {
		return l
	}
	nl := new(IntegerLiteral)
	*nl = *l
	return nl
}

// UnsignedIntegerLiteral represent integer numbers.
type UnsignedIntegerLiteral struct {
	*BaseNode
	Value uint64 `json:"value"`
}

// Type is the abstract type
func (*UnsignedIntegerLiteral) Type() string { return "UnsignedIntegerLiteral" }

func (l *UnsignedIntegerLiteral) Copy() Node {
	if l == nil {
		return l
	}
	nl := new(UnsignedIntegerLiteral)
	*nl = *l
	return nl
}

// RegexpLiteral expressions begin and end with `/` and are regular expressions with syntax accepted by RE2
type RegexpLiteral struct {
	*BaseNode
	Value *regexp.Regexp `json:"value"`
}

// Type is the abstract type
func (*RegexpLiteral) Type() string { return "RegexpLiteral" }

func (l *RegexpLiteral) Copy() Node {
	if l == nil {
		return l
	}
	nl := new(RegexpLiteral)
	*nl = *l
	return nl
}

// DurationLiteral represents the elapsed time between two instants as an
// int64 nanosecond count with syntax of golang's time.Duration
// TODO: this may be better as a class initialization
type DurationLiteral struct {
	*BaseNode
	Value time.Duration `json:"value"`
}

// Type is the abstract type
func (*DurationLiteral) Type() string { return "DurationLiteral" }

func (l *DurationLiteral) Copy() Node {
	if l == nil {
		return l
	}
	nl := new(DurationLiteral)
	*nl = *l
	return nl
}

// DateTimeLiteral represents an instant in time with nanosecond precision using
// the syntax of golang's RFC3339 Nanosecond variant
// TODO: this may be better as a class initialization
type DateTimeLiteral struct {
	*BaseNode
	Value time.Time `json:"value"`
}

// Type is the abstract type
func (*DateTimeLiteral) Type() string { return "DateTimeLiteral" }

func (l *DateTimeLiteral) Copy() Node {
	if l == nil {
		return l
	}
	nl := new(DateTimeLiteral)
	*nl = *l
	return nl
}

// OperatorTokens converts OperatorKind to string
var OperatorTokens = map[OperatorKind]string{
	MultiplicationOperator:   "*",
	DivisionOperator:         "/",
	AdditionOperator:         "+",
	SubtractionOperator:      "-",
	LessThanEqualOperator:    "<=",
	LessThanOperator:         "<",
	GreaterThanOperator:      ">",
	GreaterThanEqualOperator: ">=",
	InOperator:               "in",
	NotOperator:              "not",
	NotEmptyOperator:         "not empty",
	EmptyOperator:            "empty",
	StartsWithOperator:       "startswith",
	EqualOperator:            "==",
	NotEqualOperator:         "!=",
	RegexpMatchOperator:      "=~",
	NotRegexpMatchOperator:   "!~",
}

// LogicalOperatorTokens converts LogicalOperatorKind to string
var LogicalOperatorTokens = map[LogicalOperatorKind]string{
	AndOperator: "and",
	OrOperator:  "or",
}

var operators map[string]OperatorKind
var logOperators map[string]LogicalOperatorKind

func init() {
	operators = make(map[string]OperatorKind)
	for op := opBegin + 1; op < opEnd; op++ {
		operators[OperatorTokens[op]] = op
	}

	logOperators = make(map[string]LogicalOperatorKind)
	for op := logOpBegin + 1; op < logOpEnd; op++ {
		logOperators[LogicalOperatorTokens[op]] = op
	}
}
