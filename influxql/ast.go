package influxql

import (
	"time"
)

// DataType represents the primitive data types available in InfluxQL.
type DataType string

const (
	Number   = DataType("number")
	Boolean  = DataType("boolean")
	String   = DataType("string")
	Time     = DataType("time")
	Duration = DataType("duration")
)

// Node represents a node in the InfluxDB abstract syntax tree.
type Node interface {
	node()
}

func (_ *Query) node()     {}
func (_ Statements) node() {}

func (_ *SelectStatement) node()                {}
func (_ *DeleteStatement) node()                {}
func (_ *ListSeriesStatement) node()            {}
func (_ *DropSeriesStatement) node()            {}
func (_ *ListContinuousQueriesStatement) node() {}
func (_ *CreateContinuousQueryStatement) node() {}
func (_ *DropContinuousQueryStatement) node()   {}

func (_ Fields) node()           {}
func (_ *Field) node()           {}
func (_ Dimensions) node()       {}
func (_ *Dimension) node()       {}
func (_ *Series) node()          {}
func (_ *InnerJoin) node()       {}
func (_ *Merge) node()           {}
func (_ *VarRef) node()          {}
func (_ *Call) node()            {}
func (_ *NumberLiteral) node()   {}
func (_ *StringLiteral) node()   {}
func (_ *BooleanLiteral) node()  {}
func (_ *TimeLiteral) node()     {}
func (_ *DurationLiteral) node() {}
func (_ *BinaryExpr) node()      {}
func (_ *ParenExpr) node()       {}
func (_ *Wildcard) node()        {}

// Query represents a collection of order statements.
type Query struct {
	Statements Statements
}

// Statements represents a list of statements.
type Statements []Statement

// Statement represents a single command in InfluxQL.
type Statement interface {
	Node
	stmt()
}

func (_ *SelectStatement) stmt()                {}
func (_ *DeleteStatement) stmt()                {}
func (_ *ListSeriesStatement) stmt()            {}
func (_ *DropSeriesStatement) stmt()            {}
func (_ *ListContinuousQueriesStatement) stmt() {}
func (_ *CreateContinuousQueryStatement) stmt() {}
func (_ *DropContinuousQueryStatement) stmt()   {}

// Expr represents an expression that can be evaluated to a value.
type Expr interface {
	Node
	expr()
}

func (_ *VarRef) expr()          {}
func (_ *Call) expr()            {}
func (_ *NumberLiteral) expr()   {}
func (_ *StringLiteral) expr()   {}
func (_ *BooleanLiteral) expr()  {}
func (_ *TimeLiteral) expr()     {}
func (_ *DurationLiteral) expr() {}
func (_ *BinaryExpr) expr()      {}
func (_ *ParenExpr) expr()       {}
func (_ *Wildcard) expr()        {}

// Source represents a source of data for a statement.
type Source interface {
	Node
	source()
}

func (_ *Series) source()    {}
func (_ *InnerJoin) source() {}
func (_ *Merge) source()     {}

// SelectStatement represents a command for extracting data from the database.
type SelectStatement struct {
	// Expressions returned from the selection.
	Fields Fields

	// Expressions used for grouping the selection.
	Dimensions Dimensions

	// Data source that fields are extracted from.
	Source Source

	// An expression evaluated on data point.
	Condition Expr

	// Maximum number of rows to be returned.
	// Unlimited if zero.
	Limit int

	// Sort order.
	Ascending bool
}

// DeleteStatement represents a command for removing data from the database.
type DeleteStatement struct {
	// Data source that values are removed from.
	Source Source

	// An expression evaluated on data point.
	Condition Expr
}

// ListSeriesStatement represents a command for listing series in the database.
type ListSeriesStatement struct{}

// DropSeriesStatement represents a command for removing a series from the database.
type DropSeriesStatement struct {
	Name string
}

// ListContinuousQueriesStatement represents a command for listing continuous queries.
type ListContinuousQueriesStatement struct{}

// CreateContinuousQueriesStatement represents a command for creating a continuous query.
type CreateContinuousQueryStatement struct {
	Name   string
	Source *SelectStatement
	Target string
}

// DropContinuousQueriesStatement represents a command for removing a continuous query.
type DropContinuousQueryStatement struct {
	Name string
}

// Fields represents a list of fields.
type Fields []*Field

// Field represents an expression retrieved from a select statement.
type Field struct {
	Expr  Expr
	Alias string
}

// Dimensions represents a list of dimensions.
type Dimensions []*Dimension

// Dimension represents an expression that a select statement is grouped by.
type Dimension struct {
	Expr Expr
}

// Series represents a single series used as a datasource.
type Series struct {
	Name string
}

// InnerJoin represents two datasources joined together.
type InnerJoin struct {
	LHS       Source
	RHS       Source
	Condition Expr
}

// Merge represents a datasource created by merging two datasources.
type Merge struct {
	LHS       Source
	RHS       Source
	Condition Expr
}

// VarRef represents a reference to a variable.
type VarRef struct {
	Val string
}

// Call represents a function call.
type Call struct {
	Name string
	Args []Expr
}

// NumberLiteral represents a numeric literal.
type NumberLiteral struct {
	Val float64
}

// BooleanLiteral represents a boolean literal.
type BooleanLiteral struct {
	Val bool
}

// StringLiteral represents a string literal.
type StringLiteral struct {
	Val string
}

// TimeLiteral represents a point-in-time literal.
type TimeLiteral struct {
	Val time.Time
}

// DurationLiteral represents a duration literal.
type DurationLiteral struct {
	Val time.Duration
}

// BinaryExpr represents an operation between two expressions.
type BinaryExpr struct {
	Op  Token
	LHS Expr
	RHS Expr
}

// ParenExpr represents a parenthesized expression.
type ParenExpr struct {
	Expr Expr
}

// Wildcard represents a wild card expression.
type Wildcard struct {
}

// Visitor can be called by Walk to traverse an AST hierarchy.
// The Visit() function is called once per node.
type Visitor interface {
	Visit(Node) Visitor
}

// Walk traverses a node hierarchy in depth-first order.
func Walk(v Visitor, node Node) {
	if v = v.Visit(node); v == nil {
		return
	}

	switch n := node.(type) {
	case *Query:
		Walk(v, n.Statements)

	case Statements:
		for _, s := range n {
			Walk(v, s)
		}

	case *SelectStatement:
		Walk(v, n.Fields)
		Walk(v, n.Dimensions)
		Walk(v, n.Source)
		Walk(v, n.Condition)

	case Fields:
		for _, c := range n {
			Walk(v, c)
		}

	case *Field:
		Walk(v, n.Expr)

	case Dimensions:
		for _, c := range n {
			Walk(v, c)
		}

	case *Dimension:
		Walk(v, n.Expr)

	case *BinaryExpr:
		Walk(v, n.LHS)
		Walk(v, n.RHS)

	case *ParenExpr:
		Walk(v, n.Expr)

	case *Call:
		for _, expr := range n.Args {
			Walk(v, expr)
		}
	}
}

// WalkFunc traverses a node hierarchy in depth-first order.
func WalkFunc(node Node, fn func(Node)) {
	Walk(walkFuncVisitor(fn), node)
}

type walkFuncVisitor func(Node)

func (fn walkFuncVisitor) Visit(n Node) Visitor { fn(n); return fn }
