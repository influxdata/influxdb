package influxql

import (
	"time"
)

// DataType represents the primitive data types available in InfluxQL.
type DataType string

const (
	Integer  = DataType("integer")
	Float    = DataType("float")
	Boolean  = DataType("boolean")
	String   = DataType("string")
	Time     = DataType("time")
	Duration = DataType("duration")
)

// Node represents a node in the InfluxDB abstract syntax tree.
type Node interface {
	node()
}

func (_ *SelectQuery) node()     {}
func (_ *DeleteQuery) node()     {}
func (_ Fields) node()           {}
func (_ *Field) node()           {}
func (_ Dimensions) node()       {}
func (_ *Dimension) node()       {}
func (_ *ImplicitJoin) node()    {}
func (_ *InnerJoin) node()       {}
func (_ *MergeJoin) node()       {}
func (_ *VarRef) node()          {}
func (_ *Call) node()            {}
func (_ *IntegerLiteral) node()  {}
func (_ *FloatLiteral) node()    {}
func (_ *StringLiteral) node()   {}
func (_ *BooleanLiteral) node()  {}
func (_ *TimeLiteral) node()     {}
func (_ *DurationLiteral) node() {}
func (_ *BinaryExpr) node()      {}

// Query represents a top-level query object.
type Query interface {
	query()
}

func (_ *SelectQuery) query() {}
func (_ *DeleteQuery) query() {}

// Expr represents an expression that can be evaluated to a value.
type Expr interface {
	Node
	expr()
}

func (_ *VarRef) expr()          {}
func (_ *Call) expr()            {}
func (_ *IntegerLiteral) expr()  {}
func (_ *FloatLiteral) expr()    {}
func (_ *StringLiteral) expr()   {}
func (_ *BooleanLiteral) expr()  {}
func (_ *TimeLiteral) expr()     {}
func (_ *DurationLiteral) expr() {}
func (_ *BinaryExpr) expr()      {}

// Join represents a join between one or more series.
type Join interface {
	Node
	join()
}

func (_ *ImplicitJoin) join() {}
func (_ *InnerJoin) join()    {}
func (_ *MergeJoin) join()    {}

// SelectQuery represents a query for extracting data from the database.
type SelectQuery struct {
	// Expressions returned from the selection.
	Fields Fields

	// Expressions used for grouping the selection.
	Dimensions Dimensions

	// Data source that fields are extracted from.
	Source Join

	// An expression evaluated on data point.
	Condition Expr

	// Maximum number of rows to be returned.
	// Unlimited if zero.
	Limit int

	// Sort order.
	Ascending bool
}

// DeleteQuery represents a query for removing data from the database.
type DeleteQuery struct {
	// Data source that values are removed from.
	Source Join

	// An expression evaluated on data point.
	Condition Expr
}

// Fields represents a list of fields.
type Fields []*Field

// Field represents an expression retrieved from a select query.
type Field struct {
	Expr  Expr
	Alias string
}

// Dimensions represents a list of dimensions.
type Dimensions []*Dimension

// Dimension represents an expression that a select query is grouped by.
type Dimension struct {
	Expr  Expr
	Alias string
}

// ImplicitJoin represents a list of tables to join on time.
type ImplicitJoin struct {
	// TODO: Add sources.
}

// InnerJoin represents a join between two series.
type InnerJoin struct {
	// TODO: Add sources.
	Condition Expr
}

// MergeJoin represents a merging join between two series.
type MergeJoin struct {
	// TODO: Add sources.
	Condition Expr
}

// VarRef represents a reference to a variable in the query.
type VarRef struct {
	Val string
}

// Call represents a function call.
type Call struct {
	Name string
	Expr Expr
}

// IntegerLiteral represents an integer literal in the query.
type IntegerLiteral struct {
	Val int64
}

// FloatLiteral represents a floating-point literal in the query.
type FloatLiteral struct {
	Val float64
}

// BooleanLiteral represents a boolean literal in the query.
type BooleanLiteral struct {
	Val bool
}

// StringLiteral represents a string literal in the query.
type StringLiteral struct {
	Val string
}

// TimeLiteral represents a point-in-time literal in the query.
type TimeLiteral struct {
	Val time.Time
}

// DurationLiteral represents a duration literal in the query.
type DurationLiteral struct {
	Val time.Duration
}

// BinaryExpr represents an operation between two expressions.
type BinaryExpr struct {
	Op  Token
	LHS Expr
	RHS Expr
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
	case *SelectQuery:
		if n != nil {
			Walk(v, n.Fields)
			Walk(v, n.Dimensions)
			Walk(v, n.Source)
			Walk(v, n.Condition)
		}

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

	case *Call:
		Walk(v, n.Expr)
	}
}

// WalkFunc traverses a node hierarchy in depth-first order.
func WalkFunc(node Node, fn func(Node)) {
	Walk(walkFuncVisitor(fn), node)
}

type walkFuncVisitor func(Node)

func (fn walkFuncVisitor) Visit(n Node) Visitor { fn(n); return fn }
