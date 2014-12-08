package influxql

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
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
	String() string
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
func (_ *Join) node()            {}
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

// String returns a string representation of the query.
func (q *Query) String() string { return q.Statements.String() }

// Statements represents a list of statements.
type Statements []Statement

// String returns a string representation of the statements
func (a Statements) String() string {
	var str []string
	for _, stmt := range a {
		str = append(str, stmt.String())
	}
	return strings.Join(str, ";\n")
}

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

func (_ *Series) source() {}
func (_ *Join) source()   {}
func (_ *Merge) source()  {}

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

// String returns a string representation of the select statement.
func (s *SelectStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("SELECT ")
	_, _ = buf.WriteString(s.Fields.String())
	_, _ = buf.WriteString(" FROM ")
	_, _ = buf.WriteString(s.Source.String())
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_, _ = buf.WriteString(s.Condition.String())
	}
	if len(s.Dimensions) > 0 {
		_, _ = buf.WriteString(" GROUP BY ")
		_, _ = buf.WriteString(s.Dimensions.String())
	}
	if s.Limit > 0 {
		_, _ = fmt.Fprintf(&buf, " LIMIT %d", s.Limit)
	}
	if s.Ascending {
		_, _ = buf.WriteString(" ORDER BY ASC")
	}
	return buf.String()
}

// Aggregated returns true if the statement uses aggregate functions.
func (s *SelectStatement) Aggregated() bool {
	var v bool
	WalkFunc(s.Fields, func(n Node) {
		if _, ok := n.(*Call); ok {
			v = true
		}
	})
	return v
}

// Substatement returns a single-series statement for a given variable reference.
func (s *SelectStatement) Substatement(ref *VarRef) (*SelectStatement, error) {
	// Copy dimensions and properties to new statement.
	other := &SelectStatement{
		Fields:     Fields{{Expr: ref}},
		Dimensions: s.Dimensions,
		Limit:      s.Limit,
		Ascending:  s.Ascending,
	}

	// If there is only one series source then return it with the whole condition.
	if _, ok := s.Source.(*Series); ok {
		other.Source = s.Source
		other.Condition = s.Condition
		return other, nil
	}

	// Find the matching source.
	name := MatchSource(s.Source, ref.Val)
	if name == "" {
		return nil, fmt.Errorf("field source not found: %s", ref.Val)
	}
	other.Source = &Series{Name: name}

	// Filter out conditions.
	if s.Condition != nil {
		other.Condition = filterExprBySource(name, s.Condition)
	}

	return other, nil
}

// filters an expression to exclude expressions related to a source.
func filterExprBySource(name string, expr Expr) Expr {
	switch expr := expr.(type) {
	case *VarRef:
		if !strings.HasPrefix(expr.Val, name) {
			return nil
		}

	case *BinaryExpr:
		lhs := filterExprBySource(name, expr.LHS)
		rhs := filterExprBySource(name, expr.RHS)

		// If an expr is logical then return either LHS/RHS or both.
		// If an expr is arithmetic or comparative then require both sides.
		if expr.Op == AND || expr.Op == OR {
			if lhs == nil && rhs == nil {
				return nil
			} else if lhs != nil && rhs == nil {
				return lhs
			} else if lhs == nil && rhs != nil {
				return rhs
			}
		} else {
			if lhs == nil || rhs == nil {
				return nil
			}
		}

	case *ParenExpr:
		if filterExprBySource(name, expr.Expr) == nil {
			return nil
		}
	}
	return expr
}

// MatchSource returns the source name that matches a field name.
// Returns a blank string if no sources match.
func MatchSource(src Source, name string) string {
	switch src := src.(type) {
	case *Series:
		if strings.HasPrefix(name, src.Name) {
			return src.Name
		}
	case *Join:
		if str := MatchSource(src.LHS, name); str != "" {
			return str
		}
		if str := MatchSource(src.RHS, name); str != "" {
			return str
		}
	case *Merge:
		if str := MatchSource(src.LHS, name); str != "" {
			return str
		}
		if str := MatchSource(src.RHS, name); str != "" {
			return str
		}
	}
	return ""
}

// DeleteStatement represents a command for removing data from the database.
type DeleteStatement struct {
	// Data source that values are removed from.
	Source Source

	// An expression evaluated on data point.
	Condition Expr
}

// String returns a string representation of the delete statement.
func (s *DeleteStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("DELETE ")
	_, _ = buf.WriteString(s.Source.String())
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_, _ = buf.WriteString(s.Condition.String())
	}
	return s.String()
}

// ListSeriesStatement represents a command for listing series in the database.
type ListSeriesStatement struct{}

// String returns a string representation of the list series statement.
func (s *ListSeriesStatement) String() string { return "LIST SERIES" }

// DropSeriesStatement represents a command for removing a series from the database.
type DropSeriesStatement struct {
	Name string
}

// String returns a string representation of the drop series statement.
func (s *DropSeriesStatement) String() string { return fmt.Sprintf("DROP SERIES %s", s.Name) }

// ListContinuousQueriesStatement represents a command for listing continuous queries.
type ListContinuousQueriesStatement struct{}

// String returns a string representation of the list continuous queries statement.
func (s *ListContinuousQueriesStatement) String() string { return "LIST CONTINUOUS QUERIES" }

// CreateContinuousQueriesStatement represents a command for creating a continuous query.
type CreateContinuousQueryStatement struct {
	Name   string
	Source *SelectStatement
	Target string
}

// String returns a string representation of the statement.
func (s *CreateContinuousQueryStatement) String() string {
	return fmt.Sprintf("CREATE CONTINUOUS QUERY %s AS %s INTO %s", s.Name, s.Source.String(), s.Target)
}

// DropContinuousQueriesStatement represents a command for removing a continuous query.
type DropContinuousQueryStatement struct {
	Name string
}

// String returns a string representation of the statement.
func (s *DropContinuousQueryStatement) String() string {
	return fmt.Sprintf("DROP CONTINUOUS QUERY %s", s.Name)
}

// Fields represents a list of fields.
type Fields []*Field

// String returns a string representation of the fields.
func (a Fields) String() string {
	var str []string
	for _, f := range a {
		str = append(str, f.String())
	}
	return strings.Join(str, ", ")
}

// Field represents an expression retrieved from a select statement.
type Field struct {
	Expr  Expr
	Alias string
}

// Name returns the name of the field. Returns alias, if set.
// Otherwise uses the function name or variable name.
func (f *Field) Name() string {
	// Return alias, if set.
	if f.Alias != "" {
		return f.Alias
	}

	// Return the function name or variable name, if available.
	switch expr := f.Expr.(type) {
	case *Call:
		return expr.Name
	case *VarRef:
		return expr.Val
	}

	// Otherwise return a blank name.
	return ""
}

// String returns a string representation of the field.
func (f *Field) String() string {
	if f.Alias == "" {
		return f.Expr.String()
	}
	return fmt.Sprintf("%s AS %s", f.Expr.String(), QuoteIdent(f.Alias))
}

// Dimensions represents a list of dimensions.
type Dimensions []*Dimension

// String returns a string representation of the dimensions.
func (a Dimensions) String() string {
	var str []string
	for _, d := range a {
		str = append(str, d.String())
	}
	return strings.Join(str, ", ")
}

// Dimension represents an expression that a select statement is grouped by.
type Dimension struct {
	Expr Expr
}

// String returns a string representation of the dimension.
func (d *Dimension) String() string { return d.Expr.String() }

// Series represents a single series used as a datasource.
type Series struct {
	Name string
}

// String returns a string representation of the series.
func (s *Series) String() string { return s.Name }

// Join represents two datasources joined together.
type Join struct {
	LHS Source
	RHS Source
}

// String returns a string representation of the join.
func (j *Join) String() string {
	return fmt.Sprintf("%s JOIN %s", j.LHS.String(), j.RHS.String())
}

// Merge represents a datasource created by merging two datasources.
type Merge struct {
	LHS Source
	RHS Source
}

// String returns a string representation of the merge.
func (m *Merge) String() string {
	return fmt.Sprintf("%s JOIN %s", m.LHS.String(), m.RHS.String())
}

// VarRef represents a reference to a variable.
type VarRef struct {
	Val string
}

// String returns a string representation of the variable reference.
func (r *VarRef) String() string { return QuoteIdent(r.Val) }

// Call represents a function call.
type Call struct {
	Name string
	Args []Expr
}

// String returns a string representation of the call.
func (c *Call) String() string {
	// Join arguments.
	var str []string
	for _, arg := range c.Args {
		str = append(str, arg.String())
	}

	// Write function name and args.
	return fmt.Sprintf("%s(%s)", c.Name, strings.Join(str, ", "))
}

// NumberLiteral represents a numeric literal.
type NumberLiteral struct {
	Val float64
}

// String returns a string representation of the literal.
func (l *NumberLiteral) String() string { return strconv.FormatFloat(l.Val, 'f', 3, 64) }

// BooleanLiteral represents a boolean literal.
type BooleanLiteral struct {
	Val bool
}

// String returns a string representation of the literal.
func (l *BooleanLiteral) String() string {
	if l.Val {
		return "true"
	}
	return "false"
}

// StringLiteral represents a string literal.
type StringLiteral struct {
	Val string
}

// String returns a string representation of the literal.
func (l *StringLiteral) String() string { return Quote(l.Val) }

// TimeLiteral represents a point-in-time literal.
type TimeLiteral struct {
	Val time.Time
}

// String returns a string representation of the literal.
func (l *TimeLiteral) String() string { return l.Val.UTC().Format("2006-01-02 15:04:05.999") }

// DurationLiteral represents a duration literal.
type DurationLiteral struct {
	Val time.Duration
}

// String returns a string representation of the literal.
func (l *DurationLiteral) String() string { return FormatDuration(l.Val) }

// BinaryExpr represents an operation between two expressions.
type BinaryExpr struct {
	Op  Token
	LHS Expr
	RHS Expr
}

// String returns a string representation of the binary expression.
func (e *BinaryExpr) String() string {
	return fmt.Sprintf("%s %s %s", e.LHS.String(), e.Op.String(), e.RHS.String())
}

// ParenExpr represents a parenthesized expression.
type ParenExpr struct {
	Expr Expr
}

// String returns a string representation of the parenthesized expression.
func (e *ParenExpr) String() string { return fmt.Sprintf("(%s)", e.Expr.String()) }

// Wildcard represents a wild card expression.
type Wildcard struct{}

// String returns a string representation of the wildcard.
func (e *Wildcard) String() string { return "*" }

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
