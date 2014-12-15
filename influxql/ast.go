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
	Unknown  = DataType("")
	Number   = DataType("number")
	Boolean  = DataType("boolean")
	String   = DataType("string")
	Time     = DataType("time")
	Duration = DataType("duration")
)

// InspectDataType returns the data type of a given value.
func InspectDataType(v interface{}) DataType {
	switch v.(type) {
	case float64:
		return Number
	case bool:
		return Boolean
	case string:
		return String
	case time.Time:
		return Time
	case time.Duration:
		return Duration
	default:
		return Unknown
	}
}

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
func (_ SortFields) node()    {}
func (_ *SortField) node()    {}

// Query represents a collection of ordered statements.
type Query struct {
	Statements Statements
}

// String returns a string representation of the query.
func (q *Query) String() string { return q.Statements.String() }

// Statements represents a list of statements.
type Statements []Statement

// String returns a string representation of the statements.
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

// SortField represens a field to sort results by.
type SortField struct {
	// Name of the field
	Name string

	// Sort order.
	Ascending bool
}

// String returns a string representation of a sort field
func (field *SortField) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString(field.Name)
	_, _ = buf.WriteString(" ")
	_, _ = buf.WriteString(strconv.FormatBool(field.Ascending))
	return buf.String()
}

// SortFields represents an ordered list of ORDER BY fields
type SortFields []*SortField

// String returns a string representation of sort fields
func (a SortFields) String() string {
	fields := make([]string, 0, len(a))
	for _, field := range a {
		fields = append(fields, field.String())
	}
	return strings.Join(fields, ", ")
}

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

	// Fields to sort results by
	SortFields SortFields

	// Maximum number of rows to be returned.
	// Unlimited if zero.
	Limit int
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
	if len(s.SortFields) > 0 {
		_, _ = buf.WriteString(" ORDER BY ")
		_, _ = buf.WriteString(s.SortFields.String())
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

/*

BinaryExpr

SELECT mean(xxx.value) + avg(yyy.value) FROM xxx JOIN yyy WHERE xxx.host = 123

from xxx where host = 123
select avg(value) from yyy where host = 123

SELECT xxx.value FROM xxx WHERE xxx.host = 123
SELECT yyy.value FROM yyy

---

SELECT MEAN(xxx.value) + MEAN(cpu.load.value)
FROM xxx JOIN yyy
GROUP BY host
WHERE (xxx.region == "uswest" OR yyy.region == "uswest") AND xxx.otherfield == "XXX"

select * from (
	select mean + mean from xxx join yyy
	group by time(5m), host
) (xxx.region == "uswest" OR yyy.region == "uswest") AND xxx.otherfield == "XXX"

(seriesIDS for xxx.region = 'uswest' union seriesIDs for yyy.regnion = 'uswest') | seriesIDS xxx.otherfield = 'XXX'

WHERE xxx.region == "uswest" AND xxx.otherfield == "XXX"
WHERE yyy.region == "uswest"


*/

// Substatement returns a single-series statement for a given variable reference.
func (s *SelectStatement) Substatement(ref *VarRef) (*SelectStatement, error) {
	// Copy dimensions and properties to new statement.
	other := &SelectStatement{
		Fields:     Fields{{Expr: ref}},
		Dimensions: s.Dimensions,
		Limit:      s.Limit,
		SortFields:    s.SortFields,
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
type ListSeriesStatement struct{
	// An expression evaluated on a series name or tag.
	Condition Expr

	// Maximum number of rows to be returned.
	// Unlimited if zero.
	Limit int

	// Fields to sort results by
	SortFields SortFields
}

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
func (l *TimeLiteral) String() string {
	return `"` + l.Val.UTC().Format("2006-01-02 15:04:05.999999") + `"`
}

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

// Fold performs constant folding on an expression.
// The function, "now()", is expanded into the current time during folding.
func Fold(expr Expr, now *time.Time) Expr {
	switch expr := expr.(type) {
	case *Call:
		// Replace "now()" with current time.
		if strings.ToLower(expr.Name) == "now" && now != nil {
			return &TimeLiteral{Val: *now}
		}

		// Fold call arguments.
		for i, arg := range expr.Args {
			expr.Args[i] = Fold(arg, now)
		}
		return expr

	case *BinaryExpr:
		// Fold and evaluate binary expression.
		return foldBinaryExpr(expr, now)

	case *ParenExpr:
		// Fold inside expression.
		// Return inside expression if not a binary expression.
		expr.Expr = Fold(expr.Expr, now)
		if _, ok := expr.Expr.(*BinaryExpr); !ok {
			return expr.Expr
		}
		return expr

	default:
		return expr
	}
}

// foldBinaryExpr performs constant folding if the binary expression has two literals.
func foldBinaryExpr(expr *BinaryExpr, now *time.Time) Expr {
	// Fold both sides of binary expression first.
	expr.LHS = Fold(expr.LHS, now)
	expr.RHS = Fold(expr.RHS, now)

	// Evaluate operations if both sides are the same type.
	switch lhs := expr.LHS.(type) {
	case *NumberLiteral:
		if _, ok := expr.RHS.(*NumberLiteral); ok {
			return foldNumberLiterals(expr)
		}
	case *BooleanLiteral:
		if _, ok := expr.RHS.(*BooleanLiteral); ok {
			return foldBooleanLiterals(expr)
		}
	case *TimeLiteral:
		switch expr.RHS.(type) {
		case *TimeLiteral:
			return foldTimeLiterals(expr)
		case *DurationLiteral:
			return foldTimeDurationLiterals(expr)
		}
	case *DurationLiteral:
		switch rhs := expr.RHS.(type) {
		case *DurationLiteral:
			return foldDurationLiterals(expr)
		case *NumberLiteral:
			return foldDurationNumberLiterals(expr)
		case *TimeLiteral:
			if expr.Op == ADD {
				return &TimeLiteral{Val: rhs.Val.Add(lhs.Val)}
			}
		}
	case *StringLiteral:
		if rhs, ok := expr.RHS.(*StringLiteral); ok && expr.Op == ADD {
			return &StringLiteral{Val: lhs.Val + rhs.Val}
		}
	}

	return expr
}

// foldNumberLiterals performs constant folding on two number literals.
func foldNumberLiterals(expr *BinaryExpr) Expr {
	lhs := expr.LHS.(*NumberLiteral)
	rhs := expr.RHS.(*NumberLiteral)

	switch expr.Op {
	case ADD:
		return &NumberLiteral{Val: lhs.Val + rhs.Val}
	case SUB:
		return &NumberLiteral{Val: lhs.Val - rhs.Val}
	case MUL:
		return &NumberLiteral{Val: lhs.Val * rhs.Val}
	case DIV:
		if rhs.Val == 0 {
			return &NumberLiteral{Val: 0}
		}
		return &NumberLiteral{Val: lhs.Val / rhs.Val}
	case EQ:
		return &BooleanLiteral{Val: lhs.Val == rhs.Val}
	case NEQ:
		return &BooleanLiteral{Val: lhs.Val != rhs.Val}
	case GT:
		return &BooleanLiteral{Val: lhs.Val > rhs.Val}
	case GTE:
		return &BooleanLiteral{Val: lhs.Val >= rhs.Val}
	case LT:
		return &BooleanLiteral{Val: lhs.Val < rhs.Val}
	case LTE:
		return &BooleanLiteral{Val: lhs.Val <= rhs.Val}
	default:
		return expr
	}
}

// foldBooleanLiterals performs constant folding on two boolean literals.
func foldBooleanLiterals(expr *BinaryExpr) Expr {
	lhs := expr.LHS.(*BooleanLiteral)
	rhs := expr.RHS.(*BooleanLiteral)

	switch expr.Op {
	case EQ:
		return &BooleanLiteral{Val: lhs.Val == rhs.Val}
	case NEQ:
		return &BooleanLiteral{Val: lhs.Val != rhs.Val}
	case AND:
		return &BooleanLiteral{Val: lhs.Val && rhs.Val}
	case OR:
		return &BooleanLiteral{Val: lhs.Val || rhs.Val}
	default:
		return expr
	}
}

// foldTimeLiterals performs constant folding on two time literals.
func foldTimeLiterals(expr *BinaryExpr) Expr {
	lhs := expr.LHS.(*TimeLiteral)
	rhs := expr.RHS.(*TimeLiteral)

	switch expr.Op {
	case SUB:
		return &DurationLiteral{Val: lhs.Val.Sub(rhs.Val)}
	case EQ:
		return &BooleanLiteral{Val: lhs.Val.Equal(rhs.Val)}
	case NEQ:
		return &BooleanLiteral{Val: !lhs.Val.Equal(rhs.Val)}
	case GT:
		return &BooleanLiteral{Val: lhs.Val.After(rhs.Val)}
	case GTE:
		return &BooleanLiteral{Val: lhs.Val.After(rhs.Val) || lhs.Val.Equal(rhs.Val)}
	case LT:
		return &BooleanLiteral{Val: lhs.Val.Before(rhs.Val)}
	case LTE:
		return &BooleanLiteral{Val: lhs.Val.Before(rhs.Val) || lhs.Val.Equal(rhs.Val)}
	default:
		return expr
	}
}

// foldTimeDurationLiterals performs constant folding on a time and duration literal.
func foldTimeDurationLiterals(expr *BinaryExpr) Expr {
	lhs := expr.LHS.(*TimeLiteral)
	rhs := expr.RHS.(*DurationLiteral)

	switch expr.Op {
	case ADD:
		return &TimeLiteral{Val: lhs.Val.Add(rhs.Val)}
	case SUB:
		return &TimeLiteral{Val: lhs.Val.Add(-rhs.Val)}
	default:
		return expr
	}
}

// foldDurationLiterals performs constant folding on two duration literals.
func foldDurationLiterals(expr *BinaryExpr) Expr {
	lhs := expr.LHS.(*DurationLiteral)
	rhs := expr.RHS.(*DurationLiteral)

	switch expr.Op {
	case ADD:
		return &DurationLiteral{Val: lhs.Val + rhs.Val}
	case SUB:
		return &DurationLiteral{Val: lhs.Val - rhs.Val}
	case EQ:
		return &BooleanLiteral{Val: lhs.Val == rhs.Val}
	case NEQ:
		return &BooleanLiteral{Val: lhs.Val != rhs.Val}
	case GT:
		return &BooleanLiteral{Val: lhs.Val > rhs.Val}
	case GTE:
		return &BooleanLiteral{Val: lhs.Val >= rhs.Val}
	case LT:
		return &BooleanLiteral{Val: lhs.Val < rhs.Val}
	case LTE:
		return &BooleanLiteral{Val: lhs.Val <= rhs.Val}
	default:
		return expr
	}
}

// foldDurationNumberLiterals performs constant folding on duration and number literal.
func foldDurationNumberLiterals(expr *BinaryExpr) Expr {
	lhs := expr.LHS.(*DurationLiteral)
	rhs := expr.RHS.(*NumberLiteral)

	switch expr.Op {
	case MUL:
		return &DurationLiteral{Val: lhs.Val * time.Duration(rhs.Val)}
	case DIV:
		if rhs.Val == 0 {
			return &DurationLiteral{Val: 0}
		}
		return &DurationLiteral{Val: lhs.Val / time.Duration(rhs.Val)}
	default:
		return expr
	}
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
