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

func (_ *AlterRetentionPolicyStatement) node()  {}
func (_ *CreateContinuousQueryStatement) node() {}
func (_ *CreateDatabaseStatement) node()        {}
func (_ *CreateRetentionPolicyStatement) node() {}
func (_ *CreateUserStatement) node()            {}
func (_ *DeleteStatement) node()                {}
func (_ *DropContinuousQueryStatement) node()   {}
func (_ *DropDatabaseStatement) node()          {}
func (_ *DropRetentionPolicyStatement) node()   {}
func (_ *DropSeriesStatement) node()            {}
func (_ *DropUserStatement) node()              {}
func (_ *GrantStatement) node()                 {}
func (_ *ListContinuousQueriesStatement) node() {}
func (_ *ListDatabasesStatement) node()         {}
func (_ *ListFieldKeysStatement) node()         {}
func (_ *ListFieldValuesStatement) node()       {}
func (_ *ListRetentionPoliciesStatement) node() {}
func (_ *ListMeasurementsStatement) node()      {}
func (_ *ListSeriesStatement) node()            {}
func (_ *ListTagKeysStatement) node()           {}
func (_ *ListTagValuesStatement) node()         {}
func (_ *ListUsersStatement) node()             {}
func (_ *RevokeStatement) node()                {}
func (_ *SelectStatement) node()                {}

func (_ *BinaryExpr) node()      {}
func (_ *BooleanLiteral) node()  {}
func (_ *Call) node()            {}
func (_ *Dimension) node()       {}
func (_ Dimensions) node()       {}
func (_ *DurationLiteral) node() {}
func (_ *Field) node()           {}
func (_ Fields) node()           {}
func (_ *Join) node()            {}
func (_ *Measurement) node()     {}
func (_ Measurements) node()     {}
func (_ *Merge) node()           {}
func (_ *NumberLiteral) node()   {}
func (_ *ParenExpr) node()       {}
func (_ *SortField) node()       {}
func (_ SortFields) node()       {}
func (_ *StringLiteral) node()   {}
func (_ *Target) node()          {}
func (_ *TimeLiteral) node()     {}
func (_ *VarRef) node()          {}
func (_ *Wildcard) node()        {}

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

func (_ *AlterRetentionPolicyStatement) stmt()  {}
func (_ *CreateContinuousQueryStatement) stmt() {}
func (_ *CreateDatabaseStatement) stmt()        {}
func (_ *CreateRetentionPolicyStatement) stmt() {}
func (_ *CreateUserStatement) stmt()            {}
func (_ *DeleteStatement) stmt()                {}
func (_ *DropContinuousQueryStatement) stmt()   {}
func (_ *DropDatabaseStatement) stmt()          {}
func (_ *DropRetentionPolicyStatement) stmt()   {}
func (_ *DropSeriesStatement) stmt()            {}
func (_ *DropUserStatement) stmt()              {}
func (_ *GrantStatement) stmt()                 {}
func (_ *ListContinuousQueriesStatement) stmt() {}
func (_ *ListDatabasesStatement) stmt()         {}
func (_ *ListFieldKeysStatement) stmt()         {}
func (_ *ListFieldValuesStatement) stmt()       {}
func (_ *ListMeasurementsStatement) stmt()      {}
func (_ *ListRetentionPoliciesStatement) stmt() {}
func (_ *ListSeriesStatement) stmt()            {}
func (_ *ListTagKeysStatement) stmt()           {}
func (_ *ListTagValuesStatement) stmt()         {}
func (_ *ListUsersStatement) stmt()             {}
func (_ *RevokeStatement) stmt()                {}
func (_ *SelectStatement) stmt()                {}

// Expr represents an expression that can be evaluated to a value.
type Expr interface {
	Node
	expr()
}

func (_ *BinaryExpr) expr()      {}
func (_ *BooleanLiteral) expr()  {}
func (_ *Call) expr()            {}
func (_ *DurationLiteral) expr() {}
func (_ *NumberLiteral) expr()   {}
func (_ *ParenExpr) expr()       {}
func (_ *StringLiteral) expr()   {}
func (_ *TimeLiteral) expr()     {}
func (_ *VarRef) expr()          {}
func (_ *Wildcard) expr()        {}

// Source represents a source of data for a statement.
type Source interface {
	Node
	source()
}

func (_ *Join) source()        {}
func (_ *Measurement) source() {}
func (_ *Merge) source()       {}

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

// CreateDatabaseStatement represents a command for creating a new database.
type CreateDatabaseStatement struct {
	// Name of the database to be created.
	Name string
}

// String returns a string representation of the create database statement.
func (s *CreateDatabaseStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("CREATE DATABASE ")
	_, _ = buf.WriteString(s.Name)
	return buf.String()
}

// DropDatabaseStatement represents a command to drop a database.
type DropDatabaseStatement struct {
	// Name of the database to be dropped.
	Name string
}

// String returns a string representation of the drop database statement.
func (s *DropDatabaseStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("DROP DATABASE ")
	_, _ = buf.WriteString(s.Name)
	return buf.String()
}

// DropRetentionPolicyStatement represents a command to drop a retention policy from a database.
type DropRetentionPolicyStatement struct {
	// Name of the policy to drop.
	Name string

	// Name of the database to drop the policy from.
	Database string
}

// String returns a string representation of the drop retention policy statement.
func (s *DropRetentionPolicyStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("DROP RETENTION POLICY ")
	_, _ = buf.WriteString(s.Name)
	_, _ = buf.WriteString(" ON ")
	_, _ = buf.WriteString(s.Database)
	return buf.String()
}

// CreateUserStatement represents a command for creating a new user.
type CreateUserStatement struct {
	// Name of the user to be created.
	Name string

	// User's password
	Password string

	// User's privilege level.
	Privilege *Privilege
}

// String returns a string representation of the create user statement.
func (s *CreateUserStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("CREATE USER ")
	_, _ = buf.WriteString(s.Name)
	_, _ = buf.WriteString(" WITH PASSWORD ")
	_, _ = buf.WriteString(s.Password)

	if s.Privilege != nil {
		_, _ = buf.WriteString(" WITH ")
		_, _ = buf.WriteString(s.Privilege.String())
	}

	return buf.String()
}

// DropUserStatement represents a command for dropping a user.
type DropUserStatement struct {
	// Name of the user to drop.
	Name string
}

// String returns a string representation of the drop user statement.
func (s *DropUserStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("DROP USER ")
	_, _ = buf.WriteString(s.Name)
	return buf.String()
}

// Privilege is a type of action a user can be granted the right to use.
type Privilege int

const (
	ReadPrivilege Privilege = iota
	WritePrivilege
	AllPrivileges
)

// NewPrivilege returns an initialized *Privilege.
func NewPrivilege(p Privilege) *Privilege { return &p }

// String returns a string representation of a Privilege.
func (p Privilege) String() string {
	switch p {
	case ReadPrivilege:
		return "READ"
	case WritePrivilege:
		return "WRITE"
	case AllPrivileges:
		return "ALL PRIVILEGES"
	}
	return ""
}

// GrantStatement represents a command for granting a privilege.
type GrantStatement struct {
	// The privilege to be granted.
	Privilege Privilege

	// Thing to grant privilege on (e.g., a DB).
	On string

	// Who to grant the privilege to.
	User string
}

// String returns a string representation of the grant statement.
func (s *GrantStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("GRANT ")
	_, _ = buf.WriteString(s.Privilege.String())
	if s.On != "" {
		_, _ = buf.WriteString(" ON ")
		_, _ = buf.WriteString(s.On)
	}
	_, _ = buf.WriteString(" TO ")
	_, _ = buf.WriteString(s.User)
	return buf.String()
}

// RevokeStatement represents a command to revoke a privilege from a user.
type RevokeStatement struct {
	// Privilege to be revoked.
	Privilege Privilege

	// Thing to revoke privilege to (e.g., a DB)
	On string

	// Who to revoke privilege from.
	User string
}

// String returns a string representation of the revoke statement.
func (s *RevokeStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("REVOKE ")
	_, _ = buf.WriteString(s.Privilege.String())
	if s.On != "" {
		_, _ = buf.WriteString(" ON ")
		_, _ = buf.WriteString(s.On)
	}
	_, _ = buf.WriteString(" FROM ")
	_, _ = buf.WriteString(s.User)
	return buf.String()
}

// CreateRetentionPolicyStatement represents a command to create a retention policy.
type CreateRetentionPolicyStatement struct {
	// Name of policy to create.
	Name string

	// Name of database this policy belongs to.
	Database string

	// Duration data written to this policy will be retained.
	Duration time.Duration

	// Replication factor for data written to this policy.
	Replication int

	// Should this policy be set as default for the database?
	Default bool
}

// String returns a string representation of the create retention policy.
func (s *CreateRetentionPolicyStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("CREATE RETENTION POLICY ")
	_, _ = buf.WriteString(s.Name)
	_, _ = buf.WriteString(" ON ")
	_, _ = buf.WriteString(s.Database)
	_, _ = buf.WriteString(" DURATION ")
	_, _ = buf.WriteString(FormatDuration(s.Duration))
	_, _ = buf.WriteString(" REPLICATION ")
	_, _ = buf.WriteString(strconv.Itoa(s.Replication))
	if s.Default {
		_, _ = buf.WriteString(" DEFAULT")
	}
	return buf.String()
}

// AlterRetentionPolicyStatement represents a command to alter an existing retention policy.
type AlterRetentionPolicyStatement struct {
	// Name of policy to alter.
	Name string

	// Name of the database this policy belongs to.
	Database string

	// Duration data written to this policy will be retained.
	Duration *time.Duration

	// Replication factor for data written to this policy.
	Replication *int

	// Should this policy be set as defalut for the database?
	Default bool
}

// String returns a string representation of the alter retention policy statement.
func (s *AlterRetentionPolicyStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("ALTER RETENTION POLICY ")
	_, _ = buf.WriteString(s.Name)
	_, _ = buf.WriteString(" ON ")
	_, _ = buf.WriteString(s.Database)

	if s.Duration != nil {
		_, _ = buf.WriteString(" DURATION ")
		_, _ = buf.WriteString(FormatDuration(*s.Duration))
	}

	if s.Replication != nil {
		_, _ = buf.WriteString(" REPLICATION ")
		_, _ = buf.WriteString(strconv.Itoa(*s.Replication))
	}

	if s.Default {
		_, _ = buf.WriteString(" DEFAULT")
	}

	return buf.String()
}

// SelectStatement represents a command for extracting data from the database.
type SelectStatement struct {
	// Expressions returned from the selection.
	Fields Fields

	// Target (destination) for the result of the select.
	Target *Target

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

	if s.Target != nil {
		_, _ = buf.WriteString(" ")
		_, _ = buf.WriteString(s.Target.String())
	}
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
	if len(s.SortFields) > 0 {
		_, _ = buf.WriteString(" ORDER BY ")
		_, _ = buf.WriteString(s.SortFields.String())
	}
	if s.Limit > 0 {
		_, _ = fmt.Fprintf(&buf, " LIMIT %d", s.Limit)
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
		SortFields: s.SortFields,
	}

	// If there is only one series source then return it with the whole condition.
	if _, ok := s.Source.(*Measurement); ok {
		other.Source = s.Source
		other.Condition = s.Condition
		return other, nil
	}

	// Find the matching source.
	name := MatchSource(s.Source, ref.Val)
	if name == "" {
		return nil, fmt.Errorf("field source not found: %s", ref.Val)
	}
	other.Source = &Measurement{Name: name}

	// Filter out conditions.
	if s.Condition != nil {
		other.Condition = filterExprBySource(name, s.Condition)
	}

	return other, nil
}

// filters an expression to exclude expressions unrelated to a source.
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
		return &BinaryExpr{Op: expr.Op, LHS: lhs, RHS: rhs}

	case *ParenExpr:
		exp := filterExprBySource(name, expr.Expr)
		if exp == nil {
			return nil
		}
		return &ParenExpr{Expr: exp}
	}
	return expr
}

// MatchSource returns the source name that matches a field name.
// Returns a blank string if no sources match.
func MatchSource(src Source, name string) string {
	switch src := src.(type) {
	case *Measurement:
		if strings.HasPrefix(name, src.Name) {
			return src.Name
		}
	case *Join:
		for _, m := range src.Measurements {
			if strings.HasPrefix(name, m.Name) {
				return m.Name
			}
		}
	case *Merge:
		for _, m := range src.Measurements {
			if strings.HasPrefix(name, m.Name) {
				return m.Name
			}
		}
	}
	return ""
}

// Target represents a target (destination) policy, measurment, and DB.
type Target struct {
	// Measurement to write into.
	Measurement string

	// Database to write into.
	Database string
}

// String returns a string representation of the Target.
func (t *Target) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("INTO ")
	_, _ = buf.WriteString(t.Measurement)

	if t.Database != "" {
		_, _ = buf.WriteString(" ON ")
		_, _ = buf.WriteString(t.Database)
	}

	return buf.String()
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
type ListSeriesStatement struct {
	// An expression evaluated on a series name or tag.
	Condition Expr

	// Fields to sort results by
	SortFields SortFields

	// Maximum number of rows to be returned.
	// Unlimited if zero.
	Limit int
}

// String returns a string representation of the list series statement.
func (s *ListSeriesStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("LIST SERIES")

	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_, _ = buf.WriteString(s.Condition.String())
	}
	if len(s.SortFields) > 0 {
		_, _ = buf.WriteString(" ORDER BY ")
		_, _ = buf.WriteString(s.SortFields.String())
	}
	if s.Limit > 0 {
		_, _ = buf.WriteString(" LIMIT ")
		_, _ = buf.WriteString(strconv.Itoa(s.Limit))
	}
	return buf.String()
}

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

// ListDatabasesStatement represents a command for listing all databases in the cluster.
type ListDatabasesStatement struct{}

// String returns a string representation of the list databases command.
func (s *ListDatabasesStatement) String() string { return "LIST DATABASES" }

// CreateContinuousQueriesStatement represents a command for creating a continuous query.
type CreateContinuousQueryStatement struct {
	// Name of the continuous query to be created.
	Name string

	// Name of the database to create the continuous query on.
	Database string

	// Source of data (SELECT statement).
	Source *SelectStatement
}

// String returns a string representation of the statement.
func (s *CreateContinuousQueryStatement) String() string {
	return fmt.Sprintf("CREATE CONTINUOUS QUERY %s ON %s BEGIN %s END", s.Name, s.Database, s.Source.String())
}

// DropContinuousQueriesStatement represents a command for removing a continuous query.
type DropContinuousQueryStatement struct {
	Name string
}

// String returns a string representation of the statement.
func (s *DropContinuousQueryStatement) String() string {
	return fmt.Sprintf("DROP CONTINUOUS QUERY %s", s.Name)
}

// ListMeasurementsStatement represents a command for listing measurements.
type ListMeasurementsStatement struct {
	// An expression evaluated on data point.
	Condition Expr

	// Fields to sort results by
	SortFields SortFields

	// Maximum number of rows to be returned.
	// Unlimited if zero.
	Limit int
}

// String returns a string representation of the statement.
func (s *ListMeasurementsStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("LIST MEASUREMENTS")

	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_, _ = buf.WriteString(s.Condition.String())
	}
	if len(s.SortFields) > 0 {
		_, _ = buf.WriteString(" ORDER BY ")
		_, _ = buf.WriteString(s.SortFields.String())
	}
	if s.Limit > 0 {
		_, _ = buf.WriteString(" LIMIT ")
		_, _ = buf.WriteString(strconv.Itoa(s.Limit))
	}
	return buf.String()
}

// ListRetentionPoliciesStatement represents a command for listing retention policies.
type ListRetentionPoliciesStatement struct {
	// Name of the database to list policies for.
	Database string
}

// String returns a string representation of a ListRetentionPoliciesStatement.
func (s *ListRetentionPoliciesStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("LIST RETENTION POLICIES ")
	_, _ = buf.WriteString(s.Database)
	return buf.String()
}

// ListTagKeysStatement represents a command for listing tag keys.
type ListTagKeysStatement struct {
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

// String returns a string representation of the statement.
func (s *ListTagKeysStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("LIST TAG KEYS")

	if s.Source != nil {
		_, _ = buf.WriteString(" FROM ")
		_, _ = buf.WriteString(s.Source.String())
	}
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_, _ = buf.WriteString(s.Condition.String())
	}
	if len(s.SortFields) > 0 {
		_, _ = buf.WriteString(" ORDER BY ")
		_, _ = buf.WriteString(s.SortFields.String())
	}
	if s.Limit > 0 {
		_, _ = buf.WriteString(" LIMIT ")
		_, _ = buf.WriteString(strconv.Itoa(s.Limit))
	}
	return buf.String()
}

// ListTagValuesStatement represents a command for listing tag values.
type ListTagValuesStatement struct {
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

// String returns a string representation of the statement.
func (s *ListTagValuesStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("LIST TAG VALUES")

	if s.Source != nil {
		_, _ = buf.WriteString(" FROM ")
		_, _ = buf.WriteString(s.Source.String())
	}
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_, _ = buf.WriteString(s.Condition.String())
	}
	if len(s.SortFields) > 0 {
		_, _ = buf.WriteString(" ORDER BY ")
		_, _ = buf.WriteString(s.SortFields.String())
	}
	if s.Limit > 0 {
		_, _ = buf.WriteString(" LIMIT ")
		_, _ = buf.WriteString(strconv.Itoa(s.Limit))
	}
	return buf.String()
}

// ListUsersStatement represents a command for listing users.
type ListUsersStatement struct{}

// String retuns a string representation of the ListUsersStatement.
func (s *ListUsersStatement) String() string {
	return "LIST USERS"
}

// ListFieldKeyStatement represents a command for listing field keys.
type ListFieldKeysStatement struct {
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

// String returns a string representation of the statement.
func (s *ListFieldKeysStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("LIST FIELD KEYS")

	if s.Source != nil {
		_, _ = buf.WriteString(" FROM ")
		_, _ = buf.WriteString(s.Source.String())
	}
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_, _ = buf.WriteString(s.Condition.String())
	}
	if len(s.SortFields) > 0 {
		_, _ = buf.WriteString(" ORDER BY ")
		_, _ = buf.WriteString(s.SortFields.String())
	}
	if s.Limit > 0 {
		_, _ = buf.WriteString(" LIMIT ")
		_, _ = buf.WriteString(strconv.Itoa(s.Limit))
	}
	return buf.String()
}

// ListFieldValuesStatement represents a command for listing field values.
type ListFieldValuesStatement struct {
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

// String returns a string representation of the statement.
func (s *ListFieldValuesStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("LIST FIELD VALUES")

	if s.Source != nil {
		_, _ = buf.WriteString(" FROM ")
		_, _ = buf.WriteString(s.Source.String())
	}
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_, _ = buf.WriteString(s.Condition.String())
	}
	if len(s.SortFields) > 0 {
		_, _ = buf.WriteString(" ORDER BY ")
		_, _ = buf.WriteString(s.SortFields.String())
	}
	if s.Limit > 0 {
		_, _ = buf.WriteString(" LIMIT ")
		_, _ = buf.WriteString(strconv.Itoa(s.Limit))
	}
	return buf.String()
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
	return fmt.Sprintf("%s AS %s", f.Expr.String(), f.Alias)
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

// Measurements represents a list of measurements.
type Measurements []*Measurement

// String returns a string representation of the measurements.
func (a Measurements) String() string {
	var str []string
	for _, m := range a {
		str = append(str, m.String())
	}
	return strings.Join(str, ", ")
}

// Measurement represents a single measurement used as a datasource.
type Measurement struct {
	Name string
}

// String returns a string representation of the measurement.
func (m *Measurement) String() string { return m.Name }

// Join represents two datasources joined together.
type Join struct {
	Measurements Measurements
}

// String returns a string representation of the join.
func (j *Join) String() string {
	return fmt.Sprintf("join(%s)", j.Measurements.String())
}

// Merge represents a datasource created by merging two datasources.
type Merge struct {
	Measurements Measurements
}

// String returns a string representation of the merge.
func (m *Merge) String() string {
	return fmt.Sprintf("merge(%s)", m.Measurements.String())
}

// VarRef represents a reference to a variable.
type VarRef struct {
	Val string
}

// String returns a string representation of the variable reference.
func (r *VarRef) String() string { return r.Val }

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
func (l *StringLiteral) String() string { return QuoteString(l.Val) }

// TimeLiteral represents a point-in-time literal.
type TimeLiteral struct {
	Val time.Time
}

// String returns a string representation of the literal.
func (l *TimeLiteral) String() string {
	return `"` + l.Val.UTC().Format(DateTimeFormat) + `"`
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

// TimeRange returns the minimum and maximum times specified by an expression.
// Returns zero times if there is no bound.
func TimeRange(expr Expr) (min, max time.Time) {
	WalkFunc(expr, func(n Node) {
		if n, ok := n.(*BinaryExpr); ok {
			// Extract literal expression & operator on LHS.
			// Check for "time" on the left-hand side first.
			// Otherwise check for for the right-hand side and flip the operator.
			value, op := timeExprValue(n.LHS, n.RHS), n.Op
			if value.IsZero() {
				if value = timeExprValue(n.RHS, n.LHS); value.IsZero() {
					return
				} else if op == LT {
					op = GT
				} else if op == LTE {
					op = GTE
				} else if op == GT {
					op = LT
				} else if op == GTE {
					op = LTE
				}
			}

			// Update the min/max depending on the operator.
			// The GT & LT update the value by +/- 1µs not make them "not equal".
			switch op {
			case GT:
				if min.IsZero() || value.After(min) {
					min = value.Add(time.Microsecond)
				}
			case GTE:
				if min.IsZero() || value.After(min) {
					min = value
				}
			case LT:
				if max.IsZero() || value.Before(max) {
					max = value.Add(-time.Microsecond)
				}
			case LTE:
				if max.IsZero() || value.Before(max) {
					max = value
				}
			case EQ:
				if min.IsZero() || value.After(min) {
					min = value
				}
				if max.IsZero() || value.Before(max) {
					max = value
				}
			}
		}
	})
	return
}

// timeExprValue returns the time literal value of a "time == <TimeLiteral>" expression.
// Returns zero time if the expression is not a time expression.
func timeExprValue(ref Expr, lit Expr) time.Time {
	if ref, ok := ref.(*VarRef); ok && strings.ToLower(ref.Val) == "time" {
		switch lit := lit.(type) {
		case *TimeLiteral:
			return lit.Val
		case *DurationLiteral:
			return time.Unix(0, int64(lit.Val)).UTC()
		}
	}
	return time.Time{}
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

// Rewriter can be called by Rewrite to replace nodes in the AST hierarchy.
// The Rewrite() function is called once per node.
type Rewriter interface {
	Rewrite(Node) Node
}

// Rewrite recursively invokes the rewriter to replace each node.
// Nodes are traversed depth-first and rewritten from leaf to root.
func Rewrite(r Rewriter, node Node) Node {
	switch n := node.(type) {
	case *Query:
		n.Statements = Rewrite(r, n.Statements).(Statements)

	case Statements:
		for i, s := range n {
			n[i] = Rewrite(r, s).(Statement)
		}

	case *SelectStatement:
		n.Fields = Rewrite(r, n.Fields).(Fields)
		n.Dimensions = Rewrite(r, n.Dimensions).(Dimensions)
		n.Source = Rewrite(r, n.Source).(Source)
		n.Condition = Rewrite(r, n.Condition).(Expr)

	case Fields:
		for i, f := range n {
			n[i] = Rewrite(r, f).(*Field)
		}

	case *Field:
		n.Expr = Rewrite(r, n.Expr).(Expr)

	case Dimensions:
		for i, d := range n {
			n[i] = Rewrite(r, d).(*Dimension)
		}

	case *Dimension:
		n.Expr = Rewrite(r, n.Expr).(Expr)

	case *BinaryExpr:
		n.LHS = Rewrite(r, n.LHS).(Expr)
		n.RHS = Rewrite(r, n.RHS).(Expr)

	case *ParenExpr:
		n.Expr = Rewrite(r, n.Expr).(Expr)

	case *Call:
		for i, expr := range n.Args {
			n.Args[i] = Rewrite(r, expr).(Expr)
		}
	}

	return r.Rewrite(node)
}

// RewriteFunc rewrites a node hierarchy.
func RewriteFunc(node Node, fn func(Node) Node) Node {
	return Rewrite(rewriterFunc(fn), node)
}

type rewriterFunc func(Node) Node

func (fn rewriterFunc) Rewrite(n Node) Node { return fn(n) }
