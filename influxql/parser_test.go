package influxql_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/influxdb/influxdb/influxql"
)

// Ensure the parser can parse strings into Statement ASTs.
func TestParser_ParseStatement(t *testing.T) {
	var tests = []struct {
		s    string
		stmt influxql.Statement
		err  string
	}{
		// SELECT statements
		{
			s: `SELECT field1, field2 ,field3 AS field_x FROM myseries WHERE host = 'hosta.influxdb.org' GROUP BY 10h LIMIT 20 ORDER BY ASC`,
			stmt: &influxql.SelectStatement{
				Fields: influxql.Fields{
					&influxql.Field{Expr: &influxql.VarRef{Val: "field1"}},
					&influxql.Field{Expr: &influxql.VarRef{Val: "field2"}},
					&influxql.Field{Expr: &influxql.VarRef{Val: "field3"}, Alias: "field_x"},
				},
				Source: &influxql.Series{Name: "myseries"},
				Condition: &influxql.BinaryExpr{
					Op:  influxql.EQ,
					LHS: &influxql.VarRef{Val: "host"},
					RHS: &influxql.StringLiteral{Val: "hosta.influxdb.org"},
				},
				Dimensions: influxql.Dimensions{
					&influxql.Dimension{Expr: &influxql.DurationLiteral{Val: 10 * time.Hour}},
				},
				Limit:     20,
				Ascending: true,
			},
		},

		// Errors
		{s: ``, err: `found EOF, expected SELECT at line 1, char 1`},
		{s: `SELECT`, err: `found EOF, expected identifier, string, number, bool at line 1, char 8`},
	}

	for i, tt := range tests {
		stmt, err := influxql.NewParser(strings.NewReader(tt.s)).ParseStatement()
		if !reflect.DeepEqual(tt.err, errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		} else if tt.err == "" && !reflect.DeepEqual(tt.stmt, stmt) {
			t.Errorf("%d. %q\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.s, tt.stmt, stmt)
		}
	}
}

// Ensure the parser can parse expressions into an AST.
func TestParser_ParseExpr(t *testing.T) {
	var tests = []struct {
		s    string
		expr influxql.Expr
		err  string
	}{
		// 0-4. Primitives
		{s: `100`, expr: &influxql.NumberLiteral{Val: 100}},
		{s: `"foo bar"`, expr: &influxql.StringLiteral{Val: "foo bar"}},
		{s: `true`, expr: &influxql.BooleanLiteral{Val: true}},
		{s: `false`, expr: &influxql.BooleanLiteral{Val: false}},
		{s: `my_ident`, expr: &influxql.VarRef{Val: "my_ident"}},

		// 5. Simple binary expression
		{
			s: `1 + 2`,
			expr: &influxql.BinaryExpr{
				Op:  influxql.ADD,
				LHS: &influxql.NumberLiteral{Val: 1},
				RHS: &influxql.NumberLiteral{Val: 2},
			},
		},

		// 6. Binary expression with LHS precedence
		{
			s: `1 * 2 + 3`,
			expr: &influxql.BinaryExpr{
				Op: influxql.ADD,
				LHS: &influxql.BinaryExpr{
					Op:  influxql.MUL,
					LHS: &influxql.NumberLiteral{Val: 1},
					RHS: &influxql.NumberLiteral{Val: 2},
				},
				RHS: &influxql.NumberLiteral{Val: 3},
			},
		},

		// 7. Binary expression with RHS precedence
		{
			s: `1 + 2 * 3`,
			expr: &influxql.BinaryExpr{
				Op:  influxql.ADD,
				LHS: &influxql.NumberLiteral{Val: 1},
				RHS: &influxql.BinaryExpr{
					Op:  influxql.MUL,
					LHS: &influxql.NumberLiteral{Val: 2},
					RHS: &influxql.NumberLiteral{Val: 3},
				},
			},
		},

		// 8. Complex binary expression.
		{
			s: `value + 3 < 30 AND 1 + 2 OR true`,
			expr: &influxql.BinaryExpr{
				Op: influxql.OR,
				LHS: &influxql.BinaryExpr{
					Op: influxql.AND,
					LHS: &influxql.BinaryExpr{
						Op: influxql.LT,
						LHS: &influxql.BinaryExpr{
							Op:  influxql.ADD,
							LHS: &influxql.VarRef{Val: "value"},
							RHS: &influxql.NumberLiteral{Val: 3},
						},
						RHS: &influxql.NumberLiteral{Val: 30},
					},
					RHS: &influxql.BinaryExpr{
						Op:  influxql.ADD,
						LHS: &influxql.NumberLiteral{Val: 1},
						RHS: &influxql.NumberLiteral{Val: 2},
					},
				},
				RHS: &influxql.BooleanLiteral{Val: true},
			},
		},
	}

	for i, tt := range tests {
		expr, err := influxql.NewParser(strings.NewReader(tt.s)).ParseExpr()
		if !reflect.DeepEqual(tt.err, errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		} else if tt.err == "" && !reflect.DeepEqual(tt.expr, expr) {
			t.Errorf("%d. %q\n\nexpr mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.s, tt.expr, expr)
		}
	}
}

// Ensure a time duration can be parsed.
func TestParseDuration(t *testing.T) {
	var tests = []struct {
		s   string
		d   time.Duration
		err string
	}{
		{s: `3`, d: 3 * time.Microsecond},
		{s: `1000`, d: 1000 * time.Microsecond},
		{s: `10u`, d: 10 * time.Microsecond},
		{s: `15ms`, d: 15 * time.Millisecond},
		{s: `100s`, d: 100 * time.Second},
		{s: `2m`, d: 2 * time.Minute},
		{s: `2h`, d: 2 * time.Hour},
		{s: `2d`, d: 2 * 24 * time.Hour},
		{s: `2w`, d: 2 * 7 * 24 * time.Hour},

		{s: ``, err: "invalid duration"},
		{s: `w`, err: "invalid duration"},
		{s: `1.2w`, err: "invalid duration"},
		{s: `10x`, err: "invalid duration"},
	}

	for i, tt := range tests {
		d, err := influxql.ParseDuration(tt.s)
		if !reflect.DeepEqual(tt.err, errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		} else if tt.d != d {
			t.Errorf("%d. %q\n\nduration mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.s, tt.d, d)
		}
	}
}

// errstring converts an error to its string representation.
func errstring(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}
