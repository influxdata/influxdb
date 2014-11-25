package influxql_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/influxdb/influxdb/influxql"
)

// Ensure the parser can parse a multi-statement query.
func TestParser_ParseQuery(t *testing.T) {
	s := `SELECT a FROM b; SELECT c FROM d`
	q, err := influxql.NewParser(strings.NewReader(s)).ParseQuery()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if len(q.Statements) != 2 {
		t.Fatalf("unexpected statement count: %d", len(q.Statements))
	}
}

// Ensure the parser can parse an empty query.
func TestParser_ParseQuery_Empty(t *testing.T) {
	q, err := influxql.NewParser(strings.NewReader(``)).ParseQuery()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if len(q.Statements) != 0 {
		t.Fatalf("unexpected statement count: %d", len(q.Statements))
	}
}

// Ensure the parser can return an error from an malformed statement.
func TestParser_ParseQuery_ParseError(t *testing.T) {
	_, err := influxql.NewParser(strings.NewReader(`SELECT`)).ParseQuery()
	if err == nil || err.Error() != `found EOF, expected identifier, string, number, bool at line 1, char 8` {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure the parser can parse strings into Statement ASTs.
func TestParser_ParseStatement(t *testing.T) {
	var tests = []struct {
		s    string
		stmt influxql.Statement
		err  string
	}{
		// SELECT statement
		{
			s: `SELECT field1, field2 ,field3 AS field_x FROM myseries WHERE host = 'hosta.influxdb.org' GROUP BY 10h LIMIT 20 ORDER BY ASC;`,
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

		// SELECT statement (lowercase)
		{
			s: `select my_field from myseries`,
			stmt: &influxql.SelectStatement{
				Fields: influxql.Fields{&influxql.Field{Expr: &influxql.VarRef{Val: "my_field"}}},
				Source: &influxql.Series{Name: "myseries"},
			},
		},

		// DELETE statement
		{
			s: `DELETE FROM myseries WHERE host = 'hosta.influxdb.org'`,
			stmt: &influxql.DeleteStatement{
				Source: &influxql.Series{Name: "myseries"},
				Condition: &influxql.BinaryExpr{
					Op:  influxql.EQ,
					LHS: &influxql.VarRef{Val: "host"},
					RHS: &influxql.StringLiteral{Val: "hosta.influxdb.org"},
				},
			},
		},

		// LIST SERIES statement
		{
			s:    `LIST SERIES`,
			stmt: &influxql.ListSeriesStatement{},
		},

		// DROP SERIES statement
		{
			s:    `DROP SERIES myseries`,
			stmt: &influxql.DropSeriesStatement{Name: "myseries"},
		},

		// LIST CONTINUOUS QUERIES statement
		{
			s:    `LIST CONTINUOUS QUERIES`,
			stmt: &influxql.ListContinuousQueriesStatement{},
		},

		// CREATE CONTINUOUS QUERY statement
		{
			s: `CREATE CONTINUOUS QUERY myquery AS SELECT count() FROM myseries INTO foo`,
			stmt: &influxql.CreateContinuousQueryStatement{
				Name: "myquery",
				Source: &influxql.SelectStatement{
					Fields: influxql.Fields{&influxql.Field{Expr: &influxql.Call{Name: "count"}}},
					Source: &influxql.Series{Name: "myseries"},
				},
				Target: "foo",
			},
		},

		// DROP CONTINUOUS QUERY statement
		{
			s:    `DROP CONTINUOUS QUERY myquery`,
			stmt: &influxql.DropContinuousQueryStatement{Name: "myquery"},
		},

		// Errors
		{s: ``, err: `found EOF, expected SELECT at line 1, char 1`},
		{s: `SELECT`, err: `found EOF, expected identifier, string, number, bool at line 1, char 8`},
		{s: `blah blah`, err: `found blah, expected SELECT at line 1, char 1`},
		{s: `SELECT field X`, err: `found X, expected FROM at line 1, char 14`},
		{s: `SELECT field FROM "series" WHERE X +;`, err: `found ;, expected identifier, string, number, bool at line 1, char 37`},
		{s: `SELECT field FROM myseries GROUP`, err: `found EOF, expected BY at line 1, char 34`},
		{s: `SELECT field FROM myseries LIMIT`, err: `found EOF, expected number at line 1, char 34`},
		{s: `SELECT field FROM myseries LIMIT 10.5`, err: `fractional parts not allowed in limit at line 1, char 34`},
		{s: `SELECT field FROM myseries ORDER`, err: `found EOF, expected BY at line 1, char 34`},
		{s: `SELECT field FROM myseries ORDER BY /`, err: `found /, expected ASC, DESC at line 1, char 37`},
		{s: `SELECT field AS`, err: `found EOF, expected identifier, string at line 1, char 17`},
		{s: `SELECT field FROM 12`, err: `found 12, expected identifier, string at line 1, char 19`},
		{s: `SELECT field FROM myseries GROUP BY *`, err: `found *, expected identifier, string, number, bool at line 1, char 37`},
		{s: `SELECT 1000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000 FROM myseries`, err: `unable to parse number at line 1, char 8`},
		{s: `SELECT 10.5h FROM myseries`, err: `found h, expected FROM at line 1, char 12`},
		{s: `DELETE`, err: `found EOF, expected FROM at line 1, char 8`},
		{s: `DELETE FROM`, err: `found EOF, expected identifier, string at line 1, char 13`},
		{s: `DELETE FROM myseries WHERE`, err: `found EOF, expected identifier, string, number, bool at line 1, char 28`},
		{s: `DROP SERIES`, err: `found EOF, expected identifier, string at line 1, char 13`},
		{s: `LIST CONTINUOUS`, err: `found EOF, expected QUERIES at line 1, char 17`},
		{s: `LIST FOO`, err: `found FOO, expected SERIES, CONTINUOUS at line 1, char 6`},
		{s: `DROP CONTINUOUS`, err: `found EOF, expected QUERY at line 1, char 17`},
		{s: `DROP CONTINUOUS QUERY`, err: `found EOF, expected identifier, string at line 1, char 23`},
		{s: `DROP FOO`, err: `found FOO, expected SERIES, CONTINUOUS at line 1, char 6`},
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

		// 8. Binary expression with LHS paren group.
		{
			s: `(1 + 2) * 3`,
			expr: &influxql.BinaryExpr{
				Op: influxql.MUL,
				LHS: &influxql.ParenExpr{
					Expr: &influxql.BinaryExpr{
						Op:  influxql.ADD,
						LHS: &influxql.NumberLiteral{Val: 1},
						RHS: &influxql.NumberLiteral{Val: 2},
					},
				},
				RHS: &influxql.NumberLiteral{Val: 3},
			},
		},

		// 9. Complex binary expression.
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

		// 10. Function call (empty)
		{
			s: `my_func()`,
			expr: &influxql.Call{
				Name: "my_func",
			},
		},

		// 11. Function call (multi-arg)
		{
			s: `my_func(1, 2 + 3)`,
			expr: &influxql.Call{
				Name: "my_func",
				Args: []influxql.Expr{
					&influxql.NumberLiteral{Val: 1},
					&influxql.BinaryExpr{
						Op:  influxql.ADD,
						LHS: &influxql.NumberLiteral{Val: 2},
						RHS: &influxql.NumberLiteral{Val: 3},
					},
				},
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
		{s: `10µ`, d: 10 * time.Microsecond},
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

func BenchmarkParserParseStatement(b *testing.B) {
	b.ReportAllocs()
	s := `SELECT field FROM "series" WHERE value > 10`
	for i := 0; i < b.N; i++ {
		if stmt, err := influxql.NewParser(strings.NewReader(s)).ParseStatement(); err != nil {
			b.Fatalf("unexpected error: %s", err)
		} else if stmt == nil {
			b.Fatalf("expected statement", stmt)
		}
	}
	b.SetBytes(int64(len(s)))
}

// errstring converts an error to its string representation.
func errstring(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}
