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
		// SELECT * statement
		{
			s: `SELECT * FROM myseries`,
			stmt: &influxql.SelectStatement{
				Fields: influxql.Fields{
					&influxql.Field{Expr: &influxql.Wildcard{}},
				},
				Source: &influxql.Measurement{Name: "myseries"},
			},
		},

		// SELECT statement
		{
			s: `SELECT field1, field2 ,field3 AS field_x FROM myseries WHERE host = 'hosta.influxdb.org' GROUP BY 10h ORDER BY ASC LIMIT 20;`,
			stmt: &influxql.SelectStatement{
				Fields: influxql.Fields{
					&influxql.Field{Expr: &influxql.VarRef{Val: "field1"}},
					&influxql.Field{Expr: &influxql.VarRef{Val: "field2"}},
					&influxql.Field{Expr: &influxql.VarRef{Val: "field3"}, Alias: "field_x"},
				},
				Source: &influxql.Measurement{Name: "myseries"},
				Condition: &influxql.BinaryExpr{
					Op:  influxql.EQ,
					LHS: &influxql.VarRef{Val: "host"},
					RHS: &influxql.StringLiteral{Val: "hosta.influxdb.org"},
				},
				Dimensions: influxql.Dimensions{
					&influxql.Dimension{Expr: &influxql.DurationLiteral{Val: 10 * time.Hour}},
				},
				Limit: 20,
				SortFields: influxql.SortFields{
					&influxql.SortField{Ascending: true},
				},
			},
		},

		// SELECT statement with JOIN
		{
			s: `SELECT field1 FROM join(aa,"bb", cc) JOIN cc`,
			stmt: &influxql.SelectStatement{
				Fields: influxql.Fields{&influxql.Field{Expr: &influxql.VarRef{Val: "field1"}}},
				Source: &influxql.Join{
					Measurements: influxql.Measurements{
						{Name: "aa"},
						{Name: "bb"},
						{Name: "cc"},
					},
				},
			},
		},

		// SELECT statement with MERGE
		{
			s: `SELECT field1 FROM merge(aa,b.b)`,
			stmt: &influxql.SelectStatement{
				Fields: influxql.Fields{&influxql.Field{Expr: &influxql.VarRef{Val: "field1"}}},
				Source: &influxql.Merge{
					Measurements: influxql.Measurements{
						{Name: "aa"},
						{Name: "b.b"},
					},
				},
			},
		},

		// SELECT statement (lowercase)
		{
			s: `select my_field from myseries`,
			stmt: &influxql.SelectStatement{
				Fields: influxql.Fields{&influxql.Field{Expr: &influxql.VarRef{Val: "my_field"}}},
				Source: &influxql.Measurement{Name: "myseries"},
			},
		},

		// SELECT statement with multiple ORDER BY fields
		{
			s: `SELECT field1 FROM myseries ORDER BY ASC, field1, field2 DESC LIMIT 10`,
			stmt: &influxql.SelectStatement{
				Fields: influxql.Fields{&influxql.Field{Expr: &influxql.VarRef{Val: "field1"}}},
				Source: &influxql.Measurement{Name: "myseries"},
				SortFields: influxql.SortFields{
					&influxql.SortField{Ascending: true},
					&influxql.SortField{Name: "field1"},
					&influxql.SortField{Name: "field2"},
				},
				Limit: 10,
			},
		},

		// DELETE statement
		{
			s: `DELETE FROM myseries WHERE host = 'hosta.influxdb.org'`,
			stmt: &influxql.DeleteStatement{
				Source: &influxql.Measurement{Name: "myseries"},
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

		// LIST SERIES WHERE with ORDER BY and LIMIT
		{
			s: `LIST SERIES WHERE region = 'uswest' ORDER BY ASC, field1, field2 DESC LIMIT 10`,
			stmt: &influxql.ListSeriesStatement{
				Condition: &influxql.BinaryExpr{
					Op:  influxql.EQ,
					LHS: &influxql.VarRef{Val: "region"},
					RHS: &influxql.StringLiteral{Val: "uswest"},
				},
				SortFields: influxql.SortFields{
					&influxql.SortField{Ascending: true},
					&influxql.SortField{Name: "field1"},
					&influxql.SortField{Name: "field2"},
				},
				Limit: 10,
			},
		},

		// LIST MEASUREMENTS WHERE with ORDER BY and LIMIT
		{
			s: `LIST MEASUREMENTS WHERE region = 'uswest' ORDER BY ASC, field1, field2 DESC LIMIT 10`,
			stmt: &influxql.ListMeasurementsStatement{
				Condition: &influxql.BinaryExpr{
					Op:  influxql.EQ,
					LHS: &influxql.VarRef{Val: "region"},
					RHS: &influxql.StringLiteral{Val: "uswest"},
				},
				SortFields: influxql.SortFields{
					&influxql.SortField{Ascending: true},
					&influxql.SortField{Name: "field1"},
					&influxql.SortField{Name: "field2"},
				},
				Limit: 10,
			},
		},

		// LIST TAG KEYS
		{
			s: `LIST TAG KEYS FROM src WHERE region = 'uswest' ORDER BY ASC, field1, field2 DESC LIMIT 10`,
			stmt: &influxql.ListTagKeysStatement{
				Source: &influxql.Measurement{Name: "src"},
				Condition: &influxql.BinaryExpr{
					Op:  influxql.EQ,
					LHS: &influxql.VarRef{Val: "region"},
					RHS: &influxql.StringLiteral{Val: "uswest"},
				},
				SortFields: influxql.SortFields{
					&influxql.SortField{Ascending: true},
					&influxql.SortField{Name: "field1"},
					&influxql.SortField{Name: "field2"},
				},
				Limit: 10,
			},
		},

		// LIST TAG VALUES
		{
			s: `LIST TAG VALUES FROM src WHERE region = 'uswest' ORDER BY ASC, field1, field2 DESC LIMIT 10`,
			stmt: &influxql.ListTagValuesStatement{
				Source: &influxql.Measurement{Name: "src"},
				Condition: &influxql.BinaryExpr{
					Op:  influxql.EQ,
					LHS: &influxql.VarRef{Val: "region"},
					RHS: &influxql.StringLiteral{Val: "uswest"},
				},
				SortFields: influxql.SortFields{
					&influxql.SortField{Ascending: true},
					&influxql.SortField{Name: "field1"},
					&influxql.SortField{Name: "field2"},
				},
				Limit: 10,
			},
		},

		// LIST FIELD KEYS
		{
			s: `LIST FIELD KEYS FROM src WHERE region = 'uswest' ORDER BY ASC, field1, field2 DESC LIMIT 10`,
			stmt: &influxql.ListFieldKeysStatement{
				Source: &influxql.Measurement{Name: "src"},
				Condition: &influxql.BinaryExpr{
					Op:  influxql.EQ,
					LHS: &influxql.VarRef{Val: "region"},
					RHS: &influxql.StringLiteral{Val: "uswest"},
				},
				SortFields: influxql.SortFields{
					&influxql.SortField{Ascending: true},
					&influxql.SortField{Name: "field1"},
					&influxql.SortField{Name: "field2"},
				},
				Limit: 10,
			},
		},

		// LIST FIELD VALUES
		{
			s: `LIST FIELD VALUES FROM src WHERE region = 'uswest' ORDER BY ASC, field1, field2 DESC LIMIT 10`,
			stmt: &influxql.ListFieldValuesStatement{
				Source: &influxql.Measurement{Name: "src"},
				Condition: &influxql.BinaryExpr{
					Op:  influxql.EQ,
					LHS: &influxql.VarRef{Val: "region"},
					RHS: &influxql.StringLiteral{Val: "uswest"},
				},
				SortFields: influxql.SortFields{
					&influxql.SortField{Ascending: true},
					&influxql.SortField{Name: "field1"},
					&influxql.SortField{Name: "field2"},
				},
				Limit: 10,
			},
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
					Source: &influxql.Measurement{Name: "myseries"},
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
		{s: `SELECT field1 X`, err: `found X, expected FROM at line 1, char 15`},
		{s: `SELECT field1 FROM "series" WHERE X +;`, err: `found ;, expected identifier, string, number, bool at line 1, char 38`},
		{s: `SELECT field1 FROM myseries GROUP`, err: `found EOF, expected BY at line 1, char 35`},
		{s: `SELECT field1 FROM myseries LIMIT`, err: `found EOF, expected number at line 1, char 35`},
		{s: `SELECT field1 FROM myseries LIMIT 10.5`, err: `fractional parts not allowed in limit at line 1, char 35`},
		{s: `SELECT field1 FROM myseries LIMIT 0`, err: `LIMIT must be > 0 at line 1, char 35`},
		{s: `SELECT field1 FROM myseries ORDER`, err: `found EOF, expected BY at line 1, char 35`},
		{s: `SELECT field1 FROM myseries ORDER BY /`, err: `found /, expected identifier, ASC, or DESC at line 1, char 38`},
		{s: `SELECT field1 FROM myseries ORDER BY 1`, err: `found 1, expected identifier, ASC, or DESC at line 1, char 38`},
		{s: `SELECT field1 AS`, err: `found EOF, expected identifier, string at line 1, char 18`},
		{s: `SELECT field1 FROM 12`, err: `found 12, expected identifier, string at line 1, char 20`},
		{s: `SELECT field1 FROM myseries GROUP BY *`, err: `found *, expected identifier, string, number, bool at line 1, char 38`},
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
		// Primitives
		{s: `100`, expr: &influxql.NumberLiteral{Val: 100}},
		{s: `"foo bar"`, expr: &influxql.StringLiteral{Val: "foo bar"}},
		{s: `true`, expr: &influxql.BooleanLiteral{Val: true}},
		{s: `false`, expr: &influxql.BooleanLiteral{Val: false}},
		{s: `my_ident`, expr: &influxql.VarRef{Val: "my_ident"}},
		{s: `"2000-01-01 00:00:00"`, expr: &influxql.TimeLiteral{Val: mustParseTime("2000-01-01T00:00:00Z")}},
		{s: `"2000-01-32 00:00:00"`, err: `unable to parse datetime at line 1, char 1`},
		{s: `"2000-01-01"`, expr: &influxql.TimeLiteral{Val: mustParseTime("2000-01-01T00:00:00Z")}},
		{s: `"2000-01-99"`, err: `unable to parse date at line 1, char 1`},

		// Simple binary expression
		{
			s: `1 + 2`,
			expr: &influxql.BinaryExpr{
				Op:  influxql.ADD,
				LHS: &influxql.NumberLiteral{Val: 1},
				RHS: &influxql.NumberLiteral{Val: 2},
			},
		},

		// Binary expression with LHS precedence
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

		// Binary expression with RHS precedence
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

		// Binary expression with LHS paren group.
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

		// Complex binary expression.
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

		// Function call (empty)
		{
			s: `my_func()`,
			expr: &influxql.Call{
				Name: "my_func",
			},
		},

		// Function call (multi-arg)
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

// Ensure a time duration can be formatted.
func TestFormatDuration(t *testing.T) {
	var tests = []struct {
		d time.Duration
		s string
	}{
		{d: 3 * time.Microsecond, s: `3`},
		{d: 1001 * time.Microsecond, s: `1001`},
		{d: 15 * time.Millisecond, s: `15ms`},
		{d: 100 * time.Second, s: `100s`},
		{d: 2 * time.Minute, s: `2m`},
		{d: 2 * time.Hour, s: `2h`},
		{d: 2 * 24 * time.Hour, s: `2d`},
		{d: 2 * 7 * 24 * time.Hour, s: `2w`},
	}

	for i, tt := range tests {
		s := influxql.FormatDuration(tt.d)
		if tt.s != s {
			t.Errorf("%d. %v: mismatch: %s != %s", i, tt.d, tt.s, s)
		}
	}
}

// Ensure a string can be quoted.
func TestQuote(t *testing.T) {
	for i, tt := range []struct {
		in  string
		out string
	}{
		{``, `""`},
		{`foo`, `"foo"`},
		{"foo\nbar", `"foo\nbar"`},
		{`foo bar\\`, `"foo bar\\\\"`},
		{`"foo"`, `"\"foo\""`},
	} {
		if out := influxql.Quote(tt.in); tt.out != out {
			t.Errorf("%d. %s: mismatch: %s != %s", i, tt.in, tt.out, out)
		}
	}
}

// Ensure an identifier can be quoted when appropriate.
func TestQuoteIdent(t *testing.T) {
	for i, tt := range []struct {
		ident string
		s     string
	}{
		{``, `""`},
		{`foo`, `foo`},
		{`foo.bar.baz`, `foo.bar.baz`},
		{`my var`, `"my var"`},
		{`my-var`, `"my-var"`},
		{`my_var`, `my_var`},
	} {
		if s := influxql.QuoteIdent(tt.ident); tt.s != s {
			t.Errorf("%d. %s: mismatch: %s != %s", i, tt.ident, tt.s, s)
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

// MustParseSelectStatement parses a select statement. Panic on error.
func MustParseSelectStatement(s string) *influxql.SelectStatement {
	stmt, err := influxql.NewParser(strings.NewReader(s)).ParseStatement()
	if err != nil {
		panic(err.Error())
	}
	return stmt.(*influxql.SelectStatement)
}

// MustParseExpr parses an expression. Panic on error.
func MustParseExpr(s string) influxql.Expr {
	expr, err := influxql.NewParser(strings.NewReader(s)).ParseExpr()
	if err != nil {
		panic(err.Error())
	}
	return expr
}

// errstring converts an error to its string representation.
func errstring(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}
