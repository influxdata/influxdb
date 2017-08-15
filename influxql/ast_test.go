package influxql_test

import (
	"fmt"
	"go/importer"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/query"
)

func BenchmarkQuery_String(b *testing.B) {
	p := influxql.NewParser(strings.NewReader(`SELECT foo AS zoo, a AS b FROM bar WHERE value > 10 AND q = 'hello'`))
	q, _ := p.ParseStatement()
	for i := 0; i < b.N; i++ {
		_ = q.String()
	}
}

// Ensure a value's data type can be retrieved.
func TestInspectDataType(t *testing.T) {
	for i, tt := range []struct {
		v   interface{}
		typ influxql.DataType
	}{
		{float64(100), influxql.Float},
		{int64(100), influxql.Integer},
		{int32(100), influxql.Integer},
		{100, influxql.Integer},
		{true, influxql.Boolean},
		{"string", influxql.String},
		{time.Now(), influxql.Time},
		{time.Second, influxql.Duration},
		{nil, influxql.Unknown},
	} {
		if typ := influxql.InspectDataType(tt.v); tt.typ != typ {
			t.Errorf("%d. %v (%s): unexpected type: %s", i, tt.v, tt.typ, typ)
			continue
		}
	}
}

func TestDataType_String(t *testing.T) {
	for i, tt := range []struct {
		typ influxql.DataType
		v   string
	}{
		{influxql.Float, "float"},
		{influxql.Integer, "integer"},
		{influxql.Boolean, "boolean"},
		{influxql.String, "string"},
		{influxql.Time, "time"},
		{influxql.Duration, "duration"},
		{influxql.Tag, "tag"},
		{influxql.Unknown, "unknown"},
	} {
		if v := tt.typ.String(); tt.v != v {
			t.Errorf("%d. %v (%s): unexpected string: %s", i, tt.typ, tt.v, v)
		}
	}
}

func TestDataType_LessThan(t *testing.T) {
	for i, tt := range []struct {
		typ   influxql.DataType
		other influxql.DataType
		exp   bool
	}{
		{typ: influxql.Unknown, other: influxql.Unknown, exp: true},
		{typ: influxql.Unknown, other: influxql.Float, exp: true},
		{typ: influxql.Unknown, other: influxql.Integer, exp: true},
		{typ: influxql.Unknown, other: influxql.String, exp: true},
		{typ: influxql.Unknown, other: influxql.Boolean, exp: true},
		{typ: influxql.Unknown, other: influxql.Tag, exp: true},
		{typ: influxql.Float, other: influxql.Unknown, exp: false},
		{typ: influxql.Integer, other: influxql.Unknown, exp: false},
		{typ: influxql.String, other: influxql.Unknown, exp: false},
		{typ: influxql.Boolean, other: influxql.Unknown, exp: false},
		{typ: influxql.Tag, other: influxql.Unknown, exp: false},
		{typ: influxql.Float, other: influxql.Float, exp: false},
		{typ: influxql.Float, other: influxql.Integer, exp: false},
		{typ: influxql.Float, other: influxql.String, exp: false},
		{typ: influxql.Float, other: influxql.Boolean, exp: false},
		{typ: influxql.Float, other: influxql.Tag, exp: false},
		{typ: influxql.Integer, other: influxql.Float, exp: true},
		{typ: influxql.Integer, other: influxql.Integer, exp: false},
		{typ: influxql.Integer, other: influxql.String, exp: false},
		{typ: influxql.Integer, other: influxql.Boolean, exp: false},
		{typ: influxql.Integer, other: influxql.Tag, exp: false},
		{typ: influxql.String, other: influxql.Float, exp: true},
		{typ: influxql.String, other: influxql.Integer, exp: true},
		{typ: influxql.String, other: influxql.String, exp: false},
		{typ: influxql.String, other: influxql.Boolean, exp: false},
		{typ: influxql.String, other: influxql.Tag, exp: false},
		{typ: influxql.Boolean, other: influxql.Float, exp: true},
		{typ: influxql.Boolean, other: influxql.Integer, exp: true},
		{typ: influxql.Boolean, other: influxql.String, exp: true},
		{typ: influxql.Boolean, other: influxql.Boolean, exp: false},
		{typ: influxql.Boolean, other: influxql.Tag, exp: false},
		{typ: influxql.Tag, other: influxql.Float, exp: true},
		{typ: influxql.Tag, other: influxql.Integer, exp: true},
		{typ: influxql.Tag, other: influxql.String, exp: true},
		{typ: influxql.Tag, other: influxql.Boolean, exp: true},
		{typ: influxql.Tag, other: influxql.Tag, exp: false},
	} {
		if got, exp := tt.typ.LessThan(tt.other), tt.exp; got != exp {
			t.Errorf("%d. %q.LessThan(%q) = %v; exp = %v", i, tt.typ, tt.other, got, exp)
		}
	}
}

// Ensure the SELECT statement can extract GROUP BY interval.
func TestSelectStatement_GroupByInterval(t *testing.T) {
	q := "SELECT sum(value) from foo  where time < now() GROUP BY time(10m)"
	stmt, err := influxql.NewParser(strings.NewReader(q)).ParseStatement()
	if err != nil {
		t.Fatalf("invalid statement: %q: %s", stmt, err)
	}

	s := stmt.(*influxql.SelectStatement)
	d, err := s.GroupByInterval()
	if d != 10*time.Minute {
		t.Fatalf("group by interval not equal:\nexp=%s\ngot=%s", 10*time.Minute, d)
	}
	if err != nil {
		t.Fatalf("error parsing group by interval: %s", err.Error())
	}
}

// Ensure the SELECT statement can have its start and end time set
func TestSelectStatement_SetTimeRange(t *testing.T) {
	q := "SELECT sum(value) from foo where time < now() GROUP BY time(10m)"
	stmt, err := influxql.NewParser(strings.NewReader(q)).ParseStatement()
	if err != nil {
		t.Fatalf("invalid statement: %q: %s", stmt, err)
	}

	s := stmt.(*influxql.SelectStatement)
	start := time.Now().Add(-20 * time.Hour).Round(time.Second).UTC()
	end := time.Now().Add(10 * time.Hour).Round(time.Second).UTC()
	s.SetTimeRange(start, end)
	min, max := MustTimeRange(s.Condition)

	if min != start {
		t.Fatalf("start time wasn't set properly.\n  exp: %s\n  got: %s", start, min)
	}
	// the end range is actually one nanosecond before the given one since end is exclusive
	end = end.Add(-time.Nanosecond)
	if max != end {
		t.Fatalf("end time wasn't set properly.\n  exp: %s\n  got: %s", end, max)
	}

	// ensure we can set a time on a select that already has one set
	start = time.Now().Add(-20 * time.Hour).Round(time.Second).UTC()
	end = time.Now().Add(10 * time.Hour).Round(time.Second).UTC()
	q = fmt.Sprintf("SELECT sum(value) from foo WHERE time >= %ds and time <= %ds GROUP BY time(10m)", start.Unix(), end.Unix())
	stmt, err = influxql.NewParser(strings.NewReader(q)).ParseStatement()
	if err != nil {
		t.Fatalf("invalid statement: %q: %s", stmt, err)
	}

	s = stmt.(*influxql.SelectStatement)
	min, max = MustTimeRange(s.Condition)
	if start != min || end != max {
		t.Fatalf("start and end times weren't equal:\n  exp: %s\n  got: %s\n  exp: %s\n  got:%s\n", start, min, end, max)
	}

	// update and ensure it saves it
	start = time.Now().Add(-40 * time.Hour).Round(time.Second).UTC()
	end = time.Now().Add(20 * time.Hour).Round(time.Second).UTC()
	s.SetTimeRange(start, end)
	min, max = MustTimeRange(s.Condition)

	// TODO: right now the SetTimeRange can't override the start time if it's more recent than what they're trying to set it to.
	//       shouldn't matter for our purposes with continuous queries, but fix this later

	if min != start {
		t.Fatalf("start time wasn't set properly.\n  exp: %s\n  got: %s", start, min)
	}
	// the end range is actually one nanosecond before the given one since end is exclusive
	end = end.Add(-time.Nanosecond)
	if max != end {
		t.Fatalf("end time wasn't set properly.\n  exp: %s\n  got: %s", end, max)
	}

	// ensure that when we set a time range other where clause conditions are still there
	q = "SELECT sum(value) from foo WHERE foo = 'bar' and time < now() GROUP BY time(10m)"
	stmt, err = influxql.NewParser(strings.NewReader(q)).ParseStatement()
	if err != nil {
		t.Fatalf("invalid statement: %q: %s", stmt, err)
	}

	s = stmt.(*influxql.SelectStatement)

	// update and ensure it saves it
	start = time.Now().Add(-40 * time.Hour).Round(time.Second).UTC()
	end = time.Now().Add(20 * time.Hour).Round(time.Second).UTC()
	s.SetTimeRange(start, end)
	min, max = MustTimeRange(s.Condition)

	if min != start {
		t.Fatalf("start time wasn't set properly.\n  exp: %s\n  got: %s", start, min)
	}
	// the end range is actually one nanosecond before the given one since end is exclusive
	end = end.Add(-time.Nanosecond)
	if max != end {
		t.Fatalf("end time wasn't set properly.\n  exp: %s\n  got: %s", end, max)
	}

	// ensure the where clause is there
	hasWhere := false
	influxql.WalkFunc(s.Condition, func(n influxql.Node) {
		if ex, ok := n.(*influxql.BinaryExpr); ok {
			if lhs, ok := ex.LHS.(*influxql.VarRef); ok {
				if lhs.Val == "foo" {
					if rhs, ok := ex.RHS.(*influxql.StringLiteral); ok {
						if rhs.Val == "bar" {
							hasWhere = true
						}
					}
				}
			}
		}
	})
	if !hasWhere {
		t.Fatal("set time range cleared out the where clause")
	}
}

// Test SELECT statement regex conditions rewrite.
func TestSelectStatement_RewriteRegexConditions(t *testing.T) {
	var tests = []struct {
		in  string
		out string
	}{
		{in: `SELECT value FROM cpu`, out: `SELECT value FROM cpu`},
		{in: `SELECT value FROM cpu WHERE host='server-1'`, out: `SELECT value FROM cpu WHERE host='server-1'`},
		{in: `SELECT value FROM cpu WHERE host = 'server-1'`, out: `SELECT value FROM cpu WHERE host = 'server-1'`},
		{in: `SELECT value FROM cpu WHERE host != 'server-1'`, out: `SELECT value FROM cpu WHERE host != 'server-1'`},

		// Non matching regex
		{in: `SELECT value FROM cpu WHERE host =~ /server-1|server-2|server-3/`, out: `SELECT value FROM cpu WHERE host =~ /server-1|server-2|server-3/`},
		{in: `SELECT value FROM cpu WHERE host =~ /server-1/`, out: `SELECT value FROM cpu WHERE host =~ /server-1/`},
		{in: `SELECT value FROM cpu WHERE host !~ /server-1/`, out: `SELECT value FROM cpu WHERE host !~ /server-1/`},
		{in: `SELECT value FROM cpu WHERE host =~ /^server-1/`, out: `SELECT value FROM cpu WHERE host =~ /^server-1/`},
		{in: `SELECT value FROM cpu WHERE host =~ /server-1$/`, out: `SELECT value FROM cpu WHERE host =~ /server-1$/`},
		{in: `SELECT value FROM cpu WHERE host !~ /\^server-1$/`, out: `SELECT value FROM cpu WHERE host !~ /\^server-1$/`},
		{in: `SELECT value FROM cpu WHERE host !~ /\^$/`, out: `SELECT value FROM cpu WHERE host !~ /\^$/`},
		{in: `SELECT value FROM cpu WHERE host !~ /^server-1\$/`, out: `SELECT value FROM cpu WHERE host !~ /^server-1\$/`},
		{in: `SELECT value FROM cpu WHERE host =~ /^\$/`, out: `SELECT value FROM cpu WHERE host =~ /^\$/`},
		{in: `SELECT value FROM cpu WHERE host !~ /^a/`, out: `SELECT value FROM cpu WHERE host !~ /^a/`},

		// These regexes are not supported due to the presence of escaped or meta characters.
		{in: `SELECT value FROM cpu WHERE host !~ /^(foo|bar)$/`, out: `SELECT value FROM cpu WHERE host !~ /^(foo|bar)$/`},
		{in: `SELECT value FROM cpu WHERE host !~ /^?a$/`, out: `SELECT value FROM cpu WHERE host !~ /^?a$/`},
		{in: `SELECT value FROM cpu WHERE host !~ /^[a-z]$/`, out: `SELECT value FROM cpu WHERE host !~ /^[a-z]$/`},
		{in: `SELECT value FROM cpu WHERE host !~ /^\d$/`, out: `SELECT value FROM cpu WHERE host !~ /^\d$/`},
		{in: `SELECT value FROM cpu WHERE host !~ /^a*$/`, out: `SELECT value FROM cpu WHERE host !~ /^a*$/`},
		{in: `SELECT value FROM cpu WHERE host !~ /^a.b$/`, out: `SELECT value FROM cpu WHERE host !~ /^a.b$/`},
		{in: `SELECT value FROM cpu WHERE host !~ /^ab+$/`, out: `SELECT value FROM cpu WHERE host !~ /^ab+$/`},
		{in: `SELECT value FROM cpu WHERE host =~ /^hello\world$/`, out: `SELECT value FROM cpu WHERE host =~ /^hello\world$/`},

		// These regexes all match and will be rewritten.
		{in: `SELECT value FROM cpu WHERE host !~ /^a[2]$/`, out: `SELECT value FROM cpu WHERE host != 'a2'`},
		{in: `SELECT value FROM cpu WHERE host =~ /^server-1$/`, out: `SELECT value FROM cpu WHERE host = 'server-1'`},
		{in: `SELECT value FROM cpu WHERE host !~ /^server-1$/`, out: `SELECT value FROM cpu WHERE host != 'server-1'`},
		{in: `SELECT value FROM cpu WHERE host =~ /^server 1$/`, out: `SELECT value FROM cpu WHERE host = 'server 1'`},
		{in: `SELECT value FROM cpu WHERE host =~ /^$/`, out: `SELECT value FROM cpu WHERE host = ''`},
		{in: `SELECT value FROM cpu WHERE host !~ /^$/`, out: `SELECT value FROM cpu WHERE host != ''`},
		{in: `SELECT value FROM cpu WHERE host =~ /^server-1$/ OR host =~ /^server-2$/`, out: `SELECT value FROM cpu WHERE host = 'server-1' OR host = 'server-2'`},
		{in: `SELECT value FROM cpu WHERE host =~ /^server-1$/ OR host =~ /^server]a$/`, out: `SELECT value FROM cpu WHERE host = 'server-1' OR host = 'server]a'`},
		{in: `SELECT value FROM cpu WHERE host =~ /^hello\?$/`, out: `SELECT value FROM cpu WHERE host = 'hello?'`},
		{in: `SELECT value FROM cpu WHERE host !~ /^\\$/`, out: `SELECT value FROM cpu WHERE host != '\\'`},
		{in: `SELECT value FROM cpu WHERE host !~ /^\\\$$/`, out: `SELECT value FROM cpu WHERE host != '\\$'`},
	}

	for i, test := range tests {
		stmt, err := influxql.NewParser(strings.NewReader(test.in)).ParseStatement()
		if err != nil {
			t.Fatalf("[Example %d], %v", i, err)
		}

		// Rewrite any supported regex conditions.
		stmt.(*influxql.SelectStatement).RewriteRegexConditions()

		// Get the expected rewritten statement.
		expStmt, err := influxql.NewParser(strings.NewReader(test.out)).ParseStatement()
		if err != nil {
			t.Fatalf("[Example %d], %v", i, err)
		}

		// Compare the (potentially) rewritten AST to the expected AST.
		if got, exp := stmt, expStmt; !reflect.DeepEqual(got, exp) {
			t.Errorf("[Example %d]\nattempting %v\ngot %v\n%s\n\nexpected %v\n%s\n", i+1, test.in, got, mustMarshalJSON(got), exp, mustMarshalJSON(exp))
		}
	}
}

// Ensure that the IsRawQuery flag gets set properly
func TestSelectStatement_IsRawQuerySet(t *testing.T) {
	var tests = []struct {
		stmt  string
		isRaw bool
	}{
		{
			stmt:  "select * from foo",
			isRaw: true,
		},
		{
			stmt:  "select value1,value2 from foo",
			isRaw: true,
		},
		{
			stmt:  "select value1,value2 from foo, time(10m)",
			isRaw: true,
		},
		{
			stmt:  "select mean(value) from foo where time < now() group by time(5m)",
			isRaw: false,
		},
		{
			stmt:  "select mean(value) from foo group by bar",
			isRaw: false,
		},
		{
			stmt:  "select mean(value) from foo group by *",
			isRaw: false,
		},
		{
			stmt:  "select mean(value) from foo group by *",
			isRaw: false,
		},
	}

	for _, tt := range tests {
		s := MustParseSelectStatement(tt.stmt)
		if s.IsRawQuery != tt.isRaw {
			t.Errorf("'%s', IsRawQuery should be %v", tt.stmt, tt.isRaw)
		}
	}
}

// Ensure binary expression names can be evaluated.
func TestBinaryExprName(t *testing.T) {
	for i, tt := range []struct {
		expr string
		name string
	}{
		{expr: `value + 1`, name: `value`},
		{expr: `"user" / total`, name: `user_total`},
		{expr: `("user" + total) / total`, name: `user_total_total`},
	} {
		expr := influxql.MustParseExpr(tt.expr)
		switch expr := expr.(type) {
		case *influxql.BinaryExpr:
			name := influxql.BinaryExprName(expr)
			if name != tt.name {
				t.Errorf("%d. unexpected name %s, got %s", i, name, tt.name)
			}
		default:
			t.Errorf("%d. unexpected expr type: %T", i, expr)
		}
	}
}

// Ensure that we see if a where clause has only time limitations
func TestOnlyTimeExpr(t *testing.T) {
	var tests = []struct {
		stmt string
		exp  bool
	}{
		{
			stmt: `SELECT value FROM myseries WHERE value > 1`,
			exp:  false,
		},
		{
			stmt: `SELECT value FROM foo WHERE time >= '2000-01-01T00:00:05Z'`,
			exp:  true,
		},
		{
			stmt: `SELECT value FROM foo WHERE time >= '2000-01-01T00:00:05Z' AND time < '2000-01-01T00:00:05Z'`,
			exp:  true,
		},
		{
			stmt: `SELECT value FROM foo WHERE time >= '2000-01-01T00:00:05Z' AND asdf = 'bar'`,
			exp:  false,
		},
		{
			stmt: `SELECT value FROM foo WHERE asdf = 'jkl' AND (time >= '2000-01-01T00:00:05Z' AND time < '2000-01-01T00:00:05Z')`,
			exp:  false,
		},
	}

	for i, tt := range tests {
		// Parse statement.
		stmt, err := influxql.NewParser(strings.NewReader(tt.stmt)).ParseStatement()
		if err != nil {
			t.Fatalf("invalid statement: %q: %s", tt.stmt, err)
		}
		if influxql.OnlyTimeExpr(stmt.(*influxql.SelectStatement).Condition) != tt.exp {
			t.Fatalf("%d. expected statement to return only time dimension to be %t: %s", i, tt.exp, tt.stmt)
		}
	}
}

// Ensure an AST node can be rewritten.
func TestRewrite(t *testing.T) {
	expr := MustParseExpr(`time > 1 OR foo = 2`)

	// Flip LHS & RHS in all binary expressions.
	act := influxql.RewriteFunc(expr, func(n influxql.Node) influxql.Node {
		switch n := n.(type) {
		case *influxql.BinaryExpr:
			return &influxql.BinaryExpr{Op: n.Op, LHS: n.RHS, RHS: n.LHS}
		default:
			return n
		}
	})

	// Verify that everything is flipped.
	if act := act.String(); act != `2 = foo OR 1 > time` {
		t.Fatalf("unexpected result: %s", act)
	}
}

// Ensure an Expr can be rewritten handling nils.
func TestRewriteExpr(t *testing.T) {
	expr := MustParseExpr(`(time > 1 AND time < 10) OR foo = 2`)

	// Remove all time expressions.
	act := influxql.RewriteExpr(expr, func(e influxql.Expr) influxql.Expr {
		switch e := e.(type) {
		case *influxql.BinaryExpr:
			if lhs, ok := e.LHS.(*influxql.VarRef); ok && lhs.Val == "time" {
				return nil
			}
		}
		return e
	})

	// Verify that everything is flipped.
	if act := act.String(); act != `foo = 2` {
		t.Fatalf("unexpected result: %s", act)
	}
}

// Ensure that the String() value of a statement is parseable
func TestParseString(t *testing.T) {
	var tests = []struct {
		stmt string
	}{
		{
			stmt: `SELECT "cpu load" FROM myseries`,
		},
		{
			stmt: `SELECT "cpu load" FROM "my series"`,
		},
		{
			stmt: `SELECT "cpu\"load" FROM myseries`,
		},
		{
			stmt: `SELECT "cpu'load" FROM myseries`,
		},
		{
			stmt: `SELECT "cpu load" FROM "my\"series"`,
		},
		{
			stmt: `SELECT "field with spaces" FROM "\"ugly\" db"."\"ugly\" rp"."\"ugly\" measurement"`,
		},
		{
			stmt: `SELECT * FROM myseries`,
		},
		{
			stmt: `DROP DATABASE "!"`,
		},
		{
			stmt: `DROP RETENTION POLICY "my rp" ON "a database"`,
		},
		{
			stmt: `CREATE RETENTION POLICY "my rp" ON "a database" DURATION 1d REPLICATION 1`,
		},
		{
			stmt: `ALTER RETENTION POLICY "my rp" ON "a database" DEFAULT`,
		},
		{
			stmt: `SHOW RETENTION POLICIES ON "a database"`,
		},
		{
			stmt: `SHOW TAG VALUES WITH KEY IN ("a long name", short)`,
		},
		{
			stmt: `DROP CONTINUOUS QUERY "my query" ON "my database"`,
		},
		// See issues https://github.com/influxdata/influxdb/issues/1647
		// and https://github.com/influxdata/influxdb/issues/4404
		//{
		//	stmt: `DELETE FROM "my db"."my rp"."my measurement"`,
		//},
		{
			stmt: `DROP SUBSCRIPTION "ugly \"subscription\" name" ON "\"my\" db"."\"my\" rp"`,
		},
		{
			stmt: `CREATE SUBSCRIPTION "ugly \"subscription\" name" ON "\"my\" db"."\"my\" rp" DESTINATIONS ALL 'my host', 'my other host'`,
		},
		{
			stmt: `SHOW MEASUREMENTS WITH MEASUREMENT =~ /foo/`,
		},
		{
			stmt: `SHOW MEASUREMENTS WITH MEASUREMENT = "and/or"`,
		},
		{
			stmt: `DROP USER "user with spaces"`,
		},
		{
			stmt: `GRANT ALL PRIVILEGES ON "db with spaces" TO "user with spaces"`,
		},
		{
			stmt: `GRANT ALL PRIVILEGES TO "user with spaces"`,
		},
		{
			stmt: `SHOW GRANTS FOR "user with spaces"`,
		},
		{
			stmt: `REVOKE ALL PRIVILEGES ON "db with spaces" FROM "user with spaces"`,
		},
		{
			stmt: `REVOKE ALL PRIVILEGES FROM "user with spaces"`,
		},
		{
			stmt: `CREATE DATABASE "db with spaces"`,
		},
	}

	for _, tt := range tests {
		// Parse statement.
		stmt, err := influxql.NewParser(strings.NewReader(tt.stmt)).ParseStatement()
		if err != nil {
			t.Fatalf("invalid statement: %q: %s", tt.stmt, err)
		}

		stmtCopy, err := influxql.NewParser(strings.NewReader(stmt.String())).ParseStatement()
		if err != nil {
			t.Fatalf("failed to parse string: %v\norig: %v\ngot: %v", err, tt.stmt, stmt.String())
		}

		if !reflect.DeepEqual(stmt, stmtCopy) {
			t.Fatalf("statement changed after stringifying and re-parsing:\noriginal : %v\nre-parsed: %v\n", tt.stmt, stmtCopy.String())
		}
	}
}

// Ensure an expression can be reduced.
func TestEval(t *testing.T) {
	for i, tt := range []struct {
		in   string
		out  interface{}
		data map[string]interface{}
	}{
		// Number literals.
		{in: `1 + 2`, out: int64(3)},
		{in: `(foo*2) + ( (4/2) + (3 * 5) - 0.5 )`, out: float64(26.5), data: map[string]interface{}{"foo": float64(5)}},
		{in: `foo / 2`, out: float64(2), data: map[string]interface{}{"foo": float64(4)}},
		{in: `4 = 4`, out: true},
		{in: `4 <> 4`, out: false},
		{in: `6 > 4`, out: true},
		{in: `4 >= 4`, out: true},
		{in: `4 < 6`, out: true},
		{in: `4 <= 4`, out: true},
		{in: `4 AND 5`, out: nil},
		{in: `0 = 'test'`, out: false},
		{in: `1.0 = 1`, out: true},
		{in: `1.2 = 1`, out: false},

		// Boolean literals.
		{in: `true AND false`, out: false},
		{in: `true OR false`, out: true},
		{in: `false = 4`, out: false},

		// String literals.
		{in: `'foo' = 'bar'`, out: false},
		{in: `'foo' = 'foo'`, out: true},
		{in: `'' = 4`, out: nil},

		// Regex literals.
		{in: `'foo' =~ /f.*/`, out: true},
		{in: `'foo' =~ /b.*/`, out: false},
		{in: `'foo' !~ /f.*/`, out: false},
		{in: `'foo' !~ /b.*/`, out: true},

		// Variable references.
		{in: `foo`, out: "bar", data: map[string]interface{}{"foo": "bar"}},
		{in: `foo = 'bar'`, out: true, data: map[string]interface{}{"foo": "bar"}},
		{in: `foo = 'bar'`, out: nil, data: map[string]interface{}{"foo": nil}},
		{in: `'bar' = foo`, out: nil, data: map[string]interface{}{"foo": nil}},
		{in: `foo <> 'bar'`, out: true, data: map[string]interface{}{"foo": "xxx"}},
		{in: `foo =~ /b.*/`, out: true, data: map[string]interface{}{"foo": "bar"}},
		{in: `foo !~ /b.*/`, out: false, data: map[string]interface{}{"foo": "bar"}},
		{in: `foo > 2 OR bar > 3`, out: true, data: map[string]interface{}{"foo": float64(4)}},
		{in: `foo > 2 OR bar > 3`, out: true, data: map[string]interface{}{"bar": float64(4)}},
	} {
		// Evaluate expression.
		out := influxql.Eval(MustParseExpr(tt.in), tt.data)

		// Compare with expected output.
		if !reflect.DeepEqual(tt.out, out) {
			t.Errorf("%d. %s: unexpected output:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.in, tt.out, out)
			continue
		}
	}
}

// Ensure an expression can be reduced.
func TestReduce(t *testing.T) {
	now := mustParseTime("2000-01-01T00:00:00Z")

	for i, tt := range []struct {
		in   string
		out  string
		data Valuer
	}{
		// Number literals.
		{in: `1 + 2`, out: `3`},
		{in: `(foo*2) + ( (4/2) + (3 * 5) - 0.5 )`, out: `(foo * 2) + 16.500`},
		{in: `foo(bar(2 + 3), 4)`, out: `foo(bar(5), 4)`},
		{in: `4 / 0`, out: `0.000`},
		{in: `1 / 2`, out: `0.500`},
		{in: `2 % 3`, out: `2`},
		{in: `5 % 2`, out: `1`},
		{in: `2 % 0`, out: `0`},
		{in: `2.5 % 0`, out: `NaN`},
		{in: `254 & 3`, out: `2`},
		{in: `254 | 3`, out: `255`},
		{in: `254 ^ 3`, out: `253`},
		{in: `-3 & 3`, out: `1`},
		{in: `8 & -3`, out: `8`},
		{in: `8.5 & -3`, out: `8.500 & -3`},
		{in: `4 = 4`, out: `true`},
		{in: `4 <> 4`, out: `false`},
		{in: `6 > 4`, out: `true`},
		{in: `4 >= 4`, out: `true`},
		{in: `4 < 6`, out: `true`},
		{in: `4 <= 4`, out: `true`},
		{in: `4 AND 5`, out: `4 AND 5`},

		// Boolean literals.
		{in: `true AND false`, out: `false`},
		{in: `true OR false`, out: `true`},
		{in: `true OR (foo = bar AND 1 > 2)`, out: `true`},
		{in: `(foo = bar AND 1 > 2) OR true`, out: `true`},
		{in: `false OR (foo = bar AND 1 > 2)`, out: `false`},
		{in: `(foo = bar AND 1 > 2) OR false`, out: `false`},
		{in: `true = false`, out: `false`},
		{in: `true <> false`, out: `true`},
		{in: `true + false`, out: `true + false`},

		// Time literals with now().
		{in: `now() + 2h`, out: `'2000-01-01T02:00:00Z'`, data: map[string]interface{}{"now()": now}},
		{in: `now() / 2h`, out: `'2000-01-01T00:00:00Z' / 2h`, data: map[string]interface{}{"now()": now}},
		{in: `4µ + now()`, out: `'2000-01-01T00:00:00.000004Z'`, data: map[string]interface{}{"now()": now}},
		{in: `now() + 2000000000`, out: `'2000-01-01T00:00:02Z'`, data: map[string]interface{}{"now()": now}},
		{in: `2000000000 + now()`, out: `'2000-01-01T00:00:02Z'`, data: map[string]interface{}{"now()": now}},
		{in: `now() - 2000000000`, out: `'1999-12-31T23:59:58Z'`, data: map[string]interface{}{"now()": now}},
		{in: `now() = now()`, out: `true`, data: map[string]interface{}{"now()": now}},
		{in: `now() <> now()`, out: `false`, data: map[string]interface{}{"now()": now}},
		{in: `now() < now() + 1h`, out: `true`, data: map[string]interface{}{"now()": now}},
		{in: `now() <= now() + 1h`, out: `true`, data: map[string]interface{}{"now()": now}},
		{in: `now() >= now() - 1h`, out: `true`, data: map[string]interface{}{"now()": now}},
		{in: `now() > now() - 1h`, out: `true`, data: map[string]interface{}{"now()": now}},
		{in: `now() - (now() - 60s)`, out: `1m`, data: map[string]interface{}{"now()": now}},
		{in: `now() AND now()`, out: `'2000-01-01T00:00:00Z' AND '2000-01-01T00:00:00Z'`, data: map[string]interface{}{"now()": now}},
		{in: `now()`, out: `now()`},
		{in: `946684800000000000 + 2h`, out: `'2000-01-01T02:00:00Z'`},

		// Time literals.
		{in: `'2000-01-01T00:00:00Z' + 2h`, out: `'2000-01-01T02:00:00Z'`},
		{in: `'2000-01-01T00:00:00Z' / 2h`, out: `'2000-01-01T00:00:00Z' / 2h`},
		{in: `4µ + '2000-01-01T00:00:00Z'`, out: `'2000-01-01T00:00:00.000004Z'`},
		{in: `'2000-01-01T00:00:00Z' + 2000000000`, out: `'2000-01-01T00:00:02Z'`},
		{in: `2000000000 + '2000-01-01T00:00:00Z'`, out: `'2000-01-01T00:00:02Z'`},
		{in: `'2000-01-01T00:00:00Z' - 2000000000`, out: `'1999-12-31T23:59:58Z'`},
		{in: `'2000-01-01T00:00:00Z' = '2000-01-01T00:00:00Z'`, out: `true`},
		{in: `'2000-01-01T00:00:00.000000000Z' = '2000-01-01T00:00:00Z'`, out: `true`},
		{in: `'2000-01-01T00:00:00Z' <> '2000-01-01T00:00:00Z'`, out: `false`},
		{in: `'2000-01-01T00:00:00.000000000Z' <> '2000-01-01T00:00:00Z'`, out: `false`},
		{in: `'2000-01-01T00:00:00Z' < '2000-01-01T00:00:00Z' + 1h`, out: `true`},
		{in: `'2000-01-01T00:00:00.000000000Z' < '2000-01-01T00:00:00Z' + 1h`, out: `true`},
		{in: `'2000-01-01T00:00:00Z' <= '2000-01-01T00:00:00Z' + 1h`, out: `true`},
		{in: `'2000-01-01T00:00:00.000000000Z' <= '2000-01-01T00:00:00Z' + 1h`, out: `true`},
		{in: `'2000-01-01T00:00:00Z' > '2000-01-01T00:00:00Z' - 1h`, out: `true`},
		{in: `'2000-01-01T00:00:00.000000000Z' > '2000-01-01T00:00:00Z' - 1h`, out: `true`},
		{in: `'2000-01-01T00:00:00Z' >= '2000-01-01T00:00:00Z' - 1h`, out: `true`},
		{in: `'2000-01-01T00:00:00.000000000Z' >= '2000-01-01T00:00:00Z' - 1h`, out: `true`},
		{in: `'2000-01-01T00:00:00Z' - ('2000-01-01T00:00:00Z' - 60s)`, out: `1m`},
		{in: `'2000-01-01T00:00:00Z' AND '2000-01-01T00:00:00Z'`, out: `'2000-01-01T00:00:00Z' AND '2000-01-01T00:00:00Z'`},

		// Duration literals.
		{in: `10m + 1h - 60s`, out: `69m`},
		{in: `(10m / 2) * 5`, out: `25m`},
		{in: `60s = 1m`, out: `true`},
		{in: `60s <> 1m`, out: `false`},
		{in: `60s < 1h`, out: `true`},
		{in: `60s <= 1h`, out: `true`},
		{in: `60s > 12s`, out: `true`},
		{in: `60s >= 1m`, out: `true`},
		{in: `60s AND 1m`, out: `1m AND 1m`},
		{in: `60m / 0`, out: `0s`},
		{in: `60m + 50`, out: `1h + 50`},

		// String literals.
		{in: `'foo' + 'bar'`, out: `'foobar'`},

		// Variable references.
		{in: `foo`, out: `'bar'`, data: map[string]interface{}{"foo": "bar"}},
		{in: `foo = 'bar'`, out: `true`, data: map[string]interface{}{"foo": "bar"}},
		{in: `foo = 'bar'`, out: `false`, data: map[string]interface{}{"foo": nil}},
		{in: `foo <> 'bar'`, out: `false`, data: map[string]interface{}{"foo": nil}},
	} {
		// Fold expression.
		expr := influxql.Reduce(MustParseExpr(tt.in), tt.data)

		// Compare with expected output.
		if out := expr.String(); tt.out != out {
			t.Errorf("%d. %s: unexpected expr:\n\nexp=%s\n\ngot=%s\n\n", i, tt.in, tt.out, out)
			continue
		}
	}
}

func TestSelect_Privileges(t *testing.T) {
	stmt := &influxql.SelectStatement{
		Target: &influxql.Target{
			Measurement: &influxql.Measurement{Database: "db2"},
		},
		Sources: []influxql.Source{
			&influxql.Measurement{Database: "db0"},
			&influxql.Measurement{Database: "db1"},
		},
	}

	exp := influxql.ExecutionPrivileges{
		influxql.ExecutionPrivilege{Name: "db0", Privilege: influxql.ReadPrivilege},
		influxql.ExecutionPrivilege{Name: "db1", Privilege: influxql.ReadPrivilege},
		influxql.ExecutionPrivilege{Name: "db2", Privilege: influxql.WritePrivilege},
	}

	got, err := stmt.RequiredPrivileges()
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(exp, got) {
		t.Errorf("exp: %v, got: %v", exp, got)
	}
}

func TestSelect_SubqueryPrivileges(t *testing.T) {
	stmt := &influxql.SelectStatement{
		Target: &influxql.Target{
			Measurement: &influxql.Measurement{Database: "db2"},
		},
		Sources: []influxql.Source{
			&influxql.Measurement{Database: "db0"},
			&influxql.SubQuery{
				Statement: &influxql.SelectStatement{
					Sources: []influxql.Source{
						&influxql.Measurement{Database: "db1"},
					},
				},
			},
		},
	}

	exp := influxql.ExecutionPrivileges{
		influxql.ExecutionPrivilege{Name: "db0", Privilege: influxql.ReadPrivilege},
		influxql.ExecutionPrivilege{Name: "db1", Privilege: influxql.ReadPrivilege},
		influxql.ExecutionPrivilege{Name: "db2", Privilege: influxql.WritePrivilege},
	}

	got, err := stmt.RequiredPrivileges()
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(exp, got) {
		t.Errorf("exp: %v, got: %v", exp, got)
	}
}

func TestShow_Privileges(t *testing.T) {
	for _, c := range []struct {
		stmt influxql.Statement
		exp  influxql.ExecutionPrivileges
	}{
		{
			stmt: &influxql.ShowDatabasesStatement{},
			exp:  influxql.ExecutionPrivileges{{Admin: false, Privilege: influxql.NoPrivileges}},
		},
		{
			stmt: &influxql.ShowFieldKeysStatement{},
			exp:  influxql.ExecutionPrivileges{{Admin: false, Privilege: influxql.ReadPrivilege}},
		},
		{
			stmt: &influxql.ShowMeasurementsStatement{},
			exp:  influxql.ExecutionPrivileges{{Admin: false, Privilege: influxql.ReadPrivilege}},
		},
		{
			stmt: &influxql.ShowQueriesStatement{},
			exp:  influxql.ExecutionPrivileges{{Admin: false, Privilege: influxql.ReadPrivilege}},
		},
		{
			stmt: &influxql.ShowRetentionPoliciesStatement{},
			exp:  influxql.ExecutionPrivileges{{Admin: false, Privilege: influxql.ReadPrivilege}},
		},
		{
			stmt: &influxql.ShowSeriesStatement{},
			exp:  influxql.ExecutionPrivileges{{Admin: false, Privilege: influxql.ReadPrivilege}},
		},
		{
			stmt: &influxql.ShowShardGroupsStatement{},
			exp:  influxql.ExecutionPrivileges{{Admin: true, Privilege: influxql.AllPrivileges}},
		},
		{
			stmt: &influxql.ShowShardsStatement{},
			exp:  influxql.ExecutionPrivileges{{Admin: true, Privilege: influxql.AllPrivileges}},
		},
		{
			stmt: &influxql.ShowStatsStatement{},
			exp:  influxql.ExecutionPrivileges{{Admin: true, Privilege: influxql.AllPrivileges}},
		},
		{
			stmt: &influxql.ShowSubscriptionsStatement{},
			exp:  influxql.ExecutionPrivileges{{Admin: true, Privilege: influxql.AllPrivileges}},
		},
		{
			stmt: &influxql.ShowDiagnosticsStatement{},
			exp:  influxql.ExecutionPrivileges{{Admin: true, Privilege: influxql.AllPrivileges}},
		},
		{
			stmt: &influxql.ShowTagKeysStatement{},
			exp:  influxql.ExecutionPrivileges{{Admin: false, Privilege: influxql.ReadPrivilege}},
		},
		{
			stmt: &influxql.ShowTagValuesStatement{},
			exp:  influxql.ExecutionPrivileges{{Admin: false, Privilege: influxql.ReadPrivilege}},
		},
		{
			stmt: &influxql.ShowUsersStatement{},
			exp:  influxql.ExecutionPrivileges{{Admin: true, Privilege: influxql.AllPrivileges}},
		},
	} {
		got, err := c.stmt.RequiredPrivileges()
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(c.exp, got) {
			t.Errorf("exp: %v, got: %v", c.exp, got)
		}
	}
}

func TestSources_HasSystemSource(t *testing.T) {
	sources := influxql.Sources([]influxql.Source{
		&influxql.Measurement{
			Name: "_measurements",
		},
	})

	ok := sources.HasSystemSource()
	if !ok {
		t.Errorf("expected to find a system source, found none")
	}

	sources = influxql.Sources([]influxql.Source{
		&influxql.Measurement{
			Name: "cpu",
		},
	})

	ok = sources.HasSystemSource()
	if ok {
		t.Errorf("expected to find no system source, found one")
	}
}

// This test checks to ensure that we have given thought to the database
// context required for security checks.  If a new statement is added, this
// test will fail until it is categorized into the correct bucket below.
func Test_EnforceHasDefaultDatabase(t *testing.T) {
	pkg, err := importer.Default().Import("github.com/influxdata/influxdb/influxql")
	if err != nil {
		fmt.Printf("error: %s\n", err.Error())
		return
	}
	statements := []string{}

	// this is a list of statements that do not have a database context
	exemptStatements := []string{
		"CreateDatabaseStatement",
		"CreateUserStatement",
		"DeleteSeriesStatement",
		"DropDatabaseStatement",
		"DropMeasurementStatement",
		"DropSeriesStatement",
		"DropShardStatement",
		"DropUserStatement",
		"ExplainStatement",
		"GrantAdminStatement",
		"KillQueryStatement",
		"RevokeAdminStatement",
		"SelectStatement",
		"SetPasswordUserStatement",
		"ShowContinuousQueriesStatement",
		"ShowDatabasesStatement",
		"ShowDiagnosticsStatement",
		"ShowGrantsForUserStatement",
		"ShowQueriesStatement",
		"ShowShardGroupsStatement",
		"ShowShardsStatement",
		"ShowStatsStatement",
		"ShowSubscriptionsStatement",
		"ShowUsersStatement",
	}

	exists := func(stmt string) bool {
		switch stmt {
		// These are functions with the word statement in them, and can be ignored
		case "Statement", "MustParseStatement", "ParseStatement", "RewriteStatement":
			return true
		default:
			// check the exempt statements
			for _, s := range exemptStatements {
				if s == stmt {
					return true
				}
			}
			// check the statements that passed the interface test for HasDefaultDatabase
			for _, s := range statements {
				if s == stmt {
					return true
				}
			}
			return false
		}
	}

	needsHasDefault := []interface{}{
		&influxql.AlterRetentionPolicyStatement{},
		&influxql.CreateContinuousQueryStatement{},
		&influxql.CreateRetentionPolicyStatement{},
		&influxql.CreateSubscriptionStatement{},
		&influxql.DeleteStatement{},
		&influxql.DropContinuousQueryStatement{},
		&influxql.DropRetentionPolicyStatement{},
		&influxql.DropSubscriptionStatement{},
		&influxql.GrantStatement{},
		&influxql.RevokeStatement{},
		&influxql.ShowFieldKeysStatement{},
		&influxql.ShowMeasurementsStatement{},
		&influxql.ShowRetentionPoliciesStatement{},
		&influxql.ShowSeriesStatement{},
		&influxql.ShowTagKeysStatement{},
		&influxql.ShowTagValuesStatement{},
	}

	for _, stmt := range needsHasDefault {
		statements = append(statements, strings.TrimPrefix(fmt.Sprintf("%T", stmt), "*influxql."))
		if _, ok := stmt.(influxql.HasDefaultDatabase); !ok {
			t.Errorf("%T was expected to declare DefaultDatabase method", stmt)
		}

	}

	for _, declName := range pkg.Scope().Names() {
		if strings.HasSuffix(declName, "Statement") {
			if !exists(declName) {
				t.Errorf("unchecked statement %s.  please update this test to determine if this statement needs to declare 'DefaultDatabase'", declName)
			}
		}
	}
}

// Valuer represents a simple wrapper around a map to implement the influxql.Valuer interface.
type Valuer map[string]interface{}

// Value returns the value and existence of a key.
func (o Valuer) Value(key string) (v interface{}, ok bool) {
	v, ok = o[key]
	return
}

// MustTimeRange will parse a time range. Panic on error.
func MustTimeRange(expr influxql.Expr) (min, max time.Time) {
	_, timeRange, err := query.ParseCondition(expr, nil)
	if err != nil {
		panic(err)
	}
	return timeRange.Min, timeRange.Max
}

// mustParseTime parses an IS0-8601 string. Panic on error.
func mustParseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err.Error())
	}
	return t
}
