package influxql_test

import (
	"strings"
	"testing"

	"github.com/influxdb/influxdb/influxql"
)

// Ensure a value's data type can be retrieved.
func TestInspectDataType(t *testing.T) {
	for i, tt := range []struct {
		v   interface{}
		typ influxql.DataType
	}{
		{float64(100), influxql.Number},
	} {
		if typ := influxql.InspectDataType(tt.v); tt.typ != typ {
			t.Errorf("%d. %v (%s): unexpected type: %s", i, tt.v, tt.typ, typ)
			continue
		}
	}
}

// Ensure the SELECT statement can extract substatements.
func TestSelectStatement_Substatement(t *testing.T) {
	var tests = []struct {
		stmt string
		expr *influxql.VarRef
		sub  string
		err  string
	}{
		// 0. Single series
		{
			stmt: `SELECT value FROM myseries WHERE value > 1`,
			expr: &influxql.VarRef{Val: "value"},
			sub:  `SELECT value FROM myseries WHERE value > 1.000`,
		},

		// 1. Simple join
		{
			stmt: `SELECT sum(aa.value) + sum(bb.value) FROM aa JOIN bb`,
			expr: &influxql.VarRef{Val: "aa.value"},
			sub:  `SELECT aa.value FROM aa`,
		},

		// 2. Simple merge
		{
			stmt: `SELECT sum(aa.value) + sum(bb.value) FROM aa MERGE bb`,
			expr: &influxql.VarRef{Val: "bb.value"},
			sub:  `SELECT bb.value FROM bb`,
		},

		// 3. Join with condition
		{
			stmt: `SELECT sum(aa.value) + sum(bb.value) FROM aa JOIN bb WHERE aa.host = "servera" AND bb.host = "serverb"`,
			expr: &influxql.VarRef{Val: "bb.value"},
			sub:  `SELECT bb.value FROM bb WHERE bb.host = "serverb"`,
		},

		// 4. Join with complex condition
		{
			stmt: `SELECT sum(aa.value) + sum(bb.value) FROM aa JOIN bb WHERE aa.host = "servera" AND (bb.host = "serverb" OR bb.host = "serverc") AND 1 = 2`,
			expr: &influxql.VarRef{Val: "bb.value"},
			sub:  `SELECT bb.value FROM bb WHERE (bb.host = "serverb" OR bb.host = "serverc") AND 1.000 = 2.000`,
		},
	}

	for i, tt := range tests {
		// Parse statement.
		stmt, err := influxql.NewParser(strings.NewReader(tt.stmt)).ParseStatement()
		if err != nil {
			t.Fatalf("invalid statement: %q: %s", tt.stmt, err)
		}

		// Extract substatement.
		sub, err := stmt.(*influxql.SelectStatement).Substatement(tt.expr)
		if err != nil {
			t.Errorf("%d. %q: unexpected error: %s", i, tt.stmt, err)
			continue
		}
		if substr := sub.String(); tt.sub != substr {
			t.Errorf("%d. %q: unexpected substatement:\n\nexp=%s\n\ngot=%s\n\n", i, tt.stmt, tt.sub, substr)
			continue
		}
	}
}

// Ensure an expression can be folded.
func TestFold(t *testing.T) {
	for i, tt := range []struct {
		in  string
		out string
	}{
		// Number literals.
		{`1 + 2`, `3.000`},
		{`(foo*2) + (4/2) + (3 * 5) - 0.5`, `(foo * 2.000) + 16.500`},
		{`foo(bar(2 + 3), 4)`, `foo(bar(5.000), 4.000)`},
		{`4 / 0`, `0.000`},
		{`4 = 4`, `true`},
		{`4 != 4`, `false`},
		{`6 > 4`, `true`},
		{`4 >= 4`, `true`},
		{`4 < 6`, `true`},
		{`4 <= 4`, `true`},
		{`4 AND 5`, `4.000 AND 5.000`},

		// Boolean literals.
		{`true AND false`, `false`},
		{`true OR false`, `true`},
		{`true = false`, `false`},
		{`true != false`, `true`},
		{`true + false`, `true + false`},

		// Time literals.
		{`now() + 2h`, `"2000-01-01 02:00:00"`},
		{`now() / 2h`, `"2000-01-01 00:00:00" / 2h`},
		{`4µ + now()`, `"2000-01-01 00:00:00.000004"`},
		{`now() = now()`, `true`},
		{`now() != now()`, `false`},
		{`now() < now() + 1h`, `true`},
		{`now() <= now() + 1h`, `true`},
		{`now() >= now() - 1h`, `true`},
		{`now() > now() - 1h`, `true`},
		{`now() - (now() - 60s)`, `1m`},
		{`now() AND now()`, `"2000-01-01 00:00:00" AND "2000-01-01 00:00:00"`},

		// Duration literals.
		{`10m + 1h - 60s`, `69m`},
		{`(10m / 2) * 5`, `25m`},
		{`60s = 1m`, `true`},
		{`60s != 1m`, `false`},
		{`60s < 1h`, `true`},
		{`60s <= 1h`, `true`},
		{`60s > 12s`, `true`},
		{`60s >= 1m`, `true`},
		{`60s AND 1m`, `1m AND 1m`},
		{`60m / 0`, `0s`},
		{`60m + 50`, `1h + 50.000`},

		// String literals.
		{`"foo" + 'bar'`, `"foobar"`},
	} {
		// Parse incoming expression.
		expr, err := influxql.NewParser(strings.NewReader(tt.in)).ParseExpr()
		if err != nil {
			t.Errorf("%d. %s: parse error: %s", i, tt.in, err)
			continue
		}

		// Fold expression.
		expr = influxql.Fold(expr, mustParseTime("2000-01-01T00:00:00Z"))

		// Compare with expected output.
		if out := expr.String(); tt.out != out {
			t.Errorf("%d. %s: unexpected expr:\n\nexp=%s\n\ngot=%s\n\n", i, tt.in, tt.out, out)
			continue
		}
	}
}
