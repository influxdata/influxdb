package influxql_test

import (
	"strings"
	"testing"

	"github.com/influxdb/influxdb/influxql"
)

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

		// 4. Join with complete condition
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
