package tsdb

import (
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxql"
)

func TestGuard(t *testing.T) {
	tests := []struct {
		min, max int64
		names    []string
		expr     string
		point    string
		matches  bool
	}{
		{ // in time matching
			min: 0, max: 1000,
			point:   "cpu value=1 100",
			matches: true,
		},
		{ // out of time range doesn't match
			min: 0, max: 10,
			names:   []string{"cpu"},
			point:   "cpu value=1 100",
			matches: false,
		},
		{ // measurement name matches
			min: 0, max: 1000,
			names:   []string{"cpu"},
			point:   "cpu value=1 100",
			matches: true,
		},
		{ // measurement doesn't match
			min: 0, max: 1000,
			names:   []string{"mem"},
			point:   "cpu value=1 100",
			matches: false,
		},
		{ // basic expression matching
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "host = 'server1'",
			matches: true,
		},
		{ // basic expression matching
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "host != 'server2'",
			matches: true,
		},
		{ // basic expression mismatch
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "host = 'server2'",
			matches: false,
		},
		{ // basic expression mismatch
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "host != 'server1'",
			matches: false,
		},
		{ // parenthesis unwrap
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "(host = 'server1')",
			matches: true,
		},
		{ // compound expression matching
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "host = 'server2' or host = 'server1'",
			matches: true,
		},
		{ // compound expression mismatch
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "host = 'server1' and host = 'server2'",
			matches: false,
		},
		{ // regex expression matching
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "host =~ /server1/",
			matches: true,
		},
		{ // regex expression mismatch
			min: 0, max: 1000,
			point:   "cpu,foo=server1 value=1 100",
			expr:    "host =~ /server1/",
			matches: false,
		},
		{ // regex over-approximation
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "host =~ /server2/",
			matches: true,
		},
		{ // regex over-approximation
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "host !~ /server1/",
			matches: true,
		},
		{ // key doesn't have to come first
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "'server1' = host",
			matches: true,
		},
		{ // key doesn't have to come first
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "'server2' = host",
			matches: false,
		},
		{ // conservative on no var refs
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "1 = 2",
			matches: true,
		},
		{ // expr matches measurement
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "_name = 'cpu'",
			matches: true,
		},
		{ // expr mismatches measurement
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "_name = 'mem'",
			matches: false,
		},
		{ // expr conservative on dual var ref
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "host = test",
			matches: true,
		},
		{ // expr conservative on dual var ref mismatches
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "foo = bar",
			matches: false,
		},
		{ // expr conservative on dual var ref involving measurement
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "_name = host",
			matches: true,
		},
		{ // expr conservative on dual var ref involving measurement
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "host = _name",
			matches: true,
		},
		{ // boolean literal matches
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "true",
			matches: true,
		},
		{ // boolean literal mismatches
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "false",
			matches: false,
		},
		{ // reduce and
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "true and host = 'server1'",
			matches: true,
		},
		{ // reduce and
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "host = 'server1' and true",
			matches: true,
		},
		{ // reduce or
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "false or host = 'server1'",
			matches: true,
		},
		{ // reduce or
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "host = 'server1' or false",
			matches: true,
		},
		{ // short circuit and
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "false and host = 'server1'",
			matches: false,
		},
		{ // short circuit and
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "host = 'server1' and false",
			matches: false,
		},
		{ // short circuit or
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "true or host = 'server2'",
			matches: true,
		},
		{ // short circuit or
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "host = 'server2' or true",
			matches: true,
		},
		{ // conservative match weird exprs
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "'wierd'",
			matches: true,
		},
		{ // conservative match weird exprs
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "value::field = '1'",
			matches: true,
		},
		{ // conservative match weird exprs
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "host <= 'aaa'",
			matches: true,
		},
		{ // conservative match weird exprs
			min: 0, max: 1000,
			point:   "cpu,host=server1 value=1 100",
			expr:    "host = ('server2')",
			matches: true,
		},
	}

	for i, test := range tests {
		var expr influxql.Expr
		if test.expr != "" {
			var err error
			expr, err = influxql.ParseExpr(test.expr)
			if err != nil {
				t.Fatal(err)
			}
		}
		points, err := models.ParsePointsString(test.point)
		if err != nil {
			t.Fatal(err)
		}
		guard := newGuard(test.min, test.max, test.names, expr)

		if guard.Matches(points) != test.matches {
			t.Errorf("%d: expected matching %q with time:[%d, %d] measurements:%v expr:%q to be %t",
				i, test.point, test.min, test.max, test.names, test.expr, test.matches)
			cs := &spew.ConfigState{DisableMethods: true, SpewKeys: true, Indent: "  "}
			t.Errorf("%d: expr: %s", i, cs.Sdump(expr))
			t.Errorf("%d: guard: %s", i, cs.Sdump(guard.expr))
		}
	}
}

func BenchmarkGuard(b *testing.B) {
	tag := func(key, value string) models.Tag {
		return models.Tag{Key: []byte(key), Value: []byte(value)}
	}

	run := func(b *testing.B, g *guard) {
		run := func(b *testing.B, batch int) {
			points := make([]models.Point, batch)
			for i := range points {
				points[i] = models.MustNewPoint("cpu", models.Tags{
					tag("t0", "v0"), tag("t1", "v1"), tag("t2", "v2"),
					tag("t3", "v3"), tag("t4", "v4"), tag("t5", "v5"),
					tag("t6", "v6"), tag("t7", "v7"), tag("t8", "v8"),
				}, models.Fields{"value": 100}, time.Unix(0, 50))
			}

			for i := 0; i < b.N; i++ {
				if g.Matches(points) {
					b.Fatal("matched")
				}
			}
		}

		b.Run("1", func(b *testing.B) { run(b, 1) })
		b.Run("100", func(b *testing.B) { run(b, 100) })
		b.Run("10000", func(b *testing.B) { run(b, 10000) })
	}

	b.Run("Time Filtered", func(b *testing.B) {
		run(b, newGuard(0, 10, nil, nil))
	})

	b.Run("Measurement Filtered", func(b *testing.B) {
		run(b, newGuard(0, 100, []string{"mem"}, nil))
	})

	b.Run("Tag Filtered", func(b *testing.B) {
		expr, _ := influxql.ParseExpr("t4 = 'v5'")
		run(b, newGuard(0, 100, []string{"cpu"}, expr))
	})
}
