package check_test

import (
	"testing"

	"github.com/andreyvit/diff"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/parser"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/notification/check"
)

func TestCheck_Valid(t *testing.T) {
	type args struct {
		custom *check.Custom
	}
	type wants struct {
		err    error
		script string
	}

	validQuery := `package main
import "influxdata/influxdb/monitor"
import "influxdata/influxdb/v1"

data = from(bucket: "_tasks")
|> range(start: -1m)
|> filter(fn: (r) =>
				(r._measurement == "runs"))
|> filter(fn: (r) =>
				(r._field == "finishedAt"))
|> aggregateWindow(every: 1m, fn: mean, createEmpty: false)

option task = {name: "moo", every: 1m, offset: 0s}

check = {
		_check_id: "000000000000000a",
		_check_name: "moo",
		_type: "custom",
		tags: {a: "b", c: "d"},
}
warn = (r) =>
		(r.finishedAt > 20)
crit = (r) =>
		(r.finishedAt > 20)
info = (r) =>
		(r.finishedAt > 20)
messageFn = (r) =>
		("Check: ${r._check_name} is: ${r._level}")

data
		|> v1.fieldsAsCols()
		|> monitor.check(
						data: check,
						messageFn: messageFn,
						warn: warn,
						crit: crit,
						info: info,
		)`

	validQuery = ast.Format(parser.ParseSource(validQuery))

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name: "valid flux script is valid and unchanged",
			args: args{
				custom: &check.Custom{
					ID:   10,
					Name: "moo",
					Query: influxdb.DashboardQuery{
						Text: validQuery,
					},
				},
			},
			wants: wants{
				err:    nil,
				script: validQuery,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			err := tt.args.custom.Valid()
			if exp, got := tt.wants.err, err; exp != got {
				t.Errorf("expected:\n%v\n\ngot:\n%v\n", exp, got)
			}
			if exp, got := tt.wants.script, tt.args.custom.Query.Text; exp != got {
				t.Errorf("\n\nStrings do not match:\n\n%s", diff.LineDiff(exp, got))
			}

		})
	}

}
