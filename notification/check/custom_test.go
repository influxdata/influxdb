package check_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/andreyvit/diff"
	"github.com/influxdata/flux/parser"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/notification/check"
	"github.com/influxdata/influxdb/v2/query/fluxlang"
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
|> filter(fn: (r) => r._measurement == "runs")
|> filter(fn: (r) => r._field == "finishedAt")
|> aggregateWindow(every: 1m, fn: mean, createEmpty: false)

option task = {name: "moo", every: 1m, offset: 0s}

check = {
		_check_id: "%s",
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

	invalidTaskQuery := `package main
	import "influxdata/influxdb/monitor"
	import "influxdata/influxdb/v1"

	data = from(bucket: "_tasks")
	|> range(start: -1m)
	|> filter(fn: (r) =>
					(r._measurement == "runs"))
	|> filter(fn: (r) =>
					(r._field == "finishedAt"))
	|> aggregateWindow(every: 1m, fn: mean, createEmpty: false)

	check = {
			_check_id: "%s",
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
						Text: mustFormatPackage(t, parser.ParseSource(fmt.Sprintf(validQuery, "000000000000000a"))),
					},
				},
			},
			wants: wants{
				err:    nil,
				script: mustFormatPackage(t, parser.ParseSource(fmt.Sprintf(validQuery, "000000000000000a"))),
			},
		},
		{
			name: "valid flux script is valid but check ID is replaced if wrong",
			args: args{
				custom: &check.Custom{
					ID:   10,
					Name: "moo",
					Query: influxdb.DashboardQuery{
						Text: mustFormatPackage(t, parser.ParseSource(fmt.Sprintf(validQuery, "000000000000000b"))),
					},
				},
			},
			wants: wants{
				err:    nil,
				script: mustFormatPackage(t, parser.ParseSource(fmt.Sprintf(validQuery, "000000000000000a"))),
			},
		},
		{
			name: "empty check query returns helpful error",
			args: args{
				custom: &check.Custom{
					ID:   10,
					Name: "moo",
					Query: influxdb.DashboardQuery{
						Text: "",
					},
				},
			},
			wants: wants{
				err: errors.New("Custom flux must have an object called 'check'"),
			},
		},
		{
			name: "Script missing task option receives error that says so",
			args: args{
				custom: &check.Custom{
					ID:   10,
					Name: "moo",
					Query: influxdb.DashboardQuery{
						Text: mustFormatPackage(t, parser.ParseSource(fmt.Sprintf(invalidTaskQuery, "000000000000000b"))),
					},
				},
			},
			wants: wants{
				err: errors.New("Custom flux missing task option statement"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			err := tt.args.custom.Valid(fluxlang.DefaultService)

			if exp, got := tt.wants.err, err; exp != nil && got != nil {
				// expected error, got error check that they match
				if exp.Error() != got.Error() {
					t.Errorf("expected:\n%v\n\ngot:\n%v\n", exp, got)
				}
			} else if (exp == nil || got == nil) && got != exp {
				//either exp or got are nil
				t.Errorf("expected:\n%v\n\ngot:\n%v\n", exp, got)
			} else {
				// neither errs are nil check that scripts match
				if exp, got := tt.wants.script, tt.args.custom.Query.Text; exp != got {
					t.Errorf("\n\nStrings do not match:\n\n%s", diff.LineDiff(exp, got))
				}
			}
		})
	}

}
