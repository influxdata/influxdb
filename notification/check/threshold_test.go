package check_test

import (
	"testing"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/notification"
	"github.com/influxdata/influxdb/notification/check"
)

func TestThreshold_GenerateFlux(t *testing.T) {
	type args struct {
		threshold check.Threshold
	}
	type wants struct {
		script string
	}

	var l float64 = 10
	var u float64 = 40

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name: "all levels",
			args: args{
				threshold: check.Threshold{
					Base: check.Base{
						ID:   10,
						Name: "moo",
						Tags: []notification.Tag{
							{Key: "aaa", Value: "vaaa"},
							{Key: "bbb", Value: "vbbb"},
						},
						Every:                 mustDuration("1h"),
						StatusMessageTemplate: "whoa! {check.yeah}",
						Query: influxdb.DashboardQuery{
							Text: `data = from(bucket: "foo") |> range(start: -1d)`,
						},
					},
					Thresholds: []check.ThresholdConfig{
						check.ThresholdConfig{
							Level:      notification.Ok,
							LowerBound: &l,
						},
						check.ThresholdConfig{
							Level:      notification.Info,
							UpperBound: &u,
						},
						check.ThresholdConfig{
							Level:      notification.Warn,
							LowerBound: &l,
							UpperBound: &u,
						},
						check.ThresholdConfig{
							Level:      notification.Critical,
							LowerBound: &l,
							UpperBound: &u,
						},
					},
				},
			},
			wants: wants{
				script: `package main
import "influxdata/influxdb/alerts"

data = from(bucket: "foo")
	|> range(start: -1d)

option task = {name: "moo", every: 1h}

check = {checkID: "000000000000000a", tags: {aaa: "vaaa", bbb: "vbbb"}}
ok = (r) =>
	(r._value > 10.0)
info = (r) =>
	(r._value < 40.0)
warn = (r) =>
	(r._value < 40.0 and r._value > 10.0)
crit = (r) =>
	(r._value < 40.0 and r._value > 10.0)
messageFn = (r, check) =>
	("whoa! {check.yeah}")

data
	|> alerts.check(
		check: check,
		messageFn: messageFn,
		ok: ok,
		info: info,
		warn: warn,
		crit: crit,
	)`,
			},
		},
		{
			name: "crit and warn",
			args: args{
				threshold: check.Threshold{
					Base: check.Base{
						ID:   10,
						Name: "moo",
						Tags: []notification.Tag{
							{Key: "aaa", Value: "vaaa"},
							{Key: "bbb", Value: "vbbb"},
						},
						Every:                 mustDuration("1h"),
						Offset:                mustDuration("10m"),
						StatusMessageTemplate: "whoa! {check.yeah}",
						Query: influxdb.DashboardQuery{
							Text: `data = from(bucket: "foo") |> range(start: -1d)`,
						},
					},
					Thresholds: []check.ThresholdConfig{
						check.ThresholdConfig{
							Level:      notification.Warn,
							UpperBound: &u,
						},
						check.ThresholdConfig{
							Level:      notification.Critical,
							LowerBound: &u,
						},
					},
				},
			},
			wants: wants{
				script: `package main
import "influxdata/influxdb/alerts"

data = from(bucket: "foo")
	|> range(start: -1d)

option task = {name: "moo", every: 1h, offset: 10m}

check = {checkID: "000000000000000a", tags: {aaa: "vaaa", bbb: "vbbb"}}
warn = (r) =>
	(r._value < 40.0)
crit = (r) =>
	(r._value > 40.0)
messageFn = (r, check) =>
	("whoa! {check.yeah}")

data
	|> alerts.check(
		check: check,
		messageFn: messageFn,
		warn: warn,
		crit: crit,
	)`,
			},
		},
		{
			name: "no levels",
			args: args{
				threshold: check.Threshold{
					Base: check.Base{
						ID:   10,
						Name: "moo",
						Tags: []notification.Tag{
							{Key: "aaa", Value: "vaaa"},
							{Key: "bbb", Value: "vbbb"},
						},
						StatusMessageTemplate: "whoa! {check.yeah}",
						Query: influxdb.DashboardQuery{
							Text: `data = from(bucket: "foo") |> range(start: -1d)`,
						},
					},
					Thresholds: []check.ThresholdConfig{},
				},
			},
			wants: wants{
				script: `package main
import "influxdata/influxdb/alerts"

data = from(bucket: "foo")
	|> range(start: -1d)

option task = {name: "moo"}

check = {checkID: "000000000000000a", tags: {aaa: "vaaa", bbb: "vbbb"}}
messageFn = (r, check) =>
	("whoa! {check.yeah}")

data
	|> alerts.check(check: check, messageFn: messageFn)`,
			},
		},
		{
			name: "no tags",
			args: args{
				threshold: check.Threshold{
					Base: check.Base{
						ID:                    10,
						Name:                  "moo",
						Cron:                  "5 4 * * *",
						Tags:                  []notification.Tag{},
						StatusMessageTemplate: "whoa! {check.yeah}",
						Query: influxdb.DashboardQuery{
							Text: `data = from(bucket: "foo") |> range(start: -1d)`,
						},
					},
					Thresholds: []check.ThresholdConfig{},
				},
			},
			wants: wants{
				script: `package main
import "influxdata/influxdb/alerts"

data = from(bucket: "foo")
	|> range(start: -1d)

option task = {name: "moo", cron: "5 4 * * *"}

check = {checkID: "000000000000000a", tags: {}}
messageFn = (r, check) =>
	("whoa! {check.yeah}")

data
	|> alerts.check(check: check, messageFn: messageFn)`,
			},
		},
		{
			name: "many tags",
			args: args{
				threshold: check.Threshold{
					Base: check.Base{
						ID:   10,
						Name: "foo",
						Tags: []notification.Tag{
							{Key: "a", Value: "b"},
							{Key: "b", Value: "c"},
							{Key: "c", Value: "d"},
							{Key: "d", Value: "e"},
							{Key: "e", Value: "f"},
							{Key: "f", Value: "g"},
						},
						StatusMessageTemplate: "whoa! {check.yeah}",
						Query: influxdb.DashboardQuery{
							Text: `data = from(bucket: "foo") |> range(start: -1d)`,
						},
					},
					Thresholds: []check.ThresholdConfig{},
				},
			},
			wants: wants{
				script: `package main
import "influxdata/influxdb/alerts"

data = from(bucket: "foo")
	|> range(start: -1d)

option task = {name: "foo"}

check = {checkID: "000000000000000a", tags: {
	a: "b",
	b: "c",
	c: "d",
	d: "e",
	e: "f",
	f: "g",
}}
messageFn = (r, check) =>
	("whoa! {check.yeah}")

data
	|> alerts.check(check: check, messageFn: messageFn)`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// TODO(desa): change this to GenerateFlux() when we don't need to code
			// around the alerts package not being available.
			p, err := tt.args.threshold.GenerateFluxASTReal()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if exp, got := tt.wants.script, ast.Format(p); exp != got {
				t.Errorf("expected:\n%v\n\ngot:\n%v\n", exp, got)
			}
		})
	}

}
