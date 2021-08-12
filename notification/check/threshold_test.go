package check_test

import (
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/notification"
	"github.com/influxdata/influxdb/v2/notification/check"
	"github.com/influxdata/influxdb/v2/query/fluxlang"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			name: "all levels with yield and stop",
			args: args{
				threshold: check.Threshold{
					Base: check.Base{
						ID:   10,
						Name: "moo",
						Tags: []influxdb.Tag{
							{Key: "aaa", Value: "vaaa"},
							{Key: "bbb", Value: "vbbb"},
						},
						Every:                 mustDuration("1h"),
						StatusMessageTemplate: "whoa! {r[\"usage_user\"]}",
						Query: influxdb.DashboardQuery{
							Text: `from(bucket: "foo") |> range(start: -1d, stop: now()) |> filter(fn: (r) => r._field == "usage_user") |> aggregateWindow(every: 1m, fn: mean) |> yield()`,
						},
					},
					Thresholds: []check.ThresholdConfig{
						check.Greater{
							ThresholdConfigBase: check.ThresholdConfigBase{
								Level: notification.Ok,
							},
							Value: l,
						},
						check.Lesser{
							ThresholdConfigBase: check.ThresholdConfigBase{
								Level: notification.Info,
							},
							Value: u,
						},
						check.Range{
							ThresholdConfigBase: check.ThresholdConfigBase{
								Level: notification.Warn,
							},
							Min:    l,
							Max:    u,
							Within: true,
						},
						check.Range{
							ThresholdConfigBase: check.ThresholdConfigBase{
								Level: notification.Critical,
							},
							Min:    l,
							Max:    u,
							Within: false,
						},
					},
				},
			},
			wants: wants{
				script: `import "influxdata/influxdb/monitor"
import "influxdata/influxdb/v1"

data = from(bucket: "foo") |> range(start: -1h) |> filter(fn: (r) => r._field == "usage_user")
    |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)

option task = {name: "moo", every: 1h}

check = {_check_id: "000000000000000a", _check_name: "moo", _type: "threshold", tags: {aaa: "vaaa", bbb: "vbbb"}}
ok = (r) => r["usage_user"] > 10.0
info = (r) => r["usage_user"] < 40.0
warn = (r) => r["usage_user"] < 40.0 and r["usage_user"] > 10.0
crit = (r) => r["usage_user"] < 10.0 or r["usage_user"] > 40.0
messageFn = (r) => "whoa! {r[\"usage_user\"]}"

data |> v1["fieldsAsCols"]() |> monitor["check"](
    data: check,
    messageFn: messageFn,
    ok: ok,
    info: info,
    warn: warn,
    crit: crit,
)`,
			},
		},
		{
			name: "all levels with yield",
			args: args{
				threshold: check.Threshold{
					Base: check.Base{
						ID:   10,
						Name: "moo",
						Tags: []influxdb.Tag{
							{Key: "aaa", Value: "vaaa"},
							{Key: "bbb", Value: "vbbb"},
						},
						Every:                 mustDuration("1h"),
						StatusMessageTemplate: "whoa! {r[\"usage_user\"]}",
						Query: influxdb.DashboardQuery{
							Text: `from(bucket: "foo") |> range(start: -1d) |> filter(fn: (r) => r._field == "usage_user") |> aggregateWindow(every: 1m, fn: mean) |> yield()`,
						},
					},
					Thresholds: []check.ThresholdConfig{
						check.Greater{
							ThresholdConfigBase: check.ThresholdConfigBase{
								Level: notification.Ok,
							},
							Value: l,
						},
						check.Lesser{
							ThresholdConfigBase: check.ThresholdConfigBase{
								Level: notification.Info,
							},
							Value: u,
						},
						check.Range{
							ThresholdConfigBase: check.ThresholdConfigBase{
								Level: notification.Warn,
							},
							Min:    l,
							Max:    u,
							Within: true,
						},
						check.Range{
							ThresholdConfigBase: check.ThresholdConfigBase{
								Level: notification.Critical,
							},
							Min:    l,
							Max:    u,
							Within: false,
						},
					},
				},
			},
			wants: wants{
				script: `import "influxdata/influxdb/monitor"
import "influxdata/influxdb/v1"

data = from(bucket: "foo") |> range(start: -1h) |> filter(fn: (r) => r._field == "usage_user")
    |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)

option task = {name: "moo", every: 1h}

check = {_check_id: "000000000000000a", _check_name: "moo", _type: "threshold", tags: {aaa: "vaaa", bbb: "vbbb"}}
ok = (r) => r["usage_user"] > 10.0
info = (r) => r["usage_user"] < 40.0
warn = (r) => r["usage_user"] < 40.0 and r["usage_user"] > 10.0
crit = (r) => r["usage_user"] < 10.0 or r["usage_user"] > 40.0
messageFn = (r) => "whoa! {r[\"usage_user\"]}"

data |> v1["fieldsAsCols"]() |> monitor["check"](
    data: check,
    messageFn: messageFn,
    ok: ok,
    info: info,
    warn: warn,
    crit: crit,
)`,
			},
		},
		{
			name: "all levels with yield and space in field name",
			args: args{
				threshold: check.Threshold{
					Base: check.Base{
						ID:   10,
						Name: "moo",
						Tags: []influxdb.Tag{
							{Key: "aaa", Value: "vaaa"},
							{Key: "bbb", Value: "vbbb"},
						},
						Every:                 mustDuration("1h"),
						StatusMessageTemplate: "whoa! {r[\"usage user\"]}",
						Query: influxdb.DashboardQuery{
							Text: `from(bucket: "foo") |> range(start: -1d) |> filter(fn: (r) => r._field == "usage user") |> aggregateWindow(every: 1m, fn: mean) |> yield()`,
						},
					},
					Thresholds: []check.ThresholdConfig{
						check.Greater{
							ThresholdConfigBase: check.ThresholdConfigBase{
								Level: notification.Ok,
							},
							Value: l,
						},
						check.Lesser{
							ThresholdConfigBase: check.ThresholdConfigBase{
								Level: notification.Info,
							},
							Value: u,
						},
						check.Range{
							ThresholdConfigBase: check.ThresholdConfigBase{
								Level: notification.Warn,
							},
							Min:    l,
							Max:    u,
							Within: true,
						},
						check.Range{
							ThresholdConfigBase: check.ThresholdConfigBase{
								Level: notification.Critical,
							},
							Min:    l,
							Max:    u,
							Within: false,
						},
					},
				},
			},
			wants: wants{
				script: `import "influxdata/influxdb/monitor"
import "influxdata/influxdb/v1"

data = from(bucket: "foo") |> range(start: -1h) |> filter(fn: (r) => r._field == "usage user")
    |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)

option task = {name: "moo", every: 1h}

check = {_check_id: "000000000000000a", _check_name: "moo", _type: "threshold", tags: {aaa: "vaaa", bbb: "vbbb"}}
ok = (r) => r["usage user"] > 10.0
info = (r) => r["usage user"] < 40.0
warn = (r) => r["usage user"] < 40.0 and r["usage user"] > 10.0
crit = (r) => r["usage user"] < 10.0 or r["usage user"] > 40.0
messageFn = (r) => "whoa! {r[\"usage user\"]}"

data |> v1["fieldsAsCols"]() |> monitor["check"](
    data: check,
    messageFn: messageFn,
    ok: ok,
    info: info,
    warn: warn,
    crit: crit,
)`,
			},
		},
		{
			name: "all levels without yield",
			args: args{
				threshold: check.Threshold{
					Base: check.Base{
						ID:   10,
						Name: "moo",
						Tags: []influxdb.Tag{
							{Key: "aaa", Value: "vaaa"},
							{Key: "bbb", Value: "vbbb"},
						},
						Every:                 mustDuration("1h"),
						StatusMessageTemplate: "whoa! {r[\"usage_user\"]}",
						Query: influxdb.DashboardQuery{
							Text: `from(bucket: "foo") |> range(start: -1d) |> filter(fn: (r) => r._field == "usage_user") |> aggregateWindow(every: 1m, fn: mean)`,
						},
					},
					Thresholds: []check.ThresholdConfig{
						check.Greater{
							ThresholdConfigBase: check.ThresholdConfigBase{
								Level: notification.Ok,
							},
							Value: l,
						},
						check.Lesser{
							ThresholdConfigBase: check.ThresholdConfigBase{
								Level: notification.Info,
							},
							Value: u,
						},
						check.Range{
							ThresholdConfigBase: check.ThresholdConfigBase{
								Level: notification.Warn,
							},
							Min:    l,
							Max:    u,
							Within: true,
						},
						check.Range{
							ThresholdConfigBase: check.ThresholdConfigBase{
								Level: notification.Critical,
							},
							Min:    l,
							Max:    u,
							Within: true,
						},
					},
				},
			},
			wants: wants{
				script: `import "influxdata/influxdb/monitor"
import "influxdata/influxdb/v1"

data = from(bucket: "foo") |> range(start: -1h) |> filter(fn: (r) => r._field == "usage_user")
    |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)

option task = {name: "moo", every: 1h}

check = {_check_id: "000000000000000a", _check_name: "moo", _type: "threshold", tags: {aaa: "vaaa", bbb: "vbbb"}}
ok = (r) => r["usage_user"] > 10.0
info = (r) => r["usage_user"] < 40.0
warn = (r) => r["usage_user"] < 40.0 and r["usage_user"] > 10.0
crit = (r) => r["usage_user"] < 40.0 and r["usage_user"] > 10.0
messageFn = (r) => "whoa! {r[\"usage_user\"]}"

data |> v1["fieldsAsCols"]() |> monitor["check"](
    data: check,
    messageFn: messageFn,
    ok: ok,
    info: info,
    warn: warn,
    crit: crit,
)`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := tt.args.threshold.GenerateFlux(fluxlang.DefaultService)
			require.NoError(t, err)
			assert.Equal(t, tt.wants.script, s)
		})
	}

}
