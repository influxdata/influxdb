package rule_test

import (
	"testing"

	"github.com/andreyvit/diff"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/notification"
	"github.com/influxdata/influxdb/v2/notification/endpoint"
	"github.com/influxdata/influxdb/v2/notification/rule"
	itesting "github.com/influxdata/influxdb/v2/testing"
)

func TestPagerDuty_GenerateFlux(t *testing.T) {
	tests := []struct {
		name     string
		rule     *rule.PagerDuty
		endpoint *endpoint.PagerDuty
		script   string
	}{
		{
			name: "notify on crit",
			endpoint: &endpoint.PagerDuty{
				Base: endpoint.Base{
					ID:   idPtr(2),
					Name: "foo",
				},
				ClientURL: "http://localhost:7777/host/${r.host}",
				RoutingKey: influxdb.SecretField{
					Key: "pagerduty_token",
				},
			},
			rule: &rule.PagerDuty{
				MessageTemplate: "blah",
				Base: rule.Base{
					ID:         1,
					EndpointID: 2,
					Name:       "foo",
					Every:      mustDuration("1h"),
					StatusRules: []notification.StatusRule{
						{
							CurrentLevel: notification.Critical,
						},
					},
					TagRules: []notification.TagRule{
						{
							Tag: influxdb.Tag{
								Key:   "foo",
								Value: "bar",
							},
							Operator: influxdb.Equal,
						},
						{
							Tag: influxdb.Tag{
								Key:   "baz",
								Value: "bang",
							},
							Operator: influxdb.Equal,
						},
					},
				},
			},
			script: `import "influxdata/influxdb/monitor"
import "pagerduty"
import "influxdata/influxdb/secrets"
import "experimental"

option task = {name: "foo", every: 1h}

pagerduty_secret = secrets["get"](key: "pagerduty_token")
pagerduty_endpoint = pagerduty["endpoint"]()
notification = {
    _notification_rule_id: "0000000000000001",
    _notification_rule_name: "foo",
    _notification_endpoint_id: "0000000000000002",
    _notification_endpoint_name: "foo",
}
statuses = monitor["from"](start: -2h, fn: (r) => r["foo"] == "bar" and r["baz"] == "bang")
crit = statuses |> filter(fn: (r) => r["_level"] == "crit")
all_statuses = crit |> filter(fn: (r) => r["_time"] >= experimental["subDuration"](from: now(), d: 1h))

all_statuses
    |> monitor["notify"](
        data: notification,
        endpoint:
            pagerduty_endpoint(
                mapFn: (r) =>
                    ({
                        routingKey: pagerduty_secret,
                        client: "influxdata",
                        clientURL: "http://localhost:7777/host/${r.host}",
                        class: r._check_name,
                        group: r["_source_measurement"],
                        severity: pagerduty["severityFromLevel"](level: r["_level"]),
                        eventAction: pagerduty["actionFromLevel"](level: r["_level"]),
                        source: notification["_notification_rule_name"],
                        summary: r["_message"],
                        timestamp: time(v: r["_source_timestamp"]),
                    }),
            ),
    )
`,
		},
		{
			name: "notify on info to crit",
			endpoint: &endpoint.PagerDuty{
				Base: endpoint.Base{
					ID:   idPtr(2),
					Name: "foo",
				},
				ClientURL: "http://localhost:7777/host/${r.host}",
				RoutingKey: influxdb.SecretField{
					Key: "pagerduty_token",
				},
			},
			rule: &rule.PagerDuty{
				MessageTemplate: "blah",
				Base: rule.Base{
					ID:         1,
					EndpointID: 2,
					Name:       "foo",
					Every:      mustDuration("1h"),
					StatusRules: []notification.StatusRule{
						{
							CurrentLevel:  notification.Critical,
							PreviousLevel: statusRulePtr(notification.Info),
						},
					},
					TagRules: []notification.TagRule{
						{
							Tag: influxdb.Tag{
								Key:   "foo",
								Value: "bar",
							},
							Operator: influxdb.Equal,
						},
						{
							Tag: influxdb.Tag{
								Key:   "baz",
								Value: "bang",
							},
							Operator: influxdb.Equal,
						},
					},
				},
			},
			script: `import "influxdata/influxdb/monitor"
import "pagerduty"
import "influxdata/influxdb/secrets"
import "experimental"

option task = {name: "foo", every: 1h}

pagerduty_secret = secrets["get"](key: "pagerduty_token")
pagerduty_endpoint = pagerduty["endpoint"]()
notification = {
    _notification_rule_id: "0000000000000001",
    _notification_rule_name: "foo",
    _notification_endpoint_id: "0000000000000002",
    _notification_endpoint_name: "foo",
}
statuses = monitor["from"](start: -2h, fn: (r) => r["foo"] == "bar" and r["baz"] == "bang")
info_to_crit = statuses |> monitor["stateChanges"](fromLevel: "info", toLevel: "crit")
all_statuses = info_to_crit |> filter(fn: (r) => r["_time"] >= experimental["subDuration"](from: now(), d: 1h))

all_statuses
    |> monitor["notify"](
        data: notification,
        endpoint:
            pagerduty_endpoint(
                mapFn: (r) =>
                    ({
                        routingKey: pagerduty_secret,
                        client: "influxdata",
                        clientURL: "http://localhost:7777/host/${r.host}",
                        class: r._check_name,
                        group: r["_source_measurement"],
                        severity: pagerduty["severityFromLevel"](level: r["_level"]),
                        eventAction: pagerduty["actionFromLevel"](level: r["_level"]),
                        source: notification["_notification_rule_name"],
                        summary: r["_message"],
                        timestamp: time(v: r["_source_timestamp"]),
                    }),
            ),
    )
`,
		},
		{
			name: "notify on crit or ok to warn",
			endpoint: &endpoint.PagerDuty{
				Base: endpoint.Base{
					ID:   idPtr(2),
					Name: "foo",
				},
				ClientURL: "http://localhost:7777/host/${r.host}",
				RoutingKey: influxdb.SecretField{
					Key: "pagerduty_token",
				},
			},
			rule: &rule.PagerDuty{
				MessageTemplate: "blah",
				Base: rule.Base{
					ID:         1,
					EndpointID: 2,
					Name:       "foo",
					Every:      mustDuration("1h"),
					StatusRules: []notification.StatusRule{
						{
							CurrentLevel: notification.Critical,
						},
						{
							CurrentLevel:  notification.Warn,
							PreviousLevel: statusRulePtr(notification.Ok),
						},
					},
					TagRules: []notification.TagRule{
						{
							Tag: influxdb.Tag{
								Key:   "foo",
								Value: "bar",
							},
							Operator: influxdb.Equal,
						},
						{
							Tag: influxdb.Tag{
								Key:   "baz",
								Value: "bang",
							},
							Operator: influxdb.Equal,
						},
					},
				},
			},
			script: `import "influxdata/influxdb/monitor"
import "pagerduty"
import "influxdata/influxdb/secrets"
import "experimental"

option task = {name: "foo", every: 1h}

pagerduty_secret = secrets["get"](key: "pagerduty_token")
pagerduty_endpoint = pagerduty["endpoint"]()
notification = {
    _notification_rule_id: "0000000000000001",
    _notification_rule_name: "foo",
    _notification_endpoint_id: "0000000000000002",
    _notification_endpoint_name: "foo",
}
statuses = monitor["from"](start: -2h, fn: (r) => r["foo"] == "bar" and r["baz"] == "bang")
crit = statuses |> filter(fn: (r) => r["_level"] == "crit")
ok_to_warn = statuses |> monitor["stateChanges"](fromLevel: "ok", toLevel: "warn")
all_statuses =
    union(tables: [crit, ok_to_warn])
        |> sort(columns: ["_time"])
        |> filter(fn: (r) => r["_time"] >= experimental["subDuration"](from: now(), d: 1h))

all_statuses
    |> monitor["notify"](
        data: notification,
        endpoint:
            pagerduty_endpoint(
                mapFn: (r) =>
                    ({
                        routingKey: pagerduty_secret,
                        client: "influxdata",
                        clientURL: "http://localhost:7777/host/${r.host}",
                        class: r._check_name,
                        group: r["_source_measurement"],
                        severity: pagerduty["severityFromLevel"](level: r["_level"]),
                        eventAction: pagerduty["actionFromLevel"](level: r["_level"]),
                        source: notification["_notification_rule_name"],
                        summary: r["_message"],
                        timestamp: time(v: r["_source_timestamp"]),
                    }),
            ),
    )
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			script, err := tt.rule.GenerateFlux(tt.endpoint)
			if err != nil {
				panic(err)
			}

			if got, want := script, itesting.FormatFluxString(t, tt.script); got != want {
				t.Errorf("\n\nStrings do not match:\n\n%s", diff.LineDiff(got, want))

			}

		})
	}

}
