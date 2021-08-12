package rule_test

import (
	"testing"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/parser"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/notification"
	"github.com/influxdata/influxdb/v2/notification/endpoint"
	"github.com/influxdata/influxdb/v2/notification/rule"
)

func mustDuration(d string) *notification.Duration {
	dur, err := parser.ParseDuration(d)
	if err != nil {
		panic(err)
	}
	dur.BaseNode = ast.BaseNode{}
	return (*notification.Duration)(dur)
}

func statusRulePtr(r notification.CheckLevel) *notification.CheckLevel {
	return &r
}

func idPtr(i int) *influxdb.ID {
	id := influxdb.ID(i)
	return &id
}

func TestSlack_GenerateFlux(t *testing.T) {
	tests := []struct {
		name     string
		want     string
		rule     *rule.Slack
		endpoint *endpoint.Slack
	}{
		{
			name: "with any status",
			want: `import "influxdata/influxdb/monitor"
import "slack"
import "influxdata/influxdb/secrets"
import "experimental"

option task = {name: "foo", every: 1h}

slack_endpoint = slack["endpoint"](url: "http://localhost:7777")
notification = {_notification_rule_id: "0000000000000001", _notification_rule_name: "foo", _notification_endpoint_id: "0000000000000002", _notification_endpoint_name: "foo"}
statuses = monitor["from"](start: -2h, fn: (r) => r["foo"] == "bar" and r["baz"] == "bang")
any = statuses |> filter(fn: (r) => true)
all_statuses = any |> filter(fn: (r) => r["_time"] >= experimental["subDuration"](from: now(), d: 1h))

all_statuses |> monitor["notify"](data: notification, endpoint: slack_endpoint(mapFn: (r) => ({channel: "bar", text: "blah", color: if r["_level"] == "crit" then "danger" else if r["_level"] == "warn" then "warning" else "good"})))`,
			rule: &rule.Slack{
				Channel:         "bar",
				MessageTemplate: "blah",
				Base: rule.Base{
					ID:         1,
					EndpointID: 2,
					Name:       "foo",
					Every:      mustDuration("1h"),
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
					StatusRules: []notification.StatusRule{
						{
							CurrentLevel: notification.Any,
						},
					},
				},
			},
			endpoint: &endpoint.Slack{
				Base: endpoint.Base{
					ID:   idPtr(2),
					Name: "foo",
				},
				URL: "http://localhost:7777",
			},
		},
		{
			name: "with url",
			want: `import "influxdata/influxdb/monitor"
import "slack"
import "influxdata/influxdb/secrets"
import "experimental"

option task = {name: "foo", every: 1h}

slack_endpoint = slack["endpoint"](url: "http://localhost:7777")
notification = {_notification_rule_id: "0000000000000001", _notification_rule_name: "foo", _notification_endpoint_id: "0000000000000002", _notification_endpoint_name: "foo"}
statuses = monitor["from"](start: -2h, fn: (r) => r["foo"] == "bar" and r["baz"] == "bang")
crit = statuses |> filter(fn: (r) => r["_level"] == "crit")
info_to_warn = statuses |> monitor["stateChanges"](fromLevel: "info", toLevel: "warn")
all_statuses = union(tables: [crit, info_to_warn]) |> sort(columns: ["_time"]) |> filter(fn: (r) => r["_time"] >= experimental["subDuration"](from: now(), d: 1h))

all_statuses |> monitor["notify"](data: notification, endpoint: slack_endpoint(mapFn: (r) => ({channel: "bar", text: "blah", color: if r["_level"] == "crit" then "danger" else if r["_level"] == "warn" then "warning" else "good"})))`,
			rule: &rule.Slack{
				Channel:         "bar",
				MessageTemplate: "blah",
				Base: rule.Base{
					ID:         1,
					EndpointID: 2,
					Name:       "foo",
					Every:      mustDuration("1h"),
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
					StatusRules: []notification.StatusRule{
						{
							CurrentLevel: notification.Critical,
						},
						{
							CurrentLevel:  notification.Warn,
							PreviousLevel: statusRulePtr(notification.Info),
						},
					},
				},
			},
			endpoint: &endpoint.Slack{
				Base: endpoint.Base{
					ID:   idPtr(2),
					Name: "foo",
				},
				URL: "http://localhost:7777",
			},
		},
		{
			name: "with token",
			want: `import "influxdata/influxdb/monitor"
import "slack"
import "influxdata/influxdb/secrets"
import "experimental"

option task = {name: "foo", every: 1h}

slack_secret = secrets["get"](key: "slack_token")
slack_endpoint = slack["endpoint"](token: slack_secret)
notification = {_notification_rule_id: "0000000000000001", _notification_rule_name: "foo", _notification_endpoint_id: "0000000000000002", _notification_endpoint_name: "foo"}
statuses = monitor["from"](start: -2h, fn: (r) => r["foo"] == "bar" and r["baz"] == "bang")
crit = statuses |> filter(fn: (r) => r["_level"] == "crit")
info_to_warn = statuses |> monitor["stateChanges"](fromLevel: "info", toLevel: "warn")
all_statuses = union(tables: [crit, info_to_warn]) |> sort(columns: ["_time"]) |> filter(fn: (r) => r["_time"] >= experimental["subDuration"](from: now(), d: 1h))

all_statuses |> monitor["notify"](data: notification, endpoint: slack_endpoint(mapFn: (r) => ({channel: "bar", text: "blah", color: if r["_level"] == "crit" then "danger" else if r["_level"] == "warn" then "warning" else "good"})))`,
			rule: &rule.Slack{
				Channel:         "bar",
				MessageTemplate: "blah",
				Base: rule.Base{
					ID:         1,
					EndpointID: 2,
					Name:       "foo",
					Every:      mustDuration("1h"),
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
					StatusRules: []notification.StatusRule{
						{
							CurrentLevel: notification.Critical,
						},
						{
							CurrentLevel:  notification.Warn,
							PreviousLevel: statusRulePtr(notification.Info),
						},
					},
				},
			},
			endpoint: &endpoint.Slack{
				Base: endpoint.Base{
					ID:   idPtr(2),
					Name: "foo",
				},
				Token: influxdb.SecretField{
					Key: "slack_token",
				},
			},
		},
		{
			name: "with token and url",
			want: `import "influxdata/influxdb/monitor"
import "slack"
import "influxdata/influxdb/secrets"
import "experimental"

option task = {name: "foo", every: 1h}

slack_secret = secrets["get"](key: "slack_token")
slack_endpoint = slack["endpoint"](token: slack_secret, url: "http://localhost:7777")
notification = {_notification_rule_id: "0000000000000001", _notification_rule_name: "foo", _notification_endpoint_id: "0000000000000002", _notification_endpoint_name: "foo"}
statuses = monitor["from"](start: -2h, fn: (r) => r["foo"] == "bar" and r["baz"] == "bang")
crit = statuses |> filter(fn: (r) => r["_level"] == "crit")
info_to_warn = statuses |> monitor["stateChanges"](fromLevel: "info", toLevel: "warn")
all_statuses = union(tables: [crit, info_to_warn]) |> sort(columns: ["_time"]) |> filter(fn: (r) => r["_time"] >= experimental["subDuration"](from: now(), d: 1h))

all_statuses |> monitor["notify"](data: notification, endpoint: slack_endpoint(mapFn: (r) => ({channel: "bar", text: "blah", color: if r["_level"] == "crit" then "danger" else if r["_level"] == "warn" then "warning" else "good"})))`,
			rule: &rule.Slack{
				Channel:         "bar",
				MessageTemplate: "blah",
				Base: rule.Base{
					ID:         1,
					EndpointID: 2,
					Name:       "foo",
					Every:      mustDuration("1h"),
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
					StatusRules: []notification.StatusRule{
						{
							CurrentLevel: notification.Critical,
						},
						{
							CurrentLevel:  notification.Warn,
							PreviousLevel: statusRulePtr(notification.Info),
						},
					},
				},
			},
			endpoint: &endpoint.Slack{
				Base: endpoint.Base{
					ID:   idPtr(2),
					Name: "foo",
				},
				URL: "http://localhost:7777",
				Token: influxdb.SecretField{
					Key: "slack_token",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := tt.rule.GenerateFlux(tt.endpoint)
			if err != nil {
				t.Fatal(err)
			}

			if f != tt.want {
				t.Errorf("scripts did not match. want:\n%v\n\ngot:\n%v", tt.want, f)
			}
		})
	}
}
