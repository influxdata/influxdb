package rule_test

import (
	"testing"

	"github.com/andreyvit/diff"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/notification"
	"github.com/influxdata/influxdb/v2/notification/endpoint"
	"github.com/influxdata/influxdb/v2/notification/rule"
	influxTesting "github.com/influxdata/influxdb/v2/testing"
)

var _ influxdb.NotificationRule = &rule.Teams{}

func TestTeams_GenerateFlux(t *testing.T) {
	tests := []struct {
		name     string
		rule     *rule.Teams
		endpoint influxdb.NotificationEndpoint
		script   string
	}{
		{
			name: "incompatible with endpoint",
			endpoint: &endpoint.Slack{
				Base: endpoint.Base{
					ID:   idPtr(3),
					Name: "foo",
				},
				URL: "http://whatever",
			},
			rule: &rule.Teams{
				Title:           "blah",
				MessageTemplate: "blah",
				Base: rule.Base{
					ID:         1,
					EndpointID: 3,
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
			script: "", //no script generated because of incompatible endpoint
		},
		{
			name: "notify on crit",
			endpoint: &endpoint.Teams{
				Base: endpoint.Base{
					ID:   idPtr(3),
					Name: "foo",
				},
				URL: "http://whatever",
			},
			rule: &rule.Teams{
				Title:           "bleh",
				MessageTemplate: "blah",
				Base: rule.Base{
					ID:         1,
					EndpointID: 3,
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
			script: `package main
// foo
import "influxdata/influxdb/monitor"
import "contrib/sranka/teams"
import "influxdata/influxdb/secrets"
import "experimental"

option task = {name: "foo", every: 1h}

teams_url_suffix = ""
teams_endpoint = teams["endpoint"](url: "http://whatever${teams_url_suffix}")
notification = {
	_notification_rule_id: "0000000000000001",
	_notification_rule_name: "foo",
	_notification_endpoint_id: "0000000000000003",
	_notification_endpoint_name: "foo",
}
statuses = monitor["from"](start: -2h, fn: (r) =>
	(r["foo"] == "bar" and r["baz"] == "bang"))
crit = statuses
	|> filter(fn: (r) =>
		(r["_level"] == "crit"))
all_statuses = crit
	|> filter(fn: (r) =>
		(r["_time"] > experimental["subDuration"](from: now(), d: 1h)))

all_statuses
	|> monitor["notify"](data: notification, endpoint: teams_endpoint(mapFn: (r) =>
		({title: "bleh", text: "blah", summary: ""})))`,
		},
		{
			name: "with SecretUrlSuffix",
			endpoint: &endpoint.Teams{
				Base: endpoint.Base{
					ID:   idPtr(3),
					Name: "foo",
				},
				URL:             "http://whatever",
				SecretURLSuffix: influxdb.SecretField{Key: "3-token"},
			},
			rule: &rule.Teams{
				Title:           "bleh",
				MessageTemplate: "blah",
				Base: rule.Base{
					ID:         1,
					EndpointID: 3,
					Name:       "foo",
					Every:      mustDuration("1h"),
					StatusRules: []notification.StatusRule{
						{
							CurrentLevel: notification.Any,
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
			script: `package main
// foo
import "influxdata/influxdb/monitor"
import "contrib/sranka/teams"
import "influxdata/influxdb/secrets"
import "experimental"

option task = {name: "foo", every: 1h}

teams_url_suffix = secrets["get"](key: "3-token")
teams_endpoint = teams["endpoint"](url: "http://whatever${teams_url_suffix}")
notification = {
	_notification_rule_id: "0000000000000001",
	_notification_rule_name: "foo",
	_notification_endpoint_id: "0000000000000003",
	_notification_endpoint_name: "foo",
}
statuses = monitor["from"](start: -2h, fn: (r) =>
	(r["foo"] == "bar" and r["baz"] == "bang"))
any = statuses
	|> filter(fn: (r) =>
		(true))
all_statuses = any
	|> filter(fn: (r) =>
		(r["_time"] > experimental["subDuration"](from: now(), d: 1h)))

all_statuses
	|> monitor["notify"](data: notification, endpoint: teams_endpoint(mapFn: (r) =>
		({title: "bleh", text: "blah", summary: ""})))`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			script, err := tt.rule.GenerateFlux(tt.endpoint)
			if err != nil {
				if script != "" {
					t.Errorf("Failed to generate flux: %v", err)
				}
				return
			}

			if got, want := script, tt.script; got != want {
				t.Errorf("\n\nStrings do not match:\n\n%s", diff.LineDiff(got, want))
			}
		})
	}
}

func TestTeams_Valid(t *testing.T) {
	cases := []struct {
		name string
		rule *rule.Teams
		err  error
	}{
		{
			name: "valid template",
			rule: &rule.Teams{
				Title:           "abc",
				MessageTemplate: "blah",
				Base: rule.Base{
					ID:         1,
					EndpointID: 3,
					OwnerID:    4,
					OrgID:      5,
					Name:       "foo",
					Every:      mustDuration("1h"),
					StatusRules: []notification.StatusRule{
						{
							CurrentLevel: notification.Critical,
						},
					},
					TagRules: []notification.TagRule{},
				},
			},
			err: nil,
		},
		{
			name: "missing MessageTemplate",
			rule: &rule.Teams{
				Title:           "abc",
				MessageTemplate: "",
				Base: rule.Base{
					ID:         1,
					EndpointID: 3,
					OwnerID:    4,
					OrgID:      5,
					Name:       "foo",
					Every:      mustDuration("1h"),
					StatusRules: []notification.StatusRule{
						{
							CurrentLevel: notification.Critical,
						},
					},
					TagRules: []notification.TagRule{},
				},
			},
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "teams: empty messageTemplate",
			},
		},
		{
			name: "missing Title",
			rule: &rule.Teams{
				Title:           "",
				MessageTemplate: "abc",
				Base: rule.Base{
					ID:         1,
					EndpointID: 3,
					OwnerID:    4,
					OrgID:      5,
					Name:       "foo",
					Every:      mustDuration("1h"),
					StatusRules: []notification.StatusRule{
						{
							CurrentLevel: notification.Critical,
						},
					},
					TagRules: []notification.TagRule{},
				},
			},
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "teams: empty title",
			},
		},
		{
			name: "missing EndpointID",
			rule: &rule.Teams{
				MessageTemplate: "",
				Base: rule.Base{
					ID: 1,
					// EndpointID: 3,
					OwnerID: 4,
					OrgID:   5,
					Name:    "foo",
					Every:   mustDuration("1h"),
					StatusRules: []notification.StatusRule{
						{
							CurrentLevel: notification.Critical,
						},
					},
					TagRules: []notification.TagRule{},
				},
			},
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "Notification Rule EndpointID is invalid",
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := c.rule.Valid()
			influxTesting.ErrorsEqual(t, got, c.err)
		})
	}

}
