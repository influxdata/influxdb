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

var _ influxdb.NotificationRule = &rule.Telegram{}

func TestTelegram_GenerateFlux(t *testing.T) {
	tests := []struct {
		name     string
		rule     *rule.Telegram
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
			rule: &rule.Telegram{
				MessageTemplate: "blah",
				Channel:         "-12345",
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
			script: "", //no script generater, because of incompatible endpoint
		},
		{
			name: "notify on crit",
			endpoint: &endpoint.Telegram{
				Base: endpoint.Base{
					ID:   idPtr(3),
					Name: "foo",
				},
				Token: influxdb.SecretField{Key: "3-key"},
			},
			rule: &rule.Telegram{
				MessageTemplate: "blah",
				Channel:         "-12345",
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
import "contrib/sranka/telegram"
import "influxdata/influxdb/secrets"
import "experimental"

option task = {name: "foo", every: 1h}

telegram_secret = secrets["get"](key: "3-key")
telegram_endpoint = telegram["endpoint"](token: telegram_secret, disableWebPagePreview: false)
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
	|> monitor["notify"](data: notification, endpoint: telegram_endpoint(mapFn: (r) =>
		({channel: "-12345", text: "blah", silent: if r["_level"] == "crit" then true else if r["_level"] == "warn" then true else false})))`,
		},
		{
			name: "with DisableWebPagePreview and ParseMode",
			endpoint: &endpoint.Telegram{
				Base: endpoint.Base{
					ID:   idPtr(3),
					Name: "foo",
				},
				Token: influxdb.SecretField{Key: "3-key"},
			},
			rule: &rule.Telegram{
				MessageTemplate:       "blah",
				Channel:               "-12345",
				DisableWebPagePreview: true,
				ParseMode:             "HTML",
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
import "contrib/sranka/telegram"
import "influxdata/influxdb/secrets"
import "experimental"

option task = {name: "foo", every: 1h}

telegram_secret = secrets["get"](key: "3-key")
telegram_endpoint = telegram["endpoint"](token: telegram_secret, parseMode: "HTML", disableWebPagePreview: true)
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
	|> monitor["notify"](data: notification, endpoint: telegram_endpoint(mapFn: (r) =>
		({channel: "-12345", text: "blah", silent: if r["_level"] == "crit" then true else if r["_level"] == "warn" then true else false})))`,
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

func TestTelegram_Valid(t *testing.T) {
	cases := []struct {
		name string
		rule *rule.Telegram
		err  error
	}{
		{
			name: "valid template",
			rule: &rule.Telegram{
				MessageTemplate: "blah",
				Channel:         "-12345",
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
			rule: &rule.Telegram{
				MessageTemplate: "",
				Channel:         "-12345",
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
				Msg:  "Telegram MessageTemplate is invalid",
			},
		},
		{
			name: "missing EndpointID",
			rule: &rule.Telegram{
				MessageTemplate: "",
				Channel:         "-12345",
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
		{
			name: "missing Channel",
			rule: &rule.Telegram{
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
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "Telegram Channel is invalid",
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
