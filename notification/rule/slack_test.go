package rule_test

import (
	"testing"

	"github.com/influxdata/flux/parser"
	"github.com/influxdata/influxdb/notification"
	"github.com/influxdata/influxdb/notification/rule"
)

func mustDuration(d string) *notification.Duration {
	dur, err := parser.ParseDuration(d)
	if err != nil {
		panic(err)
	}

	return (*notification.Duration)(dur)
}

func TestSlack_GenerateFlux(t *testing.T) {
	want := `package main
// foo
import "influxdata/influxdb/alerts"
import "slack"
import "secrets"

option task = {name: "foo", every: 1h}

slack_secret = secrets.get(key: "<secrets key>")
slack_endpoint = slack.endpoint(token: slack_secret)
notification = {notificationID: "0000000000000001", name: "foo"}
statuses = from(bucket: "system_bucket")
	|> range(start: 1h)
	|> filter(fn: (r) =>
		(r.foo == "bar"))
	|> filter(fn: (r) =>
		(r.baz == "bang"))

statuses
	|> alerts.notify(name: "foo", notification: notification, endpoint: slack_endpoint(mapFn: (r) =>
		({channel: "bar", text: "blah"})))`

	s := &rule.Slack{
		Channel:         "bar",
		MessageTemplate: "blah",
		Base: rule.Base{
			ID:    1,
			Name:  "foo",
			Every: mustDuration("1h"),
			TagRules: []notification.TagRule{
				{
					Tag: notification.Tag{
						Key:   "foo",
						Value: "bar",
					},
					Operator: notification.Equal,
				},
				{
					Tag: notification.Tag{
						Key:   "baz",
						Value: "bang",
					},
					Operator: notification.Equal,
				},
			},
		},
	}

	f, err := s.GenerateFluxReal(nil)
	if err != nil {
		panic(err)
	}

	if f != want {
		t.Errorf("scripts did not match. want:\n%v\n\ngot:\n%v", want, f)
	}
}
