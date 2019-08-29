package rule_test

import (
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/notification"
	"github.com/influxdata/influxdb/notification/endpoint"
	"github.com/influxdata/influxdb/notification/rule"
)

func TestPagerDuty_GenerateFlux(t *testing.T) {
	want := `package main
// foo
import "influxdata/influxdb/monitor"
import "pagerduty"
import "influxdata/influxdb/secrets"

option task = {name: "foo", every: 1h}

pagerduty_secret = secrets.get(key: "pagerduty_token")
pagerduty_endpoint = pagerduty.endpoint(routing_key: pagerduty_secret, url: "http://localhost:7777")
notification = {
	_notification_rule_id: "0000000000000001",
	_notification_rule_name: "foo",
	_notification_endpoint_id: "0000000000000002",
	_notification_endpoint_name: "foo",
}
statuses = monitor.from(start: -1h, fn: (r) =>
	(r.foo == "bar" and r.baz == "bang"))

statuses
	|> monitor.notify(data: notification, endpoint: pagerduty_endpoint(mapFn: (r) =>
		({text: "blah"})))`

	s := &rule.PagerDuty{
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
					Operator: notification.Equal,
				},
				{
					Tag: influxdb.Tag{
						Key:   "baz",
						Value: "bang",
					},
					Operator: notification.Equal,
				},
			},
		},
	}
	e := &endpoint.PagerDuty{
		Base: endpoint.Base{
			ID:   2,
			Name: "foo",
		},
		URL: "http://localhost:7777",
		RoutingKey: influxdb.SecretField{
			Key: "pagerduty_token",
		},
	}

	f, err := s.GenerateFlux(e)
	if err != nil {
		panic(err)
	}

	if f != want {
		t.Errorf("scripts did not match. want:\n%v\n\ngot:\n%v", want, f)
	}
}
