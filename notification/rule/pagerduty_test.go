package rule_test

import (
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/notification"
	"github.com/influxdata/influxdb/v2/notification/endpoint"
	"github.com/influxdata/influxdb/v2/notification/rule"
)

func TestPagerDuty_GenerateFlux(t *testing.T) {
	want := `package main
// foo
import "influxdata/influxdb/monitor"
import "pagerduty"
import "influxdata/influxdb/secrets"

option task = {name: "foo", every: 1h}

pagerduty_secret = secrets.get(key: "pagerduty_token")
pagerduty_endpoint = pagerduty.endpoint()
notification = {
	_notification_rule_id: "0000000000000001",
	_notification_rule_name: "foo",
	_notification_endpoint_id: "0000000000000002",
	_notification_endpoint_name: "foo",
}
statuses = monitor.from(start: -2h, fn: (r) =>
	(r.foo == "bar" and r.baz == "bang"))

statuses
	|> monitor.notify(data: notification, endpoint: pagerduty_endpoint(mapFn: (r) =>
		({
			routingKey: pagerduty_secret,
			client: "influxdata",
			clientURL: "http://localhost:7777/host/${r.host}",
			class: r._check_name,
			group: r._source_measurement,
			severity: pagerduty.severityFromLevel(level: r._level),
			eventAction: pagerduty.actionFromLevel(level: r._level),
			source: notification._notification_rule_name,
			summary: r._message,
			timestamp: time(v: r._source_timestamp),
		})))`

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
	}

	id := influxdb.ID(2)
	e := &endpoint.PagerDuty{
		Base: endpoint.Base{
			ID:   &id,
			Name: "foo",
		},
		ClientURL: "http://localhost:7777/host/${r.host}",
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
