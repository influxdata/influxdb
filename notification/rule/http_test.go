package rule_test

import (
	"testing"

	"github.com/influxdata/influxdb/notification"
	"github.com/influxdata/influxdb/notification/rule"
)

func TestHTTP_GenerateFlux(t *testing.T) {
	want := `package main
// foo
import "influxdata/influxdb/alerts"
import "http"
import "json"

option task = {name: "foo", every: 1h, offset: 1s}

endpoint = http.endpoint(url: "http://localhost:7777")
notification = {
	_notification_rule_id: "0000000000000001",
	_notification_rule_name: "foo",
	_notification_endpoint_id: "0000000000000002",
	_notification_endpoint_name: "http-endpoint",
}
statuses = alerts.from(start: -1h, fn: (r) =>
	(r.foo == "bar" and r.baz == "bang"))

statuses
	|> alerts.notify(name: "foo", data: notification, endpoint: endpoint(mapFn: (r) =>
		({data: json.encode(v: r)})))`

	s := &rule.HTTP{
		URL: "http://localhost:7777",
		Base: rule.Base{
			ID:         1,
			Name:       "foo",
			Every:      mustDuration("1h"),
			Offset:     mustDuration("1s"),
			EndpointID: 2,
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

	f, err := s.GenerateFlux(nil)
	if err != nil {
		panic(err)
	}

	if f != want {
		t.Errorf("scripts did not match. want:\n%v\n\ngot:\n%v", want, f)
	}
}
