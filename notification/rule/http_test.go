package rule_test

import (
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/notification"
	"github.com/influxdata/influxdb/notification/endpoint"
	"github.com/influxdata/influxdb/notification/rule"
)

func TestHTTP_GenerateFlux(t *testing.T) {
	want := `package main
// foo
import "influxdata/influxdb/monitor"
import "http"
import "json"
import "experimental"
import "influxdata/influxdb/v1"

option task = {name: "foo", every: 2h, offset: 1s}

endpoint = http.endpoint(url: "http://localhost:7777")
notification = {
	_notification_rule_id: "0000000000000001",
	_notification_rule_name: "foo",
	_notification_endpoint_id: "0000000000000002",
	_notification_endpoint_name: "foo",
}
statuses = monitor.from(start: -2h, fn: (r) =>
	(r.foo == "bar" and r.baz == "bang"))
	|> v1.fieldsAsCols()
any_to_crit = statuses
	|> monitor.stateChanges(fromLevel: "any", toLevel: "crit")
all_statuses = any_to_crit
	|> filter(fn: (r) =>
		(r._time > experimental.subDuration(from: now(), d: 1h)))

all_statuses
	|> monitor.notify(data: notification, endpoint: endpoint(mapFn: (r) =>
		({data: json.encode(v: r)})))`

	s := &rule.HTTP{
		Base: rule.Base{
			ID:         1,
			Name:       "foo",
			Every:      mustDuration("1h"),
			Offset:     mustDuration("1s"),
			EndpointID: 2,
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
			StatusRules: []notification.StatusRule{
				{
					CurrentLevel: notification.Critical,
				},
			},
		},
	}

	e := &endpoint.HTTP{
		Base: endpoint.Base{
			ID:   2,
			Name: "foo",
		},
		URL: "http://localhost:7777",
	}

	f, err := s.GenerateFlux(e)
	if err != nil {
		panic(err)
	}

	if f != want {
		t.Errorf("scripts did not match. want:\n%v\n\ngot:\n%v", want, f)
	}
}
