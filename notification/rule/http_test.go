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

option task = {name: "foo", every: 1h}

http_endpoint = http.endpoint(url: "http://localhost:7777")
notification = {notificationID: "0000000000000001", name: "foo"}
statuses = from(bucket: "system_bucket")
	|> range(start: 1h)
	|> filter(fn: (r) =>
		(r.foo == "bar"))
	|> filter(fn: (r) =>
		(r.baz == "bang"))

statuses
	|> alerts.notify(name: "foo", notification: notification, endpoint: http_endpoint(mapFn: (r) =>
		({data: json.encode(v: r)})))`

	s := &rule.HTTP{
		URL: "http://localhost:7777",
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
