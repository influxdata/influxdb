package rule_test

import (
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/notification"
	"github.com/influxdata/influxdb/v2/notification/endpoint"
	"github.com/influxdata/influxdb/v2/notification/rule"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTP_GenerateFlux(t *testing.T) {
	want := `import "influxdata/influxdb/monitor"
import "http"
import "json"
import "experimental"

option task = {name: "foo", every: 1h, offset: 1s}

headers = {"Content-Type": "application/json"}
endpoint = http["endpoint"](url: "http://localhost:7777")
notification = {_notification_rule_id: "0000000000000001", _notification_rule_name: "foo", _notification_endpoint_id: "0000000000000002", _notification_endpoint_name: "foo"}
statuses = monitor["from"](start: -2h)
crit = statuses |> filter(fn: (r) => r["_level"] == "crit")
all_statuses = crit |> filter(fn: (r) => r["_time"] >= experimental["subDuration"](from: now(), d: 1h))

all_statuses |> monitor["notify"](data: notification, endpoint: endpoint(mapFn: (r) => {
    body = {r with _version: 1}

    return {headers: headers, data: json["encode"](v: body)}
}))`

	s := &rule.HTTP{
		Base: rule.Base{
			ID:         1,
			Name:       "foo",
			Every:      mustDuration("1h"),
			Offset:     mustDuration("1s"),
			EndpointID: 2,
			TagRules:   []notification.TagRule{},
			StatusRules: []notification.StatusRule{
				{
					CurrentLevel: notification.Critical,
				},
			},
		},
	}

	id := platform.ID(2)
	e := &endpoint.HTTP{
		Base: endpoint.Base{
			ID:   &id,
			Name: "foo",
		},
		URL: "http://localhost:7777",
	}

	f, err := s.GenerateFlux(e)
	if err != nil {
		t.Fatal(err)
	}

	if f != want {
		t.Errorf("scripts did not match. want:\n%v\n\ngot:\n%v", want, f)
	}
}

func TestHTTP_GenerateFlux_basicAuth(t *testing.T) {
	want := `import "influxdata/influxdb/monitor"
import "http"
import "json"
import "experimental"
import "influxdata/influxdb/secrets"

option task = {name: "foo", every: 1h, offset: 1s}

headers = {"Content-Type": "application/json", "Authorization": http["basicAuth"](u: secrets["get"](key: "000000000000000e-username"), p: secrets["get"](key: "000000000000000e-password"))}
endpoint = http["endpoint"](url: "http://localhost:7777")
notification = {_notification_rule_id: "0000000000000001", _notification_rule_name: "foo", _notification_endpoint_id: "0000000000000002", _notification_endpoint_name: "foo"}
statuses = monitor["from"](start: -2h)
crit = statuses |> filter(fn: (r) => r["_level"] == "crit")
all_statuses = crit |> filter(fn: (r) => r["_time"] >= experimental["subDuration"](from: now(), d: 1h))

all_statuses |> monitor["notify"](data: notification, endpoint: endpoint(mapFn: (r) => {
    body = {r with _version: 1}

    return {headers: headers, data: json["encode"](v: body)}
}))`
	s := &rule.HTTP{
		Base: rule.Base{
			ID:         1,
			Name:       "foo",
			Every:      mustDuration("1h"),
			Offset:     mustDuration("1s"),
			EndpointID: 2,
			TagRules:   []notification.TagRule{},
			StatusRules: []notification.StatusRule{
				{
					CurrentLevel: notification.Critical,
				},
			},
		},
	}

	id := platform.ID(2)
	e := &endpoint.HTTP{
		Base: endpoint.Base{
			ID:   &id,
			Name: "foo",
		},
		URL:        "http://localhost:7777",
		AuthMethod: "basic",
		Username: influxdb.SecretField{
			Key: "000000000000000e-username",
		},
		Password: influxdb.SecretField{
			Key: "000000000000000e-password",
		},
	}

	f, err := s.GenerateFlux(e)
	if err != nil {
		t.Fatal(err)
	}

	if f != want {
		t.Errorf("scripts did not match. want:\n%v\n\ngot:\n%v", want, f)
	}
}

func TestHTTP_GenerateFlux_bearer(t *testing.T) {
	want := `import "influxdata/influxdb/monitor"
import "http"
import "json"
import "experimental"
import "influxdata/influxdb/secrets"

option task = {name: "foo", every: 1h, offset: 1s}

headers = {"Content-Type": "application/json", "Authorization": "Bearer " + secrets["get"](key: "000000000000000e-token")}
endpoint = http["endpoint"](url: "http://localhost:7777")
notification = {_notification_rule_id: "0000000000000001", _notification_rule_name: "foo", _notification_endpoint_id: "0000000000000002", _notification_endpoint_name: "foo"}
statuses = monitor["from"](start: -2h)
crit = statuses |> filter(fn: (r) => r["_level"] == "crit")
all_statuses = crit |> filter(fn: (r) => r["_time"] >= experimental["subDuration"](from: now(), d: 1h))

all_statuses |> monitor["notify"](data: notification, endpoint: endpoint(mapFn: (r) => {
    body = {r with _version: 1}

    return {headers: headers, data: json["encode"](v: body)}
}))`

	s := &rule.HTTP{
		Base: rule.Base{
			ID:         1,
			Name:       "foo",
			Every:      mustDuration("1h"),
			Offset:     mustDuration("1s"),
			EndpointID: 2,
			TagRules:   []notification.TagRule{},
			StatusRules: []notification.StatusRule{
				{
					CurrentLevel: notification.Critical,
				},
			},
		},
	}

	id := platform.ID(2)
	e := &endpoint.HTTP{
		Base: endpoint.Base{
			ID:   &id,
			Name: "foo",
		},
		URL:        "http://localhost:7777",
		AuthMethod: "bearer",
		Token: influxdb.SecretField{
			Key: "000000000000000e-token",
		},
	}

	f, err := s.GenerateFlux(e)
	if err != nil {
		t.Fatal(err)
	}

	if f != want {
		t.Errorf("scripts did not match. want:\n%v\n\ngot:\n%v", want, f)
	}
}

func TestHTTP_GenerateFlux_bearer_every_second(t *testing.T) {
	want := `import "influxdata/influxdb/monitor"
import "http"
import "json"
import "experimental"
import "influxdata/influxdb/secrets"

option task = {name: "foo", every: 5s, offset: 1s}

headers = {"Content-Type": "application/json", "Authorization": "Bearer " + secrets["get"](key: "000000000000000e-token")}
endpoint = http["endpoint"](url: "http://localhost:7777")
notification = {_notification_rule_id: "0000000000000001", _notification_rule_name: "foo", _notification_endpoint_id: "0000000000000002", _notification_endpoint_name: "foo"}
statuses = monitor["from"](start: -10s)
crit = statuses |> filter(fn: (r) => r["_level"] == "crit")
all_statuses = crit |> filter(fn: (r) => r["_time"] >= experimental["subDuration"](from: now(), d: 5s))

all_statuses |> monitor["notify"](data: notification, endpoint: endpoint(mapFn: (r) => {
    body = {r with _version: 1}

    return {headers: headers, data: json["encode"](v: body)}
}))`

	s := &rule.HTTP{
		Base: rule.Base{
			ID:         1,
			Name:       "foo",
			Every:      mustDuration("5s"),
			Offset:     mustDuration("1s"),
			EndpointID: 2,
			TagRules:   []notification.TagRule{},
			StatusRules: []notification.StatusRule{
				{
					CurrentLevel: notification.Critical,
				},
			},
		},
	}

	id := platform.ID(2)
	e := &endpoint.HTTP{
		Base: endpoint.Base{
			ID:   &id,
			Name: "foo",
		},
		URL:        "http://localhost:7777",
		AuthMethod: "bearer",
		Token: influxdb.SecretField{
			Key: "000000000000000e-token",
		},
	}

	f, err := s.GenerateFlux(e)
	require.NoError(t, err)
	assert.Equal(t, want, f)
}
