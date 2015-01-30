package client_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/client"
)

func TestNewClient(t *testing.T) {
	config := client.Config{}
	_, err := client.NewClient(config)
	if err != nil {
		t.Fatalf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestClient_Ping(t *testing.T) {
	ts := emptyTestServer()
	defer ts.Close()

	u, _ := url.Parse(ts.URL)
	config := client.Config{URL: *u}
	c, err := client.NewClient(config)
	if err != nil {
		t.Fatalf("unexpected error.  expected %v, actual %v", nil, err)
	}
	d, version, err := c.Ping()
	if err != nil {
		t.Fatalf("unexpected error.  expected %v, actual %v", nil, err)
	}
	if d == 0 {
		t.Fatalf("expected a duration greater than zero.  actual %v", d)
	}
	if version != "x.x" {
		t.Fatalf("unexpected version.  expected %s,  actual %v", "x.x", version)
	}
}

func TestClient_Query(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data influxdb.Results
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	u, _ := url.Parse(ts.URL)
	config := client.Config{URL: *u}
	c, err := client.NewClient(config)
	if err != nil {
		t.Fatalf("unexpected error.  expected %v, actual %v", nil, err)
	}

	query := client.Query{}
	_, err = c.Query(query)
	if err != nil {
		t.Fatalf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestClient_BasicAuth(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()

		if !ok {
			t.Errorf("basic auth failed")
		}
		if u != "username" {
			t.Errorf("unexpected username, expected %q, actual %q", "username", u)
		}
		if p != "password" {
			t.Errorf("unexpected password, expected %q, actual %q", "password", p)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	u, _ := url.Parse(ts.URL)
	u.User = url.UserPassword("username", "password")
	config := client.Config{URL: *u, Username: "username", Password: "password"}
	c, err := client.NewClient(config)
	if err != nil {
		t.Fatalf("unexpected error.  expected %v, actual %v", nil, err)
	}

	_, _, err = c.Ping()
	if err != nil {
		t.Fatalf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestClient_Write(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data influxdb.Results
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	u, _ := url.Parse(ts.URL)
	config := client.Config{URL: *u}
	c, err := client.NewClient(config)
	if err != nil {
		t.Fatalf("unexpected error.  expected %v, actual %v", nil, err)
	}

	write := client.Write{}
	_, err = c.Write(write)
	if err != nil {
		t.Fatalf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestPoint_UnmarshalEpoch(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name      string
		epoch     int64
		precision string
		expected  time.Time
	}{
		{
			name:      "nanoseconds",
			epoch:     now.UnixNano(),
			precision: "n",
			expected:  now,
		},
		{
			name:      "microseconds",
			epoch:     now.Round(time.Microsecond).UnixNano() / int64(time.Microsecond),
			precision: "u",
			expected:  now.Round(time.Microsecond),
		},
		{
			name:      "milliseconds",
			epoch:     now.Round(time.Millisecond).UnixNano() / int64(time.Millisecond),
			precision: "ms",
			expected:  now.Round(time.Millisecond),
		},
		{
			name:      "seconds",
			epoch:     now.Round(time.Second).UnixNano() / int64(time.Second),
			precision: "s",
			expected:  now.Round(time.Second),
		},
		{
			name:      "minutes",
			epoch:     now.Round(time.Minute).UnixNano() / int64(time.Minute),
			precision: "m",
			expected:  now.Round(time.Minute),
		},
		{
			name:      "hours",
			epoch:     now.Round(time.Hour).UnixNano() / int64(time.Hour),
			precision: "h",
			expected:  now.Round(time.Hour),
		},
		{
			name:      "max int64",
			epoch:     9223372036854775807,
			precision: "n",
			expected:  time.Unix(0, 9223372036854775807),
		},
		{
			name:      "100 years from now",
			epoch:     now.Add(time.Hour * 24 * 365 * 100).UnixNano(),
			precision: "n",
			expected:  now.Add(time.Hour * 24 * 365 * 100),
		},
	}

	for _, test := range tests {
		t.Logf("testing %q\n", test.name)
		data := []byte(fmt.Sprintf(`{"timestamp": %d, "precision":"%s"}`, test.epoch, test.precision))
		t.Logf("json: %s", string(data))
		var p client.Point
		err := json.Unmarshal(data, &p)
		if err != nil {
			t.Fatalf("unexpected error.  exptected: %v, actual: %v", nil, err)
		}
		if !p.Timestamp.Time().Equal(test.expected) {
			t.Fatalf("Unexpected time.  expected: %v, actual: %v", test.expected, p.Timestamp.Time())
		}
	}
}

func TestPoint_UnmarshalRFC(t *testing.T) {
	now := time.Now().UTC()
	tests := []struct {
		name     string
		rfc      string
		now      time.Time
		expected time.Time
	}{
		{
			name:     "RFC3339Nano",
			rfc:      time.RFC3339Nano,
			now:      now,
			expected: now,
		},
		{
			name:     "RFC3339",
			rfc:      time.RFC3339,
			now:      now.Round(time.Second),
			expected: now.Round(time.Second),
		},
	}

	for _, test := range tests {
		t.Logf("testing %q\n", test.name)
		ts := test.now.Format(test.rfc)
		data := []byte(fmt.Sprintf(`{"timestamp": %q}`, ts))
		t.Logf("json: %s", string(data))
		var p client.Point
		err := json.Unmarshal(data, &p)
		if err != nil {
			t.Fatalf("unexpected error.  exptected: %v, actual: %v", nil, err)
		}
		if !p.Timestamp.Time().Equal(test.expected) {
			t.Fatalf("Unexpected time.  expected: %v, actual: %v", test.expected, p.Timestamp.Time())
		}
	}
}

func TestEpochToTime(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name      string
		epoch     int64
		precision string
		expected  time.Time
	}{
		{name: "nanoseconds", epoch: now.UnixNano(), precision: "n", expected: now},
		{name: "microseconds", epoch: now.Round(time.Microsecond).UnixNano() / int64(time.Microsecond), precision: "u", expected: now.Round(time.Microsecond)},
		{name: "milliseconds", epoch: now.Round(time.Millisecond).UnixNano() / int64(time.Millisecond), precision: "ms", expected: now.Round(time.Millisecond)},
		{name: "seconds", epoch: now.Round(time.Second).UnixNano() / int64(time.Second), precision: "s", expected: now.Round(time.Second)},
		{name: "minutes", epoch: now.Round(time.Minute).UnixNano() / int64(time.Minute), precision: "m", expected: now.Round(time.Minute)},
		{name: "hours", epoch: now.Round(time.Hour).UnixNano() / int64(time.Hour), precision: "h", expected: now.Round(time.Hour)},
	}

	for _, test := range tests {
		t.Logf("testing %q\n", test.name)
		tm, e := client.EpochToTime(test.epoch, test.precision)
		if e != nil {
			t.Fatalf("unexpected error: expected %v, actual: %v", nil, e)
		}
		if tm != test.expected {
			t.Fatalf("unexpected time: expected %v, actual %v", test.expected, tm)
		}
	}
}

// helper functions

func emptyTestServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Influxdb-Version", "x.x")
		return
	}))
}
