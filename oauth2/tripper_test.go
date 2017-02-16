package oauth2_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"

	"github.com/influxdata/chronograf"
)

func NewTestTripper(log chronograf.Logger, ts *httptest.Server, rt http.RoundTripper) (*TestTripper, error) {
	url, err := url.Parse(ts.URL)
	if err != nil {
		return nil, err
	}
	return &TestTripper{log, rt, url}, nil
}

type TestTripper struct {
	Log chronograf.Logger

	rt    http.RoundTripper
	tsURL *url.URL
}

// RoundTrip modifies the Hostname of the incoming request to be directed to the
// test server.
func (tt *TestTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	tt.Log.
		WithField("component", "test").
		WithField("remote_addr", r.RemoteAddr).
		WithField("method", r.Method).
		WithField("url", r.URL).
		Info("Request")

	r.URL.Host = tt.tsURL.Host
	r.URL.Scheme = tt.tsURL.Scheme

	return tt.rt.RoundTrip(r)
}
