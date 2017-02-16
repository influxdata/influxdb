package oauth2_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/influxdata/chronograf/oauth2"
)

func NewTestTripper(ts *httptest.Server, rt http.RoundTripper) (*TestTripper, error) {
	url, err := url.Parse(ts.URL)
	if err != nil {
		return nil, err
	}
	return &TestTripper{rt, url}, nil
}

type TestTripper struct {
	rt    http.RoundTripper
	tsURL *url.URL
}

// RoundTrip modifies the Hostname of the incoming request to be directed to the
// test server.
func (tt *TestTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	r.URL.Host = tt.tsURL.Host
	r.URL.Scheme = tt.tsURL.Scheme

	return tt.rt.RoundTrip(r)
}

func Test_Heroku_PrincipalID_ExtractsEmailAddress(t *testing.T) {
	t.Parallel()

	expected := struct {
		Email string `json:"email"`
	}{
		"martymcfly@example.com",
	}

	mockAPI := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/account" {
			rw.WriteHeader(http.StatusNotFound)
			return
		}
		enc := json.NewEncoder(rw)

		rw.WriteHeader(http.StatusOK)
		_ = enc.Encode(expected)
	}))
	defer mockAPI.Close()

	prov := oauth2.Heroku{}
	tt, err := NewTestTripper(mockAPI, http.DefaultTransport)
	if err != nil {
		t.Fatal("Error initializing TestTripper: err:", err)
	}

	tc := &http.Client{
		Transport: tt,
	}

	email, err := prov.PrincipalID(tc)
	if err != nil {
		t.Fatal("Unexpected error while retrieiving PrincipalID: err:", err)
	}

	if email != expected.Email {
		t.Fatal("Retrieved email was not as expected. Want:", expected.Email, "Got:", email)
	}
}
