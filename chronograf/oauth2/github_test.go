package oauth2_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/influxdb/chronograf"
	"github.com/influxdata/influxdb/chronograf/oauth2"
)

func TestGithubPrincipalID(t *testing.T) {
	t.Parallel()

	expected := []struct {
		Email    string `json:"email"`
		Primary  bool   `json:"primary"`
		Verified bool   `json:"verified"`
	}{
		{"mcfly@example.com", false, true},
		{"martymcspelledwrong@example.com", false, false},
		{"martymcfly@example.com", true, true},
	}
	mockAPI := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/user/emails" {
			rw.WriteHeader(http.StatusNotFound)
			return
		}
		enc := json.NewEncoder(rw)

		rw.WriteHeader(http.StatusOK)
		_ = enc.Encode(expected)
	}))
	defer mockAPI.Close()

	logger := &chronograf.NoopLogger{}
	prov := oauth2.Github{
		Logger: logger,
	}
	tt, err := oauth2.NewTestTripper(logger, mockAPI, http.DefaultTransport)
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

	if got, want := email, "martymcfly@example.com"; got != want {
		t.Fatal("Retrieved email was not as expected. Want:", want, "Got:", got)
	}
}

func TestGithubPrincipalIDOrganization(t *testing.T) {
	t.Parallel()

	expectedUser := []struct {
		Email    string `json:"email"`
		Primary  bool   `json:"primary"`
		Verified bool   `json:"verified"`
	}{
		{"martymcfly@example.com", true, true},
	}
	expectedOrg := []struct {
		Login string `json:"login"`
	}{
		{"Hill Valley Preservation Society"},
	}

	mockAPI := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/user/emails" {
			enc := json.NewEncoder(rw)
			rw.WriteHeader(http.StatusOK)
			_ = enc.Encode(expectedUser)
			return
		}
		if r.URL.Path == "/user/orgs" {
			enc := json.NewEncoder(rw)
			rw.WriteHeader(http.StatusOK)
			_ = enc.Encode(expectedOrg)
			return
		}
		rw.WriteHeader(http.StatusNotFound)
	}))
	defer mockAPI.Close()

	logger := &chronograf.NoopLogger{}
	prov := oauth2.Github{
		Logger: logger,
		Orgs:   []string{"Hill Valley Preservation Society"},
	}
	tt, err := oauth2.NewTestTripper(logger, mockAPI, http.DefaultTransport)
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

	if email != expectedUser[0].Email {
		t.Fatal("Retrieved email was not as expected. Want:", expectedUser[0].Email, "Got:", email)
	}
}
