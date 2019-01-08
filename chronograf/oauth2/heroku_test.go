package oauth2_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/influxdb/chronograf"
	"github.com/influxdata/influxdb/chronograf/oauth2"
)

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

	logger := &chronograf.NoopLogger{}
	prov := oauth2.Heroku{
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

	if email != expected.Email {
		t.Fatal("Retrieved email was not as expected. Want:", expected.Email, "Got:", email)
	}
}

func Test_Heroku_PrincipalID_RestrictsByOrganization(t *testing.T) {
	t.Parallel()

	expected := struct {
		Email               string            `json:"email"`
		DefaultOrganization map[string]string `json:"default_organization"`
	}{
		"martymcfly@example.com",
		map[string]string{
			"id":   "a85eac89-56cc-498e-9a89-d8f49f6aed71",
			"name": "hill-valley-preservation-society",
		},
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

	logger := &chronograf.NoopLogger{}
	prov := oauth2.Heroku{
		Logger:        logger,
		Organizations: []string{"enchantment-under-the-sea-dance-committee"},
	}

	tt, err := oauth2.NewTestTripper(logger, mockAPI, http.DefaultTransport)
	if err != nil {
		t.Fatal("Error initializing TestTripper: err:", err)
	}

	tc := &http.Client{
		Transport: tt,
	}

	_, err = prov.PrincipalID(tc)
	if err == nil {
		t.Fatal("Expected error while authenticating user with mismatched orgs, but received none")
	}
}
