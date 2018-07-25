package oauth2_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	clog "github.com/influxdata/platform/chronograf/log"
	"github.com/influxdata/platform/chronograf/oauth2"
)

func TestGooglePrincipalID(t *testing.T) {
	t.Parallel()

	expected := struct {
		Email string `json:"email"`
	}{
		"martymcfly@example.com",
	}
	mockAPI := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/oauth2/v2/userinfo" {
			rw.WriteHeader(http.StatusNotFound)
			return
		}

		enc := json.NewEncoder(rw)
		rw.WriteHeader(http.StatusOK)
		_ = enc.Encode(expected)
	}))
	defer mockAPI.Close()

	logger := clog.New(clog.ParseLevel("debug"))
	prov := oauth2.Google{
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

func TestGooglePrincipalIDDomain(t *testing.T) {
	t.Parallel()

	expectedUser := struct {
		Email string `json:"email"`
		Hd    string `json:"hd"`
	}{
		"martymcfly@example.com",
		"Hill Valley Preservation Society",
	}
	mockAPI := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/oauth2/v2/userinfo" {
			rw.WriteHeader(http.StatusNotFound)
			return
		}

		enc := json.NewEncoder(rw)
		rw.WriteHeader(http.StatusOK)
		_ = enc.Encode(expectedUser)
	}))
	defer mockAPI.Close()

	logger := clog.New(clog.ParseLevel("debug"))
	prov := oauth2.Google{
		Logger:  logger,
		Domains: []string{"Hill Valley Preservation Society"},
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

	if email != expectedUser.Email {
		t.Fatal("Retrieved email was not as expected. Want:", expectedUser.Email, "Got:", email)
	}
}
