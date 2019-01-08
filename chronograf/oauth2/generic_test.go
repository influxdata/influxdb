package oauth2_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/influxdb/chronograf"
	"github.com/influxdata/influxdb/chronograf/oauth2"
)

func TestGenericGroup_withNotEmail(t *testing.T) {
	t.Parallel()

	response := struct {
		Email string `json:"not-email"`
	}{
		"martymcfly@pinheads.rok",
	}
	mockAPI := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			rw.WriteHeader(http.StatusNotFound)
			return
		}
		enc := json.NewEncoder(rw)

		rw.WriteHeader(http.StatusOK)
		_ = enc.Encode(response)
	}))
	defer mockAPI.Close()

	logger := &chronograf.NoopLogger{}
	prov := oauth2.Generic{
		Logger: logger,
		APIURL: mockAPI.URL,
		APIKey: "not-email",
	}
	tt, err := oauth2.NewTestTripper(logger, mockAPI, http.DefaultTransport)
	if err != nil {
		t.Fatal("Error initializing TestTripper: err:", err)
	}

	tc := &http.Client{
		Transport: tt,
	}

	got, err := prov.Group(tc)
	if err != nil {
		t.Fatal("Unexpected error while retrieiving PrincipalID: err:", err)
	}

	want := "pinheads.rok"
	if got != want {
		t.Fatal("Retrieved group was not as expected. Want:", want, "Got:", got)
	}
}

func TestGenericGroup_withEmail(t *testing.T) {
	t.Parallel()

	response := struct {
		Email string `json:"email"`
	}{
		"martymcfly@pinheads.rok",
	}
	mockAPI := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			rw.WriteHeader(http.StatusNotFound)
			return
		}
		enc := json.NewEncoder(rw)

		rw.WriteHeader(http.StatusOK)
		_ = enc.Encode(response)
	}))
	defer mockAPI.Close()

	logger := &chronograf.NoopLogger{}
	prov := oauth2.Generic{
		Logger: logger,
		APIURL: mockAPI.URL,
		APIKey: "email",
	}
	tt, err := oauth2.NewTestTripper(logger, mockAPI, http.DefaultTransport)
	if err != nil {
		t.Fatal("Error initializing TestTripper: err:", err)
	}

	tc := &http.Client{
		Transport: tt,
	}

	got, err := prov.Group(tc)
	if err != nil {
		t.Fatal("Unexpected error while retrieiving PrincipalID: err:", err)
	}

	want := "pinheads.rok"
	if got != want {
		t.Fatal("Retrieved group was not as expected. Want:", want, "Got:", got)
	}
}

func TestGenericPrincipalID(t *testing.T) {
	t.Parallel()

	response := struct {
		Email string `json:"email"`
	}{
		"martymcfly@pinheads.rok",
	}
	mockAPI := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			rw.WriteHeader(http.StatusNotFound)
			return
		}
		enc := json.NewEncoder(rw)

		rw.WriteHeader(http.StatusOK)
		_ = enc.Encode(response)
	}))
	defer mockAPI.Close()

	logger := &chronograf.NoopLogger{}
	prov := oauth2.Generic{
		Logger: logger,
		APIURL: mockAPI.URL,
		APIKey: "email",
	}
	tt, err := oauth2.NewTestTripper(logger, mockAPI, http.DefaultTransport)
	if err != nil {
		t.Fatal("Error initializing TestTripper: err:", err)
	}

	tc := &http.Client{
		Transport: tt,
	}

	got, err := prov.PrincipalID(tc)
	if err != nil {
		t.Fatal("Unexpected error while retrieiving PrincipalID: err:", err)
	}

	want := "martymcfly@pinheads.rok"
	if got != want {
		t.Fatal("Retrieved email was not as expected. Want:", want, "Got:", got)
	}
}

func TestGenericPrincipalIDDomain(t *testing.T) {
	t.Parallel()
	expectedEmail := []struct {
		Email    string `json:"email"`
		Primary  bool   `json:"primary"`
		Verified bool   `json:"verified"`
	}{
		{"martymcfly@pinheads.rok", true, false},
	}
	mockAPI := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			enc := json.NewEncoder(rw)
			rw.WriteHeader(http.StatusOK)
			_ = enc.Encode(struct{}{})
			return
		}
		if r.URL.Path == "/emails" {
			enc := json.NewEncoder(rw)
			rw.WriteHeader(http.StatusOK)
			_ = enc.Encode(expectedEmail)
			return
		}

		rw.WriteHeader(http.StatusNotFound)
	}))
	defer mockAPI.Close()

	logger := &chronograf.NoopLogger{}
	prov := oauth2.Generic{
		Logger:  logger,
		Domains: []string{"pinheads.rok"},
	}
	tt, err := oauth2.NewTestTripper(logger, mockAPI, http.DefaultTransport)
	if err != nil {
		t.Fatal("Error initializing TestTripper: err:", err)
	}

	tc := &http.Client{
		Transport: tt,
	}

	got, err := prov.PrincipalID(tc)
	if err != nil {
		t.Fatal("Unexpected error while retrieiving PrincipalID: err:", err)
	}
	want := "martymcfly@pinheads.rok"
	if got != want {
		t.Fatal("Retrieved email was not as expected. Want:", want, "Got:", got)
	}
}
