package oauth2_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	clog "github.com/influxdata/chronograf/log"
	"github.com/influxdata/chronograf/oauth2"
)

var auth0Tests = []struct {
	name         string
	email        string
	organization string // empty string is "no organization"

	allowedUsers []string
	allowedOrgs  []string // empty disables organization checking
	shouldErr    bool
}{
	{
		"Simple, no orgs",
		"marty.mcfly@example.com",
		"",

		[]string{"marty.mcfly@example.com"},
		[]string{},
		false,
	},
	{
		"Unauthorized",
		"marty.mcfly@example.com",
		"",

		[]string{"doc.brown@example.com"},
		[]string{},
		true,
	},
	{
		"Success - member of an org",
		"marty.mcfly@example.com",
		"time-travelers",

		[]string{"marty.mcfly@example.com"},
		[]string{"time-travelers"},
		false,
	},
	{
		"Failure - not a member of an org",
		"marty.mcfly@example.com",
		"time-travelers",

		[]string{"marty.mcfly@example.com"},
		[]string{"biffs-gang"},
		true,
	},
}

func Test_Auth0_PrincipalID_RestrictsByOrganization(t *testing.T) {
	for _, test := range auth0Tests {
		t.Run(test.name, func(tt *testing.T) {
			tt.Parallel()
			expected := struct {
				Email        string `json:"email"`
				Organization string `json:"organization"`
			}{
				test.email,
				test.organization,
			}

			mockAPI := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/userinfo" {
					rw.WriteHeader(http.StatusNotFound)
					return
				}

				allowed := false
				for _, user := range test.allowedUsers {
					if test.email == user {
						allowed = true
					}
				}

				if !allowed {
					rw.WriteHeader(http.StatusUnauthorized)
					return
				}

				enc := json.NewEncoder(rw)

				rw.WriteHeader(http.StatusOK)
				_ = enc.Encode(expected)
			}))
			defer mockAPI.Close()

			logger := clog.New(clog.ParseLevel("debug"))
			prov, err := oauth2.NewAuth0(mockAPI.URL, "id", "secret", mockAPI.URL+"/callback", test.allowedOrgs, logger)
			if err != nil {
				tt.Fatal("Unexpected error instantiating Auth0 provider: err:", err)
			}

			tripper, err := oauth2.NewTestTripper(logger, mockAPI, http.DefaultTransport)
			if err != nil {
				tt.Fatal("Error initializing TestTripper: err:", err)
			}

			tc := &http.Client{
				Transport: tripper,
			}

			var email string
			email, err = prov.PrincipalID(tc)
			if !test.shouldErr {
				if err != nil {
					tt.Fatal(test.name, ": Unexpected error while attempting to authenticate: err:", err)
				}

				if email != test.email {
					tt.Fatal(test.name, ": email mismatch. Got", email, "want:", test.email)
				}
			}

			if err == nil && test.shouldErr {
				tt.Fatal(test.name, ": Expected error while attempting to authenticate but received none")
			}
		})
	}
}

func Test_Auth0_ErrsWithBadDomain(t *testing.T) {
	t.Parallel()

	logger := clog.New(clog.ParseLevel("debug"))
	_, err := oauth2.NewAuth0("!!@#$!@$%@#$%", "id", "secret", "http://example.com", []string{}, logger)
	if err == nil {
		t.Fatal("Expected err with bad domain but received none")
	}
}
