package server_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	clog "github.com/influxdata/chronograf/log"
	"github.com/influxdata/chronograf/oauth2"
	"github.com/influxdata/chronograf/server"
)

type MockAuthenticator struct {
	Principal   oauth2.Principal
	ValidateErr error
	Serialized  string
}

func (m *MockAuthenticator) Validate(context.Context, *http.Request) (oauth2.Principal, error) {
	return m.Principal, m.ValidateErr
}
func (m *MockAuthenticator) Authorize(ctx context.Context, w http.ResponseWriter, p oauth2.Principal) error {
	cookie := http.Cookie{}

	http.SetCookie(w, &cookie)
	return nil
}
func (m *MockAuthenticator) Expire(http.ResponseWriter) {}
func (m *MockAuthenticator) ValidAuthorization(ctx context.Context, serializedAuthorization string) (oauth2.Principal, error) {
	return oauth2.Principal{}, nil
}
func (m *MockAuthenticator) Serialize(context.Context, oauth2.Principal) (string, error) {
	return m.Serialized, nil
}

func TestAuthorizedToken(t *testing.T) {
	var tests = []struct {
		Desc        string
		Code        int
		Principal   oauth2.Principal
		ValidateErr error
		Expected    string
	}{
		{
			Desc:        "Error in validate",
			Code:        http.StatusForbidden,
			ValidateErr: errors.New("error"),
		},
		{
			Desc: "Authorized ok",
			Code: http.StatusOK,
			Principal: oauth2.Principal{
				Subject: "Principal Strickland",
			},
			Expected: "Principal Strickland",
		},
	}
	for _, test := range tests {
		// next is a sentinel StatusOK and
		// principal recorder.
		var principal oauth2.Principal
		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			principal = r.Context().Value(oauth2.PrincipalKey).(oauth2.Principal)
		})
		req, _ := http.NewRequest("GET", "", nil)
		w := httptest.NewRecorder()

		a := &MockAuthenticator{
			Principal:   test.Principal,
			ValidateErr: test.ValidateErr,
		}

		logger := clog.New(clog.DebugLevel)
		handler := server.AuthorizedToken(a, logger, next)
		handler.ServeHTTP(w, req)
		if w.Code != test.Code {
			t.Errorf("Status code expected: %d actual %d", test.Code, w.Code)
		} else if principal != test.Principal {
			t.Errorf("Principal mismatch expected: %s actual %s", test.Principal, principal)
		}
	}
}
