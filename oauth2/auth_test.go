package oauth2_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	clog "github.com/influxdata/chronograf/log"
	"github.com/influxdata/chronograf/oauth2"
)

func TestCookieExtractor(t *testing.T) {
	var test = []struct {
		Desc     string
		Name     string
		Value    string
		Lookup   string
		Expected string
		Err      error
	}{
		{
			Desc:     "No cookie of this name",
			Name:     "Auth",
			Value:    "reallyimportant",
			Lookup:   "Doesntexist",
			Expected: "",
			Err:      oauth2.ErrAuthentication,
		},
		{
			Desc:     "Cookie token extracted",
			Name:     "Auth",
			Value:    "reallyimportant",
			Lookup:   "Auth",
			Expected: "reallyimportant",
			Err:      nil,
		},
	}
	for _, test := range test {
		req, _ := http.NewRequest("", "http://howdy.com", nil)
		req.AddCookie(&http.Cookie{
			Name:  test.Name,
			Value: test.Value,
		})

		var e oauth2.TokenExtractor = &oauth2.CookieExtractor{
			Name: test.Lookup,
		}
		actual, err := e.Extract(req)
		if err != test.Err {
			t.Errorf("Cookie extract error; expected %v  actual %v", test.Err, err)
		}

		if actual != test.Expected {
			t.Errorf("Token extract error; expected %v  actual %v", test.Expected, actual)
		}
	}
}

func TestBearerExtractor(t *testing.T) {
	var test = []struct {
		Desc     string
		Header   string
		Value    string
		Lookup   string
		Expected string
		Err      error
	}{
		{
			Desc:     "No header of this name",
			Header:   "Doesntexist",
			Value:    "reallyimportant",
			Expected: "",
			Err:      oauth2.ErrAuthentication,
		},
		{
			Desc:     "Auth header doesn't have Bearer",
			Header:   "Authorization",
			Value:    "Bad Value",
			Expected: "",
			Err:      oauth2.ErrAuthentication,
		},
		{
			Desc:     "Auth header doesn't have Bearer token",
			Header:   "Authorization",
			Value:    "Bearer",
			Expected: "",
			Err:      oauth2.ErrAuthentication,
		},
		{
			Desc:     "Authorization Bearer token success",
			Header:   "Authorization",
			Value:    "Bearer howdy",
			Expected: "howdy",
			Err:      nil,
		},
	}
	for _, test := range test {
		req, _ := http.NewRequest("", "http://howdy.com", nil)
		req.Header.Add(test.Header, test.Value)

		var e oauth2.TokenExtractor = &oauth2.BearerExtractor{}
		actual, err := e.Extract(req)
		if err != test.Err {
			t.Errorf("Bearer extract error; expected %v  actual %v", test.Err, err)
		}

		if actual != test.Expected {
			t.Errorf("Token extract error; expected %v  actual %v", test.Expected, actual)
		}
	}
}

type MockExtractor struct {
	Err error
}

func (m *MockExtractor) Extract(*http.Request) (string, error) {
	return "", m.Err
}

type MockAuthenticator struct {
	Principal oauth2.Principal
	Err       error
}

func (m *MockAuthenticator) Authenticate(context.Context, string) (oauth2.Principal, error) {
	return m.Principal, m.Err
}

func (m *MockAuthenticator) Token(context.Context, oauth2.Principal, time.Duration) (string, error) {
	return "", m.Err
}

func TestAuthorizedToken(t *testing.T) {
	var tests = []struct {
		Desc         string
		Code         int
		Principal    oauth2.Principal
		ExtractorErr error
		AuthErr      error
		Expected     string
	}{
		{
			Desc:         "Error in extractor",
			Code:         http.StatusUnauthorized,
			ExtractorErr: errors.New("error"),
		},
		{
			Desc:    "Error in extractor",
			Code:    http.StatusUnauthorized,
			AuthErr: errors.New("error"),
		},
		{
			Desc:      "Authorized ok",
			Code:      http.StatusOK,
			Principal: "Principal Strickland",
			Expected:  "Principal Strickland",
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

		e := &MockExtractor{
			Err: test.ExtractorErr,
		}
		a := &MockAuthenticator{
			Err:       test.AuthErr,
			Principal: test.Principal,
		}

		logger := clog.New(clog.DebugLevel)
		handler := oauth2.AuthorizedToken(a, e, logger, next)
		handler.ServeHTTP(w, req)
		if w.Code != test.Code {
			t.Errorf("Status code expected: %d actual %d", test.Code, w.Code)
		} else if principal != test.Principal {
			t.Errorf("Principal mismatch expected: %s actual %s", test.Principal, principal)
		}
	}
}
