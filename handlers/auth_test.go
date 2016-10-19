package handlers_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/influxdata/mrfusion"
	"github.com/influxdata/mrfusion/handlers"
	fusionlog "github.com/influxdata/mrfusion/log"
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
			Err:      mrfusion.ErrAuthentication,
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

		var e mrfusion.TokenExtractor = &handlers.CookieExtractor{
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
			Err:      mrfusion.ErrAuthentication,
		},
		{
			Desc:     "Auth header doesn't have Bearer",
			Header:   "Authorization",
			Value:    "Bad Value",
			Expected: "",
			Err:      mrfusion.ErrAuthentication,
		},
		{
			Desc:     "Auth header doesn't have Bearer token",
			Header:   "Authorization",
			Value:    "Bearer",
			Expected: "",
			Err:      mrfusion.ErrAuthentication,
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

		var e mrfusion.TokenExtractor = &handlers.BearerExtractor{}
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
	Principal mrfusion.Principal
	Err       error
}

func (m *MockAuthenticator) Authenticate(context.Context, string) (mrfusion.Principal, error) {
	return m.Principal, m.Err
}

func (m *MockAuthenticator) Token(context.Context, mrfusion.Principal, time.Duration) (string, error) {
	return "", m.Err
}

func TestAuthorizedToken(t *testing.T) {
	var tests = []struct {
		Desc         string
		Code         int
		Principal    mrfusion.Principal
		ExtractorErr error
		AuthErr      error
		Expected     string
	}{
		{
			Desc:         "Error in extractor",
			Code:         http.StatusTemporaryRedirect,
			ExtractorErr: errors.New("error"),
		},
		{
			Desc:    "Error in extractor",
			Code:    http.StatusTemporaryRedirect,
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
		var principal mrfusion.Principal
		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			principal = r.Context().Value(mrfusion.PrincipalKey).(mrfusion.Principal)
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

		logger := fusionlog.New()
		handler := handlers.AuthorizedToken(a, e, "/login", logger, next)
		handler.ServeHTTP(w, req)
		if w.Code != test.Code {
			t.Errorf("Status code expected: %d actual %d", test.Code, w.Code)
		} else if principal != test.Principal {
			t.Errorf("Principal mismatch expected: %s actual %s", test.Principal, principal)
		}
	}
}
