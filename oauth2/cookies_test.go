package oauth2

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type MockTokenizer struct {
	Principal Principal
	ValidErr  error
	Token     Token
	CreateErr error
}

func (m *MockTokenizer) ValidPrincipal(ctx context.Context, token Token, duration time.Duration) (Principal, error) {
	return m.Principal, m.ValidErr
}

func (m *MockTokenizer) Create(ctx context.Context, p Principal, t time.Duration) (Token, error) {
	return m.Token, m.CreateErr
}

func TestCookieAuthorize(t *testing.T) {
	var test = []struct {
		Desc      string
		Value     string
		Expected  string
		Err       error
		CreateErr error
	}{
		{
			Desc:      "Unable to create token",
			Err:       ErrAuthentication,
			CreateErr: ErrAuthentication,
		},
		{
			Desc:     "Cookie token extracted",
			Value:    "reallyimportant",
			Expected: "reallyimportant",
			Err:      nil,
		},
	}
	for _, test := range test {
		cook := cookie{
			Duration: 1 * time.Second,
			Now: func() time.Time {
				return time.Unix(0, 0)
			},
			Tokens: &MockTokenizer{
				Token:     Token(test.Value),
				CreateErr: test.CreateErr,
			},
		}
		principal := Principal{}
		w := httptest.NewRecorder()
		err := cook.Authorize(context.Background(), w, principal)
		if err != test.Err {
			t.Fatalf("Cookie extract error; expected %v  actual %v", test.Err, err)
		}
		if test.Err != nil {
			continue
		}

		cookies := w.HeaderMap["Set-Cookie"]

		if len(cookies) == 0 {
			t.Fatal("Expected some cookies but got zero")
		}
		log.Printf("%s", cookies[0])
		if !strings.Contains(cookies[0], fmt.Sprintf("%s=%s", DefaultCookieName, test.Expected)) {
			t.Errorf("Token extract error; expected %v  actual %v", test.Expected, principal.Subject)
		}
	}
}

func TestCookieValidate(t *testing.T) {
	var test = []struct {
		Desc     string
		Name     string
		Value    string
		Lookup   string
		Expected string
		Err      error
		ValidErr error
	}{
		{
			Desc:     "No cookie of this name",
			Name:     "Auth",
			Value:    "reallyimportant",
			Lookup:   "Doesntexist",
			Expected: "",
			Err:      ErrAuthentication,
		},
		{
			Desc:     "Unable to create token",
			Name:     "Auth",
			Lookup:   "Auth",
			Err:      ErrAuthentication,
			ValidErr: ErrAuthentication,
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

		cook := cookie{
			Name:     test.Lookup,
			Duration: 1 * time.Second,
			Now: func() time.Time {
				return time.Unix(0, 0)
			},
			Tokens: &MockTokenizer{
				Principal: Principal{
					Subject: test.Value,
				},
				ValidErr: test.ValidErr,
			},
		}
		principal, err := cook.Validate(context.Background(), req)
		if err != test.Err {
			t.Errorf("Cookie extract error; expected %v  actual %v", test.Err, err)
		}

		if principal.Subject != test.Expected {
			t.Errorf("Token extract error; expected %v  actual %v", test.Expected, principal.Subject)
		}
	}
}

func TestNewCookieJWT(t *testing.T) {
	auth := NewCookieJWT("secret", time.Second)
	if _, ok := auth.(*cookie); !ok {
		t.Errorf("NewCookieJWT() did not create cookie Authenticator")
	}
}
