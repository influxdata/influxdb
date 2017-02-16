package oauth2_test

import (
	"context"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	clog "github.com/influxdata/chronograf/log"
	"github.com/influxdata/chronograf/oauth2"
	goauth "golang.org/x/oauth2"
)

type MockProvider struct {
	Email string
}

func (mp *MockProvider) Config() *goauth.Config {
	return &goauth.Config{}
}

func (mp *MockProvider) ID() string {
	return "8675309"
}

func (mp *MockProvider) Name() string {
	return "mockly"
}

func (mp *MockProvider) PrincipalID(provider *http.Client) (string, error) {
	return mp.Email, nil
}

func (mp *MockProvider) Scopes() []string {
	return []string{}
}

func (mp *MockProvider) Secret() string {
	return "4815162342"
}

var _ oauth2.Authenticator = &YesManAuthenticator{}

type YesManAuthenticator struct{}

func (y *YesManAuthenticator) Authenticate(ctx context.Context, token string) (oauth2.Principal, error) {
	return oauth2.Principal{
		Subject: "biff@example.com",
		Issuer:  "Biff Tannen's Pleasure Paradise",
	}, nil
}

func (y *YesManAuthenticator) Token(ctx context.Context, p oauth2.Principal, t time.Duration) (string, error) {
	return "HELLO?!MCFLY?!ANYONEINTHERE?!", nil
}

var _ oauth2.Provider = &MockProvider{}

func Test_JWTMux_Logout_DeletesSessionCookie(t *testing.T) {
	t.Parallel()

	mp := &MockProvider{"biff@example.com"}
	jm := oauth2.NewJWTMux(mp, &YesManAuthenticator{}, clog.New(clog.ParseLevel("debug")))

	testTime := time.Date(1985, time.October, 25, 18, 0, 0, 0, time.UTC)

	jm.Now = func() time.Time {
		return testTime
	}

	ts := httptest.NewServer(jm.Logout())
	defer ts.Close()

	tsUrl, _ := url.Parse(ts.URL)

	jar, _ := cookiejar.New(nil)
	hc := http.Client{
		Jar: jar,
		CheckRedirect: func(r *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	hc.Jar.SetCookies(tsUrl, []*http.Cookie{
		&http.Cookie{
			Name:  oauth2.DefaultCookieName,
			Value: "",
		},
	})

	resp, err := hc.Get(ts.URL)
	if err != nil {
		t.Fatal("Error communicating with Logout() handler: err:", err)
	}

	if resp.StatusCode < 300 || resp.StatusCode >= 400 {
		t.Fatal("Expected to be redirected, but received status code", resp.StatusCode)
	}

	cookies := resp.Cookies()
	if len(cookies) != 1 {
		t.Fatal("Expected that cookie would be present but wasn't")
	}

	c := cookies[0]
	if c.Name != oauth2.DefaultCookieName || c.Expires != testTime.Add(-1*time.Hour) {
		t.Fatal("Expected cookie to be expired but wasn't")
	}
}
