package oauth2_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"time"

	goauth "golang.org/x/oauth2"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/oauth2"
)

var _ oauth2.Provider = &MockProvider{}

type MockProvider struct {
	Email string

	ProviderURL string
}

func (mp *MockProvider) Config() *goauth.Config {
	return &goauth.Config{
		RedirectURL:  "http://www.example.com",
		ClientID:     "4815162342",
		ClientSecret: "8675309",
		Endpoint: goauth.Endpoint{
			AuthURL:  mp.ProviderURL + "/oauth/auth",
			TokenURL: mp.ProviderURL + "/oauth/token",
		},
	}
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

func NewTestTripper(log chronograf.Logger, ts *httptest.Server, rt http.RoundTripper) (*TestTripper, error) {
	url, err := url.Parse(ts.URL)
	if err != nil {
		return nil, err
	}
	return &TestTripper{log, rt, url}, nil
}

type TestTripper struct {
	Log chronograf.Logger

	rt    http.RoundTripper
	tsURL *url.URL
}

// RoundTrip modifies the Hostname of the incoming request to be directed to the
// test server.
func (tt *TestTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	tt.Log.
		WithField("component", "test").
		WithField("remote_addr", r.RemoteAddr).
		WithField("method", r.Method).
		WithField("url", r.URL).
		Info("Request")

	r.URL.Host = tt.tsURL.Host
	r.URL.Scheme = tt.tsURL.Scheme

	return tt.rt.RoundTrip(r)
}
