package oauth2

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"time"

	goauth "golang.org/x/oauth2"

	"github.com/influxdata/chronograf"
)

var _ Provider = &MockProvider{}

type MockProvider struct {
	Email string
	Orgs  string

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

func (mp *MockProvider) Group(provider *http.Client) (string, error) {
	return mp.Orgs, nil
}

func (mp *MockProvider) Scopes() []string {
	return []string{}
}

func (mp *MockProvider) Secret() string {
	return "4815162342"
}

var _ Tokenizer = &YesManTokenizer{}

type YesManTokenizer struct{}

func (y *YesManTokenizer) ValidPrincipal(ctx context.Context, token Token, duration time.Duration) (Principal, error) {
	return Principal{
		Subject: "biff@example.com",
		Issuer:  "Biff Tannen's Pleasure Paradise",
	}, nil
}

func (y *YesManTokenizer) Create(ctx context.Context, p Principal) (Token, error) {
	return Token("HELLO?!MCFLY?!ANYONEINTHERE?!"), nil
}

func (y *YesManTokenizer) ExtendedPrincipal(ctx context.Context, p Principal, ext time.Duration) (Principal, error) {
	return p, nil
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
