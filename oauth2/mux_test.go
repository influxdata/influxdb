package oauth2

import (
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	clog "github.com/influxdata/chronograf/log"
)

var testTime = time.Date(1985, time.October, 25, 18, 0, 0, 0, time.UTC)

// setupMuxTest produces an http.Client and an httptest.Server configured to
// use a particular http.Handler selected from a AuthMux. As this selection is
// done during the setup process, this configuration is performed by providing
// a function, and returning the desired handler. Cleanup is still the
// responsibility of the test writer, so the httptest.Server's Close() method
// should be deferred.
func setupMuxTest(selector func(*AuthMux) http.Handler) (*http.Client, *httptest.Server, *httptest.Server) {
	provider := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusOK)
	}))

	now := func() time.Time {
		return testTime
	}
	mp := &MockProvider{
		Email:       "biff@example.com",
		ProviderURL: provider.URL,
		Orgs:        "",
	}
	mt := &YesManTokenizer{}
	auth := &cookie{
		Name:       DefaultCookieName,
		Lifespan:   1 * time.Hour,
		Inactivity: DefaultInactivityDuration,
		Now:        now,
		Tokens:     mt,
	}

	jm := NewAuthMux(mp, auth, mt, "", clog.New(clog.ParseLevel("debug")))
	ts := httptest.NewServer(selector(jm))
	jar, _ := cookiejar.New(nil)
	hc := http.Client{
		Jar: jar,
		CheckRedirect: func(r *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return &hc, ts, provider
}

// teardownMuxTest cleans up any resources created by setupMuxTest. This should
// be deferred in your test after setupMuxTest is called
func teardownMuxTest(hc *http.Client, backend *httptest.Server, provider *httptest.Server) {
	provider.Close()
	backend.Close()
}

func Test_AuthMux_Logout_DeletesSessionCookie(t *testing.T) {
	t.Parallel()

	hc, ts, prov := setupMuxTest(func(j *AuthMux) http.Handler {
		return j.Logout()
	})
	defer teardownMuxTest(hc, ts, prov)

	tsURL, _ := url.Parse(ts.URL)

	hc.Jar.SetCookies(tsURL, []*http.Cookie{
		&http.Cookie{
			Name:  DefaultCookieName,
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
	if c.Name != DefaultCookieName || c.Expires != testTime.Add(-1*time.Hour) {
		t.Fatal("Expected cookie to be expired but wasn't")
	}
}

func Test_AuthMux_Login_RedirectsToCorrectURL(t *testing.T) {
	t.Parallel()

	hc, ts, prov := setupMuxTest(func(j *AuthMux) http.Handler {
		return j.Login() // Use Login handler for httptest server.
	})
	defer teardownMuxTest(hc, ts, prov)

	resp, err := hc.Get(ts.URL)
	if err != nil {
		t.Fatal("Error communicating with Login() handler: err:", err)
	}

	// Ensure we were redirected
	if resp.StatusCode < 300 || resp.StatusCode >= 400 {
		t.Fatal("Expected to be redirected, but received status code", resp.StatusCode)
	}

	loc, err := resp.Location()
	if err != nil {
		t.Fatal("Expected a location to be redirected to, but wasn't present")
	}

	if state := loc.Query().Get("state"); state != "HELLO?!MCFLY?!ANYONEINTHERE?!" {
		t.Fatalf("Expected state to be %s set but was %s", "HELLO?!MCFLY?!ANYONEINTHERE?!", state)
	}
}

func Test_AuthMux_Callback_SetsCookie(t *testing.T) {
	hc, ts, prov := setupMuxTest(func(j *AuthMux) http.Handler {
		return j.Callback()
	})
	defer teardownMuxTest(hc, ts, prov)

	tsURL, _ := url.Parse(ts.URL)

	v := url.Values{
		"code":  {"4815162342"},
		"state": {"foobar"},
	}

	tsURL.RawQuery = v.Encode()

	resp, err := hc.Get(tsURL.String())
	if err != nil {
		t.Fatal("Error communicating with Callback() handler: err", err)
	}

	// Ensure we were redirected
	if resp.StatusCode < 300 || resp.StatusCode >= 400 {
		t.Fatal("Expected to be redirected, but received status code", resp.StatusCode)
	}

	// Check that cookie was set
	cookies := resp.Cookies()
	if count := len(cookies); count != 1 {
		t.Fatal("Expected exactly one cookie to be set but found", count)
	}

	c := cookies[0]

	if c.Name != DefaultCookieName {
		t.Fatal("Expected cookie to be named", DefaultCookieName, "but was", c.Name)
	}
}
