package oauth2_test

import (
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	clog "github.com/influxdata/chronograf/log"
	"github.com/influxdata/chronograf/oauth2"
)

var testTime time.Time = time.Date(1985, time.October, 25, 18, 0, 0, 0, time.UTC)

func Test_JWTMux_Logout_DeletesSessionCookie(t *testing.T) {
	t.Parallel()

	mp := &MockProvider{"biff@example.com"}
	jm := oauth2.NewJWTMux(mp, &YesManAuthenticator{}, clog.New(clog.ParseLevel("debug")))

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

func Test_JWTMux_Login_RedirectsToCorrectURL(t *testing.T) {
	t.Parallel()

	mp := &MockProvider{"biff@example.com"}
	jm := oauth2.NewJWTMux(mp, &YesManAuthenticator{}, clog.New(clog.ParseLevel("debug")))

	jm.Now = func() time.Time {
		return testTime
	}

	ts := httptest.NewServer(jm.Login())
	defer ts.Close()

	hc := http.Client{
		CheckRedirect: func(r *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

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
		t.Fatal("Expected state to be set but was", state)
	}
}
