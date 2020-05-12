package feature

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

const (
	destBody = "hello from destination"
	srcBody  = "hello from source"
	flagKey  = "fancy-feature"
)

func TestHTTPProxy_Proxying(t *testing.T) {
	en := enabler{key: flagKey, state: true}
	logger := zaptest.NewLogger(t)
	resp, err := testHTTPProxy(logger, en)
	if err != nil {
		t.Error(err)
	}

	proxyFlag := resp.Header.Get("X-Platform-Proxy-Flag")
	if proxyFlag != flagKey {
		t.Error("X-Platform-Proxy-Flag header not populated")
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Error(err)
	}

	bodyStr := string(body)
	if bodyStr != destBody {
		t.Errorf("expected body of destination handler, but got: %q", bodyStr)
	}
}

func TestHTTPProxy_DefaultBehavior(t *testing.T) {
	en := enabler{key: flagKey, state: false}
	logger := zaptest.NewLogger(t)
	resp, err := testHTTPProxy(logger, en)
	if err != nil {
		t.Error(err)
	}

	proxyFlag := resp.Header.Get("X-Platform-Proxy-Flag")
	if proxyFlag != "" {
		t.Error("X-Platform-Proxy-Flag header populated")
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Error(err)
	}

	bodyStr := string(body)
	if bodyStr != srcBody {
		t.Errorf("expected body of source handler, but got: %q", bodyStr)
	}
}

func TestHTTPProxy_RequestHeader(t *testing.T) {
	h := func(w http.ResponseWriter, r *http.Request) {
		proxyFlag := r.Header.Get("X-Platform-Proxy-Flag")
		if proxyFlag != flagKey {
			t.Error("expected X-Proxy-Flag to contain feature flag key")
		}
	}

	s := httptest.NewServer(http.HandlerFunc(h))
	defer s.Close()

	sURL, err := url.Parse(s.URL)
	if err != nil {
		t.Error(err)
	}

	logger := zaptest.NewLogger(t)
	en := enabler{key: flagKey, state: true}
	proxy := NewHTTPProxy(sURL, logger, en)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "http://example.com/foo", nil)
	srcHandler(proxy)(w, r)
}

func testHTTPProxy(logger *zap.Logger, enabler ProxyEnabler) (*http.Response, error) {
	s := httptest.NewServer(http.HandlerFunc(destHandler))
	defer s.Close()

	sURL, err := url.Parse(s.URL)
	if err != nil {
		return nil, err
	}

	proxy := NewHTTPProxy(sURL, logger, enabler)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "http://example.com/foo", nil)
	srcHandler(proxy)(w, r)

	return w.Result(), nil
}

func destHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, destBody)
}

func srcHandler(proxy *HTTPProxy) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if proxy.Do(w, r) {
			return
		}
		fmt.Fprint(w, srcBody)
	}
}

type enabler struct {
	key   string
	state bool
}

func (e enabler) Key() string {
	return e.key
}

func (e enabler) Enabled(context.Context, ...Flagger) bool {
	return e.state
}
