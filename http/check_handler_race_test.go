package http

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/check"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
)

// TestHealthReadyHandler_ConcurrentAuthWiring exercises the fields the launcher
// publishes while the server is already serving: the credential resolver is
// installed once the authorization service exists and replaced again once
// sessions are wired, the auth dependency checker lands with the KV store, and
// the delegate lands last. All of that races live probe traffic in production,
// so it must race it here too.
//
// The invariant asserted throughout is that the status code never depends on
// wiring state: both endpoints have a failing check registered before the gate
// opens, so every response must be 503 no matter which goroutine won.
func TestHealthReadyHandler_ConcurrentAuthWiring(t *testing.T) {
	const (
		requesters = 32
		wirers     = 8
	)

	h := NewHealthReadyHandler(zaptest.NewLogger(t))
	h.SetHealthAuthRequired(true)
	h.AddNamedHealthCheck(failingChecker{name: "kv", message: "detail"})
	h.AddNamedReadyCheck(failingChecker{name: "engine", message: "detail"})

	// Build everything the goroutines publish before the gate opens, so no
	// allocation happens inside the measured window.
	resolvers := make([]*stubResolver, wirers)
	for i := range resolvers {
		resolvers[i] = &stubResolver{auth: mock.NewMockAuthorizer(false, platform.OperPermissions())}
	}

	var startMu sync.RWMutex
	var concurrency, maxConcurrency atomic.Int64

	record := func() {
		c := concurrency.Add(1)
		if old := maxConcurrency.Load(); c > old {
			maxConcurrency.CompareAndSwap(old, c)
		}
	}

	var wg sync.WaitGroup
	startMu.Lock()

	for i := range requesters {
		wg.Add(1)
		go func(idx int) {
			startMu.RLock()
			defer startMu.RUnlock()
			defer wg.Done()
			record()
			defer concurrency.Add(-1)

			path := "/health"
			if idx%2 == 1 {
				path = "/ready"
			}
			req := httptest.NewRequest(http.MethodGet, path, nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusServiceUnavailable {
				t.Errorf("%s: expected 503 regardless of wiring state, got %d", path, rec.Code)
			}
		}(i)
	}

	for i := range wirers {
		wg.Add(1)
		go func(idx int) {
			startMu.RLock()
			defer startMu.RUnlock()
			defer wg.Done()
			record()
			defer concurrency.Add(-1)

			h.SetCredentialResolver(resolvers[idx])
			h.SetAuthDependencyChecker(staticChecker{name: "kv", resp: check.NamedPass("kv")})
			h.SetHandler(http.NotFoundHandler())
			h.AddNamedHealthCheck(failingChecker{name: "late", message: "detail"})
		}(i)
	}

	startMu.Unlock() // release to start all goroutines simultaneously
	wg.Wait()

	t.Logf("max concurrency: %d", maxConcurrency.Load())
}

// TestHealthReadyHandler_SetCredentialResolver_NilIgnored pins that a nil
// resolver cannot displace an installed one -- the same guard SetHandler has,
// for the same reason: a published nil would panic every subsequent request.
func TestHealthReadyHandler_SetCredentialResolver_NilIgnored(t *testing.T) {
	h, resolver := authHandler(t, platform.OperPermissions())
	h.AddNamedHealthCheck(failingChecker{name: "kv", message: "secret detail"})

	h.SetCredentialResolver(nil)
	h.SetAuthDependencyChecker(nil)

	res := doRequest(t, h, http.MethodGet, "/health")
	defer closeBody(t, res)

	assert.Equal(t, int64(1), resolver.called.Load(),
		"expected the original resolver to still be installed")
	assert.Contains(t, decodeBody(t, res), "message",
		"expected full detail from the still-installed resolver")
}
