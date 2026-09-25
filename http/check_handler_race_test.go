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

	// A start gate cannot support the assertion at the end of this test.
	// Released together, these goroutines may still run one at a time: nothing
	// in their bodies blocks, so at GOMAXPROCS=1 each runs to completion before
	// the next is scheduled, and the test would pass having exercised no
	// concurrent access at all. A barrier makes the overlap structural instead
	// of hoped for -- no goroutine proceeds until every one of them has arrived,
	// so all of them are live at once on any scheduler.
	const total = requesters + wirers
	var arrived sync.WaitGroup
	arrived.Add(total)
	proceed := make(chan struct{})
	go func() {
		arrived.Wait()
		close(proceed)
	}()

	var concurrency, maxConcurrency atomic.Int64

	// enter counts this goroutine in, publishes the new high-water mark, then
	// waits for the rest. The max update retries rather than making a single
	// CompareAndSwap attempt: a CAS that loses its race has not necessarily lost
	// to a larger value, so abandoning the write can discard the highest count
	// -- which is the one being asserted on.
	enter := func() {
		c := concurrency.Add(1)
		for {
			old := maxConcurrency.Load()
			if c <= old || maxConcurrency.CompareAndSwap(old, c) {
				break
			}
		}
		arrived.Done()
		<-proceed
	}

	var wg sync.WaitGroup

	for i := range requesters {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			enter()
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
			defer wg.Done()
			enter()
			defer concurrency.Add(-1)

			h.SetCredentialResolver(resolvers[idx])
			h.SetAuthDependencyChecker(staticChecker{name: "kv", resp: check.NamedPass("kv")})
			h.SetHandler(http.NotFoundHandler())
			h.AddNamedHealthCheck(failingChecker{name: "late", message: "detail"})
		}(i)
	}

	wg.Wait()

	// The claim every assertion above rests on. No goroutine can decrement
	// before all of them have incremented -- the barrier stands between the two
	// -- so the high-water mark is exactly the number of goroutines, on any
	// scheduler and at any GOMAXPROCS. Anything less means they were serialized
	// and the wiring never actually raced the serving.
	assert.Equal(t, int64(total), maxConcurrency.Load(),
		"every goroutine must be live at once, or this test proves nothing about concurrent access")
}

// TestHealthReadyHandler_SetCredentialResolver_NilIgnored pins that a nil
// resolver cannot displace an installed one -- the same guard SetHandler has,
// for the same reason: a published nil would panic every subsequent request.
func TestHealthReadyHandler_SetCredentialResolver_NilIgnored(t *testing.T) {
	h, resolver := authHandler(t, platform.OperPermissions())
	h.AddNamedHealthCheck(failingChecker{name: "kv", message: "secret detail"})

	h.SetCredentialResolver(nil)
	h.SetAuthDependencyChecker(nil)

	res := doAuthRequest(t, h, http.MethodGet, "/health")
	defer closeBody(t, res)

	assert.Equal(t, int64(1), resolver.called.Load(),
		"expected the original resolver to still be installed")
	assert.Contains(t, decodeBody(t, res), "message",
		"expected full detail from the still-installed resolver")
}
