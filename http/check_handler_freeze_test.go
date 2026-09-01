package http

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/check"
	"github.com/influxdata/influxdb/v2/kit/check/checktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// captureCheckDocuments fetches /health and /ready and returns their decoded
// bodies with the values that move on their own replaced by sentinels, so two
// captures taken either side of the freeze can be compared for equality. See
// checktest.Normalize for which values those are and why each is masked.
func captureCheckDocuments(t *testing.T, h http.Handler) (health, ready map[string]any) {
	t.Helper()

	healthRes := doRequest(t, h, http.MethodGet, "/health")
	defer closeBody(t, healthRes)
	health = normalizedBody(t, healthRes)

	readyRes := doRequest(t, h, http.MethodGet, "/ready")
	defer closeBody(t, readyRes)
	ready = normalizedBody(t, readyRes)

	return health, ready
}

// normalizedBody reads a rendered check document off res and normalizes it.
func normalizedBody(t *testing.T, res *http.Response) map[string]any {
	t.Helper()
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	return checktest.NormalizeJSON(t, body)
}

// TestHealthReadyHandler_FreezeChecks_BodiesUnchanged is the wire-compatibility
// pin for the freeze: a frozen handler serves the same document it served a
// moment earlier, with the same status codes. Freezing is meant to be invisible
// to a scraper except that the answer stops moving.
func TestHealthReadyHandler_FreezeChecks_BodiesUnchanged(t *testing.T) {
	h := NewHealthReadyHandler(zaptest.NewLogger(t))
	h.AddNamedHealthCheck(failingChecker{name: "engine", message: "failed to open engine: not a directory"})
	h.AddNamedHealthCheck(check.Named("bolt", check.CheckerFunc(func(context.Context) check.Response {
		return check.NamedPass("bolt")
	})))
	h.AddNamedReadyCheck(check.NewReadyGate("bolt"))
	h.AddNamedReadyCheck(failingChecker{name: "engine", message: "failed to open engine: not a directory"})

	beforeHealth, beforeReady := captureCheckDocuments(t, h)

	h.FreezeChecks(context.Background())

	afterHealth, afterReady := captureCheckDocuments(t, h)
	require.Equal(t, beforeHealth, afterHealth)
	require.Equal(t, beforeReady, afterReady)

	// And the codes, which the comparison above does not cover.
	healthRes := doRequest(t, h, http.MethodGet, "/health")
	defer closeBody(t, healthRes)
	require.Equal(t, http.StatusServiceUnavailable, healthRes.StatusCode)
	readyRes := doRequest(t, h, http.MethodGet, "/ready")
	defer closeBody(t, readyRes)
	require.Equal(t, http.StatusServiceUnavailable, readyRes.StatusCode)
}

// TestHealthReadyHandler_FreezeChecks_PinsTornDownSubsystem is the reason the
// freeze exists. After the freeze the stores are closed, and their checks start
// reporting that teardown as a failure of their own; because failures sort
// first and /health's top-level message is the first of them, an alphabetically
// earlier subsystem would otherwise mask the one that actually failed.
func TestHealthReadyHandler_FreezeChecks_PinsTornDownSubsystem(t *testing.T) {
	h := NewHealthReadyHandler(zaptest.NewLogger(t))

	// bolt sorts ahead of engine, so once it starts failing it owns the
	// top-level message.
	var boltClosed bool
	h.AddNamedHealthCheck(check.Named("bolt", check.CheckerFunc(func(context.Context) check.Response {
		if boltClosed {
			return check.NamedFail("bolt", "stale: last probe 6s ago (threshold 5s)")
		}
		return check.NamedPass("bolt")
	})))
	h.AddNamedHealthCheck(failingChecker{name: "engine", message: "failed to open engine: not a directory"})

	h.FreezeChecks(context.Background())
	boltClosed = true

	res := doRequest(t, h, http.MethodGet, "/health")
	defer closeBody(t, res)
	require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)

	var got testHealthBody
	require.NoError(t, json.NewDecoder(res.Body).Decode(&got))
	require.Equal(t, "failed to open engine: not a directory", got.Message,
		"the teardown of bolt masked the failure the window exists to publish")
	require.Len(t, got.Checks, 2)
	for _, c := range got.Checks {
		if c.Name() == "bolt" {
			require.Equal(t, check.StatusPass, c.Status())
		}
	}
}

// TestHealthReadyHandler_FreezeChecks_RetiresAuthDependency covers decision 6.
// The store credentials resolve against is closed by the teardown that follows
// the freeze, so the auth dependency checker is pinned to fail and the window
// answers detailNames from its first request rather than degrading from
// detailNone as the store's own probe ages out.
func TestHealthReadyHandler_FreezeChecks_RetiresAuthDependency(t *testing.T) {
	t.Run("with an auth dependency installed", func(t *testing.T) {
		h, resolver := authHandler(t, platform.OperPermissions())
		h.SetAuthDependencyChecker(staticChecker{name: "bolt", resp: check.NamedPass("bolt")})
		h.AddNamedHealthCheck(failingChecker{name: "engine", message: "failed to open engine: not a directory"})

		// Before the freeze an operator reads everything.
		res := doAuthRequest(t, h, http.MethodGet, "/health")
		got := decodeBody(t, res)
		closeBody(t, res)
		require.Equal(t, "failed to open engine: not a directory", got["message"])
		require.Contains(t, got, "version", "an operator should have had detailFull")
		require.Positive(t, resolver.called.Load())

		h.FreezeChecks(context.Background())

		// After it the same operator reads names and statuses only, and the
		// credential is not resolved at all -- there is nothing left to resolve
		// it against.
		calledBefore := resolver.called.Load()
		res = doAuthRequest(t, h, http.MethodGet, "/health")
		got = decodeBody(t, res)
		closeBody(t, res)
		require.NotContains(t, got, "version")
		require.NotContains(t, got, "message",
			"detailNames withholds messages, which is where startup error text lives")
		checks, ok := got["checks"].([]any)
		require.True(t, ok, "names and statuses must survive: %v", got)
		require.Len(t, checks, 1)
		entry, ok := checks[0].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "engine", entry["name"])
		assert.Equal(t, "fail", entry["status"])
		assert.NotContains(t, entry, "message")
		require.Equal(t, calledBefore, resolver.called.Load(),
			"the credential was resolved against a store that is being closed")
	})

	t.Run("with no auth dependency installed", func(t *testing.T) {
		// Nothing was gated on a store's liveness, so nothing is retired and an
		// operator keeps the detail they are entitled to. No production
		// configuration reaches this: the launcher skips
		// SetAuthDependencyChecker only for the in-memory KV store, which is
		// test-only. Pinned anyway, because the branch exists.
		h, _ := authHandler(t, platform.OperPermissions())
		h.AddNamedHealthCheck(failingChecker{name: "engine", message: "failed to open engine: not a directory"})

		h.FreezeChecks(context.Background())

		res := doAuthRequest(t, h, http.MethodGet, "/health")
		defer closeBody(t, res)
		got := decodeBody(t, res)
		require.Equal(t, "failed to open engine: not a directory", got["message"])
		require.Contains(t, got, "version")
	})
}
