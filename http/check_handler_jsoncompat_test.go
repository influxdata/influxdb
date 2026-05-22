package http

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/check"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// TestCheckHandler_WireFormat_PassRoundtrip pins the byte-level shape of
// /health when every check passes: the legacy field names must be
// present and "checks" must be an array of objects, each with name,
// status, and either no message or a string message. This guards the
// Response-interface refactor against accidentally breaking external
// /health clients.
func TestCheckHandler_WireFormat_PassRoundtrip(t *testing.T) {
	h := NewHealthReadyHandler(zaptest.NewLogger(t))

	// One BasicResponse-backed checker, one FreshnessResponse-backed
	// checker. Both should render through the same wire shape.
	h.AddHealthCheck(check.Named("static", check.CheckerFunc(func(context.Context) check.Response {
		return check.NamedPass("static")
	})))

	fresh := check.NewFreshnessResponse("fresh", time.Hour)
	fresh.Update(check.Pass())
	h.AddHealthCheck(staticChecker{name: "fresh", resp: fresh})

	res := doRequest(t, h, http.MethodGet, "/health")
	defer closeBody(t, res)
	require.Equal(t, http.StatusOK, res.StatusCode)

	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)

	var generic map[string]any
	require.NoError(t, json.Unmarshal(body, &generic))

	require.Equal(t, "influxdb", generic["name"])
	require.Equal(t, "pass", generic["status"])
	require.Equal(t, "healthy", generic["message"])
	require.NotNil(t, generic["checks"])

	rawChecks, ok := generic["checks"].([]any)
	require.True(t, ok, "checks must be an array, got %T", generic["checks"])
	require.Len(t, rawChecks, 2)

	for _, c := range rawChecks {
		obj, ok := c.(map[string]any)
		require.True(t, ok, "each check must be a JSON object")
		require.Contains(t, obj, "name")
		require.Contains(t, obj, "status")
		// message and checks fields are omitempty on the wire.
		if m, present := obj["message"]; present {
			_, isString := m.(string)
			require.True(t, isString, "message must be a string when present")
		}
	}
}

// TestCheckHandler_WireFormat_FreshnessFailMessage pins the "stale"
// message format that a FreshnessResponse renders when its snapshot
// has aged past staleness. /health must surface that as the top-level
// message.
func TestCheckHandler_WireFormat_FreshnessFailMessage(t *testing.T) {
	h := NewHealthReadyHandler(zaptest.NewLogger(t))

	// Construct a freshness wrapper with an already-aged snapshot.
	// Trick: use a tiny staleness and sleep past it.
	fresh := check.NewFreshnessResponse("svc", 10*time.Millisecond)
	fresh.Update(check.Pass())
	time.Sleep(30 * time.Millisecond)
	h.AddHealthCheck(staticChecker{name: "svc", resp: fresh})

	res := doRequest(t, h, http.MethodGet, "/health")
	defer closeBody(t, res)
	require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)

	var got testHealthBody
	require.NoError(t, json.NewDecoder(res.Body).Decode(&got))
	require.Equal(t, "fail", got.Status)
	require.Regexp(t, `^stale: last probe .* ago \(threshold 10ms\)$`, got.Message)
	require.Len(t, got.Checks, 1)
	require.Equal(t, "svc", got.Checks[0].Name())
	require.Equal(t, check.StatusFail, got.Checks[0].Status())
}

// staticChecker exposes a fixed Response as a NamedChecker. Used to
// inject a *FreshnessResponse into the handler without going through
// the namedChecker renaming wrapper.
type staticChecker struct {
	name string
	resp check.Response
}

func (s staticChecker) CheckName() string                    { return s.name }
func (s staticChecker) Check(context.Context) check.Response { return s.resp }

// TestCheckHandler_WireFormat_FullDocumentPin asserts the entire JSON
// response — every key, every value, and the surrounding tree shape —
// for /health, /ready, and the pre-delegate 503 fallback. The expected
// shape is spelled as a literal map[string]any whose keys are the wire
// JSON keys exactly as a client would see them, NOT the Go struct
// field names. The round-trip tests above decode into production-shaped
// structs and so silently absorb JSON-tag renames; this one does not.
//
// Fragile by design: if you arrived here because this test failed, you
// almost certainly changed the API contract. Update the expected
// document below to match and announce the change in release notes —
// renaming or restructuring a wire field is a breaking change to any
// /health or /ready consumer (k8s probes, dashboards, scripts).
//
// Dynamic fields (started, up, version, commit) are validated for
// type/format and then replaced with sentinels so the rest of the tree
// can be compared against constants.
func TestCheckHandler_WireFormat_FullDocumentPin(t *testing.T) {
	const (
		sentinelStarted = "<started>"
		sentinelUp      = "<up>"
		sentinelVersion = "<version>"
		sentinelCommit  = "<commit>"
	)

	decodeTree := func(t *testing.T, body io.Reader) map[string]any {
		t.Helper()
		var m map[string]any
		require.NoError(t, json.NewDecoder(body).Decode(&m))
		return m
	}

	info := platform.GetBuildInfo()

	t.Run("health 200 with one passing named check", func(t *testing.T) {
		h := NewHealthReadyHandler(zaptest.NewLogger(t))
		h.AddNamedHealthCheck(check.Named("alpha", check.CheckerFunc(func(context.Context) check.Response {
			return check.NamedPass("alpha")
		})))

		res := doRequest(t, h, http.MethodGet, "/health")
		defer closeBody(t, res)
		require.Equal(t, http.StatusOK, res.StatusCode)
		require.Equal(t, "application/json; charset=utf-8", res.Header.Get("Content-Type"))
		require.Equal(t, "OSS", res.Header.Get("X-Influxdb-Build"))
		require.Equal(t, info.Version, res.Header.Get("X-Influxdb-Version"))

		got := decodeTree(t, res.Body)
		require.Equal(t, info.Version, got["version"])
		require.Equal(t, info.Commit, got["commit"])
		got["version"] = sentinelVersion
		got["commit"] = sentinelCommit

		require.Equal(t, map[string]any{
			"name":    "influxdb",
			"status":  "pass",
			"message": "healthy",
			"checks": []any{
				map[string]any{
					"name":   "alpha",
					"status": "pass",
				},
			},
			"version": sentinelVersion,
			"commit":  sentinelCommit,
		}, got)
	})

	t.Run("health 200 with no checks registered", func(t *testing.T) {
		h := NewHealthReadyHandler(zaptest.NewLogger(t))

		res := doRequest(t, h, http.MethodGet, "/health")
		defer closeBody(t, res)
		require.Equal(t, http.StatusOK, res.StatusCode)

		got := decodeTree(t, res.Body)
		got["version"] = sentinelVersion
		got["commit"] = sentinelCommit

		require.Equal(t, map[string]any{
			"name":    "influxdb",
			"status":  "pass",
			"message": "healthy",
			"checks":  []any{},
			"version": sentinelVersion,
			"commit":  sentinelCommit,
		}, got)
	})

	t.Run("health 503 with one failing named check", func(t *testing.T) {
		h := NewHealthReadyHandler(zaptest.NewLogger(t))
		h.AddNamedHealthCheck(failingChecker{name: "query", message: "unreachable"})

		res := doRequest(t, h, http.MethodGet, "/health")
		defer closeBody(t, res)
		require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)
		require.Equal(t, "application/json; charset=utf-8", res.Header.Get("Content-Type"))

		got := decodeTree(t, res.Body)
		got["version"] = sentinelVersion
		got["commit"] = sentinelCommit

		require.Equal(t, map[string]any{
			"name":    "influxdb",
			"status":  "fail",
			"message": "unreachable",
			"checks": []any{
				map[string]any{
					"name":    "query",
					"status":  "fail",
					"message": "unreachable",
				},
			},
			"version": sentinelVersion,
			"commit":  sentinelCommit,
		}, got)
	})

	t.Run("ready 200 with no checks registered", func(t *testing.T) {
		h := NewHealthReadyHandler(zaptest.NewLogger(t))

		res := doRequest(t, h, http.MethodGet, "/ready")
		defer closeBody(t, res)
		require.Equal(t, http.StatusOK, res.StatusCode)
		require.Equal(t, "application/json; charset=utf-8", res.Header.Get("Content-Type"))

		got := decodeTree(t, res.Body)
		require.IsType(t, "", got["started"])
		require.Regexp(t, `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}`, got["started"])
		require.IsType(t, "", got["up"])
		got["started"] = sentinelStarted
		got["up"] = sentinelUp

		require.Equal(t, map[string]any{
			"status":  "ready",
			"started": sentinelStarted,
			"up":      sentinelUp,
		}, got)
	})

	t.Run("ready 503 with one failing ReadyGate", func(t *testing.T) {
		h := NewHealthReadyHandler(zaptest.NewLogger(t))
		h.AddNamedReadyCheck(check.NewReadyGate("metastores"))

		res := doRequest(t, h, http.MethodGet, "/ready")
		defer closeBody(t, res)
		require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)

		got := decodeTree(t, res.Body)
		require.IsType(t, "", got["started"])
		require.IsType(t, "", got["up"])
		got["started"] = sentinelStarted
		got["up"] = sentinelUp

		require.Equal(t, map[string]any{
			"status":  "starting",
			"started": sentinelStarted,
			"up":      sentinelUp,
			"checks": []any{
				map[string]any{
					"name":    "metastores",
					"status":  "fail",
					"message": "not ready",
				},
			},
		}, got)
	})

	t.Run("pre-delegate 503 byte-exact body", func(t *testing.T) {
		h := NewHealthReadyHandler(zaptest.NewLogger(t))

		res := doRequest(t, h, http.MethodGet, "/anything")
		defer closeBody(t, res)
		require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)
		require.Equal(t, "application/json; charset=utf-8", res.Header.Get("Content-Type"))

		body, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		require.Equal(t, "{\"status\":\"starting\"}\n", string(body))
	})
}
