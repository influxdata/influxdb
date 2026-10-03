package http

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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
	require.NoError(t, h.AddNamedHealthCheck(check.Named("static", check.CheckerFunc(func(context.Context) check.Response {
		return check.NamedPass("static")
	}))))

	fresh := check.NewFreshnessResponse("fresh", time.Hour)
	fresh.Update(check.Pass())
	require.NoError(t, h.AddNamedHealthCheck(staticChecker{name: "fresh", resp: fresh}))

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
	require.NoError(t, h.AddNamedHealthCheck(staticChecker{name: "svc", resp: fresh}))

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
// Dynamic fields (started, up, uptime, version, commit) are validated for
// type/format and then replaced with sentinels so the rest of the tree
// can be compared against constants.
func TestCheckHandler_WireFormat_FullDocumentPin(t *testing.T) {
	const (
		sentinelStarted = "<started>"
		sentinelUp      = "<up>"
		sentinelVersion = "<version>"
		sentinelCommit  = "<commit>"
	)

	info := platform.GetBuildInfo()

	t.Run("health 200 with one passing named check", func(t *testing.T) {
		h := NewHealthReadyHandler(zaptest.NewLogger(t))
		require.NoError(t, h.AddNamedHealthCheck(check.Named("alpha", check.CheckerFunc(func(context.Context) check.Response {
			return check.NamedPass("alpha")
		}))))

		res := doRequest(t, h, http.MethodGet, "/health")
		defer closeBody(t, res)
		require.Equal(t, http.StatusOK, res.StatusCode)
		require.Equal(t, "application/json; charset=utf-8", res.Header.Get("Content-Type"))
		require.Equal(t, "OSS", res.Header.Get("X-Influxdb-Build"))
		require.Equal(t, info.Version, res.Header.Get("X-Influxdb-Version"))

		got := decodeBody(t, res)
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

		got := decodeBody(t, res)
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
		require.NoError(t, h.AddNamedHealthCheck(failingChecker{name: "query", message: "unreachable"}))

		res := doRequest(t, h, http.MethodGet, "/health")
		defer closeBody(t, res)
		require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)
		require.Equal(t, "application/json; charset=utf-8", res.Header.Get("Content-Type"))

		got := decodeBody(t, res)
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

		got := decodeBody(t, res)
		require.IsType(t, "", got["started"])
		require.Regexp(t, `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}`, got["started"])
		require.IsType(t, "", got["up"])
		requireUptimeMatchesUp(t, got)
		got["started"] = sentinelStarted
		got["up"] = sentinelUp
		got["uptime"] = sentinelUp

		require.Equal(t, map[string]any{
			"status":  "ready",
			"started": sentinelStarted,
			"up":      sentinelUp,
			"uptime":  sentinelUp,
		}, got)
	})

	t.Run("ready 503 with one failing ReadyGate", func(t *testing.T) {
		h := NewHealthReadyHandler(zaptest.NewLogger(t))
		require.NoError(t, h.AddNamedReadyCheck(check.NewReadyGate("metastores")))

		res := doRequest(t, h, http.MethodGet, "/ready")
		defer closeBody(t, res)
		require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)

		got := decodeBody(t, res)
		require.IsType(t, "", got["started"])
		require.IsType(t, "", got["up"])
		requireUptimeMatchesUp(t, got)
		got["started"] = sentinelStarted
		got["up"] = sentinelUp
		got["uptime"] = sentinelUp

		require.Equal(t, map[string]any{
			"status":  "starting",
			"started": sentinelStarted,
			"up":      sentinelUp,
			"uptime":  sentinelUp,
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

// requireUptimeMatchesUp checks that /ready's uptime is a seconds measure and
// that it carries the same reading as the up string beside it: both are
// rendered from one time.Since, so they agree to the nanosecond.
func requireUptimeMatchesUp(t *testing.T, got map[string]any) {
	t.Helper()
	up, err := time.ParseDuration(got["up"].(string))
	require.NoError(t, err)
	uptime, ok := got["uptime"].(map[string]any)
	require.True(t, ok, "uptime must be an object, got %T", got["uptime"])
	require.Len(t, uptime, 2, "uptime must carry exactly value and unit")
	require.Equal(t, check.UnitSeconds, uptime["unit"])
	value, ok := uptime["value"].(float64)
	require.True(t, ok, "uptime.value must be a number, got %T", uptime["value"])
	require.InDelta(t, up.Seconds(), value, 1e-9)
}

// TestCheckHandler_WireFormat_MeasurePin pins how a check's measure groups
// render on /health: each group is a key of its own on the check object,
// beside name, status and message, holding its values and a unit.
func TestCheckHandler_WireFormat_MeasurePin(t *testing.T) {
	h := NewHealthReadyHandler(zaptest.NewLogger(t))
	require.NoError(t, h.AddNamedHealthCheck(check.Named("shards", check.CheckerFunc(func(context.Context) check.Response {
		return check.Fail("2 shard(s) failed to load").
			WithMeasure("failures", check.Measure{Unit: "shards", Values: map[string]float64{"count": 2}})
	}))))

	res := doRequest(t, h, http.MethodGet, "/health")
	defer closeBody(t, res)
	require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)

	got := decodeBody(t, res)
	got["version"] = "<version>"
	got["commit"] = "<commit>"

	require.Equal(t, map[string]any{
		"name":    "influxdb",
		"status":  "fail",
		"message": "2 shard(s) failed to load",
		"checks": []any{
			map[string]any{
				"name":     "shards",
				"status":   "fail",
				"message":  "2 shard(s) failed to load",
				"failures": map[string]any{"count": float64(2), "unit": "shards"},
			},
		},
		"version": "<version>",
		"commit":  "<commit>",
	}, got)
}

// TestCheckHandler_MeasuresSurviveQueryHealthCheck is the client half of the
// round trip: the in-repo remote-health client decodes a full /health body --
// version, commit and all -- and must keep each check's measure groups.
func TestCheckHandler_MeasuresSurviveQueryHealthCheck(t *testing.T) {
	progress := check.Measure{Unit: "shards", Values: map[string]float64{"completed": 94, "total": 200}}
	h := NewHealthReadyHandler(zaptest.NewLogger(t))
	require.NoError(t, h.AddNamedHealthCheck(check.Named("shards", check.CheckerFunc(func(context.Context) check.Response {
		return check.Info("loading").WithMeasure("progress", progress)
	}))))
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp := QueryHealthCheck(srv.URL, false)
	require.Equal(t, check.StatusPass, resp.Status(), "message: %s", resp.Message())
	require.Nil(t, resp.Measures(), "the top-level envelope carries no groups of its own")
	require.Len(t, resp.Checks(), 1)
	require.Equal(t, "shards", resp.Checks()[0].Name())
	require.Equal(t, check.Measures{"progress": progress}, resp.Checks()[0].Measures())
}
