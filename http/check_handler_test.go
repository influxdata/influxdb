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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// decodeHealth parses the JSON body emitted by writeHealth. Local struct
// (not healthBody) so the test is checking the wire format.
type testHealthBody struct {
	Name    string           `json:"name"`
	Status  string           `json:"status"`
	Message string           `json:"message"`
	Checks  []check.Response `json:"checks"`
	Version string           `json:"version"`
	Commit  string           `json:"commit"`
}

type testReadyBody struct {
	Status  string           `json:"status"`
	Started time.Time        `json:"started"`
	Up      string           `json:"up"`
	Checks  []check.Response `json:"checks"`
}

func doRequest(t *testing.T, h http.Handler, method, target string) *http.Response {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Result()
}

func TestHealthReadyHandler_Health_NoChecksPasses(t *testing.T) {
	h := NewHealthReadyHandler()

	res := doRequest(t, h, http.MethodGet, "/health")
	defer res.Body.Close()

	require.Equal(t, http.StatusOK, res.StatusCode)
	require.Equal(t, "application/json; charset=utf-8", res.Header.Get("Content-Type"))
	require.Equal(t, "OSS", res.Header.Get("X-Influxdb-Build"))
	require.Equal(t, platform.GetBuildInfo().Version, res.Header.Get("X-Influxdb-Version"))

	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)

	var got testHealthBody
	require.NoError(t, json.Unmarshal(body, &got))
	info := platform.GetBuildInfo()
	require.Equal(t, "influxdb", got.Name)
	require.Equal(t, "pass", got.Status)
	require.Equal(t, "ready for queries and writes", got.Message)
	require.Equal(t, info.Version, got.Version)
	require.Equal(t, info.Commit, got.Commit)
	require.NotNil(t, got.Checks)
	require.Empty(t, got.Checks)
}

type failingChecker struct {
	name    string
	message string
}

func (f failingChecker) CheckName() string { return f.name }
func (f failingChecker) Check(context.Context) check.Response {
	return check.Response{Status: check.StatusFail, Message: f.message}
}

func TestHealthReadyHandler_Health_FailingChecker(t *testing.T) {
	h := NewHealthReadyHandler()
	h.AddHealthCheck(failingChecker{name: "query", message: "unreachable"})

	res := doRequest(t, h, http.MethodGet, "/health")
	defer res.Body.Close()

	require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)

	var got testHealthBody
	require.NoError(t, json.NewDecoder(res.Body).Decode(&got))
	assert.Equal(t, "fail", got.Status)
	assert.Equal(t, "unreachable", got.Message)
	require.Len(t, got.Checks, 1)
	assert.Equal(t, "query", got.Checks[0].Name)
	assert.Equal(t, check.StatusFail, got.Checks[0].Status)
}

func TestHealthReadyHandler_Ready_FailingGate(t *testing.T) {
	h := NewHealthReadyHandler()
	gate := check.NewReadyGate("engine")
	h.AddReadyCheck(gate)

	res := doRequest(t, h, http.MethodGet, "/ready")
	defer res.Body.Close()

	require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)
	require.Equal(t, "OSS", res.Header.Get("X-Influxdb-Build"))

	var got testReadyBody
	require.NoError(t, json.NewDecoder(res.Body).Decode(&got))
	assert.Equal(t, "starting", got.Status)
	assert.False(t, got.Started.IsZero())
	assert.NotEmpty(t, got.Up)
	require.Len(t, got.Checks, 1)
	assert.Equal(t, "engine", got.Checks[0].Name)
	assert.Equal(t, check.StatusFail, got.Checks[0].Status)
	assert.Equal(t, "not ready", got.Checks[0].Message)
}

func TestHealthReadyHandler_Ready_PassingGateOmitsChecks(t *testing.T) {
	h := NewHealthReadyHandler()
	gate := check.NewReadyGate("engine")
	h.AddReadyCheck(gate)
	gate.Ready()

	res := doRequest(t, h, http.MethodGet, "/ready")
	defer res.Body.Close()

	require.Equal(t, http.StatusOK, res.StatusCode)

	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)

	// Decode into a generic map to confirm the "checks" key is absent on pass.
	var generic map[string]any
	require.NoError(t, json.Unmarshal(body, &generic))
	_, hasChecks := generic["checks"]
	assert.False(t, hasChecks, "passing /ready should omit the checks field, got %v", generic)
	assert.Equal(t, "ready", generic["status"])
}

func TestHealthReadyHandler_Health_ForceOverride(t *testing.T) {
	h := NewHealthReadyHandler()
	// No checkers → passes by default.

	// Force fail.
	res := doRequest(t, h, http.MethodGet, "/health?force=true&healthy=false")
	defer res.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)

	// Clear override.
	res2 := doRequest(t, h, http.MethodGet, "/health?force=false")
	defer res2.Body.Close()
	require.Equal(t, http.StatusOK, res2.StatusCode)
}

func TestHealthReadyHandler_Ready_ForceOverride(t *testing.T) {
	h := NewHealthReadyHandler()
	gate := check.NewReadyGate("engine")
	h.AddReadyCheck(gate)

	// Force pass while gate still reports not-ready.
	res := doRequest(t, h, http.MethodGet, "/ready?force=true&ready=true")
	defer res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)

	// Clear override returns to the underlying failure.
	res2 := doRequest(t, h, http.MethodGet, "/ready?force=false")
	defer res2.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, res2.StatusCode)
}

func TestHealthReadyHandler_Health_ForceFail_ExposesOverrideMessage(t *testing.T) {
	h := NewHealthReadyHandler()
	// No real checkers registered; force-fail the health status.

	res := doRequest(t, h, http.MethodGet, "/health?force=true&healthy=false")
	defer res.Body.Close()

	require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)

	var got testHealthBody
	require.NoError(t, json.NewDecoder(res.Body).Decode(&got))
	assert.Equal(t, "fail", got.Status)
	assert.Equal(t, "health manually overridden", got.Message)
	require.Len(t, got.Checks, 1)
	assert.Equal(t, "manual-override", got.Checks[0].Name)
	assert.Equal(t, check.StatusFail, got.Checks[0].Status)
}

func TestHealthReadyHandler_Ready_ForceFail_ExposesOverrideInChecks(t *testing.T) {
	h := NewHealthReadyHandler()
	// No real checkers registered; force-fail the ready status.

	res := doRequest(t, h, http.MethodGet, "/ready?force=true&ready=false")
	defer res.Body.Close()

	require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)

	var got testReadyBody
	require.NoError(t, json.NewDecoder(res.Body).Decode(&got))
	assert.Equal(t, "starting", got.Status)
	require.Len(t, got.Checks, 1)
	assert.Equal(t, "manual-override", got.Checks[0].Name)
	assert.Equal(t, check.StatusFail, got.Checks[0].Status)
	assert.Equal(t, "ready manually overridden", got.Checks[0].Message)
}

func TestHealthReadyHandler_NoDelegate_ReturnsStarting(t *testing.T) {
	h := NewHealthReadyHandler()

	res := doRequest(t, h, http.MethodGet, "/api/v2/buckets")
	defer res.Body.Close()

	require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)
	require.Equal(t, "application/json; charset=utf-8", res.Header.Get("Content-Type"))
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, `{"status":"starting"}`+"\n", string(body))
	assert.Equal(t, "OSS", res.Header.Get("X-Influxdb-Build"))
}

func TestHealthReadyHandler_DelegateInstalled_ForwardsNonCheckRequests(t *testing.T) {
	h := NewHealthReadyHandler()

	delegate := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte("delegated: " + r.URL.Path))
	})
	h.SetHandler(delegate)

	res := doRequest(t, h, http.MethodGet, "/api/v2/buckets")
	defer res.Body.Close()
	require.Equal(t, http.StatusTeapot, res.StatusCode)
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, "delegated: /api/v2/buckets", string(body))
	// Build-info headers are NOT set on delegated responses — the delegate is
	// expected to have its own header middleware (see NewRootHandler).
	assert.Empty(t, res.Header.Values("X-Influxdb-Build"))
	assert.Empty(t, res.Header.Values("X-Influxdb-Version"))

	// /health and /ready are still served locally, not forwarded, and still
	// carry the build-info headers.
	resHealth := doRequest(t, h, http.MethodGet, "/health")
	defer resHealth.Body.Close()
	require.Equal(t, http.StatusOK, resHealth.StatusCode)
	assert.Equal(t, "OSS", resHealth.Header.Get("X-Influxdb-Build"))
}
