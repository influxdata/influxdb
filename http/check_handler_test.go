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
	"go.uber.org/zap/zaptest"
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

// closeBody closes res.Body and requires no error. Paired with defer so every
// test verifies the close — useful for catching double-close regressions or
// handlers that wrap the body in a Closer that can report an error.
func closeBody(t *testing.T, res *http.Response) {
	t.Helper()
	require.NoError(t, res.Body.Close())
}

func TestHealthReadyHandler_Health_NoChecksPasses(t *testing.T) {
	h := NewHealthReadyHandler(zaptest.NewLogger(t))

	res := doRequest(t, h, http.MethodGet, "/health")
	defer closeBody(t, res)

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
	h := NewHealthReadyHandler(zaptest.NewLogger(t))
	h.AddHealthCheck(failingChecker{name: "query", message: "unreachable"})

	res := doRequest(t, h, http.MethodGet, "/health")
	defer closeBody(t, res)

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
	h := NewHealthReadyHandler(zaptest.NewLogger(t))
	gate := check.NewReadyGate("engine")
	h.AddReadyCheck(gate)

	res := doRequest(t, h, http.MethodGet, "/ready")
	defer closeBody(t, res)

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
	h := NewHealthReadyHandler(zaptest.NewLogger(t))
	gate := check.NewReadyGate("engine")
	h.AddReadyCheck(gate)
	gate.Ready()

	res := doRequest(t, h, http.MethodGet, "/ready")
	defer closeBody(t, res)

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

func TestHealthReadyHandler_NoDelegate_ReturnsStarting(t *testing.T) {
	h := NewHealthReadyHandler(zaptest.NewLogger(t))

	res := doRequest(t, h, http.MethodGet, "/api/v2/buckets")
	defer closeBody(t, res)

	require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)
	require.Equal(t, "application/json; charset=utf-8", res.Header.Get("Content-Type"))
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, `{"status":"starting"}`+"\n", string(body))
	assert.Equal(t, "OSS", res.Header.Get("X-Influxdb-Build"))
}

func TestHealthReadyHandler_DelegateInstalled_ForwardsNonCheckRequests(t *testing.T) {
	h := NewHealthReadyHandler(zaptest.NewLogger(t))

	delegate := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
		_, err := w.Write([]byte("delegated: " + r.URL.Path))
		assert.NoError(t, err)
	})
	h.SetHandler(delegate)

	res := doRequest(t, h, http.MethodGet, "/api/v2/buckets")
	defer closeBody(t, res)
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
	defer closeBody(t, resHealth)
	require.Equal(t, http.StatusOK, resHealth.StatusCode)
	assert.Equal(t, "OSS", resHealth.Header.Get("X-Influxdb-Build"))
}

func TestHealthReadyHandler_SetHandler_ReplacesDelegate(t *testing.T) {
	h := NewHealthReadyHandler(zaptest.NewLogger(t))

	first := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	second := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusPaymentRequired)
	})

	h.SetHandler(first)
	res := doRequest(t, h, http.MethodGet, "/any")
	require.NoError(t, res.Body.Close())
	require.Equal(t, http.StatusTeapot, res.StatusCode)

	h.SetHandler(second)
	res2 := doRequest(t, h, http.MethodGet, "/any")
	require.NoError(t, res2.Body.Close())
	require.Equal(t, http.StatusPaymentRequired, res2.StatusCode)
}

func TestHealthReadyHandler_SetHandler_NilIsIgnored(t *testing.T) {
	h := NewHealthReadyHandler(zaptest.NewLogger(t))

	// Before any delegate, non-check requests should return 503 starting.
	res := doRequest(t, h, http.MethodGet, "/any")
	require.NoError(t, res.Body.Close())
	require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)

	// A nil delegate must not be published; subsequent requests still get 503.
	h.SetHandler(nil)
	res2 := doRequest(t, h, http.MethodGet, "/any")
	body, err := io.ReadAll(res2.Body)
	require.NoError(t, res2.Body.Close())
	require.NoError(t, err)
	require.Equal(t, http.StatusServiceUnavailable, res2.StatusCode)
	require.Equal(t, `{"status":"starting"}`+"\n", string(body))

	// Install a real delegate, then passing nil again must not clear it.
	delegate := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	h.SetHandler(delegate)
	h.SetHandler(nil)

	res3 := doRequest(t, h, http.MethodGet, "/any")
	require.NoError(t, res3.Body.Close())
	require.Equal(t, http.StatusTeapot, res3.StatusCode)
}
