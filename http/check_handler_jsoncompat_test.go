package http

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

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
