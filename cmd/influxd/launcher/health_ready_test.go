package launcher_test

import (
	"encoding/json"
	nethttp "net/http"
	"testing"

	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	"github.com/influxdata/influxdb/v2/cmd/influxd/run"
	"github.com/influxdata/influxdb/v2/kit/check"
	"github.com/influxdata/influxdb/v2/sqlite"
	"github.com/stretchr/testify/require"
)

// httpGetJSON issues a GET against url and decodes the JSON body into out.
// The HTTP status is returned so callers can distinguish 200 from 503 in
// addition to whatever the body carries. /health and /ready use different
// envelope shapes, so out is a per-call struct.
func httpGetJSON(t *testing.T, url string, out interface{}) int {
	t.Helper()
	req, err := nethttp.NewRequestWithContext(ctx, "GET", url, nil)
	require.NoError(t, err)
	resp, err := nethttp.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	require.NoError(t, json.NewDecoder(resp.Body).Decode(out))
	return resp.StatusCode
}

// healthBody mirrors the JSON shape served by the /health endpoint.
// Defined locally to avoid taking a test-only dependency on the http
// package's unexported `healthBody`.
type healthBody struct {
	Name   string          `json:"name"`
	Status check.Status    `json:"status"`
	Checks check.Responses `json:"checks"`
}

// checkNames returns the set of names present in a Responses slice.
func checkNames(rs check.Responses) map[string]check.Status {
	out := make(map[string]check.Status, len(rs))
	for _, c := range rs {
		out[c.Name] = c.Status
	}
	return out
}

func TestLauncher_HealthEndpoint(t *testing.T) {
	tests := []struct {
		name        string
		newLauncher func() *launcher.TestLauncher
		expected    []string
	}{
		{
			name:        "memory_mode",
			newLauncher: launcher.NewTestLauncher,
			// In memory mode the KV backend is *inmem.KVStore, so the
			// launcher's type-assertion at registration time skips the
			// bolt health check. NoopScheduler is *not* used by default
			// (only set when opts.NoTasks), so task-scheduler is wired.
			expected: []string{"query", "influxql", sqlite.HealthCheckName, run.TaskSchedulerCheckName},
		},
		{
			name:        "disk_mode",
			newLauncher: launcher.NewTestLauncherServer,
			expected: []string{
				"query", "influxql",
				bolt.HealthCheckName,
				sqlite.HealthCheckName,
				run.TaskSchedulerCheckName,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := tt.newLauncher()
			l.RunOrFail(t, ctx)
			defer l.ShutdownOrFail(t, ctx)
			l.SetupOrFail(t)

			var body healthBody
			status := httpGetJSON(t, l.URL().String()+"/health", &body)
			require.Equal(t, nethttp.StatusOK, status)
			require.Equal(t, check.StatusPass, body.Status)

			got := checkNames(body.Checks)
			require.Len(t, got, len(tt.expected),
				"unexpected check set on /health: %v", got)
			for _, name := range tt.expected {
				st, ok := got[name]
				require.Truef(t, ok, "missing health check %q in %v", name, got)
				require.Equalf(t, check.StatusPass, st,
					"check %q expected pass, got %q", name, st)
			}
		})
	}
}

// TestLauncher_ReadyEndpoint verifies the /ready endpoint returns a
// passing response after the launcher finishes setup. The /ready body
// only enumerates checks when failing (`omitempty` on Checks); we cannot
// assert per-check names from a passing response, so this is a smoke test
// for endpoint wiring and overall ready state. The per-check semantics
// are covered by the unit tests on each Checker type.
func TestLauncher_ReadyEndpoint(t *testing.T) {
	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t)
	defer l.ShutdownOrFail(t, ctx)

	var body struct {
		Status string `json:"status"`
	}
	status := httpGetJSON(t, l.URL().String()+"/ready", &body)
	require.Equal(t, nethttp.StatusOK, status)
	require.Equal(t, "ready", body.Status)
}
