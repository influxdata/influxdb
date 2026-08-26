package launcher_test

import (
	"encoding/json"
	"io"
	nethttp "net/http"
	"testing"

	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	"github.com/influxdata/influxdb/v2/kit/check"
	"github.com/stretchr/testify/require"
)

// httpGetJSON issues a GET against url and decodes the JSON body into out.
// The HTTP status is returned so callers can distinguish 200 from 503 in
// addition to whatever the body carries. /health and /ready use different
// envelope shapes, so out is a per-call struct. An empty token sends no
// Authorization header at all, which is what the anonymous-probe cases need.
func httpGetJSON(t *testing.T, url, token string, out interface{}) int {
	t.Helper()
	req, err := nethttp.NewRequestWithContext(ctx, "GET", url, nil)
	require.NoError(t, err)
	if token != "" {
		req.Header.Set("Authorization", "Token "+token)
	}
	resp, err := nethttp.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoErrorf(t, json.Unmarshal(body, out), "body: %s", body)
	return resp.StatusCode
}

// healthBody mirrors the JSON shape served by the /health endpoint.
// Defined locally to avoid taking a test-only dependency on the http
// package's unexported `healthBody`. check.Response is an interface
// and cannot be a decode target, so nested checks decode as
// BasicResponse.
type healthBody struct {
	Name    string                `json:"name"`
	Status  check.Status          `json:"status"`
	Message string                `json:"message"`
	Checks  []check.BasicResponse `json:"checks"`
	Version string                `json:"version"`
	Commit  string                `json:"commit"`
}

// checkNames returns the set of names present in a checks slice.
func checkNames(rs []check.BasicResponse) map[string]check.Status {
	out := make(map[string]check.Status, len(rs))
	for _, c := range rs {
		out[c.Name()] = c.Status()
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
			expected: []string{
				launcher.SubsystemQuery,
				launcher.SubsystemInfluxQL,
				launcher.SubsystemSQLite,
				launcher.SubsystemTaskScheduler,
				launcher.SubsystemShards,
			},
		},
		{
			name:        "disk_mode",
			newLauncher: launcher.NewTestLauncherServer,
			expected: []string{
				launcher.SubsystemQuery,
				launcher.SubsystemInfluxQL,
				launcher.SubsystemKV,
				launcher.SubsystemSQLite,
				launcher.SubsystemTaskScheduler,
				launcher.SubsystemShards,
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
			status := httpGetJSON(t, l.URL().String()+"/health", "", &body)
			require.Equal(t, nethttp.StatusOK, status)
			require.Equal(t, check.StatusPass, body.Status)

			// With health auth off (the default), an anonymous caller gets the
			// full envelope. This is the backward-compat pin for that flag.
			require.Equal(t, "healthy", body.Message)
			require.NotEmpty(t, body.Name)

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
// passing response after the launcher finishes setup, and that the
// expected ready checks are registered. The /ready body only enumerates
// checks when failing (`omitempty` on Checks), so we cross-check the
// registered set via the launcher's ReadyCheckNames accessor.
func TestLauncher_ReadyEndpoint(t *testing.T) {
	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t)
	defer l.ShutdownOrFail(t, ctx)

	var body struct {
		Status string `json:"status"`
	}
	status := httpGetJSON(t, l.URL().String()+"/ready", "", &body)
	require.Equal(t, nethttp.StatusOK, status)
	require.Equal(t, "ready", body.Status)

	expected := []string{
		launcher.SubsystemKV,
		launcher.SubsystemSQLite,
		launcher.SubsystemEngine,
		launcher.SubsystemReplications,
		launcher.SubsystemQuery,
		launcher.SubsystemTasks,
		launcher.SubsystemTaskScheduler,
		launcher.SubsystemShards,
	}
	got := l.ReadyCheckNames()
	require.ElementsMatchf(t, expected, got,
		"unexpected /ready check set: %v", got)
}
