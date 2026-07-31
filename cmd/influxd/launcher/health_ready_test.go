package launcher_test

import (
	"encoding/json"
	nethttp "net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	"github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/kit/check"
	"github.com/stretchr/testify/require"
)

const (
	// engineFailPrefix is what failSubsystem stamps onto the single startup
	// entry on /health. Derived from the subsystem constant so renaming the
	// subsystem cannot leave a stale expectation behind.
	engineFailPrefix = launcher.SubsystemEngine + ": "

	pidFileName = "influxd.pid"
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
// package's unexported `healthBody`. check.Response is an interface
// and cannot be a decode target, so nested checks decode as
// BasicResponse.
type healthBody struct {
	Name    string                `json:"name"`
	Status  check.Status          `json:"status"`
	Message string                `json:"message"`
	Checks  []check.BasicResponse `json:"checks"`
}

// readyBody mirrors the JSON shape served by /ready. The checks array is
// populated only when the response is failing (`omitempty`), which is exactly
// the case the startup-error tests exercise.
type readyBody struct {
	Status string                `json:"status"`
	Checks []check.BasicResponse `json:"checks"`
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
				launcher.SubsystemStartup,
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
				launcher.SubsystemStartup,
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
			status := httpGetJSON(t, l.URL().String()+http.HealthPath, &body)
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
// passing response after the launcher finishes setup, and that the
// expected ready checks are registered. The /ready body only enumerates
// checks when failing (`omitempty` on Checks), so we cross-check the
// registered set via the launcher's ReadyCheckNames accessor.
func TestLauncher_ReadyEndpoint(t *testing.T) {
	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t)
	defer l.ShutdownOrFail(t, ctx)

	var body readyBody
	status := httpGetJSON(t, l.URL().String()+http.ReadyPath, &body)
	require.Equal(t, nethttp.StatusOK, status)
	require.Equal(t, "ready", body.Status)

	expected := []string{
		launcher.SubsystemStartup,
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

// failEngineOpen points the engine at a regular file so storage engine open
// fails. The failure lands after runHTTP has established the listener, which
// is what makes the endpoints scrapeable while the error is latched.
func failEngineOpen(t *testing.T) launcher.OptSetter {
	t.Helper()
	// A regular file where the engine expects a directory.
	enginePath := filepath.Join(t.TempDir(), "engine-is-a-file")
	require.NoError(t, os.WriteFile(enginePath, []byte("not a directory"), 0600))
	return func(o *launcher.InfluxdOpts) { o.EnginePath = enginePath }
}

// TestLauncher_StartupError_SurfacedOnHealthAndReady verifies that an error
// aborting startup is retrievable over both endpoints before the process
// exits. The listener stays up until Shutdown, so the scrape below happens in
// exactly the window an operator or probe would use.
func TestLauncher_StartupError_SurfacedOnHealthAndReady(t *testing.T) {
	l := launcher.NewTestLauncherServer()
	err := l.Run(t, ctx, failEngineOpen(t))
	require.Error(t, err, "engine open should have failed")
	defer l.ShutdownOrFail(t, ctx)

	// The returned error carries the subsystem name, so the exit-code path
	// and the HTTP path agree on attribution.
	require.ErrorContains(t, err, launcher.SubsystemEngine)

	t.Run("health", func(t *testing.T) {
		var body healthBody
		status := httpGetJSON(t, l.URL().String()+http.HealthPath, &body)
		require.Equal(t, nethttp.StatusServiceUnavailable, status)
		require.Equal(t, check.StatusFail, body.Status)

		startup := findCheck(t, body.Checks, launcher.SubsystemStartup)
		require.Equal(t, check.StatusFail, startup.Status())
		// /health carries a single startup entry, so the subsystem name must
		// be in the message or the failure is unattributable. Assert the
		// prefix explicitly: a regression that drops it is otherwise silent.
		require.Truef(t, strings.HasPrefix(startup.Message(), engineFailPrefix),
			"startup message %q should be prefixed with %q",
			startup.Message(), engineFailPrefix)
	})

	t.Run("ready", func(t *testing.T) {
		var body readyBody
		status := httpGetJSON(t, l.URL().String()+http.ReadyPath, &body)
		require.Equal(t, nethttp.StatusServiceUnavailable, status)

		// The engine gate carries the unprefixed message: its response is
		// already stamped with the subsystem name.
		engine := findCheck(t, body.Checks, launcher.SubsystemEngine)
		require.Equal(t, check.StatusFail, engine.Status())
		require.NotEqual(t, check.MsgNotReady, engine.Message(),
			"engine gate should carry the failure reason, not the default")
		require.False(t, strings.HasPrefix(engine.Message(), engineFailPrefix),
			"gate message should not repeat the subsystem name")

		// The catch-all is registered on /ready too. This is what closes the
		// window in which every gate has fired but a later step failed, which
		// would otherwise report "ready" until the process exits.
		startup := findCheck(t, body.Checks, launcher.SubsystemStartup)
		require.Equal(t, check.StatusFail, startup.Status())
		require.Contains(t, startup.Message(), engine.Message(),
			"startup should carry the same cause the gate does")
	})
}

// TestLauncher_StartupError_Shutdown verifies that after a failed startup
// Shutdown's closers still run — the PID file closer is registered early, and
// a partially-constructed launcher must not prevent it from firing — and that
// they run at most once, since the startup-failure path and the normal exit
// path can both call Shutdown and a second teardown would double-close stores.
//
// NOTE: this does not cover the cmdRunE change that makes Shutdown run at all
// on the startup-error path. cmdRunE calls fluxinit.FluxInit, which panics on
// a second call, and this package already finalizes the Flux runtime via the
// fluxinit/static import in launcher_test.go. Exercising the real exit path
// requires a subprocess test.
func TestLauncher_StartupError_Shutdown(t *testing.T) {
	// Keep the PID file outside the launcher's own temp dir, which Shutdown
	// removes wholesale — otherwise the assertion would pass trivially.
	pidFile := filepath.Join(t.TempDir(), pidFileName)

	l := launcher.NewTestLauncherServer()
	err := l.Run(t, ctx, failEngineOpen(t), func(o *launcher.InfluxdOpts) {
		o.PIDFile = pidFile
	})
	require.Error(t, err, "engine open should have failed")
	require.FileExists(t, pidFile, "PID file should exist before shutdown")

	require.NoError(t, l.Launcher.Shutdown(ctx))
	require.NoFileExists(t, pidFile, "failed startup must not orphan the PID file")

	// A second call must not attempt to remove the now-absent PID file, which
	// would surface as a teardown error.
	require.NoError(t, l.Launcher.Shutdown(ctx), "Shutdown should be idempotent")

	l.ShutdownOrFail(t, ctx)
}

// TestLauncher_StartupError_HoldReleasesSubsystems covers the teardown split
// around the startup-error hold. Everything but the listener is closed before
// the process parks, so the state a restart needs — the PID file here, and with
// it the bolt flock, the sqlite file and the engine directory — is already
// released while /health is still serving the reason it failed.
func TestLauncher_StartupError_HoldReleasesSubsystems(t *testing.T) {
	// Keep the PID file outside the launcher's own temp dir, which teardown
	// removes wholesale — otherwise the assertion would pass trivially.
	pidFile := filepath.Join(t.TempDir(), pidFileName)

	l := launcher.NewTestLauncherServer()
	err := l.Run(t, ctx, failEngineOpen(t), func(o *launcher.InfluxdOpts) {
		o.PIDFile = pidFile
	})
	defer l.ShutdownOrFail(t, ctx)
	require.Error(t, err, "engine open should have failed")
	require.FileExists(t, pidFile, "PID file should exist before the hold")

	// The subsystem teardown runs before the wait, so a short window is enough:
	// what is asserted below is the state an operator scraping during a long
	// hold would see.
	l.Launcher.HoldForStartupError(ctx, 50*time.Millisecond)

	require.NoFileExists(t, pidFile,
		"subsystems must be released before the hold, not after it")

	var body healthBody
	status := httpGetJSON(t, l.URL().String()+http.HealthPath, &body)
	require.Equal(t, nethttp.StatusServiceUnavailable, status,
		"the listener must still serve after the subsystems are torn down")
	require.Equal(t, check.StatusFail, body.Status)

	// Only the catch-all survives. The subsystems just closed publish their own
	// checks, and left registered they would report that deliberate teardown as
	// a fresh failure — and outrank "startup" in the sort, since the top-level
	// message is taken from the first failing check by name.
	require.Equal(t,
		map[string]check.Status{launcher.SubsystemStartup: check.StatusFail},
		checkNames(body.Checks))
	require.True(t, strings.HasPrefix(body.Message, engineFailPrefix),
		"top-level message %q should name the cause, not a teardown symptom",
		body.Message)
}

// findCheck returns the named response, failing the test if absent.
func findCheck(t *testing.T, rs []check.BasicResponse, name string) check.BasicResponse {
	t.Helper()
	for _, c := range rs {
		if c.Name() == name {
			return c
		}
	}
	require.FailNowf(t, "missing check", "check %q not found in %v", name, checkNames(rs))
	return check.BasicResponse{}
}
