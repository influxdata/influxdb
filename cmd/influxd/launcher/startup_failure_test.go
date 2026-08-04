package launcher_test

import (
	nethttp "net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	"github.com/influxdata/influxdb/v2/kit/check"
	"github.com/stretchr/testify/require"
)

// readyBody mirrors the JSON shape served by /ready. Its checks are populated
// only when the aggregate is failing, which is the case under test here.
type readyBody struct {
	Status string                `json:"status"`
	Checks []check.BasicResponse `json:"checks"`
}

// assertEngineFailure checks the endpoints of a launcher whose run failed
// during engine initialization. It must be called before Shutdown: the
// listener stays up until then, which is the whole window in which an
// in-process test can observe what a probe would have seen.
func assertEngineFailure(t *testing.T, l *launcher.TestLauncher) {
	t.Helper()

	var health healthBody
	status := httpGetJSON(t, l.URL().String()+"/health", "", &health)
	require.Equal(t, nethttp.StatusServiceUnavailable, status)
	require.Equal(t, check.StatusFail, health.Status)

	got := checkNames(health.Checks)
	require.Len(t, got, len(health.Checks), "duplicate check names in %v", health.Checks)
	require.Containsf(t, got, launcher.SubsystemEngine,
		"no check named %q on /health: %v", launcher.SubsystemEngine, got)
	require.Equal(t, check.StatusFail, got[launcher.SubsystemEngine])

	// The message must name the failing phase rather than a bare "starting".
	// firstFailureMessage picks the alphabetically first failing check, and
	// the engine sorts ahead of shards, so an engine failure is what an
	// operator reads at the top of the body.
	var engineMsg string
	for _, c := range health.Checks {
		if c.Name() == launcher.SubsystemEngine {
			engineMsg = c.Message()
		}
	}
	require.NotEmpty(t, engineMsg, "the engine check carries no reason")
	require.Equal(t, engineMsg, health.Message)

	// The two checks named "shards" are not the same check. The /ready one is
	// driven by StartupProgressLogger.Finish and latches this failure; the
	// /health one reports errors accumulated from individual shards, and an
	// engine that never opened loaded none. So an engine failure is visible on
	// /health under "engine" and nowhere else -- a monitoring rule looking for
	// a failing "shards" there would never fire. HEALTH_READY.md documents this
	// asymmetry; this pins it.
	require.Containsf(t, got, launcher.SubsystemShards,
		"no check named %q on /health: %v", launcher.SubsystemShards, got)
	require.Equal(t, check.StatusPass, got[launcher.SubsystemShards],
		"an engine failure loads no shards, so /health's shards check has nothing to report")

	var ready readyBody
	status = httpGetJSON(t, l.URL().String()+"/ready", "", &ready)
	require.Equal(t, nethttp.StatusServiceUnavailable, status)
	require.Equal(t, "starting", ready.Status)

	var engineReady *check.BasicResponse
	for i, c := range ready.Checks {
		if c.Name() == launcher.SubsystemEngine {
			engineReady = &ready.Checks[i]
		}
	}
	require.NotNil(t, engineReady, "no engine entry on /ready: %v", ready.Checks)
	require.Equal(t, check.StatusFail, engineReady.Status())
	require.NotEqual(t, check.MsgNotReady, engineReady.Message(),
		"the engine gate reports no reason for a failure that already happened")

	// The phases downstream of the engine never ran, and say so rather than
	// reporting check.MsgNotReady, which means "not yet" and reads as a server
	// still working through startup. This is Launcher.failUnreachedGates, and
	// the deferred call in run is the only thing that invokes it -- so this is
	// what pins that wiring. Both callers of this helper fail under the name
	// engine, so the phase in the message is the same for both.
	//
	// query and replications rather than tasks or task-scheduler: those two are
	// pre-fired under --no-tasks and so are not reliably un-fired here.
	readyByName := make(map[string]check.BasicResponse, len(ready.Checks))
	for _, c := range ready.Checks {
		readyByName[c.Name()] = c
	}
	for _, name := range []string{launcher.SubsystemQuery, launcher.SubsystemReplications} {
		c, ok := readyByName[name]
		require.Truef(t, ok, "no %q entry on /ready: %v", name, ready.Checks)
		require.Equalf(t, "not reached: startup failed at "+launcher.SubsystemEngine, c.Message(),
			"%s does not report that it was never reached", name)
	}
}

// TestLauncher_StartupFailure_EngineOpen forces engine.Open to fail after the
// HTTP server is up, and asserts that /health and /ready name the engine and
// say why. Before per-subsystem attribution both endpoints were silent about
// it: /health had no engine entry at all, since the health checks for a
// subsystem are registered only once it is up.
func TestLauncher_StartupFailure_EngineOpen(t *testing.T) {
	l := launcher.NewTestLauncherServer()

	// A regular file where the engine expects its directory: Open fails with
	// ENOTDIR, well after runHTTP has a listener bound.
	enginePath := filepath.Join(l.Path, "engine")
	require.NoError(t, os.WriteFile(enginePath, []byte("not a directory"), 0600))

	// Registered before the first assertion: a failing require would otherwise
	// leak a running launcher and its temp directory.
	defer func() { require.NoError(t, l.Shutdown(ctx)) }()

	err := l.Run(t, ctx)
	require.Error(t, err, "engine.Open must fail with a file in place of its directory")

	assertEngineFailure(t, l)
}

// TestLauncher_StartupFailure_PriorVersion covers the prior-version check,
// which called os.Exit(1) directly. That skipped every deferred function in
// run — the HTTP server's closer and the PID file's among them — and made the
// path untestable at all: this test could not have been written before the
// conversion, because it would have killed the test binary.
func TestLauncher_StartupFailure_PriorVersion(t *testing.T) {
	l := launcher.NewTestLauncherServer()

	// A _series directory under the engine path is 1.x-era layout, which
	// checkForPriorVersion refuses to start on.
	require.NoError(t, os.MkdirAll(filepath.Join(l.Path, "engine", "_series"), 0700))

	defer func() { require.NoError(t, l.Shutdown(ctx)) }()

	err := l.Run(t, ctx)
	require.Error(t, err, "the prior-version check must reject a _series directory")

	assertEngineFailure(t, l)
}
