package launcher_test

import (
	"errors"
	nethttp "net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	"github.com/influxdata/influxdb/v2/kit/check"
	"github.com/influxdata/influxdb/v2/kit/check/checktest"
	"github.com/influxdata/influxdb/v2/kit/exit"
	"github.com/stretchr/testify/require"
	bbolt "go.etcd.io/bbolt"
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

	// A path that is not the kind of object it was used as. Nothing tags this
	// site: the ENOTDIR travels up inside the engine's own wrapping and
	// exit.Classify reads it there, which is the property worth pinning --
	// a fixed status at the call site could not tell this apart from the
	// permission or out-of-space failures the same call can produce.
	require.Equal(t, exit.CodeNoInput, exit.Code(err),
		"a bad --engine-path must exit %s", exit.Name(exit.CodeNoInput))

	assertEngineFailure(t, l)
}

// failEngineOpen puts a regular file where the engine expects its directory, so
// engine.Open fails with ENOTDIR well after runHTTP has a listener bound. It is
// the same failure the two tests above force, hoisted because the linger tests
// need it too.
func failEngineOpen(t *testing.T, l *launcher.TestLauncher) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(l.Path, "engine"),
		[]byte("not a directory"), 0600))
}

// fetchCheckDocuments returns the /health and /ready bodies with the values that
// move on their own replaced by sentinels, so two captures taken a moment apart
// can be compared for equality. See checktest.Normalize for which those are.
//
// The presence assertions stay here rather than moving into Normalize, which
// masks only what it finds: /health carries neither field, so a Normalize that
// required them could not serve both endpoints. That /ready reports them at all
// is this endpoint's contract, and worth failing on.
func fetchCheckDocuments(t *testing.T, l *launcher.TestLauncher) (health, ready map[string]any) {
	t.Helper()

	health = make(map[string]any)
	status := httpGetJSON(t, l.URL().String()+"/health", "", &health)
	require.Equal(t, nethttp.StatusServiceUnavailable, status)

	ready = make(map[string]any)
	status = httpGetJSON(t, l.URL().String()+"/ready", "", &ready)
	require.Equal(t, nethttp.StatusServiceUnavailable, status)
	require.Contains(t, ready, checktest.FieldStarted)
	require.Contains(t, ready, checktest.FieldUp)

	return checktest.Normalize(t, health), checktest.Normalize(t, ready)
}

// TestLauncher_StartupFailure_LingerServesFrozenAttribution is the end-to-end
// shape of --startup-error-linger: a failed startup releases the store locks a
// restart needs while retaining the PID file, and serves the failure report
// until the window closes; final shutdown then releases the PID file.
func TestLauncher_StartupFailure_LingerServesFrozenAttribution(t *testing.T) {
	l := launcher.NewTestLauncherServer()

	// Outside l.Path: TestLauncher.Shutdown removes that tree wholesale, which
	// would make "the PID file is gone" pass without the teardown doing
	// anything at all.
	pidFile := filepath.Join(t.TempDir(), "influxd.pid")
	failEngineOpen(t, l)

	defer func() { require.NoError(t, l.Shutdown(ctx)) }()

	err := l.Run(t, ctx, func(o *launcher.InfluxdOpts) { o.PIDFile = pidFile })
	require.Error(t, err, "engine.Open must fail with a file in place of its directory")
	require.FileExists(t, pidFile, "run must have written the PID file before failing")

	assertEngineFailure(t, l)
	healthBefore, readyBefore := fetchCheckDocuments(t, l)

	// A window long enough that nothing but the cancel below can end it.
	held := make(chan struct{})
	go func() {
		defer close(held)
		l.HoldForStartupError(ctx, time.Hour)
	}()

	// Phase 1 has run once the bolt flock is released: it belongs to the next
	// run, not to this one. The endpoints are still answering at that point,
	// which is the whole property under test -- the state a restart needs is
	// released while the report stays readable. A live handle makes Open block
	// for the timeout and fail, so this polls rather than asserting once.
	boltPath := filepath.Join(l.Path, bolt.DefaultFilename)
	require.Eventually(t, func() bool {
		db, err := bbolt.Open(boltPath, 0600, &bbolt.Options{Timeout: 10 * time.Millisecond})
		if err != nil {
			return false
		}
		return db.Close() == nil
	}, 30*time.Second, 10*time.Millisecond,
		"the bolt flock was not released while the endpoints were still up")

	// The PID file is not released with it, and that asymmetry is the point.
	// This process is still running and still holding its port, so the
	// interlock that keeps a second influxd off this directory has to outlive
	// the window: released here, a concurrent start would get past the check
	// that exists to catch exactly this and fail on "address already in use"
	// instead -- a worse error, naming the wrong cause.
	require.FileExists(t, pidFile,
		"the PID file was released while the process still held the port")

	healthAfter, readyAfter := fetchCheckDocuments(t, l)
	require.Equal(t, healthBefore, healthAfter,
		"the frozen /health document changed after the stores were torn down")
	require.Equal(t, readyBefore, readyAfter,
		"the frozen /ready document changed after the stores were torn down")

	l.CancelRun()
	launcher.RequireReturnsWithin(t, 30*time.Second, func() { <-held })

	// Phase 2 is what releases the PID file, and it runs on this path now.
	require.NoError(t, l.Shutdown(ctx))
	require.NoFileExists(t, pidFile)
}

// TestLauncher_StartupFailure_NoLingerTearsNothingDown pins the default. At
// zero the hold does nothing at all — no freeze, no early teardown — and the
// whole teardown belongs to Shutdown, exactly as before the flag existed.
func TestLauncher_StartupFailure_NoLingerTearsNothingDown(t *testing.T) {
	l := launcher.NewTestLauncherServer()

	pidFile := filepath.Join(t.TempDir(), "influxd.pid")
	failEngineOpen(t, l)

	// Registered before the first assertion so a failing require cannot leak a
	// running launcher. Shutdown is idempotent, so the explicit call below is
	// still the one that proves the PID file is released.
	defer func() { require.NoError(t, l.Shutdown(ctx)) }()

	err := l.Run(t, ctx, func(o *launcher.InfluxdOpts) { o.PIDFile = pidFile })
	require.Error(t, err, "engine.Open must fail with a file in place of its directory")

	launcher.RequireReturnsWithin(t, 5*time.Second, func() { l.HoldForStartupError(ctx, 0) })
	require.FileExists(t, pidFile, "a zero linger must tear nothing down")
	assertEngineFailure(t, l)

	// Shutdown is what releases it, and cmdRunE now reaches Shutdown on this
	// path — which it did not before, leaving the PID file for the next start
	// to trip over.
	require.NoError(t, l.Shutdown(ctx))
	require.NoFileExists(t, pidFile)
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

	// On-disk data this build cannot read. No restart fixes it -- an operator
	// has to move the files -- so the status has to be one a supervisor can be
	// configured to stop retrying on.
	require.Equal(t, exit.CodeDataErr, exit.Code(err),
		"an incompatible engine directory must exit %s", exit.Name(exit.CodeDataErr))

	// And pinned once. checkForPriorVersion knew the category and said so, so
	// run's deferred hook must leave the error alone rather than wrap it in a
	// second layer carrying the same status: unwrapping the status has to reach
	// the error the site built, not another copy of the status.
	require.Equal(t, exit.CodeGeneric, exit.Code(errors.Unwrap(err)),
		"the status must be pinned once, not layered")

	assertEngineFailure(t, l)
}

// TestLauncher_StartupFailure_TLSKeyPair covers the one site that overrides the
// classifier: a TLS pair the OS handed over intact but that will not parse is a
// configuration error, while a pair the OS refused keeps whatever its errno
// says. The two cases must not collapse into one status -- 78 is on the
// non-restartable list in EXIT_CODES.md, and a path mistake is not the same
// mistake as a corrupt certificate.
func TestLauncher_StartupFailure_TLSKeyPair(t *testing.T) {
	tests := []struct {
		name string
		// write returns the cert and key paths to start with.
		write func(t *testing.T, dir string) (cert, key string)
		want  int
		// wantMsg keeps a case honest: every phase before runHTTP can fail with
		// a status of its own, so the error has to be shown to come from the
		// key pair and not from something earlier that happens to agree.
		wantMsg string
	}{
		{
			name: "unreadable path",
			write: func(t *testing.T, dir string) (string, string) {
				// A regular file where a directory belongs, so opening the
				// cert underneath it fails with ENOTDIR rather than ENOENT --
				// an errno no io/fs sentinel matches, which is what makes this
				// the case worth pinning: only the errno table can place it.
				blocker := filepath.Join(dir, "certs")
				require.NoError(t, os.WriteFile(blocker, []byte("not a directory"), 0600))
				return filepath.Join(blocker, "influxd.crt"), filepath.Join(blocker, "influxd.key")
			},
			want:    exit.CodeNoInput,
			wantMsg: "not a directory",
		},
		{
			name: "unparseable pair",
			write: func(t *testing.T, dir string) (string, string) {
				cert := filepath.Join(dir, "influxd.crt")
				key := filepath.Join(dir, "influxd.key")
				require.NoError(t, os.WriteFile(cert, []byte("not a certificate"), 0600))
				require.NoError(t, os.WriteFile(key, []byte("not a key"), 0600))
				return cert, key
			},
			want:    exit.CodeConfig,
			wantMsg: "PEM",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := launcher.NewTestLauncherServer()
			cert, key := tt.write(t, l.Path)

			defer func() { require.NoError(t, l.Shutdown(ctx)) }()

			err := l.Run(t, ctx, func(o *launcher.InfluxdOpts) {
				o.HttpTLSCert = cert
				o.HttpTLSKey = key
			})
			require.Error(t, err, "runHTTP must reject this key pair")
			require.ErrorContains(t, err, tt.wantMsg,
				"startup failed somewhere other than the key pair")
			require.Equal(t, tt.want, exit.Code(err),
				"a %s TLS pair must exit %s, got %s",
				tt.name, exit.Name(tt.want), exit.Name(exit.Code(err)))
		})
	}
}
