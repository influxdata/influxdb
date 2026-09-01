package launcher

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/kit/check"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"
)

// requireReturnsWithin fails the test unless fn returns within d.
//
// On timeout the goroutine running fn is abandoned, still holding whatever fn
// holds: there is no way to interrupt it from here. So fn must have an
// independent way out — cancelling the launcher context ends a hold — and the
// caller must arrange one, typically as a t.Cleanup, rather than leaving a
// blocked goroutine to outlive the test binary's other cases.
//
// It exists because the alternative is calling a blocking method directly:
// a regression that never returns then hangs the whole package instead of
// failing one subtest.
func requireReturnsWithin(t *testing.T, d time.Duration, fn func()) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()

	select {
	case <-done:
	case <-time.After(d):
		require.FailNowf(t, "call did not return", "still blocked after %s", d)
	}
}

// newShutdownLauncher returns a Launcher with just enough wired up to tear
// down: a logger, a check handler, and the run context holdForStartupError
// waits on. No subsystem is started; closers are added by the test.
func newShutdownLauncher(t *testing.T) *Launcher {
	t.Helper()

	m := NewLauncher()
	m.log = zaptest.NewLogger(t)
	m.checkHandler = http.NewHealthReadyHandler(m.log)

	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	m.doneChan = ctx.Done()
	t.Cleanup(cancel)

	return m
}

// recordCloser registers a closer under label that appends its label to *order
// when it runs and then returns err. Closers run serially under shutdownMu, so
// the slice needs no lock of its own.
func recordCloser(m *Launcher, order *[]string, label string, err error) {
	m.closers = append(m.closers, labeledCloser{
		label: label,
		closer: func(context.Context) error {
			*order = append(*order, label)
			return err
		},
	})
}

// TestLauncher_ShutdownPhases pins the split: phase 1 releases everything but
// the listener and the PID file, phase 2 releases those two, and no closer runs
// twice across the two. The PID file goes last of all, which is reverse
// registration order -- it is written before anything else is opened.
func TestLauncher_ShutdownPhases(t *testing.T) {
	ctx := context.Background()
	m := newShutdownLauncher(t)

	var order []string
	recordCloser(m, &order, SubsystemPIDFile, nil)
	recordCloser(m, &order, SubsystemKV, nil)
	recordCloser(m, &order, SubsystemHTTPServer, nil)
	recordCloser(m, &order, SubsystemEngine, nil)

	require.NoError(t, m.shutdownSubsystems(ctx))
	require.Equal(t, []string{SubsystemEngine, SubsystemKV}, order,
		"phase 1 must run every closer but the listener's and the PID file's, "+
			"once, in reverse registration order")

	require.NoError(t, m.Shutdown(ctx))
	require.Equal(t,
		[]string{SubsystemEngine, SubsystemKV, SubsystemHTTPServer, SubsystemPIDFile},
		order, "phase 2 must run the two kept closers and nothing else again")
}

// TestLauncher_ShutdownWithoutPhaseOne is the unsplit path, which is what every
// existing caller takes and what --startup-error-linger=0 still takes.
func TestLauncher_ShutdownWithoutPhaseOne(t *testing.T) {
	ctx := context.Background()
	m := newShutdownLauncher(t)

	var order []string
	recordCloser(m, &order, SubsystemKV, nil)
	recordCloser(m, &order, SubsystemHTTPServer, nil)

	require.NoError(t, m.Shutdown(ctx))
	require.Equal(t, []string{SubsystemHTTPServer, SubsystemKV}, order)

	require.NoError(t, m.Shutdown(ctx), "a second Shutdown must be a no-op")
	require.Len(t, order, 2)
}

// TestLauncher_ShutdownAccumulatesErrors pins that a single Shutdown call site
// reports failures from both phases, which is what lets cmdRunE call Shutdown
// unconditionally and still report the whole teardown. Each failure stays
// individually matchable: the accumulated error joins them rather than
// flattening them into one message.
func TestLauncher_ShutdownAccumulatesErrors(t *testing.T) {
	ctx := context.Background()
	m := newShutdownLauncher(t)

	errKV := errors.New("kv close failed")
	errListener := errors.New("listener close failed")

	var order []string
	recordCloser(m, &order, SubsystemKV, errKV)
	recordCloser(m, &order, SubsystemHTTPServer, errListener)

	err := m.shutdownSubsystems(ctx)
	require.ErrorIs(t, err, errKV)
	require.NotErrorIs(t, err, errListener, "phase 1 has not touched the listener")
	require.ErrorContains(t, err, SubsystemKV, "a closer failure must name its subsystem")

	err = m.Shutdown(ctx)
	require.ErrorIs(t, err, errKV)
	require.ErrorIs(t, err, errListener)
	require.ErrorContains(t, err, SubsystemHTTPServer)

	again := m.Shutdown(ctx)
	require.Equal(t, err.Error(), again.Error(),
		"a repeat call must report the same accumulated error")
	require.Len(t, order, 2, "a repeat call must run nothing")
}

// TestLauncher_HoldForStartupError_NoWait covers every case in which the hold
// declines to do anything at all: there is nothing to scrape, or the operator
// asked for no window. Nothing may be torn down and nothing may be frozen —
// the caller's Shutdown still owns the whole teardown, exactly as before this
// flag existed.
func TestLauncher_HoldForStartupError_NoWait(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name        string
		linger      time.Duration
		httpServing bool
	}{
		{name: "zero linger", linger: 0, httpServing: true},
		{name: "negative linger", linger: -time.Second, httpServing: true},
		{name: "no listener", linger: time.Hour, httpServing: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newShutdownLauncher(t)
			if tc.httpServing {
				m.setHTTPServing()
			}

			var order []string
			recordCloser(m, &order, SubsystemKV, nil)
			recordCloser(m, &order, SubsystemHTTPServer, nil)

			requireReturnsWithin(t, time.Second, func() {
				m.holdForStartupError(ctx, tc.linger)
			})
			require.Empty(t, order, "the hold tore something down")

			// Not frozen: a check registered afterwards still appears.
			m.checkHandler.AddNamedHealthCheck(check.Named("late", check.ErrCheck(func() error { return nil })))
			require.Contains(t, healthCheckNames(t, m), "late")
		})
	}
}

// TestLauncher_HoldForStartupError_ReleasesAndWaits is the shape of the real
// window: everything but the listener and the PID file is released before the
// wait, those two survive it, and the wait ends when the launcher context is
// cancelled — which is what a SIGINT does.
func TestLauncher_HoldForStartupError_ReleasesAndWaits(t *testing.T) {
	ctx := context.Background()
	m := newShutdownLauncher(t)
	m.setHTTPServing()

	var order []string
	recordCloser(m, &order, SubsystemPIDFile, nil)
	recordCloser(m, &order, SubsystemKV, nil)
	recordCloser(m, &order, SubsystemHTTPServer, nil)

	// Long enough that the timer cannot be what ends this: only the cancel can.
	const linger = time.Hour
	released := make(chan struct{})
	go func() {
		// The hold releases the subsystems before it parks, so this fires well
		// before the cancel below. Only kv is released: the listener and the
		// PID file are both held for the window.
		for {
			m.shutdownMu.Lock()
			done := len(order) == 1
			m.shutdownMu.Unlock()
			if done {
				close(released)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	go func() {
		<-released
		m.cancel()
	}()

	requireReturnsWithin(t, 30*time.Second, func() {
		m.holdForStartupError(ctx, linger)
	})

	require.Equal(t, []string{SubsystemKV}, order,
		"the listener's and the PID file's closers must survive the window")
	require.NoError(t, m.Shutdown(ctx))
	require.Equal(t, []string{SubsystemKV, SubsystemHTTPServer, SubsystemPIDFile}, order)
}

// TestLauncher_HoldForStartupError_AlreadyDone covers a launcher whose context
// was cancelled before the hold began — a serve goroutine that gave up, or a
// signal that arrived during the failure itself. The teardown still runs; only
// the wait is skipped.
func TestLauncher_HoldForStartupError_AlreadyDone(t *testing.T) {
	ctx := context.Background()
	m := newShutdownLauncher(t)
	m.setHTTPServing()
	m.cancel()

	var order []string
	recordCloser(m, &order, SubsystemKV, nil)
	recordCloser(m, &order, SubsystemHTTPServer, nil)

	requireReturnsWithin(t, 5*time.Second, func() {
		m.holdForStartupError(ctx, time.Hour)
	})
	require.Equal(t, []string{SubsystemKV}, order)
}

// TestLauncher_HoldForStartupError_ElapsesTimer pins that the timer alone ends
// the wait, with no cancel involved. Short enough to keep the test quick, long
// enough that a hold returning instantly would be visible as an ordering
// failure rather than passing by luck.
func TestLauncher_HoldForStartupError_ElapsesTimer(t *testing.T) {
	ctx := context.Background()
	m := newShutdownLauncher(t)
	m.setHTTPServing()

	const linger = 50 * time.Millisecond
	start := time.Now()
	requireReturnsWithin(t, 30*time.Second, func() {
		m.holdForStartupError(ctx, linger)
	})
	require.GreaterOrEqual(t, time.Since(start), linger)
}

// TestLauncher_FreezeChecks_PinsStartupAttribution runs the freeze over the
// state a real startup failure leaves behind: one subsystem latched with its
// reason, and every gate downstream of it reporting that it was never reached.
// Both envelopes must survive the freeze unchanged, and the set must be closed
// to anything registered afterwards — a closer that registers a check while
// tearing down cannot rewrite the report.
func TestLauncher_FreezeChecks_PinsStartupAttribution(t *testing.T) {
	ctx := context.Background()
	m := newCheckLauncher(t)

	require.Error(t, m.failSubsystem(SubsystemEngine, "Failed to open engine",
		errors.New("not a directory")))
	m.failUnreachedGates(ctx)

	healthBefore, healthStatusBefore := serveCheck(t, m, "/health")
	readyBefore, readyStatusBefore := serveCheck(t, m, "/ready")

	m.freezeChecks(ctx)

	m.checkHandler.AddNamedHealthCheck(check.Named("late", check.ErrCheck(func() error {
		return errors.New("registered while tearing down")
	})))
	m.checkHandler.AddNamedReadyCheck(check.Named("late", check.ErrCheck(func() error {
		return errors.New("registered while tearing down")
	})))

	healthAfter, healthStatusAfter := serveCheck(t, m, "/health")
	readyAfter, readyStatusAfter := serveCheck(t, m, "/ready")

	require.Equal(t, healthBefore, healthAfter)
	require.Equal(t, healthStatusBefore, healthStatusAfter)
	require.Equal(t, readyBefore, readyAfter)
	require.Equal(t, readyStatusBefore, readyStatusAfter)

	require.NotContains(t, checkNamesOf(healthAfter.Checks), "late")
	require.NotContains(t, checkNamesOf(readyAfter.Checks), "late")

	// The attribution itself, so this test fails loudly if the freeze ever
	// starts pinning an empty report.
	require.Contains(t, healthAfter.Message, "Failed to open engine")
	require.Contains(t, checkNamesOf(healthAfter.Checks), SubsystemEngine)
}

func checkNamesOf(rs []check.BasicResponse) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r.Name()
	}
	return out
}

// healthCheckNames returns the names on the launcher's /health envelope.
func healthCheckNames(t *testing.T, m *Launcher) []string {
	t.Helper()
	body, _ := serveCheck(t, m, "/health")
	return checkNamesOf(body.Checks)
}

// TestLauncher_CappedLinger pins the upper bound on --startup-error-linger.
// The window holds the HTTP port on a process that has already failed, and the
// supervisor that would restart it is waiting on that process to exit, so an
// operator typo — an hour meant as a minute, a duration string read as
// something else — must not be able to turn a failed start into an indefinite
// outage. The warning is pinned alongside the value because it is the only
// notice an operator gets that the duration they chose is not the one they
// will get.
func TestLauncher_CappedLinger(t *testing.T) {
	for _, tc := range []struct {
		name     string
		in       time.Duration
		want     time.Duration
		wantWarn bool
	}{
		{name: "under the cap", in: 30 * time.Second, want: 30 * time.Second},
		{name: "at the cap", in: maxStartupErrorLinger, want: maxStartupErrorLinger},
		{name: "over the cap", in: 24 * time.Hour, want: maxStartupErrorLinger, wantWarn: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newShutdownLauncher(t)
			core, logs := observer.New(zap.WarnLevel)
			m.log = zap.New(core)

			require.Equal(t, tc.want, m.cappedLinger(tc.in))

			if !tc.wantWarn {
				require.Zero(t, logs.Len(), "capping nothing must not warn")
				return
			}
			require.Equal(t, 1, logs.Len(),
				"an operator whose value was capped must be told exactly once")
			entry := logs.All()[0]
			require.Contains(t, entry.Message, "capping")
			require.Equal(t, startupErrorLingerFlag, entry.ContextMap()["flag"],
				"the warning must name the flag to be actionable on its own")
		})
	}
}
