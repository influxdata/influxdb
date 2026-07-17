package tlsconfig

import (
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	th "github.com/influxdata/influxdb/pkg/testing/helper"
	"github.com/influxdata/influxdb/pkg/testing/selfsigned"
)

// newExpiredCert returns a self-signed certificate that expired a week ago.
func newExpiredCert(t *testing.T) *selfsigned.Cert {
	t.Helper()
	notAfter := time.Now().UTC().Truncate(time.Hour).Add(-7 * 24 * time.Hour)
	return selfsigned.NewSelfSignedCert(t,
		selfsigned.WithNotBefore(notAfter.Add(-7*24*time.Hour)),
		selfsigned.WithNotAfter(notAfter))
}

// newQuietCertLoader creates a cert loader whose own logging is discarded, so
// only the monitor's log output is observed.
func newQuietCertLoader(t *testing.T, monitor *TLSCertMonitor, certPath, keyPath, usage string) *TLSCertLoader {
	t.Helper()
	cl, err := NewTLSCertLoader(
		ServerOnlyRole,
		monitor,
		WithCertLoaderCertificate(certPath, keyPath),
		WithCertLoaderLogger(zap.NewNop()),
		WithCertLoaderUsage(usage))
	require.NoError(t, err)
	require.NotNil(t, cl)
	return cl
}

// takeMessages drains every observed entry and returns those matching msg.
// It drains rather than filters so that each call only reports entries logged
// since the previous call: ObservedLogs.FilterMessage returns a copy, and
// calling TakeAll on that copy leaves the original entries in place.
func takeMessages(logs *observer.ObservedLogs, msg string) []observer.LoggedEntry {
	var matched []observer.LoggedEntry
	for _, e := range logs.TakeAll() {
		if e.Message == msg {
			matched = append(matched, e)
		}
	}
	return matched
}

func takeExpiredWarnings(logs *observer.ObservedLogs) []observer.LoggedEntry {
	return takeMessages(logs, "Certificate is expired")
}

func TestTLSCertMonitor_Defaults(t *testing.T) {
	t.Run("unset options use defaults", func(t *testing.T) {
		m := NewTLSCertMonitor()
		require.NotNil(t, m)

		require.Equal(t, DefaultCertificateCheckTime, m.checkInterval())
		require.Equal(t, DefaultExpirationWarnTime, m.expirationAdvanced())
		require.Equal(t, DefaultTriggerDelay, m.triggerDelayInterval)
		require.NotNil(t, m.logger(), "a monitor must always have a usable logger")
	})

	t.Run("non-positive durations fall back to defaults", func(t *testing.T) {
		m := NewTLSCertMonitor(
			WithMonitorCheckInterval(0),
			WithMonitorExpirationAdvanced(-time.Second))
		require.NotNil(t, m)

		require.Equal(t, DefaultCertificateCheckTime, m.checkInterval())
		require.Equal(t, DefaultExpirationWarnTime, m.expirationAdvanced())
	})

	t.Run("options are honored", func(t *testing.T) {
		m := NewTLSCertMonitor(
			WithMonitorCheckInterval(time.Minute),
			WithMonitorExpirationAdvanced(time.Hour),
			WithMonitorTriggerDelay(time.Second))
		require.NotNil(t, m)

		require.Equal(t, time.Minute, m.checkInterval())
		require.Equal(t, time.Hour, m.expirationAdvanced())
		require.Equal(t, time.Second, m.triggerDelayInterval)
	})
}

func TestTLSCertMonitor_OpenCloseIdempotent(t *testing.T) {
	m := NewTLSCertMonitor(WithMonitorCheckInterval(time.Hour))
	require.NotNil(t, m)

	// Open starts exactly one goroutine no matter how many times it is called.
	require.NoError(t, m.Open())
	require.NoError(t, m.Open())
	m.WaitForMonitorStart()

	require.NoError(t, m.Close())
	require.NoError(t, m.Close())
	m.WaitForMonitorStop()
}

func TestTLSCertMonitor_SetLogger(t *testing.T) {
	ss := newExpiredCert(t)

	coreA, logsA := observer.New(zapcore.InfoLevel)
	coreB, logsB := observer.New(zapcore.InfoLevel)

	// A long check interval keeps the periodic check from firing so the test
	// controls exactly when certificates are checked.
	monitor := newTestCertMonitor(t,
		WithMonitorLogger(zap.New(coreA)),
		WithMonitorCheckInterval(time.Hour),
		WithMonitorTriggerDelay(time.Millisecond))
	defer th.CheckedClose(t, monitor)()

	cl := newQuietCertLoader(t, monitor, ss.CertPath, ss.KeyPath, "httpd.server")
	defer th.CheckedClose(t, cl)()

	// The load queued at construction warns through the original logger.
	time.Sleep(testWarnWaitTime)
	require.Len(t, takeExpiredWarnings(logsA), 1)
	require.Empty(t, takeExpiredWarnings(logsB))

	monitor.SetLogger(zap.New(coreB))

	// A subsequent check must warn through the new logger only.
	require.NoError(t, cl.Load(ss.CertPath, ss.KeyPath))
	time.Sleep(testWarnWaitTime)

	require.Len(t, takeExpiredWarnings(logsB), 1, "warnings should go to the logger set by SetLogger")
	require.Empty(t, takeExpiredWarnings(logsA), "the replaced logger should no longer receive warnings")
}

func TestTLSCertMonitor_SetCheckInterval(t *testing.T) {
	ss := newExpiredCert(t)

	core, logs := observer.New(zapcore.InfoLevel)

	// Both the periodic check and the queued trigger are effectively disabled,
	// so nothing is checked until SetCheckInterval shortens the interval.
	monitor := newTestCertMonitor(t,
		WithMonitorLogger(zap.New(core)),
		WithMonitorCheckInterval(time.Hour),
		WithMonitorTriggerDelay(time.Hour))
	defer th.CheckedClose(t, monitor)()

	cl := newQuietCertLoader(t, monitor, ss.CertPath, ss.KeyPath, "httpd.server")
	defer th.CheckedClose(t, cl)()

	time.Sleep(testWarnWaitTime)
	require.Empty(t, takeExpiredWarnings(logs), "no check should have run yet")

	monitor.SetCheckInterval(testCheckTime)
	require.Equal(t, testCheckTime, monitor.checkInterval())

	time.Sleep(testCheckCapture)
	require.NotEmpty(t, takeExpiredWarnings(logs), "shortened check interval should trigger a periodic check")
}

func TestTLSCertMonitor_SetExpirationAdvanced(t *testing.T) {
	// A certificate that expires in a day: inside a 48 hour warn window but
	// outside a one hour window.
	notAfter := time.Now().UTC().Truncate(time.Hour).Add(24 * time.Hour)
	ss := selfsigned.NewSelfSignedCert(t,
		selfsigned.WithNotBefore(time.Now().UTC().Add(-24*time.Hour)),
		selfsigned.WithNotAfter(notAfter))

	core, logs := observer.New(zapcore.InfoLevel)

	monitor := newTestCertMonitor(t,
		WithMonitorLogger(zap.New(core)),
		WithMonitorCheckInterval(time.Hour),
		WithMonitorExpirationAdvanced(time.Hour),
		WithMonitorTriggerDelay(time.Millisecond))
	defer th.CheckedClose(t, monitor)()

	cl := newQuietCertLoader(t, monitor, ss.CertPath, ss.KeyPath, "httpd.server")
	defer th.CheckedClose(t, cl)()

	time.Sleep(testWarnWaitTime)
	require.Empty(t, takeMessages(logs, "Certificate will expire soon"),
		"certificate expires outside the one hour warn window")

	// Widening the warn window re-checks the registered certificates, so the
	// same certificate now warns without needing a reload.
	monitor.SetExpirationAdvanced(48 * time.Hour)
	require.Equal(t, 48*time.Hour, monitor.expirationAdvanced())

	time.Sleep(testWarnWaitTime)
	warnings := takeMessages(logs, "Certificate will expire soon")
	require.Len(t, warnings, 1, "widened warn window should trigger a re-check")
	require.Equal(t, ss.CertPath, warnings[0].ContextMap()["cert"])
	require.Equal(t, notAfter, warnings[0].ContextMap()["NotAfter"])
}

func TestTLSCertMonitor_SetTriggerDelay(t *testing.T) {
	ss := newExpiredCert(t)

	core, logs := observer.New(zapcore.InfoLevel)

	monitor := newTestCertMonitor(t,
		WithMonitorLogger(zap.New(core)),
		WithMonitorCheckInterval(time.Hour),
		WithMonitorTriggerDelay(time.Hour))
	defer th.CheckedClose(t, monitor)()

	cl := newQuietCertLoader(t, monitor, ss.CertPath, ss.KeyPath, "httpd.server")
	defer th.CheckedClose(t, cl)()

	time.Sleep(testWarnWaitTime)
	require.Empty(t, takeExpiredWarnings(logs), "the long trigger delay should defer the queued check")

	// SetTriggerDelay only affects the next queued trigger; it does not reset a
	// trigger that is already pending.
	monitor.SetTriggerDelay(time.Millisecond)
	time.Sleep(testWarnWaitTime)
	require.Empty(t, takeExpiredWarnings(logs), "SetTriggerDelay should not fire an already pending trigger")

	// Queueing a new warning picks up the shortened delay.
	require.NoError(t, cl.Load(ss.CertPath, ss.KeyPath))
	time.Sleep(testWarnWaitTime)
	require.NotEmpty(t, takeExpiredWarnings(logs), "next queued trigger should use the new delay")
}

func TestTLSCertMonitor_DuplicateCertificatesGrouped(t *testing.T) {
	ss := newExpiredCert(t)

	core, logs := observer.New(zapcore.InfoLevel)

	monitor := newTestCertMonitor(t,
		WithMonitorLogger(zap.New(core)),
		WithMonitorCheckInterval(time.Hour),
		WithMonitorTriggerDelay(time.Hour))
	defer th.CheckedClose(t, monitor)()

	// Two services sharing one certificate should produce one warning listing
	// both usages, rather than one warning per service.
	cl1 := newQuietCertLoader(t, monitor, ss.CertPath, ss.KeyPath, "opentsdb.server")
	defer th.CheckedClose(t, cl1)()
	cl2 := newQuietCertLoader(t, monitor, ss.CertPath, ss.KeyPath, "httpd.server")
	defer th.CheckedClose(t, cl2)()

	monitor.SetCheckInterval(testCheckTime)
	time.Sleep(testCheckCapture)

	warnings := takeExpiredWarnings(logs)
	require.Len(t, warnings, 1, "a shared certificate should only be reported once")
	require.Equal(t, ss.CertPath, warnings[0].ContextMap()["cert"])
	require.Equal(t, []any{"httpd.server", "opentsdb.server"}, warnings[0].ContextMap()["usages"],
		"usages should be merged and sorted")
}

func TestTLSCertMonitor_DistinctCertificatesLoggedSeparately(t *testing.T) {
	ss1 := newExpiredCert(t)
	ss2 := newExpiredCert(t)

	core, logs := observer.New(zapcore.InfoLevel)

	monitor := newTestCertMonitor(t,
		WithMonitorLogger(zap.New(core)),
		WithMonitorCheckInterval(time.Hour),
		WithMonitorTriggerDelay(time.Hour))
	defer th.CheckedClose(t, monitor)()

	cl1 := newQuietCertLoader(t, monitor, ss1.CertPath, ss1.KeyPath, "httpd.server")
	defer th.CheckedClose(t, cl1)()
	cl2 := newQuietCertLoader(t, monitor, ss2.CertPath, ss2.KeyPath, "opentsdb.server")
	defer th.CheckedClose(t, cl2)()

	monitor.SetCheckInterval(testCheckTime)
	time.Sleep(testCheckCapture)

	warnings := takeExpiredWarnings(logs)
	require.Len(t, warnings, 2, "distinct certificates must each be reported")

	byCert := make(map[string][]any, len(warnings))
	for _, w := range warnings {
		byCert[w.ContextMap()["cert"].(string)] = w.ContextMap()["usages"].([]any)
	}
	require.Equal(t, []any{"httpd.server"}, byCert[ss1.CertPath])
	require.Equal(t, []any{"opentsdb.server"}, byCert[ss2.CertPath])
}

func TestTLSCertMonitor_UnregisterStopsMonitoring(t *testing.T) {
	ss := newExpiredCert(t)

	core, logs := observer.New(zapcore.InfoLevel)

	monitor := newTestCertMonitor(t,
		WithMonitorLogger(zap.New(core)),
		WithMonitorCheckInterval(time.Hour),
		WithMonitorTriggerDelay(time.Hour))
	defer th.CheckedClose(t, monitor)()

	cl := newQuietCertLoader(t, monitor, ss.CertPath, ss.KeyPath, "httpd.server")
	require.Len(t, takeMessages(logs, "Registered certificate loader"), 1)

	// Closing the loader unregisters it, so its certificate is no longer checked.
	require.NoError(t, cl.Close())
	unregistered := takeMessages(logs, "Unregistered certificate loader")
	require.Len(t, unregistered, 1)
	require.Equal(t, "httpd.server", unregistered[0].ContextMap()["usage"])

	// Close is safe to call more than once and only unregisters once.
	require.NoError(t, cl.Close())
	require.Empty(t, takeMessages(logs, "Unregistered certificate loader"),
		"a second Close should not log another unregistration")

	monitor.SetCheckInterval(testCheckTime)
	time.Sleep(testCheckCapture)
	require.Empty(t, takeExpiredWarnings(logs), "an unregistered certificate should not be checked")
}

func TestTLSCertMonitor_ClosedMonitorDoesNotBlock(t *testing.T) {
	ss := newExpiredCert(t)

	monitor := newTestCertMonitor(t, WithMonitorCheckInterval(time.Hour))
	cl := newQuietCertLoader(t, monitor, ss.CertPath, ss.KeyPath, "httpd.server")
	defer th.CheckedClose(t, cl)()

	require.NoError(t, monitor.Close())
	monitor.WaitForMonitorStop()

	// Once the monitor goroutine has stopped nothing drains the reset channels.
	// These calls must not block, and loads must keep working. The counts here
	// exceed the reset channel depth to catch a regression that only appears
	// once the buffer fills.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 50 {
			monitor.SetCheckInterval(time.Minute)
			monitor.SetExpirationAdvanced(time.Hour)
			require.NoError(t, cl.Load(ss.CertPath, ss.KeyPath))
		}
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("monitor calls blocked after the monitor was closed")
	}
}

func TestTLSCertMonitor_LogIssuesGuards(t *testing.T) {
	monitor := newTestCertMonitor(t, WithMonitorCheckInterval(time.Hour))
	defer th.CheckedClose(t, monitor)()

	core, logs := observer.New(zapcore.DebugLevel)
	log := zap.New(core)

	t.Run("nil logger does not panic", func(t *testing.T) {
		require.NotPanics(t, func() {
			monitor.logIssues(nil, LoadedCertificate{valid: true, CertificatePath: "cert.pem"})
		})
	})

	t.Run("empty certificate reports nothing", func(t *testing.T) {
		monitor.logIssues(log, LoadedCertificate{})
		require.Zero(t, logs.Len(), "an empty certificate has no issues to report")
	})

	t.Run("unusable leaf is reported", func(t *testing.T) {
		// A certificate with no leaf cannot come out of LoadCertificate. If one
		// ever appears, the monitor should say so rather than skip it silently.
		monitor.logIssues(log, LoadedCertificate{valid: true, CertificatePath: "cert.pem"})

		entries := takeMessages(logs, "error logging certificate issues")
		require.Len(t, entries, 1)
		require.Equal(t, zap.ErrorLevel, entries[0].Level)
	})
}

// TestTLSCertMonitor_RotatedCertificateAtSamePath covers two loaders reading the
// same certificate path where one loaded before the file was replaced. They are
// grouped by serial as well as path, so both the stale and the current
// certificate are reported.
func TestTLSCertMonitor_RotatedCertificateAtSamePath(t *testing.T) {
	copyFile := func(t *testing.T, src, dst string) {
		t.Helper()
		data, err := os.ReadFile(src)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(dst, data, 0600))
	}

	dir := t.TempDir()
	certPath := path.Join(dir, "cert.pem")
	keyPath := path.Join(dir, "key.pem")

	core, logs := observer.New(zapcore.InfoLevel)

	monitor := newTestCertMonitor(t,
		WithMonitorLogger(zap.New(core)),
		WithMonitorCheckInterval(time.Hour),
		WithMonitorTriggerDelay(time.Hour))
	defer th.CheckedClose(t, monitor)()

	original := newExpiredCert(t)
	copyFile(t, original.CertPath, certPath)
	copyFile(t, original.KeyPath, keyPath)

	cl1 := newQuietCertLoader(t, monitor, certPath, keyPath, "httpd.server")
	defer th.CheckedClose(t, cl1)()

	// Replace the files on disk. cl1 keeps serving the certificate it already
	// loaded; cl2 picks up the replacement.
	rotated := newExpiredCert(t)
	copyFile(t, rotated.CertPath, certPath)
	copyFile(t, rotated.KeyPath, keyPath)

	cl2 := newQuietCertLoader(t, monitor, certPath, keyPath, "opentsdb.server")
	defer th.CheckedClose(t, cl2)()

	originalSerial := cl1.Leaf().SerialNumber.String()
	rotatedSerial := cl2.Leaf().SerialNumber.String()
	require.NotEqual(t, originalSerial, rotatedSerial, "the rotated certificate should be a different one")

	monitor.SetCheckInterval(testCheckTime)
	time.Sleep(testCheckCapture)

	warnings := takeExpiredWarnings(logs)
	require.Len(t, warnings, 2, "certificates sharing a path but not a serial are reported separately")

	bySerial := make(map[string][]any, len(warnings))
	for _, w := range warnings {
		require.Equal(t, certPath, w.ContextMap()["cert"])
		bySerial[w.ContextMap()["serial"].(string)] = w.ContextMap()["usages"].([]any)
	}
	require.Equal(t, []any{"httpd.server"}, bySerial[originalSerial])
	require.Equal(t, []any{"opentsdb.server"}, bySerial[rotatedSerial])
}
