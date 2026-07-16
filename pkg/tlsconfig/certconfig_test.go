package tlsconfig

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	th "github.com/influxdata/influxdb/pkg/testing/helper"
	"github.com/influxdata/influxdb/pkg/testing/selfsigned"
)

func TestTLSCertLoader_HappyPath(t *testing.T) {
	const DNSName = "data1.influxdata.edge"
	ss := selfsigned.NewSelfSignedCert(t, selfsigned.WithDNSName(DNSName))

	core, logs := observer.New(zapcore.InfoLevel)
	logger := zap.New(core)

	certMonitor := newTestCertMonitor(t, WithMonitorLogger(logger))
	defer th.CheckedClose(t, certMonitor)()

	// We should be able to call WaitForMonitorStart multiple times without issues.
	certMonitor.WaitForMonitorStart()
	certMonitor.WaitForMonitorStart()

	// Start cert loader
	usage := "data.server"
	cl, err := NewTLSCertLoader(
		ServerOnlyRole,
		certMonitor,
		WithCertLoaderCertificate(ss.CertPath, ss.KeyPath),
		WithCertLoaderLogger(logger),
		WithCertLoaderUsage(usage))
	require.NoError(t, err)
	require.NotNil(t, cl)
	defer th.CheckedClose(t, cl)()

	{
		// Check for expected log output
		require.Equal(t, 5, logs.Len())
		logLines := logs.TakeAll()
		require.Equal(t, "Starting TLS certificate monitor", logLines[0].Message)
		require.Equal(t, "Loading TLS certificate (start)", logLines[1].Message)
		require.Equal(t, "Successfully loaded TLS certificate", logLines[2].Message)
		require.Equal(t, "Loading TLS certificate (end)", logLines[3].Message)
		require.Equal(t, "Registered certificate loader", logLines[4].Message)
		for _, l := range logLines[1:3] { // "Starting TLS certificate monitor" doesn't include the cert name and key
			cm := l.ContextMap()
			require.Equal(t, usage, cm["usage"])
			require.Equal(t, ss.CertPath, cm["cert"])
			require.Equal(t, ss.KeyPath, cm["key"])
		}
		require.Equal(t, usage, logLines[4].ContextMap()["usage"])

		// Get certificate and do some checks on it.
		cp, kp := cl.Paths()
		require.Equal(t, ss.CertPath, cp)
		require.Equal(t, ss.KeyPath, kp)
		require.NotNil(t, cl.Certificate())
		cert, err := cl.GetCertificate(nil)
		require.NoError(t, err)
		require.NotNil(t, cert)
		require.Equal(t, cl.Certificate(), cert)
		require.NotEmpty(t, cert.Certificate, "expected at least 1 certificate")
		require.NotNil(t, cl.Leaf())
		x509Cert, err := x509.ParseCertificate(cert.Certificate[0])
		require.NoError(t, err)
		require.NotNil(t, x509Cert)
		require.Equal(t, []string{DNSName}, x509Cert.DNSNames)
		require.Equal(t, x509Cert, cl.Leaf())
	}

	{
		// Create new certificate and reload
		const DNSName2 = "data1-ultimate-form.influxdata.edge"
		logs.TakeAll()
		ss := selfsigned.NewSelfSignedCert(t, selfsigned.WithDNSName(DNSName2))
		require.NoError(t, cl.Load(ss.CertPath, ss.KeyPath))

		require.Equal(t, 3, logs.Len())
		logLines := logs.TakeAll()
		require.Equal(t, "Loading TLS certificate (start)", logLines[0].Message)
		require.Equal(t, "Successfully loaded TLS certificate", logLines[1].Message)
		require.Equal(t, "Loading TLS certificate (end)", logLines[2].Message)
		for _, l := range logLines {
			cm := l.ContextMap()
			require.Equal(t, ss.CertPath, cm["cert"])
			require.Equal(t, ss.KeyPath, cm["key"])
			require.Equal(t, usage, cm["usage"])
		}

		cp, kp := cl.Paths()
		require.Equal(t, ss.CertPath, cp)
		require.Equal(t, ss.KeyPath, kp)
		require.NotNil(t, cl.Certificate())
		cert, err := cl.GetCertificate(nil)
		require.NoError(t, err)
		require.NotNil(t, cert)
		require.Equal(t, cl.Certificate(), cert)
		require.NotEmpty(t, cert.Certificate, "expected at least 1 certificate")
		require.NotNil(t, cl.Leaf())
		x509Cert, err := x509.ParseCertificate(cert.Certificate[0])
		require.NoError(t, err)
		require.NotNil(t, x509Cert)
		require.Equal(t, []string{DNSName2}, x509Cert.DNSNames)
		require.Equal(t, x509Cert, cl.Leaf())
	}

	{
		// Close everything and check for proper logs.
		logs.TakeAll()
		require.NoError(t, cl.Close())
		require.NoError(t, certMonitor.Close())

		// Should be able to call WaitForMonitorStop multiple times.
		certMonitor.WaitForMonitorStop()
		certMonitor.WaitForMonitorStop()

		require.Equal(t, 2, logs.Len())
		logLines := logs.TakeAll()
		require.Equal(t, "Unregistered certificate loader", logLines[0].Message)
		cm := logLines[0].ContextMap()
		require.Equal(t, usage, cm["usage"])

		require.Equal(t, "Stopping TLS certificate monitor", logLines[1].Message)
	}
}

func TestTLSCertLoader_GoodCertPersists(t *testing.T) {
	const DNSName = "data1.influxdata.edge"
	ss := selfsigned.NewSelfSignedCert(t, selfsigned.WithDNSName(DNSName))

	core, logs := observer.New(zapcore.InfoLevel)
	logger := zap.New(core)

	certMonitor := newTestCertMonitor(t, WithMonitorLogger(logger))
	defer th.CheckedClose(t, certMonitor)()

	// Start cert loader
	cl, err := NewTLSCertLoader(
		ServerOnlyRole,
		certMonitor,
		WithCertLoaderCertificate(ss.CertPath, ss.KeyPath),
		WithCertLoaderLogger(logger))
	require.NoError(t, err)
	require.NotNil(t, cl)
	defer th.CheckedClose(t, cl)()

	var goodSerial big.Int
	{
		cp, kp := cl.Paths()
		require.Equal(t, ss.CertPath, cp)
		require.Equal(t, ss.KeyPath, kp)

		cert, err := cl.GetCertificate(nil)
		require.NoError(t, err)
		require.NotNil(t, cert)
		require.NotEmpty(t, cert.Certificate)
		x509Cert, err := x509.ParseCertificate(cert.Certificate[0])
		require.NoError(t, err)
		require.NotNil(t, x509Cert)
		require.NotNil(t, x509Cert.SerialNumber)
		goodSerial = *x509Cert.SerialNumber

		// Logs for happy case are checked in another test, just dump them here.
		logs.TakeAll()
	}

	{
		// Create and load bad cert / key (empty files)
		tmpdir := t.TempDir()
		emptyFile, err := os.CreateTemp(tmpdir, "badcert-*.pem")
		require.NoError(t, err)
		emptyPath := emptyFile.Name()
		require.NoError(t, emptyFile.Close())

		loadErr := cl.Load(emptyPath, emptyPath)
		require.ErrorContains(t, loadErr, fmt.Sprintf("error loading x509 key pair (%q / %q): tls: failed to find any PEM data in certificate input", emptyPath, emptyPath))

		// Check that we are still using the previously loaded certificate
		cp, kp := cl.Paths()
		require.Equal(t, ss.CertPath, cp)
		require.Equal(t, ss.KeyPath, kp)

		cert, err := cl.GetCertificate(nil)
		require.NoError(t, err)
		require.NotNil(t, cert)
		require.NotEmpty(t, cert.Certificate)
		x509Cert, err := x509.ParseCertificate(cert.Certificate[0])
		require.NoError(t, err)
		require.NotNil(t, x509Cert)
		require.NotNil(t, x509Cert.SerialNumber)
		require.Equal(t, goodSerial, *x509Cert.SerialNumber)

		// The failed reload should log that it kept the previously loaded
		// certificate, naming both the certificate that failed and the one still
		// in use (with its serial), so operators can tell the reload was rejected
		// rather than silently applied.
		require.Equal(t, 3, logs.Len())
		logLines := logs.TakeAll()
		require.Equal(t, "Loading TLS certificate (start)", logLines[0].Message)
		require.Equal(t, "Error loading TLS certificate, continuing to use previously loaded certificate", logLines[1].Message)
		require.Equal(t, "Loading TLS certificate (end)", logLines[2].Message)

		// The start and end lines name the certificate whose load was attempted.
		for _, l := range []observer.LoggedEntry{logLines[0], logLines[2]} {
			ctx := l.ContextMap()
			require.Equal(t, emptyPath, ctx["cert"])
			require.Equal(t, emptyPath, ctx["key"])
		}

		// The error line is logged at error level and distinguishes the failed
		// certificate from the active one that remains in use.
		require.Equal(t, zapcore.ErrorLevel, logLines[1].Level)
		cm := logLines[1].ContextMap()
		require.Equal(t, emptyPath, cm["failedCert"])
		require.Equal(t, emptyPath, cm["failedKey"])
		require.Equal(t, ss.CertPath, cm["activeCert"])
		require.Equal(t, ss.KeyPath, cm["activeKey"])
		require.Equal(t, goodSerial.String(), cm["activeCertSerial"])
		require.Contains(t, cm["error"], "failed to find any PEM data")
	}

}

func TestTLSCertLoader_EmptyPaths(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t)

	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	cl, err := NewTLSCertLoader(
		ServerOnlyRole,
		monitor,
		WithCertLoaderCertificate("", ss.KeyPath))
	require.ErrorIs(t, err, ErrPathEmpty)
	require.Nil(t, cl)

	// This is no longer an error on instantiation for a server role,
	// only on a PrepareLoad.
	cl, err = NewTLSCertLoader(
		ServerOnlyRole,
		monitor,
		WithCertLoaderCertificate("", ""))
	require.NoError(t, err)
	require.NotNil(t, cl)

	// For this case, the loader will assume CertPath also contains, which
	// it does not, so this will fail.
	cl, err = NewTLSCertLoader(
		ServerOnlyRole,
		monitor,
		WithCertLoaderCertificate(ss.CertPath, ""))
	require.ErrorContains(t, err, "found a certificate rather than a key")
	require.Nil(t, cl)
}

func TestTLSCertLoader_FileNotFound(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t)

	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	// Non-existent certificate file
	cl, err := NewTLSCertLoader(
		ServerOnlyRole,
		monitor,
		WithCertLoaderCertificate("/nonexistent/path/to/cert.pem", ss.KeyPath))
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Nil(t, cl)

	// Non-existent key file
	cl, err = NewTLSCertLoader(
		ServerOnlyRole,
		monitor,
		WithCertLoaderCertificate(ss.CertPath, "/nonexistent/path/to/key.pem"))
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Nil(t, cl)

	// Both files non-existent
	cl, err = NewTLSCertLoader(
		ServerOnlyRole,
		monitor,
		WithCertLoaderCertificate("/nonexistent/cert.pem", "/nonexistent/key.pem"))
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Nil(t, cl)
}

func TestTLSCertLoader_MismatchedCertAndKey(t *testing.T) {
	// Create two different certificate/key pairs
	ss1 := selfsigned.NewSelfSignedCert(t, selfsigned.WithDNSName("cert1.influxdata.edge"))
	ss2 := selfsigned.NewSelfSignedCert(t, selfsigned.WithDNSName("cert2.influxdata.edge"))

	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	// Try to load cert from first pair with key from second pair
	cl, err := NewTLSCertLoader(
		ServerOnlyRole,
		monitor,
		WithCertLoaderCertificate(ss1.CertPath, ss2.KeyPath))
	require.ErrorContains(t, err, "error loading x509 key pair")
	require.ErrorContains(t, err, "tls: private key does not match public key")
	require.Nil(t, cl)

	// Try to load cert from second pair with key from first pair
	cl, err = NewTLSCertLoader(
		ServerOnlyRole,
		monitor,
		WithCertLoaderCertificate(ss2.CertPath, ss1.KeyPath))
	require.ErrorContains(t, err, "error loading x509 key pair")
	require.ErrorContains(t, err, "tls: private key does not match public key")
	require.Nil(t, cl)
}

func TestTLSCertLoader_CombinedFile(t *testing.T) {
	const DNSName = "combined.influxdata.edge"
	ss := selfsigned.NewSelfSignedCert(t, selfsigned.WithDNSName(DNSName), selfsigned.WithCombinedFile())

	// Verify that CertPath and KeyPath point to the same file
	require.Equal(t, ss.CertPath, ss.KeyPath, "expected CertPath and KeyPath to be the same for combined file")

	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	// Start cert loader with the combined file. Let the cert loader infer that the key is combined with the cert.
	cl, err := NewTLSCertLoader(
		ServerOnlyRole,
		monitor,
		WithCertLoaderCertificate(ss.CertPath, ""))
	require.NoError(t, err)
	require.NotNil(t, cl)
	defer th.CheckedClose(t, cl)()

	// Get certificate and verify it
	cert, err := cl.GetCertificate(nil)
	require.NoError(t, err)
	require.NotNil(t, cert)
	require.NotEmpty(t, cert.Certificate, "expected at least 1 certificate")
	x509Cert, err := x509.ParseCertificate(cert.Certificate[0])
	require.NoError(t, err)
	require.NotNil(t, x509Cert)
	require.Equal(t, []string{DNSName}, x509Cert.DNSNames)
}

func TestTLSLoader_CertPermissionsTooOpen(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t)

	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	require.NoError(t, os.Chmod(ss.CertPath, 0660))
	cl, err := NewTLSCertLoader(
		ServerOnlyRole,
		monitor,
		WithCertLoaderCertificate(ss.CertPath, ss.KeyPath))
	require.ErrorContains(t, err, fmt.Sprintf("LoadCertificate: file permissions are too open: for %q, maximum is 0644 (-rw-r--r--) but found 0660 (-rw-rw----); extra permissions: 0020 (-----w----)", ss.CertPath))
	require.Nil(t, cl)
}

func TestTLSLoader_KeyPermissionsTooOpen(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t)

	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	require.NoError(t, os.Chmod(ss.KeyPath, 0644))
	cl, err := NewTLSCertLoader(
		ServerOnlyRole,
		monitor,
		WithCertLoaderCertificate(ss.CertPath, ss.KeyPath))
	require.ErrorContains(t, err, fmt.Sprintf("LoadCertificate: file permissions are too open: for %q, maximum is 0600 (-rw-------) but found 0644 (-rw-r--r--); extra permissions: 0044 (----r--r--)", ss.KeyPath))
	require.Nil(t, cl)
}

const (
	// testCheckTme is the TLS certificate check time in logging tests.
	testCheckTime = 333 * time.Millisecond

	// testCheckCapture time is how long to capture logs during logging tests. To prevent flaky tests,
	// it should be more than testCheckTime, but less than 2 * testCheckTime. Furthermore, it should be at least
	// 100 ms more than testCheckCapture time and more than 100 ms less than 2 * testCheckTime.
	testCheckCapture = 500 * time.Millisecond

	// testWarnWaitTime is the time to wait for a warning to be logged for a triggered warning.
	testWarnWaitTime = 50 * time.Millisecond
)

func newTestCertMonitor(t *testing.T, opts ...TLSCertMonitorOpt) *TLSCertMonitor {
	// Put default test options first so opts can override them.
	combinedOpts := append([]TLSCertMonitorOpt{WithMonitorCheckInterval(testCheckTime)}, opts...)
	certMonitor := NewTLSCertMonitor(combinedOpts...)
	require.NotNil(t, certMonitor)

	require.NoError(t, certMonitor.Open())
	certMonitor.WaitForMonitorStart()
	return certMonitor
}

func TestTLSCertLoader_PrematureCertificateLogging(t *testing.T) {
	notBefore := time.Now().UTC().Truncate(time.Hour).Add(7 * 24 * time.Hour)
	notAfter := notBefore.Add(7 * 24 * time.Hour)
	ss := selfsigned.NewSelfSignedCert(t, selfsigned.WithNotBefore(notBefore), selfsigned.WithNotAfter(notAfter))

	core, logs := observer.New(zapcore.InfoLevel)
	logger := zap.New(core)

	monitor := newTestCertMonitor(t, WithMonitorLogger(logger), WithMonitorTriggerDelay(1))
	defer th.CheckedClose(t, monitor)()

	usage := "httpd.server"
	cl, err := NewTLSCertLoader(
		ServerOnlyRole,
		monitor,
		WithCertLoaderCertificate(ss.CertPath, ss.KeyPath),
		WithCertLoaderLogger(logger),
		WithCertLoaderUsage(usage))
	require.NoError(t, err)
	require.NotNil(t, cl)
	defer th.CheckedClose(t, cl)()

	checkWarning := func(t *testing.T) {
		warning := logs.FilterMessage("Certificate is not valid yet").TakeAll()
		require.Len(t, warning, 1)
		require.Equal(t, zap.WarnLevel, warning[0].Level)
		require.Equal(t, ss.CertPath, warning[0].ContextMap()["cert"])
		require.Equal(t, ss.KeyPath, warning[0].ContextMap()["key"])
		require.Equal(t, notBefore, warning[0].ContextMap()["NotBefore"])
		require.Equal(t, []any{usage}, warning[0].ContextMap()["usages"])
		logs.TakeAll() // dump all logs
	}
	time.Sleep(testWarnWaitTime)
	checkWarning(t)

	// Check for warning during monitor
	require.Zero(t, logs.Len(), "init logs not dumped properly")
	time.Sleep(testCheckCapture)
	checkWarning(t)
}

func TestTLSCertLoader_ExpiredCertificateLogging(t *testing.T) {
	notAfter := time.Now().UTC().Truncate(time.Hour).Add(-7 * 24 * time.Hour)
	notBefore := notAfter.Add(-7 * 24 * time.Hour)
	ss := selfsigned.NewSelfSignedCert(t, selfsigned.WithNotBefore(notBefore), selfsigned.WithNotAfter(notAfter))

	core, logs := observer.New(zapcore.InfoLevel)
	logger := zap.New(core)

	monitor := newTestCertMonitor(t, WithMonitorLogger(logger), WithMonitorTriggerDelay(0))
	defer th.CheckedClose(t, monitor)()

	cl, err := NewTLSCertLoader(
		ServerOnlyRole,
		monitor,
		WithCertLoaderCertificate(ss.CertPath, ss.KeyPath),
		WithCertLoaderLogger(logger))
	require.NoError(t, err)
	require.NotNil(t, cl)
	defer th.CheckedClose(t, cl)()

	checkWarning := func(t *testing.T) {
		warning := logs.FilterMessage("Certificate is expired").TakeAll()
		require.Len(t, warning, 1)
		require.Equal(t, zap.WarnLevel, warning[0].Level)
		require.Equal(t, ss.CertPath, warning[0].ContextMap()["cert"])
		require.Equal(t, ss.KeyPath, warning[0].ContextMap()["key"])
		require.Equal(t, notAfter, warning[0].ContextMap()["NotAfter"])
		logs.TakeAll() // dump all logs
	}
	time.Sleep(testWarnWaitTime)
	checkWarning(t)

	// Check for warning during monitor
	require.Zero(t, logs.Len(), "init logs not dumped properly")
	time.Sleep(testCheckCapture)
	checkWarning(t)
}

func TestTLSCertLoader_CertificateExpiresSoonLogging(t *testing.T) {
	notBefore := time.Now().UTC().Truncate(time.Minute).Add(-7 * 24 * time.Hour)
	notAfter := time.Now().UTC().Truncate(time.Hour).Add(24 * time.Hour)

	ss := selfsigned.NewSelfSignedCert(t, selfsigned.WithNotBefore(notBefore), selfsigned.WithNotAfter(notAfter))

	core, logs := observer.New(zapcore.InfoLevel)
	logger := zap.New(core)

	monitor := newTestCertMonitor(t, WithMonitorExpirationAdvanced(2*24*time.Hour), WithMonitorLogger(logger), WithMonitorTriggerDelay(0))
	defer th.CheckedClose(t, monitor)()

	cl, err := NewTLSCertLoader(
		ServerOnlyRole,
		monitor,
		WithCertLoaderCertificate(ss.CertPath, ss.KeyPath),
		WithCertLoaderLogger(logger))
	require.NoError(t, err)
	require.NotNil(t, cl)
	defer th.CheckedClose(t, cl)()

	checkWarning := func(t *testing.T) {
		warning := logs.FilterMessage("Certificate will expire soon").TakeAll()
		require.Len(t, warning, 1)
		require.Equal(t, zap.WarnLevel, warning[0].Level)
		require.Equal(t, ss.CertPath, warning[0].ContextMap()["cert"])
		require.Equal(t, ss.KeyPath, warning[0].ContextMap()["key"])
		require.Equal(t, notAfter, warning[0].ContextMap()["NotAfter"])
		untilExpires, ok := warning[0].ContextMap()["untilExpires"].(time.Duration)
		require.True(t, ok)
		timeExpires := time.Now().Add(untilExpires)
		require.WithinDuration(t, notAfter, timeExpires, 2*time.Minute, "untilExpires varies more than expected")
		logs.TakeAll() // dump all logs
	}
	time.Sleep(testWarnWaitTime)
	checkWarning(t)

	// Check for warning during monitor
	require.Zero(t, logs.Len(), "init logs not dumped properly")
	time.Sleep(testCheckCapture)
	checkWarning(t)
}

func TestTLSCertLoader_VerifyLoad(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t)

	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	cl, err := NewTLSCertLoader(
		ServerOnlyRole,
		monitor,
		WithCertLoaderCertificate(ss.CertPath, ss.KeyPath))
	require.NoError(t, err)
	require.NotNil(t, cl)
	defer th.CheckedClose(t, cl)()

	cert1, err := cl.GetCertificate(nil)
	require.NoError(t, err)
	require.NotNil(t, cert1)
	leaf1 := cl.Leaf()
	require.NotNil(t, leaf1)
	sn1 := leaf1.SerialNumber.String()
	require.NotEmpty(t, sn1)

	ss2 := selfsigned.NewSelfSignedCert(t)

	// Test VerifyLoad with a cert pair that will not load properly.
	{
		apply, err := cl.PrepareLoad(WithCertLoaderCertificate(ss2.CACertPath, ss2.KeyPath)) // mismatched cert and key
		require.ErrorContains(t, err, "private key does not match public key")
		require.Nil(t, apply)

		// Make sure nothing in cl changed.
		cp, kp := cl.Paths()
		require.Equal(t, ss.CertPath, cp)
		require.Equal(t, ss.KeyPath, kp)
		require.Equal(t, cert1, cl.Certificate())
		require.Equal(t, leaf1, cl.Leaf())
	}

	// Test VerifyLoad with a proper cert pair.
	{
		// Get the certificate data to compare against the actual loaded certificate.
		exp, err := LoadCertificate(ss2.CertPath, ss2.KeyPath)
		require.NoError(t, err)
		sn2 := exp.Leaf.SerialNumber.String()
		require.NotEmpty(t, sn2)
		require.NotEqual(t, sn1, sn2)

		apply, err := cl.PrepareLoad(WithCertLoaderCertificate(ss2.CertPath, ss2.KeyPath))
		require.NoError(t, err)
		require.NotNil(t, apply)

		// Make sure nothing in cl changed yet.
		cp, kp := cl.Paths()
		require.Equal(t, ss.CertPath, cp)
		require.Equal(t, ss.KeyPath, kp)
		require.Equal(t, cert1, cl.Certificate())
		require.Equal(t, leaf1, cl.Leaf())

		// Call apply function and check for appropriate changes.
		require.NoError(t, apply())
		cp, kp = cl.Paths()
		require.Equal(t, ss2.CertPath, cp)
		require.Equal(t, ss2.KeyPath, kp)

		// Verify cert and leaf are different now, then verify the serial on leaf.
		require.NotNil(t, cl.Certificate())
		require.NotNil(t, cl.Leaf())
		require.NotEqual(t, cert1, cl.Certificate())
		require.NotEqual(t, leaf1, cl.Leaf())
		require.Equal(t, sn2, cl.Leaf().SerialNumber.String())
	}
}

func TestTLSCertLoader_GetClientCertificate(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t, selfsigned.WithDNSName("client.influxdata.edge"))

	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	cl, err := NewTLSCertLoader(
		ServerOnlyRole,
		monitor,
		WithCertLoaderCertificate(ss.CertPath, ss.KeyPath))
	require.NoError(t, err)
	require.NotNil(t, cl)
	defer th.CheckedClose(t, cl)()

	// Test happy path: certificate supports the request.
	// The selfsigned package creates RSA certificates, so we use RSA signature schemes.
	t.Run("supported certificate", func(t *testing.T) {
		cri := &tls.CertificateRequestInfo{
			SignatureSchemes: []tls.SignatureScheme{
				tls.PKCS1WithSHA256,
				tls.PKCS1WithSHA384,
				tls.PKCS1WithSHA512,
			},
		}

		cert, err := cl.GetClientCertificate(cri)
		require.NoError(t, err)
		require.NotNil(t, cert)
		require.Equal(t, cl.Certificate(), cert)
	})

	t.Run("nil CertificateRequestInfo", func(t *testing.T) {
		cert, err := cl.GetClientCertificate(nil)
		require.ErrorIs(t, err, ErrCertificateRequestInfoNil)
		require.NotNil(t, cert)
		require.Empty(t, cert.Certificate)
	})

	// Test unsupported certificate: CertificateRequestInfo only accepts Ed25519,
	// but our certificate uses RSA.
	t.Run("unsupported certificate", func(t *testing.T) {
		cri := &tls.CertificateRequestInfo{
			SignatureSchemes: []tls.SignatureScheme{
				tls.Ed25519, // Our RSA cert doesn't support this
			},
		}

		// We should get an empty certificate with no error. This replicates Go's behavior when
		// tls.Config.Certificates is used and none of the certificates are accepted by the server.
		cert, err := cl.GetClientCertificate(cri)
		require.NoError(t, err)
		// GetClientCertificate must return a non-nil certificate even on error
		// (per the tls.Config.GetClientCertificate contract).
		require.NotNil(t, cert)
		// The returned certificate should be an empty certificate, not the loaded one.
		require.NotEqual(t, cl.Certificate(), cert)
		require.Empty(t, cert.Certificate)
	})

	// Test with AcceptableCAs that include our CA.
	t.Run("acceptable CA", func(t *testing.T) {
		// Verify that if we change cri to ss's CA subject then we do get cert.
		caCert, err := os.ReadFile(ss.CACertPath)
		require.NoError(t, err)

		// Parse the CA cert to get its RawSubject for AcceptableCAs.
		block, _ := pem.Decode(caCert)
		require.NotNil(t, block)
		parsedCA, err := x509.ParseCertificate(block.Bytes)
		require.NoError(t, err)

		cri := &tls.CertificateRequestInfo{
			SignatureSchemes: []tls.SignatureScheme{
				tls.PKCS1WithSHA256,
			},
			AcceptableCAs: [][]byte{parsedCA.RawSubject},
		}

		cert, err := cl.GetClientCertificate(cri)
		require.NoError(t, err)
		require.NotNil(t, cert)
		require.Equal(t, cl.Certificate(), cert)
	})

	// Test with AcceptableCAs that don't include our CA.
	t.Run("unacceptable CA", func(t *testing.T) {
		// Create a certificate with a different CA subject.
		ss2 := selfsigned.NewSelfSignedCert(t,
			selfsigned.WithCASubject("different_org", "Different CA"),
		)
		caCert2, err := os.ReadFile(ss2.CACertPath)
		require.NoError(t, err)

		// Parse the CA cert to get its RawSubject for AcceptableCAs.
		block2, _ := pem.Decode(caCert2)
		require.NotNil(t, block2)
		parsedCA2, err := x509.ParseCertificate(block2.Bytes)
		require.NoError(t, err)

		cri := &tls.CertificateRequestInfo{
			SignatureSchemes: []tls.SignatureScheme{
				tls.PKCS1WithSHA256,
			},
			AcceptableCAs: [][]byte{parsedCA2.RawSubject},
		}

		// This should return an empty certificate with no error to replicate
		// Go's behavior when tls.Config.Certificates is used.
		cert, err := cl.GetClientCertificate(cri)
		require.NoError(t, err)
		require.NotNil(t, cert)
		require.Empty(t, cert.Certificate)
	})
}

func TestTLSCertLoader_SetupTLSConfig(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t)

	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	cl, err := NewTLSCertLoader(
		ServerOnlyRole,
		monitor,
		WithCertLoaderCertificate(ss.CertPath, ss.KeyPath))
	require.NoError(t, err)
	require.NotNil(t, cl)
	defer th.CheckedClose(t, cl)()

	t.Run("nil config", func(t *testing.T) {
		require.NotPanics(t, func() {
			cl.SetupTLSConfig(nil)
		})
	})

	t.Run("sets callbacks", func(t *testing.T) {
		tlsConfig := &tls.Config{}

		require.Nil(t, tlsConfig.GetCertificate)
		require.Nil(t, tlsConfig.GetClientCertificate)

		cl.SetupTLSConfig(tlsConfig)

		require.NotNil(t, tlsConfig.GetCertificate)
		require.Nil(t, tlsConfig.GetClientCertificate)
	})
}

func TestTLSCertLoader_ConstructorValidation(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t)

	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	t.Run("combined role is rejected", func(t *testing.T) {
		// A cert loader holds one certificate, so it serves one role. A manager
		// that acts as both uses two loaders.
		cl, err := NewTLSCertLoader(
			ServerAndClientRole,
			monitor,
			WithCertLoaderCertificate(ss.CertPath, ss.KeyPath))
		require.ErrorIs(t, err, ErrSingleRoleRequired)
		require.Nil(t, cl)
	})

	t.Run("invalid role is rejected", func(t *testing.T) {
		cl, err := NewTLSCertLoader(
			InvalidRole,
			monitor,
			WithCertLoaderCertificate(ss.CertPath, ss.KeyPath))
		require.ErrorIs(t, err, ErrSingleRoleRequired)
		require.Nil(t, cl)
	})

	t.Run("missing monitor is rejected", func(t *testing.T) {
		cl, err := NewTLSCertLoader(
			ServerOnlyRole,
			nil,
			WithCertLoaderCertificate(ss.CertPath, ss.KeyPath))
		require.ErrorIs(t, err, ErrNoCertificateMonitor)
		require.Nil(t, cl)
	})

	t.Run("error names the usage", func(t *testing.T) {
		cl, err := NewTLSCertLoader(
			ServerAndClientRole,
			monitor,
			WithCertLoaderUsage("httpd.server"),
			WithCertLoaderCertificate(ss.CertPath, ss.KeyPath))
		require.ErrorContains(t, err, `usage="httpd.server"`)
		require.Nil(t, cl)
	})
}

func TestTLSCertLoader_GetCertificateWithoutCertificate(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	// Only a client loader can exist without a certificate; a server loader
	// with an empty certificate is refused at construction.
	cl, err := NewTLSCertLoader(ClientOnlyRole, monitor)
	require.NoError(t, err)
	require.NotNil(t, cl)
	defer th.CheckedClose(t, cl)()

	require.Nil(t, cl.Certificate())
	require.Nil(t, cl.Leaf())
	require.True(t, cl.LoadedCertificate().IsEmpty())

	cert, err := cl.GetCertificate(&tls.ClientHelloInfo{})
	require.ErrorIs(t, err, ErrCertificateNil)
	require.Nil(t, cert)

	// GetClientCertificate must honor its contract and return a non-nil,
	// empty certificate alongside the error.
	clientCert, err := cl.GetClientCertificate(&tls.CertificateRequestInfo{
		SignatureSchemes: []tls.SignatureScheme{tls.PKCS1WithSHA256},
	})
	require.ErrorIs(t, err, ErrCertificateNil)
	require.NotNil(t, clientCert)
	require.Empty(t, clientCert.Certificate)

	// A loader with no certificate leaves the tls.Config callbacks unset so the
	// config falls back to whatever the base config specified.
	tlsConfig := &tls.Config{}
	cl.SetupTLSConfig(tlsConfig)
	require.Nil(t, tlsConfig.GetClientCertificate)
	require.Nil(t, tlsConfig.GetCertificate)
}

func TestTLSCertLoader_EmptyCertificateRejectedForServer(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	ss := selfsigned.NewSelfSignedCert(t)

	cl, err := NewTLSCertLoader(
		ServerOnlyRole,
		monitor,
		WithCertLoaderCertificate(ss.CertPath, ss.KeyPath))
	require.NoError(t, err)
	defer th.CheckedClose(t, cl)()

	// Clearing the paths would leave the server with no certificate to present.
	apply, err := cl.PrepareLoad(WithCertLoaderCertificate("", ""))
	require.ErrorIs(t, err, ErrCertificateEmpty)
	require.Nil(t, apply)

	// The previously loaded certificate is still in place.
	certPath, _ := cl.Paths()
	require.Equal(t, ss.CertPath, certPath)
}

func TestTLSCertLoader_Clear(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	ss := selfsigned.NewSelfSignedCert(t)

	cl, err := NewTLSCertLoader(
		ClientOnlyRole,
		monitor,
		WithCertLoaderCertificate(ss.CertPath, ss.KeyPath))
	require.NoError(t, err)
	defer th.CheckedClose(t, cl)()
	require.False(t, cl.LoadedCertificate().IsEmpty())

	cl.Clear()

	require.True(t, cl.LoadedCertificate().IsEmpty())
	require.Nil(t, cl.Certificate())
	certPath, keyPath := cl.Paths()
	require.Empty(t, certPath)
	require.Empty(t, keyPath)

	// A cleared loader can be reloaded.
	require.NoError(t, cl.Load(ss.CertPath, ss.KeyPath))
	certPath, keyPath = cl.Paths()
	require.Equal(t, ss.CertPath, certPath)
	require.Equal(t, ss.KeyPath, keyPath)
}

func TestRole_Predicates(t *testing.T) {
	tests := []struct {
		name                                  string
		role                                  Role
		valid, single, serverRole, clientRole bool
	}{
		{name: "invalid", role: InvalidRole},
		{name: "server only", role: ServerOnlyRole, valid: true, single: true, serverRole: true},
		{name: "client only", role: ClientOnlyRole, valid: true, single: true, clientRole: true},
		{name: "server and client", role: ServerAndClientRole, valid: true, serverRole: true, clientRole: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.valid, tt.role.IsValid())
			require.Equal(t, tt.single, tt.role.IsSingleRole())
			require.Equal(t, tt.serverRole, tt.role.IsServerRole())
			require.Equal(t, tt.clientRole, tt.role.IsClientRole())
		})
	}
}

func TestTLSCertLoader_ServerCertificateMustSupportServerAuth(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	// A certificate issued only for client authentication. Every compliant TLS
	// client refuses it from a server, so a server must not load it.
	clientOnlySS := selfsigned.NewSelfSignedCert(t,
		selfsigned.WithExtKeyUsage(x509.ExtKeyUsageClientAuth))
	serverSS := selfsigned.NewSelfSignedCert(t)

	t.Run("server role rejects a client-only certificate", func(t *testing.T) {
		cl, err := NewTLSCertLoader(
			ServerOnlyRole,
			monitor,
			WithCertLoaderUsage("httpd.server"),
			WithCertLoaderCertificate(clientOnlySS.CertPath, clientOnlySS.KeyPath))
		require.ErrorIs(t, err, ErrCertificateNotServerAuth)
		require.ErrorContains(t, err, "httpd.server: ", "the error should name the usage that failed")
		require.Nil(t, cl)
	})

	t.Run("client role accepts a client-only certificate", func(t *testing.T) {
		// The check is specific to how a server uses the certificate; a client
		// presenting it is exactly what it was issued for.
		cl, err := NewTLSCertLoader(
			ClientOnlyRole,
			monitor,
			WithCertLoaderCertificate(clientOnlySS.CertPath, clientOnlySS.KeyPath))
		require.NoError(t, err)
		require.NotNil(t, cl)
		defer th.CheckedClose(t, cl)()
	})

	t.Run("a certificate with no extended key usage is unrestricted", func(t *testing.T) {
		unrestrictedSS := selfsigned.NewSelfSignedCert(t, selfsigned.WithExtKeyUsage())

		cl, err := NewTLSCertLoader(
			ServerOnlyRole,
			monitor,
			WithCertLoaderCertificate(unrestrictedSS.CertPath, unrestrictedSS.KeyPath))
		require.NoError(t, err)
		require.NotNil(t, cl)
		defer th.CheckedClose(t, cl)()
	})

	t.Run("reload to a client-only certificate is refused", func(t *testing.T) {
		cl, err := NewTLSCertLoader(
			ServerOnlyRole,
			monitor,
			WithCertLoaderCertificate(serverSS.CertPath, serverSS.KeyPath))
		require.NoError(t, err)
		defer th.CheckedClose(t, cl)()

		apply, err := cl.PrepareLoad(WithCertLoaderCertificate(clientOnlySS.CertPath, clientOnlySS.KeyPath))
		require.ErrorIs(t, err, ErrCertificateNotServerAuth)
		require.Nil(t, apply)

		// The working certificate stays in place.
		certPath, _ := cl.Paths()
		require.Equal(t, serverSS.CertPath, certPath)
	})
}

func TestTLSConfigManager_ServerCertificateMustSupportServerAuth(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	clientOnlySS := selfsigned.NewSelfSignedCert(t,
		selfsigned.WithExtKeyUsage(x509.ExtKeyUsageClientAuth))

	t.Run("server manager refuses it", func(t *testing.T) {
		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(clientOnlySS.CertPath, clientOnlySS.KeyPath))
		require.ErrorIs(t, err, ErrCertificateNotServerAuth)
		require.Nil(t, manager)
	})

	t.Run("client manager accepts it as a client certificate", func(t *testing.T) {
		manager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithClientCertificate(clientOnlySS.CertPath, clientOnlySS.KeyPath))
		require.NoError(t, err)
		require.NotNil(t, manager)
		defer th.CheckedClose(t, manager)()
	})
}

func TestTLSCertLoader_IgnoreSanityChecks(t *testing.T) {
	// A certificate issued only for client authentication: real and parseable,
	// but a server should not be presenting it.
	clientOnlySS := selfsigned.NewSelfSignedCert(t,
		selfsigned.WithExtKeyUsage(x509.ExtKeyUsageClientAuth))

	t.Run("loads the certificate and warns", func(t *testing.T) {
		core, logs := observer.New(zapcore.InfoLevel)

		monitor := newTestCertMonitor(t)
		defer th.CheckedClose(t, monitor)()

		cl, err := NewTLSCertLoader(
			ServerOnlyRole,
			monitor,
			WithCertLoaderUsage("httpd.server"),
			WithCertLoaderLogger(zap.New(core)),
			WithCertLoaderIgnoreSanityChecks(true),
			WithCertLoaderCertificate(clientOnlySS.CertPath, clientOnlySS.KeyPath))
		require.NoError(t, err, "an ignored sanity check must not fail the load")
		require.NotNil(t, cl)
		defer th.CheckedClose(t, cl)()

		// The certificate is genuinely in use, not merely accepted.
		certPath, _ := cl.Paths()
		require.Equal(t, clientOnlySS.CertPath, certPath)
		require.NotNil(t, cl.Certificate())

		warnings := logs.FilterMessage("TLS certificate failed sanity checks, loading it anyway because sanity checks are being ignored").TakeAll()
		require.Len(t, warnings, 1, "ignoring a sanity check must still say so")
		require.Equal(t, zap.WarnLevel, warnings[0].Level)
		require.Contains(t, warnings[0].ContextMap()["error"], ErrCertificateNotServerAuth.Error(),
			"the warning should say which check failed")
	})

	t.Run("still fails when not ignored", func(t *testing.T) {
		monitor := newTestCertMonitor(t)
		defer th.CheckedClose(t, monitor)()

		cl, err := NewTLSCertLoader(
			ServerOnlyRole,
			monitor,
			WithCertLoaderIgnoreSanityChecks(false),
			WithCertLoaderCertificate(clientOnlySS.CertPath, clientOnlySS.KeyPath))
		require.ErrorIs(t, err, ErrCertificateNotServerAuth)
		require.Nil(t, cl)
	})

	t.Run("an empty certificate still fails", func(t *testing.T) {
		monitor := newTestCertMonitor(t)
		defer th.CheckedClose(t, monitor)()

		ss := selfsigned.NewSelfSignedCert(t)
		cl, err := NewTLSCertLoader(
			ServerOnlyRole,
			monitor,
			WithCertLoaderIgnoreSanityChecks(true),
			WithCertLoaderCertificate(ss.CertPath, ss.KeyPath))
		require.NoError(t, err)
		defer th.CheckedClose(t, cl)()

		// A server always needs a certificate: that is a fault, not a sanity
		// check, so ignoring sanity checks does not excuse it.
		apply, err := cl.PrepareLoad(WithCertLoaderCertificate("", ""))
		require.ErrorIs(t, err, ErrCertificateEmpty)
		require.Nil(t, apply)
	})

	t.Run("reload can ignore sanity checks", func(t *testing.T) {
		monitor := newTestCertMonitor(t)
		defer th.CheckedClose(t, monitor)()

		ss := selfsigned.NewSelfSignedCert(t)
		cl, err := NewTLSCertLoader(
			ServerOnlyRole,
			monitor,
			WithCertLoaderCertificate(ss.CertPath, ss.KeyPath))
		require.NoError(t, err)
		defer th.CheckedClose(t, cl)()

		// The option carries through a reload like the rest of the config.
		apply, err := cl.PrepareLoad(
			WithCertLoaderIgnoreSanityChecks(true),
			WithCertLoaderCertificate(clientOnlySS.CertPath, clientOnlySS.KeyPath))
		require.NoError(t, err)
		require.NoError(t, apply())

		certPath, _ := cl.Paths()
		require.Equal(t, clientOnlySS.CertPath, certPath)
	})
}

func TestTLSConfigManager_WithIgnoreSanityChecks(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	clientOnlySS := selfsigned.NewSelfSignedCert(t,
		selfsigned.WithExtKeyUsage(x509.ExtKeyUsageClientAuth))

	t.Run("passes through to the cert loader", func(t *testing.T) {
		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithIgnoreSanityChecks(true),
			WithServerCertificate(clientOnlySS.CertPath, clientOnlySS.KeyPath))
		require.NoError(t, err, "the manager option must reach the cert loader")
		require.NotNil(t, manager)
		defer th.CheckedClose(t, manager)()

		certPath, _ := manager.serverCertLoader.Paths()
		require.Equal(t, clientOnlySS.CertPath, certPath)
		require.NotNil(t, manager.TLSConfig().GetCertificate)
	})

	t.Run("defaults to enforcing", func(t *testing.T) {
		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(clientOnlySS.CertPath, clientOnlySS.KeyPath))
		require.ErrorIs(t, err, ErrCertificateNotServerAuth)
		require.Nil(t, manager)
	})
}
