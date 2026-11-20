package tlsconfig

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
	cl := newTestCertLoader(t, certMonitor, logger, usage, ss.CertPath, ss.KeyPath)
	defer th.CheckedClose(t, cl)()

	{
		// Check for expected log output. The loader registers with the monitor
		// when it is created, before it loads a certificate.
		require.Equal(t, 5, logs.Len())
		logLines := logs.TakeAll()
		require.Equal(t, "Starting TLS certificate monitor", logLines[0].Message)
		require.Equal(t, "Registered certificate loader", logLines[1].Message)
		require.Equal(t, usage, logLines[1].ContextMap()["usage"])
		require.Equal(t, "Loading TLS certificate (start)", logLines[2].Message)
		require.Equal(t, "Successfully loaded TLS certificate", logLines[3].Message)
		require.Equal(t, "Loading TLS certificate (end)", logLines[4].Message)
		for _, l := range logLines[2:4] { // only the load logs include the cert name and key
			cm := l.ContextMap()
			require.Equal(t, usage, cm["usage"])
			require.Equal(t, ss.CertPath, cm["cert"])
			require.Equal(t, ss.KeyPath, cm["key"])
		}

		// Get certificate and do some checks on it.
		cp, kp := loadedPaths(cl)
		require.Equal(t, ss.CertPath, cp)
		require.Equal(t, ss.KeyPath, kp)
		cert := cl.LoadedCertificate().Certificate
		require.NotNil(t, cert)
		require.NotEmpty(t, cert.Certificate, "expected at least 1 certificate")
		require.NotNil(t, cl.LoadedCertificate().Leaf)
		x509Cert, err := x509.ParseCertificate(cert.Certificate[0])
		require.NoError(t, err)
		require.NotNil(t, x509Cert)
		require.Equal(t, []string{DNSName}, x509Cert.DNSNames)
		require.Equal(t, x509Cert, cl.LoadedCertificate().Leaf)
	}

	{
		// Create new certificate and reload
		const DNSName2 = "data1-ultimate-form.influxdata.edge"
		logs.TakeAll()
		ss := selfsigned.NewSelfSignedCert(t, selfsigned.WithDNSName(DNSName2))
		require.NoError(t, loadCert(t, cl, ss.CertPath, ss.KeyPath))

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

		cp, kp := loadedPaths(cl)
		require.Equal(t, ss.CertPath, cp)
		require.Equal(t, ss.KeyPath, kp)
		cert := cl.LoadedCertificate().Certificate
		require.NotNil(t, cert)
		require.NotEmpty(t, cert.Certificate, "expected at least 1 certificate")
		require.NotNil(t, cl.LoadedCertificate().Leaf)
		x509Cert, err := x509.ParseCertificate(cert.Certificate[0])
		require.NoError(t, err)
		require.NotNil(t, x509Cert)
		require.Equal(t, []string{DNSName2}, x509Cert.DNSNames)
		require.Equal(t, x509Cert, cl.LoadedCertificate().Leaf)
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
	cl := newTestCertLoader(t, certMonitor, logger, "", ss.CertPath, ss.KeyPath)
	defer th.CheckedClose(t, cl)()

	var goodSerial big.Int
	{
		cp, kp := loadedPaths(cl)
		require.Equal(t, ss.CertPath, cp)
		require.Equal(t, ss.KeyPath, kp)

		cert := cl.LoadedCertificate().Certificate
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

		loadErr := loadCert(t, cl, emptyPath, emptyPath)
		require.ErrorContains(t, loadErr, fmt.Sprintf("error loading x509 key pair (%q / %q): tls: failed to find any PEM data in certificate input", emptyPath, emptyPath))

		// Check that we are still using the previously loaded certificate
		cp, kp := loadedPaths(cl)
		require.Equal(t, ss.CertPath, cp)
		require.Equal(t, ss.KeyPath, kp)

		cert := cl.LoadedCertificate().Certificate
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

	cl, err := newTLSCertLoader(monitor, nil, "")
	require.NoError(t, err)
	defer th.CheckedClose(t, cl)()

	require.ErrorIs(t, loadCert(t, cl, "", ss.KeyPath), ErrPathEmpty)
	require.True(t, cl.LoadedCertificate().IsEmpty())

	// Empty paths load an empty certificate. Whether that is acceptable is up
	// to the manager.
	require.NoError(t, loadCert(t, cl, "", ""))
	require.True(t, cl.LoadedCertificate().IsEmpty())

	// For this case, the loader will assume CertPath also contains the key,
	// which it does not, so this will fail.
	require.ErrorContains(t, loadCert(t, cl, ss.CertPath, ""), "found a certificate rather than a key")
	require.True(t, cl.LoadedCertificate().IsEmpty())
}

func TestTLSCertLoader_FileNotFound(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t)

	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	cl, err := newTLSCertLoader(monitor, nil, "")
	require.NoError(t, err)
	defer th.CheckedClose(t, cl)()

	// Non-existent certificate file
	require.ErrorIs(t, loadCert(t, cl, "/nonexistent/path/to/cert.pem", ss.KeyPath), os.ErrNotExist)

	// Non-existent key file
	require.ErrorIs(t, loadCert(t, cl, ss.CertPath, "/nonexistent/path/to/key.pem"), os.ErrNotExist)

	// Both files non-existent
	require.ErrorIs(t, loadCert(t, cl, "/nonexistent/cert.pem", "/nonexistent/key.pem"), os.ErrNotExist)

	require.True(t, cl.LoadedCertificate().IsEmpty())
}

func TestTLSCertLoader_MismatchedCertAndKey(t *testing.T) {
	// Create two different certificate/key pairs
	ss1 := selfsigned.NewSelfSignedCert(t, selfsigned.WithDNSName("cert1.influxdata.edge"))
	ss2 := selfsigned.NewSelfSignedCert(t, selfsigned.WithDNSName("cert2.influxdata.edge"))

	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	cl, err := newTLSCertLoader(monitor, nil, "")
	require.NoError(t, err)
	defer th.CheckedClose(t, cl)()

	// Try to load cert from first pair with key from second pair
	err = loadCert(t, cl, ss1.CertPath, ss2.KeyPath)
	require.ErrorContains(t, err, "error loading x509 key pair")
	require.ErrorContains(t, err, "tls: private key does not match public key")

	// Try to load cert from second pair with key from first pair
	err = loadCert(t, cl, ss2.CertPath, ss1.KeyPath)
	require.ErrorContains(t, err, "error loading x509 key pair")
	require.ErrorContains(t, err, "tls: private key does not match public key")

	require.True(t, cl.LoadedCertificate().IsEmpty())
}

func TestTLSCertLoader_CombinedFile(t *testing.T) {
	const DNSName = "combined.influxdata.edge"
	ss := selfsigned.NewSelfSignedCert(t, selfsigned.WithDNSName(DNSName), selfsigned.WithCombinedFile())

	// Verify that CertPath and KeyPath point to the same file
	require.Equal(t, ss.CertPath, ss.KeyPath, "expected CertPath and KeyPath to be the same for combined file")

	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	// Start cert loader with the combined file. Let the cert loader infer that the key is combined with the cert.
	cl := newTestCertLoader(t, monitor, nil, "", ss.CertPath, "")
	defer th.CheckedClose(t, cl)()

	// Get certificate and verify it
	cert := cl.LoadedCertificate().Certificate
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

	cl, err := newTLSCertLoader(monitor, nil, "")
	require.NoError(t, err)
	defer th.CheckedClose(t, cl)()

	require.NoError(t, os.Chmod(ss.CertPath, 0660))
	err = loadCert(t, cl, ss.CertPath, ss.KeyPath)
	require.ErrorContains(t, err, fmt.Sprintf("LoadCertificate: file permissions are too open: for %q, maximum is 0644 (-rw-r--r--) but found 0660 (-rw-rw----); extra permissions: 0020 (-----w----)", ss.CertPath))
	require.True(t, cl.LoadedCertificate().IsEmpty())

	// ignoreFilePermissions loads it anyway.
	apply, prepared, err := cl.prepareLoad(ss.CertPath, ss.KeyPath, true)
	require.NoError(t, err)
	require.NotNil(t, apply)
	require.Equal(t, ss.CertPath, prepared.CertificatePath)
	require.Equal(t, ss.KeyPath, prepared.KeyPath)
	require.NotNil(t, prepared.Certificate)
	require.NoError(t, apply())
	require.False(t, cl.LoadedCertificate().IsEmpty())
	require.Same(t, prepared.Certificate, cl.LoadedCertificate().Certificate)
}

func TestTLSLoader_KeyPermissionsTooOpen(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t)

	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	cl, err := newTLSCertLoader(monitor, nil, "")
	require.NoError(t, err)
	defer th.CheckedClose(t, cl)()

	require.NoError(t, os.Chmod(ss.KeyPath, 0644))
	err = loadCert(t, cl, ss.CertPath, ss.KeyPath)
	require.ErrorContains(t, err, fmt.Sprintf("LoadCertificate: file permissions are too open: for %q, maximum is 0600 (-rw-------) but found 0644 (-rw-r--r--); extra permissions: 0044 (----r--r--)", ss.KeyPath))
	require.True(t, cl.LoadedCertificate().IsEmpty())
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

// newTestCertLoader returns a cert loader registered with monitor that has
// loaded the certificate at certPath and keyPath.
func newTestCertLoader(t *testing.T, monitor *TLSCertMonitor, logger *zap.Logger, usage, certPath, keyPath string) *tlsCertLoader {
	t.Helper()
	cl, err := newTLSCertLoader(monitor, logger, usage)
	require.NoError(t, err)
	require.NotNil(t, cl)
	require.NoError(t, loadCert(t, cl, certPath, keyPath))
	return cl
}

// loadCert loads the certificate at certPath and keyPath into cl and makes it
// active, the way TLSConfigManager does, and returns prepareLoad's error. It
// also checks that a failed prepareLoad returns no certificate, and that apply
// makes active exactly the certificate prepareLoad returned. The checks use
// assert so loadCert is safe off the test goroutine; a failed check is also
// returned as an error.
func loadCert(t *testing.T, cl *tlsCertLoader, certPath, keyPath string) error {
	t.Helper()
	apply, prepared, err := cl.prepareLoad(certPath, keyPath, false)
	if err != nil {
		assert.Nil(t, apply)
		assert.Zero(t, prepared, "a failed prepareLoad should return no certificate")
		return err
	}
	if !assert.NotNil(t, apply) {
		return errors.New("loadCert: prepareLoad returned no apply function")
	}
	if err := apply(); err != nil {
		return err
	}

	active := cl.LoadedCertificate()
	if !assert.Equal(t, prepared, active) || !assert.Same(t, prepared.Certificate, active.Certificate) {
		return errors.New("loadCert: the active certificate is not the one prepareLoad returned")
	}
	return nil
}

// loadedPaths returns the paths of cl's active certificate.
func loadedPaths(cl *tlsCertLoader) (certPath, keyPath string) {
	lc := cl.LoadedCertificate()
	return lc.CertificatePath, lc.KeyPath
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
	cl := newTestCertLoader(t, monitor, logger, usage, ss.CertPath, ss.KeyPath)
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

	cl := newTestCertLoader(t, monitor, logger, "", ss.CertPath, ss.KeyPath)
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

	cl := newTestCertLoader(t, monitor, logger, "", ss.CertPath, ss.KeyPath)
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

	cl := newTestCertLoader(t, monitor, nil, "", ss.CertPath, ss.KeyPath)
	defer th.CheckedClose(t, cl)()

	cert1 := cl.LoadedCertificate().Certificate
	require.NotNil(t, cert1)
	leaf1 := cl.LoadedCertificate().Leaf
	require.NotNil(t, leaf1)
	sn1 := leaf1.SerialNumber.String()
	require.NotEmpty(t, sn1)

	ss2 := selfsigned.NewSelfSignedCert(t)

	// Test VerifyLoad with a cert pair that will not load properly.
	{
		apply, prepared, err := cl.prepareLoad(ss2.CACertPath, ss2.KeyPath, false) // mismatched cert and key
		require.ErrorContains(t, err, "private key does not match public key")
		require.Nil(t, apply)
		require.Zero(t, prepared, "a failed prepareLoad should return no certificate")

		// Make sure nothing in cl changed.
		cp, kp := loadedPaths(cl)
		require.Equal(t, ss.CertPath, cp)
		require.Equal(t, ss.KeyPath, kp)
		require.Equal(t, cert1, cl.LoadedCertificate().Certificate)
		require.Equal(t, leaf1, cl.LoadedCertificate().Leaf)
	}

	// Test VerifyLoad with a proper cert pair.
	{
		// Get the certificate data to compare against the actual loaded certificate.
		exp, err := LoadCertificate(ss2.CertPath, ss2.KeyPath)
		require.NoError(t, err)
		sn2 := exp.Leaf.SerialNumber.String()
		require.NotEmpty(t, sn2)
		require.NotEqual(t, sn1, sn2)

		apply, prepared, err := cl.prepareLoad(ss2.CertPath, ss2.KeyPath, false)
		require.NoError(t, err)
		require.NotNil(t, apply)
		require.Equal(t, sn2, prepared.Serial(), "prepareLoad should return the certificate apply will load")

		// Make sure nothing in cl changed yet.
		cp, kp := loadedPaths(cl)
		require.Equal(t, ss.CertPath, cp)
		require.Equal(t, ss.KeyPath, kp)
		require.Equal(t, cert1, cl.LoadedCertificate().Certificate)
		require.Equal(t, leaf1, cl.LoadedCertificate().Leaf)

		// Call apply function and check for appropriate changes.
		require.NoError(t, apply())
		cp, kp = loadedPaths(cl)
		require.Equal(t, ss2.CertPath, cp)
		require.Equal(t, ss2.KeyPath, kp)

		// Verify cert and leaf are different now, then verify the serial on leaf.
		require.NotNil(t, cl.LoadedCertificate().Certificate)
		require.NotNil(t, cl.LoadedCertificate().Leaf)
		require.NotEqual(t, cert1, cl.LoadedCertificate().Certificate)
		require.NotEqual(t, leaf1, cl.LoadedCertificate().Leaf)
		require.Equal(t, sn2, cl.LoadedCertificate().Leaf.SerialNumber.String())
	}
}

func TestTLSCertLoader_ConstructorValidation(t *testing.T) {
	t.Run("missing monitor is rejected", func(t *testing.T) {
		cl, err := newTLSCertLoader(nil, nil, "httpd.server")
		require.ErrorIs(t, err, ErrNoCertificateMonitor)
		require.ErrorContains(t, err, `usage="httpd.server"`, "the error should name the usage")
		require.Nil(t, cl)
	})
}

func TestTLSCertLoader_WithoutCertificate(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	cl, err := newTLSCertLoader(monitor, nil, "")
	require.NoError(t, err)
	require.NotNil(t, cl)
	defer th.CheckedClose(t, cl)()

	require.Nil(t, cl.LoadedCertificate().Certificate)
	require.Nil(t, cl.LoadedCertificate().Leaf)
	require.True(t, cl.LoadedCertificate().IsEmpty())

	// Whether a certificate is required is up to the manager, so the loader
	// accepts a reload to no certificate.
	ss := selfsigned.NewSelfSignedCert(t)
	require.NoError(t, loadCert(t, cl, ss.CertPath, ss.KeyPath))
	require.NoError(t, loadCert(t, cl, "", ""))
	require.True(t, cl.LoadedCertificate().IsEmpty())
}

func TestTLSConfigManager_EmptyCertificateRejectedForServer(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	ss := selfsigned.NewSelfSignedCert(t)

	manager, err := NewServerTLSConfigManager(
		monitor,
		WithUseTLS(true),
		WithServerCertificate(ss.CertPath, ss.KeyPath))
	require.NoError(t, err)
	defer th.CheckedClose(t, manager)()

	// Clearing the paths would leave the server with no certificate to present.
	apply, err := manager.PrepareReconfigure(WithServerCertificate("", ""))
	require.ErrorIs(t, err, ErrCertificateEmpty)
	require.Nil(t, apply)

	// The previously loaded certificate is still in place.
	certPath, _ := loadedPaths(manager.serverCertLoader)
	require.Equal(t, ss.CertPath, certPath)
}

func TestTLSCertLoader_Clear(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	ss := selfsigned.NewSelfSignedCert(t)

	cl := newTestCertLoader(t, monitor, nil, "", ss.CertPath, ss.KeyPath)
	defer th.CheckedClose(t, cl)()
	require.False(t, cl.LoadedCertificate().IsEmpty())

	cl.Clear()

	require.True(t, cl.LoadedCertificate().IsEmpty())
	require.Nil(t, cl.LoadedCertificate().Certificate)
	certPath, keyPath := loadedPaths(cl)
	require.Empty(t, certPath)
	require.Empty(t, keyPath)

	// A cleared loader can be reloaded.
	require.NoError(t, loadCert(t, cl, ss.CertPath, ss.KeyPath))
	certPath, keyPath = loadedPaths(cl)
	require.Equal(t, ss.CertPath, certPath)
	require.Equal(t, ss.KeyPath, keyPath)
}

func TestRole_Predicates(t *testing.T) {
	tests := []struct {
		name                          string
		role                          Role
		valid, serverRole, clientRole bool
	}{
		{name: "invalid", role: InvalidRole},
		{name: "server only", role: ServerOnlyRole, valid: true, serverRole: true},
		{name: "client only", role: ClientOnlyRole, valid: true, clientRole: true},
		{name: "server and client", role: ServerAndClientRole, valid: true, serverRole: true, clientRole: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.valid, tt.role.IsValid())
			require.Equal(t, tt.serverRole, tt.role.IsServerRole())
			require.Equal(t, tt.clientRole, tt.role.IsClientRole())
		})
	}
}

func TestTLSConfigManager_ServerCertificateMustSupportServerAuth(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	// A certificate issued only for client authentication. Every compliant TLS
	// client refuses it from a server, so a server must not load it.
	clientOnlySS := selfsigned.NewSelfSignedCert(t,
		selfsigned.WithExtKeyUsage(x509.ExtKeyUsageClientAuth))
	serverSS := selfsigned.NewSelfSignedCert(t)

	t.Run("server manager refuses it", func(t *testing.T) {
		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithUsage("httpd"),
			WithServerCertificate(clientOnlySS.CertPath, clientOnlySS.KeyPath))
		require.ErrorIs(t, err, ErrCertificateNotServerAuth)
		require.ErrorContains(t, err, "httpd: ", "the error should name the usage that failed")
		require.Nil(t, manager)
	})

	t.Run("client manager accepts it as a client certificate", func(t *testing.T) {
		// The check is specific to how a server uses the certificate; a client
		// presenting it is exactly what it was issued for.
		manager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithClientCertificate(clientOnlySS.CertPath, clientOnlySS.KeyPath))
		require.NoError(t, err)
		require.NotNil(t, manager)
		defer th.CheckedClose(t, manager)()
	})

	t.Run("a certificate with no extended key usage is unrestricted", func(t *testing.T) {
		unrestrictedSS := selfsigned.NewSelfSignedCert(t, selfsigned.WithExtKeyUsage())

		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(unrestrictedSS.CertPath, unrestrictedSS.KeyPath))
		require.NoError(t, err)
		require.NotNil(t, manager)
		defer th.CheckedClose(t, manager)()
	})

	t.Run("reload to a client-only certificate is refused", func(t *testing.T) {
		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(serverSS.CertPath, serverSS.KeyPath))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		apply, err := manager.PrepareReconfigure(WithServerCertificate(clientOnlySS.CertPath, clientOnlySS.KeyPath))
		require.ErrorIs(t, err, ErrCertificateNotServerAuth)
		require.Nil(t, apply)

		// The working certificate stays in place.
		certPath, _ := loadedPaths(manager.serverCertLoader)
		require.Equal(t, serverSS.CertPath, certPath)
	})
}

func TestTLSConfigManager_WithIgnoreSanityChecks(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	// A certificate issued only for client authentication: real and parseable,
	// but a server should not be presenting it.
	clientOnlySS := selfsigned.NewSelfSignedCert(t,
		selfsigned.WithExtKeyUsage(x509.ExtKeyUsageClientAuth))

	t.Run("loads the certificate and warns", func(t *testing.T) {
		core, logs := observer.New(zapcore.InfoLevel)

		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithLogger(zap.New(core)),
			WithIgnoreSanityChecks(true),
			WithServerCertificate(clientOnlySS.CertPath, clientOnlySS.KeyPath))
		require.NoError(t, err, "an ignored sanity check must not fail the load")
		require.NotNil(t, manager)
		defer th.CheckedClose(t, manager)()

		// The certificate is genuinely in use, not merely accepted.
		certPath, _ := loadedPaths(manager.serverCertLoader)
		require.Equal(t, clientOnlySS.CertPath, certPath)
		cert, err := manager.TLSConfig().GetCertificate(&tls.ClientHelloInfo{})
		require.NoError(t, err)
		require.Same(t, manager.serverCertLoader.LoadedCertificate().Certificate, cert)

		warnings := logs.FilterMessage("TLS certificate failed sanity checks, loading it anyway because sanity checks are being ignored").TakeAll()
		require.Len(t, warnings, 1, "ignoring a sanity check must still say so")
		require.Equal(t, zap.WarnLevel, warnings[0].Level)
		require.Contains(t, warnings[0].ContextMap()["error"], ErrCertificateNotServerAuth.Error(),
			"the warning should say which check failed")
		require.Equal(t, clientOnlySS.CertPath, warnings[0].ContextMap()[logCertContext],
			"the warning should name the certificate")
	})

	t.Run("defaults to enforcing", func(t *testing.T) {
		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(clientOnlySS.CertPath, clientOnlySS.KeyPath))
		require.ErrorIs(t, err, ErrCertificateNotServerAuth)
		require.Nil(t, manager)
	})

	t.Run("an empty certificate still fails", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)
		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithIgnoreSanityChecks(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		// A server always needs a certificate: that is a fault, not a sanity
		// check, so ignoring sanity checks does not excuse it.
		apply, err := manager.PrepareReconfigure(WithServerCertificate("", ""))
		require.ErrorIs(t, err, ErrCertificateEmpty)
		require.Nil(t, apply)
	})

	t.Run("reload can ignore sanity checks", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)
		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		// The option carries through a reload like the rest of the config.
		apply, err := manager.PrepareReconfigure(
			WithIgnoreSanityChecks(true),
			WithServerCertificate(clientOnlySS.CertPath, clientOnlySS.KeyPath))
		require.NoError(t, err)
		require.NoError(t, apply())

		certPath, _ := loadedPaths(manager.serverCertLoader)
		require.Equal(t, clientOnlySS.CertPath, certPath)
	})
}
