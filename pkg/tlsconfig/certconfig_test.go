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

	"github.com/influxdata/influxdb/pkg/testing/selfsigned"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestTLSCertLoader_HappyPath(t *testing.T) {
	const DNSName = "data1.influxdata.edge"
	ss := selfsigned.NewSelfSignedCert(t, selfsigned.WithDNSName(DNSName))

	core, logs := observer.New(zapcore.InfoLevel)
	logger := zap.New(core)

	// Start cert loader
	cl, err := NewTLSCertLoader(ss.CertPath, ss.KeyPath, WithLogger(logger))
	require.NoError(t, err)
	require.NotNil(t, cl)
	defer func() {
		require.NoError(t, cl.Close())
	}()

	{
		// Check for expected log output
		require.Equal(t, 4, logs.Len())
		logLines := logs.TakeAll()
		require.Equal(t, "Loading TLS certificate (start)", logLines[0].Message)
		require.Equal(t, "Successfully loaded TLS certificate", logLines[1].Message)
		require.Equal(t, "Loading TLS certificate (end)", logLines[2].Message)
		require.Equal(t, "Starting TLS certificate monitor", logLines[3].Message)
		for _, l := range logLines[:3] { // "Starting TLS certificate monitor" doesn't include the cert name and key
			cm := l.ContextMap()
			require.Equal(t, ss.CertPath, cm["cert"])
			require.Equal(t, ss.KeyPath, cm["key"])
		}

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
}

func TestTLSCertLoader_GoodCertPersists(t *testing.T) {
	const DNSName = "data1.influxdata.edge"
	ss := selfsigned.NewSelfSignedCert(t, selfsigned.WithDNSName(DNSName))

	core, logs := observer.New(zapcore.InfoLevel)
	logger := zap.New(core)

	// Start cert loader
	cl, err := NewTLSCertLoader(ss.CertPath, ss.KeyPath, WithLogger(logger))
	require.NoError(t, err)
	require.NotNil(t, cl)
	defer func() {
		require.NoError(t, cl.Close())
	}()

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
	}

}

func TestTLSCertLoader_EmptyPaths(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t)

	cl, err := NewTLSCertLoader("", ss.KeyPath)
	require.ErrorIs(t, err, ErrPathEmpty)
	require.Nil(t, cl)

	cl, err = NewTLSCertLoader("", "")
	require.ErrorIs(t, err, ErrPathEmpty)
	require.Nil(t, cl)

	// For this case, the loader will assume CertPath also contains, which
	// it does not, so this will fail.
	cl, err = NewTLSCertLoader(ss.CertPath, "")
	require.ErrorContains(t, err, "found a certificate rather than a key")
	require.Nil(t, cl)
}

func TestTLSCertLoader_FileNotFound(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t)

	// Non-existent certificate file
	cl, err := NewTLSCertLoader("/nonexistent/path/to/cert.pem", ss.KeyPath)
	require.ErrorContains(t, err, "no such file or directory")
	require.Nil(t, cl)

	// Non-existent key file
	cl, err = NewTLSCertLoader(ss.CertPath, "/nonexistent/path/to/key.pem")
	require.ErrorContains(t, err, "no such file or directory")
	require.Nil(t, cl)

	// Both files non-existent
	cl, err = NewTLSCertLoader("/nonexistent/cert.pem", "/nonexistent/key.pem")
	require.ErrorContains(t, err, "no such file or directory")
	require.Nil(t, cl)
}

func TestTLSCertLoader_MismatchedCertAndKey(t *testing.T) {
	// Create two different certificate/key pairs
	ss1 := selfsigned.NewSelfSignedCert(t, selfsigned.WithDNSName("cert1.influxdata.edge"))
	ss2 := selfsigned.NewSelfSignedCert(t, selfsigned.WithDNSName("cert2.influxdata.edge"))

	// Try to load cert from first pair with key from second pair
	cl, err := NewTLSCertLoader(ss1.CertPath, ss2.KeyPath)
	require.ErrorContains(t, err, "error loading x509 key pair")
	require.ErrorContains(t, err, "tls: private key does not match public key")
	require.Nil(t, cl)

	// Try to load cert from second pair with key from first pair
	cl, err = NewTLSCertLoader(ss2.CertPath, ss1.KeyPath)
	require.ErrorContains(t, err, "error loading x509 key pair")
	require.ErrorContains(t, err, "tls: private key does not match public key")
	require.Nil(t, cl)
}

func TestTLSCertLoader_CombinedFile(t *testing.T) {
	const DNSName = "combined.influxdata.edge"
	ss := selfsigned.NewSelfSignedCert(t, selfsigned.WithDNSName(DNSName), selfsigned.WithCombinedFile())

	// Verify that CertPath and KeyPath point to the same file
	require.Equal(t, ss.CertPath, ss.KeyPath, "expected CertPath and KeyPath to be the same for combined file")

	// Start cert loader with the combined file. Let the cert loader infer that the key is combined with the cert.
	cl, err := NewTLSCertLoader(ss.CertPath, "")
	require.NoError(t, err)
	require.NotNil(t, cl)
	defer func() {
		require.NoError(t, cl.Close())
	}()

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

	require.NoError(t, os.Chmod(ss.CertPath, 0660))
	cl, err := NewTLSCertLoader(ss.CertPath, ss.KeyPath)
	require.ErrorContains(t, err, fmt.Sprintf("LoadCertificate: file permissions are too open: for %q, maximum is 0644 (-rw-r--r--) but found 0660 (-rw-rw----); extra permissions: 0020 (-----w----)", ss.CertPath))
	require.Nil(t, cl)
}

func TestTLSLoader_KeyPermissionsTooOpen(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t)

	require.NoError(t, os.Chmod(ss.KeyPath, 0644))
	cl, err := NewTLSCertLoader(ss.CertPath, ss.KeyPath)
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
)

func TestTLSCertLoader_PrematureCertificateLogging(t *testing.T) {
	notBefore := time.Now().UTC().Truncate(time.Hour).Add(7 * 24 * time.Hour)
	notAfter := notBefore.Add(7 * 24 * time.Hour)
	ss := selfsigned.NewSelfSignedCert(t, selfsigned.WithNotBefore(notBefore), selfsigned.WithNotAfter(notAfter))

	core, logs := observer.New(zapcore.InfoLevel)
	logger := zap.New(core)

	cl, err := NewTLSCertLoader(ss.CertPath, ss.KeyPath, WithLogger(logger), WithCertificateCheckInterval(testCheckTime))
	require.NoError(t, err)
	require.NotNil(t, cl)
	defer func() {
		require.NoError(t, cl.Close())
	}()

	checkWarning := func(t *testing.T) {
		warning := logs.FilterMessage("Certificate is not valid yet").TakeAll()
		require.Len(t, warning, 1)
		require.Equal(t, zap.WarnLevel, warning[0].Level)
		require.Equal(t, ss.CertPath, warning[0].ContextMap()["cert"])
		require.Equal(t, ss.KeyPath, warning[0].ContextMap()["key"])
		require.Equal(t, notBefore, warning[0].ContextMap()["NotBefore"])
		logs.TakeAll() // dump all logs
	}
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

	cl, err := NewTLSCertLoader(ss.CertPath, ss.KeyPath, WithLogger(logger), WithCertificateCheckInterval(testCheckTime))
	require.NoError(t, err)
	require.NotNil(t, cl)
	defer func() {
		require.NoError(t, cl.Close())
	}()

	checkWarning := func(t *testing.T) {
		warning := logs.FilterMessage("Certificate is expired").TakeAll()
		require.Len(t, warning, 1)
		require.Equal(t, zap.WarnLevel, warning[0].Level)
		require.Equal(t, ss.CertPath, warning[0].ContextMap()["cert"])
		require.Equal(t, ss.KeyPath, warning[0].ContextMap()["key"])
		require.Equal(t, notAfter, warning[0].ContextMap()["NotAfter"])
		logs.TakeAll() // dump all logs
	}
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

	cl, err := NewTLSCertLoader(ss.CertPath, ss.KeyPath, WithLogger(logger), WithCertificateCheckInterval(testCheckTime), WithExpirationAdvanced(2*24*time.Hour))
	require.NoError(t, err)
	require.NotNil(t, cl)
	defer func() {
		require.NoError(t, cl.Close())
	}()

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
	checkWarning(t)

	// Check for warning during monitor
	require.Zero(t, logs.Len(), "init logs not dumped properly")
	time.Sleep(testCheckCapture)
	checkWarning(t)
}

func TestTLSCertLoader_SetCertificate(t *testing.T) {
	// Initial setup
	ss := selfsigned.NewSelfSignedCert(t)

	cl, err := NewTLSCertLoader(ss.CertPath, ss.KeyPath)
	require.NoError(t, err)
	require.NotNil(t, cl)
	defer func() {
		require.NoError(t, cl.Close())
	}()

	cert1, err := cl.GetCertificate(nil)
	require.NoError(t, err)
	require.NotNil(t, cert1)
	leaf := cl.Leaf()
	require.NotNil(t, leaf)

	// Make sure that setting an invalid LoadedCertificate returns an error and doesn't
	// change the currently loaded certificate.
	{
		require.ErrorIs(t, cl.SetCertificate(LoadedCertificate{}), ErrLoadedCertificateInvalid)
		cp, kp := cl.Paths()
		require.Equal(t, ss.CertPath, cp)
		require.Equal(t, ss.KeyPath, kp)
		actualCert, err := cl.GetCertificate(nil)
		require.NoError(t, err)
		require.Equal(t, cert1, actualCert)
		require.Equal(t, leaf, cl.Leaf())
	}

	// Check that a valid LoadedCertificate works properly.
	ss2 := selfsigned.NewSelfSignedCert(t)
	{
		lc2, err := LoadCertificate(ss2.CertPath, ss2.KeyPath)
		require.NoError(t, err)
		require.True(t, lc2.IsValid())
		require.NoError(t, cl.SetCertificate(lc2))

		cp, kp := cl.Paths()
		require.Equal(t, ss2.CertPath, cp)
		require.Equal(t, ss2.KeyPath, kp)
		actualCert, err := cl.GetCertificate(nil)
		require.NoError(t, err)
		require.Equal(t, lc2.Certificate, actualCert)
		require.Equal(t, lc2.Leaf, cl.Leaf())
	}
}

func TestTLSCertLoader_VerifyLoad(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t)

	cl, err := NewTLSCertLoader(ss.CertPath, ss.KeyPath)
	require.NoError(t, err)
	require.NotNil(t, cl)
	defer func() {
		require.NoError(t, cl.Close())
	}()

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
		apply, err := cl.PrepareLoad(ss2.CACertPath, ss2.KeyPath) // mismatched cert and key
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

		apply, err := cl.PrepareLoad(ss2.CertPath, ss2.KeyPath)
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

	cl, err := NewTLSCertLoader(ss.CertPath, ss.KeyPath)
	require.NoError(t, err)
	require.NotNil(t, cl)
	defer func() {
		require.NoError(t, cl.Close())
	}()

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

		cert, err := cl.GetClientCertificate(cri)
		require.ErrorContains(t, err, "doesn't support any of the certificate's signature algorithms")
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

		cert, err := cl.GetClientCertificate(cri)
		require.ErrorContains(t, err, "not signed by an acceptable CA")
		require.NotNil(t, cert)
		require.Empty(t, cert.Certificate)
	})
}

func TestTLSCertLoader_SetupTLSConfig(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t)

	cl, err := NewTLSCertLoader(ss.CertPath, ss.KeyPath)
	require.NoError(t, err)
	require.NotNil(t, cl)
	defer func() {
		require.NoError(t, cl.Close())
	}()

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
		require.NotNil(t, tlsConfig.GetClientCertificate)
	})
}
