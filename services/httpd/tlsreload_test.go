package httpd_test

// Tests that a config reload applies every TLS setting, not just the
// certificate. Open and PrepareReloadConfig both build the manager from
// Config.TLSManagerOpts, and the listener resolves its configuration per
// connection, so a reloaded setting takes effect on the next connection without
// the listener being rebound.

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"testing"

	th "github.com/influxdata/influxdb/pkg/testing/helper"
	"github.com/influxdata/influxdb/pkg/testing/selfsigned"
	"github.com/influxdata/influxdb/pkg/tlsconfig"
	"github.com/influxdata/influxdb/services/httpd"
	"github.com/influxdata/influxdb/toml"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// openHTTPS starts an httpd service with TLS on an ephemeral port.
func openHTTPS(t *testing.T, c httpd.Config) *httpd.Service {
	t.Helper()

	certMonitor := tlsconfig.NewTLSCertMonitor()
	require.NoError(t, certMonitor.Open())
	t.Cleanup(th.CheckedClose(t, certMonitor))

	c.BindAddress = "127.0.0.1:0"
	c.HTTPSEnabled = true

	s := httpd.NewService(c, certMonitor)
	s.WithLogger(zap.NewNop())
	require.NoError(t, s.Open())
	t.Cleanup(th.CheckedClose(t, s))
	return s
}

// clientCertPool builds a pool trusting ss's CA.
func clientCertPool(t *testing.T, ss *selfsigned.Cert) *x509.CertPool {
	t.Helper()
	pool := x509.NewCertPool()
	pem, err := os.ReadFile(ss.CACertPath)
	require.NoError(t, err)
	require.True(t, pool.AppendCertsFromPEM(pem))
	return pool
}

// handshake dials the service and reports the server's view of the handshake.
// The client's own error is not enough: under TLS 1.3 a client can finish its
// handshake before the server's rejection arrives, so a read is needed to
// surface it.
func handshake(t *testing.T, s *httpd.Service, clientCfg *tls.Config) error {
	t.Helper()

	clientCfg.InsecureSkipVerify = true
	conn, err := tls.Dial("tcp", s.Addr().String(), clientCfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := conn.Handshake(); err != nil {
		return err
	}
	// Force the server's alert, if any, to surface.
	if _, err := conn.Write([]byte("GET /ping HTTP/1.0\r\n\r\n")); err != nil {
		return err
	}
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err != nil {
		return err
	}
	return nil
}

func TestService_ReloadTLSConfig_ClientAuth(t *testing.T) {
	serverSS := selfsigned.NewSelfSignedCert(t)
	clientSS := selfsigned.NewSelfSignedCert(t, selfsigned.WithCASubject("c", "Client CA"))

	// Start with client certificates not required.
	s := openHTTPS(t, httpd.Config{
		HTTPSCertificate: serverSS.CertPath,
		HTTPSPrivateKey:  serverSS.KeyPath,
	})

	require.NoError(t, handshake(t, s, &tls.Config{}),
		"a client without a certificate should be accepted before the reload")

	// Reload requiring and verifying client certificates.
	required := toml.TlsClientAuthType(tls.RequireAndVerifyClientCert)
	newConfig := httpd.Config{
		BindAddress:         "127.0.0.1:0",
		HTTPSEnabled:        true,
		HTTPSCertificate:    serverSS.CertPath,
		HTTPSPrivateKey:     serverSS.KeyPath,
		HTTPSClientAuthType: &required,
		HTTPSClientCA:       &tlsconfig.CAConfig{Paths: []string{clientSS.CACertPath}},
	}

	apply, err := s.PrepareReloadConfig(newConfig)
	require.NoError(t, err)
	require.NotNil(t, apply)

	// Preparing only validates.
	require.NoError(t, handshake(t, s, &tls.Config{}), "PrepareReloadConfig must not change the listener")

	require.NoError(t, apply())

	// The listener was never rebound, but the new client auth policy applies.
	require.Error(t, handshake(t, s, &tls.Config{}),
		"a client without a certificate should be rejected after the reload")

	clientCert, err := tls.LoadX509KeyPair(clientSS.CertPath, clientSS.KeyPath)
	require.NoError(t, err)
	require.NoError(t, handshake(t, s, &tls.Config{Certificates: []tls.Certificate{clientCert}}),
		"a client with a certificate signed by the reloaded CA should be accepted")
}

func TestService_ReloadTLSConfig_ClientCA(t *testing.T) {
	serverSS := selfsigned.NewSelfSignedCert(t)
	caA := selfsigned.NewSelfSignedCert(t, selfsigned.WithCASubject("a", "Client CA A"))
	caB := selfsigned.NewSelfSignedCert(t, selfsigned.WithCASubject("b", "Client CA B"))

	required := toml.TlsClientAuthType(tls.RequireAndVerifyClientCert)
	base := httpd.Config{
		BindAddress:         "127.0.0.1:0",
		HTTPSEnabled:        true,
		HTTPSCertificate:    serverSS.CertPath,
		HTTPSPrivateKey:     serverSS.KeyPath,
		HTTPSClientAuthType: &required,
		HTTPSClientCA:       &tlsconfig.CAConfig{Paths: []string{caA.CACertPath}},
	}
	s := openHTTPS(t, base)

	certA, err := tls.LoadX509KeyPair(caA.CertPath, caA.KeyPath)
	require.NoError(t, err)
	certB, err := tls.LoadX509KeyPair(caB.CertPath, caB.KeyPath)
	require.NoError(t, err)

	require.NoError(t, handshake(t, s, &tls.Config{Certificates: []tls.Certificate{certA}}))
	require.Error(t, handshake(t, s, &tls.Config{Certificates: []tls.Certificate{certB}}),
		"CA B is not trusted before the reload")

	// Swap the trusted client CA from A to B.
	newConfig := base
	newConfig.HTTPSClientCA = &tlsconfig.CAConfig{Paths: []string{caB.CACertPath}}

	apply, err := s.PrepareReloadConfig(newConfig)
	require.NoError(t, err)
	require.NoError(t, apply())

	require.NoError(t, handshake(t, s, &tls.Config{Certificates: []tls.Certificate{certB}}),
		"CA B should be trusted after the reload")
	require.Error(t, handshake(t, s, &tls.Config{Certificates: []tls.Certificate{certA}}),
		"CA A should no longer be trusted after the reload")
}

func TestService_ReloadTLSConfig_MinVersion(t *testing.T) {
	serverSS := selfsigned.NewSelfSignedCert(t)

	s := openHTTPS(t, httpd.Config{
		HTTPSCertificate: serverSS.CertPath,
		HTTPSPrivateKey:  serverSS.KeyPath,
	})

	require.NoError(t, handshake(t, s, &tls.Config{MaxVersion: tls.VersionTLS12}),
		"TLS 1.2 should be accepted before the reload")

	// Raise the minimum version through the base TLS config.
	newConfig := httpd.Config{
		BindAddress:      "127.0.0.1:0",
		HTTPSEnabled:     true,
		HTTPSCertificate: serverSS.CertPath,
		HTTPSPrivateKey:  serverSS.KeyPath,
		TLS:              &tls.Config{MinVersion: tls.VersionTLS13},
	}

	apply, err := s.PrepareReloadConfig(newConfig)
	require.NoError(t, err)
	require.NoError(t, apply())

	require.Error(t, handshake(t, s, &tls.Config{MaxVersion: tls.VersionTLS12}),
		"TLS 1.2 should be rejected after raising min-version")
	require.NoError(t, handshake(t, s, &tls.Config{MinVersion: tls.VersionTLS13}),
		"TLS 1.3 should still be accepted")
}

func TestService_ReloadTLSConfig_FailureKeepsPreviousConfig(t *testing.T) {
	serverSS := selfsigned.NewSelfSignedCert(t)
	clientSS := selfsigned.NewSelfSignedCert(t, selfsigned.WithCASubject("c", "Client CA"))

	s := openHTTPS(t, httpd.Config{
		HTTPSCertificate: serverSS.CertPath,
		HTTPSPrivateKey:  serverSS.KeyPath,
	})

	// A client CA that cannot be read must not disturb the running listener.
	newConfig := httpd.Config{
		BindAddress:      "127.0.0.1:0",
		HTTPSEnabled:     true,
		HTTPSCertificate: serverSS.CertPath,
		HTTPSPrivateKey:  serverSS.KeyPath,
		HTTPSClientCA:    &tlsconfig.CAConfig{Paths: []string{"/nonexistent/ca.pem"}},
	}

	apply, err := s.PrepareReloadConfig(newConfig)
	require.Error(t, err)
	require.Nil(t, apply)

	require.NoError(t, handshake(t, s, &tls.Config{}),
		"a failed reload must leave the listener serving the previous configuration")

	// Sanity check that the pool the test relies on is otherwise usable.
	require.NotNil(t, clientCertPool(t, clientSS))
}

// TestService_TLSUsage covers the service naming itself to the certificate
// monitor. The monitor groups its warnings by usage, so a service that does not
// name itself is reported as an anonymous ".server".
func TestService_TLSUsage(t *testing.T) {
	serverSS := selfsigned.NewSelfSignedCert(t)

	core, logs := observer.New(zapcore.InfoLevel)
	certMonitor := tlsconfig.NewTLSCertMonitor(tlsconfig.WithMonitorLogger(zap.New(core)))
	require.NoError(t, certMonitor.Open())
	t.Cleanup(th.CheckedClose(t, certMonitor))

	s := httpd.NewService(httpd.Config{
		BindAddress:      "127.0.0.1:0",
		HTTPSEnabled:     true,
		HTTPSCertificate: serverSS.CertPath,
		HTTPSPrivateKey:  serverSS.KeyPath,
	}, certMonitor)
	s.WithLogger(zap.NewNop())
	require.NoError(t, s.Open())
	t.Cleanup(th.CheckedClose(t, s))

	entries := logs.FilterMessage("Registered certificate loader").TakeAll()
	require.Len(t, entries, 1)
	require.Equal(t, "httpd.server", entries[0].ContextMap()["usage"])
}
