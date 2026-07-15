package tlsconfig

// Tests for TLSConfigManager's support for a client certificate that is
// separate from the server certificate, including the fallback to the server
// certificate when no client certificate is configured.

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"

	th "github.com/influxdata/influxdb/pkg/testing/helper"
	"github.com/influxdata/influxdb/pkg/testing/selfsigned"
)

// acceptableCAs builds the AcceptableCAs list a server trusting ss's CA would
// send in a certificate request.
func acceptableCAs(t *testing.T, ss *selfsigned.Cert) [][]byte {
	t.Helper()
	caPEM, err := os.ReadFile(ss.CACertPath)
	require.NoError(t, err)

	block, _ := pem.Decode(caPEM)
	require.NotNil(t, block)
	parsed, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)

	return [][]byte{parsed.RawSubject}
}

// certRequestFrom builds a CertificateRequestInfo for a server that trusts ss's
// CA. The selfsigned package issues RSA certificates.
func certRequestFrom(t *testing.T, ss *selfsigned.Cert) *tls.CertificateRequestInfo {
	t.Helper()
	return &tls.CertificateRequestInfo{
		SignatureSchemes: []tls.SignatureScheme{tls.PKCS1WithSHA256},
		AcceptableCAs:    acceptableCAs(t, ss),
	}
}

// leafSerial returns the serial of the certificate a loader currently holds.
func leafSerial(t *testing.T, cl *TLSCertLoader) string {
	t.Helper()
	require.NotNil(t, cl)
	leaf := cl.Leaf()
	require.NotNil(t, leaf)
	return leaf.SerialNumber.String()
}

func TestTLSConfigManager_WithClientCertificate_Paths(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	t.Run("separate client and server certificates", func(t *testing.T) {
		serverSS := selfsigned.NewSelfSignedCert(t)
		clientSS := selfsigned.NewSelfSignedCert(t)

		manager, err := NewClientServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(serverSS.CertPath, serverSS.KeyPath),
			WithClientCertificate(clientSS.CertPath, clientSS.KeyPath))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		serverCert, serverKey := manager.serverCertLoader.Paths()
		require.Equal(t, serverSS.CertPath, serverCert)
		require.Equal(t, serverSS.KeyPath, serverKey)

		clientCert, clientKey := manager.clientCertLoader.Paths()
		require.Equal(t, clientSS.CertPath, clientCert)
		require.Equal(t, clientSS.KeyPath, clientKey)

		// The loaders must hold genuinely different certificates, not two
		// handles onto the same one.
		require.NotEqual(t, leafSerial(t, manager.serverCertLoader), leafSerial(t, manager.clientCertLoader))
	})

	t.Run("client falls back to server certificate", func(t *testing.T) {
		serverSS := selfsigned.NewSelfSignedCert(t)

		manager, err := NewClientServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(serverSS.CertPath, serverSS.KeyPath))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		clientCert, clientKey := manager.clientCertLoader.Paths()
		require.Equal(t, serverSS.CertPath, clientCert, "client should fall back to the server certificate")
		require.Equal(t, serverSS.KeyPath, clientKey)
		require.Equal(t, leafSerial(t, manager.serverCertLoader), leafSerial(t, manager.clientCertLoader))
	})

	t.Run("client-only manager uses client certificate", func(t *testing.T) {
		serverSS := selfsigned.NewSelfSignedCert(t)
		clientSS := selfsigned.NewSelfSignedCert(t)

		manager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(serverSS.CertPath, serverSS.KeyPath),
			WithClientCertificate(clientSS.CertPath, clientSS.KeyPath))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		require.Nil(t, manager.serverCertLoader, "a client-only manager has no server cert loader")

		clientCert, clientKey := manager.clientCertLoader.Paths()
		require.Equal(t, clientSS.CertPath, clientCert)
		require.Equal(t, clientSS.KeyPath, clientKey)
	})

	t.Run("server-only manager ignores client certificate", func(t *testing.T) {
		serverSS := selfsigned.NewSelfSignedCert(t)
		clientSS := selfsigned.NewSelfSignedCert(t)

		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(serverSS.CertPath, serverSS.KeyPath),
			WithClientCertificate(clientSS.CertPath, clientSS.KeyPath))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		require.Nil(t, manager.clientCertLoader, "a server-only manager has no client cert loader")

		serverCert, _ := manager.serverCertLoader.Paths()
		require.Equal(t, serverSS.CertPath, serverCert)
	})

	t.Run("client certificate without key uses a combined file", func(t *testing.T) {
		serverSS := selfsigned.NewSelfSignedCert(t)
		combinedSS := selfsigned.NewSelfSignedCert(t, selfsigned.WithCombinedFile())

		// Setting only the client certificate path suppresses the fallback
		// entirely: the key is read from the certificate file rather than
		// borrowed from the server configuration.
		manager, err := NewClientServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithIgnoreFilePermissions(true),
			WithServerCertificate(serverSS.CertPath, serverSS.KeyPath),
			WithClientCertificate(combinedSS.CertPath, ""))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		clientCert, clientKey := manager.clientCertLoader.Paths()
		require.Equal(t, combinedSS.CertPath, clientCert)
		require.Equal(t, combinedSS.CertPath, clientKey, "key should come from the combined certificate file")
	})

	t.Run("client key without certificate is an error", func(t *testing.T) {
		serverSS := selfsigned.NewSelfSignedCert(t)
		clientSS := selfsigned.NewSelfSignedCert(t)

		// A client key with no client certificate does not fall back to the
		// server pair; there is no certificate to pair the key with.
		manager, err := NewClientServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(serverSS.CertPath, serverSS.KeyPath),
			WithClientCertificate("", clientSS.KeyPath))
		require.ErrorIs(t, err, ErrPathEmpty)
		require.Nil(t, manager)
	})

	t.Run("unloadable client certificate is an error", func(t *testing.T) {
		serverSS := selfsigned.NewSelfSignedCert(t)
		missing := path.Join(t.TempDir(), "absent.pem")

		manager, err := NewClientServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(serverSS.CertPath, serverSS.KeyPath),
			WithClientCertificate(missing, missing))
		require.ErrorContains(t, err, "error configuring client cert loader")
		require.ErrorContains(t, err, "no such file or directory")
		require.Nil(t, manager)
	})
}

func TestTLSConfigManager_WithClientCertificate_TLSConfigCallbacks(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	serverSS := selfsigned.NewSelfSignedCert(t)
	clientSS := selfsigned.NewSelfSignedCert(t)

	manager, err := NewClientServerTLSConfigManager(
		monitor,
		WithUseTLS(true),
		WithServerCertificate(serverSS.CertPath, serverSS.KeyPath),
		WithClientCertificate(clientSS.CertPath, clientSS.KeyPath))
	require.NoError(t, err)
	defer th.CheckedClose(t, manager)()

	tlsConfig := manager.TLSConfig()
	require.NotNil(t, tlsConfig)
	require.NotNil(t, tlsConfig.GetCertificate)
	require.NotNil(t, tlsConfig.GetClientCertificate)

	t.Run("GetCertificate returns the server certificate", func(t *testing.T) {
		cert, err := tlsConfig.GetCertificate(&tls.ClientHelloInfo{})
		require.NoError(t, err)
		require.NotNil(t, cert)
		require.Equal(t, manager.serverCertLoader.Certificate(), cert)
	})

	t.Run("GetClientCertificate returns the client certificate", func(t *testing.T) {
		cert, err := tlsConfig.GetClientCertificate(certRequestFrom(t, clientSS))
		require.NoError(t, err)
		require.NotNil(t, cert)
		require.Equal(t, manager.clientCertLoader.Certificate(), cert,
			"the client certificate, not the server certificate, must be offered")
	})

	t.Run("GetClientCertificate withholds a certificate the server won't accept", func(t *testing.T) {
		otherSS := selfsigned.NewSelfSignedCert(t, selfsigned.WithCASubject("other", "Other CA"))

		// Go's behavior when no configured certificate matches the request is
		// to send an empty certificate rather than fail the handshake locally.
		cert, err := tlsConfig.GetClientCertificate(certRequestFrom(t, otherSS))
		require.NoError(t, err)
		require.NotNil(t, cert)
		require.Empty(t, cert.Certificate)
	})
}

// TestTLSConfigManager_ClientCertificateMTLS exercises a real handshake where
// the server trusts only the CA that issued the separate client certificate.
// The handshake can therefore only succeed if the client presents its client
// certificate rather than its server certificate.
func TestTLSConfigManager_ClientCertificateMTLS(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	newServer := func(t *testing.T, serverSS, clientCASS *selfsigned.Cert) *TLSConfigManager {
		t.Helper()
		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(serverSS.CertPath, serverSS.KeyPath),
			WithClientCA(&CAConfig{Paths: []string{clientCASS.CACertPath}}),
			WithClientAuth(tls.RequireAndVerifyClientCert))
		require.NoError(t, err)
		return manager
	}

	t.Run("separate client certificate is accepted", func(t *testing.T) {
		serverSS := selfsigned.NewSelfSignedCert(t)
		clientSS := selfsigned.NewSelfSignedCert(t, selfsigned.WithCASubject("client_org", "Client CA"))

		// The server trusts only the client CA, so the server certificate would
		// not be accepted if it were offered instead.
		serverManager := newServer(t, serverSS, clientSS)
		defer th.CheckedClose(t, serverManager)()

		listener, err := serverManager.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer th.CheckedClose(t, listener)()

		testData := []byte("hello")
		serverDone := make(chan error, 1)
		go simpleEchoServer(serverDone, listener, len(testData))

		clientManager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(serverSS.CertPath, serverSS.KeyPath),
			WithClientCertificate(clientSS.CertPath, clientSS.KeyPath),
			WithAllowInsecure(true))
		require.NoError(t, err)
		defer th.CheckedClose(t, clientManager)()

		conn, err := clientManager.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer th.CheckedClose(t, conn)()

		n, err := conn.Write(testData)
		require.NoError(t, err)
		require.Equal(t, len(testData), n)

		buf := make([]byte, len(testData))
		n, err = conn.Read(buf)
		require.NoError(t, err)
		require.Equal(t, len(testData), n)
		require.Equal(t, testData, buf)

		require.NoError(t, <-serverDone)
	})

	t.Run("fallback server certificate is rejected", func(t *testing.T) {
		serverSS := selfsigned.NewSelfSignedCert(t)
		clientSS := selfsigned.NewSelfSignedCert(t, selfsigned.WithCASubject("client_org", "Client CA"))

		serverManager := newServer(t, serverSS, clientSS)
		defer th.CheckedClose(t, serverManager)()

		listener, err := serverManager.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer th.CheckedClose(t, listener)()

		testData := []byte("hello")
		serverDone := make(chan error, 1)
		go simpleEchoServer(serverDone, listener, len(testData))

		// Without a client certificate the manager falls back to the server
		// certificate, which this server does not trust.
		clientManager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(serverSS.CertPath, serverSS.KeyPath),
			WithAllowInsecure(true))
		require.NoError(t, err)
		defer th.CheckedClose(t, clientManager)()

		conn, dialErr := clientManager.Dial("tcp", listener.Addr().String())
		if dialErr == nil {
			defer func() {
				require.NoError(t, conn.Close())
			}()
			buf := make([]byte, 1)
			_, dialErr = conn.Read(buf)
		}
		require.ErrorContains(t, dialErr, "remote error: tls: certificate required")

		serverErr := <-serverDone
		require.ErrorContains(t, serverErr, "tls: client didn't provide a certificate")
	})
}

// TestTLSConfigManager_ReconfigureClientCertificate covers reconfiguring the
// client certificate independently of the server certificate.
func TestTLSConfigManager_ReconfigureClientCertificate(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	serverSS := selfsigned.NewSelfSignedCert(t)
	clientSS := selfsigned.NewSelfSignedCert(t)
	newClientSS := selfsigned.NewSelfSignedCert(t)

	manager, err := NewClientServerTLSConfigManager(
		monitor,
		WithUseTLS(true),
		WithServerCertificate(serverSS.CertPath, serverSS.KeyPath),
		WithClientCertificate(clientSS.CertPath, clientSS.KeyPath))
	require.NoError(t, err)
	defer th.CheckedClose(t, manager)()

	serverSerial := leafSerial(t, manager.serverCertLoader)

	apply, err := manager.PrepareReconfigure(
		WithClientCertificate(newClientSS.CertPath, newClientSS.KeyPath))
	require.NoError(t, err)
	require.NotNil(t, apply)

	// PrepareReconfigure only validates; nothing changes until apply runs.
	clientCert, _ := manager.clientCertLoader.Paths()
	require.Equal(t, clientSS.CertPath, clientCert, "PrepareReconfigure must not swap the certificate")

	require.NoError(t, apply())

	clientCert, clientKey := manager.clientCertLoader.Paths()
	require.Equal(t, newClientSS.CertPath, clientCert)
	require.Equal(t, newClientSS.KeyPath, clientKey)

	// The server certificate is left untouched by a client-only reconfigure.
	serverCert, _ := manager.serverCertLoader.Paths()
	require.Equal(t, serverSS.CertPath, serverCert)
	require.Equal(t, serverSerial, leafSerial(t, manager.serverCertLoader))

	t.Run("failed client reconfigure keeps the previous certificate", func(t *testing.T) {
		missing := path.Join(t.TempDir(), "absent.pem")

		apply, err := manager.PrepareReconfigure(WithClientCertificate(missing, missing))
		require.ErrorContains(t, err, "no such file or directory")
		require.Nil(t, apply)

		clientCert, _ := manager.clientCertLoader.Paths()
		require.Equal(t, newClientSS.CertPath, clientCert,
			"a certificate that fails to load must not disturb the active one")
	})
}
