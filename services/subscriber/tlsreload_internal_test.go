package subscriber

// Tests for reloading the subscriber's client TLS configuration. Open and
// PrepareReloadTLSCertificates both build their options from
// Config.TLSManagerOpts, so a reload can change any TLS setting, not just the
// certificate.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	th "github.com/influxdata/influxdb/pkg/testing/helper"
	"github.com/influxdata/influxdb/pkg/testing/selfsigned"
	"github.com/influxdata/influxdb/pkg/tlsconfig"
	"github.com/influxdata/influxdb/toml"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// certSerial returns the serial of ss's leaf certificate.
func certSerial(t *testing.T, ss *selfsigned.Cert) string {
	t.Helper()
	lc, err := tlsconfig.LoadCertificate(ss.CertPath, ss.KeyPath)
	require.NoError(t, err)
	return lc.Serial()
}

// presentedClientSerial returns the serial of the certificate cm's TLS config
// would offer to a server asking for a client certificate.
func presentedClientSerial(t *testing.T, cm *tlsconfig.TLSConfigManager) string {
	t.Helper()

	tlsConfig := cm.TLSConfig()
	require.NotNil(t, tlsConfig)
	require.NotNil(t, tlsConfig.GetClientCertificate)

	cert, err := tlsConfig.GetClientCertificate(&tls.CertificateRequestInfo{
		SignatureSchemes: []tls.SignatureScheme{tls.PKCS1WithSHA256},
	})
	require.NoError(t, err)
	require.NotEmpty(t, cert.Certificate)

	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	require.NoError(t, err)
	return leaf.SerialNumber.String()
}

// recordingMTLSServer starts an HTTPS server that requires a client certificate
// signed by any of clientCAs and records the serial most recently presented.
func recordingMTLSServer(t *testing.T, serverCert *selfsigned.Cert, clientCAs ...*selfsigned.Cert) (srv *httptest.Server, lastSerial func() string) {
	t.Helper()

	pool := x509.NewCertPool()
	for _, ca := range clientCAs {
		caPEM, err := os.ReadFile(ca.CACertPath)
		require.NoError(t, err)
		require.True(t, pool.AppendCertsFromPEM(caPEM))
	}

	cert, err := tls.LoadX509KeyPair(serverCert.CertPath, serverCert.KeyPath)
	require.NoError(t, err)

	var mu sync.Mutex
	var serial string
	srv = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
			serial = r.TLS.PeerCertificates[0].SerialNumber.String()
		}
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)

	return srv, func() string {
		mu.Lock()
		defer mu.Unlock()
		return serial
	}
}

// openService opens a subscriber Service with c and its own certificate monitor.
func openService(t *testing.T, c Config) *Service {
	t.Helper()

	certMonitor := tlsconfig.NewTLSCertMonitor()
	require.NoError(t, certMonitor.Open())
	t.Cleanup(th.CheckedClose(t, certMonitor))

	s := NewService(c, certMonitor)
	s.MetaClient = stubMetaClient{}
	require.NoError(t, s.Open())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

func writePoint(t *testing.T, w PointsWriter) {
	t.Helper()
	_, err := w.WritePointsContext(context.Background(), WriteRequest{
		Database:     "db0",
		lineProtocol: []byte("cpu value=1\n"),
	})
	require.NoError(t, err)
}

func TestConfig_TLSManagerOpts(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t)

	certMonitor := tlsconfig.NewTLSCertMonitor()
	require.NoError(t, certMonitor.Open())
	t.Cleanup(th.CheckedClose(t, certMonitor))

	c := NewConfig()
	c.CaCerts = ss.CACertPath
	c.Certificate = ss.CertPath
	c.PrivateKey = ss.KeyPath
	c.InsecureSkipVerify = true

	// Open and the reload hook share these options, so building a manager from
	// them must reproduce the configured settings.
	cm, err := tlsconfig.NewClientTLSConfigManager(certMonitor, c.TLSManagerOpts()...)
	require.NoError(t, err)
	defer th.CheckedClose(t, cm)()

	tlsConfig := cm.TLSConfig()
	require.NotNil(t, tlsConfig)
	require.True(t, tlsConfig.InsecureSkipVerify)
	require.NotNil(t, tlsConfig.RootCAs, "ca-certs should resolve into the root CA pool")

	// The certificate is offered as a client certificate, not a server one.
	require.NotNil(t, tlsConfig.GetClientCertificate)
	require.Nil(t, tlsConfig.GetCertificate)

	cert, err := tlsConfig.GetClientCertificate(&tls.CertificateRequestInfo{
		SignatureSchemes: []tls.SignatureScheme{tls.PKCS1WithSHA256},
	})
	require.NoError(t, err)
	require.NotEmpty(t, cert.Certificate)
}

func TestService_ReloadTLSConfig_ReconfiguresEverything(t *testing.T) {
	ssA := selfsigned.NewSelfSignedCert(t, selfsigned.WithCASubject("a", "CA A"))
	ssB := selfsigned.NewSelfSignedCert(t, selfsigned.WithCASubject("b", "CA B"))

	confA := NewConfig()
	confA.CaCerts = ssA.CACertPath
	confA.Certificate = ssA.CertPath
	confA.PrivateKey = ssA.KeyPath

	s := openService(t, confA)

	before := s.tlsManager.TLSConfig()
	require.NotNil(t, before)
	require.False(t, before.InsecureSkipVerify)

	// Reload with a different CA, a different client certificate, and insecure
	// verification enabled.
	confB := NewConfig()
	confB.CaCerts = ssB.CACertPath
	confB.Certificate = ssB.CertPath
	confB.PrivateKey = ssB.KeyPath
	confB.InsecureSkipVerify = true

	apply, err := s.PrepareReloadTLSCertificates(confB)
	require.NoError(t, err)
	require.NotNil(t, apply)

	// Preparing must not change anything on its own.
	require.False(t, s.tlsManager.TLSConfig().InsecureSkipVerify, "PrepareReload must only validate")

	require.NoError(t, apply())

	after := s.tlsManager.TLSConfig()
	require.True(t, after.InsecureSkipVerify, "allow-insecure should be reconfigured")
	require.NotNil(t, after.RootCAs)
	require.False(t, before.RootCAs.Equal(after.RootCAs), "root CAs should be rebuilt from the new config")

	require.Equal(t, certSerial(t, ssB), presentedClientSerial(t, s.tlsManager),
		"the reloaded client certificate should be the one offered to servers")
}

func TestService_ReloadTLSConfig_NewWriterUsesReloadedConfig(t *testing.T) {
	serverSS := selfsigned.NewSelfSignedCert(t)
	clientA := selfsigned.NewSelfSignedCert(t, selfsigned.WithCASubject("a", "Client CA A"))
	clientB := selfsigned.NewSelfSignedCert(t, selfsigned.WithCASubject("b", "Client CA B"))

	srv, lastSerial := recordingMTLSServer(t, serverSS, clientA, clientB)

	conf := NewConfig()
	conf.HTTPTimeout = toml.Duration(5 * time.Second)
	conf.CaCerts = serverSS.CACertPath
	conf.Certificate = clientA.CertPath
	conf.PrivateKey = clientA.KeyPath

	s := openService(t, conf)

	u, err := url.Parse(srv.URL)
	require.NoError(t, err)

	w, err := s.newPointsWriter(*u)
	require.NoError(t, err)
	writePoint(t, w)
	require.Equal(t, certSerial(t, clientA), lastSerial(), "the configured client certificate should be presented")

	// Reload with a different client certificate.
	confB := conf
	confB.Certificate = clientB.CertPath
	confB.PrivateKey = clientB.KeyPath

	apply, err := s.PrepareReloadTLSCertificates(confB)
	require.NoError(t, err)
	require.NoError(t, apply())

	// A writer created after the reload handshakes fresh and presents the new
	// certificate. This is the path subscriptions take when they are recreated.
	w2, err := s.newPointsWriter(*u)
	require.NoError(t, err)
	writePoint(t, w2)
	require.Equal(t, certSerial(t, clientB), lastSerial(), "a new writer should present the reloaded certificate")
}

// TestService_ReloadTLSConfig_EstablishedConnectionUndisturbed covers the two
// halves of a reload's effect on a writer that already exists. A connection that
// is already up is working and stays on the certificate it handshook with: a
// reload must not disturb it. Once that connection is replaced, the writer dials
// through the manager again and the new connection uses the reloaded settings.
func TestService_ReloadTLSConfig_EstablishedConnectionUndisturbed(t *testing.T) {
	serverSS := selfsigned.NewSelfSignedCert(t)
	clientA := selfsigned.NewSelfSignedCert(t, selfsigned.WithCASubject("a", "Client CA A"))
	clientB := selfsigned.NewSelfSignedCert(t, selfsigned.WithCASubject("b", "Client CA B"))

	srv, lastSerial := recordingMTLSServer(t, serverSS, clientA, clientB)

	conf := NewConfig()
	conf.HTTPTimeout = toml.Duration(5 * time.Second)
	conf.CaCerts = serverSS.CACertPath
	conf.Certificate = clientA.CertPath
	conf.PrivateKey = clientA.KeyPath

	s := openService(t, conf)

	u, err := url.Parse(srv.URL)
	require.NoError(t, err)

	w, err := s.newPointsWriter(*u)
	require.NoError(t, err)
	writePoint(t, w)
	require.Equal(t, certSerial(t, clientA), lastSerial())

	confB := conf
	confB.Certificate = clientB.CertPath
	confB.PrivateKey = clientB.KeyPath

	apply, err := s.PrepareReloadTLSCertificates(confB)
	require.NoError(t, err)
	require.NoError(t, apply())

	// Same writer, connection reused: no handshake, so the working connection
	// carries on with the certificate it was built with.
	writePoint(t, w)
	require.Equal(t, certSerial(t, clientA), lastSerial(),
		"a reload must not disturb an established connection")

	// Drop the pooled connection from the client side so the next write has to
	// dial again. Closing it from the server side instead would race: the client
	// can pick the already-dead connection for a write, which is not idempotent
	// and so is not always retried.
	require.NoError(t, w.(*HTTP).c.Close())

	// The writer dials through the manager, so its next connection picks up the
	// reloaded certificate without the writer being rebuilt.
	writePoint(t, w)
	require.Equal(t, certSerial(t, clientB), lastSerial(),
		"a reconnect should present the reloaded certificate")
}

// TestService_ReloadTLSConfig_ExistingWriterHonorsReloadedRootCA covers a
// setting that has no per-connection callback of its own. Because writers dial
// through the manager, root CAs are resolved per connection like everything
// else, so a writer that already exists picks up a reloaded pool. Nothing here
// forces a reconnect: the first handshake fails, so no connection is pooled and
// the next write dials again on its own.
func TestService_ReloadTLSConfig_ExistingWriterHonorsReloadedRootCA(t *testing.T) {
	serverSS := selfsigned.NewSelfSignedCert(t)
	otherSS := selfsigned.NewSelfSignedCert(t, selfsigned.WithCASubject("other", "Other CA"))

	cert, err := tls.LoadX509KeyPair(serverSS.CertPath, serverSS.KeyPath)
	require.NoError(t, err)

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	srv.StartTLS()
	t.Cleanup(srv.Close)

	// Trust the wrong CA to begin with, so the endpoint cannot be verified.
	conf := NewConfig()
	conf.HTTPTimeout = toml.Duration(5 * time.Second)
	conf.CaCerts = otherSS.CACertPath

	s := openService(t, conf)

	u, err := url.Parse(srv.URL)
	require.NoError(t, err)

	w, err := s.newPointsWriter(*u)
	require.NoError(t, err)

	_, err = w.WritePointsContext(context.Background(), WriteRequest{
		Database:     "db0",
		lineProtocol: []byte("cpu value=1\n"),
	})
	require.ErrorContains(t, err, "certificate signed by unknown authority")

	// Reload trusting the endpoint's CA.
	good := conf
	good.CaCerts = serverSS.CACertPath

	apply, err := s.PrepareReloadTLSCertificates(good)
	require.NoError(t, err)
	require.NoError(t, apply())

	// The same writer now verifies the endpoint against the reloaded pool.
	writePoint(t, w)
}

func TestService_ReloadTLSConfig_Failures(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t)

	conf := NewConfig()
	conf.Certificate = ss.CertPath
	conf.PrivateKey = ss.KeyPath

	t.Run("unloadable certificate keeps the previous configuration", func(t *testing.T) {
		s := openService(t, conf)

		bad := conf
		bad.Certificate = path.Join(t.TempDir(), "absent.pem")
		bad.PrivateKey = bad.Certificate

		apply, err := s.PrepareReloadTLSCertificates(bad)
		require.ErrorContains(t, err, "subscriber: TLS certificate reload failed")
		require.Nil(t, apply)

		// The message must name the path that failed to load, not the one still
		// in use, or it sends the reader to the wrong file.
		require.ErrorContains(t, err, bad.Certificate)
		require.NotContains(t, err.Error(), ss.CertPath)

		// The active certificate is untouched, so the service keeps working.
		require.Equal(t, certSerial(t, ss), presentedClientSerial(t, s.tlsManager))
	})

	t.Run("unusable root CA keeps the previous configuration", func(t *testing.T) {
		s := openService(t, conf)
		before := s.tlsManager.TLSConfig()

		bad := conf
		bad.CaCerts = path.Join(t.TempDir(), "absent-ca.pem")

		apply, err := s.PrepareReloadTLSCertificates(bad)
		require.Error(t, err)
		require.Nil(t, apply)

		require.Equal(t, before.RootCAs, s.tlsManager.TLSConfig().RootCAs)
	})
}

func TestService_ReloadTLSConfig_DisabledService(t *testing.T) {
	conf := NewConfig()
	conf.Enabled = false

	certMonitor := tlsconfig.NewTLSCertMonitor()
	require.NoError(t, certMonitor.Open())
	t.Cleanup(th.CheckedClose(t, certMonitor))

	s := NewService(conf, certMonitor)
	s.MetaClient = stubMetaClient{}

	// Open on a disabled service returns immediately without starting anything,
	// so there is nothing to Close afterwards.
	require.NoError(t, s.Open())

	// A disabled service never built a TLS manager, so a reload has nothing to
	// do and must not fail the whole configuration reload.
	require.Nil(t, s.tlsManager)

	apply, err := s.PrepareReloadTLSCertificates(NewConfig())
	require.NoError(t, err)
	require.NotNil(t, apply)
	require.NoError(t, apply())
}

// TestService_TLSUsage covers the service naming itself to the certificate
// monitor, which groups its warnings by usage.
func TestService_TLSUsage(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t)

	core, logs := observer.New(zapcore.InfoLevel)
	certMonitor := tlsconfig.NewTLSCertMonitor(tlsconfig.WithMonitorLogger(zap.New(core)))
	require.NoError(t, certMonitor.Open())
	t.Cleanup(th.CheckedClose(t, certMonitor))

	conf := NewConfig()
	conf.Certificate = ss.CertPath
	conf.PrivateKey = ss.KeyPath

	s := NewService(conf, certMonitor)
	s.MetaClient = stubMetaClient{}
	require.NoError(t, s.Open())
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	entries := logs.FilterMessage("Registered certificate loader").TakeAll()
	require.Len(t, entries, 1)
	require.Equal(t, "subscriber.client", entries[0].ContextMap()["usage"],
		"the subscriber holds a client certificate, not a server one")
}
