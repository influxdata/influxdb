package subscriber

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb/pkg/testing/selfsigned"
	"github.com/influxdata/influxdb/pkg/tlsconfig"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/toml"
	"github.com/stretchr/testify/require"
)

// newMTLSServer starts an HTTPS test server that requires and verifies client
// certificates signed by clientCACertPath. It records whether the last request
// presented a client certificate.
func newMTLSServer(t *testing.T, serverCert *selfsigned.Cert, clientCACertPath string, sawClientCert *bool) *httptest.Server {
	t.Helper()

	clientCAs := x509.NewCertPool()
	caPEM, err := os.ReadFile(clientCACertPath)
	require.NoError(t, err)
	require.True(t, clientCAs.AppendCertsFromPEM(caPEM))

	cert, err := tls.LoadX509KeyPair(serverCert.CertPath, serverCert.KeyPath)
	require.NoError(t, err)

	var mu sync.Mutex
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		*sawClientCert = r.TLS != nil && len(r.TLS.PeerCertificates) > 0
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
	}
	srv.StartTLS()
	return srv
}

// TestHTTP_PresentsClientCertificate verifies that the subscriber presents its
// configured client certificate on outbound HTTPS connections (the client half
// of mutual TLS), while trusting the endpoint's server certificate via the
// legacy ca-certs setting. A writer without a client certificate is rejected.
func TestHTTP_PresentsClientCertificate(t *testing.T) {
	// serverSS is the endpoint's server certificate (default SANs include
	// 127.0.0.1); clientSS provides the subscriber's client certificate and the
	// CA the endpoint trusts for client authentication.
	serverSS := selfsigned.NewSelfSignedCert(t)
	clientSS := selfsigned.NewSelfSignedCert(t, selfsigned.WithCASubject("client", "Client CA"))

	var sawClientCert bool
	srv := newMTLSServer(t, serverSS, clientSS.CACertPath, &sawClientCert)
	defer srv.Close()

	// Configure the subscriber the way an operator would: trust the endpoint
	// server via the legacy ca-certs, and present a client certificate.
	conf := Config{
		HTTPTimeout: toml.Duration(5 * time.Second),
		CaCerts:     serverSS.CACertPath,
		Certificate: clientSS.CertPath,
		PrivateKey:  clientSS.KeyPath,
	}

	newManager := func(withClientCert bool) *tlsconfig.TLSConfigManager {
		opts := []tlsconfig.TLSConfigManagerOpt{tlsconfig.WithRootCA(conf.effectiveRootCA())}
		if withClientCert {
			opts = append(opts, tlsconfig.WithCertificate(conf.Certificate, conf.PrivateKey))
		}
		cm, err := tlsconfig.NewClientTLSConfigManager(true, conf.TLS, conf.InsecureSkipVerify, opts...)
		require.NoError(t, err)
		return cm
	}

	t.Run("with client certificate is accepted", func(t *testing.T) {
		cm := newManager(true)
		defer cm.Close()

		w, err := NewHTTPS(srv.URL, time.Duration(conf.HTTPTimeout), cm.TLSConfig())
		require.NoError(t, err)

		_, err = w.WritePointsContext(context.Background(), WriteRequest{
			Database:     "db0",
			lineProtocol: []byte("cpu value=1\n"),
		})
		require.NoError(t, err)
		require.True(t, sawClientCert, "server should have received the client certificate")
	})

	t.Run("without client certificate is rejected", func(t *testing.T) {
		// Trusts the server, but presents no client certificate.
		cm := newManager(false)
		defer cm.Close()

		w, err := NewHTTPS(srv.URL, time.Duration(conf.HTTPTimeout), cm.TLSConfig())
		require.NoError(t, err)

		_, err = w.WritePointsContext(context.Background(), WriteRequest{
			Database:     "db0",
			lineProtocol: []byte("cpu value=1\n"),
		})
		require.Error(t, err, "endpoint should reject a client without a certificate")
	})
}

// stubMetaClient is a minimal MetaClient for opening the service with no
// subscriptions.
type stubMetaClient struct{}

func (stubMetaClient) Databases() []meta.DatabaseInfo    { return nil }
func (stubMetaClient) WaitForDataChanged() chan struct{} { return make(chan struct{}) }

// TestService_PrepareReloadTLSCertificates verifies the certificate-reload hook:
// it is a no-op when no client certificate is configured, and returns a working
// apply function when one is.
func TestService_PrepareReloadTLSCertificates(t *testing.T) {
	open := func(t *testing.T, c Config) *Service {
		t.Helper()
		s := NewService(c)
		s.MetaClient = stubMetaClient{}
		require.NoError(t, s.Open())
		t.Cleanup(func() { require.NoError(t, s.Close()) })
		return s
	}

	t.Run("no client certificate is a no-op", func(t *testing.T) {
		s := open(t, NewConfig())
		apply, err := s.PrepareReloadTLSCertificates()
		require.NoError(t, err)
		require.NotNil(t, apply)
		require.NoError(t, apply())
	})

	t.Run("reloads a configured client certificate", func(t *testing.T) {
		clientSS := selfsigned.NewSelfSignedCert(t, selfsigned.WithCASubject("client", "Client CA"))
		c := NewConfig()
		c.Certificate = clientSS.CertPath
		c.PrivateKey = clientSS.KeyPath

		s := open(t, c)
		apply, err := s.PrepareReloadTLSCertificates()
		require.NoError(t, err)
		require.NotNil(t, apply)
		require.NoError(t, apply())
	})
}
