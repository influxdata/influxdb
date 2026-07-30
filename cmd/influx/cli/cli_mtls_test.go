package cli_test

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"testing"

	"github.com/influxdata/influxdb/cmd/influx/cli"
	"github.com/influxdata/influxdb/pkg/testing/selfsigned"
	"github.com/stretchr/testify/require"
)

// mtlsTestServer starts an HTTPS test server that requires and verifies a client
// certificate signed by cert's CA. It returns the host and port the CLI should
// dial.
func mtlsTestServer(t *testing.T, cert *selfsigned.Cert) (host string, port int) {
	t.Helper()

	serverPair, err := tls.LoadX509KeyPair(cert.CertPath, cert.KeyPath)
	require.NoError(t, err)

	caPEM, err := os.ReadFile(cert.CACertPath)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(caPEM), "failed to append CA certificate")

	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Influxdb-Version", SERVER_VERSION)
	}))
	ts.TLS = &tls.Config{
		Certificates: []tls.Certificate{serverPair},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
	}
	ts.StartTLS()
	t.Cleanup(ts.Close)

	u, err := url.Parse(ts.URL)
	require.NoError(t, err)
	h, p, err := net.SplitHostPort(u.Host)
	require.NoError(t, err)
	port, err = strconv.Atoi(p)
	require.NoError(t, err)
	return h, port
}

// TestRunCLI_MutualTLS verifies the CLI presents its client certificate and
// verifies the server against the configured root CA.
func TestRunCLI_MutualTLS(t *testing.T) {
	cert := selfsigned.NewSelfSignedCert(t)
	host, port := mtlsTestServer(t, cert)

	c := cli.New(CLIENT_VERSION)
	c.Host = host
	c.Port = port
	c.Ssl = true
	c.ClientCert = cert.CertPath
	c.ClientKey = cert.KeyPath
	c.RootCA = cert.CACertPath
	c.Format = "column"
	c.ClientConfig.Precision = "ns"
	c.Execute = "SHOW DATABASES"
	c.IgnoreSignals = true
	c.ForceTTY = true

	require.NoError(t, c.Run())
	require.Equal(t, SERVER_VERSION, c.ServerVersion)
}

// TestRunCLI_MutualTLS_MissingClientCert verifies the connection is refused when
// the CLI trusts the server but presents no client certificate.
func TestRunCLI_MutualTLS_MissingClientCert(t *testing.T) {
	cert := selfsigned.NewSelfSignedCert(t)
	host, port := mtlsTestServer(t, cert)

	c := cli.New(CLIENT_VERSION)
	c.Host = host
	c.Port = port
	c.Ssl = true
	c.RootCA = cert.CACertPath // trust the server, but present no client cert
	c.Execute = "SHOW DATABASES"
	c.IgnoreSignals = true
	c.ForceTTY = true

	// The server aborts the handshake because no client certificate was sent.
	require.ErrorContains(t, c.Run(), "certificate required")
}

// TestRunCLI_MutualTLS_UntrustedServer verifies the server certificate is
// rejected when no root CA that signed it is configured.
func TestRunCLI_MutualTLS_UntrustedServer(t *testing.T) {
	cert := selfsigned.NewSelfSignedCert(t)
	host, port := mtlsTestServer(t, cert)

	c := cli.New(CLIENT_VERSION)
	c.Host = host
	c.Port = port
	c.Ssl = true
	c.ClientCert = cert.CertPath
	c.ClientKey = cert.KeyPath
	// No RootCA: the self-signed server is not in the system pool.
	c.Execute = "SHOW DATABASES"
	c.IgnoreSignals = true
	c.ForceTTY = true

	// The server certificate cannot be verified against any trusted CA, which
	// surfaces as a TLS certificate-verification error in the wrapped chain.
	err := c.Run()
	var certErr *tls.CertificateVerificationError
	require.ErrorAs(t, err, &certErr)
	require.ErrorContains(t, err, "certificate signed by unknown authority")
}
