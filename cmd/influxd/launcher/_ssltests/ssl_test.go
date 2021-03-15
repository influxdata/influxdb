package ssltests

import (
	"context"
	"os"
	"testing"

	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	"github.com/influxdata/influxdb/v2/http"
	"github.com/stretchr/testify/require"
)

const (
	certPathVar = "INFLUXDB_TEST_SSL_CERT_PATH"
	certKeyVar  = "INFLUXDB_TEST_SSL_KEY_PATH"
)

var (
	certPath string
	keyPath  string
)

func init() {
	certPath = os.Getenv(certPathVar)
	keyPath = os.Getenv(certKeyVar)
}

func TestSsl_NonStrict(t *testing.T) {
	require.NotEmpty(t, certPath, "INFLUXDB_TEST_SSL_CERT_PATH must be set to run this test")
	require.NotEmpty(t, keyPath, "INFLUXDB_TEST_SSL_KEY_PATH must be set to run this test")
	ctx := context.Background()

	l := launcher.NewTestLauncher()
	l.RunOrFail(t, ctx, func(o *launcher.InfluxdOpts) {
		o.HttpTLSCert = certPath
		o.HttpTLSKey = keyPath
		o.HttpTLSStrictCiphers = false
	})
	defer l.ShutdownOrFail(t, ctx)

	req, err := l.NewHTTPRequest("GET", "/ping", "", "")
	require.NoError(t, err)
	require.Regexp(t, "https://.*", req.URL)

	client := http.NewClient("https", true)
	_, err = client.Do(req)
	require.NoError(t, err)
}

func TestSsl_Strict(t *testing.T) {
	require.NotEmpty(t, certPath, "INFLUXDB_TEST_SSL_CERT_PATH must be set to run this test")
	require.NotEmpty(t, keyPath, "INFLUXDB_TEST_SSL_KEY_PATH must be set to run this test")
	ctx := context.Background()

	l := launcher.NewTestLauncher()
	l.RunOrFail(t, ctx, func(o *launcher.InfluxdOpts) {
		o.HttpTLSCert = certPath
		o.HttpTLSKey = keyPath
		o.HttpTLSStrictCiphers = true
	})
	defer l.ShutdownOrFail(t, ctx)

	req, err := l.NewHTTPRequest("GET", "/ping", "", "")
	require.NoError(t, err)
	require.Regexp(t, "https://.*", req.URL)

	client := http.NewClient("https", true)
	_, err = client.Do(req)
	require.NoError(t, err)
}
