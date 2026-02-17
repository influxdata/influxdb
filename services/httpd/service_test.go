package httpd_test

import (
	"crypto/tls"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"testing"

	th "github.com/influxdata/influxdb/pkg/testing/helper"
	"github.com/influxdata/influxdb/pkg/testing/selfsigned"
	"github.com/influxdata/influxdb/pkg/tlsconfig"
	"github.com/influxdata/influxdb/services/httpd"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestService_VerifyReloadedConfig(t *testing.T) {
	t.Run("HTTPS disabled, no change", func(t *testing.T) {
		// Create service with HTTPS disabled
		config := httpd.Config{
			HTTPSEnabled: false,
		}
		s := httpd.NewService(config)
		s.WithLogger(zap.NewNop())

		// Verify reload with HTTPS still disabled
		newConfig := httpd.Config{
			HTTPSEnabled: false,
		}
		applyFunc, err := s.PrepareReloadConfig(newConfig)
		require.NoError(t, err)
		require.Nil(t, applyFunc, "no apply function should be returned when HTTPS is disabled")
	})

	t.Run("HTTPS enabled to disabled returns error", func(t *testing.T) {
		// Create certificates for initial service
		ss := selfsigned.NewSelfSignedCert(t)

		// Create service with HTTPS enabled
		config := httpd.Config{
			HTTPSEnabled:     true,
			HTTPSCertificate: ss.CertPath,
			HTTPSPrivateKey:  ss.KeyPath,
		}
		s := httpd.NewService(config)
		s.WithLogger(zap.NewNop())

		// Open service to initialize certLoader
		require.NoError(t, s.Open())
		defer th.CheckedClose(t, s)()

		// Try to verify reload with HTTPS disabled
		newConfig := httpd.Config{
			HTTPSEnabled: false,
		}
		applyFunc, err := s.PrepareReloadConfig(newConfig)
		require.ErrorContains(t, err, "can not change https-enabled on a running server")
		require.Nil(t, applyFunc)
	})

	t.Run("HTTPS disabled to enabled returns error", func(t *testing.T) {
		// Create service with HTTPS disabled
		config := httpd.Config{
			HTTPSEnabled: false,
		}
		s := httpd.NewService(config)
		s.WithLogger(zap.NewNop())

		// Create certificates for reload attempt
		ss := selfsigned.NewSelfSignedCert(t)

		// Try to verify reload with HTTPS enabled
		newConfig := httpd.Config{
			HTTPSEnabled:     true,
			HTTPSCertificate: ss.CertPath,
			HTTPSPrivateKey:  ss.KeyPath,
		}
		applyFunc, err := s.PrepareReloadConfig(newConfig)
		require.ErrorContains(t, err, "can not change https-enabled on a running server")
		require.Nil(t, applyFunc)
	})

	t.Run("HTTPS enabled, valid certificate reload", func(t *testing.T) {
		// Create initial certificates
		ss1 := selfsigned.NewSelfSignedCert(t, selfsigned.WithDNSName("initial.example.com"))

		// Create service with HTTPS enabled
		config := httpd.Config{
			BindAddress:      "localhost:",
			HTTPSEnabled:     true,
			HTTPSCertificate: ss1.CertPath,
			HTTPSPrivateKey:  ss1.KeyPath,
			TLS:              new(tls.Config),
		}
		s := httpd.NewService(config)
		s.WithLogger(zap.NewNop())

		// Open service to initialize certLoader
		require.NoError(t, s.Open())
		defer th.CheckedClose(t, s)()

		// Get the initial certificate serial number for comparison
		initialCert, err := tlsconfig.LoadCertificate(ss1.CertPath, ss1.KeyPath)
		require.NoError(t, err)
		initialSerial := initialCert.Leaf.SerialNumber

		// Verify the certificate is in use.
		{
			transport := &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			}
			client := http.Client{Transport: transport}
			pingURI := fmt.Sprintf("https://%s/ping", s.Addr())
			resp, err := client.Get(pingURI)
			require.NoError(t, err)
			require.NotNil(t, resp.TLS)
			require.NotEmpty(t, resp.TLS.PeerCertificates)
			require.Equal(t, initialSerial, resp.TLS.PeerCertificates[0].SerialNumber)
		}

		// Create new certificates for reload
		ss2 := selfsigned.NewSelfSignedCert(t, selfsigned.WithDNSName("reloaded.example.com"))

		// Get the new certificate serial number
		newCert, err := tlsconfig.LoadCertificate(ss2.CertPath, ss2.KeyPath)
		require.NoError(t, err)
		newSerial := newCert.Leaf.SerialNumber
		require.NotEqual(t, initialSerial, newSerial, "test setup: certificates should have different serials")

		// Verify reload with new certificate paths
		newConfig := httpd.Config{
			HTTPSEnabled:     true,
			HTTPSCertificate: ss2.CertPath,
			HTTPSPrivateKey:  ss2.KeyPath,
		}
		applyFunc, err := s.PrepareReloadConfig(newConfig)
		require.NoError(t, err)
		require.NotNil(t, applyFunc, "apply function should be returned for successful verification")

		// Apply the certificate reload
		require.NoError(t, applyFunc())

		// Verify the new certificate is in use.
		{
			transport := &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			}
			client := http.Client{Transport: transport}
			pingURI := fmt.Sprintf("https://%s/ping", s.Addr())
			resp, err := client.Get(pingURI)
			require.NoError(t, err)
			require.NotNil(t, resp.TLS)
			require.NotEmpty(t, resp.TLS.PeerCertificates)
			require.Equal(t, newSerial, resp.TLS.PeerCertificates[0].SerialNumber)
		}
	})

	t.Run("HTTPS enabled, VerifyLoad fails", func(t *testing.T) {
		// Create initial certificates
		ss := selfsigned.NewSelfSignedCert(t)

		// Create service with HTTPS enabled
		config := httpd.Config{
			HTTPSEnabled:     true,
			HTTPSCertificate: ss.CertPath,
			HTTPSPrivateKey:  ss.KeyPath,
		}
		s := httpd.NewService(config)
		s.WithLogger(zap.NewNop())

		// Open service to initialize certLoader
		require.NoError(t, s.Open())
		defer th.CheckedClose(t, s)()

		// Try to verify reload with non-existent certificate (one example of VerifyLoad failure)
		newConfig := httpd.Config{
			HTTPSEnabled:     true,
			HTTPSCertificate: "/nonexistent/path/cert.pem",
			HTTPSPrivateKey:  "/nonexistent/path/key.pem",
		}
		applyFunc, err := s.PrepareReloadConfig(newConfig)
		require.ErrorContains(t, err, "error loading certificate")
		require.Nil(t, applyFunc)
	})

	t.Run("HTTPS enabled, no certificate change (same paths)", func(t *testing.T) {
		// Create initial certificates
		ss := selfsigned.NewSelfSignedCert(t)

		// Create service with HTTPS enabled
		config := httpd.Config{
			HTTPSEnabled:     true,
			HTTPSCertificate: ss.CertPath,
			HTTPSPrivateKey:  ss.KeyPath,
			TLS:              new(tls.Config),
		}
		s := httpd.NewService(config)
		s.WithLogger(zap.NewNop())

		// Open service to initialize certLoader
		require.NoError(t, s.Open())
		defer th.CheckedClose(t, s)()

		// Get the certificate serial number
		cert, err := tlsconfig.LoadCertificate(ss.CertPath, ss.KeyPath)
		require.NoError(t, err)
		expectedSerial := cert.Leaf.SerialNumber

		// Verify the certificate is in use.
		{
			transport := &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			}
			client := http.Client{Transport: transport}
			pingURI := fmt.Sprintf("https://%s/ping", s.Addr())
			resp, err := client.Get(pingURI)
			require.NoError(t, err)
			require.NotNil(t, resp.TLS)
			require.NotEmpty(t, resp.TLS.PeerCertificates)
			require.Equal(t, expectedSerial, resp.TLS.PeerCertificates[0].SerialNumber)
		}

		// Verify reload with same certificate paths (common case for reloading updated files)
		newConfig := httpd.Config{
			HTTPSEnabled:     true,
			HTTPSCertificate: ss.CertPath,
			HTTPSPrivateKey:  ss.KeyPath,
		}
		applyFunc, err := s.PrepareReloadConfig(newConfig)
		require.NoError(t, err)
		require.NotNil(t, applyFunc, "apply function should be returned even for same paths")

		// Apply should succeed
		require.NoError(t, applyFunc())

		// Verify the certificate is loaded correctly
		{
			transport := &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			}
			client := http.Client{Transport: transport}
			pingURI := fmt.Sprintf("https://%s/ping", s.Addr())
			resp, err := client.Get(pingURI)
			require.NoError(t, err)
			require.NotNil(t, resp.TLS)
			require.NotEmpty(t, resp.TLS.PeerCertificates)
			require.Equal(t, expectedSerial, resp.TLS.PeerCertificates[0].SerialNumber)
		}
	})
}

// openService creates, opens, and registers cleanup for an httpd.Service.
func openService(t *testing.T, config httpd.Config) *httpd.Service {
	t.Helper()
	s := httpd.NewService(config)
	s.WithLogger(zap.NewNop())
	require.NoError(t, s.Open())
	t.Cleanup(th.CheckedClose(t, s))
	return s
}

// insecureHTTPSClient returns an *http.Client that skips TLS certificate verification.
func insecureHTTPSClient() *http.Client {
	return &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}}
}

// pingHTTPS performs a GET /ping over HTTPS using the given client and returns the response.
func pingHTTPS(t *testing.T, addr net.Addr, client *http.Client) *http.Response {
	t.Helper()
	resp, err := client.Get(fmt.Sprintf("https://%s/ping", addr))
	require.NoError(t, err)
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

// requireServingCertSerial verifies the service is serving TLS with the expected certificate serial.
func requireServingCertSerial(t *testing.T, s *httpd.Service, expectedSerial *big.Int) {
	t.Helper()
	resp := pingHTTPS(t, s.Addr(), insecureHTTPSClient())
	require.NotNil(t, resp.TLS)
	require.NotEmpty(t, resp.TLS.PeerCertificates)
	require.Equal(t, expectedSerial, resp.TLS.PeerCertificates[0].SerialNumber)
}

// loadCertSerial loads a certificate and returns its serial number.
func loadCertSerial(t *testing.T, certPath, keyPath string) *big.Int {
	t.Helper()
	cert, err := tlsconfig.LoadCertificate(certPath, keyPath)
	require.NoError(t, err)
	return cert.Leaf.SerialNumber
}

// TestService_Open_TLSConfigManager verifies that the TLSConfigManager created
// in Service.Open is configured properly based on the Service's configuration.
func TestService_Open_TLSConfigManager(t *testing.T) {
	t.Run("HTTPS disabled creates non-TLS listener", func(t *testing.T) {
		s := openService(t, httpd.Config{
			BindAddress:  "localhost:",
			HTTPSEnabled: false,
		})

		resp, err := http.Get(fmt.Sprintf("http://%s/ping", s.Addr()))
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, http.StatusNoContent, resp.StatusCode)
		require.Nil(t, resp.TLS)
	})

	t.Run("HTTPS enabled creates TLS listener with correct certificate", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)
		s := openService(t, httpd.Config{
			BindAddress:      "localhost:",
			HTTPSEnabled:     true,
			HTTPSCertificate: ss.CertPath,
			HTTPSPrivateKey:  ss.KeyPath,
		})

		requireServingCertSerial(t, s, loadCertSerial(t, ss.CertPath, ss.KeyPath))
	})

	t.Run("HTTPS enabled with missing certificate fails Open", func(t *testing.T) {
		s := httpd.NewService(httpd.Config{
			BindAddress:      "localhost:",
			HTTPSEnabled:     true,
			HTTPSCertificate: "/nonexistent/cert.pem",
			HTTPSPrivateKey:  "/nonexistent/key.pem",
		})
		s.WithLogger(zap.NewNop())

		err := s.Open()
		require.Error(t, err)
		require.ErrorContains(t, err, "error creating TLS manager")
	})

	t.Run("HTTPS enabled applies base TLS config MinVersion", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)
		s := openService(t, httpd.Config{
			BindAddress:      "localhost:",
			HTTPSEnabled:     true,
			HTTPSCertificate: ss.CertPath,
			HTTPSPrivateKey:  ss.KeyPath,
			TLS:              &tls.Config{MinVersion: tls.VersionTLS13},
		})

		pingURI := fmt.Sprintf("https://%s/ping", s.Addr())

		// A client restricted to TLS 1.2 should fail the handshake.
		tls12Client := &http.Client{Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
				MinVersion:         tls.VersionTLS12,
				MaxVersion:         tls.VersionTLS12,
			},
		}}
		_, err := tls12Client.Get(pingURI)
		require.Error(t, err, "TLS 1.2 client should be rejected when server requires TLS 1.3")

		// A client using TLS 1.3 should succeed.
		resp := pingHTTPS(t, s.Addr(), insecureHTTPSClient())
		require.Equal(t, http.StatusNoContent, resp.StatusCode)
		require.Equal(t, uint16(tls.VersionTLS13), resp.TLS.Version)
	})

	t.Run("insecureCert=false rejects permissive key file permissions", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)
		require.NoError(t, os.Chmod(ss.KeyPath, 0644))

		s := httpd.NewService(httpd.Config{
			BindAddress:              "localhost:",
			HTTPSEnabled:             true,
			HTTPSCertificate:         ss.CertPath,
			HTTPSPrivateKey:          ss.KeyPath,
			HTTPSInsecureCertificate: false,
		})
		s.WithLogger(zap.NewNop())

		err := s.Open()
		require.Error(t, err, "Open should fail when key file permissions are too permissive")
		require.ErrorContains(t, err, "error creating TLS manager")
	})

	t.Run("insecureCert=true allows permissive key file permissions", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)
		require.NoError(t, os.Chmod(ss.KeyPath, 0644))

		s := openService(t, httpd.Config{
			BindAddress:              "localhost:",
			HTTPSEnabled:             true,
			HTTPSCertificate:         ss.CertPath,
			HTTPSPrivateKey:          ss.KeyPath,
			HTTPSInsecureCertificate: true,
		})

		resp := pingHTTPS(t, s.Addr(), insecureHTTPSClient())
		require.Equal(t, http.StatusNoContent, resp.StatusCode)
		require.NotNil(t, resp.TLS)
	})

	t.Run("HTTPS enabled with combined cert and key file", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t, selfsigned.WithCombinedFile())
		s := openService(t, httpd.Config{
			BindAddress:      "localhost:",
			HTTPSEnabled:     true,
			HTTPSCertificate: ss.CertPath,
			HTTPSPrivateKey:  "",
		})

		resp := pingHTTPS(t, s.Addr(), insecureHTTPSClient())
		require.Equal(t, http.StatusNoContent, resp.StatusCode)
		require.NotNil(t, resp.TLS)
		require.NotEmpty(t, resp.TLS.PeerCertificates)
	})
}
