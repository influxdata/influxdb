package httpd_test

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"testing"

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
		defer func() {
			require.NoError(t, s.Close())
		}()

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
		defer func() {
			require.NoError(t, s.Close())
		}()

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
		defer func() {
			require.NoError(t, s.Close())
		}()

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

	t.Run("HTTPS enabled, no certificate change", func(t *testing.T) {
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
		defer func() {
			require.NoError(t, s.Close())
		}()

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
