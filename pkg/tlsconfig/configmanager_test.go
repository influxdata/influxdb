package tlsconfig

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"path"
	"testing"
	"time"

	"github.com/influxdata/influxdb/pkg/testing/selfsigned"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestTLSConfigManager_ConsistentInstances(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t)

	manager, err := NewTLSConfigManager(true, nil, ss.CertPath, ss.KeyPath, false)
	require.NoError(t, err)
	require.NotNil(t, manager)
	defer func() {
		require.NoError(t, manager.Close())
	}()

	// Get TLS config
	tlsConfig := manager.TLSConfig()
	require.NotNil(t, tlsConfig)

	// Subsequent calls should return the same instance
	tlsConfig2 := manager.TLSConfig()
	require.Same(t, tlsConfig, tlsConfig2)

	// TLSCertLoader should also be available
	certLoader := manager.TLSCertLoader()
	require.NotNil(t, certLoader)

	// Subsequent calls should return the same instance
	certLoader2 := manager.TLSCertLoader()
	require.Same(t, certLoader, certLoader2)
}

func TestTLSConfigManager_BaseConfigCloned(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t)

	baseConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		MaxVersion: tls.VersionTLS13,
		ServerName: "test.example.com",
	}

	manager, err := NewTLSConfigManager(true, baseConfig, ss.CertPath, ss.KeyPath, false)
	require.NoError(t, err)
	require.NotNil(t, manager)
	defer func() {
		require.NoError(t, manager.Close())
	}()

	tlsConfig := manager.TLSConfig()
	require.NotNil(t, tlsConfig)

	// Verify base config values are preserved
	require.Equal(t, tls.VersionTLS12, int(tlsConfig.MinVersion))
	require.Equal(t, tls.VersionTLS13, int(tlsConfig.MaxVersion))
	require.Equal(t, "test.example.com", tlsConfig.ServerName)

	// Verify that modifying the original base config doesn't affect the loaded config
	baseConfig.ServerName = "modified.example.com"
	require.Equal(t, "test.example.com", tlsConfig.ServerName)

	// Verify that loaded config is a different instance
	require.NotSame(t, baseConfig, tlsConfig)
}

func TestTLSConfigManager_NilBaseConfig(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t)

	manager, err := NewTLSConfigManager(true, nil, ss.CertPath, ss.KeyPath, false)
	require.NoError(t, err)
	require.NotNil(t, manager)
	defer func() {
		require.NoError(t, manager.Close())
	}()

	tlsConfig := manager.TLSConfig()
	require.NotNil(t, tlsConfig)

	// Should have default zero values for a new tls.Config
	require.Equal(t, uint16(0), tlsConfig.MinVersion)
	require.Equal(t, uint16(0), tlsConfig.MaxVersion)
}

func TestTLSConfigManager_CertLoaderCallbacksSet(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t)

	manager, err := NewTLSConfigManager(true, nil, ss.CertPath, ss.KeyPath, false)
	require.NoError(t, err)
	require.NotNil(t, manager)
	defer func() {
		require.NoError(t, manager.Close())
	}()

	tlsConfig := manager.TLSConfig()
	require.NotNil(t, tlsConfig)

	// Verify that TLSCertLoader.SetupTLSConfig was called by checking callbacks are set
	require.NotNil(t, tlsConfig.GetCertificate, "GetCertificate callback should be set")
	require.NotNil(t, tlsConfig.GetClientCertificate, "GetClientCertificate callback should be set")
}

func TestTLSConfigManager_CertLoaderPathsCorrect(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t)

	manager, err := NewTLSConfigManager(true, nil, ss.CertPath, ss.KeyPath, false)
	require.NoError(t, err)
	require.NotNil(t, manager)
	defer func() {
		require.NoError(t, manager.Close())
	}()

	certLoader := manager.TLSCertLoader()
	require.NotNil(t, certLoader)

	// Verify the paths were passed correctly to TLSCertLoader
	certPath, keyPath := certLoader.Paths()
	require.Equal(t, ss.CertPath, certPath)
	require.Equal(t, ss.KeyPath, keyPath)
}

func TestTLSConfigManager_ConstructorError(t *testing.T) {
	// Use non-existent paths to verify NewTLSConfigLoader returns error
	manager, err := NewTLSConfigManager(true, nil, "/nonexistent/cert.pem", "/nonexistent/key.pem", false)
	require.ErrorContains(t, err, "LoadCertificate: error opening \"/nonexistent/cert.pem\" for reading: open /nonexistent/cert.pem: no such file or directory")
	require.Nil(t, manager)
}

func TestTLSConfigManager_InsecureSkipVerify(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t)

	t.Run("allowInsecure true", func(t *testing.T) {
		manager, err := NewTLSConfigManager(true, nil, ss.CertPath, ss.KeyPath, true)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		tlsConfig := manager.TLSConfig()
		require.True(t, tlsConfig.InsecureSkipVerify)
	})

	t.Run("allowInsecure false", func(t *testing.T) {
		manager, err := NewTLSConfigManager(true, nil, ss.CertPath, ss.KeyPath, false)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		tlsConfig := manager.TLSConfig()
		require.False(t, tlsConfig.InsecureSkipVerify)
	})

	t.Run("overrides base config", func(t *testing.T) {
		baseConfig := &tls.Config{
			InsecureSkipVerify: true,
		}

		manager, err := NewTLSConfigManager(true, baseConfig, ss.CertPath, ss.KeyPath, false)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		tlsConfig := manager.TLSConfig()
		require.False(t, tlsConfig.InsecureSkipVerify, "allowInsecure should override base config")
	})
}

func TestTLSConfigManager_MultipleLoadersIndependent(t *testing.T) {
	ss1 := selfsigned.NewSelfSignedCert(t, selfsigned.WithDNSName("server1.example.com"))
	ss2 := selfsigned.NewSelfSignedCert(t, selfsigned.WithDNSName("server2.example.com"))

	loader1, err := NewTLSConfigManager(true, nil, ss1.CertPath, ss1.KeyPath, false)
	require.NoError(t, err)
	loader2, err := NewTLSConfigManager(true, nil, ss2.CertPath, ss2.KeyPath, false)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, loader1.Close())
		require.NoError(t, loader2.Close())
	}()

	tlsConfig1 := loader1.TLSConfig()
	tlsConfig2 := loader2.TLSConfig()

	// Configs should be different instances
	require.NotSame(t, tlsConfig1, tlsConfig2)

	// Cert loaders should be different instances
	certLoader1 := loader1.TLSCertLoader()
	certLoader2 := loader2.TLSCertLoader()
	require.NotSame(t, certLoader1, certLoader2)

	// Each manager should have its own certificate paths
	cp1, kp1 := certLoader1.Paths()
	cp2, kp2 := certLoader2.Paths()
	require.Equal(t, ss1.CertPath, cp1)
	require.Equal(t, ss1.KeyPath, kp1)
	require.Equal(t, ss2.CertPath, cp2)
	require.Equal(t, ss2.KeyPath, kp2)
}

func TestTLSConfigManager_UseTLSFalse(t *testing.T) {
	t.Run("returns nil config and no error", func(t *testing.T) {
		manager, err := NewTLSConfigManager(false, nil, "/any/cert.pem", "/any/key.pem", false)
		require.NoError(t, err)
		require.NotNil(t, manager)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		tlsConfig := manager.TLSConfig()
		require.Nil(t, tlsConfig)
		require.False(t, manager.UseTLS())
	})

	t.Run("returns nil cert manager and no error", func(t *testing.T) {
		manager, err := NewTLSConfigManager(false, nil, "/any/cert.pem", "/any/key.pem", false)
		require.NoError(t, err)
		require.NotNil(t, manager)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		certLoader := manager.TLSCertLoader()
		require.Nil(t, certLoader)
	})

	t.Run("ignores invalid paths when disabled", func(t *testing.T) {
		// Even with nonexistent paths, useTLS=false should not error
		manager, err := NewTLSConfigManager(false, nil, "/nonexistent/cert.pem", "/nonexistent/key.pem", false)
		require.NoError(t, err)
		require.NotNil(t, manager)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		tlsConfig := manager.TLSConfig()
		require.Nil(t, tlsConfig)

		certLoader := manager.TLSCertLoader()
		require.Nil(t, certLoader)
	})

	t.Run("ignores base config when disabled", func(t *testing.T) {
		baseConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
			ServerName: "test.example.com",
		}

		manager, err := NewTLSConfigManager(false, baseConfig, "/any/cert.pem", "/any/key.pem", false)
		require.NoError(t, err)
		require.NotNil(t, manager)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		tlsConfig := manager.TLSConfig()
		require.Nil(t, tlsConfig, "should return nil config even with base config provided")
	})

	t.Run("close works when disabled", func(t *testing.T) {
		manager, err := NewTLSConfigManager(false, nil, "/any/cert.pem", "/any/key.pem", false)
		require.NoError(t, err)

		// Close should succeed
		require.NoError(t, manager.Close())
		require.NoError(t, manager.Close()) // idempotent
	})

	t.Run("explicit parameters override opts", func(t *testing.T) {
		manager, err := NewTLSConfigManager(false, nil, "", "", false, WithUseTLS(true))
		require.NoError(t, err)
		require.NotNil(t, manager)
		require.False(t, manager.UseTLS())
	})
}

func TestTLSConfigManager_UseTLSWithoutCert(t *testing.T) {
	t.Run("constructor succeeds with empty paths", func(t *testing.T) {
		manager, err := NewTLSConfigManager(true, nil, "", "", true)
		require.NoError(t, err)
		require.NotNil(t, manager)
		require.NoError(t, manager.Close())
	})

	t.Run("returns non-nil TLSConfig", func(t *testing.T) {
		manager, err := NewTLSConfigManager(true, nil, "", "", true)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		tlsConfig := manager.TLSConfig()
		require.NotNil(t, tlsConfig)
		require.True(t, manager.UseTLS())
	})

	t.Run("returns nil TLSCertLoader", func(t *testing.T) {
		manager, err := NewTLSConfigManager(true, nil, "", "", true)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		certLoader := manager.TLSCertLoader()
		require.Nil(t, certLoader)
	})

	t.Run("PrepareCertificateLoad returns ErrNoCertLoader", func(t *testing.T) {
		manager, err := NewTLSConfigManager(true, nil, "", "", true)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		callback, err := manager.PrepareCertificateLoad("/any/cert.pem", "/any/key.pem")
		require.ErrorIs(t, err, ErrNoCertLoader)
		require.Nil(t, callback)
	})

	t.Run("Listen fails without certificate", func(t *testing.T) {
		manager, err := NewTLSConfigManager(true, nil, "", "", true)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		_, err = manager.Listen("tcp", "127.0.0.1:0")
		require.ErrorContains(t, err, "tls: neither Certificates, GetCertificate, nor GetConfigForClient set in Config")
	})

	t.Run("respects base config", func(t *testing.T) {
		baseConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
			ServerName: "test.example.com",
		}

		manager, err := NewTLSConfigManager(true, baseConfig, "", "", false)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		tlsConfig := manager.TLSConfig()
		require.NotNil(t, tlsConfig)
		require.Equal(t, uint16(tls.VersionTLS12), tlsConfig.MinVersion)
		require.Equal(t, "test.example.com", tlsConfig.ServerName)
	})

	t.Run("close works without cert manager", func(t *testing.T) {
		manager, err := NewTLSConfigManager(true, nil, "", "", true)
		require.NoError(t, err)

		require.NoError(t, manager.Close())
		require.NoError(t, manager.Close()) // idempotent
	})
}

func TestTLSConfigManager_Close(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t)

	t.Run("close after construction", func(t *testing.T) {
		manager, err := NewTLSConfigManager(true, nil, ss.CertPath, ss.KeyPath, false)
		require.NoError(t, err)

		require.NoError(t, manager.Close())
	})

	t.Run("close is idempotent", func(t *testing.T) {
		manager, err := NewTLSConfigManager(true, nil, ss.CertPath, ss.KeyPath, false)
		require.NoError(t, err)

		require.NoError(t, manager.Close())
		require.NoError(t, manager.Close())
	})
}

func TestTLSConfigManager_PrepareCertificateLoad(t *testing.T) {
	t.Run("useTLS false returns NOP callback", func(t *testing.T) {
		manager, err := NewTLSConfigManager(false, nil, "/any/cert.pem", "/any/key.pem", false)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		// Should return a NOP callback even with invalid paths
		callback, err := manager.PrepareCertificateLoad("/nonexistent/cert.pem", "/nonexistent/key.pem")
		require.NoError(t, err)
		require.NotNil(t, callback)

		// Executing the NOP callback should succeed
		require.NoError(t, callback())
	})

	t.Run("useTLS true with valid paths returns callback", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		manager, err := NewTLSConfigManager(true, nil, ss.CertPath, ss.KeyPath, false)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		// Prepare loading the same cert (valid paths)
		callback, err := manager.PrepareCertificateLoad(ss.CertPath, ss.KeyPath)
		require.NoError(t, err)
		require.NotNil(t, callback)

		// Executing the callback should succeed
		require.NoError(t, callback())
	})

	t.Run("useTLS true with invalid paths returns error", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		manager, err := NewTLSConfigManager(true, nil, ss.CertPath, ss.KeyPath, false)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		// Prepare loading with nonexistent paths should fail
		callback, err := manager.PrepareCertificateLoad("/nonexistent/cert.pem", "/nonexistent/key.pem")
		require.ErrorContains(t, err, "LoadCertificate: error opening \"/nonexistent/cert.pem\" for reading: open /nonexistent/cert.pem: no such file or directory")
		require.Nil(t, callback)
	})

	t.Run("callback applies new certificate", func(t *testing.T) {
		ss1 := selfsigned.NewSelfSignedCert(t, selfsigned.WithDNSName("server1.example.com"))
		ss2 := selfsigned.NewSelfSignedCert(t, selfsigned.WithDNSName("server2.example.com"))

		manager, err := NewTLSConfigManager(true, nil, ss1.CertPath, ss1.KeyPath, false)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		// Verify initial certificate paths
		certLoader := manager.TLSCertLoader()
		certPath, keyPath := certLoader.Paths()
		require.Equal(t, ss1.CertPath, certPath)
		require.Equal(t, ss1.KeyPath, keyPath)

		// Prepare and execute loading a different certificate
		callback, err := manager.PrepareCertificateLoad(ss2.CertPath, ss2.KeyPath)
		require.NoError(t, err)
		require.NoError(t, callback())

		// Verify certificate paths have been updated
		certPath, keyPath = certLoader.Paths()
		require.Equal(t, ss2.CertPath, certPath)
		require.Equal(t, ss2.KeyPath, keyPath)
	})
}

func TestTLSConfigManager_Listen(t *testing.T) {
	testListenerConnection := func(t *testing.T, listener net.Listener, dial func(addr string) (net.Conn, error)) {
		t.Helper()

		testData := []byte("hello from client")

		// Server: accept connection and read data
		serverResult := make(chan error, 1)
		serverData := make(chan []byte, 1)
		go func() {
			conn, err := listener.Accept()
			if err != nil {
				serverResult <- err
				return
			}

			buf := make([]byte, len(testData))
			var n int
			n, err = conn.Read(buf)
			err = errors.Join(err, conn.Close())
			serverData <- buf[:n]
			serverResult <- err
		}()

		// Client: connect and send data
		conn, err := dial(listener.Addr().String())
		require.NoError(t, err)
		defer func() {
			require.NoError(t, conn.Close())
		}()
		_, err = conn.Write(testData)
		require.NoError(t, err)

		require.NoError(t, <-serverResult)
		require.Equal(t, testData, <-serverData)
	}

	t.Run("useTLS false returns plain TCP listener", func(t *testing.T) {
		manager, err := NewTLSConfigManager(false, nil, "/any/cert.pem", "/any/key.pem", false)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		listener, err := manager.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, listener.Close())
		}()

		testListenerConnection(t, listener, func(addr string) (net.Conn, error) {
			return net.Dial("tcp", addr)
		})
	})

	t.Run("useTLS true returns TLS listener", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		manager, err := NewTLSConfigManager(true, nil, ss.CertPath, ss.KeyPath, false)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		listener, err := manager.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, listener.Close())
		}()

		testListenerConnection(t, listener, func(addr string) (net.Conn, error) {
			return tls.Dial("tcp", addr, &tls.Config{
				InsecureSkipVerify: true,
			})
		})
	})

	t.Run("invalid address returns error", func(t *testing.T) {
		manager, err := NewTLSConfigManager(false, nil, "/any/cert.pem", "/any/key.pem", false)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		_, err = manager.Listen("tcp", "invalid:address:format")
		require.ErrorContains(t, err, "address invalid:address:format: too many colons in address")
	})
}

func TestTLSConfigManager_Dial(t *testing.T) {
	testDialConnection := func(t *testing.T, listener net.Listener, dial func(addr string) (net.Conn, error)) {
		t.Helper()

		testData := []byte("hello from client")

		// Server: accept connection and read data
		serverResult := make(chan error, 1)
		serverData := make(chan []byte, 1)
		go func() {
			conn, err := listener.Accept()
			if err != nil {
				serverResult <- err
				return
			}

			buf := make([]byte, len(testData))
			var n int
			n, err = conn.Read(buf)
			err = errors.Join(err, conn.Close())
			serverData <- buf[:n]
			serverResult <- err
		}()

		// Client: connect and send data
		conn, err := dial(listener.Addr().String())
		require.NoError(t, err)
		_, err = conn.Write(testData)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, conn.Close())
		}()

		require.NoError(t, <-serverResult)
		require.Equal(t, testData, <-serverData)
	}

	t.Run("useTLS false dials plain TCP", func(t *testing.T) {
		manager, err := NewTLSConfigManager(false, nil, "/any/cert.pem", "/any/key.pem", false)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		// Create plain TCP server
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, listener.Close())
		}()

		testDialConnection(t, listener, func(addr string) (net.Conn, error) {
			return manager.Dial("tcp", addr)
		})
	})

	t.Run("useTLS true dials TLS", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		manager, err := NewTLSConfigManager(true, nil, ss.CertPath, ss.KeyPath, true)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		// Create TLS server
		cert, err := tls.LoadX509KeyPair(ss.CertPath, ss.KeyPath)
		require.NoError(t, err)
		listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
			Certificates: []tls.Certificate{cert},
		})
		require.NoError(t, err)
		defer func() {
			require.NoError(t, listener.Close())
		}()

		testDialConnection(t, listener, func(addr string) (net.Conn, error) {
			return manager.Dial("tcp", addr)
		})
	})

	t.Run("useTLS true without cert dials TLS", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		// Create manager without client cert (client-only mode)
		manager, err := NewTLSConfigManager(true, nil, "", "", true)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		// Create TLS server
		cert, err := tls.LoadX509KeyPair(ss.CertPath, ss.KeyPath)
		require.NoError(t, err)
		listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
			Certificates: []tls.Certificate{cert},
		})
		require.NoError(t, err)
		defer func() {
			require.NoError(t, listener.Close())
		}()

		testDialConnection(t, listener, func(addr string) (net.Conn, error) {
			return manager.Dial("tcp", addr)
		})
	})

	t.Run("invalid address returns error", func(t *testing.T) {
		manager, err := NewTLSConfigManager(false, nil, "/any/cert.pem", "/any/key.pem", false)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		_, err = manager.Dial("tcp", "invalid:address:format")
		require.ErrorContains(t, err, "address invalid:address:format: too many colons in address")
	})
}

func TestNewDisabledTLSConfigManager(t *testing.T) {
	// NewDisabledTLSConfigManager should be equivalent to NewTLSConfigManager(false, nil, "", "", false).
	// The functional behavior of a disabled manager is already tested in TestTLSConfigManager_UseTLSFalse,
	// so we just verify the two constructors produce equivalent managers.
	disabled := NewDisabledTLSConfigManager()
	require.NotNil(t, disabled)
	explicit, err := NewTLSConfigManager(false, nil, "", "", false)
	require.NoError(t, err)
	require.NotNil(t, explicit)

	require.Equal(t, explicit.TLSConfig(), disabled.TLSConfig())
	require.Equal(t, explicit.TLSCertLoader(), disabled.TLSCertLoader())
	require.False(t, disabled.UseTLS())

	require.NoError(t, disabled.Close())
	require.NoError(t, explicit.Close())
}

func TestNewClientTLSConfigManager(t *testing.T) {
	// NewClientTLSConfigManager should be equivalent to NewTLSConfigManager(useTLS, baseConfig, "", "", allowInsecure).
	// The functional behavior is already tested in other tests (TestTLSConfigManager_UseTLSWithoutCert, etc.),
	// so we just verify the two constructors produce equivalent managers for various parameter combinations.

	t.Run("TLS disabled", func(t *testing.T) {
		client, err := NewClientTLSConfigManager(false, nil, false)
		require.NoError(t, err)
		require.NotNil(t, client)
		explicit, err := NewTLSConfigManager(false, nil, "", "", false)
		require.NoError(t, err)
		require.NotNil(t, explicit)

		require.Equal(t, explicit.TLSConfig(), client.TLSConfig())
		require.Equal(t, explicit.TLSCertLoader(), client.TLSCertLoader())
		require.False(t, client.UseTLS())

		require.NoError(t, client.Close())
		require.NoError(t, explicit.Close())
	})

	t.Run("TLS enabled allowInsecure false", func(t *testing.T) {
		client, err := NewClientTLSConfigManager(true, nil, false)
		require.NoError(t, err)
		require.NotNil(t, client)
		explicit, err := NewTLSConfigManager(true, nil, "", "", false)
		require.NoError(t, err)
		require.NotNil(t, explicit)

		require.Equal(t, explicit.TLSConfig().InsecureSkipVerify, client.TLSConfig().InsecureSkipVerify)
		require.Equal(t, explicit.TLSCertLoader(), client.TLSCertLoader())
		require.True(t, client.UseTLS())

		require.NoError(t, client.Close())
		require.NoError(t, explicit.Close())
	})

	t.Run("TLS enabled allowInsecure true", func(t *testing.T) {
		client, err := NewClientTLSConfigManager(true, nil, true)
		require.NoError(t, err)
		require.NotNil(t, client)
		explicit, err := NewTLSConfigManager(true, nil, "", "", true)
		require.NoError(t, err)
		require.NotNil(t, explicit)

		require.Equal(t, explicit.TLSConfig().InsecureSkipVerify, client.TLSConfig().InsecureSkipVerify)
		require.Equal(t, explicit.TLSCertLoader(), client.TLSCertLoader())

		require.NoError(t, client.Close())
		require.NoError(t, explicit.Close())
	})

	t.Run("with base config", func(t *testing.T) {
		baseConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
			MaxVersion: tls.VersionTLS13,
			ServerName: "test.example.com",
		}

		client, err := NewClientTLSConfigManager(true, baseConfig, false)
		require.NoError(t, err)
		require.NotNil(t, client)
		explicit, err := NewTLSConfigManager(true, baseConfig, "", "", false)
		require.NoError(t, err)
		require.NotNil(t, explicit)

		// Verify base config values are preserved in both
		require.Equal(t, explicit.TLSConfig().MinVersion, client.TLSConfig().MinVersion)
		require.Equal(t, explicit.TLSConfig().MaxVersion, client.TLSConfig().MaxVersion)
		require.Equal(t, explicit.TLSConfig().ServerName, client.TLSConfig().ServerName)
		require.Equal(t, explicit.TLSConfig().InsecureSkipVerify, client.TLSConfig().InsecureSkipVerify)
		require.Equal(t, explicit.TLSCertLoader(), client.TLSCertLoader())

		require.NoError(t, client.Close())
		require.NoError(t, explicit.Close())
	})

	t.Run("base config is cloned", func(t *testing.T) {
		baseConfig := &tls.Config{
			ServerName: "original.example.com",
		}

		client, err := NewClientTLSConfigManager(true, baseConfig, false)
		require.NoError(t, err)
		require.NotNil(t, client)

		// Verify config is cloned (not same instance)
		require.NotSame(t, baseConfig, client.TLSConfig())

		// Verify modifying base config doesn't affect client config
		baseConfig.ServerName = "modified.example.com"
		require.Equal(t, "original.example.com", client.TLSConfig().ServerName)

		require.NoError(t, client.Close())
	})

	t.Run("explicit parameters override opts", func(t *testing.T) {
		client, err := NewClientTLSConfigManager(false, nil, false, WithUseTLS(true))
		require.NoError(t, err)
		require.NotNil(t, client)
		require.False(t, client.UseTLS())
	})
}

func simpleEchoServer(serverDone chan error, listener net.Listener, bufSize int) (rErr error) {
	defer func() {
		serverDone <- rErr
	}()
	conn, err := listener.Accept()
	if err != nil {
		return fmt.Errorf("error in Accept: %w", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			rErr = errors.Join(rErr, fmt.Errorf("error in Close: %w", err))
		}
	}()
	buf := make([]byte, bufSize)
	n, err := conn.Read(buf)
	if err != nil {
		return fmt.Errorf("error in Read: %w", err)
	}
	if _, err = conn.Write(buf[:n]); err != nil {
		return fmt.Errorf("error in Write: %w", err)
	}
	return nil
}

func TestTLSConfigManager_WithRootCAFiles(t *testing.T) {
	t.Run("sets RootCAs from file", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		// Build expected CertPool
		expectedPool := x509.NewCertPool()
		caCert, err := os.ReadFile(ss.CACertPath)
		require.NoError(t, err)
		require.True(t, expectedPool.AppendCertsFromPEM(caCert))

		manager, err := NewTLSConfigManager(true, nil, ss.CertPath, ss.KeyPath, false,
			WithRootCAFiles(ss.CACertPath))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		tlsConfig := manager.TLSConfig()
		require.NotNil(t, tlsConfig)
		require.NotNil(t, tlsConfig.RootCAs)
		require.True(t, tlsConfig.RootCAs.Equal(expectedPool), "RootCAs should match expected pool")
	})

	t.Run("client trusts server with CA", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		// Server manager with certificate
		serverManager, err := NewTLSConfigManager(true, nil, ss.CertPath, ss.KeyPath, false)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, serverManager.Close())
		}()

		// Client manager trusting the CA that signed the server certificate
		clientManager, err := NewClientTLSConfigManager(true, nil, false,
			WithRootCAFiles(ss.CACertPath))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, clientManager.Close())
		}()

		// Start server
		listener, err := serverManager.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, listener.Close())
		}()

		// Server accepts and echoes
		testData := []byte("hello")
		serverDone := make(chan error, 1)
		go simpleEchoServer(serverDone, listener, len(testData))

		// Client connects with CA-trusted connection
		conn, err := clientManager.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer func() {
			require.NoError(t, conn.Close())
		}()

		n, err := conn.Write(testData)
		require.NoError(t, err)
		require.Equal(t, 5, n)

		buf := make([]byte, len(testData))
		n, err = conn.Read(buf)
		require.NoError(t, err)
		require.Equal(t, len(testData), n)
		require.Equal(t, testData, buf)

		require.NoError(t, <-serverDone)
	})

	t.Run("client rejects server without CA", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		// Server manager
		serverManager, err := NewTLSConfigManager(true, nil, ss.CertPath, ss.KeyPath, false)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, serverManager.Close())
		}()

		// Client manager with empty RootCA pool (explicitly set, no system CAs)
		clientManager, err := NewClientTLSConfigManager(true, nil, false,
			WithRootCAIncludeSystem(false))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, clientManager.Close())
		}()

		// Start server
		listener, err := serverManager.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, listener.Close())
		}()

		// Server accepts and waits for handshake to complete (will fail due to client rejecting cert)
		testData := []byte("test")
		serverDone := make(chan error, 1)
		go simpleEchoServer(serverDone, listener, len(testData))

		// Client connection should fail certificate verification
		conn, err := clientManager.Dial("tcp", listener.Addr().String())
		require.ErrorContains(t, err, "tls: failed to verify certificate: x509: certificate signed by unknown authority")
		require.Nil(t, conn)

		// Server will see a handshake failure error (client sent bad_certificate alert)
		serverErr := <-serverDone
		require.ErrorContains(t, serverErr, "tls: bad certificate")
	})

	t.Run("multiple CA files", func(t *testing.T) {
		ss1 := selfsigned.NewSelfSignedCert(t, selfsigned.WithCASubject("org1", "CA1"))
		ss2 := selfsigned.NewSelfSignedCert(t, selfsigned.WithCASubject("org2", "CA2"))

		// Build expected CertPool with both CAs
		expectedPool := x509.NewCertPool()
		caCert1, err := os.ReadFile(ss1.CACertPath)
		require.NoError(t, err)
		require.True(t, expectedPool.AppendCertsFromPEM(caCert1))
		caCert2, err := os.ReadFile(ss2.CACertPath)
		require.NoError(t, err)
		require.True(t, expectedPool.AppendCertsFromPEM(caCert2))

		manager, err := NewClientTLSConfigManager(true, nil, false,
			WithRootCAFiles(ss1.CACertPath, ss2.CACertPath))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		tlsConfig := manager.TLSConfig()
		require.NotNil(t, tlsConfig)
		require.NotNil(t, tlsConfig.RootCAs)
		require.True(t, tlsConfig.RootCAs.Equal(expectedPool), "RootCAs should contain both CAs")
	})

	t.Run("error on nonexistent file", func(t *testing.T) {
		_, err := NewClientTLSConfigManager(true, nil, false,
			WithRootCAFiles("/nonexistent/ca.pem"))
		require.ErrorIs(t, err, os.ErrNotExist)
		require.ErrorContains(t, err, "error creating root CA pool: error reading file \"/nonexistent/ca.pem\" for CA store: open /nonexistent/ca.pem: no such file or directory")
	})

	t.Run("error on invalid PEM file", func(t *testing.T) {
		// Create a temporary file with invalid PEM content
		tmpDir := t.TempDir()
		tmpFile := path.Join(tmpDir, "invalid.pem")
		require.NoError(t, os.WriteFile(tmpFile, []byte("not a valid PEM file"), 0644))

		manager, err := NewClientTLSConfigManager(true, nil, false,
			WithRootCAFiles(tmpFile))
		require.ErrorContains(t, err, "error creating root CA pool: error adding certificates from \""+tmpFile+"\" to CA store: no valid certificates found")
		require.Nil(t, manager)
	})
}

func TestTLSConfigManager_WithRootCAIncludeSystem(t *testing.T) {
	t.Run("includes system CA pool", func(t *testing.T) {
		// Get expected system pool
		expectedPool, err := x509.SystemCertPool()
		require.NoError(t, err)

		manager, err := NewClientTLSConfigManager(true, nil, false,
			WithRootCAIncludeSystem(true))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		tlsConfig := manager.TLSConfig()
		require.NotNil(t, tlsConfig)
		require.NotNil(t, tlsConfig.RootCAs)
		require.True(t, tlsConfig.RootCAs.Equal(expectedPool), "RootCAs should equal system pool")
	})

	t.Run("excludes system CA pool", func(t *testing.T) {
		// Expected: empty pool
		expectedPool := x509.NewCertPool()

		manager, err := NewClientTLSConfigManager(true, nil, false,
			WithRootCAIncludeSystem(false))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		tlsConfig := manager.TLSConfig()
		require.NotNil(t, tlsConfig)
		require.NotNil(t, tlsConfig.RootCAs)
		require.True(t, tlsConfig.RootCAs.Equal(expectedPool), "RootCAs should be empty pool")
	})

	t.Run("combined with CA files", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		// Build expected pool: system + custom CA
		expectedPool, err := x509.SystemCertPool()
		require.NoError(t, err)
		caCert, err := os.ReadFile(ss.CACertPath)
		require.NoError(t, err)
		require.True(t, expectedPool.AppendCertsFromPEM(caCert))

		manager, err := NewClientTLSConfigManager(true, nil, false,
			WithRootCAIncludeSystem(true),
			WithRootCAFiles(ss.CACertPath))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		tlsConfig := manager.TLSConfig()
		require.NotNil(t, tlsConfig)
		require.NotNil(t, tlsConfig.RootCAs)
		require.True(t, tlsConfig.RootCAs.Equal(expectedPool), "RootCAs should contain system + custom CA")
	})
}

func TestTLSConfigManager_WithClientCAFiles(t *testing.T) {
	t.Run("sets ClientCAs from file", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		// Build expected CertPool
		expectedPool := x509.NewCertPool()
		caCert, err := os.ReadFile(ss.CACertPath)
		require.NoError(t, err)
		require.True(t, expectedPool.AppendCertsFromPEM(caCert))

		manager, err := NewTLSConfigManager(true, nil, ss.CertPath, ss.KeyPath, false,
			WithClientCAFiles(ss.CACertPath))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		tlsConfig := manager.TLSConfig()
		require.NotNil(t, tlsConfig)
		require.NotNil(t, tlsConfig.ClientCAs)
		require.True(t, tlsConfig.ClientCAs.Equal(expectedPool), "ClientCAs should match expected pool")
	})

	t.Run("server verifies client certificate", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		// Server manager that requires client certificates.
		serverManager, err := NewTLSConfigManager(true, nil, ss.CertPath, ss.KeyPath, false,
			WithClientCAFiles(ss.CACertPath),
			WithClientAuth(tls.RequireAndVerifyClientCert))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, serverManager.Close())
		}()

		// Start server
		listener, err := serverManager.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, listener.Close())
		}()

		// Server accepts and echoes
		testData := []byte("hello")
		serverDone := make(chan error, 1)
		go simpleEchoServer(serverDone, listener, len(testData))

		// Client config with client certificate. Ignore certificate validity from server, we're testing
		// client certificate functionality here.
		clientManager, err := NewTLSConfigManager(true, nil, ss.CertPath, ss.KeyPath, true)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, clientManager.Close())
		}()
		conn, err := clientManager.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer func() {
			require.NoError(t, conn.Close())
		}()

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

	t.Run("server rejects client without valid certificate", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)
		otherSS := selfsigned.NewSelfSignedCert(t, selfsigned.WithCASubject("other", "Other CA"))

		// Server manager that requires client certificates
		serverManager, err := NewTLSConfigManager(true, nil, ss.CertPath, ss.KeyPath, false,
			WithClientAuth(tls.RequireAndVerifyClientCert),
			WithClientCAFiles(ss.CACertPath))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, serverManager.Close())
		}()
		serverTLSConfig := serverManager.TLSConfig()
		require.NotNil(t, serverTLSConfig)
		require.Equal(t, tls.RequireAndVerifyClientCert, serverTLSConfig.ClientAuth)

		// Client with certificate signed by different CA. Ignore server certificate validity. We're
		// checking client certificate functionality here.
		clientManager, err := NewTLSConfigManager(true, nil, otherSS.CertPath, otherSS.KeyPath, true)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, clientManager.Close())
		}()

		// Start server
		listener, err := serverManager.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, listener.Close())
		}()

		// Server accepts and waits for handshake (will fail due to untrusted client cert)
		testData := []byte("hello")
		serverDone := make(chan error, 1)
		go simpleEchoServer(serverDone, listener, len(testData))

		// Client connects - Dial may succeed but handshake fails during Read
		conn, dialErr := clientManager.Dial("tcp", listener.Addr().String())
		if dialErr == nil {
			defer func() {
				require.NoError(t, conn.Close())
			}()
			// Read to wait for server response - this will fail when server rejects our cert
			buf := make([]byte, 1)
			_, dialErr = conn.Read(buf)
		}
		// Client sees remote error when server requires cert but client sends untrusted cert.
		// (TLS client doesn't send cert if it's not signed by a CA the server trusts)
		require.ErrorContains(t, dialErr, "remote error: tls: certificate required")

		// Server sees that client didn't provide a certificate (because client's cert
		// wasn't signed by a CA in server's ClientCAs, so TLS stack didn't send it)
		serverErr := <-serverDone
		require.ErrorContains(t, serverErr, "tls: client didn't provide a certificate")
	})

	t.Run("multiple CA files", func(t *testing.T) {
		ss1 := selfsigned.NewSelfSignedCert(t, selfsigned.WithCASubject("org1", "CA1"))
		ss2 := selfsigned.NewSelfSignedCert(t, selfsigned.WithCASubject("org2", "CA2"))

		// Build expected CertPool with both CAs
		expectedPool := x509.NewCertPool()
		caCert1, err := os.ReadFile(ss1.CACertPath)
		require.NoError(t, err)
		require.True(t, expectedPool.AppendCertsFromPEM(caCert1))
		caCert2, err := os.ReadFile(ss2.CACertPath)
		require.NoError(t, err)
		require.True(t, expectedPool.AppendCertsFromPEM(caCert2))

		manager, err := NewTLSConfigManager(true, nil, ss1.CertPath, ss1.KeyPath, false,
			WithClientCAFiles(ss1.CACertPath, ss2.CACertPath))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		tlsConfig := manager.TLSConfig()
		require.NotNil(t, tlsConfig)
		require.NotNil(t, tlsConfig.ClientCAs)
		require.True(t, tlsConfig.ClientCAs.Equal(expectedPool), "ClientCAs should contain both CAs")
	})

	t.Run("error on nonexistent file", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		_, err := NewTLSConfigManager(true, nil, ss.CertPath, ss.KeyPath, false,
			WithClientCAFiles("/nonexistent/ca.pem"))
		require.ErrorIs(t, err, os.ErrNotExist)
		require.ErrorContains(t, err, "error creating client CA pool: error reading file \"/nonexistent/ca.pem\" for CA store: open /nonexistent/ca.pem: no such file or directory")
	})

	t.Run("error on invalid PEM file", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		// Create a temporary file with invalid PEM content
		tmpDir := t.TempDir()
		tmpFile := path.Join(tmpDir, "invalid.pem")
		require.NoError(t, os.WriteFile(tmpFile, []byte("not a valid PEM file"), 0644))

		manager, err := NewTLSConfigManager(true, nil, ss.CertPath, ss.KeyPath, false,
			WithClientCAFiles(tmpFile))
		require.ErrorContains(t, err, "error creating client CA pool: error adding certificates from \""+tmpFile+"\" to CA store: no valid certificates found")
		require.Nil(t, manager)
	})
}

func TestTLSConfigManager_WithClientCAIncludeSystem(t *testing.T) {
	t.Run("includes system CA pool", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		// Get expected system pool
		expectedPool, err := x509.SystemCertPool()
		require.NoError(t, err)

		manager, err := NewTLSConfigManager(true, nil, ss.CertPath, ss.KeyPath, false,
			WithClientCAIncludeSystem(true))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		tlsConfig := manager.TLSConfig()
		require.NotNil(t, tlsConfig)
		require.NotNil(t, tlsConfig.ClientCAs)
		require.True(t, tlsConfig.ClientCAs.Equal(expectedPool), "ClientCAs should equal system pool")
	})

	t.Run("excludes system CA pool", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		// Expected: empty pool
		expectedPool := x509.NewCertPool()

		manager, err := NewTLSConfigManager(true, nil, ss.CertPath, ss.KeyPath, false,
			WithClientCAIncludeSystem(false))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		tlsConfig := manager.TLSConfig()
		require.NotNil(t, tlsConfig)
		require.NotNil(t, tlsConfig.ClientCAs)
		require.True(t, tlsConfig.ClientCAs.Equal(expectedPool), "ClientCAs should be empty pool")
	})

	t.Run("combined with CA files", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		// Build expected pool: system + custom CA
		expectedPool, err := x509.SystemCertPool()
		require.NoError(t, err)
		caCert, err := os.ReadFile(ss.CACertPath)
		require.NoError(t, err)
		require.True(t, expectedPool.AppendCertsFromPEM(caCert))

		manager, err := NewTLSConfigManager(true, nil, ss.CertPath, ss.KeyPath, false,
			WithClientCAIncludeSystem(true),
			WithClientCAFiles(ss.CACertPath))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		tlsConfig := manager.TLSConfig()
		require.NotNil(t, tlsConfig)
		require.NotNil(t, tlsConfig.ClientCAs)
		require.True(t, tlsConfig.ClientCAs.Equal(expectedPool), "ClientCAs should contain system + custom CA")
	})
}

func TestTLSConfigManager_CAOptionsNotSetByDefault(t *testing.T) {
	// When no CA options are specified, the TLS config should not have
	// custom CA pools set (allowing Go's default behavior)
	ss := selfsigned.NewSelfSignedCert(t)

	manager, err := NewTLSConfigManager(true, nil, ss.CertPath, ss.KeyPath, false)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, manager.Close())
	}()

	tlsConfig := manager.TLSConfig()
	require.NotNil(t, tlsConfig)
	require.Nil(t, tlsConfig.RootCAs, "RootCAs should be nil when no CA options specified")
	require.Nil(t, tlsConfig.ClientCAs, "ClientCAs should be nil when no CA options specified")
}

func TestTLSConfigManager_CAOptionsWithBaseConfig(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t)
	anotherSS := selfsigned.NewSelfSignedCert(t, selfsigned.WithCASubject("another", "Another CA"))

	// Create a base config with existing RootCAs
	basePool := x509.NewCertPool()
	baseCACert, err := os.ReadFile(ss.CACertPath)
	require.NoError(t, err)
	require.True(t, basePool.AppendCertsFromPEM(baseCACert))

	baseConfig := &tls.Config{
		RootCAs: basePool,
	}

	// Build expected pool from anotherSS CA only
	expectedPool := x509.NewCertPool()
	anotherCACert, err := os.ReadFile(anotherSS.CACertPath)
	require.NoError(t, err)
	require.True(t, expectedPool.AppendCertsFromPEM(anotherCACert))

	// Create manager with different CA file - should override base config
	manager, err := NewClientTLSConfigManager(true, baseConfig, false,
		WithRootCAFiles(anotherSS.CACertPath))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, manager.Close())
	}()

	tlsConfig := manager.TLSConfig()
	require.NotNil(t, tlsConfig)
	require.NotNil(t, tlsConfig.RootCAs)
	// The RootCAs should match the new CA, not the base config
	require.True(t, tlsConfig.RootCAs.Equal(expectedPool), "RootCAs should be overridden by WithRootCAFiles")
	require.False(t, tlsConfig.RootCAs.Equal(basePool), "RootCAs should not equal base pool")
}

// testManagerCheckTime is the TLS certificate check time in logging tests.
const testManagerCheckTime = 333 * time.Millisecond

// testManagerCheckCapture time is how long to capture logs during logging tests. To prevent flaky tests,
// it should be more than testManagerCheckTime, but less than 2 * testManagerCheckTime.
const testManagerCheckCapture = 500 * time.Millisecond

func TestTLSConfigManager_WithCertLoaderOptions(t *testing.T) {
	// This test verifies that WithLogger, WithCertificateCheckInterval, and WithExpirationAdvanced
	// properly pass through to the underlying TLSCertLoader.

	// Create a certificate that expires in 24 hours
	notBefore := time.Now().UTC().Truncate(time.Minute).Add(-7 * 24 * time.Hour)
	notAfter := time.Now().UTC().Truncate(time.Hour).Add(24 * time.Hour)

	ss := selfsigned.NewSelfSignedCert(t, selfsigned.WithNotBefore(notBefore), selfsigned.WithNotAfter(notAfter))

	core, logs := observer.New(zapcore.InfoLevel)
	logger := zap.New(core)

	// Create TLSConfigManager with all three options:
	// - WithLogger: to capture log output
	// - WithCertificateCheckInterval: to set a short check interval
	// - WithExpirationAdvanced: to set the expiration warning window to 2 days (> 24 hours remaining)
	manager, err := NewTLSConfigManager(true, nil, ss.CertPath, ss.KeyPath, false,
		WithLogger(logger),
		WithCertificateCheckInterval(testManagerCheckTime),
		WithExpirationAdvanced(2*24*time.Hour))
	require.NoError(t, err)
	require.NotNil(t, manager)
	defer func() {
		require.NoError(t, manager.Close())
	}()

	// Wait for the certificate monitor to start
	certLoader := manager.TLSCertLoader()
	require.NotNil(t, certLoader)
	certLoader.WaitForMonitorStart()

	// Verify the "Certificate will expire soon" warning is logged
	checkWarning := func(t *testing.T) {
		t.Helper()
		warning := logs.FilterMessage("Certificate will expire soon").TakeAll()
		require.Len(t, warning, 1)
		require.Equal(t, zap.WarnLevel, warning[0].Level)
		require.Equal(t, ss.CertPath, warning[0].ContextMap()["cert"])
		require.Equal(t, ss.KeyPath, warning[0].ContextMap()["key"])
		require.Equal(t, notAfter, warning[0].ContextMap()["NotAfter"])
		untilExpires, ok := warning[0].ContextMap()["untilExpires"].(time.Duration)
		require.True(t, ok, "untilExpires should be a time.Duration")
		timeExpires := time.Now().Add(untilExpires)
		require.WithinDuration(t, notAfter, timeExpires, 2*time.Minute, "untilExpires varies more than expected")
		logs.TakeAll() // dump all logs
	}
	checkWarning(t)

	// Check for warning during monitor (after another check interval)
	require.Zero(t, logs.Len(), "init logs not dumped properly")
	time.Sleep(testManagerCheckCapture)
	checkWarning(t)
}

func TestTLSConfigManager_WithIgnoreFilePermissions(t *testing.T) {
	t.Run("fails without ignore when cert permissions too open", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		require.NoError(t, os.Chmod(ss.CertPath, 0660))
		_, err := NewTLSConfigManager(true, nil, ss.CertPath, ss.KeyPath, false)
		require.ErrorContains(t, err, fmt.Sprintf("LoadCertificate: file permissions are too open: for %q, maximum is 0644 (-rw-r--r--) but found 0660 (-rw-rw----); extra permissions: 0020 (-----w----)", ss.CertPath))
	})

	t.Run("succeeds with ignore when cert permissions too open", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		require.NoError(t, os.Chmod(ss.CertPath, 0660))
		manager, err := NewTLSConfigManager(true, nil, ss.CertPath, ss.KeyPath, false,
			WithIgnoreFilePermissions(true))
		require.NoError(t, err)
		require.NotNil(t, manager)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		// Verify the manager works correctly
		tlsConfig := manager.TLSConfig()
		require.NotNil(t, tlsConfig)
		certLoader := manager.TLSCertLoader()
		require.NotNil(t, certLoader)
	})

	t.Run("fails without ignore when key permissions too open", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		require.NoError(t, os.Chmod(ss.KeyPath, 0644))
		_, err := NewTLSConfigManager(true, nil, ss.CertPath, ss.KeyPath, false)
		require.ErrorContains(t, err, fmt.Sprintf("LoadCertificate: file permissions are too open: for %q, maximum is 0600 (-rw-------) but found 0644 (-rw-r--r--); extra permissions: 0044 (----r--r--)", ss.KeyPath))
	})

	t.Run("succeeds with ignore when key permissions too open", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		require.NoError(t, os.Chmod(ss.KeyPath, 0644))
		manager, err := NewTLSConfigManager(true, nil, ss.CertPath, ss.KeyPath, false,
			WithIgnoreFilePermissions(true))
		require.NoError(t, err)
		require.NotNil(t, manager)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		// Verify the manager works correctly
		tlsConfig := manager.TLSConfig()
		require.NotNil(t, tlsConfig)
		certLoader := manager.TLSCertLoader()
		require.NotNil(t, certLoader)
	})
}

func TestTLSConfigManager_DialWithDialer(t *testing.T) {
	testDialWithDialerConnection := func(t *testing.T, listener net.Listener, dial func(dialer *net.Dialer, addr string) (net.Conn, error)) {
		t.Helper()

		testData := []byte("hello from client")

		// Server: accept connection and read data
		serverResult := make(chan error, 1)
		serverData := make(chan []byte, 1)
		go func() {
			conn, err := listener.Accept()
			if err != nil {
				serverResult <- err
				return
			}

			buf := make([]byte, len(testData))
			var n int
			n, err = conn.Read(buf)
			err = errors.Join(err, conn.Close())
			serverData <- buf[:n]
			serverResult <- err
		}()

		// Bind to a specific local address to verify the dialer is used
		localAddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		dialer := &net.Dialer{LocalAddr: localAddr}

		// Client: connect and send data
		conn, err := dial(dialer, listener.Addr().String())
		require.NoError(t, err)

		// Verify the connection's local address is from 127.0.0.1 (dialer's LocalAddr)
		localTCPAddr, ok := conn.LocalAddr().(*net.TCPAddr)
		require.True(t, ok)
		require.Equal(t, "127.0.0.1", localTCPAddr.IP.String())

		_, err = conn.Write(testData)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, conn.Close())
		}()

		require.NoError(t, <-serverResult)
		require.Equal(t, testData, <-serverData)
	}

	t.Run("dialer LocalAddr is used for plain TCP", func(t *testing.T) {
		manager, err := NewTLSConfigManager(false, nil, "/any/cert.pem", "/any/key.pem", false)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		// Create plain TCP server
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, listener.Close())
		}()

		testDialWithDialerConnection(t, listener, func(dialer *net.Dialer, addr string) (net.Conn, error) {
			return manager.DialWithDialer(dialer, "tcp", addr)
		})
	})

	t.Run("dialer LocalAddr is used for TLS", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		manager, err := NewTLSConfigManager(true, nil, "", "", true)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		// Create TLS server
		cert, err := tls.LoadX509KeyPair(ss.CertPath, ss.KeyPath)
		require.NoError(t, err)
		listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
			Certificates: []tls.Certificate{cert},
		})
		require.NoError(t, err)
		defer func() {
			require.NoError(t, listener.Close())
		}()

		testDialWithDialerConnection(t, listener, func(dialer *net.Dialer, addr string) (net.Conn, error) {
			return manager.DialWithDialer(dialer, "tcp", addr)
		})
	})
}
