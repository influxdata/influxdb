package tlsconfig

import (
	"crypto/tls"
	"errors"
	"net"
	"testing"

	"github.com/influxdata/influxdb/pkg/testing/selfsigned"
	"github.com/stretchr/testify/require"
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
