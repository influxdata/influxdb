package tlsconfig

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"path"
	"testing"
	"time"

	th "github.com/influxdata/influxdb/pkg/testing/helper"
	"github.com/influxdata/influxdb/pkg/testing/selfsigned"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestTLSConfigManager_ConsistentClonedConfigs(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t)

	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	// Create an initial baseConfig for manager.
	baseTLSConfig := ss.ClientTLSConfig(t, false, false)
	require.NotNil(t, baseTLSConfig.RootCAs, "ClientTLSConfig should have set RootCAs")

	manager, err := NewServerTLSConfigManager(
		monitor,
		WithUseTLS(true),
		WithBaseConfig(baseTLSConfig),
		WithServerCertificate(ss.CertPath, ss.KeyPath))
	require.NoError(t, err)
	require.NotNil(t, manager)
	defer th.CheckedClose(t, manager)()

	// Get TLS config
	tlsConfig := manager.TLSConfig()
	require.NotNil(t, tlsConfig)

	// The manager's TLS config and baseTLSConfig should not be the same and and should not be shared,
	// but we should be able to set that the config was cloned from baseTLSConfig by looking at RootCAs.
	require.NotSame(t, tlsConfig, baseTLSConfig)
	require.NotEqual(t, tlsConfig, baseTLSConfig)
	require.NotNil(t, tlsConfig.RootCAs)
	require.Equal(t, baseTLSConfig.RootCAs, tlsConfig.RootCAs)

	// Subsequent calls should return different instances that are equal.
	tlsConfig2 := manager.TLSConfig()
	require.NotSame(t, tlsConfig, tlsConfig2)
	// We can't compare the function pointers directly, just that they are non-nil.
	// Clear out the function pointers before calling require.Equal.
	require.NotNil(t, tlsConfig.GetCertificate)
	require.NotNil(t, tlsConfig2.GetCertificate)
	require.Nil(t, tlsConfig.GetClientCertificate)
	require.Nil(t, tlsConfig2.GetClientCertificate)
	tlsConfig.GetCertificate = nil
	tlsConfig.GetClientCertificate = nil
	tlsConfig2.GetCertificate = nil
	tlsConfig2.GetClientCertificate = nil
	require.Equal(t, tlsConfig, tlsConfig2)
}

func TestTLSConfigManager_BaseConfigCloned(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t)

	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	baseConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		MaxVersion: tls.VersionTLS13,
		ServerName: "test.example.com",
	}

	manager, err := NewServerTLSConfigManager(
		monitor,
		WithUseTLS(true),
		WithBaseConfig(baseConfig),
		WithServerCertificate(ss.CertPath, ss.KeyPath))
	require.NoError(t, err)
	require.NotNil(t, manager)
	defer th.CheckedClose(t, manager)()

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

	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	manager, err := NewServerTLSConfigManager(
		monitor,
		WithUseTLS(true),
		WithBaseConfig(nil),
		WithServerCertificate(ss.CertPath, ss.KeyPath))
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

	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	manager, err := NewServerTLSConfigManager(
		monitor,
		WithUseTLS(true),
		WithBaseConfig(nil),
		WithServerCertificate(ss.CertPath, ss.KeyPath))
	require.NoError(t, err)
	require.NotNil(t, manager)
	defer th.CheckedClose(t, manager)()

	tlsConfig := manager.TLSConfig()
	require.NotNil(t, tlsConfig)

	// Verify that TLSCertLoader.SetupTLSConfig was called by checking callbacks are set
	require.NotNil(t, tlsConfig.GetCertificate, "GetCertificate callback should be set")
	require.Nil(t, tlsConfig.GetClientCertificate, "GetClientCertificate callback should not be set")
}

func TestTLSConfigManager_ConstructorError(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	// Use non-existent paths to verify NewTLSConfigLoader returns error
	manager, err := NewServerTLSConfigManager(
		monitor,
		WithUseTLS(true),
		WithBaseConfig(nil),
		WithServerCertificate("/nonexistent/cert.pem", "/nonexistent/key.pem"))
	require.ErrorContains(t, err, "LoadCertificate: error opening \"/nonexistent/cert.pem\" for reading: open /nonexistent/cert.pem: no such file or directory")
	require.Nil(t, manager)
}

func TestTLSConfigManager_InsecureSkipVerify(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t)

	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	t.Run("allowInsecure true", func(t *testing.T) {
		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithBaseConfig(nil),
			WithServerCertificate(ss.CertPath, ss.KeyPath),
			WithAllowInsecure(true))
		require.NoError(t, err)
		th.CheckedClose(t, manager)()

		tlsConfig := manager.TLSConfig()
		require.True(t, tlsConfig.InsecureSkipVerify)
	})

	t.Run("allowInsecure false", func(t *testing.T) {
		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithBaseConfig(nil),
			WithServerCertificate(ss.CertPath, ss.KeyPath),
			WithAllowInsecure(false))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		tlsConfig := manager.TLSConfig()
		require.False(t, tlsConfig.InsecureSkipVerify)
	})

	t.Run("allowInsecure implied false", func(t *testing.T) {
		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithBaseConfig(nil),
			WithServerCertificate(ss.CertPath, ss.KeyPath))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		tlsConfig := manager.TLSConfig()
		require.False(t, tlsConfig.InsecureSkipVerify)
	})

	t.Run("overrides base config", func(t *testing.T) {
		baseConfig := &tls.Config{
			InsecureSkipVerify: true,
		}

		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithBaseConfig(baseConfig),
			WithServerCertificate(ss.CertPath, ss.KeyPath))
		require.NoError(t, err)
		th.CheckedClose(t, manager)()

		tlsConfig := manager.TLSConfig()
		require.False(t, tlsConfig.InsecureSkipVerify, "allowInsecure should override base config")
	})
}

func TestTLSConfigManager_MultipleLoadersIndependent(t *testing.T) {
	ss1 := selfsigned.NewSelfSignedCert(t, selfsigned.WithDNSName("server1.example.com"))
	ss2 := selfsigned.NewSelfSignedCert(t, selfsigned.WithDNSName("server2.example.com"))

	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	loader1, err := NewServerTLSConfigManager(
		monitor,
		WithUseTLS(true),
		WithBaseConfig(nil),
		WithServerCertificate(ss1.CertPath, ss1.KeyPath))
	require.NoError(t, err)
	defer th.CheckedClose(t, loader1)()

	loader2, err := NewServerTLSConfigManager(
		monitor,
		WithUseTLS(true),
		WithBaseConfig(nil),
		WithServerCertificate(ss2.CertPath, ss2.KeyPath))
	require.NoError(t, err)
	defer th.CheckedClose(t, loader2)()

	tlsConfig1 := loader1.TLSConfig()
	tlsConfig2 := loader2.TLSConfig()

	// Configs should be different instances
	require.NotSame(t, tlsConfig1, tlsConfig2)
}

func TestTLSConfigManager_UseTLSFalse(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	t.Run("returns nil config and no error", func(t *testing.T) {
		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(false),
			WithBaseConfig(nil),
			WithServerCertificate("/any/cert.pem", "/any/key.pem"))
		require.NoError(t, err)
		require.NotNil(t, manager)
		defer th.CheckedClose(t, manager)()

		tlsConfig := manager.TLSConfig()
		require.Nil(t, tlsConfig)
		require.False(t, manager.UseTLS())
	})

	t.Run("no error on bad cert when disabled", func(t *testing.T) {
		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(false),
			WithBaseConfig(nil),
			WithServerCertificate("/any/cert.pem", "/any/key.pem"))
		require.NoError(t, err)
		require.NotNil(t, manager)
		defer th.CheckedClose(t, manager)()
	})

	t.Run("ignores invalid paths when disabled", func(t *testing.T) {
		// Even with nonexistent paths, useTLS=false should not error
		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(false),
			WithBaseConfig(nil),
			WithServerCertificate("/nonexistent/cert.pem", "/nonexistent/key.pem"))
		require.NoError(t, err)
		require.NotNil(t, manager)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		tlsConfig := manager.TLSConfig()
		require.Nil(t, tlsConfig)
	})

	t.Run("ignores base config when disabled", func(t *testing.T) {
		baseConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
			ServerName: "test.example.com",
		}

		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(false),
			WithBaseConfig(baseConfig),
			WithServerCertificate("/any/cert.pem", "/any/key.pem"))
		require.NoError(t, err)
		require.NotNil(t, manager)
		defer th.CheckedClose(t, manager)()

		tlsConfig := manager.TLSConfig()
		require.Nil(t, tlsConfig, "should return nil config even with base config provided")
	})

	t.Run("close works when disabled", func(t *testing.T) {
		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(false),
			WithBaseConfig(nil),
			WithServerCertificate("/any/cert.pem", "/any/key.pem"))
		require.NoError(t, err)

		// Close should succeed
		require.NoError(t, manager.Close())
		require.NoError(t, manager.Close()) // idempotent
	})
}

func TestTLSConfigManager_UseTLSWithoutCert(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	t.Run("constructor succeeds with empty paths", func(t *testing.T) {
		manager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithBaseConfig(nil),
			WithServerCertificate("", ""))
		require.NoError(t, err)
		require.NotNil(t, manager)
		require.NoError(t, manager.Close())
	})

	t.Run("constructor succeeds with implied empty paths", func(t *testing.T) {
		manager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithBaseConfig(nil))
		defer th.CheckedClose(t, manager)()
		require.NoError(t, err)
		require.NotNil(t, manager)
	})

	t.Run("returns non-nil TLSConfig", func(t *testing.T) {
		manager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithBaseConfig(nil),
			WithServerCertificate("", ""),
			WithAllowInsecure(true))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		tlsConfig := manager.TLSConfig()
		require.NotNil(t, tlsConfig)
		require.True(t, manager.UseTLS())
	})

	t.Run("server constructor fails without certificate", func(t *testing.T) {
		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithAllowInsecure(true))
		require.ErrorIs(t, err, ErrCertificateEmpty)
		require.Nil(t, manager)
	})
}

func TestTLSConfigManager_Close(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t)

	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	t.Run("close after construction", func(t *testing.T) {
		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath))
		require.NoError(t, err)

		require.NoError(t, manager.Close())
	})

	t.Run("close is idempotent", func(t *testing.T) {
		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath))
		require.NoError(t, err)

		require.NoError(t, manager.Close())
		require.NoError(t, manager.Close())
	})
}

func TestTLSConfigManager_PrepareCertificateLoad(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	t.Run("useTLS false returns NOP callback", func(t *testing.T) {
		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(false),
			WithServerCertificate("/any/cert.pem", "/any/key.pem"))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		// Should return a NOP callback even with invalid paths
		callback, err := manager.PrepareReconfigure(
			WithServerCertificate("/nonexistent/cert.pem", "/nonexistent/key.pem"))
		require.NoError(t, err)
		require.NotNil(t, callback)

		// Executing the NOP callback should succeed
		require.NoError(t, callback())
	})

	t.Run("useTLS true with valid paths returns callback", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		// Prepare loading the same cert (valid paths)
		callback, err := manager.PrepareReconfigure(
			WithServerCertificate(ss.CertPath, ss.KeyPath))
		require.NoError(t, err)
		require.NotNil(t, callback)

		// Executing the callback should succeed
		require.NoError(t, callback())
	})

	t.Run("useTLS true with invalid paths returns error", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		// Prepare loading with nonexistent paths should fail
		callback, err := manager.PrepareReconfigure(
			WithServerCertificate("/nonexistent/cert.pem", "/nonexistent/key.pem"))
		require.ErrorContains(t, err, "LoadCertificate: error opening \"/nonexistent/cert.pem\" for reading: open /nonexistent/cert.pem: no such file or directory")
		require.Nil(t, callback)
	})

	t.Run("callback applies new certificate", func(t *testing.T) {
		ss1 := selfsigned.NewSelfSignedCert(t, selfsigned.WithDNSName("server1.example.com"))
		ss2 := selfsigned.NewSelfSignedCert(t, selfsigned.WithDNSName("server2.example.com"))

		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss1.CertPath, ss1.KeyPath))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		// Prepare and execute loading a different certificate
		callback, err := manager.PrepareReconfigure(
			WithServerCertificate(ss2.CertPath, ss2.KeyPath))
		require.NoError(t, err)
		require.NoError(t, callback())
	})
}

func TestTLSConfigManager_Listen(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

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
		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(false),
			WithServerCertificate("/any/cert.pem", "/any/key.pem"))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		listener, err := manager.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer th.CheckedClose(t, listener)()

		testListenerConnection(t, listener, func(addr string) (net.Conn, error) {
			return net.Dial("tcp", addr)
		})
	})

	t.Run("useTLS true returns TLS listener", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath))
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
		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(false),
			WithServerCertificate("/any/cert.pem", "/any/key.pem"))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, manager.Close())
		}()

		_, err = manager.Listen("tcp", "invalid:address:format")
		require.ErrorContains(t, err, "address invalid:address:format: too many colons in address")
	})
}

func TestTLSConfigManager_Dial(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

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
		defer th.CheckedClose(t, conn)()

		require.NoError(t, <-serverResult)
		require.Equal(t, testData, <-serverData)
	}

	t.Run("useTLS false dials plain TCP", func(t *testing.T) {
		manager, err := NewClientServerTLSConfigManager(
			monitor,
			WithUseTLS(false),
			WithServerCertificate("/any/cert.pem", "/any/key.pem"))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		// Create plain TCP server
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer th.CheckedClose(t, listener)()

		testDialConnection(t, listener, func(addr string) (net.Conn, error) {
			return manager.Dial("tcp", addr)
		})
	})

	t.Run("useTLS true dials TLS", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		manager, err := NewClientServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath),
			WithAllowInsecure(true))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		// Create TLS server
		cert, err := tls.LoadX509KeyPair(ss.CertPath, ss.KeyPath)
		require.NoError(t, err)
		listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
			Certificates: []tls.Certificate{cert},
		})
		require.NoError(t, err)
		defer th.CheckedClose(t, listener)()

		testDialConnection(t, listener, func(addr string) (net.Conn, error) {
			return manager.Dial("tcp", addr)
		})
	})

	t.Run("useTLS true without cert dials TLS", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		// Create manager without client cert (client-only mode)
		manager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithAllowInsecure(true))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		// Create TLS server
		cert, err := tls.LoadX509KeyPair(ss.CertPath, ss.KeyPath)
		require.NoError(t, err)
		listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
			Certificates: []tls.Certificate{cert},
		})
		require.NoError(t, err)
		defer th.CheckedClose(t, listener)()

		testDialConnection(t, listener, func(addr string) (net.Conn, error) {
			return manager.Dial("tcp", addr)
		})
	})

	t.Run("invalid address returns error", func(t *testing.T) {
		manager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(false),
			WithServerCertificate("/any/cert.pem", "/any/key.pem"))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		dialer, err := manager.Dial("tcp", "invalid:address:format")
		require.ErrorContains(t, err, "address invalid:address:format: too many colons in address")
		require.Nil(t, dialer)
	})
}

func TestNewDisabledTLSConfigManager(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	disabled := NewDisabledTLSConfigManager()
	defer th.CheckedClose(t, disabled)()

	require.NotNil(t, disabled)
	require.False(t, disabled.UseTLS())
	require.Nil(t, disabled.TLSConfig())
}

func TestNewClientTLSConfigManager(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	ss := selfsigned.NewSelfSignedCert(t)

	t.Run("TLS disabled", func(t *testing.T) {
		client, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(false))
		require.NoError(t, err)
		require.NotNil(t, client)
		defer th.CheckedClose(t, client)()

		server, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(false))
		require.NoError(t, err)
		require.NotNil(t, server)
		defer th.CheckedClose(t, server)()

		require.Equal(t, server.TLSConfig(), client.TLSConfig())
		require.False(t, client.UseTLS())

		require.NoError(t, client.Close())
		require.NoError(t, server.Close())
	})

	t.Run("TLS enabled allowInsecure false", func(t *testing.T) {
		client, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true))
		require.NoError(t, err)
		require.NotNil(t, client)
		defer th.CheckedClose(t, client)()

		server, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath),
		)
		require.NoError(t, err)
		require.NotNil(t, server)
		defer th.CheckedClose(t, server)()

		require.Equal(t, server.TLSConfig().InsecureSkipVerify, client.TLSConfig().InsecureSkipVerify)
		require.True(t, client.UseTLS())

		require.NoError(t, client.Close())
		require.NoError(t, server.Close())
	})

	t.Run("TLS enabled allowInsecure true", func(t *testing.T) {
		client, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithAllowInsecure(true))
		require.NoError(t, err)
		require.NotNil(t, client)
		defer th.CheckedClose(t, client)()

		server, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithAllowInsecure(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath))
		require.NoError(t, err)
		require.NotNil(t, server)
		defer th.CheckedClose(t, server)()

		require.Equal(t, server.TLSConfig().InsecureSkipVerify, client.TLSConfig().InsecureSkipVerify)
	})

	t.Run("with base config", func(t *testing.T) {
		baseConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
			MaxVersion: tls.VersionTLS13,
			ServerName: "test.example.com",
		}

		client, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithBaseConfig(baseConfig))
		require.NoError(t, err)
		require.NotNil(t, client)
		defer th.CheckedClose(t, client)()

		server, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithBaseConfig(baseConfig),
			WithServerCertificate(ss.CertPath, ss.KeyPath))
		require.NoError(t, err)
		require.NotNil(t, server)
		defer th.CheckedClose(t, server)()

		// Verify base config values are preserved in both
		require.Equal(t, server.TLSConfig().MinVersion, client.TLSConfig().MinVersion)
		require.Equal(t, server.TLSConfig().MaxVersion, client.TLSConfig().MaxVersion)
		require.Equal(t, server.TLSConfig().ServerName, client.TLSConfig().ServerName)
		require.Equal(t, server.TLSConfig().InsecureSkipVerify, client.TLSConfig().InsecureSkipVerify)
	})

	t.Run("base config is cloned", func(t *testing.T) {
		baseConfig := &tls.Config{
			ServerName: "original.example.com",
		}

		client, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithBaseConfig(baseConfig))
		require.NoError(t, err)
		require.NotNil(t, client)
		defer th.CheckedClose(t, client)()

		// Verify config is cloned (not same instance)
		require.NotSame(t, baseConfig, client.TLSConfig())

		// Verify modifying base config doesn't affect client config
		baseConfig.ServerName = "modified.example.com"
		require.Equal(t, "original.example.com", client.TLSConfig().ServerName)

		require.NoError(t, client.Close())
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
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	t.Run("sets RootCAs from file", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		// Build expected CertPool
		expectedPool := x509.NewCertPool()
		caCert, err := os.ReadFile(ss.CACertPath)
		require.NoError(t, err)
		require.True(t, expectedPool.AppendCertsFromPEM(caCert))

		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath),
			WithRootCA(&CAConfig{Paths: []string{ss.CACertPath}}))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		tlsConfig := manager.TLSConfig()
		require.NotNil(t, tlsConfig)
		require.NotNil(t, tlsConfig.RootCAs)
		require.True(t, tlsConfig.RootCAs.Equal(expectedPool), "RootCAs should match expected pool")
	})

	t.Run("client trusts server with CA", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		// Server manager with certificate
		serverManager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath))
		require.NoError(t, err)
		defer th.CheckedClose(t, serverManager)()

		// Client manager trusting the CA that signed the server certificate
		clientManager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithRootCA(&CAConfig{Paths: []string{ss.CACertPath}}))
		require.NoError(t, err)
		defer th.CheckedClose(t, clientManager)()

		// Start server
		listener, err := serverManager.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		require.NotNil(t, listener)
		defer th.CheckedClose(t, listener)()

		// Server accepts and echoes
		testData := []byte("hello")
		serverDone := make(chan error, 1)
		go simpleEchoServer(serverDone, listener, len(testData))

		// Client connects with CA-trusted connection
		conn, err := clientManager.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		require.NotNil(t, conn)
		defer th.CheckedClose(t, conn)()

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
		// A different, unrelated self-signed CA. It makes for a valid (non-empty)
		// root CA config that nonetheless does not trust the server's certificate.
		otherSS := selfsigned.NewSelfSignedCert(t, selfsigned.WithCASubject("other", "Other CA"))

		// Server manager
		serverManager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath))
		require.NoError(t, err)
		defer th.CheckedClose(t, serverManager)()

		// Client manager trusting only an unrelated CA, so it will not trust the server.
		clientManager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithRootCA(&CAConfig{Paths: []string{otherSS.CACertPath}}))
		require.NoError(t, err)
		defer th.CheckedClose(t, clientManager)()

		// Start server
		listener, err := serverManager.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer th.CheckedClose(t, listener)()

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

		manager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithRootCA(&CAConfig{Paths: []string{ss1.CACertPath, ss2.CACertPath}}))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		tlsConfig := manager.TLSConfig()
		require.NotNil(t, tlsConfig)
		require.NotNil(t, tlsConfig.RootCAs)
		require.True(t, tlsConfig.RootCAs.Equal(expectedPool), "RootCAs should contain both CAs")
	})

	t.Run("error on nonexistent file", func(t *testing.T) {
		manager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithRootCA(&CAConfig{Paths: []string{"/nonexistent/ca.pem"}}))
		require.ErrorIs(t, err, os.ErrNotExist)
		require.ErrorContains(t, err, "error creating root CA pool: error reading file \"/nonexistent/ca.pem\" for CA store: open /nonexistent/ca.pem: no such file or directory")
		require.Nil(t, manager)
	})

	t.Run("error on invalid PEM file", func(t *testing.T) {
		// Create a temporary file with invalid PEM content
		tmpDir := t.TempDir()
		tmpFile := path.Join(tmpDir, "invalid.pem")
		require.NoError(t, os.WriteFile(tmpFile, []byte("not a valid PEM file"), 0644))

		manager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithRootCA(&CAConfig{Paths: []string{tmpFile}}))
		require.ErrorContains(t, err, "error creating root CA pool: error adding certificates from \""+tmpFile+"\" to CA store: no valid certificates found")
		require.Nil(t, manager)
	})
}

func TestTLSConfigManager_WithRootCAIncludeSystem(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	t.Run("includes system CA pool", func(t *testing.T) {
		// Get expected system pool
		expectedPool, err := x509.SystemCertPool()
		require.NoError(t, err)

		manager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithRootCA(&CAConfig{IncludeSystem: true}))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		tlsConfig := manager.TLSConfig()
		require.NotNil(t, tlsConfig)
		require.NotNil(t, tlsConfig.RootCAs)
		require.True(t, tlsConfig.RootCAs.Equal(expectedPool), "RootCAs should equal system pool")
	})

	t.Run("trusts no certificates is an error", func(t *testing.T) {
		// A non-nil root CA config that neither lists paths nor includes the
		// system pool trusts nothing and is rejected at construction.
		manager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithRootCA(&CAConfig{IncludeSystem: false}))
		require.ErrorIs(t, err, ErrCATrustsNothing)
		require.Nil(t, manager)
	})

	t.Run("trusts no certificates is an error even when insecure", func(t *testing.T) {
		// A non-nil config is validated regardless of allowInsecure, so a
		// no-trust-anchors config is still rejected at construction.
		manager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithAllowInsecure(true),
			WithRootCA(&CAConfig{IncludeSystem: false}))
		require.ErrorIs(t, err, ErrCATrustsNothing)
		require.Nil(t, manager)
	})

	t.Run("combined with CA files", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		// Build expected pool: system + custom CA
		expectedPool, err := x509.SystemCertPool()
		require.NoError(t, err)
		caCert, err := os.ReadFile(ss.CACertPath)
		require.NoError(t, err)
		require.True(t, expectedPool.AppendCertsFromPEM(caCert))

		manager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithRootCA(&CAConfig{Paths: []string{ss.CACertPath}, IncludeSystem: true}))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		tlsConfig := manager.TLSConfig()
		require.NotNil(t, tlsConfig)
		require.NotNil(t, tlsConfig.RootCAs)
		require.True(t, tlsConfig.RootCAs.Equal(expectedPool), "RootCAs should contain system + custom CA")
	})
}

func TestTLSConfigManager_WithClientCAFiles(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	t.Run("sets ClientCAs from file", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		// Build expected CertPool
		expectedPool := x509.NewCertPool()
		caCert, err := os.ReadFile(ss.CACertPath)
		require.NoError(t, err)
		require.True(t, expectedPool.AppendCertsFromPEM(caCert))

		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath),
			WithClientAuth(tls.RequireAndVerifyClientCert),
			WithClientCA(&CAConfig{Paths: []string{ss.CACertPath}}))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		tlsConfig := manager.TLSConfig()
		require.NotNil(t, tlsConfig)
		require.NotNil(t, tlsConfig.ClientCAs)
		require.True(t, tlsConfig.ClientCAs.Equal(expectedPool), "ClientCAs should match expected pool")
	})

	t.Run("server verifies client certificate", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		// Server manager that requires client certificates.
		serverManager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath),
			WithClientCA(&CAConfig{Paths: []string{ss.CACertPath}}),
			WithClientAuth(tls.RequireAndVerifyClientCert))
		require.NoError(t, err)
		defer th.CheckedClose(t, serverManager)()

		// Start server
		listener, err := serverManager.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		require.NotNil(t, listener)
		defer th.CheckedClose(t, listener)()

		// Server accepts and echoes
		testData := []byte("hello")
		serverDone := make(chan error, 1)
		go simpleEchoServer(serverDone, listener, len(testData))

		// Client config with client certificate. Ignore certificate validity from server, we're testing
		// client certificate functionality here.
		clientManager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath),
			WithAllowInsecure(true))
		require.NoError(t, err)
		defer th.CheckedClose(t, clientManager)()

		conn, err := clientManager.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		require.NotNil(t, conn)
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

	t.Run("server rejects client without valid certificate", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)
		otherSS := selfsigned.NewSelfSignedCert(t, selfsigned.WithCASubject("other", "Other CA"))

		// Server manager that requires client certificates
		serverManager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath),
			WithClientAuth(tls.RequireAndVerifyClientCert),
			WithClientCA(&CAConfig{Paths: []string{ss.CACertPath}}))
		require.NoError(t, err)
		defer th.CheckedClose(t, serverManager)()

		serverTLSConfig := serverManager.TLSConfig()
		require.NotNil(t, serverTLSConfig)
		require.Equal(t, tls.RequireAndVerifyClientCert, serverTLSConfig.ClientAuth)

		// Client with certificate signed by different CA. Ignore server certificate validity. We're
		// checking client certificate functionality here.
		clientManager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(otherSS.CertPath, otherSS.KeyPath),
			WithAllowInsecure(true))
		require.NoError(t, err)
		defer th.CheckedClose(t, clientManager)()

		// Start server
		listener, err := serverManager.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		require.NotNil(t, listener)
		defer th.CheckedClose(t, listener)()

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

		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss1.CertPath, ss1.KeyPath),
			WithClientAuth(tls.RequireAndVerifyClientCert),
			WithClientCA(&CAConfig{Paths: []string{ss1.CACertPath, ss2.CACertPath}}))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		tlsConfig := manager.TLSConfig()
		require.NotNil(t, tlsConfig)
		require.NotNil(t, tlsConfig.ClientCAs)
		require.True(t, tlsConfig.ClientCAs.Equal(expectedPool), "ClientCAs should contain both CAs")
	})

	t.Run("error on nonexistent file", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath),
			WithClientAuth(tls.RequireAndVerifyClientCert),
			WithClientCA(&CAConfig{Paths: []string{"/nonexistent/ca.pem"}}))
		require.ErrorIs(t, err, os.ErrNotExist)
		require.ErrorContains(t, err, "error creating client CA pool: error reading file \"/nonexistent/ca.pem\" for CA store: open /nonexistent/ca.pem: no such file or directory")
		require.Nil(t, manager)
	})

	t.Run("error on invalid PEM file", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		// Create a temporary file with invalid PEM content
		tmpDir := t.TempDir()
		tmpFile := path.Join(tmpDir, "invalid.pem")
		require.NoError(t, os.WriteFile(tmpFile, []byte("not a valid PEM file"), 0644))

		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath),
			WithClientAuth(tls.RequireAndVerifyClientCert),
			WithClientCA(&CAConfig{Paths: []string{tmpFile}}))
		require.ErrorContains(t, err, "error creating client CA pool: error adding certificates from \""+tmpFile+"\" to CA store: no valid certificates found")
		require.Nil(t, manager)
	})
}

func TestTLSConfigManager_WithClientCAIncludeSystem(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	t.Run("includes system CA pool", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		// Get expected system pool
		expectedPool, err := x509.SystemCertPool()
		require.NoError(t, err)

		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath),
			WithClientAuth(tls.RequireAndVerifyClientCert),
			WithClientCA(&CAConfig{IncludeSystem: true}))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		tlsConfig := manager.TLSConfig()
		require.NotNil(t, tlsConfig)
		require.NotNil(t, tlsConfig.ClientCAs)
		require.True(t, tlsConfig.ClientCAs.Equal(expectedPool), "ClientCAs should equal system pool")
	})

	t.Run("trusts no certificates is an error", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		// A client CA config that neither lists paths nor includes the system pool
		// trusts nothing and is rejected at construction.
		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath),
			WithClientAuth(tls.RequireAndVerifyClientCert),
			WithClientCA(&CAConfig{IncludeSystem: false}))
		require.ErrorIs(t, err, ErrCATrustsNothing)
		require.Nil(t, manager)
	})

	t.Run("combined with CA files", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		// Build expected pool: system + custom CA
		expectedPool, err := x509.SystemCertPool()
		require.NoError(t, err)
		caCert, err := os.ReadFile(ss.CACertPath)
		require.NoError(t, err)
		require.True(t, expectedPool.AppendCertsFromPEM(caCert))

		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath),
			WithClientAuth(tls.RequireAndVerifyClientCert),
			WithClientCA(&CAConfig{Paths: []string{ss.CACertPath}, IncludeSystem: true}))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		tlsConfig := manager.TLSConfig()
		require.NotNil(t, tlsConfig)
		require.NotNil(t, tlsConfig.ClientCAs)
		require.True(t, tlsConfig.ClientCAs.Equal(expectedPool), "ClientCAs should contain system + custom CA")
	})

	t.Run("built without client auth: include-system", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		// A non-nil client CA config is validated and built even without client
		// auth (the pool is simply unused until auth is enabled).
		expectedPool, err := x509.SystemCertPool()
		require.NoError(t, err)

		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath),
			WithClientCA(&CAConfig{IncludeSystem: true}))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		tlsConfig := manager.TLSConfig()
		require.NotNil(t, tlsConfig)
		require.NotNil(t, tlsConfig.ClientCAs)
		require.True(t, tlsConfig.ClientCAs.Equal(expectedPool), "ClientCAs should be built even without client auth")
	})

	t.Run("built without client auth: paths and include-system", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		// Build expected pool: system + custom CA.
		expectedPool, err := x509.SystemCertPool()
		require.NoError(t, err)
		caCert, err := os.ReadFile(ss.CACertPath)
		require.NoError(t, err)
		require.True(t, expectedPool.AppendCertsFromPEM(caCert))

		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath),
			WithClientCA(&CAConfig{Paths: []string{ss.CACertPath}, IncludeSystem: true}))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		tlsConfig := manager.TLSConfig()
		require.NotNil(t, tlsConfig)
		require.NotNil(t, tlsConfig.ClientCAs)
		require.True(t, tlsConfig.ClientCAs.Equal(expectedPool), "ClientCAs should be built even without client auth")
	})
}

func TestTLSConfigManager_CAOptionsNotSetByDefault(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	// When no CA options are specified, the TLS config should not have
	// custom CA pools set (allowing Go's default behavior)
	ss := selfsigned.NewSelfSignedCert(t)

	manager, err := NewServerTLSConfigManager(
		monitor,
		WithUseTLS(true),
		WithServerCertificate(ss.CertPath, ss.KeyPath))
	require.NoError(t, err)
	defer th.CheckedClose(t, manager)()

	tlsConfig := manager.TLSConfig()
	require.NotNil(t, tlsConfig)
	require.Nil(t, tlsConfig.RootCAs, "RootCAs should be nil when no CA options specified")
	require.Nil(t, tlsConfig.ClientCAs, "ClientCAs should be nil when no CA options specified")
}

func TestTLSConfigManager_CAOptionsWithBaseConfig(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t)
	anotherSS := selfsigned.NewSelfSignedCert(t, selfsigned.WithCASubject("another", "Another CA"))

	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

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
	manager, err := NewClientTLSConfigManager(
		monitor,
		WithUseTLS(true),
		WithBaseConfig(baseConfig),
		WithRootCA(&CAConfig{Paths: []string{anotherSS.CACertPath}}))
	require.NoError(t, err)
	defer th.CheckedClose(t, manager)()

	tlsConfig := manager.TLSConfig()
	require.NotNil(t, tlsConfig)
	require.NotNil(t, tlsConfig.RootCAs)
	// The RootCAs should match the new CA, not the base config
	require.True(t, tlsConfig.RootCAs.Equal(expectedPool), "RootCAs should be overridden by WithRootCA")
	require.False(t, tlsConfig.RootCAs.Equal(basePool), "RootCAs should not equal base pool")
}

func TestTLSConfigManager_CAResolution(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	t.Run("root nil defers to base config RootCAs", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		// Base config supplies a RootCAs pool. With no WithRootCA, the base pool
		// should be left in place unchanged.
		basePool := x509.NewCertPool()
		caCert, err := os.ReadFile(ss.CACertPath)
		require.NoError(t, err)
		require.True(t, basePool.AppendCertsFromPEM(caCert))
		baseConfig := &tls.Config{RootCAs: basePool}

		manager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithBaseConfig(baseConfig))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		tlsConfig := manager.TLSConfig()
		require.NotNil(t, tlsConfig)
		require.NotNil(t, tlsConfig.RootCAs)
		require.True(t, tlsConfig.RootCAs.Equal(basePool), "RootCAs should equal the base config's pool")
	})

	t.Run("root no-trust-anchors is an error", func(t *testing.T) {
		manager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithRootCA(&CAConfig{}))
		require.ErrorIs(t, err, ErrCATrustsNothing)
		require.Nil(t, manager)
	})

	t.Run("root no-trust-anchors is an error even when insecure", func(t *testing.T) {
		// A non-nil config is validated regardless of allowInsecure, so a
		// no-trust-anchors config is still rejected at construction.
		manager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithAllowInsecure(true),
			WithRootCA(&CAConfig{}))
		require.ErrorIs(t, err, ErrCATrustsNothing)
		require.Nil(t, manager)
	})

	t.Run("client nil under client auth defers to base config ClientCAs", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		// Base config supplies a ClientCAs pool. With client auth enabled but no
		// WithClientCA, the base pool should be left in place unchanged.
		basePool := x509.NewCertPool()
		caCert, err := os.ReadFile(ss.CACertPath)
		require.NoError(t, err)
		require.True(t, basePool.AppendCertsFromPEM(caCert))
		baseConfig := &tls.Config{ClientCAs: basePool}

		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithBaseConfig(baseConfig),
			WithServerCertificate(ss.CertPath, ss.KeyPath),
			WithClientAuth(tls.RequireAndVerifyClientCert))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		tlsConfig := manager.TLSConfig()
		require.NotNil(t, tlsConfig)
		require.NotNil(t, tlsConfig.ClientCAs)
		require.True(t, tlsConfig.ClientCAs.Equal(basePool), "ClientCAs should equal the base config's pool")
	})

	t.Run("client no-trust-anchors is an error even without client auth", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		// A non-nil client CA config is validated regardless of whether client
		// auth is enabled, so a config that trusts nothing is rejected here even
		// with the default NoClientCert.
		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath),
			WithClientCA(&CAConfig{}))
		require.ErrorIs(t, err, ErrCATrustsNothing)
		require.Nil(t, manager)
	})

	t.Run("client config built even without client auth", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		// A non-nil client CA config is built even with the default NoClientCert;
		// the pool is simply unused until client auth is enabled.
		expectedPool := x509.NewCertPool()
		caCert, err := os.ReadFile(ss.CACertPath)
		require.NoError(t, err)
		require.True(t, expectedPool.AppendCertsFromPEM(caCert))

		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath),
			WithClientCA(&CAConfig{Paths: []string{ss.CACertPath}}))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		tlsConfig := manager.TLSConfig()
		require.NotNil(t, tlsConfig)
		require.NotNil(t, tlsConfig.ClientCAs)
		require.True(t, tlsConfig.ClientCAs.Equal(expectedPool), "ClientCAs should be built even without client auth")
	})

	t.Run("client paths without include-system trusts only those paths", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		// A non-nil config with Paths but IncludeSystem false (zero value) should
		// build a pool of only those paths, not the system pool.
		expectedPool := x509.NewCertPool()
		caCert, err := os.ReadFile(ss.CACertPath)
		require.NoError(t, err)
		require.True(t, expectedPool.AppendCertsFromPEM(caCert))

		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath),
			WithClientAuth(tls.RequireAndVerifyClientCert),
			WithClientCA(&CAConfig{Paths: []string{ss.CACertPath}}))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		tlsConfig := manager.TLSConfig()
		require.NotNil(t, tlsConfig)
		require.NotNil(t, tlsConfig.ClientCAs)
		require.True(t, tlsConfig.ClientCAs.Equal(expectedPool), "ClientCAs should contain only the configured path")
	})
}

func TestTLSConfigManager_ClientAuthOverride(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	// Base config with a non-zero ClientAuth, so we can tell whether an option
	// overrode it (including overriding it back to the zero value).
	base := func() *tls.Config {
		return &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert}
	}

	t.Run("no option leaves base ClientAuth", func(t *testing.T) {
		manager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithBaseConfig(base()))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()
		require.Equal(t, tls.RequireAndVerifyClientCert, manager.TLSConfig().ClientAuth)
	})

	t.Run("WithClientAuth overrides base", func(t *testing.T) {
		manager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true), WithBaseConfig(base()),
			WithClientAuth(tls.VerifyClientCertIfGiven))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()
		require.Equal(t, tls.VerifyClientCertIfGiven, manager.TLSConfig().ClientAuth)
	})

	t.Run("WithClientAuth overrides base with zero value", func(t *testing.T) {
		manager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithBaseConfig(base()),
			WithClientAuth(tls.NoClientCert))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()
		require.Equal(t, tls.NoClientCert, manager.TLSConfig().ClientAuth)
	})

	t.Run("WithClientAuthPtr nil leaves base ClientAuth", func(t *testing.T) {
		manager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithBaseConfig(base()),
			WithClientAuthPtr(nil))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()
		require.Equal(t, tls.RequireAndVerifyClientCert, manager.TLSConfig().ClientAuth)
	})

	t.Run("WithClientAuthPtr non-nil overrides base with zero value", func(t *testing.T) {
		auth := tls.NoClientCert
		manager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithBaseConfig(base()),
			WithClientAuthPtr(&auth))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()
		require.Equal(t, tls.NoClientCert, manager.TLSConfig().ClientAuth)
	})

	t.Run("WithClientAuthPtr non-nil overrides base with non-zero value", func(t *testing.T) {
		auth := tls.VerifyClientCertIfGiven
		manager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithBaseConfig(base()),
			WithClientAuthPtr(&auth))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()
		require.Equal(t, tls.VerifyClientCertIfGiven, manager.TLSConfig().ClientAuth)
	})
}

// testManagerCheckCapture time is how long to capture logs during logging tests. To prevent flaky tests,
// it should be more than testCheckTime, but less than 2 * testCheckTime.
const testManagerCheckCapture = 500 * time.Millisecond

func TestTLSConfigManager_WithCertLoaderOptions(t *testing.T) {
	// This test verifies that WithLogger is properly passed to the underlying TLSCertLoader.

	// Create a certificate that expires in 24 hours
	notBefore := time.Now().UTC().Truncate(time.Minute).Add(-7 * 24 * time.Hour)
	notAfter := time.Now().UTC().Truncate(time.Hour).Add(24 * time.Hour)

	ss := selfsigned.NewSelfSignedCert(t, selfsigned.WithNotBefore(notBefore), selfsigned.WithNotAfter(notAfter))

	core, logs := observer.New(zapcore.InfoLevel)
	logger := zap.New(core)

	// No logger so logging has to come through manager.
	monitor := newTestCertMonitor(t, WithMonitorExpirationAdvanced(2*24*time.Hour), WithMonitorLogger(logger), WithMonitorTriggerDelay(0))
	defer th.CheckedClose(t, monitor)()

	// Create TLSConfigManager with all three options:
	// - WithLogger: to capture log output
	// - WithCertificateCheckInterval: to set a short check interval
	// - WithExpirationAdvanced: to set the expiration warning window to 2 days (> 24 hours remaining)
	manager, err := NewServerTLSConfigManager(
		monitor,
		WithUseTLS(true),
		WithServerCertificate(ss.CertPath, ss.KeyPath),
		WithLogger(logger))
	require.NoError(t, err)
	require.NotNil(t, manager)
	defer th.CheckedClose(t, manager)()

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
	time.Sleep(testWarnWaitTime)
	checkWarning(t)

	// Check for warning during monitor (after another check interval)
	require.Zero(t, logs.Len(), "init logs not dumped properly")
	time.Sleep(testManagerCheckCapture)
	checkWarning(t)
}

func TestTLSConfigManager_WithIgnoreFilePermissions(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	t.Run("fails without ignore when cert permissions too open", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		require.NoError(t, os.Chmod(ss.CertPath, 0660))
		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath))
		require.ErrorContains(t, err, fmt.Sprintf("LoadCertificate: file permissions are too open: for %q, maximum is 0644 (-rw-r--r--) but found 0660 (-rw-rw----); extra permissions: 0020 (-----w----)", ss.CertPath))
		require.Nil(t, manager)
	})

	t.Run("succeeds with ignore when cert permissions too open", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		require.NoError(t, os.Chmod(ss.CertPath, 0660))
		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath),
			WithIgnoreFilePermissions(true))
		require.NoError(t, err)
		require.NotNil(t, manager)
		defer th.CheckedClose(t, manager)()

		// Verify the manager works correctly
		tlsConfig := manager.TLSConfig()
		require.NotNil(t, tlsConfig)
	})

	t.Run("fails without ignore when key permissions too open", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		require.NoError(t, os.Chmod(ss.KeyPath, 0644))
		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath))
		require.ErrorContains(t, err, fmt.Sprintf("LoadCertificate: file permissions are too open: for %q, maximum is 0600 (-rw-------) but found 0644 (-rw-r--r--); extra permissions: 0044 (----r--r--)", ss.KeyPath))
		require.Nil(t, manager)
	})

	t.Run("succeeds with ignore when key permissions too open", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		require.NoError(t, os.Chmod(ss.KeyPath, 0644))
		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath),
			WithIgnoreFilePermissions(true))
		require.NoError(t, err)
		require.NotNil(t, manager)
		defer th.CheckedClose(t, manager)()

		// Verify the manager works correctly
		tlsConfig := manager.TLSConfig()
		require.NotNil(t, tlsConfig)
	})
}

func TestTLSConfigManager_DialWithDialer(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

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
		defer th.CheckedClose(t, conn)()

		require.NoError(t, <-serverResult)
		require.Equal(t, testData, <-serverData)
	}

	t.Run("dialer LocalAddr is used for plain TCP", func(t *testing.T) {
		manager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(false),
			WithServerCertificate("/any/cert.pem", "/any/key.pem"))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		// Create plain TCP server
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer th.CheckedClose(t, listener)

		testDialWithDialerConnection(t, listener, func(dialer *net.Dialer, addr string) (net.Conn, error) {
			return manager.DialWithDialer(dialer, "tcp", addr)
		})
	})

	t.Run("dialer LocalAddr is used for TLS", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		manager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithAllowInsecure(true))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		// Create TLS server
		cert, err := tls.LoadX509KeyPair(ss.CertPath, ss.KeyPath)
		require.NoError(t, err)
		listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
			Certificates: []tls.Certificate{cert},
		})
		require.NoError(t, err)
		defer th.CheckedClose(t, listener)()

		testDialWithDialerConnection(t, listener, func(dialer *net.Dialer, addr string) (net.Conn, error) {
			return manager.DialWithDialer(dialer, "tcp", addr)
		})
	})
}

func TestTLSConfigManager_WithUsage(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	ss := selfsigned.NewSelfSignedCert(t)

	t.Run("usage names the cert loaders by role", func(t *testing.T) {
		manager, err := NewClientServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithUsage("subscriber"),
			WithServerCertificate(ss.CertPath, ss.KeyPath))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		require.Equal(t, "subscriber.server", manager.serverCertLoader.Usage())
		require.Equal(t, "subscriber.client", manager.clientCertLoader.Usage())
	})

	t.Run("usage prefixes manager errors", func(t *testing.T) {
		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithUsage("httpd"),
			WithServerCertificate(ss.CertPath, ss.KeyPath))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		// A server manager cannot dial; the message should say which manager.
		_, err = manager.Dial("tcp", "127.0.0.1:0")
		require.ErrorContains(t, err, "httpd: ")
	})

	t.Run("unset usage still names the loaders", func(t *testing.T) {
		manager, err := NewClientServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		require.Equal(t, ".server", manager.serverCertLoader.Usage())
		require.Equal(t, ".client", manager.clientCertLoader.Usage())
	})
}

func TestTLSConfigManager_RoleRestrictions(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	ss := selfsigned.NewSelfSignedCert(t)

	t.Run("client manager cannot Listen", func(t *testing.T) {
		manager, err := NewClientTLSConfigManager(monitor, WithUseTLS(true))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		listener, err := manager.Listen("tcp", "127.0.0.1:0")
		require.ErrorIs(t, err, ErrClientListen)
		require.Nil(t, listener)
	})

	t.Run("server manager cannot Dial", func(t *testing.T) {
		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		conn, err := manager.Dial("tcp", "127.0.0.1:0")
		require.ErrorIs(t, err, ErrServerDial)
		require.Nil(t, conn)

		conn, err = manager.DialWithDialer(&net.Dialer{}, "tcp", "127.0.0.1:0")
		require.ErrorIs(t, err, ErrServerDial)
		require.Nil(t, conn)
	})

	t.Run("client and server manager can do both", func(t *testing.T) {
		manager, err := NewClientServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath),
			WithAllowInsecure(true))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		listener, err := manager.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer th.CheckedClose(t, listener)()

		// The same manager serves and dials its own listener.
		testData := []byte("hello")
		serverDone := make(chan error, 1)
		go simpleEchoServer(serverDone, listener, len(testData))

		conn, err := manager.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer th.CheckedClose(t, conn)()

		_, err = conn.Write(testData)
		require.NoError(t, err)

		buf := make([]byte, len(testData))
		_, err = conn.Read(buf)
		require.NoError(t, err)
		require.Equal(t, testData, buf)

		require.NoError(t, <-serverDone)
	})
}

func TestTLSConfigManager_ConstructorValidation(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	t.Run("missing monitor", func(t *testing.T) {
		manager, err := NewServerTLSConfigManager(nil, WithUseTLS(false))
		require.ErrorIs(t, err, ErrNoCertificateMonitor)
		require.Nil(t, manager)
	})

	t.Run("missing role", func(t *testing.T) {
		// The exported constructors always supply a role, so this guards
		// against misuse of the internal constructor.
		manager, err := newTLSConfigManager(withMonitor(monitor), WithUseTLS(false))
		require.ErrorIs(t, err, ErrNoRole)
		require.Nil(t, manager)
	})
}

func TestTLSConfigManager_PrepareReconfigureDisabled(t *testing.T) {
	disabled := NewDisabledTLSConfigManager()
	defer th.CheckedClose(t, disabled)()

	// A disabled manager has no configuration to copy. Reconfiguring it must
	// report that clearly instead of panicking.
	require.NotPanics(t, func() {
		apply, err := disabled.PrepareReconfigure(WithUseTLS(true))
		require.ErrorIs(t, err, ErrConfigureDisabledManager)
		require.Nil(t, apply)
	})

	require.False(t, disabled.UseTLS(), "a disabled manager stays disabled")
}

func TestTLSConfigManager_PrepareReconfigureUseTLS(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	ss := selfsigned.NewSelfSignedCert(t)

	t.Run("server cannot toggle TLS", func(t *testing.T) {
		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		// A listener is already bound with the current configuration, so
		// turning TLS off underneath it is refused.
		apply, err := manager.PrepareReconfigure(WithUseTLS(false))
		require.ErrorIs(t, err, ErrNotSupportedServer)
		require.Nil(t, apply)
		require.True(t, manager.UseTLS(), "a refused reconfigure must not change the manager")
	})

	t.Run("server reconfigure without a TLS change is allowed", func(t *testing.T) {
		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		newSS := selfsigned.NewSelfSignedCert(t)
		apply, err := manager.PrepareReconfigure(WithServerCertificate(newSS.CertPath, newSS.KeyPath))
		require.NoError(t, err)
		require.NoError(t, apply())

		certPath, _ := manager.serverCertLoader.Paths()
		require.Equal(t, newSS.CertPath, certPath)
	})

	t.Run("client can turn TLS off", func(t *testing.T) {
		manager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()
		require.True(t, manager.UseTLS())

		apply, err := manager.PrepareReconfigure(WithUseTLS(false))
		require.NoError(t, err)
		require.NoError(t, apply())

		require.False(t, manager.UseTLS())
		require.Nil(t, manager.TLSConfig())

		// Disabling TLS clears the loaded certificate.
		certPath, keyPath := manager.clientCertLoader.Paths()
		require.Empty(t, certPath)
		require.Empty(t, keyPath)
	})
}

func TestTLSConfigManager_DialContext(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	t.Run("useTLS false dials plain TCP", func(t *testing.T) {
		manager, err := NewClientTLSConfigManager(monitor, WithUseTLS(false))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer th.CheckedClose(t, listener)()

		testData := []byte("hello")
		serverDone := make(chan error, 1)
		go simpleEchoServer(serverDone, listener, len(testData))

		conn, err := manager.DialContext(context.Background(), "tcp", listener.Addr().String())
		require.NoError(t, err)
		defer th.CheckedClose(t, conn)()

		_, err = conn.Write(testData)
		require.NoError(t, err)

		buf := make([]byte, len(testData))
		_, err = conn.Read(buf)
		require.NoError(t, err)
		require.Equal(t, testData, buf)
		require.NoError(t, <-serverDone)
	})

	t.Run("useTLS true dials TLS", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		serverManager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath))
		require.NoError(t, err)
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
			WithRootCA(&CAConfig{Paths: []string{ss.CACertPath}}))
		require.NoError(t, err)
		defer th.CheckedClose(t, clientManager)()

		conn, err := clientManager.DialContext(context.Background(), "tcp", listener.Addr().String())
		require.NoError(t, err)
		defer th.CheckedClose(t, conn)()

		// ServerName is inferred from the address, so the peer is verified
		// against the root CAs rather than skipped.
		require.True(t, conn.(*tls.Conn).ConnectionState().HandshakeComplete)

		_, err = conn.Write(testData)
		require.NoError(t, err)

		buf := make([]byte, len(testData))
		_, err = conn.Read(buf)
		require.NoError(t, err)
		require.Equal(t, testData, buf)
		require.NoError(t, <-serverDone)
	})

	t.Run("server manager cannot DialContext", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		manager, err := NewServerTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithServerCertificate(ss.CertPath, ss.KeyPath))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		conn, err := manager.DialContext(context.Background(), "tcp", "127.0.0.1:0")
		require.ErrorIs(t, err, ErrServerDial)
		require.Nil(t, conn)
	})

	t.Run("cancellation aborts the handshake", func(t *testing.T) {
		// A peer that accepts the connection but never speaks TLS: without
		// honoring ctx the handshake would hang here.
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer th.CheckedClose(t, listener)()

		accepted := make(chan net.Conn, 1)
		go func() {
			conn, err := listener.Accept()
			if err != nil {
				close(accepted)
				return
			}
			accepted <- conn
		}()
		t.Cleanup(func() {
			if conn, ok := <-accepted; ok && conn != nil {
				conn.Close()
			}
		})

		manager, err := NewClientTLSConfigManager(monitor, WithUseTLS(true), WithAllowInsecure(true))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		// An http.Client.Timeout reaches a dialer as a cancellation with no
		// deadline attached, so that is what is reproduced here.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, hasDeadline := ctx.Deadline()
		require.False(t, hasDeadline, "the timeout arrives as a cancellation, not a deadline")
		time.AfterFunc(50*time.Millisecond, cancel)

		start := time.Now()
		conn, err := manager.DialContext(ctx, "tcp", listener.Addr().String())
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, conn)
		require.Less(t, time.Since(start), 10*time.Second, "the handshake should be abandoned when ctx is cancelled")
	})

	t.Run("resolves the configuration per connection", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)
		otherSS := selfsigned.NewSelfSignedCert(t, selfsigned.WithCASubject("other", "Other CA"))

		cert, err := tls.LoadX509KeyPair(ss.CertPath, ss.KeyPath)
		require.NoError(t, err)
		listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
		require.NoError(t, err)
		defer th.CheckedClose(t, listener)()

		go func() {
			for {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				go func() {
					buf := make([]byte, 1)
					conn.Read(buf)
					conn.Close()
				}()
			}
		}()

		// Trust the wrong CA to begin with.
		manager, err := NewClientTLSConfigManager(
			monitor,
			WithUseTLS(true),
			WithRootCA(&CAConfig{Paths: []string{otherSS.CACertPath}}))
		require.NoError(t, err)
		defer th.CheckedClose(t, manager)()

		conn, err := manager.DialContext(context.Background(), "tcp", listener.Addr().String())
		require.ErrorContains(t, err, "certificate signed by unknown authority")
		require.Nil(t, conn)

		// Reconfiguring is enough: the next dial resolves the new roots without
		// the caller rebuilding anything.
		apply, err := manager.PrepareReconfigure(WithRootCA(&CAConfig{Paths: []string{ss.CACertPath}}))
		require.NoError(t, err)
		require.NoError(t, apply())

		conn, err = manager.DialContext(context.Background(), "tcp", listener.Addr().String())
		require.NoError(t, err)
		require.NoError(t, conn.Close())
	})
}

// TestTLSConfigManager_ListenResolvesConfigPerConnection covers the listener
// resolving its configuration on each connection rather than freezing it when
// the socket is bound.
func TestTLSConfigManager_ListenResolvesConfigPerConnection(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	ss := selfsigned.NewSelfSignedCert(t)
	clientSS := selfsigned.NewSelfSignedCert(t, selfsigned.WithCASubject("c", "Client CA"))

	manager, err := NewServerTLSConfigManager(
		monitor,
		WithUseTLS(true),
		WithServerCertificate(ss.CertPath, ss.KeyPath))
	require.NoError(t, err)
	defer th.CheckedClose(t, manager)()

	listener, err := manager.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer th.CheckedClose(t, listener)()

	// Report the server's view of each handshake: that is what enforces the
	// client auth policy.
	serverErr := make(chan error, 8)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				serverErr <- conn.(*tls.Conn).Handshake()
				conn.Close()
			}()
		}
	}()

	dial := func(t *testing.T, cfg *tls.Config) {
		t.Helper()
		cfg.InsecureSkipVerify = true
		conn, err := tls.Dial("tcp", listener.Addr().String(), cfg)
		if err == nil {
			conn.Handshake()
			conn.Close()
		}
	}

	dial(t, &tls.Config{})
	require.NoError(t, <-serverErr, "no client certificate is required initially")

	// Require client certificates. The listener is not rebound.
	apply, err := manager.PrepareReconfigure(
		WithClientAuth(tls.RequireAndVerifyClientCert),
		WithClientCA(&CAConfig{Paths: []string{clientSS.CACertPath}}))
	require.NoError(t, err)

	dial(t, &tls.Config{})
	require.NoError(t, <-serverErr, "PrepareReconfigure must not change the listener")

	require.NoError(t, apply())

	dial(t, &tls.Config{})
	require.Error(t, <-serverErr, "the reconfigured client auth policy should be enforced")

	clientCert, err := tls.LoadX509KeyPair(clientSS.CertPath, clientSS.KeyPath)
	require.NoError(t, err)
	dial(t, &tls.Config{Certificates: []tls.Certificate{clientCert}})
	require.NoError(t, <-serverErr, "the reconfigured client CA should verify the client")
}

// TestTLSConfigManager_ListenSessionResumption guards the session ticket keys.
// The listener resolves a fresh *tls.Config per connection; ticket keys are
// taken from the listener's own config, so they stay stable and resumption
// keeps working. Fresh per-connection keys would silently break it.
func TestTLSConfigManager_ListenSessionResumption(t *testing.T) {
	monitor := newTestCertMonitor(t)
	defer th.CheckedClose(t, monitor)()

	ss := selfsigned.NewSelfSignedCert(t)

	manager, err := NewServerTLSConfigManager(
		monitor,
		WithUseTLS(true),
		WithServerCertificate(ss.CertPath, ss.KeyPath))
	require.NoError(t, err)
	defer th.CheckedClose(t, manager)()

	listener, err := manager.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer th.CheckedClose(t, listener)()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				if err := conn.(*tls.Conn).Handshake(); err != nil {
					return
				}
				conn.Write([]byte("x"))
				buf := make([]byte, 1)
				conn.Read(buf)
			}()
		}
	}()

	// One cache across dials, as a real client would have.
	clientConfig := &tls.Config{
		InsecureSkipVerify: true,
		ClientSessionCache: tls.NewLRUClientSessionCache(8),
	}

	var resumed []bool
	for range 3 {
		conn, err := tls.Dial("tcp", listener.Addr().String(), clientConfig)
		require.NoError(t, err)
		resumed = append(resumed, conn.ConnectionState().DidResume)

		// Reading processes the session ticket, caching it for the next dial.
		buf := make([]byte, 1)
		_, err = conn.Read(buf)
		require.NoError(t, err)
		require.NoError(t, conn.Close())
	}

	require.Contains(t, resumed, true, "per-connection configs must not break session resumption")
}
