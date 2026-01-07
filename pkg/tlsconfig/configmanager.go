package tlsconfig

import (
	"crypto/tls"
	"errors"
	"net"
)

var (
	// ErrNoCertLoader indicates that an operation requiring a TLSCertLoader did not have one available.
	// This can happen if the TLSConfigManager was created without a certificate for client-side use only.
	ErrNoCertLoader = errors.New("no TLSCertLoader available")
)

// TLSConfigManager will manage a TLS configuration and make sure that only one instance of its tls.Config exists.
// Different TLSConfigManager's will have different configurations, even if they are instantiated in exactly the same way.
// No struct member is modified once the NewTLSConfigManager constructor is finished.
type TLSConfigManager struct {
	useTLS     bool
	tlsConfig  *tls.Config
	certLoader *TLSCertLoader
}

// NewTLSConfigManager returns a TLSConfigManager with the given configuration. If useTLS is true,
// then the certificate is loaded immediately if specified and the tls.Config instantiated.
// If no certPath and no keyPath is provided, then no TLSCertLoader is created. For this case, the returned
// TLSConfigManager can be used for client operations (e.g. Dial), but not for server operations (e.g. Listen).
// The allowInsecure parameter has no effect on server operations.
func NewTLSConfigManager(useTLS bool, baseConfig *tls.Config, certPath, keyPath string, allowInsecure bool, certLoaderOpts ...TLSCertLoaderOpt) (*TLSConfigManager, error) {
	var tlsConfig *tls.Config
	var certLoader *TLSCertLoader

	if useTLS {
		// Create / clone TLS configuration as necessary.
		tlsConfig = baseConfig.Clone() // nil configs are clonable.
		if tlsConfig == nil {
			tlsConfig = new(tls.Config)
		}

		// Modify configuration.
		tlsConfig.InsecureSkipVerify = allowInsecure

		// Create TLSCertLoader and configure tlsConfig to use it. No loader is created if no cert
		// is provided. This is useful for client-only use cases.
		if certPath != "" || keyPath != "" {
			if cl, err := NewTLSCertLoader(certPath, keyPath, certLoaderOpts...); err != nil {
				return nil, err
			} else {
				certLoader = cl
			}
			certLoader.SetupTLSConfig(tlsConfig)
		}
	}

	return &TLSConfigManager{
		useTLS:     useTLS,
		tlsConfig:  tlsConfig,
		certLoader: certLoader,
	}, nil
}

func (cm *TLSConfigManager) TLSConfig() *tls.Config {
	return cm.tlsConfig
}

func (cm *TLSConfigManager) TLSCertLoader() *TLSCertLoader {
	return cm.certLoader
}

func (cm *TLSConfigManager) Listen(network, address string) (net.Listener, error) {
	if cm.useTLS {
		return tls.Listen(network, address, cm.tlsConfig)
	} else {
		return net.Listen(network, address)
	}
}

func (cm *TLSConfigManager) Dial(network, address string) (net.Conn, error) {
	if cm.useTLS {
		return tls.Dial(network, address, cm.tlsConfig)
	} else {
		return net.Dial(network, address)
	}
}

// PrepareCertificateLoad is a wrapper for the TLSCertLoader's PrepareLoad method. If TLS is not
// enabled, then a NOP callback is returned.
func (cm *TLSConfigManager) PrepareCertificateLoad(certPath, keyPath string) (func() error, error) {
	if !cm.useTLS {
		return func() error { return nil }, nil
	} else if cm.certLoader == nil {
		return nil, ErrNoCertLoader
	}

	return cm.certLoader.PrepareLoad(certPath, keyPath)
}

// Close closes the underlying TLSCertLoader, if present. This is safe to call multiple times.
func (cm *TLSConfigManager) Close() error {
	if cm.certLoader != nil {
		return cm.certLoader.Close()
	}
	return nil
}
