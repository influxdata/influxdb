package tlsconfig

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"go.uber.org/zap"
)

var (
	// ErrNoCertLoader indicates that an operation requiring a TLSCertLoader did not have one available.
	// This can happen if the TLSConfigManager was created without a certificate for client-side use only.
	ErrNoCertLoader = errors.New("no TLSCertLoader available")
)

// TLSConfigManager will manage a TLS configuration and make sure that only one instance of its tls.Config exists.
// Different TLSConfigManager objects will have different configurations, even if they are instantiated in exactly
// the same way. No struct member is modified once the NewTLSConfigManager constructor is finished.
type TLSConfigManager struct {
	tlsConfig  *tls.Config
	certLoader *TLSCertLoader
}

// caConfig contains configuration to configure a TLS CA config.
type caConfig struct {
	// custom indicates if caConfig specifies a custom CA config. If false,
	// the rest of the configuration is ignored.
	custom bool

	// includeSystem indicates if system root CA should be included.
	includeSystem bool

	// files lists paths to PEM files to include in CA.
	files []string
}

// setIncludeSystem sets includeSystem and marks the config as custom.
func (c *caConfig) setIncludeSystem(include bool) {
	c.custom = true
	c.includeSystem = include
}

// addFiles adds a list of files to be included in CA configuration and marks config as custom.
func (c *caConfig) addFiles(files ...string) {
	c.custom = true
	c.files = append(c.files, files...)
}

// newCertPool returns a x509.CertPool for the configuration in c. If c is not a custom config, then
// nil is returned.
func (c *caConfig) newCertPool() (*x509.CertPool, error) {
	// Only create a CertPool for a custom CA config.
	if !c.custom {
		return nil, nil
	}

	// Create new CertPool, with system CA store if requested.
	var cp *x509.CertPool
	if c.includeSystem {
		var err error
		cp, err = x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("error getting system CA pool during newCertPool: %w", err)
		}
	} else {
		cp = x509.NewCertPool()
	}

	// Add PEM files to CA store.
	for _, fn := range c.files {
		pem, err := os.ReadFile(fn)
		if err != nil {
			return nil, fmt.Errorf("error reading file %q for CA store: %w", fn, err)
		}
		if ok := cp.AppendCertsFromPEM(pem); !ok {
			return nil, fmt.Errorf("error adding certificates from %q to CA store: no valid certificates found", fn)
		}
	}

	return cp, nil
}

// tlsConfigManagerConfig holds all options for a TLSConfigManager.
type tlsConfigManagerConfig struct {
	// useTLS indicates if TLS should be used. If not, the rest of the configuration is ignored.
	useTLS bool

	// baseConfig is the *tls.Config to use as the basis for the manager's *tls.Config.
	baseConfig *tls.Config

	// certPath is the path to the server certificate.
	certPath string

	// keyPath is the path to the server private key.
	keyPath string

	// allowInsecure indicates if certificate checks should be ignored.
	allowInsecure bool

	// rootCAConfig is the root CA config.
	rootCAConfig caConfig

	// clientCAConfig is the CA config for servers to use for verifying client certificates.
	clientCAConfig caConfig

	// clientAuth indicates the type of ClientAuth required by a server.
	clientAuth tls.ClientAuthType

	// certLoaderOpts are options for the underlying TLSCertLoader.
	certLoaderOpts []TLSCertLoaderOpt
}

// addCertLoaderOpt adds a TLSCertLoaderOpt to the configuration for the TLSCertLoader.
func (cl *tlsConfigManagerConfig) addCertLoaderOpt(o TLSCertLoaderOpt) {
	cl.certLoaderOpts = append(cl.certLoaderOpts, o)
}

// TLSConfigManagerOpt is an option for use with NewTLSConfigManager and related constructors.
type TLSConfigManagerOpt func(*tlsConfigManagerConfig)

// WithUseTLS sets if the config manager should use TLS.
func WithUseTLS(useTLS bool) TLSConfigManagerOpt {
	return func(cp *tlsConfigManagerConfig) {
		cp.useTLS = useTLS
	}
}

// WithBaseConfig sets the config manager's base *tls.Config.
func WithBaseConfig(baseConfig *tls.Config) TLSConfigManagerOpt {
	return func(cp *tlsConfigManagerConfig) {
		cp.baseConfig = baseConfig
	}
}

// WithCertificate sets the config manager's certificate and private key path.
func WithCertificate(certPath, keyPath string) TLSConfigManagerOpt {
	return func(cp *tlsConfigManagerConfig) {
		cp.certPath = certPath
		cp.keyPath = keyPath
	}
}

// WithAllowInsecure sets if the config manager should allow insecure TLS.
func WithAllowInsecure(allowInsecure bool) TLSConfigManagerOpt {
	return func(cp *tlsConfigManagerConfig) {
		cp.allowInsecure = allowInsecure
	}
}

// WithRootCAIncludeSystem specifies if the system CA should be included in the root CA.
func WithRootCAIncludeSystem(includeSystem bool) TLSConfigManagerOpt {
	return func(cp *tlsConfigManagerConfig) {
		cp.rootCAConfig.setIncludeSystem(includeSystem)
	}
}

// WithRootCAFiles specifies a list of paths to PEM files that contain root CAs.
func WithRootCAFiles(pemFiles ...string) TLSConfigManagerOpt {
	return func(cp *tlsConfigManagerConfig) {
		cp.rootCAConfig.addFiles(pemFiles...)
	}
}

// WithClientCAIncludeSystem specifies if the system CA should be included in the client CA for client authentication.
func WithClientCAIncludeSystem(includeSystem bool) TLSConfigManagerOpt {
	return func(cp *tlsConfigManagerConfig) {
		cp.clientCAConfig.setIncludeSystem(includeSystem)
	}
}

// WithClientCAFiles specifies a list of paths to PEM files that contain root CA for client authentication.
func WithClientCAFiles(pemFiles ...string) TLSConfigManagerOpt {
	return func(cp *tlsConfigManagerConfig) {
		cp.clientCAConfig.addFiles(pemFiles...)
	}
}

// WithClientAuth specifies the type TLS client authentication a server should perform.
func WithClientAuth(auth tls.ClientAuthType) TLSConfigManagerOpt {
	return func(cp *tlsConfigManagerConfig) {
		cp.clientAuth = auth
	}
}

// WithExpirationAdvanced sets the how far ahead the underlying CertLoader will
// warn about a certificate that is about to expire.
func WithExpirationAdvanced(d time.Duration) TLSConfigManagerOpt {
	return func(cp *tlsConfigManagerConfig) {
		cp.addCertLoaderOpt(WithCertLoaderExpirationAdvanced(d))
	}
}

// WithCertificateCheckInterval sets how often to check for certificate expiration.
func WithCertificateCheckInterval(d time.Duration) TLSConfigManagerOpt {
	return func(cl *tlsConfigManagerConfig) {
		cl.addCertLoaderOpt(WithCertLoaderCertificateCheckInterval(d))
	}
}

// WithLogger assigns a logger for to use.
func WithLogger(logger *zap.Logger) TLSConfigManagerOpt {
	return func(cl *tlsConfigManagerConfig) {
		cl.addCertLoaderOpt(WithCertLoaderLogger(logger))
	}
}

// WithIgnoreFilePermissions ignores file permissions when loading certificates.
func WithIgnoreFilePermissions(ignore bool) TLSConfigManagerOpt {
	return func(cl *tlsConfigManagerConfig) {
		cl.addCertLoaderOpt(WithCertLoaderIgnoreFilePermissions(ignore))
	}
}

// newTLSConfigManager returns a TLSConfigManager configured by opts.
func newTLSConfigManager(opts ...TLSConfigManagerOpt) (*TLSConfigManager, error) {
	c := tlsConfigManagerConfig{}
	for _, o := range opts {
		o(&c)
	}

	// Create and setup base tls.Config
	var tlsConfig *tls.Config
	var certLoader *TLSCertLoader
	if c.useTLS {
		// Create / clone TLS configuration as necessary.
		tlsConfig = c.baseConfig.Clone() // nil configs are clonable.
		if tlsConfig == nil {
			tlsConfig = new(tls.Config)
		}

		// Modify configuration.
		tlsConfig.InsecureSkipVerify = c.allowInsecure

		// Only overwrite default value of ClientAuth.
		if tlsConfig.ClientAuth == tls.NoClientCert {
			tlsConfig.ClientAuth = c.clientAuth
		}

		// Setup CA pools.
		setupPool := func(pool **x509.CertPool, c caConfig) error {
			if p, err := c.newCertPool(); err != nil {
				return err
			} else if p != nil {
				// Don't overwrite an existing pool from baseConfig with a nil pool.
				*pool = p
			}
			return nil
		}

		if err := setupPool(&tlsConfig.RootCAs, c.rootCAConfig); err != nil {
			return nil, fmt.Errorf("error creating root CA pool: %w", err)
		}
		if err := setupPool(&tlsConfig.ClientCAs, c.clientCAConfig); err != nil {
			return nil, fmt.Errorf("error creating client CA pool: %w", err)
		}

		// Create TLSCertLoader and configure it.
		// Create TLSCertLoader and configure tlsConfig to use it. No loader is created if no cert
		// is provided. This is useful for client-only use cases.
		if c.certPath != "" || c.keyPath != "" {
			if cl, err := NewTLSCertLoader(c.certPath, c.keyPath, c.certLoaderOpts...); err != nil {
				return nil, err
			} else {
				certLoader = cl
			}
			certLoader.SetupTLSConfig(tlsConfig)
		}
	}

	return &TLSConfigManager{
		tlsConfig:  tlsConfig,
		certLoader: certLoader,
	}, nil
}

// NewTLSConfigManager returns a TLSConfigManager with the given configuration. If useTLS is true,
// then the certificate is loaded immediately if specified and the tls.Config instantiated.
// If no certPath and no keyPath is provided, then no TLSCertLoader is created. For this case, the returned
// TLSConfigManager can be used for client operations (e.g. Dial), but not for server operations (e.g. Listen).
// The allowInsecure parameter has no effect on server operations.
func NewTLSConfigManager(useTLS bool, baseConfig *tls.Config, certPath, keyPath string, allowInsecure bool, opts ...TLSConfigManagerOpt) (*TLSConfigManager, error) {
	// Values from explicit parameters should override values set in opts.
	co := make([]TLSConfigManagerOpt, 0, len(opts)+4)
	co = append(co, opts...)
	co = append(co,
		WithUseTLS(useTLS),
		WithBaseConfig(baseConfig),
		WithCertificate(certPath, keyPath),
		WithAllowInsecure(allowInsecure))
	return newTLSConfigManager(co...)
}

// NewClientTLSConfigManager creates a TLSConfigManager that is only useful for clients without
// client certificates. TLS is enabled when useTLS is true. Certificate verification is skipped
// when allowInsecure is true.
// This is convenience wrapper for NewTLSConfigManager(useTLS, baseConfig, "", "", allowInsecure).
func NewClientTLSConfigManager(useTLS bool, baseConfig *tls.Config, allowInsecure bool, opts ...TLSConfigManagerOpt) (*TLSConfigManager, error) {
	co := make([]TLSConfigManagerOpt, 0, len(opts)+3)
	co = append(co, opts...)
	co = append(co,
		WithUseTLS(useTLS),
		WithBaseConfig(baseConfig),
		WithAllowInsecure(allowInsecure))
	return newTLSConfigManager(co...)
}

// NewDisabledTLSConfigManager creates a TLSConfigManager that has TLS disabled.
// This is a convenience function equivalent to NewTLSConfigManager(false, nil, "", "", false).
// In addition to being more concise, NewDisabledTLSConfigManager can not return an error.
func NewDisabledTLSConfigManager() *TLSConfigManager {
	return &TLSConfigManager{}
}

// TLSConfig returns a tls.Config for use with dial and listen functions. When TLS is disabled the return is nil.
// The returned tls.Config is a clone and does not need to be cloned again.
func (cm *TLSConfigManager) TLSConfig() *tls.Config {
	// Clone returns nil for a nil tlsConfig
	return cm.tlsConfig.Clone()
}

// TLSCertLoader returns the certificate loader for this TLSConfigManager. When no certificate is provided
// the return value is nil.
func (cm *TLSConfigManager) TLSCertLoader() *TLSCertLoader {
	return cm.certLoader
}

// UseTLS returns true if this TLSConfigManager is configured to use TLS. It is a convenience wrapper
// around TLSConfig.
func (cm *TLSConfigManager) UseTLS() bool {
	// Don't use TLSConfig() to avoid cloning a tlsConfig only to throw it away.
	return cm.tlsConfig != nil
}

// Return a net.Listener for network and address based on current configuration.
func (cm *TLSConfigManager) Listen(network, address string) (net.Listener, error) {
	if tlsConfig := cm.TLSConfig(); tlsConfig != nil {
		return tls.Listen(network, address, tlsConfig)
	} else {
		return net.Listen(network, address)
	}
}

// Dial a remote for network and addressing using the current configuration.
func (cm *TLSConfigManager) Dial(network, address string) (net.Conn, error) {
	if tlsConfig := cm.TLSConfig(); tlsConfig != nil {
		return tls.Dial(network, address, tlsConfig)
	} else {
		return net.Dial(network, address)
	}
}

// Dial a remote for network and addressing using the given dialer and current configuration.
func (cm *TLSConfigManager) DialWithDialer(dialer *net.Dialer, network, address string) (net.Conn, error) {
	if tlsConfig := cm.TLSConfig(); tlsConfig != nil {
		return tls.DialWithDialer(dialer, network, address, tlsConfig)
	} else {
		return dialer.Dial(network, address)
	}
}

// PrepareCertificateLoad is a wrapper for the TLSCertLoader's PrepareLoad method. If TLS is not
// enabled, then a NOP callback is returned.
func (cm *TLSConfigManager) PrepareCertificateLoad(certPath, keyPath string) (func() error, error) {
	if !cm.UseTLS() {
		return func() error { return nil }, nil
	}

	if certLoader := cm.TLSCertLoader(); certLoader != nil {
		return certLoader.PrepareLoad(certPath, keyPath)
	} else {
		return nil, ErrNoCertLoader
	}
}

// Close closes the underlying TLSCertLoader, if present. This is safe to call multiple times.
func (cm *TLSConfigManager) Close() error {
	if cm.certLoader != nil {
		return cm.certLoader.Close()
	}
	return nil
}
