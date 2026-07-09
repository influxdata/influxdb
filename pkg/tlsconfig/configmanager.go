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

// CAConfig configures a CA certificate pool: the PEM files to trust and whether
// to also include the host's system CA pool. It is used for both root CAs
// (verifying peer server certificates) and client CAs (verifying client
// certificates during client authentication).
//
// It is designed to be embedded in configuration structs as a *CAConfig so that
// "not configured" is distinguishable from "configured": a nil pointer leaves
// the base TLS config's pool in place (for root CAs, that means Go's implicit
// system roots), while a non-nil value is used exactly as given. In particular,
// a non-nil config with Paths but without IncludeSystem trusts only those
// paths, because IncludeSystem's zero value is the correct default once the
// user has configured a pool.
type CAConfig struct {
	// Paths are the PEM files whose certificates are added to the pool.
	Paths []string `toml:"paths"`

	// IncludeSystem includes the host's system CA pool in addition to Paths.
	IncludeSystem bool `toml:"include-system"`
}

// hasTrustAnchors reports whether the config would verify against any
// certificates. A config with no paths and no system pool trusts nothing and
// cannot verify a peer.
func (cc *CAConfig) hasTrustAnchors() bool {
	return len(cc.Paths) > 0 || cc.IncludeSystem
}

// customCAConfig returns an internal caConfig that builds a pool from cc.
func (cc *CAConfig) customCAConfig() caConfig {
	return caConfig{custom: true, includeSystem: cc.IncludeSystem, files: cc.Paths}
}

// caConfig is the internal, resolved CA configuration used to build a pool.
type caConfig struct {
	// custom indicates a certificate pool should be built. When false,
	// newCertPool returns nil so the base *tls.Config is left as-is.
	custom bool

	// includeSystem indicates if the system CA pool should be included.
	includeSystem bool

	// files lists paths to PEM files to include in the pool.
	files []string
}

// newCertPool returns a x509.CertPool for the configuration in c. If c is not a custom config, then
// nil is returned.
func (c *caConfig) newCertPool() (*x509.CertPool, error) {
	// Only create a CertPool for a custom CA config that has either a CA certificate list or
	// explicitly includes the system CA. A "custom CA" without any certificates
	// is almost certainly from default config values.
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

	// rootCA configures the CA pool used to verify peer certificates. A nil
	// value leaves the base config's roots (and Go's implicit system pool).
	rootCA *CAConfig

	// clientCA configures the CA pool used to verify client certificates during
	// client authentication. A nil value under client auth uses the system pool.
	clientCA *CAConfig

	// clientAuth indicates the type of ClientAuth required by a server. A nil
	// value means it was not configured and the base config's ClientAuth is
	// left in place; a non-nil value overrides it, even with the zero value
	// (tls.NoClientCert).
	clientAuth *tls.ClientAuthType

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

// WithRootCA configures the CA pool used to verify peer (server) certificates.
// A nil config leaves the base config's roots in place (Go's implicit system
// pool). A non-nil config is used as-is; one that trusts no certificates is an
// error at construction unless insecure connections are allowed.
func WithRootCA(cc *CAConfig) TLSConfigManagerOpt {
	return func(cp *tlsConfigManagerConfig) {
		cp.rootCA = cc
	}
}

// WithClientCA configures the CA pool used to verify client certificates during
// client authentication. A nil config leaves the base config's client pool in
// place; a non-nil config is validated and built into a pool whether or not
// client authentication is enabled via WithClientAuth, and one that trusts no
// certificates is an error at construction. The pool is only actually used to
// verify clients when client authentication is enabled.
func WithClientCA(cc *CAConfig) TLSConfigManagerOpt {
	return func(cp *tlsConfigManagerConfig) {
		cp.clientCA = cc
	}
}

// WithClientAuth specifies the type of TLS client authentication a server
// should perform. When used, it overrides the base config's ClientAuth with
// auth, even if auth is the zero value (tls.NoClientCert).
func WithClientAuth(auth tls.ClientAuthType) TLSConfigManagerOpt {
	return func(cp *tlsConfigManagerConfig) {
		cp.clientAuth = &auth
	}
}

// WithClientAuthPtr specifies the type of TLS client authentication a server
// should perform, allowing "not configured" to be distinguished from an
// explicit value. When clientAuthPtr is nil the base config's ClientAuth is left
// in place; when it is non-nil the base config's ClientAuth is overridden with
// *clientAuthPtr, even if that is the zero value (tls.NoClientCert).
func WithClientAuthPtr(clientAuthPtr *tls.ClientAuthType) TLSConfigManagerOpt {
	return func(cp *tlsConfigManagerConfig) {
		if clientAuthPtr != nil {
			auth := *clientAuthPtr
			cp.clientAuth = &auth
		}
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

// errCATrustsNothing is returned by resolveCA when a configured CA pool would
// trust no certificates. Callers wrap it with root/client context.
var errCATrustsNothing = errors.New("trusts no certificates: set paths or enable include-system")

// resolveCA resolves a user-facing *CAConfig into an internal pool config,
// shared by root and client CAs. A nil config leaves the base config's pool in
// place (for root CAs, that means Go's implicit system roots). A non-nil config
// is always validated and built into a pool, regardless of how (or whether) the
// pool will be used, so a misconfigured CA is reported at construction; one that
// trusts no certificates returns errCATrustsNothing for the caller to wrap with
// the appropriate root/client context.
func resolveCA(cc *CAConfig) (caConfig, error) {
	if cc == nil {
		return caConfig{}, nil // leave the base config's pool
	}
	if !cc.hasTrustAnchors() {
		return caConfig{}, errCATrustsNothing
	}
	return cc.customCAConfig(), nil
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

		// Override ClientAuth only when it was explicitly configured; otherwise
		// leave the base config's value in place.
		if c.clientAuth != nil {
			tlsConfig.ClientAuth = *c.clientAuth
		}

		// Resolve the user-facing *CAConfig values into internal pool configs,
		// applying the not-configured defaults and rejecting configurations
		// that trust no certificates. See CAConfig for the nil/non-nil semantics.
		rootCAConfig, err := resolveCA(c.rootCA)
		if err != nil {
			return nil, fmt.Errorf("root CA configuration %w", err)
		}
		clientCAConfig, err := resolveCA(c.clientCA)
		if err != nil {
			return nil, fmt.Errorf("client CA configuration %w", err)
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

		if err := setupPool(&tlsConfig.RootCAs, rootCAConfig); err != nil {
			return nil, fmt.Errorf("error creating root CA pool: %w", err)
		}
		if err := setupPool(&tlsConfig.ClientCAs, clientCAConfig); err != nil {
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
