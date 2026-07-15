package tlsconfig

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"

	"go.uber.org/zap"
)

const (
	// logCertContext is the key for log context with the certificate usages list.
	// Use literals instead of constants in tests to ensure no one changes
	// the constant unintentionally.
	logUsagesContext = "usages"

	// logCertContext is the key for log context with the certificate usage (singular).
	// Use literals instead of constants in tests to ensure no one changes
	// the constant unintentionally.
	logUsageContext = "usage"
)

var (
	// ErrConfigureDisabledManager is returned when an attempt is made to reconfigure
	// a disabled config managaer.
	ErrConfigureDisabledManager = errors.New("cannot configure disabled TLS manager")

	// ErrClientListen is returned when attempt is made to have a client role manager
	// create a listener.
	ErrClientListen = errors.New("client TLS manager cannot Listen")

	// ErrNoCertLoader indicates that an operation requiring a TLSCertLoader did not have one available.
	// This can happen if the TLSConfigManager was created without a certificate for client-side use only.
	ErrNoCertLoader = errors.New("no TLSCertLoader available")

	// ErrNoRole indicates that a configuration manager or other object was not initialized
	// with a valid Role. It is generally due to a misuse of an internal API.
	ErrNoRole = errors.New("no role specified for TLS certificate")

	// ErrNotSupportedServer indicates that an operation is not supported by a server role
	// config manager.
	ErrNotSupportedServer = errors.New("operation not supported by server role TLS manager")

	// ErrServerDial is returned when attempt is made to have a server role manager
	// dial a connection.
	ErrServerDial = errors.New("server TLS manager cannot Dial / DialWithDialer")
)

// Role is an enum that specifies how a config manager or cert loader
// will be used.
type Role int

const (
	// InvalidRole is an invalid role. It is the zero value so IsValid can be used
	// to determine if a role was properly initialized.
	InvalidRole Role = iota

	// ServerOnlyRole specifies that a config manager or certificate is only for
	// server use through Listen.
	ServerOnlyRole

	// ClientOnlyRole specifies that a config manager or certificate is only for
	// client use through Dial.
	ClientOnlyRole

	// ServerAndClientRole specifies that a config manager or certificate is for
	// use for both a server (through Listen) and a client (through Dial).
	ServerAndClientRole
)

// IsValid returns true if r is valid. It can be used to determine if r
// was initialized properly.
func (r Role) IsValid() bool {
	return r != InvalidRole
}

// IsSingleRole returns true if role specifies a single role, Server or Client.
// This is used to check for valid values in places where only a single role
// is accepted.
func (r Role) IsSingleRole() bool {
	return r == ServerOnlyRole || r == ClientOnlyRole
}

// IsServerRole returns true if role specifies a server role.
func (r Role) IsServerRole() bool {
	return r == ServerOnlyRole || r == ServerAndClientRole
}

// IsClientRole returns true if role specifies a client role.
func (r Role) IsClientRole() bool {
	return r == ClientOnlyRole || r == ServerAndClientRole
}

// TLSConfigManager will manage a TLS configuration and make sure that only one instance of its tls.Config exists.
// Different TLSConfigManager objects will have different configurations, even if they are instantiated in exactly
// the same way. No struct member is modified once the NewTLSConfigManager constructor is finished.
type TLSConfigManager struct {
	// Fields above mu are not protected by mu and can only be set at construction time.

	// disabled indicates if this config manager is disabled. A disabled configuration manager has
	// TLS disabled and does not support reconfiguration. A disabled configuration manager
	// is mainly used by tests.
	disabled bool

	// role is the specified role for this config manager.
	role Role

	// usage is the descriptive usage for this config manager.
	usage string

	// serverCertLoader is the cert loader for server certificates. It is only
	// created if role is a server role. It is only modified on instantiation and
	// does not require a mutex because it has its own internal locking.
	serverCertLoader *TLSCertLoader

	// clientCertLoader is the cert loader for client certificates. It is only
	// created if role is a client role. It is only modified on instantiation and
	// does not require a mutex because it has its own internal locking.
	clientCertLoader *TLSCertLoader

	// mu protects all fields below.
	mu sync.RWMutex

	// config is the currently configured tlsConfigManagerConfig.
	config *tlsConfigManagerConfig

	// tlsConfig is the tls.Config object returned when needed.
	tlsConfig *tls.Config
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
	// monitor is the certificate monitor the cert loaders use.
	monitor *TLSCertMonitor

	// role is the Role for this config manager. All enum values are allowed.
	role Role

	// usage is the descriptive usage for this config manager.
	usage string

	// logger is the logger to use for this config manager and its cert loaders.
	logger *zap.Logger

	// useTLS indicates if TLS should be used. If not, the rest of the configuration is ignored.
	useTLS bool

	// baseConfig is the *tls.Config to use as the basis for the manager's *tls.Config.
	baseConfig *tls.Config

	// serverCertPath is the path to the server certificate. It is also used a s fallback for
	// the client certificate.
	serverCertPath string

	// serverKeyPath is the path to the server private key. It is also used a fallback for
	// the client private key.
	serverKeyPath string

	// clientCertPath is the path to the client certificate. certPath is used as a
	// fallback if both clientCertPath and clientKeyPath are unset.
	clientCertPath string

	// clientKeyPath is the path to the client private key. keyPath is used as a fallback
	// if both clientCertPath and clientKeyPath are unset.
	clientKeyPath string

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

	// ignoreFilePermissions indicates if cert loaders should ignore file permissions.
	ignoreFilePermissions bool
}

// commonCertLoaderOpts returns a common list of options for the TLSCertLoader.
func (c *tlsConfigManagerConfig) commonCertLoaderOpts() []TLSCertLoaderOpt {
	return []TLSCertLoaderOpt{
		WithCertLoaderLogger(c.logger),
		WithCertLoaderIgnoreFilePermissions(c.ignoreFilePermissions),
	}
}

// serverCertLoaderOpts returns the options needed for the server TLSCertLoader.
func (c *tlsConfigManagerConfig) serverCertLoaderOpts() []TLSCertLoaderOpt {
	return append(c.commonCertLoaderOpts(),
		WithCertLoaderCertificate(c.serverCertPath, c.serverKeyPath),
	)
}

// clientCertLoaderOpts returns the options needed for the client TLSCertLoader.
func (c *tlsConfigManagerConfig) clientCertLoaderOpts() []TLSCertLoaderOpt {
	certPath := c.clientCertPath
	keyPath := c.clientKeyPath
	if certPath == "" && keyPath == "" {
		certPath = c.serverCertPath
		keyPath = c.serverKeyPath
	}
	return append(c.commonCertLoaderOpts(),
		WithCertLoaderCertificate(certPath, keyPath),
	)
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

// WithServerCertificate sets the config manager's server certificate and private
// key path. These will also be used as fallbacks for a client if no client
// certificate is configured.
func WithServerCertificate(certPath, keyPath string) TLSConfigManagerOpt {
	return func(cp *tlsConfigManagerConfig) {
		cp.serverCertPath = certPath
		cp.serverKeyPath = keyPath
	}
}

// WithServerCertificate sets the config manager's client certificate and private
// key path. These will also be used as fallbacks for a client if no client
func WithClientCertificate(certPath, keyPath string) TLSConfigManagerOpt {
	return func(cp *tlsConfigManagerConfig) {
		cp.clientCertPath = certPath
		cp.clientKeyPath = keyPath
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

// WithLogger assigns a logger for to use.
func WithLogger(logger *zap.Logger) TLSConfigManagerOpt {
	return func(cl *tlsConfigManagerConfig) {
		cl.logger = logger
	}
}

// WithIgnoreFilePermissions ignores file permissions when loading certificates.
func WithIgnoreFilePermissions(ignore bool) TLSConfigManagerOpt {
	return func(cl *tlsConfigManagerConfig) {
		cl.ignoreFilePermissions = ignore
	}
}

// WithUsage sets the config manager descriptive usage.
func WithUsage(usage string) TLSConfigManagerOpt {
	return func(c *tlsConfigManagerConfig) {
		c.usage = usage
	}
}

// withMonitor sets the TLSCertMonitor for a config manager to use. It can only
// be set at construction time. This is an internal function because the public
// constructors have it as a required positional parameter, which is then
// converted to a TLSConfigManagerOpt internally.
func withMonitor(monitor *TLSCertMonitor) TLSConfigManagerOpt {
	return func(cl *tlsConfigManagerConfig) {
		cl.monitor = monitor
	}
}

// withRole sets the role for this config manager. The role impacts what
// configurations are required and what operations are allowed.
func withRole(role Role) TLSConfigManagerOpt {
	return func(cl *tlsConfigManagerConfig) {
		cl.role = role
	}
}

// ErrCATrustsNothing is returned by resolveCA when a configured CA pool would
// trust no certificates. Callers wrap it with root/client context.
var ErrCATrustsNothing = errors.New("trusts no certificates: set paths or enable include-system")

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
		return caConfig{}, ErrCATrustsNothing
	}
	return cc.customCAConfig(), nil
}

// newTLSConfigManager returns a TLSConfigManager configured by opts.
func newTLSConfigManager(opts ...TLSConfigManagerOpt) (*TLSConfigManager, error) {
	c := tlsConfigManagerConfig{}
	for _, o := range opts {
		o(&c)
	}

	// Check for configuration errors.
	if !c.role.IsValid() {
		return nil, fmt.Errorf("newTLSConfigManager: %w", ErrNoRole)
	}
	if c.monitor == nil {
		return nil, fmt.Errorf("newTLSConfigManager: %w", ErrNoCertificateMonitor)
	}

	// Create certificate loaders.
	var serverCertLoader *TLSCertLoader
	if c.role.IsServerRole() {
		cl, err := NewTLSCertLoader(
			ServerOnlyRole,
			c.monitor,
			append(c.commonCertLoaderOpts(), WithCertLoaderUsage(c.usage+".server"))...)
		if err != nil {
			return nil, fmt.Errorf("error creating server TLS certificate loader: %w", err)
		}
		serverCertLoader = cl
	}

	var clientCertLoader *TLSCertLoader
	if c.role.IsClientRole() {
		cl, err := NewTLSCertLoader(
			ClientOnlyRole,
			c.monitor,
			append(c.commonCertLoaderOpts(), WithCertLoaderUsage(c.usage+".client"))...)
		if err != nil {
			return nil, fmt.Errorf("error creating client TLS certificate loader: %w", err)
		}
		clientCertLoader = cl
	}

	cm := &TLSConfigManager{
		role:             c.role,
		usage:            c.usage,
		serverCertLoader: serverCertLoader,
		clientCertLoader: clientCertLoader,
	}

	if apply, err := cm.prepareConfigure(&c); err != nil {
		return nil, fmt.Errorf("error configuring new TLSConfigManager: %w", err)
	} else if err := apply(); err != nil {
		return nil, fmt.Errorf("error applying configuration to a new TLSConfigManager: %w", err)
	}

	return cm, nil
}

// prepareConfigure changes the configuration of cm based on
func (cm *TLSConfigManager) prepareConfigure(c *tlsConfigManagerConfig) (func() error, error) {
	// Create and setup base tls.Config
	var tlsConfig *tls.Config

	if cm.disabled {
		return nil, ErrConfigureDisabledManager
	}
	if !c.useTLS {
		return func() error {
			if cm.clientCertLoader != nil {
				cm.clientCertLoader.Clear()
			}
			if cm.serverCertLoader != nil {
				cm.serverCertLoader.Clear()
			}
			cm.mu.Lock()
			defer cm.mu.Unlock()
			cm.config = c
			cm.tlsConfig = nil

			return nil
		}, nil
	}

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
		return nil, fmt.Errorf("%s: root CA configuration %w", cm.usage, err)
	}
	clientCAConfig, err := resolveCA(c.clientCA)
	if err != nil {
		return nil, fmt.Errorf("%s: client CA configuration %w", cm.usage, err)
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
		return nil, fmt.Errorf("%s: error creating root CA pool: %w", cm.usage, err)
	}
	if err := setupPool(&tlsConfig.ClientCAs, clientCAConfig); err != nil {
		return nil, fmt.Errorf("%s: error creating client CA pool: %w", cm.usage, err)
	}

	// Prepare certificate load for server certificate loader.
	var applyServerCert func() error
	if cm.serverCertLoader != nil {
		if apply, err := cm.serverCertLoader.PrepareLoad(c.serverCertLoaderOpts()...); err != nil {
			return nil, fmt.Errorf("%s: error configuring server cert loader: %w", cm.usage, err)
		} else {
			applyServerCert = apply
		}
	}

	// Prepare certificate load for server certificate loader.
	var applyClientCert func() error
	if cm.clientCertLoader != nil {
		if apply, err := cm.clientCertLoader.PrepareLoad(c.clientCertLoaderOpts()...); err != nil {
			return nil, fmt.Errorf("%s: error configuring client cert loader: %w", cm.usage, err)
		} else {
			applyClientCert = apply
		}
	}

	return func() error {
		if applyServerCert != nil {
			if err := applyServerCert(); err != nil {
				return fmt.Errorf("%s: error applying server certificate: %w", cm.usage, err)
			}
			cm.serverCertLoader.SetupTLSConfig(tlsConfig)
		}

		if applyClientCert != nil {
			if err := applyClientCert(); err != nil {
				return fmt.Errorf("%s: error applying client certificate: %w", cm.usage, err)
			}
			cm.clientCertLoader.SetupTLSConfig(tlsConfig)
		}

		cm.mu.Lock()
		defer cm.mu.Unlock()
		cm.config = c
		cm.tlsConfig = tlsConfig

		return nil
	}, nil

}

// NewServerTLSConfigManager returns a TLSConfigManager for a given set options for server-only
// use.
//
// Previously, many options were required parameters to this function. They are still
// required, but given using With*() parameters. This has the advantaged of allowing
// a single function to convert a TOML configuration to a slice of With* parameters for
// both construction and re-configuration. It does make missing an option a run-time
// error instead of a compile-time error.
//
// The following are require options. It is an error if they are missing.
// - WithUseTLS
// - WithBaseConfig
//
// If WithUseTLS(true) is given, then WithCertificate must also be given.
//
// All options given as direct positional parameters are required and can not be changed
// after construction.
//
// The returned TLSConfigManager can be used for server operations (e.g. Listen), but not for client operations (e.g. Dial).
func NewServerTLSConfigManager(monitor *TLSCertMonitor, opts ...TLSConfigManagerOpt) (*TLSConfigManager, error) {
	// Values from explicit parameters should override values set in opts.
	opts = append(opts, withMonitor(monitor), withRole(ServerOnlyRole))
	return newTLSConfigManager(opts...)
}

// NewClientTLSConfigManager creates a TLSConfigManager that can only be used for clients.
// See NewTLSConfigManager for further information on options.
func NewClientTLSConfigManager(monitor *TLSCertMonitor, opts ...TLSConfigManagerOpt) (*TLSConfigManager, error) {
	opts = append(opts, withMonitor(monitor), withRole(ClientOnlyRole))
	return newTLSConfigManager(opts...)
}

// NewClientServerTLSConfigManager creates a config manager that can be used for both
// client and server operations. See NewServerTLSConfig for further information on options.
func NewClientServerTLSConfigManager(monitor *TLSCertMonitor, opts ...TLSConfigManagerOpt) (*TLSConfigManager, error) {
	opts = append(opts, withMonitor(monitor), withRole(ServerAndClientRole))
	return newTLSConfigManager(opts...)
}

// NewDisabledTLSConfigManager creates a TLSConfigManager that has TLS disabled. A disabled
// config manager can not be reconfigured to enable TLS later. It is primarily useful for tests
// that do not require TLS.
func NewDisabledTLSConfigManager() *TLSConfigManager {
	return &TLSConfigManager{disabled: true}
}

// TLSConfig returns a tls.Config for use with dial and listen functions. When TLS is disabled the return is nil.
// The returned tls.Config is a clone and does not need to be cloned again.
func (cm *TLSConfigManager) TLSConfig() *tls.Config {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	// Clone returns nil for a nil tlsConfig
	return cm.tlsConfig.Clone()
}

// UseTLS returns true if this TLSConfigManager is configured to use TLS. It is a convenience wrapper
// around TLSConfig.
func (cm *TLSConfigManager) UseTLS() bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	// Don't use TLSConfig() to avoid cloning a tlsConfig only to throw it away.
	return cm.tlsConfig != nil
}

// Return a net.Listener for network and address based on current configuration.
func (cm *TLSConfigManager) Listen(network, address string) (net.Listener, error) {
	if !cm.role.IsServerRole() {
		return nil, fmt.Errorf("%s: %w", cm.usage, ErrClientListen)
	}

	if tlsConfig := cm.TLSConfig(); tlsConfig != nil {
		return tls.Listen(network, address, tlsConfig)
	} else {
		return net.Listen(network, address)
	}
}

// Dial a remote for network and addressing using the current configuration.
func (cm *TLSConfigManager) Dial(network, address string) (net.Conn, error) {
	if !cm.role.IsClientRole() {
		return nil, fmt.Errorf("%s: %w", cm.usage, ErrServerDial)
	}

	if tlsConfig := cm.TLSConfig(); tlsConfig != nil {
		return tls.Dial(network, address, tlsConfig)
	} else {
		return net.Dial(network, address)
	}
}

// Dial a remote for network and addressing using the given dialer and current configuration.
func (cm *TLSConfigManager) DialWithDialer(dialer *net.Dialer, network, address string) (net.Conn, error) {
	if !cm.role.IsClientRole() {
		return nil, fmt.Errorf("%s: %w", cm.usage, ErrServerDial)
	}

	if tlsConfig := cm.TLSConfig(); tlsConfig != nil {
		return tls.DialWithDialer(dialer, network, address, tlsConfig)
	} else {
		return dialer.Dial(network, address)
	}
}

// copyCurrentConfig creates a copy of the current configuration.
func (cm *TLSConfigManager) copyCurrentConfig() *tlsConfigManagerConfig {
	// A shallow copy is sufficient because the options all overwrite fields
	// instead modifying a field in-place.
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	c := &tlsConfigManagerConfig{}
	*c = *cm.config
	return c
}

// PrepareReconfigure creates an apply function for a new configuration of the configuration manager.
func (cm *TLSConfigManager) PrepareReconfigure(opts ...TLSConfigManagerOpt) (func() error, error) {
	// Use the current configuration. If there are no opts that set a field, then the
	// current setting will continue to be used.
	c := cm.copyCurrentConfig()
	for _, o := range opts {
		o(c)
	}

	// We cannot currently change some options when the server role is used.
	if cm.role.IsServerRole() {
		if c.useTLS != cm.UseTLS() {
			return nil, fmt.Errorf("%s: changing TLS enabled to %t: %w", cm.usage, c.useTLS, ErrNotSupportedServer)
		}
	}

	return cm.prepareConfigure(c)
}

// Close closes the underlying TLSCertLoader, if present. This is safe to call multiple times.
func (cm *TLSConfigManager) Close() error {
	var allErrs []error

	if cm.serverCertLoader != nil {
		if err := cm.serverCertLoader.Close(); err != nil {
			allErrs = append(allErrs, fmt.Errorf("%s: error closing server cert loader: %w", cm.usage, err))
		}
	}
	if cm.clientCertLoader != nil {
		if err := cm.clientCertLoader.Close(); err != nil {
			allErrs = append(allErrs, fmt.Errorf("%s: error closing client cert loader: %w", cm.usage, err))
		}
	}

	return errors.Join(allErrs...)
}
