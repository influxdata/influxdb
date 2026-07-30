package tlsconfig

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/influxdata/influxdb/logger"
	"go.uber.org/zap"
)

const (
	// CertMaxPermissions is the maximum permissions allowed for the certificate file.
	CertMaxPermissions = 0644

	// KeyMaxPermissions is the maximum permissions allowed for the key file.
	KeyMaxPermissions = 0600

	// DefaultExpirationWarnTime is the default advanced warning to give for expiring certificates.
	DefaultExpirationWarnTime = 5 * (24 * time.Hour)

	// DefaultCertificateCheckTime is the default duration between certificate checks.
	DefaultCertificateCheckTime = time.Hour
)

var (
	ErrCertificateNil            = errors.New("TLS certificate is nil")
	ErrCertificateEmpty          = errors.New("TLS certificate is empty")
	ErrCertificateInvalid        = errors.New("TLS certificate is invalid")
	ErrCertificateNotServerAuth  = errors.New("TLS certificate does not permit server authentication")
	ErrCertificateRequestInfoNil = errors.New("CertificateRequestInfo is nil")
	ErrLoadedCertificateInvalid  = errors.New("LoadedCertificate is invalid")
	ErrNoCertificateMonitor      = errors.New("no certificate monitor")
	ErrPathEmpty                 = errors.New("empty path")
	ErrSingleRoleRequired        = errors.New("single role required (Server or Client)")
)

// TLSCertLoader handles loading TLS certificates, providing them to a tls.Config, and
// monitoring the certificate for expiration.
type TLSCertLoader struct {
	// All fields before mu can only be set at construction time.

	// role specifies how a certificate will be used. Since a TLSCertLoader
	// only handles a server or client certificate but not both, Server and
	// Client are the only accepted values. ServerAndClient is an error.
	role Role

	// monitor is the certificate monitor for loader.
	monitor *TLSCertMonitor

	// logger is the logger used for logging status. It can only be
	// set at construction time using WithCertLoaderLogger.
	logger *zap.Logger

	// usage is the descriptive usage string for logging. It can only
	// be set at construction time using WIthCertLoaderUsage.
	usage string

	// mu protects all members below. All fields below can be set at construction
	// time or with Reconfigure.
	mu sync.RWMutex

	// cert is the currently active certificate.
	cert LoadedCertificate

	// config is the current configuration of the loader.
	config *tlsCertLoaderConfig
}

// tlsCertLoaderConfig holds configuration data for TLSCertLoader. It is the actual
// struct loaded by the WithCertLoader* functions.
type tlsCertLoaderConfig struct {
	// certPath is the certificate path to load.
	certPath string

	// keyPath is the key path to load.
	keyPath string

	// usage is the descriptive usage of the cert loader.
	usage string

	// ignoreFilePermissions is true if file permission checks should be bypassed
	// when loading certificates.
	ignoreFilePermissions bool

	// ignoreSanityChecks is true if failed certificate sanity checks should be
	// logged and overlooked instead of failing the load.
	ignoreSanityChecks bool

	// logger is the logger to use for logging. It is only applied when the cert
	// loader is constructed: a reconfiguration through PrepareLoad keeps the
	// logger the loader already has.
	logger *zap.Logger
}

// TLSCertLoaderOpt is a function to configure a TLSCertLoader.
type TLSCertLoaderOpt func(*tlsCertLoaderConfig)

// WithCertLoaderCertificate sets the certificate and key for the cert loader
// to load.
func WithCertLoaderCertificate(certPath string, keyPath string) TLSCertLoaderOpt {
	return func(c *tlsCertLoaderConfig) {
		c.certPath = certPath
		c.keyPath = keyPath
	}
}

// WithCertLoaderLogger assigns a logger to use. It only takes effect when given
// to NewTLSCertLoader; a loader keeps its original logger through a PrepareLoad.
func WithCertLoaderLogger(logger *zap.Logger) TLSCertLoaderOpt {
	return func(c *tlsCertLoaderConfig) {
		c.logger = logger
	}
}

// WithCertLoaderUsage assigns the descriptive usage of the cert loader.
func WithCertLoaderUsage(usage string) TLSCertLoaderOpt {
	return func(c *tlsCertLoaderConfig) {
		c.usage = usage
	}
}

// WithCertLoaderIgnoreFilePermissions skips file permission checking when loading certificates.
func WithCertLoaderIgnoreFilePermissions(ignore bool) TLSCertLoaderOpt {
	return func(c *tlsCertLoaderConfig) {
		c.ignoreFilePermissions = ignore
	}
}

// WithCertLoaderIgnoreSanityChecks logs failed certificate sanity checks and
// loads the certificate anyway, instead of failing the load. It is an escape
// hatch for a certificate this package judges unusable but a deployment relies
// on; it does not relax the checks that a certificate be present and parseable,
// which are faults rather than judgments.
func WithCertLoaderIgnoreSanityChecks(ignore bool) TLSCertLoaderOpt {
	return func(c *tlsCertLoaderConfig) {
		c.ignoreSanityChecks = ignore
	}
}

// NewTLSCertLoader creates a TLSCertLoader loaded with the certificate found in certPath and keyPath.
// Only trusted input (standard configuration files) should be used for certPath and keyPath.
// If the certificate cannot be loaded, an error is returned. On success, a monitor is setup to
// periodically check the certificate for expiration.
func NewTLSCertLoader(role Role, monitor *TLSCertMonitor, opts ...TLSCertLoaderOpt) (rCertLoader *TLSCertLoader, rErr error) {
	cl := &TLSCertLoader{
		role:    role,
		monitor: monitor,
	}

	// Configure options.
	config := &tlsCertLoaderConfig{}
	for _, o := range opts {
		o(config)
	}

	// Copy some config over.
	cl.usage = config.usage
	cl.config = config

	certPath := config.certPath
	keyPath := config.keyPath

	// Check for configuration issues.
	if !cl.role.IsSingleRole() {
		return nil, fmt.Errorf("NewTLSCertLoader: usage=%q, cert=%q, key=%q: %w", cl.usage, certPath, keyPath, ErrSingleRoleRequired)
	}

	if cl.monitor == nil {
		return nil, fmt.Errorf("NewTLSCertLoader: usage=%q, cert=%q, key=%q: %w", cl.usage, certPath, keyPath, ErrNoCertificateMonitor)
	}

	// On construction we set the logger even if none was configured to ensure we have a valid logger.
	cl.setLogger(config.logger)

	// Perform initial certificate load, if needed.
	if certPath != "" || keyPath != "" {
		if err := cl.Load(certPath, keyPath); err != nil {
			return nil, fmt.Errorf("NewTLSCertLoader: usage=%q: error loading certificate: %w", cl.usage, err)
		}
	}

	// Start monitoring certificate.
	cl.monitor.registerCertLoader(cl)

	return cl, nil
}

// setLogger sets the current logger and adds context to it.
func (cl *TLSCertLoader) setLogger(logger *zap.Logger) {
	cl.logger = logger
	if cl.logger == nil {
		cl.logger = zap.NewNop()
	}

	// Add usage to logger.
	cl.logger = cl.logger.With(zap.String(logUsageContext, cl.usage))
}

// Usage is the descriptive usage set using WithCertLoaderUsage.
func (cl *TLSCertLoader) Usage() string {
	return cl.usage
}

// Clear clears the loaded certificate.
func (cl *TLSCertLoader) Clear() {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.cert = LoadedCertificate{}
	cl.config.certPath = ""
	cl.config.keyPath = ""
}

// LoadedCertificate returns the currently loaded certificate, which may be
// invalid or empty.
func (cl *TLSCertLoader) LoadedCertificate() LoadedCertificate {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	return cl.cert
}

// Certificate returns the currently loaded certificate, which may be nil.
func (cl *TLSCertLoader) Certificate() *tls.Certificate {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	return cl.cert.Certificate
}

// SetupTLSConfig modifies tlsConfig to use cl for server and client certificates.
// tlsConfig may be nil. If other fields like tlsConfig.Certificates or
// tlsConfig.NameToCertificate have been set, then cl's certificate may not be used
// as expected.
func (cl *TLSCertLoader) SetupTLSConfig(tlsConfig *tls.Config) {
	if tlsConfig == nil {
		return
	}
	if cl.LoadedCertificate().IsEmpty() {
		return
	}
	if cl.role.IsServerRole() {
		tlsConfig.GetCertificate = cl.GetCertificate
	} else if cl.role.IsClientRole() {
		tlsConfig.GetClientCertificate = cl.GetClientCertificate
	}
}

// GetCertificate is for use with a tls.Config's GetCertificate member. This allows a
// tls.Config to dynamically update its certificate when Load changes the active
// certificate.
func (cl *TLSCertLoader) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	cert := cl.Certificate()
	if cert != nil {
		return cert, nil
	} else {
		// It should be impossible to get here. If we can't load a certificate in
		// NewTLSCertLoader then we don't return the CertLoader. If we fail during Load,
		// then we keep using the currently loaded certificate.
		return nil, ErrCertificateNil
	}
}

// GetClientCertificate is for use with a tls.Config's GetClientCertificate member. This allows a
// tls.Config to dynamically update its client certificates when Load changes the active
// certificate.
func (cl *TLSCertLoader) GetClientCertificate(cri *tls.CertificateRequestInfo) (*tls.Certificate, error) {
	if cri == nil {
		return new(tls.Certificate), fmt.Errorf("tls client: %w", ErrCertificateRequestInfoNil)
	}
	cert := cl.Certificate()
	if cert == nil {
		return new(tls.Certificate), fmt.Errorf("tls client: %w", ErrCertificateNil)
	}

	// Will our certificate be accepted by server?
	if err := cri.SupportsCertificate(cert); err == nil {
		return cert, nil
	}

	// We don't have a certificate that would be accepted by the server. Don't return an error.
	// This replicates Go's behavior when tls.Config.Certificates is used instead of GetClientCertificate
	// and gives a better error on both the client and server side.
	return new(tls.Certificate), nil
}

// Leaf returns the parsed x509 certificate of the currently loaded certificate.
// If no certificate is loaded then nil is returned.
func (cl *TLSCertLoader) Leaf() *x509.Certificate {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	return cl.cert.Leaf
}

// Load loads the certificate at the given certificate path and private keyfile path.
// Only trusted input (standard configuration files) should be used for certPath and keyPath.
func (cl *TLSCertLoader) Load(certPath, keyPath string) error {
	if apply, err := cl.PrepareLoad(WithCertLoaderCertificate(certPath, keyPath)); err != nil {
		return err
	} else if err := apply(); err != nil {
		return err
	}

	return nil
}

// copyCurrentConfig creates a copy of the current configuration.
func (cl *TLSCertLoader) copyCurrentConfig() *tlsCertLoaderConfig {
	cl.mu.RLock()
	defer cl.mu.RUnlock()

	c := &tlsCertLoaderConfig{}
	*c = *cl.config
	return c
}

// PrepareLoad verifies that the certificate at certPath and keyPath will load without error.
// If the certificate can be loaded, a function that will apply the certificate reload is
// returned. Otherwise, an error is returned.
func (cl *TLSCertLoader) PrepareLoad(opts ...TLSCertLoaderOpt) (func() error, error) {
	// Start with the current config so any options that are not overridden with opts
	// will not change.
	c := cl.copyCurrentConfig()
	for _, o := range opts {
		o(c)
	}

	log, logEnd := logger.NewOperation(cl.logger, "Loading TLS certificate", "tls_load_cert",
		zap.String(logCertContext, c.certPath), zap.String(logKeyContext, c.keyPath), zap.String(logUsageContext, cl.usage))
	defer logEnd()

	logLoadError := func(err error) {
		activeCert := cl.LoadedCertificate()
		if !activeCert.IsEmpty() {
			// The leaf should be good, but you can't be too careful with TLS certificates.
			log.Error("Error loading TLS certificate, continuing to use previously loaded certificate",
				zap.Error(err),
				zap.String("failedCert", c.certPath), zap.String("failedKey", c.keyPath),
				zap.String("activeCert", activeCert.CertificatePath), zap.String("activeKey", activeCert.KeyPath),
				zap.String("activeCertSerial", activeCert.Serial()))
		} else {
			log.Error("Error loading TLS certificate, no previously loaded TLS certificate is available",
				zap.Error(err),
				zap.String("failedCert", c.certPath), zap.String("failedKey", c.keyPath))
		}
	}

	loadedCert, err := LoadCertificate(c.certPath, c.keyPath, WithLoadCertificateIgnoreFilePermissions(c.ignoreFilePermissions))
	if err != nil {
		logLoadError(err)
		return nil, err
	}

	// Sanity check the certificate against how a server will use it. A
	// certificate a server cannot present, or that every client will refuse, is
	// caught here rather than at the first handshake, where it would surface
	// per-connection as an error naming neither the certificate nor the reason.
	if cl.role.IsServerRole() {
		// These two are faults rather than judgments, so they fail the load even
		// when sanity checks are ignored: a server using TLS always needs a
		// certificate, and a leaf that will not parse is a real problem.
		if loadedCert.IsEmpty() {
			err := fmt.Errorf("%s: cannot use an empty certificate for a server: %w", cl.usage, ErrCertificateEmpty)
			logLoadError(err)
			return nil, err
		}

		leaf, err := loadedCert.GetLeaf()
		if err != nil {
			err = fmt.Errorf("%s: %w", cl.usage, err)
			logLoadError(err)
			return nil, err
		}

		// Everything below is a sanity check: the certificate is real, but this
		// package judges a server unable to use it. Collect them all so one load
		// reports every problem rather than only the first.
		var sanityErrs []error

		if !leaf.SupportsServerAuth() {
			sanityErrs = append(sanityErrs, fmt.Errorf("%s: cannot use a certificate that does not permit server authentication for a server: %w",
				cl.usage, ErrCertificateNotServerAuth))
		}

		if len(sanityErrs) > 0 {
			sanityErr := errors.Join(sanityErrs...)
			if !c.ignoreSanityChecks {
				logLoadError(sanityErr)
				return nil, sanityErr
			}
			log.Warn("TLS certificate failed sanity checks, loading it anyway because sanity checks are being ignored",
				zap.Error(sanityErr))
		}
	}

	loadedCert.WithLogContext(log).Info("Successfully loaded TLS certificate")

	return func() error {
		func() {
			cl.mu.Lock()
			defer cl.mu.Unlock()
			cl.cert = loadedCert
			cl.config = c
		}()
		cl.monitor.QueueWarnIssues(cl)
		return nil
	}, nil
}

// Close shuts down the goroutine monitoring certificate expiration.
// Even after the monitoring goroutine is shutdown, Load and GetCertificate
// will continue to work normally.
func (cl *TLSCertLoader) Close() error {
	// unregisterCertLoader is safe to call multiple times.
	if cl.monitor != nil {
		cl.monitor.unregisterCertLoader(cl)
	}
	return nil
}

// Paths returns the path of the currently loaded certificate and private key.
// The keyPath will be the file containing the private key, even if no keyPath
// was provided to NewTLSCertLoader / Load.
func (cl *TLSCertLoader) Paths() (certPath, keyPath string) {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	return cl.cert.CertificatePath, cl.cert.KeyPath
}
