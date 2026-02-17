package tlsconfig

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/pkg/file"
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
	ErrCertificateRequestInfoNil = errors.New("CertificateRequestInfo is nil")
	ErrLoadedCertificateInvalid  = errors.New("LoadedCertificate is invalid")
	ErrPathEmpty                 = errors.New("empty path")
)

// LoadedCertificate encapsulates information about a loaded certificate.
type LoadedCertificate struct {
	// valid indicates if this object is valid.
	valid bool

	// CertPath is the path the certificate was loaded from.
	CertificatePath string

	// KeyPath is the path the private key was loaded from.
	KeyPath string

	// Certificate is the certificate that was loaded.
	Certificate *tls.Certificate

	// Leaf is the parsed x509 certificate of Certificate's leaf certificate.
	Leaf *x509.Certificate
}

func (lc *LoadedCertificate) IsValid() bool {
	return lc.valid
}

// loadCertificateConfig is an internal config for LoadCertificate.
type loadCertificateConfig struct {
	// ignoreFilePermissions indicates if file permissions should be ignored during load.
	ignoreFilePermissions bool
}

// LoadCertificateOpt are functions to change the behavior of LoadCertificate.
type LoadCertificateOpt func(*loadCertificateConfig)

// WithLoadCertificateIgnoreFilePermissions instructs LoadCertificate to ignore file permissions
// if ignore is true.
func WithLoadCertificateIgnoreFilePermissions(ignore bool) LoadCertificateOpt {
	return func(c *loadCertificateConfig) {
		c.ignoreFilePermissions = ignore
	}
}

// LoadCertificate loads a key pair from certPath and keyPath, performing several checks
// along the way. If any checks fail or an error occurs loading the files, then an error is returned.
// If keyPath is empty, then certPath is assumed to contain both the certificate and the private key.
// Only trusted input (standard configuration files) should be used for certPath and keyPath.
func LoadCertificate(certPath, keyPath string, opts ...LoadCertificateOpt) (LoadedCertificate, error) {
	fail := func(err error) (LoadedCertificate, error) { return LoadedCertificate{valid: false}, err }

	config := loadCertificateConfig{}
	for _, o := range opts {
		o(&config)
	}

	if certPath == "" {
		return fail(fmt.Errorf("LoadCertificate: certificate: %w", ErrPathEmpty))
	}

	if keyPath == "" {
		// Assume key is combined with certificate.
		keyPath = certPath
	}

	wipeData := func(d []byte) {
		for i := range d {
			d[i] = 0
		}
	}

	// Load the certificate and private key from their files.
	loadFile := func(path string, maxPerms os.FileMode) (rData []byte, rErr error) {
		f, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("LoadCertificate: error opening %q for reading: %w", path, err)
		}
		defer func() {
			if err := f.Close(); err != nil {
				wipeData(rData)
				rData = nil
				rErr = errors.Join(rErr, fmt.Errorf("LoadCertificate: error closing file %q: %w", path, err))
			}
		}()

		if !config.ignoreFilePermissions {
			if err := file.VerifyFilePermissivenessF(f, maxPerms); err != nil {
				// VerifyFilePermissivenessF includes a lot context in its errors. No need to add duplicate here.
				return nil, fmt.Errorf("LoadCertificate: %w", err)
			}
		}
		data, err := io.ReadAll(f)
		if err != nil {
			return nil, fmt.Errorf("LoadCertificate: error data from %q: %w", path, err)
		}
		return data, nil
	}
	certData, err := loadFile(certPath, CertMaxPermissions)
	defer wipeData(certData)
	if err != nil {
		return fail(err)
	}

	keyData, err := loadFile(keyPath, KeyMaxPermissions)
	defer wipeData(keyData)
	if err != nil {
		return fail(err)
	}

	// Create key pair from loaded data
	cert, err := tls.X509KeyPair(certData, keyData)
	if err != nil {
		return fail(fmt.Errorf("error loading x509 key pair (%q / %q): %w", certPath, keyPath, err))
	}

	// Parse the first X509 certificate in cert's chain.
	// X509KeyPair() guarantees that cert.Certificate is not empty.
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		// This should be impossible to reach because `tls.X509KeyPair` will fail
		// if the leaf certificate can't be parsed.
		return fail(fmt.Errorf("error parsing leaf certificate (%q / %q): %w", certPath, keyPath, err))
	}
	if leaf == nil {
		// This shouldn't happen, but we should be extra careful with TLS certs.
		return fail(fmt.Errorf("error parsing leaf certificate (%q / %q): %w", certPath, keyPath, ErrCertificateNil))
	}

	return LoadedCertificate{
		valid:           true,
		CertificatePath: certPath,
		KeyPath:         keyPath,
		Certificate:     &cert,
		Leaf:            leaf,
	}, nil
}

// TLSCertLoader handles loading TLS certificates, providing them to a tls.Config, and
// monitoring the certificate for expiration.
type TLSCertLoader struct {
	// logger is the logger used for logging status.
	logger *zap.Logger

	// expirationAdvanced determines how long before a certificate expires a warning is issued.
	expirationAdvanced time.Duration

	// certificateCheckInterval determines the duration between each certificate check.
	certificateCheckInterval time.Duration

	// ignoreFilePermissions is true if file permission checks should be bypassed.
	ignoreFilePermissions bool

	// closeOnce is used to close closeCh exactly one time.
	closeOnce sync.Once

	// closeCh is used to trigger closing the monitor.
	closeCh chan struct{}

	// monitorStartWg can be used to detect if the monitor goroutine has started.
	monitorStartWg sync.WaitGroup

	// mu protects all members below.
	mu sync.Mutex

	// certPath is the path to the TLS certificate PEM file.
	certPath string

	// keyPath is the path to the TLS certificate key file.
	keyPath string

	// cert is the TLS certificate.
	cert *tls.Certificate

	// leaf is the parsed leaf certificate.
	leaf *x509.Certificate
}

type TLSCertLoaderOpt func(*TLSCertLoader)

// WithCertLoaderExpirationAdvanced sets the how far ahead a CertLoader will
// warn about a certificate that is about to expire.
func WithCertLoaderExpirationAdvanced(d time.Duration) TLSCertLoaderOpt {
	return func(cl *TLSCertLoader) {
		cl.expirationAdvanced = d
	}
}

// WithCertLoaderCertificateCheckInterval sets how often to check for certificate expiration.
func WithCertLoaderCertificateCheckInterval(d time.Duration) TLSCertLoaderOpt {
	return func(cl *TLSCertLoader) {
		cl.certificateCheckInterval = d
	}
}

// WithCertLoaderLogger assigns a logger for to use.
func WithCertLoaderLogger(logger *zap.Logger) TLSCertLoaderOpt {
	return func(cl *TLSCertLoader) {
		cl.logger = logger
	}
}

// WithCertLoaderIgnoreFilePermissions skips file permission checking when loading certificates.
func WithCertLoaderIgnoreFilePermissions(ignore bool) TLSCertLoaderOpt {
	return func(cl *TLSCertLoader) {
		cl.ignoreFilePermissions = ignore
	}
}

// NewTLSCertLoader creates a TLSCertLoader loaded with the certifcate found in certPath and keyPath.
// Only trusted input (standard configuration files) should be used for certPath and keyPath.
// If the certificate can not be loaded, an error is returned. On success, a monitor is setup to
// periodically check the certificate for expiration.
func NewTLSCertLoader(certPath, keyPath string, opts ...TLSCertLoaderOpt) (rCertLoader *TLSCertLoader, rErr error) {
	cl := &TLSCertLoader{
		expirationAdvanced:       DefaultExpirationWarnTime,
		certificateCheckInterval: DefaultCertificateCheckTime,
		closeCh:                  make(chan struct{}),
	}

	// Configure options.
	for _, o := range opts {
		o(cl)
	}

	// Make sure there is a valid logger.
	if cl.logger == nil {
		cl.logger = zap.NewNop()
	}

	// Perform initial certificate load.
	if err := cl.Load(certPath, keyPath); err != nil {
		return nil, err
	}

	// Start monitoring certificate.
	cl.monitorStartWg.Add(1)
	go cl.monitorCert(&cl.monitorStartWg)

	return cl, nil
}

// SetCertificate sets the currently loaded certificate from a LoadedCertificate.
// Will also log any warnings about certificate (e.g. expired, about to expire, etc.).
func (cl *TLSCertLoader) SetCertificate(l LoadedCertificate) error {
	err := func() error {
		cl.mu.Lock()
		defer cl.mu.Unlock()
		return cl.setCertificate(l)
	}()
	if err != nil {
		return err
	}

	cl.checkCurrentCert()
	return nil
}

// setCertificate is an internal method used to set the current certificate information
// from a LoadedCertficate. cl.mu must be held when calling.
func (cl *TLSCertLoader) setCertificate(l LoadedCertificate) error {
	if !l.valid {
		return fmt.Errorf("setCertificate: %w", ErrLoadedCertificateInvalid)
	}

	cl.certPath = l.CertificatePath
	cl.keyPath = l.KeyPath
	cl.cert = l.Certificate
	cl.leaf = l.Leaf

	return nil
}

// Certificate returns the currently loaded certificate, which may be nil.
func (cl *TLSCertLoader) Certificate() *tls.Certificate {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	return cl.cert
}

// SetupTLSConfig modifies tlsConfig to use cl for server and client certificates.
// tlsConfig may be nil. If other fields like tlsConfig.Certificates or
// tlsConfig.NameToCertificate have been set, then cl's certificate may not be used
// as expected.
func (cl *TLSCertLoader) SetupTLSConfig(tlsConfig *tls.Config) {
	if tlsConfig == nil {
		return
	}
	tlsConfig.GetCertificate = cl.GetCertificate
	tlsConfig.GetClientCertificate = cl.GetClientCertificate
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
	cl.mu.Lock()
	defer cl.mu.Unlock()
	return cl.leaf
}

func (cl *TLSCertLoader) loadCertificate(certPath, keyPath string) (LoadedCertificate, error) {
	return LoadCertificate(certPath, keyPath, WithLoadCertificateIgnoreFilePermissions(cl.ignoreFilePermissions))
}

// Load loads the certificate at the given certificate path and private keyfile path.
// Only trusted input (standard configuration files) should be used for certPath and keyPath.
func (cl *TLSCertLoader) Load(certPath, keyPath string) (rErr error) {
	log, logEnd := logger.NewOperation(cl.logger, "Loading TLS certificate", "tls_load_cert", zap.String("cert", certPath), zap.String("key", keyPath))
	defer logEnd()

	loadedCert, err := cl.loadCertificate(certPath, keyPath)

	cl.mu.Lock()
	defer cl.mu.Unlock()
	if err == nil {
		if err := cl.setCertificate(loadedCert); err != nil {
			// There shouldn't be a way to get here.
			log.Error("error setting certificate after load", zap.Error(err))
			return err
		}
		cl.logX509CertIssues(log, loadedCert.Leaf)
		log.Info("Successfully loaded TLS certificate", zap.String("cert", certPath), zap.String("key", keyPath))
	} else if cl.cert != nil {
		// This case shouldn't be possible, but we can't be too careful with TLS certificates.
		log.Error("Error loading TLS certificate, continuing to use previously loaded certificate",
			zap.Error(err),
			zap.String("failedCert", certPath), zap.String("failedKey", keyPath),
			zap.String("activeCert", cl.certPath), zap.String("activeKey", cl.keyPath))
	} else {
		log.Error("Error loading TLS certificate, no previously loaded TLS certificate is available",
			zap.Error(err),
			zap.String("failedCert", certPath), zap.String("failedKey", keyPath))
	}

	return err
}

// PrepareLoad verifies that the certificate at certPath and keyPath will load without error.
// If the certificate can be loaded, a function that will apply the certificate reload is
// returned. Otherwise, an error is returned.
func (cl *TLSCertLoader) PrepareLoad(certPath, keyPath string) (func() error, error) {
	loadedCert, err := cl.loadCertificate(certPath, keyPath)
	if err != nil {
		return nil, err
	}
	return func() error {
		if err := cl.SetCertificate(loadedCert); err != nil {
			cl.logger.Error("error applying new certificate after VerifyLoad success", zap.Error(err))
			return err
		}
		return nil
	}, nil
}

// Close shuts down the goroutine monitoring certificate expiration.
// Even after the monitoring goroutine is shutdown, Load and GetCertificate
// will continue to work normally.
func (cl *TLSCertLoader) Close() error {
	cl.closeOnce.Do(func() {
		close(cl.closeCh)
	})

	return nil
}

// isCertPremature determines if an x509 cert is premature (not valid yet).
// Returns true if certificate is premature, false otherwise. cert must not be nil.
func (cl *TLSCertLoader) isCertPremature(cert *x509.Certificate) bool {
	return time.Now().Before(cert.NotBefore)
}

// isCertExpired determines if an x509 cert is expired. Returns if true if certificate
// is expired, false otherwise. cert must not be nil.
func (cl *TLSCertLoader) isCertExpired(cert *x509.Certificate) bool {
	return time.Now().After(cert.NotAfter)
}

// certExpiresSoon determines if an x509 cert is about to expire, as well as returning
// how long until the cert expires if we are within the expiration warn window.
// cert must not be nil.
func (cl *TLSCertLoader) certExpiresSoon(cert *x509.Certificate) (bool, time.Duration) {
	untilExpires := time.Until(cert.NotAfter)
	if untilExpires < cl.expirationAdvanced {
		return true, untilExpires
	}
	return false, 0
}

// logX509CertIssues logs issues with an x509.Certificate to log. Included issues are:
// - expired certificate
// - certificates that are about to expire
// - certificate that is not valid yet
func (cl *TLSCertLoader) logX509CertIssues(log *zap.Logger, x509Cert *x509.Certificate) {
	if log == nil || x509Cert == nil {
		return
	}

	if cl.isCertExpired(x509Cert) {
		log.Warn("Certificate is expired", zap.Time("NotAfter", x509Cert.NotAfter))
	} else if cl.isCertPremature(x509Cert) {
		log.Warn("Certificate is not valid yet", zap.Time("NotBefore", x509Cert.NotBefore))
	} else if expiresSoon, timeUntilExpires := cl.certExpiresSoon(x509Cert); expiresSoon {
		log.Warn("Certificate will expire soon", zap.Time("NotAfter", x509Cert.NotAfter), zap.Duration("untilExpires", timeUntilExpires))
	}
}

// Paths returns the path of the currently loaded certificate and private key.
// The keyPath will be the file containing the private key, even if no keyPath
// was provided to NewTLSCertLoader / Load.
func (cl *TLSCertLoader) Paths() (certPath, keyPath string) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	return cl.certPath, cl.keyPath
}

// checkCurrentCert logs errors with the currently loaded leaf certificate.
func (cl *TLSCertLoader) checkCurrentCert() {
	leaf, log := func() (*x509.Certificate, *zap.Logger) {
		cl.mu.Lock()
		defer cl.mu.Unlock()
		log := cl.logger.With(zap.String("cert", cl.certPath), zap.String("key", cl.keyPath))
		return cl.leaf, log
	}()
	if leaf != nil {
		cl.logX509CertIssues(log, leaf)
	} else {
		// There shouldn't be a way to get here because we don't return the CertLoader if the
		// initial certificate load fails, and we also don't replace the certificate if Load
		// fails.
		log.Error("No certificate loaded when TLS certificate check performed", zap.Error(ErrCertificateNil))
	}
}

// WaitForMonitorStart will wait for the certificate monitor goroutine to start. This is mainly useful
// for tests to avoid race conditions.
func (cl *TLSCertLoader) WaitForMonitorStart() {
	cl.monitorStartWg.Wait()
}

// monitorCert periodically logs errors with the currently loaded certificate.
func (cl *TLSCertLoader) monitorCert(wg *sync.WaitGroup) {
	cl.logger.Info("Starting TLS certificate monitor")

	ticker := time.NewTicker(cl.certificateCheckInterval)
	defer ticker.Stop()

	if wg != nil {
		wg.Done()
	}

	for {
		select {
		case <-ticker.C:
			cl.checkCurrentCert()

		case <-cl.closeCh:
			certPath, keyPath := cl.Paths()
			cl.logger.Info("Closing TLS certificate monitor", zap.String("cert", certPath), zap.String("key", keyPath))
			return
		}
	}
}
