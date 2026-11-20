package tlsconfig

import (
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
)

// tlsCertLoader holds a TLS certificate, loads replacements for it, and
// registers it with a TLSCertMonitor to be checked for expiration. It knows
// nothing of how the certificate is used: whether it is presented by a server
// or a client, and the checks that follow from that, are up to the
// TLSConfigManager that owns it.
type tlsCertLoader struct {
	// All fields before mu are set at construction and never change.

	// monitor is the certificate monitor for loader.
	monitor *TLSCertMonitor

	// logger is the logger used for logging status. It already carries usage.
	logger *zap.Logger

	// usage is the descriptive usage string for logging.
	usage string

	// mu protects all members below.
	mu sync.RWMutex

	// cert is the currently active certificate.
	cert LoadedCertificate
}

// newTLSCertLoader creates a tlsCertLoader with no certificate loaded and
// registers it with monitor. A nil logger logs nothing.
func newTLSCertLoader(monitor *TLSCertMonitor, logger *zap.Logger, usage string) (*tlsCertLoader, error) {
	if monitor == nil {
		return nil, fmt.Errorf("newTLSCertLoader: usage=%q: %w", usage, ErrNoCertificateMonitor)
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	cl := &tlsCertLoader{
		monitor: monitor,
		logger:  logger.With(zap.String(logUsageContext, usage)),
		usage:   usage,
	}
	cl.monitor.registerCertLoader(cl)

	return cl, nil
}

// Usage is the descriptive usage of the loader.
func (cl *tlsCertLoader) Usage() string {
	return cl.usage
}

// Clear clears the loaded certificate.
func (cl *tlsCertLoader) Clear() {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.cert = LoadedCertificate{}
}

// LoadedCertificate returns the currently loaded certificate, which may be
// invalid or empty.
func (cl *tlsCertLoader) LoadedCertificate() LoadedCertificate {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	return cl.cert
}

// prepareLoad loads the certificate at certPath and keyPath without changing
// cl. On success it returns the loaded certificate and a function that makes it
// cl's active certificate. Empty paths load an empty certificate.
func (cl *tlsCertLoader) prepareLoad(certPath, keyPath string, ignoreFilePermissions bool) (func() error, LoadedCertificate, error) {
	log, logEnd := logger.NewOperation(cl.logger, "Loading TLS certificate", "tls_load_cert",
		zap.String(logCertContext, certPath), zap.String(logKeyContext, keyPath), zap.String(logUsageContext, cl.usage))
	defer logEnd()

	logLoadError := func(err error) {
		activeCert := cl.LoadedCertificate()
		if !activeCert.IsEmpty() {
			// The leaf should be good, but you can't be too careful with TLS certificates.
			log.Error("Error loading TLS certificate, continuing to use previously loaded certificate",
				zap.Error(err),
				zap.String("failedCert", certPath), zap.String("failedKey", keyPath),
				zap.String("activeCert", activeCert.CertificatePath), zap.String("activeKey", activeCert.KeyPath),
				zap.String("activeCertSerial", activeCert.Serial()))
		} else {
			log.Error("Error loading TLS certificate, no previously loaded TLS certificate is available",
				zap.Error(err),
				zap.String("failedCert", certPath), zap.String("failedKey", keyPath))
		}
	}

	loadedCert, err := LoadCertificate(certPath, keyPath, WithLoadCertificateIgnoreFilePermissions(ignoreFilePermissions))
	if err != nil {
		logLoadError(err)
		return nil, LoadedCertificate{}, err
	}

	loadedCert.WithLogContext(log).Info("Successfully loaded TLS certificate")

	return func() error {
		func() {
			cl.mu.Lock()
			defer cl.mu.Unlock()
			cl.cert = loadedCert
		}()
		cl.monitor.queueWarnIssues(cl)
		return nil
	}, loadedCert, nil
}

// Close unregisters cl from its monitor, so its certificate is no longer
// checked for expiration. cl otherwise continues to work normally.
func (cl *tlsCertLoader) Close() error {
	// unregisterCertLoader is safe to call multiple times.
	if cl.monitor != nil {
		cl.monitor.unregisterCertLoader(cl)
	}
	return nil
}
