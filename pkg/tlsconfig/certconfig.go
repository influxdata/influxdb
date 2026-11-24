package tlsconfig

import (
	"errors"
	"fmt"
	"sync"
	"time"
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
// TLSConfigManager that owns it. So is logging a load, since only the manager
// knows whether a loaded certificate is accepted.
type tlsCertLoader struct {
	// All fields before mu are set at construction and never change.

	// monitor is the certificate monitor for loader.
	monitor *TLSCertMonitor

	// usage is the descriptive usage string for logging.
	usage string

	// mu protects all members below.
	mu sync.RWMutex

	// cert is the currently active certificate.
	cert LoadedCertificate
}

// newTLSCertLoader creates a tlsCertLoader with no certificate loaded and
// registers it with monitor.
func newTLSCertLoader(monitor *TLSCertMonitor, usage string) (*tlsCertLoader, error) {
	if monitor == nil {
		return nil, fmt.Errorf("newTLSCertLoader: usage=%q: %w", usage, ErrNoCertificateMonitor)
	}

	cl := &tlsCertLoader{
		monitor: monitor,
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
// cl's active certificate. Empty paths load an empty certificate. Nothing is
// logged; see TLSConfigManager.prepareCertLoad.
func (cl *tlsCertLoader) prepareLoad(certPath, keyPath string, ignoreFilePermissions bool) (func() error, LoadedCertificate, error) {
	loadedCert, err := LoadCertificate(certPath, keyPath, WithLoadCertificateIgnoreFilePermissions(ignoreFilePermissions))
	if err != nil {
		return nil, LoadedCertificate{}, err
	}

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
