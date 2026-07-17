package tlsconfig

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/influxdata/influxdb/pkg/file"
	"go.uber.org/zap"
)

const (
	// logCertContext is the key for log context with the certificate path.
	// Use literals instead of constants in tests to ensure no one changes
	// the constant unintentionally.
	logCertContext = "cert"

	// logKeyContext is the key for log context with the key path.
	// Use literals instead of constants in tests to ensure no one changes
	// the constant unintentionally.
	logKeyContext = "key"

	// logSerialContext is the key for log context with the certificate serial.
	// Use literals instead of constants in tests to ensure no one changes
	// the constant unintentionally.
	logSerialContext = "serial"
)

// X509Certificate is a wrapper around an x509.Certificate that adds
// some extra utility methods.
type X509Certificate struct {
	*x509.Certificate
}

// IsPremature determines if an x509 cert is premature (not valid yet).
// Returns true if certificate is premature, false otherwise.
func (xc *X509Certificate) IsPremature() bool {
	return time.Now().Before(xc.NotBefore)
}

// IsExpired determines if an x509 cert is expired. Returns if true if certificate
// is expired, false otherwise.
func (xc *X509Certificate) IsExpired() bool {
	return time.Now().After(xc.NotAfter)
}

// certExpiresSoon determines if an x509 cert is about to expire, based on expirationAdvanced.
// It also returns how long until the cert expires if we are within the expiration warn window.
func (xc *X509Certificate) ExpiresSoon(expirationAdvanced time.Duration) (bool, time.Duration) {
	untilExpires := time.Until(xc.NotAfter)
	if untilExpires < expirationAdvanced {
		return true, untilExpires
	}
	return false, 0
}

// supportsExtKeyUsage reports whether xc permits want.
//
// A certificate with no extended key usage extension is unrestricted and may be
// used for any purpose. Once the extension is present it is exhaustive, so it
// must name want (or anyExtendedKeyUsage) for a peer to accept the certificate.
// Go's verifier rejects the rest with "x509: certificate specifies an
// incompatible key usage".
func (xc *X509Certificate) supportsExtKeyUsage(want x509.ExtKeyUsage) bool {
	if xc == nil || xc.Certificate == nil {
		return false
	}

	// Both lists must be empty to conclude the extension is absent: an
	// extension naming only unrecognized OIDs still restricts the certificate,
	// and leaves ExtKeyUsage empty.
	if len(xc.ExtKeyUsage) == 0 && len(xc.UnknownExtKeyUsage) == 0 {
		return true
	}

	for _, eku := range xc.ExtKeyUsage {
		if eku == want || eku == x509.ExtKeyUsageAny {
			return true
		}
	}
	return false
}

// SupportsServerAuth reports whether xc may be presented by a TLS server.
func (xc *X509Certificate) SupportsServerAuth() bool {
	return xc.supportsExtKeyUsage(x509.ExtKeyUsageServerAuth)
}

// SupportsClientAuth reports whether xc may be presented by a TLS client for
// client authentication. It only matters against a peer that verifies the
// certificates clients present; below tls.VerifyClientCertIfGiven the usages
// are never examined.
func (xc *X509Certificate) SupportsClientAuth() bool {
	return xc.supportsExtKeyUsage(x509.ExtKeyUsageClientAuth)
}

// logIssues logs issues with xc. Issues include:
// - expired certificate
// - certificates that are about to expire
// - certificate that is not valid yet
func (xc *X509Certificate) logIssues(log *zap.Logger, expirationAdvanced time.Duration) {
	if log == nil || xc == nil {
		return
	}

	if xc.IsExpired() {
		log.Warn("Certificate is expired", zap.Time("NotAfter", xc.NotAfter))
	} else if xc.IsPremature() {
		log.Warn("Certificate is not valid yet", zap.Time("NotBefore", xc.NotBefore))
	} else if expiresSoon, timeUntilExpires := xc.ExpiresSoon(expirationAdvanced); expiresSoon {
		log.Warn("Certificate will expire soon", zap.Time("NotAfter", xc.NotAfter), zap.Duration("untilExpires", timeUntilExpires))
	}
}

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

func (lc LoadedCertificate) IsValid() bool {
	return lc.valid
}

func (lc LoadedCertificate) IsEmpty() bool {
	return !lc.IsValid() || (lc.CertificatePath == "" && lc.KeyPath == "")
}

func (lc LoadedCertificate) Serial() string {
	if lc.Leaf != nil {
		return lc.Leaf.SerialNumber.String()
	}
	return "N/A"
}

// WithLogContext adds context about lc to log and returns the new logger.
func (lc LoadedCertificate) WithLogContext(log *zap.Logger) *zap.Logger {
	return log.With(zap.String(logCertContext, lc.CertificatePath), zap.String(logKeyContext, lc.KeyPath), zap.String(logSerialContext, lc.Serial()))
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

	// Return empty certificate for empty paths.
	if certPath == "" && keyPath == "" {
		return LoadedCertificate{}, nil
	}

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

// GetLeaf returns the loaded leaf certificate, wrapped as a X509Certificate.
func (lc *LoadedCertificate) GetLeaf() (*X509Certificate, error) {
	if !lc.IsValid() {
		return nil, ErrCertificateInvalid
	}
	if lc.IsEmpty() {
		return nil, ErrCertificateEmpty
	}
	if lc.Leaf == nil {
		return nil, ErrCertificateNil
	}
	return &X509Certificate{
		Certificate: lc.Leaf,
	}, nil
}
