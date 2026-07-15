package tlsconfig

import (
	"crypto/x509"
	"encoding/asn1"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/influxdata/influxdb/pkg/testing/selfsigned"
)

func TestLoadCertificate_EmptyPaths(t *testing.T) {
	t.Run("both paths empty loads nothing", func(t *testing.T) {
		// An entirely unconfigured certificate is not an error: it is how a
		// client-only manager with no client certificate is expressed.
		lc, err := LoadCertificate("", "")
		require.NoError(t, err)
		require.True(t, lc.IsEmpty())
		require.False(t, lc.IsValid())
	})

	t.Run("key without certificate is an error", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		lc, err := LoadCertificate("", ss.KeyPath)
		require.ErrorIs(t, err, ErrPathEmpty)
		require.ErrorContains(t, err, "LoadCertificate: certificate:")
		require.False(t, lc.IsValid())
	})
}

func TestLoadCertificate_KeyDefaultsToCertPath(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t, selfsigned.WithCombinedFile())

	// An empty keyPath means the key lives in the certificate file. The loaded
	// certificate should report the certificate path as the key path.
	lc, err := LoadCertificate(ss.CertPath, "", WithLoadCertificateIgnoreFilePermissions(true))
	require.NoError(t, err)
	require.True(t, lc.IsValid())
	require.Equal(t, ss.CertPath, lc.CertificatePath)
	require.Equal(t, ss.CertPath, lc.KeyPath)
}

func TestLoadedCertificate_GetLeaf(t *testing.T) {
	t.Run("invalid certificate", func(t *testing.T) {
		lc := LoadedCertificate{}
		leaf, err := lc.GetLeaf()
		require.ErrorIs(t, err, ErrCertificateInvalid)
		require.Nil(t, leaf)
	})

	t.Run("valid but empty certificate", func(t *testing.T) {
		// valid with no paths is only reachable inside the package, but GetLeaf
		// must still report it rather than returning a nil leaf.
		lc := LoadedCertificate{valid: true}
		leaf, err := lc.GetLeaf()
		require.ErrorIs(t, err, ErrCertificateEmpty)
		require.Nil(t, leaf)
	})

	t.Run("certificate with nil leaf", func(t *testing.T) {
		lc := LoadedCertificate{valid: true, CertificatePath: "cert.pem", KeyPath: "key.pem"}
		leaf, err := lc.GetLeaf()
		require.ErrorIs(t, err, ErrCertificateNil)
		require.Nil(t, leaf)
	})

	t.Run("loaded certificate", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		lc, err := LoadCertificate(ss.CertPath, ss.KeyPath)
		require.NoError(t, err)

		leaf, err := lc.GetLeaf()
		require.NoError(t, err)
		require.NotNil(t, leaf)
		require.NotNil(t, leaf.Certificate)
		require.Equal(t, lc.Leaf.SerialNumber, leaf.SerialNumber)
	})
}

func TestLoadedCertificate_Serial(t *testing.T) {
	t.Run("no leaf reports N/A", func(t *testing.T) {
		lc := LoadedCertificate{valid: true, CertificatePath: "cert.pem"}
		require.Equal(t, "N/A", lc.Serial())
	})

	t.Run("loaded certificate reports leaf serial", func(t *testing.T) {
		ss := selfsigned.NewSelfSignedCert(t)

		lc, err := LoadCertificate(ss.CertPath, ss.KeyPath)
		require.NoError(t, err)
		require.Equal(t, lc.Leaf.SerialNumber.String(), lc.Serial())
	})
}

func TestLoadedCertificate_IsEmpty(t *testing.T) {
	tests := []struct {
		name    string
		lc      LoadedCertificate
		isEmpty bool
		isValid bool
	}{
		{
			name:    "zero value",
			lc:      LoadedCertificate{},
			isEmpty: true,
			isValid: false,
		},
		{
			name:    "invalid with paths is still empty",
			lc:      LoadedCertificate{CertificatePath: "cert.pem", KeyPath: "key.pem"},
			isEmpty: true,
			isValid: false,
		},
		{
			name:    "valid without paths is empty",
			lc:      LoadedCertificate{valid: true},
			isEmpty: true,
			isValid: true,
		},
		{
			name:    "valid with cert path only",
			lc:      LoadedCertificate{valid: true, CertificatePath: "cert.pem"},
			isEmpty: false,
			isValid: true,
		},
		{
			name:    "valid with both paths",
			lc:      LoadedCertificate{valid: true, CertificatePath: "cert.pem", KeyPath: "key.pem"},
			isEmpty: false,
			isValid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.isEmpty, tt.lc.IsEmpty())
			require.Equal(t, tt.isValid, tt.lc.IsValid())
		})
	}
}

func TestLoadedCertificate_WithLogContext(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t)

	lc, err := LoadCertificate(ss.CertPath, ss.KeyPath)
	require.NoError(t, err)

	core, logs := observer.New(zapcore.InfoLevel)
	lc.WithLogContext(zap.New(core)).Info("test message")

	entries := logs.FilterMessage("test message").TakeAll()
	require.Len(t, entries, 1)
	require.Equal(t, ss.CertPath, entries[0].ContextMap()["cert"])
	require.Equal(t, ss.KeyPath, entries[0].ContextMap()["key"])
	require.Equal(t, lc.Serial(), entries[0].ContextMap()["serial"])
}

func TestX509Certificate_ExpiresSoon(t *testing.T) {
	loadLeaf := func(t *testing.T, notBefore, notAfter time.Time) *X509Certificate {
		t.Helper()
		ss := selfsigned.NewSelfSignedCert(t,
			selfsigned.WithNotBefore(notBefore),
			selfsigned.WithNotAfter(notAfter))
		lc, err := LoadCertificate(ss.CertPath, ss.KeyPath)
		require.NoError(t, err)
		leaf, err := lc.GetLeaf()
		require.NoError(t, err)
		return leaf
	}

	t.Run("outside warn window", func(t *testing.T) {
		leaf := loadLeaf(t, time.Now().Add(-24*time.Hour), time.Now().Add(30*24*time.Hour))

		expiresSoon, untilExpires := leaf.ExpiresSoon(5 * 24 * time.Hour)
		require.False(t, expiresSoon)
		require.Zero(t, untilExpires, "untilExpires is only meaningful inside the warn window")
	})

	t.Run("inside warn window", func(t *testing.T) {
		notAfter := time.Now().Add(24 * time.Hour)
		leaf := loadLeaf(t, time.Now().Add(-24*time.Hour), notAfter)

		expiresSoon, untilExpires := leaf.ExpiresSoon(5 * 24 * time.Hour)
		require.True(t, expiresSoon)
		require.Positive(t, untilExpires)
		require.WithinDuration(t, notAfter, time.Now().Add(untilExpires), time.Minute)
	})

	t.Run("already expired is inside any window", func(t *testing.T) {
		leaf := loadLeaf(t, time.Now().Add(-48*time.Hour), time.Now().Add(-24*time.Hour))

		expiresSoon, untilExpires := leaf.ExpiresSoon(time.Hour)
		require.True(t, expiresSoon)
		require.Negative(t, untilExpires, "an expired certificate reports a negative time to expiration")
	})
}

func TestX509Certificate_IsExpiredIsPremature(t *testing.T) {
	loadLeaf := func(t *testing.T, notBefore, notAfter time.Time) *X509Certificate {
		t.Helper()
		ss := selfsigned.NewSelfSignedCert(t,
			selfsigned.WithNotBefore(notBefore),
			selfsigned.WithNotAfter(notAfter))
		lc, err := LoadCertificate(ss.CertPath, ss.KeyPath)
		require.NoError(t, err)
		leaf, err := lc.GetLeaf()
		require.NoError(t, err)
		return leaf
	}

	t.Run("current certificate", func(t *testing.T) {
		leaf := loadLeaf(t, time.Now().Add(-24*time.Hour), time.Now().Add(24*time.Hour))
		require.False(t, leaf.IsExpired())
		require.False(t, leaf.IsPremature())
	})

	t.Run("expired certificate", func(t *testing.T) {
		leaf := loadLeaf(t, time.Now().Add(-48*time.Hour), time.Now().Add(-24*time.Hour))
		require.True(t, leaf.IsExpired())
		require.False(t, leaf.IsPremature())
	})

	t.Run("premature certificate", func(t *testing.T) {
		leaf := loadLeaf(t, time.Now().Add(24*time.Hour), time.Now().Add(48*time.Hour))
		require.False(t, leaf.IsExpired())
		require.True(t, leaf.IsPremature())
	})
}

func TestX509Certificate_LogIssues(t *testing.T) {
	loadLeaf := func(t *testing.T, notBefore, notAfter time.Time) *X509Certificate {
		t.Helper()
		ss := selfsigned.NewSelfSignedCert(t,
			selfsigned.WithNotBefore(notBefore),
			selfsigned.WithNotAfter(notAfter))
		lc, err := LoadCertificate(ss.CertPath, ss.KeyPath)
		require.NoError(t, err)
		leaf, err := lc.GetLeaf()
		require.NoError(t, err)
		return leaf
	}

	t.Run("healthy certificate logs nothing", func(t *testing.T) {
		leaf := loadLeaf(t, time.Now().Add(-24*time.Hour), time.Now().Add(30*24*time.Hour))

		core, logs := observer.New(zapcore.DebugLevel)
		leaf.logIssues(zap.New(core), 5*24*time.Hour)
		require.Zero(t, logs.Len(), "a healthy certificate should not produce log noise")
	})

	t.Run("nil logger does not panic", func(t *testing.T) {
		leaf := loadLeaf(t, time.Now().Add(-48*time.Hour), time.Now().Add(-24*time.Hour))
		require.NotPanics(t, func() { leaf.logIssues(nil, time.Hour) })
	})

	t.Run("nil certificate does not panic", func(t *testing.T) {
		core, _ := observer.New(zapcore.DebugLevel)
		var xc *X509Certificate
		require.NotPanics(t, func() { xc.logIssues(zap.New(core), time.Hour) })
	})

	t.Run("expired outranks expires soon", func(t *testing.T) {
		leaf := loadLeaf(t, time.Now().Add(-48*time.Hour), time.Now().Add(-24*time.Hour))

		core, logs := observer.New(zapcore.DebugLevel)
		// An expired certificate is also inside the warn window; only the more
		// severe "expired" message should be reported.
		leaf.logIssues(zap.New(core), 30*24*time.Hour)

		entries := logs.TakeAll()
		require.Len(t, entries, 1, "expired certificate should log exactly one issue")
		require.Equal(t, "Certificate is expired", entries[0].Message)
	})
}

func TestLoadCertificate_MalformedFiles(t *testing.T) {
	ss := selfsigned.NewSelfSignedCert(t)

	t.Run("garbage certificate file", func(t *testing.T) {
		dir := t.TempDir()
		certPath := path.Join(dir, "cert.pem")
		require.NoError(t, os.WriteFile(certPath, []byte("not a certificate"), 0600))

		lc, err := LoadCertificate(certPath, ss.KeyPath)
		require.ErrorContains(t, err, "error loading x509 key pair")
		require.False(t, lc.IsValid())
	})

	t.Run("missing key file", func(t *testing.T) {
		lc, err := LoadCertificate(ss.CertPath, path.Join(t.TempDir(), "absent.pem"))
		require.ErrorContains(t, err, "LoadCertificate: error opening")
		require.False(t, lc.IsValid())
	})
}

func TestX509Certificate_SupportsServerAuth(t *testing.T) {
	tests := []struct {
		name      string
		eku       []x509.ExtKeyUsage
		unknown   []asn1.ObjectIdentifier
		supported bool
	}{
		{
			name:      "no extension is unrestricted",
			supported: true,
		},
		{
			name:      "server auth",
			eku:       []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			supported: true,
		},
		{
			name:      "server and client auth",
			eku:       []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
			supported: true,
		},
		{
			name:      "any extended key usage",
			eku:       []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
			supported: true,
		},
		{
			name:      "server auth among others",
			eku:       []x509.ExtKeyUsage{x509.ExtKeyUsageCodeSigning, x509.ExtKeyUsageServerAuth},
			supported: true,
		},
		{
			name:      "client auth only",
			eku:       []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
			supported: false,
		},
		{
			name:      "unrelated usage only",
			eku:       []x509.ExtKeyUsage{x509.ExtKeyUsageEmailProtection},
			supported: false,
		},
		{
			// The extension is present but names only OIDs x509 does not
			// recognize, which still restricts the certificate. ExtKeyUsage
			// being empty is therefore not enough to call it unrestricted.
			name:      "only unrecognized usages",
			unknown:   []asn1.ObjectIdentifier{{1, 3, 6, 1, 4, 1, 99999, 1}},
			supported: false,
		},
		{
			name:      "server auth alongside an unrecognized usage",
			eku:       []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			unknown:   []asn1.ObjectIdentifier{{1, 3, 6, 1, 4, 1, 99999, 1}},
			supported: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			xc := &X509Certificate{Certificate: &x509.Certificate{
				ExtKeyUsage:        tt.eku,
				UnknownExtKeyUsage: tt.unknown,
			}}
			require.Equal(t, tt.supported, xc.SupportsServerAuth())
		})
	}

	t.Run("nil certificate", func(t *testing.T) {
		var xc *X509Certificate
		require.False(t, xc.SupportsServerAuth())
		require.False(t, (&X509Certificate{}).SupportsServerAuth())
	})
}
