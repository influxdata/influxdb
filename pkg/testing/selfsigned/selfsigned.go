package selfsigned

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type Cert struct {
	// Path to the SSL certificates.
	CACertPath, CertPath string

	// Path to the private keys for the SSL certificates.
	CAKeyPath, KeyPath string
}

type CertOptions struct {
	// DNSNames for cert
	DNSNames []string

	// IP Addresses for cert
	IPAddrs []net.IP

	// SkipDefaultIPAddrs indicates if the default IPAddrs should be skipped if IPAddrs is not set.
	SkipDefaultIPAddrs bool

	// NotBefore for certificate
	NotBefore time.Time

	// NotAfter for certificate
	NotAfter time.Time

	// CombinedFile indicates if the certificate and key should be combined into a single file
	CombinedFile bool

	// CAOrganization sets the CA certificate's Subject.Organization field
	CAOrganization string

	// CACommonName sets the CA certificate's Subject.CommonName field
	CACommonName string
}

type CertOpt func(*CertOptions)

func WithDNSName(dnsName string) CertOpt {
	return func(o *CertOptions) {
		o.DNSNames = append(o.DNSNames, dnsName)
	}
}

func WithIPAddr(ipAddr net.IP) CertOpt {
	return func(o *CertOptions) {
		o.IPAddrs = append(o.IPAddrs, ipAddr)
	}
}

func WithNoDefaultIPAddrs() CertOpt {
	return func(o *CertOptions) {
		o.SkipDefaultIPAddrs = true
	}
}

func WithNotBefore(notBefore time.Time) CertOpt {
	return func(o *CertOptions) {
		o.NotBefore = notBefore
	}
}

func WithNotAfter(notAfter time.Time) CertOpt {
	return func(o *CertOptions) {
		o.NotAfter = notAfter
	}
}

func WithCombinedFile() CertOpt {
	return func(o *CertOptions) {
		o.CombinedFile = true
	}
}

func WithCASubject(organization, commonName string) CertOpt {
	return func(o *CertOptions) {
		o.CAOrganization = organization
		o.CACommonName = commonName
	}
}

func NewSelfSignedCert(t *testing.T, opts ...CertOpt) *Cert {
	t.Helper()
	tmpdir := t.TempDir()

	// Get options for self-signed certificate and set defaults if needed.
	options := CertOptions{}
	for _, o := range opts {
		o(&options)
	}

	if len(options.DNSNames) == 0 {
		options.DNSNames = []string{"localhost"}
	}

	if len(options.IPAddrs) == 0 && !options.SkipDefaultIPAddrs {
		options.IPAddrs = []net.IP{net.IPv4(127, 0, 0, 1)}
	}

	if options.NotBefore.IsZero() {
		options.NotBefore = time.Now()
	}

	if options.NotAfter.IsZero() {
		options.NotAfter = time.Now().Add(7 * 24 * time.Hour)
	}

	if options.CAOrganization == "" {
		options.CAOrganization = "my_test_ca"
	}

	if options.CACommonName == "" {
		options.CACommonName = "My Test CA"
	}

	// Sanity check options.
	require.NotEmpty(t, options.DNSNames)

	// Now generate the certificate.
	capk, err := rsa.GenerateKey(rand.Reader, 1024)
	require.NoError(t, err)

	caSerial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	require.NoError(t, err)

	caTemplate := x509.Certificate{
		SerialNumber: caSerial,
		NotBefore:    options.NotBefore,
		NotAfter:     options.NotAfter,

		KeyUsage:    x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},

		BasicConstraintsValid: true,

		Subject: pkix.Name{
			Organization: []string{options.CAOrganization},
			CommonName:   options.CACommonName,
		},

		IsCA: true,
	}

	caCert, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, capk.Public(), capk)
	require.NoError(t, err)
	caCertPEM := &pem.Block{Type: "CERTIFICATE", Bytes: caCert}
	caCertFile, err := os.CreateTemp(tmpdir, "cacert-*.pem")
	require.NoError(t, err)
	require.NoError(t, pem.Encode(caCertFile, caCertPEM))
	require.NoError(t, caCertFile.Close())

	caKeyFile, err := os.CreateTemp(tmpdir, "cakey-*.pem")
	require.NoError(t, err)
	require.NoError(t, pem.Encode(caKeyFile, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(capk)}))
	require.NoError(t, caKeyFile.Close())

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	require.NoError(t, err)
	// Basically the same as the CA template, but its own serial, and with ip addresses and dns names.
	template := x509.Certificate{
		SerialNumber: serial,
		NotBefore:    options.NotBefore,
		NotAfter:     options.NotAfter,

		KeyUsage:    x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},

		BasicConstraintsValid: true,

		Subject: pkix.Name{
			CommonName: options.DNSNames[0],
		},

		IPAddresses: options.IPAddrs,
		DNSNames:    options.DNSNames,
	}

	pk, err := rsa.GenerateKey(rand.Reader, 1024)
	require.NoError(t, err)
	cert, err := x509.CreateCertificate(rand.Reader, &template, &caTemplate, pk.Public(), capk)
	require.NoError(t, err)
	certPEM := &pem.Block{Type: "CERTIFICATE", Bytes: cert}
	keyPEM := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(pk)}

	var certPath, keyPath string
	if options.CombinedFile {
		// Create a single file containing both certificate and key
		combinedFile, err := os.CreateTemp(tmpdir, "combined-*.pem")
		require.NoError(t, err)
		require.NoError(t, pem.Encode(combinedFile, certPEM))
		require.NoError(t, pem.Encode(combinedFile, keyPEM))
		require.NoError(t, combinedFile.Close())
		certPath = combinedFile.Name()
		keyPath = combinedFile.Name()
	} else {
		// Create separate certificate and key files
		certFile, err := os.CreateTemp(tmpdir, "sslcert-*.pem")
		require.NoError(t, err)
		require.NoError(t, pem.Encode(certFile, certPEM))
		require.NoError(t, certFile.Close())

		keyFile, err := os.CreateTemp(tmpdir, "key-*.pem")
		require.NoError(t, err)
		require.NoError(t, pem.Encode(keyFile, keyPEM))
		require.NoError(t, keyFile.Close())

		certPath = certFile.Name()
		keyPath = keyFile.Name()
	}

	return &Cert{
		CACertPath: caCertFile.Name(),
		CAKeyPath:  caKeyFile.Name(),
		CertPath:   certPath,
		KeyPath:    keyPath,
	}
}

func (c *Cert) ClientTLSConfig(t *testing.T, insecure, addCert bool) *tls.Config {
	t.Helper()
	cert, err := os.ReadFile(c.CACertPath)
	require.NoError(t, err)

	// Return a TLS config that trusts our self-generated CA.
	var pool *x509.CertPool
	if !insecure {
		pool = x509.NewCertPool()
		require.True(t, pool.AppendCertsFromPEM(cert), "failed to append certificate")
	}
	conf := &tls.Config{
		RootCAs:            pool,
		InsecureSkipVerify: insecure,
	}
	if addCert {
		clientPair, err := tls.LoadX509KeyPair(c.CertPath, c.KeyPath)
		require.NoError(t, err)
		conf.Certificates = []tls.Certificate{clientPair}
	}
	return conf
}

func (c *Cert) ServerTLSConfig(t *testing.T) *tls.Config {
	cert, err := tls.LoadX509KeyPair(c.CertPath, c.KeyPath)
	require.NoError(t, err)
	return &tls.Config{
		ServerName:   "localhost",
		Certificates: []tls.Certificate{cert},
	}
}
