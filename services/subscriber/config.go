package subscriber

import (
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/influxdata/influxdb/monitor/diagnostics"
	"github.com/influxdata/influxdb/pkg/tlsconfig"
	"github.com/influxdata/influxdb/toml"
)

const (
	// DefaultHTTPTimeout is the default HTTP timeout for a Config.
	DefaultHTTPTimeout = 30 * time.Second

	// DefaultWriteConcurrency is the default write concurrency for a Config.
	DefaultWriteConcurrency = 40

	// DefaultWriteBufferSize is the default write buffer size for a Config.
	DefaultWriteBufferSize = 1000
)

// Config represents a configuration of the subscriber service.
type Config struct {
	// Whether to enable to Subscriber service
	Enabled bool `toml:"enabled"`

	HTTPTimeout toml.Duration `toml:"http-timeout"`

	// InsecureSkipVerify gets passed to the http client, if true, it will
	// skip https certificate verification. Defaults to false
	InsecureSkipVerify bool `toml:"insecure-skip-verify"`

	// configure the path to the PEM encoded CA certs file. If the
	// empty string, the default system certs will be used
	CaCerts string `toml:"ca-certs"`

	// RootCA configures the CA pool used to verify subscription endpoint server
	// certificates. It is combined with the legacy CaCerts (ca-certs) setting,
	// which continues to work on its own for backwards compatibility.
	RootCA *tlsconfig.CAConfig `toml:"root-ca"`

	// Certificate and PrivateKey are the client certificate the subscriber
	// presents to HTTPS endpoints for mutual TLS. Empty means no client
	// certificate is presented.
	Certificate string `toml:"certificate"`
	PrivateKey  string `toml:"private-key"`

	// InsecureCertificate is true if the client certificate's file permissions
	// should be ignored when it is loaded.
	InsecureCertificate bool `toml:"insecure-certificate"`

	// IgnoreSanityChecks loads the certificate even when it fails the
	// checks that decide whether it can be used at all. The checks currently
	// only cover server certificates, so this has no effect on the subscriber's
	// client certificate today.
	IgnoreSanityChecks bool `toml:"ignore-sanity-checks"`

	// The number of writer goroutines processing the write channel.
	WriteConcurrency int `toml:"write-concurrency"`

	// The number of in-flight writes buffered in the write channel.
	WriteBufferSize int `toml:"write-buffer-size"`

	// TotalBufferBytes is the total size in bytes allocated to buffering across all subscriptions.
	// Each named subscription will receive an even division of the total.
	TotalBufferBytes toml.SSize `toml:"total-buffer-bytes"`

	// TLS is a base tls config to use for https clients.
	TLS *tls.Config `toml:"-"`
}

// NewConfig returns a new instance of a subscriber config.
func NewConfig() Config {
	return Config{
		Enabled:            true,
		HTTPTimeout:        toml.Duration(DefaultHTTPTimeout),
		InsecureSkipVerify: false,
		CaCerts:            "",
		WriteConcurrency:   DefaultWriteConcurrency,
		WriteBufferSize:    DefaultWriteBufferSize,
	}
}

// Validate returns an error if the config is invalid.
func (c Config) Validate() error {
	if c.HTTPTimeout <= 0 {
		return errors.New("http-timeout must be greater than 0")
	}

	if c.CaCerts != "" && !fileExists(c.CaCerts) {
		abspath, err := filepath.Abs(c.CaCerts)
		if err != nil {
			return fmt.Errorf("ca-certs file %s does not exist. Wrapped Error: %v", c.CaCerts, err)
		}
		return fmt.Errorf("ca-certs file %s does not exist", abspath)
	}

	if c.WriteBufferSize <= 0 {
		return errors.New("write-buffer-size must be greater than 0")
	}

	if c.WriteConcurrency <= 0 {
		return errors.New("write-concurrency must be greater than 0")
	}

	return nil
}

// effectiveRootCA combines the RootCA block with the legacy CaCerts (ca-certs)
// setting into a single *tlsconfig.CAConfig for verifying subscription endpoint
// server certificates. A nil result leaves the base TLS config's roots in place
// (Go's system pool). CaCerts is appended to the block's paths so both settings
// work together, and ca-certs continues to behave as before on its own.
func (c Config) effectiveRootCA() *tlsconfig.CAConfig {
	var cc *tlsconfig.CAConfig
	if c.RootCA != nil {
		dup := *c.RootCA
		dup.Paths = append([]string(nil), c.RootCA.Paths...)
		cc = &dup
	}
	if c.CaCerts != "" {
		if cc == nil {
			cc = &tlsconfig.CAConfig{}
		}
		cc.Paths = append(cc.Paths, c.CaCerts)
	}
	return cc
}

// TLSManagerOpts returns the list of TLS manager options specified by c.
func (c Config) TLSManagerOpts() []tlsconfig.TLSConfigManagerOpt {
	return []tlsconfig.TLSConfigManagerOpt{
		tlsconfig.WithUsage("subscriber"),
		tlsconfig.WithUseTLS(true),
		tlsconfig.WithBaseConfig(c.TLS),
		tlsconfig.WithAllowInsecure(c.InsecureSkipVerify),
		tlsconfig.WithClientCertificate(c.Certificate, c.PrivateKey),
		tlsconfig.WithRootCA(c.effectiveRootCA()),
		tlsconfig.WithIgnoreFilePermissions(c.InsecureCertificate),
		tlsconfig.WithIgnoreSanityChecks(c.IgnoreSanityChecks),
	}
}

func fileExists(fileName string) bool {
	info, err := os.Stat(fileName)
	return err == nil && !info.IsDir()
}

// Diagnostics returns a diagnostics representation of a subset of the Config.
func (c Config) Diagnostics() (*diagnostics.Diagnostics, error) {
	if !c.Enabled {
		return diagnostics.RowFromMap(map[string]interface{}{
			"enabled": false,
		}), nil
	}

	return diagnostics.RowFromMap(map[string]interface{}{
		"enabled":           true,
		"http-timeout":      c.HTTPTimeout,
		"write-concurrency": c.WriteConcurrency,
		"write-buffer-size": c.WriteBufferSize,
	}), nil
}
