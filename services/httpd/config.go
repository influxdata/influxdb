package httpd

const (
	// DefaultBindAddress is the default address to bind to.
	DefaultBindAddress = ":8086"

	// DefaultRealm is the default realm sent back when issuing a basic auth challenge.
	DefaultRealm = "InfluxDB"
)

// Config represents a configuration for a HTTP service.
type Config struct {
	Enabled            bool   `toml:"enabled"`
	BindAddress        string `toml:"bind-address"`
	AuthEnabled        bool   `toml:"auth-enabled"`
	LogEnabled         bool   `toml:"log-enabled"`
	WriteTracing       bool   `toml:"write-tracing"`
	HTTPSEnabled       bool   `toml:"https-enabled"`
	HTTPSCertificate   string `toml:"https-certificate"`
	HTTPSPrivateKey    string `toml:"https-private-key"`
	MaxRowLimit        int    `toml:"max-row-limit"`
	MaxConnectionLimit int    `toml:"max-connection-limit"`
	SharedSecret       string `toml:"shared-secret"`
	Realm              string `toml:"realm"`
}

// NewConfig returns a new Config with default settings.
func NewConfig() Config {
	return Config{
		Enabled:          true,
		BindAddress:      DefaultBindAddress,
		LogEnabled:       true,
		HTTPSEnabled:     false,
		HTTPSCertificate: "/etc/ssl/influxdb.pem",
		MaxRowLimit:      DefaultChunkSize,
		Realm:            DefaultRealm,
	}
}
