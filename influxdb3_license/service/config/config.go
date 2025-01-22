package config

import (
	"fmt"
	"net"
	"time"

	"github.com/alecthomas/kong"
)

// Config represents the configuration of the service
type Config struct {
	HTTPAddr             string        `help:"Address:port of the HTTP API" env:"IFLX_PRO_LIC_HTTP_ADDR" default:":8687"`
	LogLevel             string        `help:"Log level: error, warn, info (default), debug" env:"IFLX_PRO_LIC_LOG_LEVEL" default:"info"`
	LogFormat            string        `help:"Log format: auto, logfmt, json" env:"IFLX_PRO_LIC_LOG_FORMAT" default:"auto"`
	DBConnString         string        `help:"Database connection string" env:"IFLX_PRO_LIC_DB_CONN_STRING" default:"postgres://postgres:postgres@localhost:5432/influxdb_pro_license?sslmode=disable"`
	EmailDomain          string        `help:"Email domain name" env:"IFLX_PRO_LIC_EMAIL_DOMAIN" default:"mailgun.influxdata.com"`
	EmailAPIKey          string        `help:"Email api key" env:"IFLX_PRO_LIC_EMAIL_API_KEY" default:"log-only"`
	EmailVerificationURL string        `help:"Email verification base URL" env:"IFLX_PRO_LIC_EMAIL_VERIFICATION_URL" default:"http://localhost:8687"`
	EmailTemplateName    string        `help:"Email template name" env:"IFLX_PRO_LIC_EMAIL_TEMPLATE_NAME" default:"influxdb 3 enterprise verification"`
	EmailMaxRetries      int           `help:"Maximum number of email retries" env:"IFLX_PRO_LIC_EMAIL_MAX_RETRIES" default:"3"`
	PrivateKey           string        `help:"Private key path" env:"IFLX_PRO_LIC_PRIVATE_KEY" default:"projects/influxdata-v3-pro-licensing/locations/global/keyRings/pro-licensing/cryptoKeys/signing-key-1/cryptoKeyVersions/1"`
	PublicKey            string        `help:"Public key path" env:"IFLX_PRO_LIC_PUBLIC_KEY" default:"gcloud-kms_global_pro-licensing_signing-key-1_v1.pem"`
	TrialDuration        time.Duration `help:"Trial license duration (e.g. 2160h for 90 days)" env:"IFLX_PRO_LIC_TRIAL_DURATION" default:"2160h"`
	TrialEndDate         time.Time     `help:"A fixed date that all trials end. Ignored if empty or expired and TrialDuration used instead" env:"IFLX_PRO_LIC_TRIAL_END_DATE"`
	TrustedProxies       []string      `help:"Trusted proxy CIDR ranges (e.g., '10.0.0.0/8,172.16.0.0/12')" env:"IFLX_PRO_LIC_TRUSTED_PROXIES" default:"127.0.0.1/32"`
	LocalSigner          bool          `help:"Use the local dev signer" env:"IFLX_PRO_LIC_LOCAL_SIGNER" default:"false"`
	trustedProxies       []net.IPNet
}

// Parse parses the config from environment vars and command line arguments.
// The order of precedence is:
//  1. Command line arguments
//  2. Environment variables
func Parse(args []string) (*Config, error) {
	config := &Config{}

	// Create a new Kong parser
	parser, err := kong.New(config)
	if err != nil {
		return nil, err
	}

	// Parse the command line arguments
	_, err = parser.Parse(args)
	if err != nil {
		return nil, err
	}

	config.trustedProxies, err = parseCIDRs(config.TrustedProxies)
	if err != nil {
		return nil, fmt.Errorf("error parsing trusted proxies: %v", err)
	}

	return config, nil
}

func parseCIDRs(cidrs []string) ([]net.IPNet, error) {
	var networks []net.IPNet
	for _, cidr := range cidrs {
		_, network, err := net.ParseCIDR(cidr)
		if err != nil {
			return nil, fmt.Errorf("invalid CIDR %q: %w", cidr, err)
		}
		networks = append(networks, *network)
	}
	return networks, nil
}

func (c *Config) GetTrustedProxies() []net.IPNet {
	return c.trustedProxies
}
