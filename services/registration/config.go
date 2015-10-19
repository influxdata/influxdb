package registration

import ()

const (
	DefaultURL = "https://enterprise.influxdata.com"
)

type Config struct {
	Enabled bool   `toml:"enabled"`
	URL     string `toml:"url"`
	Token   string `toml:"token"`
}

func NewConfig() Config {
	return Config{
		Enabled: true,
		URL:     DefaultURL,
	}
}
