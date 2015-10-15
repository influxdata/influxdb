package subscriber

type Config struct {
	// Whether to enable to Subscriber service
	Enabled bool `toml:"enabled"`
}

func NewConfig() Config {
	return Config{Enabled: true}
}
