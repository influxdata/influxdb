package opentsdb

type Config struct {
	Enabled         bool   `toml:"enabled"`
	BindAddress     string `toml:"bind-address"`
	Database        string `toml:"database"`
	RetentionPolicy string `toml:"retention-policy"`
}

func NewConfig() Config {
	return Config{}
}
