package collectd

type Config struct {
	Enabled     bool   `toml:"enabled"`
	BindAddress string `toml:"bind-address"`
	Database    string `toml:"database"`
	TypesDB     string `toml:"typesdb"`
}

func NewConfig() Config {
	return Config{}
}
