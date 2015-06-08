package gorpc

const (
	DefaultNetwork = "unix"
	DefaultLaddr   = "/tmp/influxdb.sock"
)

type Config struct {
	Enabled     bool   `toml:"enabled"`
	Network     string `toml:"network"`
	Laddr       string `toml:"laddr"`
	AuthEnabled bool   `toml:"auth-enabled"`
}

// NewConfig returns a new instance of Config with defaults.
func NewConfig() Config {
	return Config{
		Network: DefaultNetwork,
		Laddr:   DefaultLaddr,
	}
}
