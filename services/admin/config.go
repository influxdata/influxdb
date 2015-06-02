package admin

const (
	// DefaultBindAddress is the default bind address for the HTTP server.
	DefaultBindAddress = ":8083"
)

type Config struct {
	Enabled     bool   `toml:"enabled"`
	BindAddress string `toml:"bind-address"`
}

func NewConfig() Config {
	return Config{
		BindAddress: DefaultBindAddress,
	}
}
