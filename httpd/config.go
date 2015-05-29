package httpd

type Config struct {
	BindAddress  string `toml:"bind-address"`
	AuthEnabled  bool   `toml:"auth-enabled"`
	LogEnabled   bool   `toml:"log-enabled"`
	WriteTracing bool   `toml:"write-tracing"`
	PprofEnabled bool   `toml:"pprof-enabled"`
}

func NewConfig() Config {
	return Config{
		LogEnabled:   true,
		WriteTracing: false,
	}
}
