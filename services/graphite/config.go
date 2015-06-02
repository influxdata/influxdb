package graphite

import "strings"

const (
	// DefaultDatabase is the default database if none is specified.
	DefaultDatabase = "graphite"

	// DefaultNameSeparator represents the default field separator.
	DefaultNameSeparator = "."
)

// Config represents the configuration for Graphite endpoints.
type Config struct {
	BindAddress   string `toml:"bind-address"`
	Database      string `toml:"database"`
	Enabled       bool   `toml:"enabled"`
	Protocol      string `toml:"protocol"`
	NamePosition  string `toml:"name-position"`
	NameSeparator string `toml:"name-separator"`
}

// NewConfig returns a new Config with defaults.
func NewConfig() Config {
	return Config{
		Database:      DefaultDatabase,
		NameSeparator: DefaultNameSeparator,
	}
}

// LastEnabled returns whether the server should interpret the last field as "name".
func (c *Config) LastEnabled() bool {
	return c.NamePosition == strings.ToLower("last")
}
