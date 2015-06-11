package opentsdb

const (
	// DefaultBindAddress is the default address that the service binds to.
	DefaultBindAddress = ":4242"

	// DefaultDatabase is the default database used for writes.
	DefaultDatabase = "opentsdb"

	// DefaultRetentionPolicy is the default retention policy used for writes.
	DefaultRetentionPolicy = ""

	// DefaultConsistencyLevel is the default write consistency level.
	DefaultConsistencyLevel = "one"
)

type Config struct {
	Enabled          bool   `toml:"enabled"`
	BindAddress      string `toml:"bind-address"`
	Database         string `toml:"database"`
	RetentionPolicy  string `toml:"retention-policy"`
	ConsistencyLevel string `toml:"consistency-level"`
}

func NewConfig() Config {
	return Config{
		BindAddress:      DefaultBindAddress,
		Database:         DefaultDatabase,
		RetentionPolicy:  DefaultRetentionPolicy,
		ConsistencyLevel: DefaultConsistencyLevel,
	}
}

// WithDefaults takes the given config and returns a new config with any required
// default values set.
func (c *Config) WithDefaults() *Config {
	d := *c
	if d.BindAddress == "" {
		d.BindAddress = DefaultBindAddress
	}
	if d.Database == "" {
		d.Database = DefaultDatabase
	}
	if d.RetentionPolicy == "" {
		d.RetentionPolicy = DefaultRetentionPolicy
	}
	if d.ConsistencyLevel == "" {
		d.ConsistencyLevel = DefaultConsistencyLevel
	}
	return &d
}
