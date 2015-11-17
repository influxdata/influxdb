package stress

import (
	"github.com/BurntSushi/toml"
)

// Config is a struct for the Stress test configuration
type Config struct {
	Provision Provision `toml:"provision"`
	Write     Write     `toml:"write"`
	Read      Read      `toml:"read"`
}

// Provision is a struct that contains the configuration
// parameters for all implemented Provisioner's.
type Provision struct {
	Basic BasicProvisioner `toml:"basic"`
}

// Write is a struct that contains the configuration
// parameters for the stress test Writer.
type Write struct {
	PointGenerators PointGenerators `toml:"point_generator"`
	InfluxClients   InfluxClients   `toml:"influx_client"`
}

// PointGenerators is a struct that contains the configuration
// parameters for all implemented PointGenerator's.
type PointGenerators struct {
	Basic BasicPointGenerator `toml:"basic"`
}

// InfluxClients is a struct that contains the configuration
// parameters for all implemented InfluxClient's.
type InfluxClients struct {
	Basic BasicClient `toml:"basic"`
}

// Read is a struct that contains the configuration
// parameters for the stress test Reader.
type Read struct {
	QueryGenerators QueryGenerators `toml:"query_generator"`
	QueryClients    QueryClients    `toml:"query_client"`
}

// QueryGenerators is a struct that contains the configuration
// parameters for all implemented QueryGenerator's.
type QueryGenerators struct {
	Basic BasicQuery `toml:"basic"`
}

// QueryClients is a struct that contains the configuration
// parameters for all implemented QueryClient's.
type QueryClients struct {
	Basic BasicQueryClient `toml:"basic"`
}

// NewConfig returns a pointer to a Config
func NewConfig(s string) (*Config, error) {
	var c *Config
	var err error

	if s == "" {
		c, err = BasicStress()
	} else {
		c, err = DecodeFile(s)
	}

	return c, err
}

// DecodeFile takes a file path for a toml config file
// and returns a pointer to a Config Struct.
func DecodeFile(s string) (*Config, error) {
	t := &Config{}

	// Decode the toml file
	if _, err := toml.DecodeFile(s, t); err != nil {
		return nil, err
	}

	return t, nil
}

// DecodeConfig takes a file path for a toml config file
// and returns a pointer to a Config Struct.
func DecodeConfig(s string) (*Config, error) {
	t := &Config{}

	// Decode the toml file
	if _, err := toml.Decode(s, t); err != nil {
		return nil, err
	}

	return t, nil
}
