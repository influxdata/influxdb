package bootstrap

import (
	"errors"

	"github.com/influxdata/influxdb/services/meta"
)

// Config represents bootstrap configuration which is applied
// at each server startup.
type Config struct {
	// AdminUsername, if set (along with AdminPassword), will ensure an admin username is created with this
	// username and password upon startup.
	AdminUsername string `toml:"admin_username"`

	// AdminPassword, see docs for AdminUsername
	AdminPassword string `toml:"admin_password"`

	// SeedDatabases, if set, are a list of databases that will be created upon startup, if they do
	// not already exist.
	SeedDatabases []string `toml:"seed_databases"`
}

// NewConfig returns a new Config with default settings.
func NewConfig() Config {
	return Config{}
}

func (c *Config) bootstrapAdminUser(mc *meta.Client) error {
	if c.AdminUsername == "" || c.AdminPassword == "" {
		return errors.New("must set either both bootstrap admin_username and admin_password or neither")
	}

	// See if we exist
	_, err := mc.Authenticate(c.AdminUsername, c.AdminPassword)
	switch err {
	case meta.ErrUserNotFound:
		// Create user with right password and admin level and we're done
		_, err = mc.CreateUser(c.AdminUsername, c.AdminPassword, true)
		return err

	case meta.ErrAuthenticate:
		// Password is wrong, update
		err = mc.UpdateUser(c.AdminUsername, c.AdminPassword)
		if err != nil {
			return err
		}

		// We're not sure if we're admin, so set anyway
		return mc.SetAdminPrivilege(c.AdminUsername, true)

	case nil:
		// We exist and have right password, but we're not sure if we're admin or not so set anyway
		return mc.SetAdminPrivilege(c.AdminUsername, true)

	default:
		// Unknown error, fail
		return err
	}
}

// Apply will apply this bootstrap configuration to the given client
func (c *Config) Apply(mc *meta.Client) error {
	// Bootstrap admin user
	if c.AdminUsername != "" || c.AdminPassword != "" {
		err := c.bootstrapAdminUser(mc)
		if err != nil {
			return err
		}
	}

	// Bootstrap databases
	for _, db := range c.SeedDatabases {
		// Create will return the db if it already exists
		_, err := mc.CreateDatabase(db)
		if err != nil {
			return err
		}
	}

	return nil
}
