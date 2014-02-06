package configuration

import (
	. "launchpad.net/gocheck"
	"testing"
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) {
	TestingT(t)
}

type LoadConfigurationSuite struct{}

var _ = Suite(&LoadConfigurationSuite{})

func (self *LoadConfigurationSuite) TestConfig(c *C) {
	config := LoadConfiguration("config.toml")
	c.Assert(config.Hostname, Equals, "")

	c.Assert(config.LogFile, Equals, "influxdb.log")
	c.Assert(config.LogLevel, Equals, "info")

	c.Assert(config.AdminAssetsDir, Equals, "./admin")
	c.Assert(config.AdminHttpPort, Equals, 8083)

	// the default should be 100, this shouldn't be set in the test toml
	// file
	c.Assert(config.LevelDbMaxOpenFiles, Equals, 100)

	c.Assert(config.ApiHttpPort, Equals, 0)
	c.Assert(config.ApiHttpSslPort, Equals, 8087)
	c.Assert(config.ApiHttpCertPath, Equals, "../cert.pem")
	c.Assert(config.ApiHttpPortString(), Equals, "")

	c.Assert(config.RaftDir, Equals, "/tmp/influxdb/development/raft")
	c.Assert(config.RaftServerPort, Equals, 8090)

	c.Assert(config.DataDir, Equals, "/tmp/influxdb/development/db")

	c.Assert(config.ProtobufPort, Equals, 8099)
	c.Assert(config.SeedServers, DeepEquals, []string{"hosta:8090", "hostb:8090"})
}
