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

	c.Assert(config.ApiHttpPort, Equals, 8086)

	c.Assert(config.RaftDir, Equals, "/tmp/influxdb/development/raft")
	c.Assert(config.RaftServerPort, Equals, 8090)

	c.Assert(config.DataDir, Equals, "/tmp/influxdb/development/db")

	c.Assert(config.ProtobufPort, Equals, 8099)
	c.Assert(config.SeedServers, DeepEquals, []string{"hosta:8090", "hostb:8090"})
}
