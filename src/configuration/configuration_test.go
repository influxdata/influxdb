package configuration

import (
	. "launchpad.net/gocheck"
	"testing"
	"time"
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
	c.Assert(config.ProtobufHeartbeatInterval.Duration, Equals, 200*time.Millisecond)
	c.Assert(config.ProtobufTimeout.Duration, Equals, 2*time.Second)
	c.Assert(config.SeedServers, DeepEquals, []string{"hosta:8090", "hostb:8090"})

	c.Assert(config.WalDir, Equals, "/tmp/influxdb/development/wal")
	c.Assert(config.WalFlushAfterRequests, Equals, 0)
	c.Assert(config.WalBookmarkAfterRequests, Equals, 0)
	c.Assert(config.WalIndexAfterRequests, Equals, 1000)
	c.Assert(config.WalRequestsPerLogFile, Equals, 10000)
}

func (self *LoadConfigurationSuite) TestSizeParsing(c *C) {
	var s size
	c.Assert(s.UnmarshalText([]byte("200m")), IsNil)
	c.Assert(s.int, Equals, 200*ONE_MEGABYTE)
	c.Assert(s.UnmarshalText([]byte("10g")), IsNil)
	c.Assert(s.int, Equals, 10*ONE_GIGABYTE)
}
