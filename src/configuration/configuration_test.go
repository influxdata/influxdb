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
	config := LoadConfiguration("../../config.json.sample")
	c.Assert(config.DataDir, Equals, "/tmp/influxdb/development/db")
}
