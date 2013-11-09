package server

import (
	. "launchpad.net/gocheck"
	"runtime"
	"testing"
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) {
	TestingT(t)
}

type ServerSuite struct{}

var _ = Suite(&ServerSuite{})

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)
}

func (self *ServerSuite) TestDataReplication(c *C) {
}

func (self *ServerSuite) TestCrossClusterQueries(c *C) {

}
