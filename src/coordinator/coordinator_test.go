package coordinator

import (
	. "launchpad.net/gocheck"
	"testing"
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) {
	TestingT(t)
}

type CoordinatorSuite struct{}

var _ = Suite(&CoordinatorSuite{})

func (self *CoordinatorSuite) TestCanCreateCoordinatorWithNoSeed(c *C) {
}

func (self *CoordinatorSuite) TestCanCreateCoordinatorWithSeedThatIsNotRunning(c *C) {
}
