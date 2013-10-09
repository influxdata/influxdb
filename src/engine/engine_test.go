package engine

import (
	"coordinator"
	. "launchpad.net/gocheck"
	"parser"
	"protocol"
	"testing"
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) {
	TestingT(t)
}

type EngineSuite struct{}

var _ = Suite(&EngineSuite{})

type MockCoordinator struct {
}

func (self *MockCoordinator) DistributeQuery(query *parser.Query, yield func(*protocol.Series) error) error {
	return nil
}

func (self *MockCoordinator) WriteSeriesData(series *protocol.Series) error {
	return nil
}

func createMockCoordinator() coordinator.Coordinator {
	return &MockCoordinator{}
}

func (self *EngineSuite) TestBasicQuery(c *C) {
	engine, err := NewQueryEngine(createMockCoordinator())
	c.Assert(err, IsNil)
	c.Assert(engine, NotNil)
}
