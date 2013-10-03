package protocol

import (
	. "launchpad.net/gocheck"
	"testing"
	"time"
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) {
	TestingT(t)
}

type ProtocolSuite struct{}

var _ = Suite(&ProtocolSuite{})

func (self *ProtocolSuite) TestCanMarshalAndUnmarshal(c *C) {
	p := &Point{}
	v := &FieldValue{}

	f := float64(232342332233432)
	v.DoubleValue = &f
	p.Values = []*FieldValue{v}
	t := time.Now().Unix()
	p.Timestamp = &t
	s := uint32(23432423)
	p.SequenceNumber = &s

	d, err := MarshalPoint(p)
	c.Assert(err, Equals, nil)
	c.Assert(len(d), Equals, 22)

	point, err2 := UnmarshalPoint(d)
	c.Assert(err2, Equals, nil)
	c.Assert(point.Values[0].GetDoubleValue(), Equals, f)
}
