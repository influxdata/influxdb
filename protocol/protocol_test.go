package protocol

import (
	"bytes"
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

func (self *ProtocolSuite) TestCanEncodeAndDecode(c *C) {
	p := &Point{}
	v := &FieldValue{}

	f := float64(232342332233432)
	v.DoubleValue = &f
	p.Values = []*FieldValue{v}
	t := time.Now().Unix()
	p.Timestamp = &t
	s := uint64(23432423)
	p.SequenceNumber = &s

	d, err := p.Encode()
	c.Assert(err, Equals, nil)
	c.Assert(len(d), Equals, 22)

	point, err2 := DecodePoint(bytes.NewBuffer(d))
	c.Assert(err2, Equals, nil)
	c.Assert(point.Values[0].GetDoubleValue(), Equals, f)
}
