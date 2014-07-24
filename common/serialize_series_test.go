package common

import (
	"bytes"
	"encoding/json"
	. "launchpad.net/gocheck"
	"testing"
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) {
	TestingT(t)
}

type SerializeSeriesSuite struct{}

var _ = Suite(&SerializeSeriesSuite{})

func (self *SerializeSeriesSuite) TestValidApiSeries(c *C) {
	serializedSeries := serializeSeriesJson([]byte(`
  {
    "name" : "hd_used",
    "columns" : ["time", "value", "host", "mount"],
    "points" : [
      [1403761091094, 23.2, "serverA", "/mnt"]
    ]
  }`))
	_, err := ConvertToDataStoreSeries(serializedSeries, MillisecondPrecision)
	c.Assert(err, IsNil)
}

func (self *SerializeSeriesSuite) TestDuplicateTimeApiSeries(c *C) {
	serializedSeries := serializeSeriesJson([]byte(`
  {
    "name" : "hd_used",
    "columns" : ["time", "value", "time", "host", "mount"],
    "points" : [
      [1403761091094, 23.2, 1403761091094, "serverA", "/mnt"]
    ]
  }`))
	_, err := ConvertToDataStoreSeries(serializedSeries, MillisecondPrecision)
	c.Assert(err, Not(IsNil))
}

func (self *SerializeSeriesSuite) TestDuplicateSequenceNumberApiSeries(c *C) {
	serializedSeries := serializeSeriesJson([]byte(`
  {
    "name" : "hd_used",
    "columns" : ["time", "sequence_number", "sequence_number", "host", "mount"],
    "points" : [
      [1403761091094, 100, 100, "serverA", "/mnt"]
    ]
  }`))
	_, err := ConvertToDataStoreSeries(serializedSeries, MillisecondPrecision)
	c.Assert(err, Not(IsNil))
}

func serializeSeriesJson(jsonData []byte) (serializedSeries *SerializedSeries) {
	decoder := json.NewDecoder(bytes.NewBuffer(jsonData))
	decoder.UseNumber()
	decoder.Decode(&serializedSeries)
	return
}
