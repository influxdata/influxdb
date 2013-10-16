package datastore

import (
	"encoding/json"
	. "launchpad.net/gocheck"
	"os"
	"parser"
	"protocol"
	"testing"
	"time"
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) {
	TestingT(t)
}

type DatastoreSuite struct{}

var _ = Suite(&DatastoreSuite{})

const DB_DIR = "/tmp/chronosdb/datastore_test"

func newDatastore(c *C) Datastore {
	os.MkdirAll(DB_DIR, 0744)
	db, err := NewLevelDbDatastore(DB_DIR)
	c.Assert(err, Equals, nil)
	return db
}

func cleanup(db Datastore) {
	db.Close()
	os.RemoveAll(DB_DIR)
}

func stringToSeries(seriesString string, timestamp int64, c *C) *protocol.Series {
	series := &protocol.Series{}
	err := json.Unmarshal([]byte(seriesString), &series)
	c.Assert(err, IsNil)
	for _, point := range series.Points {
		point.Timestamp = &timestamp
	}
	return series
}

func (self *DatastoreSuite) TestCanWriteAndRetrievePoints(c *C) {
	db := newDatastore(c)
	defer cleanup(db)
	mock := `
  {
    "points": [
      {
        "values": [
          {
            "int_value": 2
          }
        ],
        "sequence_number": 2
      },
      {
        "values": [
          {
            "int_value": 3
          }
        ],
        "sequence_number": 1
      }
    ],
    "name": "foo",
    "fields": [
      {
        "type": "INT32",
        "name": "value"
      }
    ]
  }`
	pointTime := time.Now().Unix()
	series := stringToSeries(mock, pointTime, c)
	err := db.WriteSeriesData("test", series)
	c.Assert(err, IsNil)
	q, errQ := parser.ParseQuery("select value from foo;")
	c.Assert(errQ, IsNil)
	done := make(chan int, 1)
	resultSeries := &protocol.Series{}
	yield := func(series *protocol.Series) error {
		resultSeries = series
		done <- 1
		return nil
	}
	err = db.ExecuteQuery("test", q, yield)
	c.Assert(err, IsNil)
	<-done
	c.Assert(resultSeries, Not(IsNil))
	c.Assert(len(resultSeries.Points), Equals, 2)
	c.Assert(len(resultSeries.Fields), Equals, 1)
	c.Assert(*resultSeries.Points[0].SequenceNumber, Equals, uint32(2))
	c.Assert(*resultSeries.Points[1].SequenceNumber, Equals, uint32(1))
	c.Assert(*resultSeries.Points[0].Timestamp, Equals, pointTime)
	c.Assert(*resultSeries.Points[1].Timestamp, Equals, pointTime)
	c.Assert(*resultSeries.Points[0].Values[0].IntValue, Equals, int32(2))
	c.Assert(*resultSeries.Points[1].Values[0].IntValue, Equals, int32(3))
}
