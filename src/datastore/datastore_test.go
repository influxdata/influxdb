package datastore

import (
	. "checkers"
	"common"
	"encoding/json"
	"fmt"
	. "launchpad.net/gocheck"
	"os"
	"parser"
	"protocol"
	"regexp"
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
	if db != nil {
		db.Close()
	}
	os.RemoveAll(DB_DIR)
}

func stringToSeries(seriesString string, timestamp int64, c *C) *protocol.Series {
	series := &protocol.Series{}
	err := json.Unmarshal([]byte(seriesString), &series)
	c.Assert(err, IsNil)
	timestamp *= 1000000
	for _, point := range series.Points {
		point.Timestamp = &timestamp
	}
	return series
}

func executeQuery(user common.User, database, query string, db Datastore, c *C) []*protocol.Series {
	q, errQ := parser.ParseQuery(query)
	c.Assert(errQ, IsNil)
	resultSeries := []*protocol.Series{}
	yield := func(series *protocol.Series) error {
		// ignore time series which have no data, this includes
		// end of series indicator
		if len(series.Points) > 0 {
			resultSeries = append(resultSeries, series)
		}
		return nil
	}
	err := db.ExecuteQuery(user, database, q, yield)
	c.Assert(err, IsNil)
	return resultSeries
}

func (self *DatastoreSuite) TestPropagateErrorsProperly(c *C) {
	cleanup(nil)
	db := newDatastore(c)
	defer cleanup(db)
	mock := `
  {
    "points": [
      {
        "values": [
          {
            "int64_value": 3
          }
        ],
        "sequence_number": 1
      }
    ],
    "name": "foo",
    "fields": ["value"]
  }`
	pointTime := time.Now().Unix()
	series := stringToSeries(mock, pointTime, c)
	err := db.WriteSeriesData("test", series)
	c.Assert(err, IsNil)
	q, err := parser.ParseQuery("select value from foo;")
	c.Assert(err, IsNil)
	yield := func(series *protocol.Series) error {
		return fmt.Errorf("Whatever")
	}
	user := &MockUser{}
	err = db.ExecuteQuery(user, "test", q, yield)
	c.Assert(err, ErrorMatches, "Whatever")
}

func (self *DatastoreSuite) TestDeletingData(c *C) {
	cleanup(nil)
	db := newDatastore(c)
	defer cleanup(db)
	mock := `
  {
    "points": [
      {
        "values": [
          {
            "int64_value": 3
          }
        ],
        "sequence_number": 1
      }
    ],
    "name": "foo",
    "fields": ["value"]
  }`
	pointTime := time.Now().Unix()
	series := stringToSeries(mock, pointTime, c)
	err := db.WriteSeriesData("test", series)
	c.Assert(err, IsNil)
	q, err := parser.ParseQuery("select value from foo;")
	c.Assert(err, IsNil)
	yield := func(series *protocol.Series) error {
		if len(series.Points) > 0 {
			panic("Series contains points")
		}
		return nil
	}
	c.Assert(db.DropDatabase("test"), IsNil)
	user := &MockUser{}
	err = db.ExecuteQuery(user, "test", q, yield)
	c.Assert(err, ErrorMatches, ".*Field value doesn't exist.*")
}

func (self *DatastoreSuite) TestCanWriteAndRetrievePointsWithAlias(c *C) {
	cleanup(nil)
	db := newDatastore(c)
	defer cleanup(db)
	mock := `
  {
    "points": [
      {
        "values": [
          {
            "int64_value": 3
          }
        ],
        "sequence_number": 1
      },
      {
        "values": [
          {
            "int64_value": 2
          }
        ],
        "sequence_number": 2
      }
    ],
    "name": "foo",
    "fields": ["value"]
  }`
	pointTime := time.Now().Unix()
	series := stringToSeries(mock, pointTime, c)
	err := db.WriteSeriesData("test", series)
	c.Assert(err, IsNil)
	q, errQ := parser.ParseQuery("select * from foo as f1 inner join foo as f2;")
	c.Assert(errQ, IsNil)
	resultSeries := map[string][]*protocol.Series{}
	yield := func(series *protocol.Series) error {
		resultSeries[*series.Name] = append(resultSeries[*series.Name], series)
		return nil
	}
	user := &MockUser{}
	err = db.ExecuteQuery(user, "test", q, yield)
	c.Assert(err, IsNil)
	// we should get the actual data and the end of series data
	// indicator , i.e. a series with no points
	c.Assert(resultSeries, HasLen, 2)
	c.Assert(resultSeries["f1"], HasLen, 2)
	c.Assert(resultSeries["f1"][0].Points, HasLen, 2)
	c.Assert(resultSeries["f1"][1].Points, HasLen, 0)
	c.Assert(resultSeries["f2"], HasLen, 2)
	c.Assert(resultSeries["f2"][0].Points, HasLen, 2)
	c.Assert(resultSeries["f2"][1].Points, HasLen, 0)
}

func (self *DatastoreSuite) TestCanWriteAndRetrievePoints(c *C) {
	cleanup(nil)
	db := newDatastore(c)
	defer cleanup(db)
	mock := `
  {
    "points": [
      {
        "values": [
          {
            "int64_value": 3
          }
        ],
        "sequence_number": 1
      },
      {
        "values": [
          {
            "int64_value": 2
          }
        ],
        "sequence_number": 2
      }
    ],
    "name": "foo",
    "fields": ["value"]
  }`
	pointTime := time.Now().Unix()
	series := stringToSeries(mock, pointTime, c)
	err := db.WriteSeriesData("test", series)
	c.Assert(err, IsNil)
	q, errQ := parser.ParseQuery("select value from foo;")
	c.Assert(errQ, IsNil)
	resultSeries := []*protocol.Series{}
	yield := func(series *protocol.Series) error {
		resultSeries = append(resultSeries, series)
		return nil
	}
	user := &MockUser{}
	err = db.ExecuteQuery(user, "test", q, yield)
	c.Assert(err, IsNil)
	// we should get the actual data and the end of series data
	// indicator , i.e. a series with no points
	c.Assert(resultSeries, HasLen, 2)
	c.Assert(resultSeries[0].Points, HasLen, 2)
	c.Assert(resultSeries[0].Fields, HasLen, 1)
	c.Assert(*resultSeries[0].Points[0].SequenceNumber, Equals, uint32(2))
	c.Assert(*resultSeries[0].Points[1].SequenceNumber, Equals, uint32(1))
	c.Assert(*resultSeries[0].Points[0].GetTimestampInMicroseconds(), Equals, pointTime*1000000)
	c.Assert(*resultSeries[0].Points[1].GetTimestampInMicroseconds(), Equals, pointTime*1000000)
	c.Assert(*resultSeries[0].Points[0].Values[0].Int64Value, Equals, int64(2))
	c.Assert(*resultSeries[0].Points[1].Values[0].Int64Value, Equals, int64(3))
	c.Assert(resultSeries[1].Points, HasLen, 0)
	c.Assert(resultSeries[1].Fields, HasLen, 1)
	c.Assert(resultSeries, Not(DeepEquals), series)
}

func (self *DatastoreSuite) TestCanPersistDataAndWriteNewData(c *C) {
	cleanup(nil)
	db := newDatastore(c)
	mock := `
  {
    "points": [
      {
        "values": [
          {
            "int64_value": 3
          }
        ],
        "sequence_number": 1
      }
    ],
    "name": "foo",
    "fields": ["value"]
  }`
	series := stringToSeries(mock, time.Now().Unix(), c)
	err := db.WriteSeriesData("asdf", series)
	c.Assert(err, IsNil)
	user := &MockUser{}
	results := executeQuery(user, "asdf", "select value from foo;", db, c)
	c.Assert(results[0], DeepEquals, series)
	db.Close()
	db = newDatastore(c)
	defer cleanup(db)
	results = executeQuery(user, "asdf", "select value from foo;", db, c)
	c.Assert(results[0], DeepEquals, series)
}

func (self *DatastoreSuite) TestCanWriteDataWithDifferentTimesAndSeries(c *C) {
	cleanup(nil)
	db := newDatastore(c)
	defer cleanup(db)
	mock := `{
    "points":[{"values":[{"double_value":23.2}],"sequence_number":3}],
    "name": "events",
    "fields": ["blah"]
  }`
	secondAgo := time.Now().Add(-time.Second).Unix()
	eventsSeries := stringToSeries(mock, secondAgo, c)
	err := db.WriteSeriesData("db1", eventsSeries)
	c.Assert(err, IsNil)
	mock = `{
    "points":[{"values":[{"int64_value":4}],"sequence_number":3}],
    "name": "foo",
    "fields": ["val"]
  }`
	fooSeries := stringToSeries(mock, secondAgo, c)
	err = db.WriteSeriesData("db1", fooSeries)
	c.Assert(err, IsNil)

	user := &MockUser{}
	results := executeQuery(user, "db1", "select blah from events;", db, c)
	c.Assert(results[0], DeepEquals, eventsSeries)
	results = executeQuery(user, "db1", "select val from foo;", db, c)
	c.Assert(results[0], DeepEquals, fooSeries)

	now := time.Now().Unix()
	mock = `{
    "points":[{"values":[{"double_value": 0.1}],"sequence_number":1}],
    "name":"events",
    "fields": ["blah"]
  }`

	newEvents := stringToSeries(mock, now, c)
	err = db.WriteSeriesData("db1", newEvents)
	c.Assert(err, IsNil)

	results = executeQuery(user, "db1", "select blah from events;", db, c)
	c.Assert(results[0].Points, HasLen, 2)
	c.Assert(results[0].Fields, HasLen, 1)
	c.Assert(*results[0].Points[0].SequenceNumber, Equals, uint32(1))
	c.Assert(*results[0].Points[1].SequenceNumber, Equals, uint32(3))
	c.Assert(*results[0].Points[0].GetTimestampInMicroseconds(), Equals, now*1000000)
	c.Assert(*results[0].Points[1].GetTimestampInMicroseconds(), Equals, secondAgo*1000000)
	c.Assert(*results[0].Points[0].Values[0].DoubleValue, Equals, float64(0.1))
	c.Assert(*results[0].Points[1].Values[0].DoubleValue, Equals, float64(23.2))
	results = executeQuery(user, "db1", "select val from foo;", db, c)
	c.Assert(results[0], DeepEquals, fooSeries)
}

func (self *DatastoreSuite) TestCanWriteDataToDifferentDatabases(c *C) {
	cleanup(nil)
	db := newDatastore(c)
	defer cleanup(db)
	mock := `{
    "points":[{"values":[{"double_value":23.2}],"sequence_number":3}],
    "name": "events",
    "fields": ["blah"]
  }`
	secondAgo := time.Now().Add(-time.Second).Unix()
	db1Series := stringToSeries(mock, secondAgo, c)
	err := db.WriteSeriesData("db1", db1Series)
	c.Assert(err, IsNil)
	mock = `{
    "points":[{"values":[{"double_value":3.2}],"sequence_number":2}],
    "name": "events",
    "fields": ["blah"]
  }`
	otherDbSeries := stringToSeries(mock, secondAgo, c)
	err = db.WriteSeriesData("other_db", otherDbSeries)
	c.Assert(err, IsNil)

	user := &MockUser{}
	results := executeQuery(user, "db1", "select blah from events;", db, c)
	c.Assert(results[0], DeepEquals, db1Series)
	results = executeQuery(user, "other_db", "select blah from events;", db, c)
	c.Assert(results[0], DeepEquals, otherDbSeries)
}

func (self *DatastoreSuite) TestCanQueryBasedOnTime(c *C) {
	cleanup(nil)
	db := newDatastore(c)
	defer cleanup(db)

	minutesAgo := time.Now().Add(-10 * time.Minute).Unix()
	now := time.Now().Unix()
	mock := `{
    "points":[{"values":[{"int64_value":4}],"sequence_number":3}],
    "name": "foo",
    "fields": ["val"]
  }`
	oldData := stringToSeries(mock, minutesAgo, c)
	err := db.WriteSeriesData("db1", oldData)
	c.Assert(err, IsNil)

	mock = `{
    "points":[{"values":[{"int64_value":3}],"sequence_number":3}],
    "name": "foo",
    "fields": ["val"]
  }`
	newData := stringToSeries(mock, now, c)
	err = db.WriteSeriesData("db1", newData)
	c.Assert(err, IsNil)

	user := &MockUser{}
	results := executeQuery(user, "db1", "select val from foo where time>now()-1m;", db, c)
	c.Assert(results[0], DeepEquals, newData)
	results = executeQuery(user, "db1", "select val from foo where time>now()-1h and time<now()-1m;", db, c)
	c.Assert(results[0], DeepEquals, oldData)

	results = executeQuery(user, "db1", "select val from foo;", db, c)
	c.Assert(results[0].Points, HasLen, 2)
	c.Assert(results[0].Fields, HasLen, 1)
	c.Assert(*results[0].Points[0].SequenceNumber, Equals, uint32(3))
	c.Assert(*results[0].Points[1].SequenceNumber, Equals, uint32(3))
	c.Assert(*results[0].Points[0].GetTimestampInMicroseconds(), Equals, now*1000000)
	c.Assert(*results[0].Points[1].GetTimestampInMicroseconds(), Equals, minutesAgo*1000000)
	c.Assert(*results[0].Points[0].Values[0].Int64Value, Equals, int64(3))
	c.Assert(*results[0].Points[1].Values[0].Int64Value, Equals, int64(4))
}

func (self *DatastoreSuite) TestCanDoWhereQueryEquals(c *C) {
	cleanup(nil)
	db := newDatastore(c)
	defer cleanup(db)

	mock := `{
    "points":[{"values":[{"string_value":"paul"}],"sequence_number":2},{"values":[{"string_value":"todd"}],"sequence_number":1}],
    "name":"events",
    "fields":["name"]
    }`
	allData := stringToSeries(mock, time.Now().Unix(), c)
	err := db.WriteSeriesData("db1", allData)
	c.Assert(err, IsNil)

	user := &MockUser{}
	results := executeQuery(user, "db1", "select name from events;", db, c)
	c.Assert(results[0], DeepEquals, allData)
	results = executeQuery(user, "db1", "select name from events where name == 'paul';", db, c)
	c.Assert(results[0].Points, HasLen, 1)
	c.Assert(results[0].Fields, HasLen, 1)
	c.Assert(*results[0].Points[0].SequenceNumber, Equals, uint32(2))
	c.Assert(*results[0].Points[0].Values[0].StringValue, Equals, "paul")
}

func (self *DatastoreSuite) TestCanDoSelectStarQueries(c *C) {
	cleanup(nil)
	db := newDatastore(c)
	defer cleanup(db)

	mock := `{
    "points":[
      {"values":[{"int64_value":3},{"string_value":"paul"}],"sequence_number":2},
      {"values":[{"int64_value":1},{"string_value":"todd"}],"sequence_number":1}],
      "name":"user_things",
      "fields":["count","name"]
    }`
	series := stringToSeries(mock, time.Now().Unix(), c)
	err := db.WriteSeriesData("foobar", series)
	c.Assert(err, IsNil)
	user := &MockUser{}
	results := executeQuery(user, "foobar", "select * from user_things;", db, c)
	c.Assert(results[0], DeepEquals, series)
}

func (self *DatastoreSuite) TestCanDoCountStarQueries(c *C) {
	cleanup(nil)
	db := newDatastore(c)
	defer cleanup(db)

	mock := `{
    "points":[
      {"values":[{"int64_value":3},{"string_value":"paul"}],"sequence_number":2},
      {"values":[{"int64_value":1},{"string_value":"todd"}],"sequence_number":1}],
      "name":"user_things",
      "fields":["count","name"]
    }`
	series := stringToSeries(mock, time.Now().Unix(), c)
	err := db.WriteSeriesData("foobar", series)
	c.Assert(err, IsNil)
	user := &MockUser{}
	results := executeQuery(user, "foobar", "select count(*) from user_things;", db, c)
	c.Assert(results[0].Points, HasLen, 2)
	c.Assert(results[0].Fields, HasLen, 1)
	c.Assert(*results[0].Points[0].SequenceNumber, Equals, uint32(2))
	c.Assert(*results[0].Points[0].Values[0].Int64Value, Equals, int64(3))
	c.Assert(*results[0].Points[1].SequenceNumber, Equals, uint32(1))
	c.Assert(*results[0].Points[1].Values[0].Int64Value, Equals, int64(1))
}

func (self *DatastoreSuite) TestLimitsPointsReturnedBasedOnQuery(c *C) {
	cleanup(nil)
	db := newDatastore(c)
	defer cleanup(db)

	mock := `{
    "points":[
      {"values":[{"int64_value":3},{"string_value":"paul"}],"sequence_number":2},
      {"values":[{"int64_value":1},{"string_value":"todd"}],"sequence_number":1}],
      "name":"user_things",
      "fields":["count","name"]
    }`
	series := stringToSeries(mock, time.Now().Unix(), c)
	err := db.WriteSeriesData("foobar", series)
	c.Assert(err, IsNil)
	user := &MockUser{}
	results := executeQuery(user, "foobar", "select name from user_things limit 1;", db, c)
	c.Assert(results[0].Points, HasLen, 1)
	c.Assert(results[0].Fields, HasLen, 1)
	c.Assert(*results[0].Points[0].SequenceNumber, Equals, uint32(2))
	c.Assert(*results[0].Points[0].Values[0].StringValue, Equals, "paul")
}

func (self *DatastoreSuite) TestReturnsResultsInAscendingOrder(c *C) {
	cleanup(nil)
	db := newDatastore(c)
	defer cleanup(db)

	minuteAgo := time.Now().Add(-time.Minute).Unix()
	mock := `{
    "points":[
      {"values":[{"string_value":"paul"}],"sequence_number":1},
      {"values":[{"string_value":"todd"}],"sequence_number":2}],
      "name":"user_things",
      "fields":["name"]
    }`
	series := stringToSeries(mock, minuteAgo, c)
	err := db.WriteSeriesData("foobar", series)
	c.Assert(err, IsNil)
	user := &MockUser{}
	results := executeQuery(user, "foobar", "select name from user_things order asc;", db, c)
	c.Assert(results, HasLen, 1)
	c.Assert(results[0], DeepEquals, series)

	mock = `{
    "points":[
      {"values":[{"string_value":"john"}],"sequence_number":1}],
      "name":"user_things",
      "fields":["name"]
    }`
	newSeries := stringToSeries(mock, time.Now().Unix(), c)
	err = db.WriteSeriesData("foobar", newSeries)
	c.Assert(err, IsNil)
	results = executeQuery(user, "foobar", "select name from user_things order asc;", db, c)
	c.Assert(results[0].Points, HasLen, 3)
	c.Assert(*results[0].Points[0].Values[0].StringValue, Equals, "paul")
	c.Assert(*results[0].Points[1].Values[0].StringValue, Equals, "todd")
	c.Assert(*results[0].Points[2].Values[0].StringValue, Equals, "john")

	results = executeQuery(user, "foobar", "select name from user_things where time < now() - 30s order asc;", db, c)
	c.Assert(results[0], DeepEquals, series)
}

func (self *DatastoreSuite) TestReturnsResultsInAscendingOrderWithNulls(c *C) {
	cleanup(nil)
	db := newDatastore(c)
	defer cleanup(db)

	minuteAgo := time.Now().Add(-time.Minute).Unix()
	mock := `{
    "points":[
      {"values":[null, {"string_value": "dix"}],"sequence_number":1},
      {"values":[{"string_value":"todd"}, {"string_value": "persen"}],"sequence_number":2}],
      "name":"user_things",
      "fields":["first_name", "last_name"]
    }`
	series := stringToSeries(mock, minuteAgo, c)
	err := db.WriteSeriesData("foobar", series)
	c.Assert(err, IsNil)
	user := &MockUser{}
	results := executeQuery(user, "foobar", "select first_name, last_name from user_things order asc;", db, c)
	c.Assert(results, HasLen, 1)
	c.Assert(results[0], DeepEquals, series)
}

func (self *DatastoreSuite) TestNullValues(c *C) {
	cleanup(nil)
	db := newDatastore(c)
	defer cleanup(db)

	minuteAgo := time.Now().Add(-time.Minute).Unix()
	mock := `{
    "points":[
      {"values":[null, {"string_value": "dix"}],"sequence_number":1},
      {"values":[{"string_value": "dix"}, null],"sequence_number":2},
      {"values":[null, {"string_value": "dix"}],"sequence_number":3},
      {"values":[{"string_value":"todd"}, null],"sequence_number":4}],
      "name":"user_things",
      "fields":["first_name", "last_name"]
    }`
	series := stringToSeries(mock, minuteAgo, c)
	err := db.WriteSeriesData("foobar", series)
	c.Assert(err, IsNil)
	user := &MockUser{}
	results := executeQuery(user, "foobar", "select * from user_things", db, c)
	c.Assert(results, HasLen, 1)
	c.Assert(results[0].Points, HasLen, 4)
}

func (self *DatastoreSuite) TestLimitShouldLimitPointsThatMatchTheFilter(c *C) {
	cleanup(nil)
	db := newDatastore(c)
	defer cleanup(db)

	minuteAgo := time.Now().Add(-time.Minute).Unix()
	mock := `{
    "points":[
      {"values":[{"string_value": "paul"}, {"string_value": "dix"}],"sequence_number":1},
      {"values":[{"string_value":"todd"}, {"string_value": "persen"}],"sequence_number":2}],
      "name":"user_things",
      "fields":["first_name", "last_name"]
    }`
	series := stringToSeries(mock, minuteAgo, c)
	err := db.WriteSeriesData("foobar", series)
	c.Assert(err, IsNil)
	user := &MockUser{}
	results := executeQuery(user, "foobar", "select last_name from user_things where first_name == 'paul' limit 1", db, c)
	c.Assert(results, HasLen, 1)
	c.Assert(results[0].Points, HasLen, 1)
	c.Assert(results[0].Points[0].Values, HasLen, 1)
	c.Assert(*results[0].Points[0].Values[0].StringValue, Equals, "dix")
}

func (self *DatastoreSuite) TestCanDeleteARangeOfData(c *C) {
	cleanup(nil)
	db := newDatastore(c)
	defer cleanup(db)

	minutesAgo := time.Now().Add(-5 * time.Minute).Unix()
	mock := `{
    "points":[
      {"values":[{"int64_value":3},{"string_value":"paul"}],"sequence_number":2},
      {"values":[{"int64_value":1},{"string_value":"todd"}],"sequence_number":1}],
      "name":"user_things",
      "fields":["count","name"]
    }`
	series := stringToSeries(mock, minutesAgo, c)
	err := db.WriteSeriesData("foobar", series)
	c.Assert(err, IsNil)
	user := &MockUser{}
	results := executeQuery(user, "foobar", "select count, name from user_things;", db, c)
	c.Assert(results[0], DeepEquals, series)

	mock = `{
    "points":[
      {"values":[{"int64_value":3},{"string_value":"john"}],"sequence_number":1}],
    "name":"user_things",
    "fields":["count","name"]
    }`
	series = stringToSeries(mock, time.Now().Unix(), c)
	err = db.WriteSeriesData("foobar", series)
	c.Assert(err, IsNil)
	results = executeQuery(user, "foobar", "select count, name from user_things;", db, c)
	c.Assert(results[0].Points, HasLen, 3)

	err = db.DeleteRangeOfSeries("foobar", "user_things", time.Now().Add(-time.Hour), time.Now().Add(-time.Minute))
	c.Assert(err, IsNil)
	results = executeQuery(user, "foobar", "select count, name from user_things;", db, c)
	c.Assert(results[0].Points, HasLen, 1)
	c.Assert(results[0], DeepEquals, series)
}

func (self *DatastoreSuite) TestCanDeleteRangeOfDataFromRegex(c *C) {
	cleanup(nil)
	db := newDatastore(c)
	defer cleanup(db)

	mock := `{
    "points":[
      {"values":[{"int64_value":3},{"string_value":"paul"}],"sequence_number":2},
      {"values":[{"int64_value":1},{"string_value":"todd"}],"sequence_number":1}
    ],
    "name":"events",
    "fields":["count","name"]
  }`
	series := stringToSeries(mock, time.Now().Unix(), c)
	err := db.WriteSeriesData("foobar", series)
	c.Assert(err, IsNil)
	user := &MockUser{}
	results := executeQuery(user, "foobar", "select count, name from events;", db, c)
	c.Assert(results[0], DeepEquals, series)

	mock = `{
    "points":[{"values":[{"double_value":10.1}],"sequence_number":23}],
    "name":"response_times",
    "fields":["ms"]
  }`
	responseSeries := stringToSeries(mock, time.Now().Unix(), c)
	err = db.WriteSeriesData("foobar", responseSeries)
	c.Assert(err, IsNil)
	results = executeQuery(user, "foobar", "select ms from response_times;", db, c)
	c.Assert(results[0], DeepEquals, responseSeries)

	mock = `{
    "points":[{"values":[{"double_value":232.1}],"sequence_number":23}, {"values":[{"double_value":10.1}],"sequence_number":20}],
    "name":"queue_time",
    "fields":["processed_time"]
  }`
	otherSeries := stringToSeries(mock, time.Now().Unix(), c)
	err = db.WriteSeriesData("foobar", otherSeries)
	c.Assert(err, IsNil)
	results = executeQuery(user, "foobar", "select processed_time from queue_time;", db, c)
	c.Assert(results[0], DeepEquals, otherSeries)

	regex, _ := regexp.Compile(".*time.*")
	db.DeleteRangeOfRegex(user, "foobar", regex, time.Now().Add(-time.Hour), time.Now())

	results = executeQuery(user, "foobar", "select * from events;", db, c)
	c.Assert(results[0], DeepEquals, series)
	results = executeQuery(user, "foobar", "select * from response_times;", db, c)
	c.Assert(results, HasLen, 0)
	results = executeQuery(user, "foobar", "select * from queue_time;", db, c)
	c.Assert(results, HasLen, 0)
}

func (self *DatastoreSuite) TestCanSelectFromARegex(c *C) {
	cleanup(nil)
	db := newDatastore(c)
	defer cleanup(db)

	mock := `{
    "points":[
      {"values":[{"int64_value":3},{"string_value":"paul"}],"sequence_number":2},
      {"values":[{"int64_value":1},{"string_value":"todd"}],"sequence_number":1}],
      "name":"user_things",
      "fields":["count", "name"]
    }`
	series := stringToSeries(mock, time.Now().Unix(), c)
	err := db.WriteSeriesData("foobar", series)
	c.Assert(err, IsNil)
	user := &MockUser{}
	results := executeQuery(user, "foobar", "select count, name from user_things;", db, c)
	c.Assert(results[0], DeepEquals, series)

	mock = `{
    "points":[{"values":[{"double_value":10.1}],"sequence_number":23}],
    "name":"response_times",
    "fields":["ms"]
  }`
	responseSeries := stringToSeries(mock, time.Now().Unix(), c)
	err = db.WriteSeriesData("foobar", responseSeries)
	c.Assert(err, IsNil)
	results = executeQuery(user, "foobar", "select ms from response_times;", db, c)
	c.Assert(results[0], DeepEquals, responseSeries)

	mock = `{
    "points":[{"values":[{"string_value":"NY"}],"sequence_number":23}, {"values":[{"string_value":"CO"}],"sequence_number":20}],
    "name":"other_things",
    "fields":["state"]
  }`
	otherSeries := stringToSeries(mock, time.Now().Unix(), c)
	err = db.WriteSeriesData("foobar", otherSeries)
	c.Assert(err, IsNil)
	results = executeQuery(user, "foobar", "select state from other_things;", db, c)
	c.Assert(results[0], DeepEquals, otherSeries)

	q, errQ := parser.ParseQuery("select * from /.*things/;")
	c.Assert(errQ, IsNil)
	resultSeries := make([]*protocol.Series, 0)
	yield := func(series *protocol.Series) error {
		if len(series.Points) > 0 {
			resultSeries = append(resultSeries, series)
		}
		return nil
	}
	err = db.ExecuteQuery(user, "foobar", q, yield)
	c.Assert(err, IsNil)
	c.Assert(resultSeries, HasLen, 2)
	c.Assert(resultSeries[0], DeepEquals, otherSeries)
	c.Assert(resultSeries[1], DeepEquals, series)
}

func (self *DatastoreSuite) TestBreaksLargeResultsIntoMultipleBatches(c *C) {
	cleanup(nil)
	db := newDatastore(c)
	defer cleanup(db)

	mock := `{
    "points":[
      {"values":[{"double_value":23.1},{"string_value":"paul"}],"sequence_number":2},
      {"values":[{"double_value":56.8},{"string_value":"todd"}],"sequence_number":1}],
      "name":"user_things",
      "fields":["response_time","name"]
  }`
	series := stringToSeries(mock, time.Now().Unix(), c)
	sequence := 0
	writtenPoints := 0
	for i := 0; i < 50000; i++ {
		for _, p := range series.Points {
			sequence += 1
			s := uint32(sequence)
			p.SequenceNumber = &s
		}
		writtenPoints += 2
		err := db.WriteSeriesData("foobar", series)
		c.Assert(err, IsNil)
	}

	q, errQ := parser.ParseQuery("select * from user_things limit 0;")
	c.Assert(errQ, IsNil)
	resultSeries := make([]*protocol.Series, 0)
	yield := func(series *protocol.Series) error {
		resultSeries = append(resultSeries, series)
		return nil
	}
	user := &MockUser{}
	err := db.ExecuteQuery(user, "foobar", q, yield)
	c.Assert(err, IsNil)
	c.Assert(len(resultSeries), InRange, 2, 20)
	pointCount := 0
	for _, s := range resultSeries {
		pointCount += len(s.Points)
	}
	c.Assert(pointCount, Equals, writtenPoints)
}

func (self *DatastoreSuite) TestCheckReadAccess(c *C) {
	cleanup(nil)
	db := newDatastore(c)
	defer cleanup(db)

	mock := `{
    "points":[
      {"values":[{"int64_value":3},{"string_value":"paul"}],"sequence_number":2},
      {"values":[{"int64_value":1},{"string_value":"todd"}],"sequence_number":1}],
      "name":"user_things",
      "fields":["count", "name"]
    }`
	series := stringToSeries(mock, time.Now().Unix(), c)
	err := db.WriteSeriesData("foobar", series)
	c.Assert(err, IsNil)

	mock = `{
    "points":[{"values":[{"string_value":"NY"}],"sequence_number":23}, {"values":[{"string_value":"CO"}],"sequence_number":20}],
    "name":"other_things",
    "fields":["state"]
  }`
	otherSeries := stringToSeries(mock, time.Now().Unix(), c)
	err = db.WriteSeriesData("foobar", otherSeries)

	user := &MockUser{
		dbCannotRead: map[string]bool{"other_things": true},
	}
	q, errQ := parser.ParseQuery("select * from /.*things/;")
	c.Assert(errQ, IsNil)
	resultSeries := make([]*protocol.Series, 0)
	yield := func(series *protocol.Series) error {
		if len(series.Points) > 0 {
			resultSeries = append(resultSeries, series)
		}
		return nil
	}
	err = db.ExecuteQuery(user, "foobar", q, yield)
	c.Assert(err, ErrorMatches, ".*one or more.*")
	c.Assert(len(resultSeries), Equals, 1)
	c.Assert(*resultSeries[0].Name, Equals, "user_things")
	c.Assert(resultSeries[0], DeepEquals, series)
}

func (self *DatastoreSuite) TestCheckWriteAccess(c *C) {
	cleanup(nil)
	db := newDatastore(c)
	defer cleanup(db)

	mock := `{
    "points":[
      {"values":[{"int64_value":3},{"string_value":"paul"}],"sequence_number":2},
      {"values":[{"int64_value":1},{"string_value":"todd"}],"sequence_number":1}],
      "name":"user_things",
      "fields":["count", "name"]
    }`
	series := stringToSeries(mock, time.Now().Unix(), c)
	err := db.WriteSeriesData("foobar", series)
	c.Assert(err, IsNil)

	mock = `{
    "points":[{"values":[{"string_value":"NY"}],"sequence_number":23}, {"values":[{"string_value":"CO"}],"sequence_number":20}],
    "name":"other_things",
    "fields":["state"]
  }`
	otherSeries := stringToSeries(mock, time.Now().Unix(), c)
	err = db.WriteSeriesData("foobar", otherSeries)
	c.Assert(err, IsNil)

	user := &MockUser{
		dbCannotWrite: map[string]bool{"other_things": true},
	}
	regex, _ := regexp.Compile(".*")
	err = db.DeleteRangeOfRegex(user, "foobar", regex, time.Now().Add(-time.Hour), time.Now())
	c.Assert(err, ErrorMatches, ".*one or more.*")

	q, errQ := parser.ParseQuery("select * from /.*things/;")
	c.Assert(errQ, IsNil)
	resultSeries := make([]*protocol.Series, 0)
	yield := func(series *protocol.Series) error {
		if len(series.Points) > 0 {
			resultSeries = append(resultSeries, series)
		}
		return nil
	}
	err = db.ExecuteQuery(user, "foobar", q, yield)
	c.Assert(err, IsNil)
	c.Assert(resultSeries, HasLen, 1)
	c.Assert(resultSeries[0], DeepEquals, otherSeries)
}
