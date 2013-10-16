package datastore

import (
	"common"
	. "launchpad.net/gocheck"
	"parser"
)

type FilteringSuite struct{}

var _ = Suite(&FilteringSuite{})

func (self *FilteringSuite) TestEqualityFiltering(c *C) {
	queryStr := "select * from t where column_one == 100 and column_two != 6;"
	query, err := parser.ParseQuery(queryStr)
	c.Assert(err, IsNil)
	series, err := common.StringToSeriesArray(`
[
 {
   "points": [
     {"values": [{"int_value": 100},{"int_value": 5 }], "timestamp": 1381346631, "sequence_number": 1},
     {"values": [{"int_value": 100},{"int_value": 6 }], "timestamp": 1381346631, "sequence_number": 1},
     {"values": [{"int_value": 90 },{"int_value": 15}], "timestamp": 1381346632, "sequence_number": 1}
   ],
   "name": "t",
   "fields": [
     {"type": "INT32", "name": "column_one"},
     {"type": "INT32", "name": "column_two"}
   ]
 }
]
`)
	c.Assert(err, IsNil)
	result, err := Filter(query, series[0])
	c.Assert(err, IsNil)
	c.Assert(result, NotNil)
	c.Assert(result.Points, HasLen, 1)
	c.Assert(*result.Points[0].Values[0].IntValue, Equals, int32(100))
	c.Assert(*result.Points[0].Values[1].IntValue, Equals, int32(5))
}

func (self *FilteringSuite) TestInequalityFiltering(c *C) {
	queryStr := "select * from t where column_one >= 100 and column_two > 6 and time > now() - 1d;"
	query, err := parser.ParseQuery(queryStr)
	c.Assert(err, IsNil)
	series, err := common.StringToSeriesArray(`
[
 {
   "points": [
     {"values": [{"int_value": 100},{"int_value": 7 }], "timestamp": 1381346631, "sequence_number": 1},
     {"values": [{"int_value": 100},{"int_value": 6 }], "timestamp": 1381346631, "sequence_number": 1},
     {"values": [{"int_value": 90 },{"int_value": 15}], "timestamp": 1381346632, "sequence_number": 1}
   ],
   "name": "t",
   "fields": [
     {"type": "INT32", "name": "column_one"},
     {"type": "INT32", "name": "column_two"}
   ]
 }
]
`)
	c.Assert(err, IsNil)
	result, err := Filter(query, series[0])
	c.Assert(err, IsNil)
	c.Assert(result, NotNil)
	c.Assert(result.Points, HasLen, 1)
	c.Assert(*result.Points[0].Values[0].IntValue, Equals, int32(100))
	c.Assert(*result.Points[0].Values[1].IntValue, Equals, int32(7))
}
