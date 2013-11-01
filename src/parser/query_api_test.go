package parser

import (
	. "launchpad.net/gocheck"
	"time"
)

type QueryApiSuite struct{}

var _ = Suite(&QueryApiSuite{})

func (self *QueryApiSuite) TestGetStartTime(c *C) {
	for _, queryStr := range []string{
		"select * from t where time > now() - 1d and time < now() - 1h;",
		"select * from t where now() - 1d < time and time < now() - 1h;",
	} {
		query, err := ParseQuery(queryStr)
		c.Assert(err, IsNil)
		startTime := query.GetStartTime()
		roundedStartTime := startTime.Round(time.Minute).Unix()
		yesterday := time.Now().Add(-24 * time.Hour).Round(time.Minute).Unix()
		c.Assert(roundedStartTime, Equals, yesterday)
		c.Assert(query.GetWhereCondition(), IsNil)
	}
}

func (self *QueryApiSuite) TestGetEndTime(c *C) {
	for _, queryStr := range []string{
		"select * from t where time > now() - 1d and time < now() - 1h;",
		"select * from t where now() - 1d < time and now() - 1h > time;",
	} {
		query, err := ParseQuery(queryStr)
		c.Assert(err, IsNil)
		endTime := query.GetEndTime()
		roundedEndTime := endTime.Round(time.Minute).Unix()
		anHourAgo := time.Now().Add(-1 * time.Hour).Round(time.Minute).Unix()
		c.Assert(roundedEndTime, Equals, anHourAgo)
		c.Assert(query.GetWhereCondition(), IsNil)
	}
}

func (self *QueryApiSuite) TestGetReferencedColumns(c *C) {
	queryStr := "select value1, sum(value2) from t where value > 90.0 and value2 < 10.0 group by value3;"
	query, err := ParseQuery(queryStr)
	c.Assert(err, IsNil)
	columns := query.GetReferencedColumns()
	c.Assert(columns, HasLen, 1)
	for v, columns := range columns {
		c.Assert(columns, DeepEquals, []string{"value", "value1", "value2", "value3"})
		c.Assert(v.Name, Equals, "t")
	}
}

func (self *QueryApiSuite) TestGetReferencedColumnsReturnsTheStarAsAColumn(c *C) {
	queryStr := "select * from events;"
	query, err := ParseQuery(queryStr)
	c.Assert(err, IsNil)
	columns := query.GetReferencedColumns()
	c.Assert(columns, HasLen, 1)
	for v, columns := range columns {
		c.Assert(v.Name, Equals, "events")
		c.Assert(columns, DeepEquals, []string{"*"})
	}
}

func (self *QueryApiSuite) TestGetReferencedColumnsReturnsEmptyArrayIfQueryIsAggregateStar(c *C) {
	queryStr := "select count(*) from events;"
	query, err := ParseQuery(queryStr)
	c.Assert(err, IsNil)
	columns := query.GetReferencedColumns()
	c.Assert(columns, HasLen, 1)
	for v, columns := range columns {
		c.Assert(v.Name, Equals, "events")
		c.Assert(columns, DeepEquals, []string{})
	}
}

func (self *QueryApiSuite) TestGetReferencedColumnsWithTablesMerge(c *C) {
	queryStr := "select * from events merge other_events;"
	query, err := ParseQuery(queryStr)
	c.Assert(err, IsNil)
	columns := query.GetReferencedColumns()
	c.Assert(columns, HasLen, 2)
	expectedNames := []string{"events", "other_events"}
	index := 0
	for v, columns := range columns {
		c.Assert(v.Name, Equals, expectedNames[index])
		index++
		c.Assert(columns, DeepEquals, []string{"*"})
	}
}

func (self *QueryApiSuite) TestGetReferencedColumnsWithARegexTable(c *C) {
	queryStr := "select count(*), region from /events.*/ group by time(1h), region;"
	query, err := ParseQuery(queryStr)
	c.Assert(err, IsNil)
	columns := query.GetReferencedColumns()
	c.Assert(columns, HasLen, 1)
	for v, columns := range columns {
		c.Assert(v.compiledRegex, NotNil)
		c.Assert(v.Name, Equals, "events.*")
		c.Assert(columns, DeepEquals, []string{"region"})
	}
}

func (self *QueryApiSuite) TestGetReferencedColumnsWithWhereClause(c *C) {
	queryStr := "select * from foo where a == 5;"
	query, err := ParseQuery(queryStr)
	c.Assert(err, IsNil)
	columns := query.GetReferencedColumns()
	c.Assert(columns, HasLen, 1)
	for v, columns := range columns {
		c.Assert(v.Name, Equals, "foo")
		c.Assert(columns, DeepEquals, []string{"*"})
	}
}

func (self *QueryApiSuite) TestDefaultStartTime(c *C) {
	for queryStr, t := range map[string]time.Time{
		"select * from t where time < now() - 1d;": time.Now().Add(-24 * time.Hour).Add(-1 * time.Hour).Round(time.Minute),
		"select * from t;":                         time.Now().Add(-1 * time.Hour).Round(time.Minute),
	} {
		query, err := ParseQuery(queryStr)
		c.Assert(err, IsNil)
		startTime := query.GetStartTime()
		roundedStartTime := startTime.Round(time.Minute)
		c.Assert(roundedStartTime, Equals, t)
	}
}

func (self *QueryApiSuite) TestDefaultEndTime(c *C) {
	for queryStr, t := range map[string]time.Time{
		"select * from t where time > now() - 1d;": time.Now().Round(time.Minute),
		"select * from t;":                         time.Now().Round(time.Minute),
	} {
		query, err := ParseQuery(queryStr)
		c.Assert(err, IsNil)
		endTime := query.GetEndTime()
		roundedEndTime := endTime.Round(time.Minute)
		c.Assert(roundedEndTime, Equals, t)
	}
}

func (self *QueryApiSuite) TestGetStartTimeWithOr(c *C) {
	for _, queryStr := range []string{
		"select * from t where time > now() - 1d and (value > 90 or value < 10);",
	} {
		query, err := ParseQuery(queryStr)
		c.Assert(err, IsNil)
		startTime := query.GetStartTime()
		roundedStartTime := startTime.Round(time.Minute).Unix()
		yesterday := time.Now().Add(-24 * time.Hour).Round(time.Minute).Unix()
		c.Assert(roundedStartTime, Equals, yesterday)
	}
}

func (self *QueryApiSuite) TestErrorInStartTime(c *C) {
	queriesAndErrors := map[string]string{
		"select * from t where time > now() * 1d and time < now() - 1h;":  ".*'\\*'.*",
		"select * from t where time > blah * 1d and time < now() - 1h;":   ".*strconv.ParseInt.*",
		"select * from t where time == now() * 1d and time < now() - 1h;": ".*Cannot use time with '=='.*",
		"select * from t where time > now() - 1d or time > now() - 1h;":   ".*Invalid where.*",
		"select * from t where time > foo() - 1d or time > now() - 1h;":   ".*Invalid use of function foo.*",
	}

	for queryStr, error := range queriesAndErrors {
		_, err := ParseQuery(queryStr)
		c.Assert(err, ErrorMatches, error)
	}
}
