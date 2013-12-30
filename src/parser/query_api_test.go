package parser

import (
	. "launchpad.net/gocheck"
	"math"
	"time"
)

type QueryApiSuite struct{}

var _ = Suite(&QueryApiSuite{})

func (self *QueryApiSuite) TestWillReturnSingleSeries(c *C) {
	for queryStr, expected := range map[string]bool{
		"select * from t":                  true,
		"select * from /uesrs.*/":          false,
		"select * from foo merge bar":      false,
		"select * from foo inner join bar": false,
	} {
		query, err := ParseSelectQuery(queryStr)
		c.Assert(err, IsNil)
		c.Assert(query.WillReturnSingleSeries(), Equals, expected)
	}
}

func (self *QueryApiSuite) TestGetStartTime(c *C) {
	for _, queryStr := range []string{
		"select * from t where time > now() - 1d and time < now() - 1h;",
		"select * from t where now() - 1d < time and time < now() - 1h;",
	} {
		query, err := ParseSelectQuery(queryStr)
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
		query, err := ParseSelectQuery(queryStr)
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
	query, err := ParseSelectQuery(queryStr)
	c.Assert(err, IsNil)
	columns := query.GetReferencedColumns()
	c.Assert(columns, HasLen, 1)
	for v, columns := range columns {
		c.Assert(columns, DeepEquals, []string{"value", "value1", "value2", "value3"})
		c.Assert(v.Name, Equals, "t")
	}
}

func (self *QueryApiSuite) TestGetReferencedColumnsWithInClause(c *C) {
	queryStr := "select value1, sum(value2) from t where value In (90.0, 100.0) group by value3;"
	query, err := ParseSelectQuery(queryStr)
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
	query, err := ParseSelectQuery(queryStr)
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
	query, err := ParseSelectQuery(queryStr)
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
	query, err := ParseSelectQuery(queryStr)
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
	query, err := ParseSelectQuery(queryStr)
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
	queryStr := "select * from foo where a = 5;"
	query, err := ParseSelectQuery(queryStr)
	c.Assert(err, IsNil)
	columns := query.GetReferencedColumns()
	c.Assert(columns, HasLen, 1)
	for v, columns := range columns {
		c.Assert(v.Name, Equals, "foo")
		c.Assert(columns, DeepEquals, []string{"*"})
	}
}

func (self *QueryApiSuite) TestGetReferencedColumnsWithInnerJoin(c *C) {
	queryStr := "select f2.b from foo as f1 inner join foo as f2 where f1.a = 5 and f2.a = 6;"
	query, err := ParseSelectQuery(queryStr)
	c.Assert(err, IsNil)
	columns := query.GetReferencedColumns()
	c.Assert(columns, HasLen, 1)
	aliases := make(map[string][]string)
	for v, columns := range columns {
		if _, ok := aliases[v.Name]; ok {
			c.Fail()
		}
		aliases[v.Name] = columns
	}
	c.Assert(aliases["foo"], DeepEquals, []string{"a", "b"})
}

func (self *QueryApiSuite) TestGetReferencedColumnsWithInnerJoinAndWildcard(c *C) {
	queryStr := "select * from foo as f1 inner join foo as f2"
	query, err := ParseSelectQuery(queryStr)
	c.Assert(err, IsNil)
	columns := query.GetReferencedColumns()
	c.Assert(columns, HasLen, 1)
	aliases := make(map[string][]string)
	for v, columns := range columns {
		if _, ok := aliases[v.Name]; ok {
			c.Fail()
		}
		aliases[v.Name] = columns
	}
	c.Assert(aliases["foo"], DeepEquals, []string{"*"})
}

func (self *QueryApiSuite) TestDefaultLimit(c *C) {
	for queryStr, limit := range map[string]int{
		"select * from t limit 0":    0,
		"select * from t limit 1000": 1000,
		"select * from t;":           0,
	} {
		query, err := ParseSelectQuery(queryStr)
		c.Assert(err, IsNil)
		c.Assert(query.Limit, Equals, limit)
	}
}

func (self *QueryApiSuite) TestDefaultStartTime(c *C) {
	for queryStr, t := range map[string]time.Time{
		"select * from t where time < now() - 1d;": time.Unix(math.MinInt64, 0),
		"select * from t;":                         time.Unix(math.MinInt64, 0),
	} {
		query, err := ParseSelectQuery(queryStr)
		c.Assert(err, IsNil)
		startTime := query.GetStartTime()
		c.Assert(startTime, Equals, t)
	}
}

func (self *QueryApiSuite) TestDefaultEndTime(c *C) {
	for queryStr, t := range map[string]time.Time{
		"select * from t where time > now() - 1d;": time.Now().Round(time.Minute),
		"select * from t;":                         time.Now().Round(time.Minute),
	} {
		query, err := ParseSelectQuery(queryStr)
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
		query, err := ParseSelectQuery(queryStr)
		c.Assert(err, IsNil)
		startTime := query.GetStartTime()
		roundedStartTime := startTime.Round(time.Minute).Unix()
		yesterday := time.Now().Add(-24 * time.Hour).Round(time.Minute).Unix()
		c.Assert(roundedStartTime, Equals, yesterday)
	}
}

func (self *QueryApiSuite) TestErrorInStartTime(c *C) {
	queriesAndErrors := map[string]string{
		"select * from t where time > now() * 1d and time < now() - 1h;": ".*'\\*'.*",
		"select * from t where time > blah * 1d and time < now() - 1h;":  ".*strconv.ParseFloat.*",
		"select * from t where time = now() * 1d and time < now() - 1h;": ".*Cannot use '\\*' in a time expression.*",
		"select * from t where time > now() - 1d or time > now() - 1h;":  ".*Invalid where.*",
		"select * from t where time > foo() - 1d or time > now() - 1h;":  ".*Invalid use of function foo.*",
	}

	for queryStr, error := range queriesAndErrors {
		_, err := ParseSelectQuery(queryStr)
		c.Assert(err, ErrorMatches, error)
	}
}

func (self *QueryApiSuite) TestAliasing(c *C) {
	query, err := ParseSelectQuery("select * from user.events")
	c.Assert(err, IsNil)
	c.Assert(query.GetTableAliases("user.events"), DeepEquals, []string{"user.events"})

	query, err = ParseSelectQuery("select * from user.events as events inner join user.events as clicks")
	c.Assert(err, IsNil)
	c.Assert(query.GetTableAliases("user.events"), DeepEquals, []string{"events", "clicks"})

	// aliasing is ignored in case of a regex
	query, err = ParseSelectQuery("select * from /.*events.*/i")
	c.Assert(err, IsNil)
	c.Assert(query.GetTableAliases("user.events"), DeepEquals, []string{"user.events"})
}
