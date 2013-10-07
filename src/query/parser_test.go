package query

import (
	. "launchpad.net/gocheck"
	"testing"
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) {
	TestingT(t)
}

type QueryParserSuite struct{}

var _ = Suite(&QueryParserSuite{})

func (self *QueryParserSuite) TestParseBasicSelectQuery(c *C) {
	q, err := ParseQuery("select value from t where c == '5';")
	c.Assert(err, IsNil)
	c.Assert(q.GetColumnNames(), DeepEquals, []string{"value"})
	w := q.GetWhereCondition()
	c.Assert(q.GetFromClause().TableName, Equals, "t")

	boolExpression := w.Left
	leftExpression := boolExpression.Left
	rightExpression := boolExpression.Right
	leftValue := leftExpression.Left.Name
	rightValue := rightExpression.Left.Name

	c.Assert(leftValue, Equals, "c")
	c.Assert(rightValue, Equals, "5")
}

func (self *QueryParserSuite) TestParseSelectWithoutWhereClause(c *C) {
	q, err := ParseQuery("select value, time from t;")
	c.Assert(err, IsNil)
	c.Assert(q.GetColumnNames(), DeepEquals, []string{"value", "time"})
	c.Assert(q.GetFromClause().TableName, Equals, "t")
	c.Assert(q.GetWhereCondition(), IsNil)
}

// func (self *QueryParserSuite) TestParseSelectWithUpperCase(c *C) {
// 	q, err := ParseQuery("SELECT VALUE, TIME FROM t WHERE C = '5';")
// 	c.Assert(err, IsNil)
// 	c.Assert(q.GetColumnNames(), DeepEquals, []string{"VALUE", "TIME"})
// 	w := q.GetWhereClause()
// 	c.Assert(q.GetFromClause().TableName, Equals, "t")
// 	c.Assert(w.ColumnName, Equals, "C")
// 	c.Assert(w.Value, Equals, "5")
// }

// func (self *QueryParserSuite) TestParseSelectWithMultipleColumns(c *C) {
// 	q, err := ParseQuery("select value, time from t where c = '5';")
// 	c.Assert(err, IsNil)
// 	c.Assert(q.GetColumnNames(), DeepEquals, []string{"value", "time"})
// 	w := q.GetWhereClause()
// 	c.Assert(q.GetFromClause().TableName, Equals, "t")
// 	c.Assert(w.ColumnName, Equals, "c")
// 	c.Assert(w.Value, Equals, "5")
// }

// func (self *QueryParserSuite) TestParseSelectWithInequality(c *C) {
// 	q, err := ParseQuery("select value, time from t where c < 5;")
// 	c.Assert(err, IsNil)
// 	c.Assert(q.GetColumnNames(), DeepEquals, []string{"value", "time"})
// 	w := q.GetWhereClause()
// 	c.Assert(q.GetFromClause().TableName, Equals, "t")
// 	c.Assert(w.ColumnName, Equals, "c")
// 	c.Assert(int(w.Op), Equals, LESS_THAN)
// 	// TODO: fix this
// 	c.Assert(w.Value, Equals, 5)
// }

// func (self *QueryParserSuite) TestParseSelectWithTimeCondition(c *C) {
// 	q, err := ParseQuery("select value, time from t where time > now() - 1d;")
// 	c.Assert(err, IsNil)
// 	c.Assert(q.GetColumnNames(), DeepEquals, []string{"value", "time"})
// 	w := q.GetWhereClause()
// 	c.Assert(q.GetFromClause().TableName, Equals, "t")
// 	c.Assert(w.ColumnName, Equals, "c")
// 	c.Assert(w.Value, Equals, "5")
// }

// write specs for the following queries

// select value from cpu.* where time>now()-7d and time<now()-6d
// select count(*) from users.events group_by user_email,time(1h) where time>now()-7d
// select top(10, count(*)) from=users.events group_by user_email,time(1h) where time>now()-7d
// select value from .* last 1
// select count(*) from merge(newsletter.signups,user.signups) group_by time(1h) where time>now()-1d
// select diff(t1.value, t2.value) from inner_join(memory.total, t1, memory.used, t2) group_by time(1m) where time>now()-6h
// select count(distinct(email)) from user.events where time>now()-1d group_by time(15m)
// select percentile(95, value) from response_times group_by time(10m) where time>now()-6h
// select count(*) from events where type='login'
// insert into user.events.count.per_day select count(*) from user.events where time<forever group_by time(1d)
// insert into :series_name.percentiles.95 select percentile(95,value) from stats.* where time<forever group_by time(1d)
// select email from users.events where email ~= /gmail\.com/i and time>now()-2d group_by time(10m)
