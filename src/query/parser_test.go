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

func ToValueArray(strings ...string) (values []*Value) {
	for _, str := range strings {
		values = append(values, &Value{str, nil})
	}
	return
}

func (self *QueryParserSuite) TestParseBasicSelectQuery(c *C) {
	q, err := ParseQuery("select value from t where c == '5';")
	defer q.Close()
	c.Assert(err, IsNil)
	c.Assert(q.GetColumnNames(), DeepEquals, ToValueArray("value"))
	w := q.GetWhereCondition()
	c.Assert(q.GetFromClause().TableName, Equals, "t")

	boolExpression := w.Left
	leftExpression := boolExpression.Left
	rightExpression := boolExpression.Right
	leftValue := leftExpression.Left.Name
	rightValue := rightExpression.Left.Name

	c.Assert(leftValue, Equals, "c")
	c.Assert(boolExpression.Operation, Equals, "==")
	c.Assert(rightValue, Equals, "5")
}

func (self *QueryParserSuite) TestParseSelectWithoutWhereClause(c *C) {
	q, err := ParseQuery("select value, time from t;")
	defer q.Close()
	c.Assert(err, IsNil)
	c.Assert(q.GetColumnNames(), DeepEquals, ToValueArray("value", "time"))
	c.Assert(q.GetFromClause().TableName, Equals, "t")
	c.Assert(q.GetWhereCondition(), IsNil)
}

func (self *QueryParserSuite) TestParseSelectWithUpperCase(c *C) {
	q, err := ParseQuery("SELECT VALUE, TIME FROM t WHERE C == '5';")
	defer q.Close()
	c.Assert(err, IsNil)
	c.Assert(q.GetColumnNames(), DeepEquals, ToValueArray("VALUE", "TIME"))
	w := q.GetWhereCondition()
	c.Assert(q.GetFromClause().TableName, Equals, "t")

	boolExpression := w.Left
	leftExpression := boolExpression.Left
	rightExpression := boolExpression.Right
	leftValue := leftExpression.Left.Name
	rightValue := rightExpression.Left.Name

	c.Assert(leftValue, Equals, "C")
	c.Assert(rightValue, Equals, "5")
}

func (self *QueryParserSuite) TestParseSelectWithMultipleColumns(c *C) {
	q, err := ParseQuery("select value, time from t;")
	defer q.Close()
	c.Assert(err, IsNil)
	c.Assert(q.GetFromClause().TableName, Equals, "t")
}

func (self *QueryParserSuite) TestParseSelectWithInequality(c *C) {
	q, err := ParseQuery("select value, time from t where c < 5;")
	defer q.Close()
	c.Assert(err, IsNil)
	w := q.GetWhereCondition()
	c.Assert(q.GetFromClause().TableName, Equals, "t")

	boolExpression := w.Left
	leftExpression := boolExpression.Left
	rightExpression := boolExpression.Right
	leftValue := leftExpression.Left.Name
	rightValue := rightExpression.Left.Name

	c.Assert(leftValue, Equals, "c")
	c.Assert(boolExpression.Operation, Equals, "<")
	c.Assert(rightValue, Equals, "5")
}

func (self *QueryParserSuite) TestParseSelectWithTimeCondition(c *C) {
	q, err := ParseQuery("select value, time from t where time > now() - 1d;")
	defer q.Close()
	c.Assert(err, IsNil)
	w := q.GetWhereCondition()

	boolExpression := w.Left
	leftExpression := boolExpression.Left
	rightExpression := boolExpression.Right
	leftValue := leftExpression.Left.Name

	funCall := rightExpression.Left
	oneDay := rightExpression.Right

	c.Assert(q.GetFromClause().TableName, Equals, "t")
	c.Assert(leftValue, Equals, "time")
	c.Assert(funCall.IsFunctionCall(), Equals, true)
	c.Assert(funCall.Name, Equals, "now")
	c.Assert(oneDay.IsFunctionCall(), Equals, false)
	c.Assert(oneDay.Name, Equals, "1d")
	c.Assert(rightExpression.Operation, Equals, byte('-'))
}

func (self *QueryParserSuite) TestParseSelectWithAnd(c *C) {
	q, err := ParseQuery("select value from cpu.idle where time>now()-7d and time<now()-6d;")
	defer q.Close()
	c.Assert(err, IsNil)
	w := q.GetWhereCondition()

	c.Assert(q.GetFromClause().TableName, Equals, "cpu.idle")

	leftBoolExpression := w.Left
	c.Assert(w.Operation, Equals, "AND")
	rightBoolExpression := w.Right

	c.Assert(leftBoolExpression.Left.Left.Name, Equals, "time")
	c.Assert(leftBoolExpression.Left.Left.IsFunctionCall(), Equals, false)
	c.Assert(leftBoolExpression.Right.Left.Name, Equals, "now")
	c.Assert(leftBoolExpression.Right.Left.IsFunctionCall(), Equals, true)
	c.Assert(leftBoolExpression.Right.Right.Name, Equals, "7d")
	c.Assert(leftBoolExpression.Right.Right.IsFunctionCall(), Equals, false)
	c.Assert(leftBoolExpression.Operation, Equals, ">")

	c.Assert(rightBoolExpression.Left.Left.Name, Equals, "time")
	c.Assert(rightBoolExpression.Left.Left.IsFunctionCall(), Equals, false)
	c.Assert(rightBoolExpression.Right.Left.Name, Equals, "now")
	c.Assert(rightBoolExpression.Right.Left.IsFunctionCall(), Equals, true)
	c.Assert(rightBoolExpression.Right.Right.Name, Equals, "6d")
	c.Assert(rightBoolExpression.Right.Right.IsFunctionCall(), Equals, false)
	c.Assert(rightBoolExpression.Operation, Equals, "<")
}

func (self *QueryParserSuite) TestParseSelectWithGroupBy(c *C) {
	q, err := ParseQuery("select count(*) from users.events group_by user_email,time(1h) where time>now()-1d;")
	defer q.Close()
	c.Assert(err, IsNil)
	c.Assert(q.GetColumnNames(), HasLen, 1)
	column := q.GetColumnNames()[0]
	c.Assert(column.IsFunctionCall(), Equals, true)
	c.Assert(column.Name, Equals, "count")
	c.Assert(column.Elems, HasLen, 1)
	c.Assert(column.Elems[0].IsFunctionCall(), Equals, false)
	c.Assert(column.Elems[0].Name, Equals, "*")

	groupBy := q.GetGroupByClause()
	c.Assert(groupBy, HasLen, 2)
	c.Assert(groupBy[0].IsFunctionCall(), Equals, false)
	c.Assert(groupBy[0].Name, Equals, "user_email")
	c.Assert(groupBy[1].IsFunctionCall(), Equals, true)
	c.Assert(groupBy[1].Name, Equals, "time")
	c.Assert(groupBy[1].Elems, HasLen, 1)
	c.Assert(groupBy[1].Elems[0].Name, Equals, "1h")

	c.Assert(q.GetFromClause().TableName, Equals, "users.events")
}

func (self *QueryParserSuite) TestParseFromWithNestedFunctions(c *C) {
	q, err := ParseQuery("select top(10, count(*)) from users.events;")
	defer q.Close()
	c.Assert(err, IsNil)
	c.Assert(q.GetColumnNames(), HasLen, 1)
	column := q.GetColumnNames()[0]
	c.Assert(column.IsFunctionCall(), Equals, true)
	c.Assert(column.Name, Equals, "top")
	c.Assert(column.Elems, HasLen, 2)
	c.Assert(column.Elems[0].IsFunctionCall(), Equals, false)
	c.Assert(column.Elems[0].Name, Equals, "10")
	c.Assert(column.Elems[1].IsFunctionCall(), Equals, true)
	c.Assert(column.Elems[1].Name, Equals, "count")
	c.Assert(column.Elems[1].Elems, HasLen, 1)
	c.Assert(column.Elems[1].Elems[0].IsFunctionCall(), Equals, false)
	c.Assert(column.Elems[1].Elems[0].Name, Equals, "*")
}

// write specs for the following queries

// select value from .* last 1
// select count(*) from merge(newsletter.signups,user.signups) group_by time(1h) where time>now()-1d
// select diff(t1.value, t2.value) from inner_join(memory.total, t1, memory.used, t2) group_by time(1m) where time>now()-6h
// select count(distinct(email)) from user.events where time>now()-1d group_by time(15m)
// select percentile(95, value) from response_times group_by time(10m) where time>now()-6h
// select count(*) from events where type='login'
// insert into user.events.count.per_day select count(*) from user.events where time<forever group_by time(1d)
// insert into :series_name.percentiles.95 select percentile(95,value) from stats.* where time<forever group_by time(1d)
// select email from users.events where email ~= /gmail\.com/i and time>now()-2d group_by time(10m)
