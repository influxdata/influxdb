package parser

import (
	"common"
	"fmt"
	. "launchpad.net/gocheck"
	"testing"
	"time"
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) {
	TestingT(t)
}

type QueryParserSuite struct{}

var _ = Suite(&QueryParserSuite{})

func ToValueArray(strings ...string) (values []*Value) {
	for _, str := range strings {
		values = append(values, &Value{str, ValueSimpleName, nil, nil})
	}
	return
}

func (self *QueryParserSuite) TestInvalidFromClause(c *C) {
	query := "select value from .t"
	_, err := ParseSelectQuery(query)

	c.Assert(err, ErrorMatches, ".*\\$undefined.*")
}

func (self *QueryParserSuite) TestParseBasicSelectQuery(c *C) {
	for _, query := range []string{
		"select value from t where c = '5';",
		// semicolon is optional
		"select value from t where c = '5'",
	} {
		q, err := ParseSelectQuery(query)
		c.Assert(err, IsNil)

		c.Assert(q.GetQueryString(), Equals, query)

		c.Assert(q.GetColumnNames(), DeepEquals, ToValueArray("value"))
		w := q.GetWhereCondition()

		boolExpression, ok := w.GetBoolExpression()
		c.Assert(ok, Equals, true)

		leftValue := boolExpression.Elems[0]
		rightValue := boolExpression.Elems[1]

		c.Assert(leftValue.Name, Equals, "c")
		c.Assert(boolExpression.Name, Equals, "=")
		c.Assert(rightValue.Name, Equals, "5")
	}
}

func (self *QueryParserSuite) TestParseDeleteQueryWithEndTime(c *C) {
	query := "delete from foo where time < 1389040522000000u"
	queries, err := ParseQuery(query)
	c.Assert(err, IsNil)

	c.Assert(queries, HasLen, 1)

	_q := queries[0]

	c.Assert(_q.DeleteQuery, NotNil)

	q := _q.DeleteQuery

	c.Assert(q.GetEndTime(), Equals, time.Unix(1389040522, 0).UTC())
}

func (self *QueryParserSuite) TestGetQueryStringWithTimeCondition(c *C) {
	now := time.Now().Round(time.Minute).UTC()
	micros := common.TimeToMicroseconds(now)

	for _, q := range []string{
		"delete from foo",
		fmt.Sprintf("delete from foo where time < %du", micros),
	} {
		fmt.Printf("testing %s\n", q)

		queries, err := ParseQuery(q)
		c.Assert(err, IsNil)

		c.Assert(queries, HasLen, 1)

		_q := queries[0]

		c.Assert(_q.DeleteQuery, NotNil)

		q := _q.DeleteQuery

		// try to parse the query with the time condition
		queries, err = ParseQuery(q.GetQueryStringWithTimeCondition())
		fmt.Printf("query: %s\n", q.GetQueryStringWithTimeCondition())
		c.Assert(err, IsNil)

		_q = queries[0]
		c.Assert(_q.DeleteQuery, NotNil)

		q = _q.DeleteQuery

		c.Assert(q.GetEndTime().Round(time.Minute), Equals, now)
	}
}

func (self *QueryParserSuite) TestParseDeleteQuery(c *C) {
	query := "delete from foo where time > '2012-08-13' and time < '2013-08-13'"
	queries, err := ParseQuery(query)
	c.Assert(err, IsNil)

	c.Assert(queries, HasLen, 1)

	_q := queries[0]

	c.Assert(_q.DeleteQuery, NotNil)

	q := _q.DeleteQuery

	c.Assert(q.GetQueryString(), Equals, query)

	startTime, _ := time.Parse("2006-01-02", "2012-08-13")
	endTime, _ := time.Parse("2006-01-02", "2013-08-13")
	c.Assert(q.GetStartTime(), Equals, startTime)
	c.Assert(q.GetEndTime(), Equals, endTime)
}

func (self *QueryParserSuite) TestParseWithUnderscore(c *C) {
	queryString := "select _value from foo"
	query, err := ParseSelectQuery(queryString)
	c.Assert(err, IsNil)

	for table, columns := range query.GetReferencedColumns() {
		c.Assert(table.Name, Equals, "foo")
		c.Assert(columns, DeepEquals, []string{"_value"})
	}
}

func (self *QueryParserSuite) TestSimpleFromClause(c *C) {
	q, err := ParseSelectQuery("select value from t;")
	c.Assert(err, IsNil)

	fromClause := q.GetFromClause()
	c.Assert(fromClause.Type, Equals, FromClauseArray)
	c.Assert(fromClause.Names, HasLen, 1)
	c.Assert(fromClause.Names[0].Name.Name, Equals, "t")
}

func (self *QueryParserSuite) TestParseFromWithMergedTable(c *C) {
	q, err := ParseSelectQuery("select count(*) from newsletter.signups merge user.signups where time>now()-1d;")
	c.Assert(err, IsNil)
	fromClause := q.GetFromClause()
	c.Assert(fromClause.Type, Equals, FromClauseMerge)
	c.Assert(fromClause.Names, HasLen, 2)
	c.Assert(fromClause.Names[0].Name.Name, Equals, "newsletter.signups")
	c.Assert(fromClause.Names[1].Name.Name, Equals, "user.signups")
}

func (self *QueryParserSuite) TestMultipleAggregateFunctions(c *C) {
	q, err := ParseSelectQuery("select first(bar), last(bar) from foo")
	c.Assert(err, IsNil)
	columns := q.GetColumnNames()
	c.Assert(columns, HasLen, 2)
	c.Assert(columns[0].Name, Equals, "first")
	c.Assert(columns[1].Name, Equals, "last")
}

func (self *QueryParserSuite) TestParseFromWithJoinedTable(c *C) {
	q, err := ParseSelectQuery("select max(newsletter.signups.value, user.signups.value) from newsletter.signups inner join user.signups where time>now()-1d;")
	c.Assert(err, IsNil)
	fromClause := q.GetFromClause()
	c.Assert(fromClause.Type, Equals, FromClauseInnerJoin)
	c.Assert(fromClause.Names, HasLen, 2)
	c.Assert(fromClause.Names[0].Name.Name, Equals, "newsletter.signups")
	c.Assert(fromClause.Names[1].Name.Name, Equals, "user.signups")
}

func (self *QueryParserSuite) TestParseListSeries(c *C) {
	queries, err := ParseQuery("list series")
	c.Assert(err, IsNil)
	c.Assert(queries, HasLen, 1)
	c.Assert(queries[0].IsListQuery(), Equals, true)
}

// issue #150
func (self *QueryParserSuite) TestParseSelectWithDivisionThatLooksLikeRegex(c *C) {
	q, err := ParseSelectQuery("select a/2, b/2 from x")
	c.Assert(err, IsNil)
	c.Assert(q.GetFromClause().Names[0].Name.Name, Equals, "x")
	c.Assert(q.GetColumnNames(), HasLen, 2)
}

// issue #150
func (self *QueryParserSuite) TestParseSelectWithSlashesInRegex(c *C) {
	q, err := ParseSelectQuery("select * from foo where x =~ /users\\/login/")
	c.Assert(err, IsNil)
	whereCondition := q.GetWhereCondition()
	boolExpression, _ := whereCondition.GetBoolExpression()
	fmt.Printf("value: %v\n", boolExpression.Elems[1])
	c.Assert(boolExpression.Elems[1].Name, Equals, "users/login")
}

func (self *QueryParserSuite) TestParseSelectWithRegexNegation(c *C) {
	q, err := ParseSelectQuery("select * from foo where x !~ /users\\/login/")
	c.Assert(err, IsNil)
	whereCondition := q.GetWhereCondition()
	boolExpression, _ := whereCondition.GetBoolExpression()
	fmt.Printf("value: %v\n", boolExpression.Elems[1])
	c.Assert(boolExpression.Elems[1].Name, Equals, "users/login")
}

// issue #150
func (self *QueryParserSuite) TestParseSelectWithTwoRegexThatLooksLikeSingleRegex(c *C) {
	q, err := ParseSelectQuery("select * from /.*/ where x =~ /.*/")
	c.Assert(err, IsNil)
	fromClause := q.GetFromClause()
	c.Assert(fromClause.Names[0].Name.Name, Equals, ".*")
	whereCondition := q.GetWhereCondition()
	boolExpression, _ := whereCondition.GetBoolExpression()
	c.Assert(boolExpression.Elems[1].Name, Equals, ".*")
}

func (self *QueryParserSuite) TestParseSelectWithInsensitiveRegexTables(c *C) {
	q, err := ParseSelectQuery("select email from /users.*/i where time>now()-2d;")
	c.Assert(err, IsNil)

	fromClause := q.GetFromClause()
	c.Assert(fromClause.Type, Equals, FromClauseArray)
	c.Assert(fromClause.Names, HasLen, 1)
	regex, ok := fromClause.Names[0].Name.GetCompiledRegex()
	c.Assert(ok, Equals, true)
	c.Assert(regex.MatchString("USERSFOOBAR"), Equals, true)
}

func (self *QueryParserSuite) TestParseSelectWithRegexTables(c *C) {
	q, err := ParseSelectQuery("select email from /users.*/ where time>now()-2d;")
	c.Assert(err, IsNil)

	fromClause := q.GetFromClause()
	c.Assert(fromClause.Type, Equals, FromClauseArray)
	c.Assert(fromClause.Names, HasLen, 1)
	regex, ok := fromClause.Names[0].Name.GetCompiledRegex()
	c.Assert(ok, Equals, true)
	c.Assert(regex.MatchString("USERSFOOBAR"), Equals, false)
	c.Assert(regex.MatchString("usersfoobar"), Equals, true)
}

func (self *QueryParserSuite) TestMergeFromClause(c *C) {
	q, err := ParseSelectQuery("select value from t1 merge t2 where c = '5';")
	c.Assert(err, IsNil)

	c.Assert(q.GetColumnNames(), DeepEquals, ToValueArray("value"))
	w := q.GetWhereCondition()

	boolExpression, ok := w.GetBoolExpression()
	c.Assert(ok, Equals, true)

	leftValue := boolExpression.Elems[0]
	rightValue := boolExpression.Elems[1]

	c.Assert(leftValue.Name, Equals, "c")
	c.Assert(boolExpression.Name, Equals, "=")
	c.Assert(rightValue.Name, Equals, "5")
}

func (self *QueryParserSuite) TestParseSelectWithoutWhereClause(c *C) {
	q, err := ParseSelectQuery("select value, time from t;")
	c.Assert(err, IsNil)
	c.Assert(q.GetColumnNames(), DeepEquals, ToValueArray("value", "time"))
	c.Assert(q.GetWhereCondition(), IsNil)
}

func (self *QueryParserSuite) TestParseSelectWithUpperCase(c *C) {
	q, err := ParseSelectQuery("SELECT VALUE, TIME FROM t WHERE C = '5';")
	c.Assert(err, IsNil)
	c.Assert(q.GetColumnNames(), DeepEquals, ToValueArray("VALUE", "TIME"))
	w := q.GetWhereCondition()

	boolExpression, ok := w.GetBoolExpression()
	c.Assert(ok, Equals, true)
	leftValue := boolExpression.Elems[0]
	rightValue := boolExpression.Elems[1]

	c.Assert(leftValue.Name, Equals, "C")
	c.Assert(rightValue.Name, Equals, "5")
}

func (self *QueryParserSuite) TestParseSelectWithMultipleColumns(c *C) {
	q, err := ParseSelectQuery("select value, time from t;")

	c.Assert(err, IsNil)
	columns := q.GetColumnNames()
	c.Assert(columns, HasLen, 2)
	c.Assert(columns[0].Name, Equals, "value")
	c.Assert(columns[1].Name, Equals, "time")
}

func (self *QueryParserSuite) TestParseSelectWithInequality(c *C) {
	q, err := ParseSelectQuery("select value, time from t where c < 5;")
	c.Assert(err, IsNil)
	w := q.GetWhereCondition()

	boolExpression, ok := w.GetBoolExpression()
	c.Assert(ok, Equals, true)
	leftValue := boolExpression.Elems[0]
	rightValue := boolExpression.Elems[1]

	c.Assert(leftValue.Name, Equals, "c")
	c.Assert(boolExpression.Name, Equals, "<")
	c.Assert(rightValue.Name, Equals, "5")
}

func (self *QueryParserSuite) TestParseSelectWithTimeCondition(c *C) {
	queries := map[string]time.Time{
		"select value, time from t where time > now() - 1d and time < now() - 1m;": time.Now().Add(-time.Minute).Round(time.Minute).UTC(),
		"select value, time from t where time > now() - 1d and time < now();":      time.Now().Round(time.Minute).UTC(),
	}
	for query, expected := range queries {
		fmt.Printf("Running %s\n", query)

		q, err := ParseSelectQuery(query)
		c.Assert(err, IsNil)

		c.Assert(q.GetStartTime().Round(time.Minute), Equals, time.Now().Add(-24*time.Hour).Round(time.Minute).UTC())
		c.Assert(q.GetEndTime().Round(time.Minute).UTC(), Equals, expected)

		// note: the time condition will be removed
		c.Assert(q.GetWhereCondition(), IsNil)
	}
}

func (self *QueryParserSuite) TestParseSelectWithPartialTimeString(c *C) {
	for actual, expected := range map[string]string{
		"2013-08-15":          "2013-08-15 00:00:00",
		"2013-08-15 7":        "2013-08-15 07:00:00",
		"2013-08-15 7:04":     "2013-08-15 07:04:00",
		"2013-08-15 15":       "2013-08-15 15:00:00",
		"2013-08-15 15:14":    "2013-08-15 15:14:00",
		"2013-08-15 15:14:26": "2013-08-15 15:14:26",
	} {
		t, err := time.Parse("2006-01-02 15:04:05", expected)
		c.Assert(err, IsNil)
		q, err := ParseSelectQuery(fmt.Sprintf("select value, time from t where time > '%s';", actual))
		c.Assert(err, IsNil)

		// note: the time condition will be removed
		c.Assert(q.GetWhereCondition(), IsNil)

		startTime := q.GetStartTime()
		c.Assert(startTime, Equals, t)
	}
}

func (self *QueryParserSuite) TestParseSelectWithTimeString(c *C) {
	t, err := time.Parse("2006-01-02 15:04:05", "2013-08-12 23:32:01.232")
	c.Assert(err, IsNil)
	q, err := ParseSelectQuery("select value, time from t where time > '2013-08-12 23:32:01.232';")
	c.Assert(err, IsNil)

	// note: the time condition will be removed
	c.Assert(q.GetWhereCondition(), IsNil)

	startTime := q.GetStartTime()
	c.Assert(startTime, Equals, t)

	milliseconds := startTime.Sub(startTime.Round(time.Second)).Nanoseconds() / 1000000
	c.Assert(milliseconds, Equals, int64(232))
}

func (self *QueryParserSuite) TestParseSelectWithAnd(c *C) {
	q, err := ParseSelectQuery("select value from cpu.idle where value > exp() * 2 and value < exp() * 3;")
	c.Assert(err, IsNil)

	w := q.GetWhereCondition()
	c.Assert(w.Operation, Equals, "AND")

	// leftBoolExpression = 'value > exp() * 2'
	leftWhereCondition, ok := w.GetLeftWhereCondition()
	c.Assert(ok, Equals, true)
	leftBoolExpression, ok := leftWhereCondition.GetBoolExpression()
	c.Assert(ok, Equals, true)
	// rightBoolExpression = 'value > exp() * 3'
	rightBoolExpression, ok := w.Right.GetBoolExpression()
	c.Assert(ok, Equals, true)

	c.Assert(leftBoolExpression.Elems[0], DeepEquals, &Value{"value", ValueSimpleName, nil, nil})
	value := leftBoolExpression.Elems[1].Elems[0]
	c.Assert(value, DeepEquals, &Value{"exp", ValueFunctionCall, nil, nil})
	value = leftBoolExpression.Elems[1].Elems[1]
	c.Assert(value, DeepEquals, &Value{"2", ValueInt, nil, nil})
	c.Assert(leftBoolExpression.Name, Equals, ">")

	c.Assert(rightBoolExpression.Elems[0], DeepEquals, &Value{"value", ValueSimpleName, nil, nil})
	value = rightBoolExpression.Elems[1].Elems[0]
	c.Assert(value, DeepEquals, &Value{"exp", ValueFunctionCall, nil, nil})
	value = rightBoolExpression.Elems[1].Elems[1]
	c.Assert(value, DeepEquals, &Value{"3", ValueInt, nil, nil})
	c.Assert(rightBoolExpression.Name, Equals, "<")
}

func (self *QueryParserSuite) TestParseSelectWithGroupBy(c *C) {
	q, err := ParseSelectQuery("select count(*) from users.events group by user_email,time(1h) where time>now()-1d;")
	c.Assert(err, IsNil)

	c.Assert(q.GetColumnNames(), HasLen, 1)

	column := q.GetColumnNames()[0]
	c.Assert(column.IsFunctionCall(), Equals, true)
	c.Assert(column.Name, Equals, "count")
	c.Assert(column.Elems, HasLen, 1)
	c.Assert(column.Elems[0].IsFunctionCall(), Equals, false)
	c.Assert(column.Elems[0].Name, Equals, "*")

	groupBy := q.GetGroupByClause()
	c.Assert(groupBy.FillWithZero, Equals, false)
	c.Assert(groupBy.Elems, HasLen, 2)
	c.Assert(groupBy.Elems[0].IsFunctionCall(), Equals, false)
	c.Assert(groupBy.Elems[0].Name, Equals, "user_email")
	c.Assert(groupBy.Elems[1].IsFunctionCall(), Equals, true)
	c.Assert(groupBy.Elems[1].Name, Equals, "time")
	c.Assert(groupBy.Elems[1].Elems, HasLen, 1)
	c.Assert(groupBy.Elems[1].Elems[0].Name, Equals, "1h")
}

func (self *QueryParserSuite) TestParseSelectWithGroupByFillWithZero(c *C) {
	q, err := ParseSelectQuery("select count(*) from users.events group by user_email,time(1h) fill(0) where time>now()-1d;")
	c.Assert(err, IsNil)

	c.Assert(q.GetColumnNames(), HasLen, 1)

	column := q.GetColumnNames()[0]
	c.Assert(column.IsFunctionCall(), Equals, true)
	c.Assert(column.Name, Equals, "count")
	c.Assert(column.Elems, HasLen, 1)
	c.Assert(column.Elems[0].IsFunctionCall(), Equals, false)
	c.Assert(column.Elems[0].Name, Equals, "*")

	groupBy := q.GetGroupByClause()
	c.Assert(groupBy.FillWithZero, Equals, true)
	c.Assert(groupBy.Elems, HasLen, 2)
	c.Assert(groupBy.Elems[0].IsFunctionCall(), Equals, false)
	c.Assert(groupBy.Elems[0].Name, Equals, "user_email")
	c.Assert(groupBy.Elems[1].IsFunctionCall(), Equals, true)
	c.Assert(groupBy.Elems[1].Name, Equals, "time")
	c.Assert(groupBy.Elems[1].Elems, HasLen, 1)
	c.Assert(groupBy.Elems[1].Elems[0].Name, Equals, "1h")
}

func (self *QueryParserSuite) TestParseSelectWithGroupByWithInvalidFunctions(c *C) {
	for _, query := range []string{
		"select count(*) from users.events group by user_email,time(1h) foobar(0) where time>now()-1d;",
	} {
		_, err := ParseSelectQuery(query)
		c.Assert(err, NotNil)
	}
}

func (self *QueryParserSuite) TestParseFromWithNestedFunctions(c *C) {
	q, err := ParseSelectQuery("select top(10, count(*)) from users.events;")
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

func (self *QueryParserSuite) TestParseWhereClausePrecedence(c *C) {
	q, err := ParseSelectQuery("select value from cpu.idle where value > 90 and other_value > 10.0 or value > 80 and other_value > 20;")
	c.Assert(err, IsNil)

	whereCondition := q.GetWhereCondition()

	c.Assert(whereCondition.Operation, Equals, "OR")
	leftCondition, ok := whereCondition.Left.(*WhereCondition)
	c.Assert(ok, Equals, true)
	c.Assert(leftCondition.Operation, Equals, "AND")

	condition, ok := leftCondition.GetLeftWhereCondition()
	c.Assert(ok, Equals, true)
	leftExpression, ok := condition.GetBoolExpression()
	c.Assert(ok, Equals, true)
	c.Assert(leftExpression.Name, Equals, ">")
	c.Assert(leftExpression.Elems[0], DeepEquals, &Value{"value", ValueSimpleName, nil, nil})
	c.Assert(leftExpression.Elems[1], DeepEquals, &Value{"90", ValueInt, nil, nil})

	rightExpression, ok := leftCondition.Right.GetBoolExpression()
	c.Assert(ok, Equals, true)
	c.Assert(rightExpression.Name, Equals, ">")
	c.Assert(rightExpression.Elems[0], DeepEquals, &Value{"other_value", ValueSimpleName, nil, nil})
	c.Assert(rightExpression.Elems[1], DeepEquals, &Value{"10.0", ValueFloat, nil, nil})
}

func (self *QueryParserSuite) TestParseWhereClauseParentheses(c *C) {
	q, err := ParseSelectQuery("select value from cpu.idle where value > 90 and (other_value > 10 or value > 80) and other_value > 20;")
	c.Assert(err, IsNil)

	whereCondition := q.GetWhereCondition()

	first := whereCondition.Left.(*WhereCondition).Left.(*WhereCondition).Left.(*Value)
	second := whereCondition.Left.(*WhereCondition).Right
	third := whereCondition.Right.Left.(*Value)

	c.Assert(first.Name, Equals, ">")
	c.Assert(second.Operation, Equals, "OR")
	c.Assert(third.Name, Equals, ">")
}

func (self *QueryParserSuite) TestParseSelectWithOrderByAndLimit(c *C) {
	q, err := ParseSelectQuery("select value from t order asc limit 10;")
	c.Assert(err, IsNil)
	c.Assert(q.Limit, Equals, 10)
	c.Assert(q.Ascending, Equals, true)

	q, err = ParseSelectQuery("select value from t order desc;")
	c.Assert(err, IsNil)
	c.Assert(q.Ascending, Equals, false)

	q, err = ParseSelectQuery("select value from t limit 20;")
	c.Assert(err, IsNil)
	c.Assert(q.Limit, Equals, 20)
	// descending is the default
	c.Assert(q.Ascending, Equals, false)
}

func (self *QueryParserSuite) TestParseFromWithNestedFunctions2(c *C) {
	q, err := ParseSelectQuery("select count(distinct(email)) from user.events where time>now()-1d group by time(15m);")
	c.Assert(err, IsNil)
	c.Assert(q.GetColumnNames(), HasLen, 1)
	column := q.GetColumnNames()[0]
	c.Assert(column.IsFunctionCall(), Equals, true)
	c.Assert(column.Name, Equals, "count")
	c.Assert(column.Elems, HasLen, 1)
	c.Assert(column.Elems[0].IsFunctionCall(), Equals, true)
	c.Assert(column.Elems[0].Name, Equals, "distinct")
	c.Assert(column.Elems[0].Elems, HasLen, 1)
	c.Assert(column.Elems[0].Elems[0].Name, Equals, "email")

	c.Assert(q.GetGroupByClause().Elems, HasLen, 1)
	c.Assert(q.GetGroupByClause().Elems[0], DeepEquals, &Value{
		Name:  "time",
		Type:  ValueFunctionCall,
		Elems: []*Value{&Value{"15m", ValueDuration, nil, nil}},
	})
}

func (self *QueryParserSuite) TestParseSelectWithInvalidRegex(c *C) {
	_, err := ParseSelectQuery("select email from users.events where email =~ /[/i and time>now()-2d;")
	c.Assert(err, ErrorMatches, ".*missing closing.*")
}

func (self *QueryParserSuite) TestParseSelectWithRegexCondition(c *C) {
	q, err := ParseSelectQuery("select email from users.events where email =~ /gmail\\.com/i and time>now()-2d;")
	c.Assert(err, IsNil)
	w := q.GetWhereCondition()

	// note: conditions that involve time are removed after the query is parsed
	regexExpression, _ := w.GetBoolExpression()
	c.Assert(regexExpression.Elems[0], DeepEquals, &Value{"email", ValueSimpleName, nil, nil})
	c.Assert(regexExpression.Name, Equals, "=~")
	expr := regexExpression.Elems[1]
	c.Assert(expr.Type, Equals, ValueRegex)
	c.Assert(expr.Name, Equals, "gmail\\.com")
}

func (self *QueryParserSuite) TestParseSelectWithComplexArithmeticOperations(c *C) {
	q, err := ParseSelectQuery("select value from cpu.idle where .30 < value * 1 / 3 ;")
	c.Assert(err, IsNil)

	boolExpression, ok := q.GetWhereCondition().GetBoolExpression()
	c.Assert(ok, Equals, true)

	c.Assert(boolExpression.Elems[0], DeepEquals, &Value{".30", ValueFloat, nil, nil})

	// value * 1 / 3
	rightExpression := boolExpression.Elems[1]

	// value * 1
	left := rightExpression.Elems[0]
	c.Assert(left.Name, Equals, "*")
	value := left.Elems[0]
	c.Assert(value.Name, Equals, "value")
	one := left.Elems[1]
	c.Assert(one.Name, Equals, "1")

	// '3'
	c.Assert(rightExpression.Name, Equals, "/")
	value = rightExpression.Elems[1]
	c.Assert(value.Name, Equals, "3")
}

func (self *QueryParserSuite) TestTimeConditionWithFloats(c *C) {
	var startTime int64 = 13853965
	for _, query := range []string{
		fmt.Sprintf("select * from foo where time > %d000000000.0", startTime),
		fmt.Sprintf("select * from foo where time > %ds", startTime),
		fmt.Sprintf("select * from foo where time > %d.0s", startTime),
	} {
		q, err := ParseSelectQuery(query)
		c.Assert(err, IsNil)
		c.Assert(q.GetStartTime(), Equals, time.Unix(startTime, 0).UTC())
	}
}

func (self *QueryParserSuite) TestQueryWithInCondition(c *C) {
	query := "select * from foo where bar in ('baz', 'bazz')"
	q, err := ParseSelectQuery(query)
	c.Assert(err, IsNil)
	condition := q.GetWhereCondition()
	expr, ok := condition.GetBoolExpression()
	c.Assert(ok, Equals, true)
	c.Assert(expr.Name, Equals, "in")
	left := expr.Elems[0]
	right := expr.Elems[1:]
	c.Assert(left.Name, Equals, "bar")
	c.Assert(right, HasLen, 2)
	c.Assert(right[0].Name, Equals, "baz")
	c.Assert(right[1].Name, Equals, "bazz")
}

func (self *QueryParserSuite) TestParseSinglePointQuery(c *C) {
	q, err := ParseSelectQuery("select value from foo where time = 999 and sequence_number = 1;")
	c.Assert(err, IsNil)

	w := q.GetWhereCondition()
	c.Assert(w.Operation, Equals, "AND")

	// leftBoolExpression = 'time = 999'
	leftWhereCondition, ok := w.GetLeftWhereCondition()
	c.Assert(ok, Equals, true)
	leftBoolExpression, ok := leftWhereCondition.GetBoolExpression()
	c.Assert(ok, Equals, true)

	// rightBoolExpression = 'sequence_number = 1'
	rightBoolExpression, ok := w.Right.GetBoolExpression()
	c.Assert(ok, Equals, true)

	c.Assert(leftBoolExpression.Elems[0], DeepEquals, &Value{"time", ValueSimpleName, nil, nil})
	value := leftBoolExpression.Elems[1]
	c.Assert(value, DeepEquals, &Value{"999", ValueInt, nil, nil})
	c.Assert(leftBoolExpression.Name, Equals, "=")

	c.Assert(rightBoolExpression.Elems[0], DeepEquals, &Value{"sequence_number", ValueSimpleName, nil, nil})
	value = rightBoolExpression.Elems[1]
	c.Assert(value, DeepEquals, &Value{"1", ValueInt, nil, nil})
	c.Assert(rightBoolExpression.Name, Equals, "=")
}

// TODO: test reversed order of time and sequence_number
func (self *QueryParserSuite) TestIsSinglePointQuery(c *C) {
	query := "select * from foo where time = 123 and sequence_number = 99"
	q, err := ParseSelectQuery(query)
	c.Assert(err, IsNil)
	result := q.IsSinglePointQuery()
	c.Assert(result, Equals, true)
}

// TODO:
// insert into user.events.count.per_day select count(*) from user.events where time<forever group by time(1d)
// insert into :series_name.percentiles.95 select percentile(95,value) from stats.* where time<forever group by time(1d)
