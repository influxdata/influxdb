package parser

import (
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
		values = append(values, &Value{str, ValueSimpleName, false, nil, nil})
	}
	return
}

func (self *QueryParserSuite) TestParseBasicSelectQuery(c *C) {
	for _, query := range []string{
		"select value from t where c == '5';",
		// semicolon is optional
		"select value from t where c == '5'",
	} {
		q, err := ParseQuery(query)
		c.Assert(err, IsNil)

		c.Assert(q.GetQueryString(), Equals, query)

		c.Assert(q.GetColumnNames(), DeepEquals, ToValueArray("value"))
		w := q.GetWhereCondition()

		boolExpression, ok := w.GetBoolExpression()
		c.Assert(ok, Equals, true)

		leftExpression := boolExpression.Left
		rightExpression := boolExpression.Right

		leftValue, ok := leftExpression.GetLeftValue() // simple value is an expression with one value, e.g. it doesn't combine value using arithmetic operations
		rightValue, ok := rightExpression.GetLeftValue()

		c.Assert(leftValue.Name, Equals, "c")
		c.Assert(boolExpression.Operation, Equals, "==")
		c.Assert(rightValue.Name, Equals, "5")
	}
}

func (self *QueryParserSuite) TestSimpleFromClause(c *C) {
	q, err := ParseQuery("select value from t;")
	c.Assert(err, IsNil)

	fromClause := q.GetFromClause()
	c.Assert(fromClause.Type, Equals, FromClauseArray)
	c.Assert(fromClause.Names, HasLen, 1)
	c.Assert(fromClause.Names[0].Name.Name, Equals, "t")
}

func (self *QueryParserSuite) TestParseFromWithMergedTable(c *C) {
	q, err := ParseQuery("select count(*) from newsletter.signups merge user.signups where time>now()-1d;")
	c.Assert(err, IsNil)
	fromClause := q.GetFromClause()
	c.Assert(fromClause.Type, Equals, FromClauseMerge)
	c.Assert(fromClause.Names, HasLen, 2)
	c.Assert(fromClause.Names[0].Name.Name, Equals, "newsletter.signups")
	c.Assert(fromClause.Names[1].Name.Name, Equals, "user.signups")
}

func (self *QueryParserSuite) TestMultipleAggregateFunctions(c *C) {
	q, err := ParseQuery("select first(bar), last(bar) from foo")
	c.Assert(err, IsNil)
	columns := q.GetColumnNames()
	c.Assert(columns, HasLen, 2)
	c.Assert(columns[0].Name, Equals, "first")
	c.Assert(columns[1].Name, Equals, "last")
}

func (self *QueryParserSuite) TestParseFromWithJoinedTable(c *C) {
	q, err := ParseQuery("select max(newsletter.signups.value, user.signups.value) from newsletter.signups inner join user.signups where time>now()-1d;")
	c.Assert(err, IsNil)
	fromClause := q.GetFromClause()
	c.Assert(fromClause.Type, Equals, FromClauseInnerJoin)
	c.Assert(fromClause.Names, HasLen, 2)
	c.Assert(fromClause.Names[0].Name.Name, Equals, "newsletter.signups")
	c.Assert(fromClause.Names[1].Name.Name, Equals, "user.signups")
}

func (self *QueryParserSuite) TestParseSelectWithInsensitiveRegexTables(c *C) {
	q, err := ParseQuery("select email from /users.*/i where time>now()-2d;")
	c.Assert(err, IsNil)

	fromClause := q.GetFromClause()
	c.Assert(fromClause.Type, Equals, FromClauseArray)
	c.Assert(fromClause.Names, HasLen, 1)
	c.Assert(fromClause.Names[0].Name.Name, Equals, "users.*")
	c.Assert(fromClause.Names[0].Name.Type, Equals, ValueRegex)
	c.Assert(fromClause.Names[0].Name.IsCaseInsensitive, Equals, true)
}

func (self *QueryParserSuite) TestParseSelectWithRegexTables(c *C) {
	q, err := ParseQuery("select email from /users.*/ where time>now()-2d;")
	c.Assert(err, IsNil)

	fromClause := q.GetFromClause()
	c.Assert(fromClause.Type, Equals, FromClauseArray)
	c.Assert(fromClause.Names, HasLen, 1)
	c.Assert(fromClause.Names[0].Name.Name, Equals, "users.*")
	c.Assert(fromClause.Names[0].Name.Type, Equals, ValueRegex)
	c.Assert(fromClause.Names[0].Name.IsCaseInsensitive, Equals, false)
}

func (self *QueryParserSuite) TestMergeFromClause(c *C) {
	q, err := ParseQuery("select value from t1 merge t2 where c == '5';")
	c.Assert(err, IsNil)

	c.Assert(q.GetColumnNames(), DeepEquals, ToValueArray("value"))
	w := q.GetWhereCondition()

	boolExpression, ok := w.GetBoolExpression()
	c.Assert(ok, Equals, true)

	leftExpression := boolExpression.Left
	rightExpression := boolExpression.Right

	leftValue, ok := leftExpression.GetLeftValue() // simple value is an expression with one value, e.g. it doesn't combine value using arithmetic operations
	rightValue, ok := rightExpression.GetLeftValue()

	c.Assert(leftValue.Name, Equals, "c")
	c.Assert(boolExpression.Operation, Equals, "==")
	c.Assert(rightValue.Name, Equals, "5")
}

func (self *QueryParserSuite) TestParseSelectWithoutWhereClause(c *C) {
	q, err := ParseQuery("select value, time from t;")
	c.Assert(err, IsNil)
	c.Assert(q.GetColumnNames(), DeepEquals, ToValueArray("value", "time"))
	c.Assert(q.GetWhereCondition(), IsNil)
}

func (self *QueryParserSuite) TestParseSelectWithUpperCase(c *C) {
	q, err := ParseQuery("SELECT VALUE, TIME FROM t WHERE C == '5';")
	c.Assert(err, IsNil)
	c.Assert(q.GetColumnNames(), DeepEquals, ToValueArray("VALUE", "TIME"))
	w := q.GetWhereCondition()

	boolExpression, ok := w.GetBoolExpression()
	c.Assert(ok, Equals, true)
	leftExpression := boolExpression.Left
	rightExpression := boolExpression.Right

	leftValue, ok := leftExpression.GetLeftValue()
	c.Assert(ok, Equals, true)
	rightValue, ok := rightExpression.GetLeftValue()
	c.Assert(ok, Equals, true)

	c.Assert(leftValue.Name, Equals, "C")
	c.Assert(rightValue.Name, Equals, "5")
}

func (self *QueryParserSuite) TestParseSelectWithMultipleColumns(c *C) {
	q, err := ParseQuery("select value, time from t;")

	c.Assert(err, IsNil)
	columns := q.GetColumnNames()
	c.Assert(columns, HasLen, 2)
	c.Assert(columns[0].Name, Equals, "value")
	c.Assert(columns[1].Name, Equals, "time")
}

func (self *QueryParserSuite) TestParseSelectWithInequality(c *C) {
	q, err := ParseQuery("select value, time from t where c < 5;")
	c.Assert(err, IsNil)
	w := q.GetWhereCondition()

	boolExpression, ok := w.GetBoolExpression()
	c.Assert(ok, Equals, true)
	leftExpression := boolExpression.Left
	rightExpression := boolExpression.Right
	leftValue, ok := leftExpression.GetLeftValue()
	c.Assert(ok, Equals, true)
	rightValue, ok := rightExpression.GetLeftValue()
	c.Assert(ok, Equals, true)

	c.Assert(leftValue.Name, Equals, "c")
	c.Assert(boolExpression.Operation, Equals, "<")
	c.Assert(rightValue.Name, Equals, "5")
}

func (self *QueryParserSuite) TestParseSelectWithTimeCondition(c *C) {
	q, err := ParseQuery("select value, time from t where time > now() - 1d;")
	c.Assert(err, IsNil)

	// note: the time condition will be removed
	c.Assert(q.GetWhereCondition(), IsNil)

	c.Assert(q.GetStartTime().Round(time.Minute), Equals, time.Now().Add(-24*time.Hour).Round(time.Minute))
}

func (self *QueryParserSuite) TestParseSelectWithAnd(c *C) {
	q, err := ParseQuery("select value from cpu.idle where value > exp() * 2 and value < exp() * 3;")
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

	c.Assert(leftBoolExpression.Left.Left, DeepEquals, &Value{"value", ValueSimpleName, false, nil, nil})
	expr, ok := leftBoolExpression.Right.GetLeftExpression()
	c.Assert(ok, Equals, true)
	value, ok := expr.GetLeftValue()
	c.Assert(ok, Equals, true)
	c.Assert(value, DeepEquals, &Value{"exp", ValueFunctionCall, false, nil, nil})
	value, ok = leftBoolExpression.Right.Right.GetLeftValue()
	c.Assert(ok, Equals, true)
	c.Assert(value, DeepEquals, &Value{"2", ValueInt, false, nil, nil})
	c.Assert(leftBoolExpression.Operation, Equals, ">")

	c.Assert(rightBoolExpression.Left.Left, DeepEquals, &Value{"value", ValueSimpleName, false, nil, nil})
	expr, ok = rightBoolExpression.Right.GetLeftExpression()
	c.Assert(ok, Equals, true)
	value, ok = expr.GetLeftValue()
	c.Assert(ok, Equals, true)
	c.Assert(value, DeepEquals, &Value{"exp", ValueFunctionCall, false, nil, nil})
	value, ok = rightBoolExpression.Right.Right.GetLeftValue()
	c.Assert(ok, Equals, true)
	c.Assert(value, DeepEquals, &Value{"3", ValueInt, false, nil, nil})
	c.Assert(rightBoolExpression.Operation, Equals, "<")
}

func (self *QueryParserSuite) TestParseSelectWithGroupBy(c *C) {
	q, err := ParseQuery("select count(*) from users.events group by user_email,time(1h) where time>now()-1d;")
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
}

func (self *QueryParserSuite) TestParseFromWithNestedFunctions(c *C) {
	q, err := ParseQuery("select top(10, count(*)) from users.events;")
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
	q, err := ParseQuery("select value from cpu.idle where value > 90 and other_value > 10.0 or value > 80 and other_value > 20;")
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
	c.Assert(leftExpression.Operation, Equals, ">")
	c.Assert(leftExpression.Left.Left, DeepEquals, &Value{"value", ValueSimpleName, false, nil, nil})
	c.Assert(leftExpression.Right.Left, DeepEquals, &Value{"90", ValueInt, false, nil, nil})

	rightExpression, ok := leftCondition.Right.GetBoolExpression()
	c.Assert(ok, Equals, true)
	c.Assert(rightExpression.Operation, Equals, ">")
	c.Assert(rightExpression.Left.Left, DeepEquals, &Value{"other_value", ValueSimpleName, false, nil, nil})
	c.Assert(rightExpression.Right.Left, DeepEquals, &Value{"10.0", ValueFloat, false, nil, nil})
}

func (self *QueryParserSuite) TestParseWhereClauseParentheses(c *C) {
	q, err := ParseQuery("select value from cpu.idle where value > 90 and (other_value > 10 or value > 80) and other_value > 20;")
	c.Assert(err, IsNil)

	whereCondition := q.GetWhereCondition()

	first := whereCondition.Left.(*WhereCondition).Left.(*WhereCondition).Left.(*BoolExpression)
	second := whereCondition.Left.(*WhereCondition).Right
	third := whereCondition.Right.Left.(*BoolExpression)

	c.Assert(first.Operation, Equals, ">")
	c.Assert(second.Operation, Equals, "OR")
	c.Assert(third.Operation, Equals, ">")
}

func (self *QueryParserSuite) TestParseSelectWithOrderByAndLimit(c *C) {
	q, err := ParseQuery("select value from t order asc limit 10;")
	c.Assert(err, IsNil)
	c.Assert(q.Limit, Equals, 10)
	c.Assert(q.Ascending, Equals, true)

	q, err = ParseQuery("select value from t order desc;")
	c.Assert(err, IsNil)
	c.Assert(q.Ascending, Equals, false)

	q, err = ParseQuery("select value from t limit 20;")
	c.Assert(err, IsNil)
	c.Assert(q.Limit, Equals, 20)
	// descending is the default
	c.Assert(q.Ascending, Equals, false)
}

func (self *QueryParserSuite) TestParseFromWithNestedFunctions2(c *C) {
	q, err := ParseQuery("select count(distinct(email)) from user.events where time>now()-1d group by time(15m);")
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

	c.Assert(q.GetGroupByClause(), HasLen, 1)
	c.Assert(q.GetGroupByClause()[0], DeepEquals, &Value{
		Name:              "time",
		Type:              ValueFunctionCall,
		IsCaseInsensitive: false,
		Elems:             []*Value{&Value{"15m", ValueDuration, false, nil, nil}},
	})
}

func (self *QueryParserSuite) TestParseSelectWithInvalidRegex(c *C) {
	_, err := ParseQuery("select email from users.events where email =~ /[/i and time>now()-2d;")
	c.Assert(err, ErrorMatches, ".*missing closing.*")
}

func (self *QueryParserSuite) TestParseSelectWithRegexCondition(c *C) {
	q, err := ParseQuery("select email from users.events where email =~ /gmail\\.com/i and time>now()-2d;")
	c.Assert(err, IsNil)
	w := q.GetWhereCondition()

	// note: conditions that involve time are removed after the query is parsed
	regexExpression, _ := w.GetBoolExpression()
	c.Assert(regexExpression.Left.Left, DeepEquals, &Value{"email", ValueSimpleName, false, nil, nil})
	c.Assert(regexExpression.Operation, Equals, "=~")
	expr, ok := regexExpression.Right.GetLeftValue()
	c.Assert(ok, Equals, true)
	c.Assert(expr.Type, Equals, ValueRegex)
	c.Assert(expr.Name, Equals, "gmail\\.com")
	c.Assert(expr.IsCaseInsensitive, Equals, true)
}

func (self *QueryParserSuite) TestParseSelectWithComplexArithmeticOperations(c *C) {
	q, err := ParseQuery("select value from cpu.idle where .30 < value * 1 / 3 ;")
	c.Assert(err, IsNil)

	boolExpression, ok := q.GetWhereCondition().GetBoolExpression()
	c.Assert(ok, Equals, true)

	c.Assert(boolExpression.Left.Left, DeepEquals, &Value{".30", ValueFloat, false, nil, nil})

	// value * 1 / 3
	rightExpression := boolExpression.Right

	// value * 1
	left, ok := rightExpression.GetLeftExpression()
	c.Assert(ok, Equals, true)
	c.Assert(left.Operation, Equals, byte('*'))
	_value, _ := left.GetLeftExpression()
	value, _ := _value.GetLeftValue()
	c.Assert(value.Name, Equals, "value")
	one, _ := left.Right.GetLeftValue()
	c.Assert(one.Name, Equals, "1")

	// '3'
	c.Assert(rightExpression.Operation, Equals, byte('/'))
	value, ok = rightExpression.Right.GetLeftValue()
	c.Assert(ok, Equals, true)
	c.Assert(value.Name, Equals, "3")
}

// TODO:
// insert into user.events.count.per_day select count(*) from user.events where time<forever group by time(1d)
// insert into :series_name.percentiles.95 select percentile(95,value) from stats.* where time<forever group by time(1d)
