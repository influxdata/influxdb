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
	q, err := ParseQuery("select from t where c = '5';")
	c.Assert(err, IsNil)
	w := q.GetWhereClause()
	c.Assert(q.GetFromClause().TableName, Equals, "t")
	c.Assert(w.ColumnName, Equals, "c")
	c.Assert(w.Value, Equals, "5")
}
