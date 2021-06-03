package query

import (
	"testing"

	"github.com/influxdata/influxql"
	"github.com/stretchr/testify/require"
)

func TestCompile_RewriteSubqueryRegex(t *testing.T) {
	q := `SELECT top(mean, 10), host FROM (SELECT mean(value) FROM cpu WHERE id =~ /^(server-1|server-2|server-3)$/ GROUP BY host)`
	stmt, err := influxql.ParseStatement(q)
	require.NoError(t, err)
	s := stmt.(*influxql.SelectStatement)

	compiled, err := Compile(s, CompileOptions{})
	require.NoError(t, err)

	c := compiled.(*compiledStatement)
	require.Len(t, c.stmt.Sources, 1)

	subquery := c.stmt.Sources[0]
	require.Equal(t, `(SELECT mean(value) FROM cpu WHERE id = 'server-1' OR id = 'server-2' OR id = 'server-3' GROUP BY host)`, subquery.String())
}
