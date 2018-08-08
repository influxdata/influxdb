package influxql_test

import (
	"testing"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/influxql"
)

func TestDialect(t *testing.T) {
	var _ query.Dialect = (*influxql.Dialect)(nil)
}
