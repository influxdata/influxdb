package influxql_test

import (
	"testing"

	"github.com/influxdata/flux"
	"github.com/influxdata/influxdb/query/influxql"
)

func TestDialect(t *testing.T) {
	var _ flux.Dialect = (*influxql.Dialect)(nil)
}
