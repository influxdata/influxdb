package influxql_test

import (
	"testing"

	"github.com/influxdata/flux"
	"github.com/influxdata/influxdb/v2/query/influxql"
)

func TestCompiler(t *testing.T) {
	var _ flux.Compiler = (*influxql.Compiler)(nil)
}
