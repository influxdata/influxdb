package influxql_test

import (
	"testing"

	"github.com/influxdata/flux"
	"github.com/influxdata/influxdb/query/influxql"
)

func TestCompiler(t *testing.T) {
	var _ flux.Compiler = (*influxql.Compiler)(nil)
}
