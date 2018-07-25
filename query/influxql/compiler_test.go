package influxql_test

import (
	"testing"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/influxql"
)

func TestCompiler(t *testing.T) {
	var _ query.Compiler = (*influxql.Compiler)(nil)
}
