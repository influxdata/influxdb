package querytest

import (
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/querytest"
	"github.com/influxdata/flux/stdlib/influxdata/influxdb/v1"
	"github.com/influxdata/influxdb/query/stdlib/influxdata/influxdb"
)

// FromInfluxJSONCompiler returns a compiler that replaces all From operations with FromJSON.
func FromInfluxJSONCompiler(c flux.Compiler, jsonFile string) *querytest.ReplaceSpecCompiler {
	return querytest.NewReplaceSpecCompiler(c, func(op *flux.Operation) flux.OperationSpec {
		if op.Spec.Kind() == influxdb.FromKind {
			return &v1.FromInfluxJSONOpSpec{
				File: jsonFile,
			}
		}
		return nil
	})
}
