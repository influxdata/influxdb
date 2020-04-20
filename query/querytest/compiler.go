package querytest

import (
	"context"

	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/stdlib/influxdata/influxdb"
	"github.com/influxdata/flux/stdlib/influxdata/influxdb/v1"
	"github.com/influxdata/influxdb/v2/query/influxql"
)

// MakeFromInfluxJSONCompiler returns a compiler that replaces all From operations with FromJSON.
func MakeFromInfluxJSONCompiler(c *influxql.Compiler, jsonFile string) {
	c.WithLogicalPlannerOptions(plan.AddLogicalRules(ReplaceFromRule{Filename: jsonFile}))
}

type ReplaceFromRule struct {
	Filename string
}

func (ReplaceFromRule) Name() string {
	return "ReplaceFromRule"
}

func (ReplaceFromRule) Pattern() plan.Pattern {
	return plan.Pat(influxdb.FromKind)
}

func (r ReplaceFromRule) Rewrite(ctx context.Context, n plan.Node) (plan.Node, bool, error) {
	if err := n.ReplaceSpec(&v1.FromInfluxJSONProcedureSpec{
		File: r.Filename,
	}); err != nil {
		return nil, false, err
	}
	return n, true, nil
}
