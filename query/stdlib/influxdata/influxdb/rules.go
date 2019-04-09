package influxdb

import (
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/stdlib/universe"
)

// func init() {
// 	plan.RegisterPhysicalRules(
// 		PushDownRangeRule{},
// 	)
// }

// PushDownRangeRule pushes down a range filter to storage
type PushDownRangeRule struct{}

func (rule PushDownRangeRule) Name() string {
	return "PushDownRangeRule"
}

// Pattern matches 'from |> range'
func (rule PushDownRangeRule) Pattern() plan.Pattern {
	return plan.Pat(universe.RangeKind, plan.Pat(FromKind))
}

// Rewrite converts 'from |> range' into 'ReadRange'
func (rule PushDownRangeRule) Rewrite(node plan.Node) (plan.Node, bool, error) {
	fromNode := node.Predecessors()[0]
	fromSpec := fromNode.ProcedureSpec().(*FromProcedureSpec)

	rangeSpec := node.ProcedureSpec().(*universe.RangeProcedureSpec)
	return plan.CreatePhysicalNode("ReadRange", &ReadRangePhysSpec{
		Bucket:   fromSpec.Bucket,
		BucketID: fromSpec.BucketID,
		Bounds:   rangeSpec.Bounds,
	}), true, nil
}
