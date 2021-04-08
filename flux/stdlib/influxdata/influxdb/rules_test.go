package influxdb_test

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/execute/executetest"
	"github.com/influxdata/flux/interpreter"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/plan/plantest"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/stdlib/universe"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb/flux/stdlib/influxdata/influxdb"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
)

func fluxTime(t int64) flux.Time {
	return flux.Time{
		Absolute: time.Unix(0, t).UTC(),
	}
}

func TestPushDownRangeRule(t *testing.T) {
	fromSpec := influxdb.FromStorageProcedureSpec{
		Bucket: influxdb.NameOrID{Name: "my-bucket"},
	}
	rangeSpec := universe.RangeProcedureSpec{
		Bounds: flux.Bounds{
			Start: fluxTime(5),
			Stop:  fluxTime(10),
		},
	}
	readRangeSpec := influxdb.ReadRangePhysSpec{
		Bucket: "my-bucket",
		Bounds: flux.Bounds{
			Start: fluxTime(5),
			Stop:  fluxTime(10),
		},
	}

	tests := []plantest.RuleTestCase{
		{
			Name: "simple",
			// from -> range  =>  ReadRange
			Rules: []plan.Rule{
				influxdb.FromStorageRule{},
				influxdb.PushDownRangeRule{},
			},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreateLogicalNode("from", &fromSpec),
					plan.CreateLogicalNode("range", &rangeSpec),
				},
				Edges: [][2]int{{0, 1}},
			},
			After: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadRange", &readRangeSpec),
				},
			},
		},
		{
			Name: "with successor",
			// from -> range -> count  =>  ReadRange -> count
			Rules: []plan.Rule{
				influxdb.FromStorageRule{},
				influxdb.PushDownRangeRule{},
			},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreateLogicalNode("from", &fromSpec),
					plan.CreateLogicalNode("range", &rangeSpec),
					plan.CreatePhysicalNode("count", &universe.CountProcedureSpec{}),
				},
				Edges: [][2]int{
					{0, 1},
					{1, 2},
				},
			},
			After: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadRange", &readRangeSpec),
					plan.CreatePhysicalNode("count", &universe.CountProcedureSpec{}),
				},
				Edges: [][2]int{{0, 1}},
			},
		},
		{
			Name: "with multiple successors",
			// count      mean
			//     \     /          count     mean
			//      range       =>      \    /
			//        |                ReadRange
			//       from
			Rules: []plan.Rule{
				influxdb.FromStorageRule{},
				influxdb.PushDownRangeRule{},
			},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreateLogicalNode("from", &fromSpec),
					plan.CreateLogicalNode("range", &rangeSpec),
					plan.CreatePhysicalNode("count", &universe.CountProcedureSpec{}),
					plan.CreatePhysicalNode("mean", &universe.MeanProcedureSpec{}),
				},
				Edges: [][2]int{
					{0, 1},
					{1, 2},
					{1, 3},
				},
			},
			After: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadRange", &readRangeSpec),
					plan.CreatePhysicalNode("count", &universe.CountProcedureSpec{}),
					plan.CreatePhysicalNode("mean", &universe.MeanProcedureSpec{}),
				},
				Edges: [][2]int{
					{0, 1},
					{0, 2},
				},
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			plantest.PhysicalRuleTestHelper(t, &tc)
		})
	}
}

func TestPushDownFilterRule(t *testing.T) {
	var (
		bounds = flux.Bounds{
			Start: fluxTime(5),
			Stop:  fluxTime(10),
		}

		pushableFn1             = executetest.FunctionExpression(t, `(r) => r._measurement == "cpu"`)
		pushableFn2             = executetest.FunctionExpression(t, `(r) => r._field == "cpu"`)
		pushableFn1and2         = executetest.FunctionExpression(t, `(r) => r._measurement == "cpu" and r._field == "cpu"`)
		unpushableFn            = executetest.FunctionExpression(t, `(r) => 0.5 < r._value`)
		pushableAndUnpushableFn = executetest.FunctionExpression(t, `(r) => r._measurement == "cpu" and 0.5 < r._value`)
	)

	makeResolvedFilterFn := func(expr *semantic.FunctionExpression) interpreter.ResolvedFunction {
		return interpreter.ResolvedFunction{
			Fn: expr,
		}
	}

	toStoragePredicate := func(fn *semantic.FunctionExpression) *datatypes.Predicate {
		body, ok := fn.GetFunctionBodyExpression()
		if !ok {
			panic("more than one statement in function body")
		}

		predicate, err := influxdb.ToStoragePredicate(body, "r")
		if err != nil {
			panic(err)
		}
		return predicate
	}

	tests := []plantest.RuleTestCase{
		{
			Name: "simple",
			// ReadRange -> filter  =>  ReadRange
			Rules: []plan.Rule{influxdb.PushDownFilterRule{}},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadRange", &influxdb.ReadRangePhysSpec{
						Bounds: bounds,
					}),
					plan.CreatePhysicalNode("filter", &universe.FilterProcedureSpec{
						Fn: makeResolvedFilterFn(pushableFn1),
					}),
				},
				Edges: [][2]int{
					{0, 1},
				},
			},
			After: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("merged_ReadRange_filter", &influxdb.ReadRangePhysSpec{
						Bounds:    bounds,
						Predicate: toStoragePredicate(pushableFn1),
					}),
				},
			},
		},
		{
			Name: "two filters",
			// ReadRange -> filter -> filter  =>  ReadRange    (rule applied twice)
			Rules: []plan.Rule{influxdb.PushDownFilterRule{}},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadRange", &influxdb.ReadRangePhysSpec{
						Bounds: bounds,
					}),
					plan.CreatePhysicalNode("filter1", &universe.FilterProcedureSpec{
						Fn: makeResolvedFilterFn(pushableFn1),
					}),
					plan.CreatePhysicalNode("filter2", &universe.FilterProcedureSpec{
						Fn: makeResolvedFilterFn(pushableFn2),
					}),
				},
				Edges: [][2]int{
					{0, 1},
					{1, 2},
				},
			},
			After: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("merged_ReadRange_filter1_filter2", &influxdb.ReadRangePhysSpec{
						Bounds:    bounds,
						Predicate: toStoragePredicate(pushableFn1and2),
					}),
				},
			},
		},
		{
			Name: "partially pushable filter",
			// ReadRange -> partially-pushable-filter  =>  ReadRange -> unpushable-filter
			Rules: []plan.Rule{influxdb.PushDownFilterRule{}},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadRange", &influxdb.ReadRangePhysSpec{
						Bounds: bounds,
					}),
					plan.CreatePhysicalNode("filter", &universe.FilterProcedureSpec{
						Fn: makeResolvedFilterFn(pushableAndUnpushableFn),
					}),
				},
				Edges: [][2]int{
					{0, 1},
				},
			},
			After: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadRange", &influxdb.ReadRangePhysSpec{
						Bounds:    bounds,
						Predicate: toStoragePredicate(pushableFn1),
					}),
					plan.CreatePhysicalNode("filter", &universe.FilterProcedureSpec{
						Fn: makeResolvedFilterFn(unpushableFn),
					}),
				},
				Edges: [][2]int{
					{0, 1},
				},
			},
		},
		{
			Name: "from range filter",
			// from -> range -> filter  =>  ReadRange
			Rules: []plan.Rule{
				influxdb.FromStorageRule{},
				influxdb.PushDownRangeRule{},
				influxdb.PushDownFilterRule{},
			},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreateLogicalNode("from", &influxdb.FromStorageProcedureSpec{}),
					plan.CreatePhysicalNode("range", &universe.RangeProcedureSpec{
						Bounds: bounds,
					}),
					plan.CreatePhysicalNode("filter", &universe.FilterProcedureSpec{
						Fn: makeResolvedFilterFn(pushableFn1)},
					),
				},
				Edges: [][2]int{
					{0, 1},
					{1, 2},
				},
			},
			After: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("merged_ReadRange_filter", &influxdb.ReadRangePhysSpec{
						Bounds:    bounds,
						Predicate: toStoragePredicate(pushableFn1),
					}),
				},
			},
		},
		{
			Name: "unpushable filter",
			// from -> filter  =>  from -> filter   (no change)
			Rules: []plan.Rule{influxdb.PushDownFilterRule{}},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadRange", &influxdb.ReadRangePhysSpec{
						Bounds: bounds,
					}),
					plan.CreatePhysicalNode("filter", &universe.FilterProcedureSpec{
						Fn: makeResolvedFilterFn(unpushableFn),
					}),
				},
				Edges: [][2]int{
					{0, 1},
				},
			},
			NoChange: true,
		},
		{
			Name:  `exists r.host`,
			Rules: []plan.Rule{influxdb.PushDownFilterRule{}},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadRange", &influxdb.ReadRangePhysSpec{
						Bounds: bounds,
					}),
					plan.CreatePhysicalNode("filter", &universe.FilterProcedureSpec{
						Fn: makeResolvedFilterFn(executetest.FunctionExpression(t, `(r) => exists r.host`)),
					}),
				},
				Edges: [][2]int{
					{0, 1},
				},
			},
			After: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("merged_ReadRange_filter", &influxdb.ReadRangePhysSpec{
						Bounds:    bounds,
						Predicate: toStoragePredicate(executetest.FunctionExpression(t, `(r) => r.host != ""`)),
					}),
				},
			},
		},
		{
			Name:  `not exists r.host`,
			Rules: []plan.Rule{influxdb.PushDownFilterRule{}},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadRange", &influxdb.ReadRangePhysSpec{
						Bounds: bounds,
					}),
					plan.CreatePhysicalNode("filter", &universe.FilterProcedureSpec{
						Fn: makeResolvedFilterFn(executetest.FunctionExpression(t, `(r) => not exists r.host`)),
					}),
				},
				Edges: [][2]int{
					{0, 1},
				},
			},
			After: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("merged_ReadRange_filter", &influxdb.ReadRangePhysSpec{
						Bounds:    bounds,
						Predicate: toStoragePredicate(executetest.FunctionExpression(t, `(r) => r.host == ""`)),
					}),
				},
			},
		},
		{
			Name:  `r.host == ""`,
			Rules: []plan.Rule{influxdb.PushDownFilterRule{}},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadRange", &influxdb.ReadRangePhysSpec{
						Bounds: bounds,
					}),
					plan.CreatePhysicalNode("filter", &universe.FilterProcedureSpec{
						Fn: makeResolvedFilterFn(executetest.FunctionExpression(t, `(r) => r.host == ""`)),
					}),
				},
				Edges: [][2]int{
					{0, 1},
				},
			},
			NoChange: true,
		},
		{
			Name:  `r.host != ""`,
			Rules: []plan.Rule{influxdb.PushDownFilterRule{}},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadRange", &influxdb.ReadRangePhysSpec{
						Bounds: bounds,
					}),
					plan.CreatePhysicalNode("filter", &universe.FilterProcedureSpec{
						Fn: makeResolvedFilterFn(executetest.FunctionExpression(t, `(r) => r.host != ""`)),
					}),
				},
				Edges: [][2]int{
					{0, 1},
				},
			},
			After: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("merged_ReadRange_filter", &influxdb.ReadRangePhysSpec{
						Bounds:    bounds,
						Predicate: toStoragePredicate(executetest.FunctionExpression(t, `(r) => r.host != ""`)),
					}),
				},
			},
		},
		{
			Name:  `r._value == ""`,
			Rules: []plan.Rule{influxdb.PushDownFilterRule{}},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadRange", &influxdb.ReadRangePhysSpec{
						Bounds: bounds,
					}),
					plan.CreatePhysicalNode("filter", &universe.FilterProcedureSpec{
						Fn: makeResolvedFilterFn(executetest.FunctionExpression(t, `(r) => r._value == ""`)),
					}),
				},
				Edges: [][2]int{
					{0, 1},
				},
			},
			After: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("merged_ReadRange_filter", &influxdb.ReadRangePhysSpec{
						Bounds:    bounds,
						Predicate: toStoragePredicate(executetest.FunctionExpression(t, `(r) => r._value == ""`)),
					}),
				},
			},
		},
		{
			// TODO(jsternberg): This one should be rewritten, but is not currently.
			Name:  `not r.host == "server01"`,
			Rules: []plan.Rule{influxdb.PushDownFilterRule{}},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadRange", &influxdb.ReadRangePhysSpec{
						Bounds: bounds,
					}),
					plan.CreatePhysicalNode("filter", &universe.FilterProcedureSpec{
						Fn: makeResolvedFilterFn(executetest.FunctionExpression(t, `(r) => not r.host == "server01"`)),
					}),
				},
				Edges: [][2]int{
					{0, 1},
				},
			},
			NoChange: true,
		},
		{
			Name:  `r._measurement == "cpu" and exists r.host`,
			Rules: []plan.Rule{influxdb.PushDownFilterRule{}},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadRange", &influxdb.ReadRangePhysSpec{
						Bounds: bounds,
					}),
					plan.CreatePhysicalNode("filter", &universe.FilterProcedureSpec{
						Fn: makeResolvedFilterFn(executetest.FunctionExpression(t, `(r) => r.host == "cpu" and exists r.host`)),
					}),
				},
				Edges: [][2]int{
					{0, 1},
				},
			},
			After: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("merged_ReadRange_filter", &influxdb.ReadRangePhysSpec{
						Bounds:    bounds,
						Predicate: toStoragePredicate(executetest.FunctionExpression(t, `(r) => r.host == "cpu" and r.host != ""`)),
					}),
				},
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			plantest.PhysicalRuleTestHelper(t, &tc)
		})
	}
}

func TestPushDownGroupRule(t *testing.T) {
	readRange := influxdb.ReadRangePhysSpec{
		Bucket: "my-bucket",
		Bounds: flux.Bounds{
			Start: fluxTime(5),
			Stop:  fluxTime(10),
		},
	}

	tests := []plantest.RuleTestCase{
		{
			Name: "simple",
			// ReadRange -> group => ReadGroup
			Rules: []plan.Rule{
				influxdb.PushDownGroupRule{},
			},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreateLogicalNode("ReadRange", &readRange),
					plan.CreateLogicalNode("group", &universe.GroupProcedureSpec{
						GroupMode: flux.GroupModeBy,
						GroupKeys: []string{"_measurement", "tag0", "tag1"},
					}),
				},
				Edges: [][2]int{{0, 1}},
			},
			After: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadGroup", &influxdb.ReadGroupPhysSpec{
						ReadRangePhysSpec: readRange,
						GroupMode:         flux.GroupModeBy,
						GroupKeys:         []string{"_measurement", "tag0", "tag1"},
					}),
				},
			},
		},
		{
			Name: "with successor",
			// ReadRange -> group -> count  =>  ReadGroup -> count
			Rules: []plan.Rule{
				influxdb.PushDownGroupRule{},
			},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreateLogicalNode("ReadRange", &readRange),
					plan.CreateLogicalNode("group", &universe.GroupProcedureSpec{
						GroupMode: flux.GroupModeBy,
						GroupKeys: []string{"_measurement", "tag0", "tag1"},
					}),
					plan.CreatePhysicalNode("count", &universe.CountProcedureSpec{}),
				},
				Edges: [][2]int{
					{0, 1},
					{1, 2},
				},
			},
			After: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadGroup", &influxdb.ReadGroupPhysSpec{
						ReadRangePhysSpec: readRange,
						GroupMode:         flux.GroupModeBy,
						GroupKeys:         []string{"_measurement", "tag0", "tag1"},
					}),
					plan.CreatePhysicalNode("count", &universe.CountProcedureSpec{}),
				},
				Edges: [][2]int{{0, 1}},
			},
		},
		{
			Name: "with multiple successors",
			//
			// group    count       group    count
			//     \    /       =>      \    /
			//    ReadRange            ReadRange
			//
			Rules: []plan.Rule{
				influxdb.PushDownGroupRule{},
			},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreateLogicalNode("ReadRange", &readRange),
					plan.CreateLogicalNode("group", &universe.GroupProcedureSpec{
						GroupMode: flux.GroupModeBy,
						GroupKeys: []string{"_measurement", "tag0", "tag1"},
					}),
					plan.CreatePhysicalNode("count", &universe.CountProcedureSpec{}),
				},
				Edges: [][2]int{
					{0, 1},
					{0, 2},
				},
			},
			NoChange: true,
		},
		{
			Name: "un-group",
			// ReadRange -> group() => ReadGroup
			Rules: []plan.Rule{
				influxdb.PushDownGroupRule{},
			},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreateLogicalNode("ReadRange", &readRange),
					plan.CreateLogicalNode("group", &universe.GroupProcedureSpec{
						GroupMode: flux.GroupModeBy,
						GroupKeys: []string{},
					}),
				},
				Edges: [][2]int{
					{0, 1},
				},
			},
			After: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadGroup", &influxdb.ReadGroupPhysSpec{
						ReadRangePhysSpec: readRange,
						GroupMode:         flux.GroupModeBy,
						GroupKeys:         []string{},
					}),
				},
			},
		},
		{
			Name: "group except",
			// ReadRange -> group(mode: "except") => ReadRange -> group(mode: "except")
			Rules: []plan.Rule{
				influxdb.PushDownGroupRule{},
			},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreateLogicalNode("ReadRange", &readRange),
					plan.CreateLogicalNode("group", &universe.GroupProcedureSpec{
						GroupMode: flux.GroupModeExcept,
						GroupKeys: []string{"_measurement", "tag0", "tag1"},
					}),
				},
				Edges: [][2]int{
					{0, 1},
				},
			},
			NoChange: true,
		},
		{
			Name: "group none",
			Rules: []plan.Rule{
				influxdb.PushDownGroupRule{},
			},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreateLogicalNode("ReadRange", &readRange),
					plan.CreateLogicalNode("group", &universe.GroupProcedureSpec{
						GroupMode: flux.GroupModeNone,
						GroupKeys: []string{},
					}),
				},
				Edges: [][2]int{
					{0, 1},
				},
			},
			NoChange: true,
		},
		{
			Name: "cannot push down",
			// ReadRange -> count -> group => ReadRange -> count -> group
			Rules: []plan.Rule{
				influxdb.PushDownGroupRule{},
			},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreateLogicalNode("ReadRange", &readRange),
					plan.CreatePhysicalNode("count", &universe.CountProcedureSpec{}),
					plan.CreateLogicalNode("group", &universe.GroupProcedureSpec{
						GroupMode: flux.GroupModeBy,
						GroupKeys: []string{"_measurement", "tag0", "tag1"},
					}),
				},
				Edges: [][2]int{
					{0, 1},
					{1, 2},
				},
			},
			NoChange: true,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			plantest.PhysicalRuleTestHelper(t, &tc)
		})
	}
}

func TestReadTagKeysRule(t *testing.T) {
	fromSpec := influxdb.FromStorageProcedureSpec{
		Bucket: influxdb.NameOrID{Name: "my-bucket"},
	}
	rangeSpec := universe.RangeProcedureSpec{
		Bounds: flux.Bounds{
			Start: fluxTime(5),
			Stop:  fluxTime(10),
		},
	}
	filterSpec := universe.FilterProcedureSpec{
		Fn: interpreter.ResolvedFunction{
			Scope: nil,
			Fn: &semantic.FunctionExpression{
				Parameters: &semantic.FunctionParameters{
					List: []*semantic.FunctionParameter{{
						Key: &semantic.Identifier{
							Name: "r",
						},
					}},
				},
				Block: &semantic.Block{
					Body: []semantic.Statement{
						&semantic.ReturnStatement{
							Argument: &semantic.BinaryExpression{
								Operator: ast.EqualOperator,
								Left: &semantic.MemberExpression{
									Object: &semantic.IdentifierExpression{
										Name: "r",
									},
									Property: "_measurement",
								},
								Right: &semantic.StringLiteral{
									Value: "cpu",
								},
							},
						},
					},
				},
			},
		},
	}
	keysSpec := universe.KeysProcedureSpec{
		Column: execute.DefaultValueColLabel,
	}
	keepSpec := universe.SchemaMutationProcedureSpec{
		Mutations: []universe.SchemaMutation{
			&universe.KeepOpSpec{
				Columns: []string{
					execute.DefaultValueColLabel,
				},
			},
		},
	}
	distinctSpec := universe.DistinctProcedureSpec{
		Column: execute.DefaultValueColLabel,
	}
	readTagKeysSpec := func(filter bool) plan.PhysicalProcedureSpec {
		s := influxdb.ReadTagKeysPhysSpec{
			ReadRangePhysSpec: influxdb.ReadRangePhysSpec{
				Bucket: "my-bucket",
				Bounds: flux.Bounds{
					Start: fluxTime(5),
					Stop:  fluxTime(10),
				},
			},
		}
		if filter {
			bodyExpr, _ := filterSpec.Fn.Fn.GetFunctionBodyExpression()
			s.Predicate, _ = influxdb.ToStoragePredicate(bodyExpr, "r")
		}
		return &s
	}

	tests := []plantest.RuleTestCase{
		{
			Name: "simple",
			// from -> range -> keys -> keep -> distinct  =>  ReadTagKeys
			Rules: []plan.Rule{
				influxdb.PushDownRangeRule{},
				influxdb.PushDownReadTagKeysRule{},
			},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreateLogicalNode("from", &fromSpec),
					plan.CreateLogicalNode("range", &rangeSpec),
					plan.CreateLogicalNode("keys", &keysSpec),
					plan.CreateLogicalNode("keep", &keepSpec),
					plan.CreateLogicalNode("distinct", &distinctSpec),
				},
				Edges: [][2]int{
					{0, 1},
					{1, 2},
					{2, 3},
					{3, 4},
				},
			},
			After: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadTagKeys", readTagKeysSpec(false)),
				},
			},
		},
		{
			Name: "with filter",
			// from -> range -> filter -> keys -> keep -> distinct  =>  ReadTagKeys
			Rules: []plan.Rule{
				influxdb.PushDownRangeRule{},
				influxdb.PushDownFilterRule{},
				influxdb.PushDownReadTagKeysRule{},
			},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreateLogicalNode("from", &fromSpec),
					plan.CreateLogicalNode("range", &rangeSpec),
					plan.CreateLogicalNode("filter", &filterSpec),
					plan.CreateLogicalNode("keys", &keysSpec),
					plan.CreateLogicalNode("keep", &keepSpec),
					plan.CreateLogicalNode("distinct", &distinctSpec),
				},
				Edges: [][2]int{
					{0, 1},
					{1, 2},
					{2, 3},
					{3, 4},
					{4, 5},
				},
			},
			After: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadTagKeys", readTagKeysSpec(true)),
				},
			},
		},
		{
			Name: "with successor",
			// from -> range -> keys -> keep -> distinct -> count  =>  ReadTagKeys -> count
			Rules: []plan.Rule{
				influxdb.PushDownRangeRule{},
				influxdb.PushDownReadTagKeysRule{},
			},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreateLogicalNode("from", &fromSpec),
					plan.CreateLogicalNode("range", &rangeSpec),
					plan.CreateLogicalNode("keys", &keysSpec),
					plan.CreateLogicalNode("keep", &keepSpec),
					plan.CreateLogicalNode("distinct", &distinctSpec),
					plan.CreatePhysicalNode("count", &universe.CountProcedureSpec{}),
				},
				Edges: [][2]int{
					{0, 1},
					{1, 2},
					{2, 3},
					{3, 4},
					{4, 5},
				},
			},
			After: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadTagKeys", readTagKeysSpec(false)),
					plan.CreatePhysicalNode("count", &universe.CountProcedureSpec{}),
				},
				Edges: [][2]int{{0, 1}},
			},
		},
		{
			Name: "with multiple successors",
			// count      mean
			//     \     /          count     mean
			//      range       =>      \    /
			//        |               ReadTagKeys
			//       from
			Rules: []plan.Rule{
				influxdb.PushDownRangeRule{},
				influxdb.PushDownReadTagKeysRule{},
			},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreateLogicalNode("from", &fromSpec),
					plan.CreateLogicalNode("range", &rangeSpec),
					plan.CreateLogicalNode("keys", &keysSpec),
					plan.CreateLogicalNode("keep", &keepSpec),
					plan.CreateLogicalNode("distinct", &distinctSpec),
					plan.CreatePhysicalNode("count", &universe.CountProcedureSpec{}),
					plan.CreatePhysicalNode("mean", &universe.MeanProcedureSpec{}),
				},
				Edges: [][2]int{
					{0, 1},
					{1, 2},
					{2, 3},
					{3, 4},
					{4, 5},
					{4, 6},
				},
			},
			After: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadTagKeys", readTagKeysSpec(false)),
					plan.CreatePhysicalNode("count", &universe.CountProcedureSpec{}),
					plan.CreatePhysicalNode("mean", &universe.MeanProcedureSpec{}),
				},
				Edges: [][2]int{
					{0, 1},
					{0, 2},
				},
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			plantest.PhysicalRuleTestHelper(t, &tc)
		})
	}
}

func TestReadTagValuesRule(t *testing.T) {
	fromSpec := influxdb.FromStorageProcedureSpec{
		Bucket: influxdb.NameOrID{Name: "my-bucket"},
	}
	rangeSpec := universe.RangeProcedureSpec{
		Bounds: flux.Bounds{
			Start: fluxTime(5),
			Stop:  fluxTime(10),
		},
	}
	filterSpec := universe.FilterProcedureSpec{
		Fn: interpreter.ResolvedFunction{
			Scope: nil,
			Fn: &semantic.FunctionExpression{
				Parameters: &semantic.FunctionParameters{
					List: []*semantic.FunctionParameter{{
						Key: &semantic.Identifier{
							Name: "r",
						},
					}},
				},
				Block: &semantic.Block{
					Body: []semantic.Statement{
						&semantic.ReturnStatement{
							Argument: &semantic.BinaryExpression{
								Operator: ast.EqualOperator,
								Left: &semantic.MemberExpression{
									Object: &semantic.IdentifierExpression{
										Name: "r",
									},
									Property: "_measurement",
								},
								Right: &semantic.StringLiteral{
									Value: "cpu",
								},
							},
						},
					},
				},
			},
		},
	}
	keepSpec := universe.SchemaMutationProcedureSpec{
		Mutations: []universe.SchemaMutation{
			&universe.KeepOpSpec{
				Columns: []string{
					"host",
				},
			},
		},
	}
	groupSpec := universe.GroupProcedureSpec{
		GroupMode: flux.GroupModeBy,
		GroupKeys: []string{},
	}
	distinctSpec := universe.DistinctProcedureSpec{
		Column: "host",
	}
	readTagValuesSpec := func(filter bool) plan.PhysicalProcedureSpec {
		s := influxdb.ReadTagValuesPhysSpec{
			ReadRangePhysSpec: influxdb.ReadRangePhysSpec{
				Bucket: "my-bucket",
				Bounds: flux.Bounds{
					Start: fluxTime(5),
					Stop:  fluxTime(10),
				},
			},
			TagKey: "host",
		}
		if filter {
			bodyExpr, _ := filterSpec.Fn.Fn.GetFunctionBodyExpression()
			s.Predicate, _ = influxdb.ToStoragePredicate(bodyExpr, "r")
		}
		return &s
	}

	tests := []plantest.RuleTestCase{
		{
			Name: "simple",
			// from -> range -> keep -> group -> distinct  =>  ReadTagValues
			Rules: []plan.Rule{
				influxdb.PushDownRangeRule{},
				influxdb.PushDownReadTagValuesRule{},
			},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreateLogicalNode("from", &fromSpec),
					plan.CreateLogicalNode("range", &rangeSpec),
					plan.CreateLogicalNode("keep", &keepSpec),
					plan.CreateLogicalNode("group", &groupSpec),
					plan.CreateLogicalNode("distinct", &distinctSpec),
				},
				Edges: [][2]int{
					{0, 1},
					{1, 2},
					{2, 3},
					{3, 4},
				},
			},
			After: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadTagValues", readTagValuesSpec(false)),
				},
			},
		},
		{
			Name: "with filter",
			// from -> range -> filter -> keep -> group -> distinct  =>  ReadTagValues
			Rules: []plan.Rule{
				influxdb.PushDownRangeRule{},
				influxdb.PushDownFilterRule{},
				influxdb.PushDownReadTagValuesRule{},
			},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreateLogicalNode("from", &fromSpec),
					plan.CreateLogicalNode("range", &rangeSpec),
					plan.CreateLogicalNode("filter", &filterSpec),
					plan.CreateLogicalNode("keep", &keepSpec),
					plan.CreateLogicalNode("group", &groupSpec),
					plan.CreateLogicalNode("distinct", &distinctSpec),
				},
				Edges: [][2]int{
					{0, 1},
					{1, 2},
					{2, 3},
					{3, 4},
					{4, 5},
				},
			},
			After: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadTagValues", readTagValuesSpec(true)),
				},
			},
		},
		{
			Name: "with successor",
			// from -> range -> keep -> group -> distinct -> count  =>  ReadTagValues -> count
			Rules: []plan.Rule{
				influxdb.PushDownRangeRule{},
				influxdb.PushDownReadTagValuesRule{},
			},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreateLogicalNode("from", &fromSpec),
					plan.CreateLogicalNode("range", &rangeSpec),
					plan.CreateLogicalNode("keep", &keepSpec),
					plan.CreateLogicalNode("group", &groupSpec),
					plan.CreateLogicalNode("distinct", &distinctSpec),
					plan.CreatePhysicalNode("count", &universe.CountProcedureSpec{}),
				},
				Edges: [][2]int{
					{0, 1},
					{1, 2},
					{2, 3},
					{3, 4},
					{4, 5},
				},
			},
			After: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadTagValues", readTagValuesSpec(false)),
					plan.CreatePhysicalNode("count", &universe.CountProcedureSpec{}),
				},
				Edges: [][2]int{{0, 1}},
			},
		},
		{
			Name: "with multiple successors",
			// count      mean
			//     \     /          count     mean
			//      range       =>      \    /
			//        |               ReadTagValues
			//       from
			Rules: []plan.Rule{
				influxdb.PushDownRangeRule{},
				influxdb.PushDownReadTagValuesRule{},
			},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreateLogicalNode("from", &fromSpec),
					plan.CreateLogicalNode("range", &rangeSpec),
					plan.CreateLogicalNode("keep", &keepSpec),
					plan.CreateLogicalNode("group", &groupSpec),
					plan.CreateLogicalNode("distinct", &distinctSpec),
					plan.CreatePhysicalNode("count", &universe.CountProcedureSpec{}),
					plan.CreatePhysicalNode("mean", &universe.MeanProcedureSpec{}),
				},
				Edges: [][2]int{
					{0, 1},
					{1, 2},
					{2, 3},
					{3, 4},
					{4, 5},
					{4, 6},
				},
			},
			After: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadTagValues", readTagValuesSpec(false)),
					plan.CreatePhysicalNode("count", &universe.CountProcedureSpec{}),
					plan.CreatePhysicalNode("mean", &universe.MeanProcedureSpec{}),
				},
				Edges: [][2]int{
					{0, 1},
					{0, 2},
				},
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			plantest.PhysicalRuleTestHelper(t, &tc)
		})
	}
}

func minProcedureSpec() *universe.MinProcedureSpec {
	return &universe.MinProcedureSpec{
		SelectorConfig: execute.SelectorConfig{Column: execute.DefaultValueColLabel},
	}
}
func maxProcedureSpec() *universe.MaxProcedureSpec {
	return &universe.MaxProcedureSpec{
		SelectorConfig: execute.SelectorConfig{Column: execute.DefaultValueColLabel},
	}
}
func countProcedureSpec() *universe.CountProcedureSpec {
	return &universe.CountProcedureSpec{
		AggregateConfig: execute.AggregateConfig{Columns: []string{execute.DefaultValueColLabel}},
	}
}
func sumProcedureSpec() *universe.SumProcedureSpec {
	return &universe.SumProcedureSpec{
		AggregateConfig: execute.AggregateConfig{Columns: []string{execute.DefaultValueColLabel}},
	}
}
func firstProcedureSpec() *universe.FirstProcedureSpec {
	return &universe.FirstProcedureSpec{
		SelectorConfig: execute.SelectorConfig{Column: execute.DefaultValueColLabel},
	}
}
func lastProcedureSpec() *universe.LastProcedureSpec {
	return &universe.LastProcedureSpec{
		SelectorConfig: execute.SelectorConfig{Column: execute.DefaultValueColLabel},
	}
}
func meanProcedureSpec() *universe.MeanProcedureSpec {
	return &universe.MeanProcedureSpec{
		AggregateConfig: execute.AggregateConfig{Columns: []string{execute.DefaultValueColLabel}},
	}
}

//
// Window Aggregate Testing
//
func TestPushDownWindowAggregateRule(t *testing.T) {
	readRange := influxdb.ReadRangePhysSpec{
		Bucket: "my-bucket",
		Bounds: flux.Bounds{
			Start: fluxTime(5),
			Stop:  fluxTime(10),
		},
	}

	dur1m := values.ConvertDurationNsecs(60 * time.Second)
	dur2m := values.ConvertDurationNsecs(120 * time.Second)
	dur0 := values.ConvertDurationNsecs(0)
	durNeg, _ := values.ParseDuration("-60s")
	dur1mo, _ := values.ParseDuration("1mo")
	dur1y, _ := values.ParseDuration("1y")
	durInf := values.ConvertDurationNsecs(math.MaxInt64)
	durMixed, _ := values.ParseDuration("1mo5m")

	window := func(dur values.Duration) universe.WindowProcedureSpec {
		return universe.WindowProcedureSpec{
			Window: plan.WindowSpec{
				Every:  dur,
				Period: dur,
				Offset: dur0,
			},
			TimeColumn:  "_time",
			StartColumn: "_start",
			StopColumn:  "_stop",
			CreateEmpty: false,
		}
	}

	window1m := window(dur1m)
	window2m := window(dur2m)
	windowNeg := window(durNeg)
	window1y := window(dur1y)
	window1mo := window(dur1mo)
	windowInf := window(durInf)
	windowInfCreateEmpty := windowInf
	windowInfCreateEmpty.CreateEmpty = true

	tests := make([]plantest.RuleTestCase, 0)

	// construct a simple plan with a specific window and aggregate function
	simplePlanWithWindowAgg := func(window universe.WindowProcedureSpec, agg plan.NodeID, spec plan.ProcedureSpec) *plantest.PlanSpec {
		return &plantest.PlanSpec{
			Nodes: []plan.Node{
				plan.CreateLogicalNode("ReadRange", &readRange),
				plan.CreateLogicalNode("window", &window),
				plan.CreateLogicalNode(agg, spec),
			},
			Edges: [][2]int{
				{0, 1},
				{1, 2},
			},
		}
	}

	// construct a simple result
	simpleResult := func(proc plan.ProcedureKind, createEmpty bool, successors ...plan.Node) *plantest.PlanSpec {
		spec := &plantest.PlanSpec{
			Nodes: []plan.Node{
				plan.CreatePhysicalNode("ReadWindowAggregate", &influxdb.ReadWindowAggregatePhysSpec{
					ReadRangePhysSpec: readRange,
					Aggregates:        []plan.ProcedureKind{proc},
					WindowEvery:       flux.ConvertDuration(60000000000 * time.Nanosecond),
					CreateEmpty:       createEmpty,
				}),
			},
		}
		for i, successor := range successors {
			spec.Nodes = append(spec.Nodes, successor)
			spec.Edges = append(spec.Edges, [2]int{i, i + 1})
		}
		return spec
	}

	// ReadRange -> window -> min => ReadWindowAggregate
	tests = append(tests, plantest.RuleTestCase{
		Context: context.Background(),
		Name:    "SimplePassMin",
		Rules:   []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
		Before:  simplePlanWithWindowAgg(window1m, universe.MinKind, minProcedureSpec()),
		After:   simpleResult(universe.MinKind, false),
	})

	// ReadRange -> window -> max => ReadWindowAggregate
	tests = append(tests, plantest.RuleTestCase{
		Context: context.Background(),
		Name:    "SimplePassMax",
		Rules:   []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
		Before:  simplePlanWithWindowAgg(window1m, universe.MaxKind, maxProcedureSpec()),
		After:   simpleResult(universe.MaxKind, false),
	})

	// ReadRange -> window -> mean => ReadWindowAggregate
	tests = append(tests, plantest.RuleTestCase{
		Context: context.Background(),
		Name:    "SimplePassMean",
		Rules:   []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
		Before:  simplePlanWithWindowAgg(window1m, universe.MeanKind, meanProcedureSpec()),
		After:   simpleResult(universe.MeanKind, false),
	})

	// ReadRange -> window -> count => ReadWindowAggregate
	tests = append(tests, plantest.RuleTestCase{
		Context: context.Background(),
		Name:    "SimplePassCount",
		Rules:   []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
		Before:  simplePlanWithWindowAgg(window1m, universe.CountKind, countProcedureSpec()),
		After:   simpleResult(universe.CountKind, false),
	})

	// ReadRange -> window -> sum => ReadWindowAggregate
	tests = append(tests, plantest.RuleTestCase{
		Context: context.Background(),
		Name:    "SimplePassSum",
		Rules:   []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
		Before:  simplePlanWithWindowAgg(window1m, universe.SumKind, sumProcedureSpec()),
		After:   simpleResult(universe.SumKind, false),
	})

	// ReadRange -> window -> first => ReadWindowAggregate
	tests = append(tests, plantest.RuleTestCase{
		Context: context.Background(),
		Name:    "SimplePassFirst",
		Rules:   []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
		Before:  simplePlanWithWindowAgg(window1m, universe.FirstKind, firstProcedureSpec()),
		After:   simpleResult(universe.FirstKind, false),
	})

	// ReadRange -> window -> last => ReadWindowAggregate
	tests = append(tests, plantest.RuleTestCase{
		Context: context.Background(),
		Name:    "SimplePassLast",
		Rules:   []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
		Before:  simplePlanWithWindowAgg(window1m, universe.LastKind, lastProcedureSpec()),
		After:   simpleResult(universe.LastKind, false),
	})

	// Rewrite with successors
	// ReadRange -> window -> min -> count {2} => ReadWindowAggregate -> count {2}
	tests = append(tests, plantest.RuleTestCase{
		Context: context.Background(),
		Name:    "WithSuccessor",
		Rules:   []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
		Before: &plantest.PlanSpec{
			Nodes: []plan.Node{
				plan.CreateLogicalNode("ReadRange", &readRange),
				plan.CreateLogicalNode("window", &window1m),
				plan.CreateLogicalNode("min", minProcedureSpec()),
				plan.CreateLogicalNode("count", countProcedureSpec()),
				plan.CreateLogicalNode("count", countProcedureSpec()),
			},
			Edges: [][2]int{
				{0, 1},
				{1, 2},
				{2, 3},
				{2, 4},
			},
		},
		After: &plantest.PlanSpec{
			Nodes: []plan.Node{
				plan.CreatePhysicalNode("ReadWindowAggregate", &influxdb.ReadWindowAggregatePhysSpec{
					ReadRangePhysSpec: readRange,
					Aggregates:        []plan.ProcedureKind{"min"},
					WindowEvery:       flux.ConvertDuration(60000000000 * time.Nanosecond),
				}),
				plan.CreateLogicalNode("count", countProcedureSpec()),
				plan.CreateLogicalNode("count", countProcedureSpec()),
			},
			Edges: [][2]int{
				{0, 1},
				{0, 2},
			},
		},
	})

	// ReadRange -> window(offset: ...) -> last => ReadWindowAggregate
	tests = append(tests, plantest.RuleTestCase{
		Context: context.Background(),
		Name:    "WindowPositiveOffset",
		Rules:   []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
		Before: simplePlanWithWindowAgg(universe.WindowProcedureSpec{
			Window: plan.WindowSpec{
				Every:  dur2m,
				Period: dur2m,
				Offset: dur1m,
			},
			TimeColumn:  "_time",
			StartColumn: "_start",
			StopColumn:  "_stop",
		}, universe.LastKind, lastProcedureSpec()),
		After: &plantest.PlanSpec{
			Nodes: []plan.Node{
				plan.CreatePhysicalNode("ReadWindowAggregate", &influxdb.ReadWindowAggregatePhysSpec{
					ReadRangePhysSpec: readRange,
					Aggregates:        []plan.ProcedureKind{universe.LastKind},
					WindowEvery:       flux.ConvertDuration(120000000000 * time.Nanosecond),
					Offset:            flux.ConvertDuration(60000000000 * time.Nanosecond),
				}),
			},
		},
	})

	// ReadRange -> window(every: 1mo) -> last => ReadWindowAggregate
	tests = append(tests, plantest.RuleTestCase{
		Context: context.Background(),
		Name:    "WindowByMonth",
		Rules:   []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
		Before:  simplePlanWithWindowAgg(window1mo, universe.LastKind, lastProcedureSpec()),
		After: &plantest.PlanSpec{
			Nodes: []plan.Node{
				plan.CreatePhysicalNode("ReadWindowAggregate", &influxdb.ReadWindowAggregatePhysSpec{
					ReadRangePhysSpec: readRange,
					Aggregates:        []plan.ProcedureKind{universe.LastKind},
					WindowEvery:       dur1mo,
				}),
			},
		},
	})

	// ReadRange -> window(every: 1y) -> last => ReadWindowAggregate
	tests = append(tests, plantest.RuleTestCase{
		Context: context.Background(),
		Name:    "WindowByYear",
		Rules:   []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
		Before:  simplePlanWithWindowAgg(window1y, universe.LastKind, lastProcedureSpec()),
		After: &plantest.PlanSpec{
			Nodes: []plan.Node{
				plan.CreatePhysicalNode("ReadWindowAggregate", &influxdb.ReadWindowAggregatePhysSpec{
					ReadRangePhysSpec: readRange,
					Aggregates:        []plan.ProcedureKind{universe.LastKind},
					WindowEvery:       dur1y,
				}),
			},
		},
	})

	// ReadRange -> window(every: 1y, offset: 1mo) -> last => ReadWindowAggregate
	tests = append(tests, plantest.RuleTestCase{
		Context: context.Background(),
		Name:    "WindowMonthlyOffset",
		Rules:   []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
		Before: simplePlanWithWindowAgg(func() universe.WindowProcedureSpec {
			spec := window1y
			spec.Window.Offset = dur1mo
			return spec
		}(), universe.LastKind, lastProcedureSpec()),
		After: &plantest.PlanSpec{
			Nodes: []plan.Node{
				plan.CreatePhysicalNode("ReadWindowAggregate", &influxdb.ReadWindowAggregatePhysSpec{
					ReadRangePhysSpec: readRange,
					Aggregates:        []plan.ProcedureKind{universe.LastKind},
					WindowEvery:       dur1y,
					Offset:            dur1mo,
				}),
			},
		},
	})

	// ReadRange -> window(every: 1y, offset: 1mo5m) -> last => ReadWindowAggregate
	tests = append(tests, plantest.RuleTestCase{
		Context: context.Background(),
		Name:    "WindowMixedOffset",
		Rules:   []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
		Before: simplePlanWithWindowAgg(func() universe.WindowProcedureSpec {
			spec := window1y
			spec.Window.Offset = durMixed
			return spec
		}(), universe.LastKind, lastProcedureSpec()),
		After: &plantest.PlanSpec{
			Nodes: []plan.Node{
				plan.CreatePhysicalNode("ReadWindowAggregate", &influxdb.ReadWindowAggregatePhysSpec{
					ReadRangePhysSpec: readRange,
					Aggregates:        []plan.ProcedureKind{universe.LastKind},
					WindowEvery:       dur1y,
					Offset:            durMixed,
				}),
			},
		},
	})

	// Helper that adds a test with a simple plan that does not pass due to a
	// specified bad window
	simpleMinUnchanged := func(name string, window universe.WindowProcedureSpec) {
		// Note: NoChange is not working correctly for these tests. It is
		// expecting empty time, start, and stop column fields.
		tests = append(tests, plantest.RuleTestCase{
			Name:     name,
			Context:  context.Background(),
			Rules:    []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
			Before:   simplePlanWithWindowAgg(window, "min", countProcedureSpec()),
			NoChange: true,
		})
	}

	// Condition not met: period not equal to every
	badWindow1 := window1m
	badWindow1.Window.Period = dur2m
	simpleMinUnchanged("BadPeriod", badWindow1)

	// Condition not met: negative offset
	badWindow2 := window1m
	badWindow2.Window.Offset = durNeg
	simpleMinUnchanged("NegOffset", badWindow2)

	// Condition not met: non-standard _time column
	badWindow3 := window1m
	badWindow3.TimeColumn = "_timmy"
	simpleMinUnchanged("BadTime", badWindow3)

	// Condition not met: non-standard start column
	badWindow4 := window1m
	badWindow4.StartColumn = "_stooort"
	simpleMinUnchanged("BadStart", badWindow4)

	// Condition not met: non-standard stop column
	badWindow5 := window1m
	badWindow5.StopColumn = "_stappp"
	simpleMinUnchanged("BadStop", badWindow5)

	// Condition met: createEmpty is true.
	windowCreateEmpty1m := window1m
	windowCreateEmpty1m.CreateEmpty = true
	tests = append(tests, plantest.RuleTestCase{
		Context: context.Background(),
		Name:    "CreateEmptyPassMin",
		Rules:   []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
		Before:  simplePlanWithWindowAgg(windowCreateEmpty1m, "min", minProcedureSpec()),
		After:   simpleResult("min", true),
	})

	// Condition not met: neg duration.
	simpleMinUnchanged("WindowNeg", windowNeg)

	// Bad min column
	// ReadRange -> window -> min => NO-CHANGE
	tests = append(tests, plantest.RuleTestCase{
		Name:    "BadMinCol",
		Context: context.Background(),
		Rules:   []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
		Before: simplePlanWithWindowAgg(window1m, "min", &universe.MinProcedureSpec{
			SelectorConfig: execute.SelectorConfig{Column: "_valmoo"},
		}),
		NoChange: true,
	})

	// Bad max column
	// ReadRange -> window -> max => NO-CHANGE
	tests = append(tests, plantest.RuleTestCase{
		Name:    "BadMaxCol",
		Context: context.Background(),
		Rules:   []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
		Before: simplePlanWithWindowAgg(window1m, "max", &universe.MaxProcedureSpec{
			SelectorConfig: execute.SelectorConfig{Column: "_valmoo"},
		}),
		NoChange: true,
	})

	// Bad mean columns
	// ReadRange -> window -> mean => NO-CHANGE
	tests = append(tests, plantest.RuleTestCase{
		Name:    "BadMeanCol1",
		Context: context.Background(),
		Rules:   []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
		Before: simplePlanWithWindowAgg(window1m, "mean", &universe.MeanProcedureSpec{
			AggregateConfig: execute.AggregateConfig{Columns: []string{"_valmoo"}},
		}),
		NoChange: true,
	})
	tests = append(tests, plantest.RuleTestCase{
		Name:    "BadMeanCol2",
		Context: context.Background(),
		Rules:   []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
		Before: simplePlanWithWindowAgg(window1m, "mean", &universe.MeanProcedureSpec{
			AggregateConfig: execute.AggregateConfig{Columns: []string{"_value", "_valmoo"}},
		}),
		NoChange: true,
	})

	// No match due to a collapsed node having a successor
	// ReadRange -> window -> min
	//                    \-> min
	tests = append(tests, plantest.RuleTestCase{
		Name:    "CollapsedWithSuccessor1",
		Context: context.Background(),
		Rules:   []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
		Before: &plantest.PlanSpec{
			Nodes: []plan.Node{
				plan.CreateLogicalNode("ReadRange", &readRange),
				plan.CreateLogicalNode("window", &window1m),
				plan.CreateLogicalNode("min", minProcedureSpec()),
				plan.CreateLogicalNode("min", minProcedureSpec()),
			},
			Edges: [][2]int{
				{0, 1},
				{1, 2},
				{1, 3},
			},
		},
		NoChange: true,
	})

	// No match due to a collapsed node having a successor
	// ReadRange -> window -> min
	//          \-> window
	tests = append(tests, plantest.RuleTestCase{
		Name:    "CollapsedWithSuccessor2",
		Context: context.Background(),
		Rules:   []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
		Before: &plantest.PlanSpec{
			Nodes: []plan.Node{
				plan.CreateLogicalNode("ReadRange", &readRange),
				plan.CreateLogicalNode("window", &window1m),
				plan.CreateLogicalNode("min", minProcedureSpec()),
				plan.CreateLogicalNode("window", &window2m),
			},
			Edges: [][2]int{
				{0, 1},
				{1, 2},
				{0, 3},
			},
		},
		NoChange: true,
	})

	// No pattern match
	// ReadRange -> filter -> window -> min -> NO-CHANGE
	pushableFn1 := executetest.FunctionExpression(t, `(r) => true`)

	makeResolvedFilterFn := func(expr *semantic.FunctionExpression) interpreter.ResolvedFunction {
		return interpreter.ResolvedFunction{
			Scope: nil,
			Fn:    expr,
		}
	}
	noPatternMatch1 := func() *plantest.PlanSpec {
		return &plantest.PlanSpec{
			Nodes: []plan.Node{
				plan.CreateLogicalNode("ReadRange", &readRange),
				plan.CreatePhysicalNode("filter", &universe.FilterProcedureSpec{
					Fn: makeResolvedFilterFn(pushableFn1),
				}),
				plan.CreateLogicalNode("window", &window1m),
				plan.CreateLogicalNode("min", minProcedureSpec()),
			},
			Edges: [][2]int{
				{0, 1},
				{1, 2},
				{2, 3},
			},
		}
	}
	tests = append(tests, plantest.RuleTestCase{
		Name:     "NoPatternMatch1",
		Context:  context.Background(),
		Rules:    []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
		Before:   noPatternMatch1(),
		NoChange: true,
	})

	// No pattern match 2
	// ReadRange -> window -> filter -> min -> NO-CHANGE
	noPatternMatch2 := func() *plantest.PlanSpec {
		return &plantest.PlanSpec{
			Nodes: []plan.Node{
				plan.CreateLogicalNode("ReadRange", &readRange),
				plan.CreateLogicalNode("window", &window1m),
				plan.CreatePhysicalNode("filter", &universe.FilterProcedureSpec{
					Fn: makeResolvedFilterFn(pushableFn1),
				}),
				plan.CreateLogicalNode("min", minProcedureSpec()),
			},
			Edges: [][2]int{
				{0, 1},
				{1, 2},
				{2, 3},
			},
		}
	}
	tests = append(tests, plantest.RuleTestCase{
		Name:     "NoPatternMatch2",
		Context:  context.Background(),
		Rules:    []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
		Before:   noPatternMatch2(),
		NoChange: true,
	})

	duplicate := func(column, as string) *universe.SchemaMutationProcedureSpec {
		return &universe.SchemaMutationProcedureSpec{
			Mutations: []universe.SchemaMutation{
				&universe.DuplicateOpSpec{
					Column: column,
					As:     as,
				},
			},
		}
	}

	aggregateWindowPlan := func(window universe.WindowProcedureSpec, agg plan.NodeID, spec plan.ProcedureSpec, timeColumn string) *plantest.PlanSpec {
		return &plantest.PlanSpec{
			Nodes: []plan.Node{
				plan.CreateLogicalNode("ReadRange", &readRange),
				plan.CreateLogicalNode("window1", &window),
				plan.CreateLogicalNode(agg, spec),
				plan.CreateLogicalNode("duplicate", duplicate(timeColumn, "_time")),
				plan.CreateLogicalNode("window2", &windowInf),
			},
			Edges: [][2]int{
				{0, 1},
				{1, 2},
				{2, 3},
				{3, 4},
			},
		}
	}

	aggregateWindowResult := func(proc plan.ProcedureKind, createEmpty bool, timeColumn string, successors ...plan.Node) *plantest.PlanSpec {
		spec := &plantest.PlanSpec{
			Nodes: []plan.Node{
				plan.CreatePhysicalNode("ReadWindowAggregateByTime", &influxdb.ReadWindowAggregatePhysSpec{
					ReadRangePhysSpec: readRange,
					Aggregates:        []plan.ProcedureKind{proc},
					WindowEvery:       flux.ConvertDuration(60000000000 * time.Nanosecond),
					CreateEmpty:       createEmpty,
					TimeColumn:        timeColumn,
				}),
			},
		}
		for i, successor := range successors {
			spec.Nodes = append(spec.Nodes, successor)
			spec.Edges = append(spec.Edges, [2]int{i, i + 1})
		}
		return spec
	}

	// Push down the duplicate |> window(every: inf)
	tests = append(tests, plantest.RuleTestCase{
		Context: context.Background(),
		Name:    "AggregateWindowCount",
		Rules: []plan.Rule{
			influxdb.PushDownWindowAggregateRule{},
			influxdb.PushDownWindowAggregateByTimeRule{},
		},
		Before: aggregateWindowPlan(window1m, "count", countProcedureSpec(), "_stop"),
		After:  aggregateWindowResult("count", false, "_stop"),
	})

	// Push down the duplicate |> window(every: inf) using _start column
	tests = append(tests, plantest.RuleTestCase{
		Context: context.Background(),
		Name:    "AggregateWindowCount",
		Rules: []plan.Rule{
			influxdb.PushDownWindowAggregateRule{},
			influxdb.PushDownWindowAggregateByTimeRule{},
		},
		Before: aggregateWindowPlan(window1m, "count", countProcedureSpec(), "_start"),
		After:  aggregateWindowResult("count", false, "_start"),
	})

	// Push down duplicate |> window(every: inf) with create empty.
	tests = append(tests, plantest.RuleTestCase{
		Context: context.Background(),
		Name:    "AggregateWindowCountCreateEmpty",
		Rules: []plan.Rule{
			influxdb.PushDownWindowAggregateRule{},
			influxdb.PushDownWindowAggregateByTimeRule{},
		},
		Before: aggregateWindowPlan(windowCreateEmpty1m, "count", countProcedureSpec(), "_stop"),
		After:  aggregateWindowResult("count", true, "_stop"),
	})

	// Invalid duplicate column.
	tests = append(tests, plantest.RuleTestCase{
		Context: context.Background(),
		Name:    "AggregateWindowCountInvalidDuplicateColumn",
		Rules: []plan.Rule{
			influxdb.PushDownWindowAggregateRule{},
			influxdb.PushDownWindowAggregateByTimeRule{},
		},
		Before: aggregateWindowPlan(window1m, "count", countProcedureSpec(), "_value"),
		After: simpleResult("count", false,
			plan.CreatePhysicalNode("duplicate", duplicate("_value", "_time")),
			plan.CreatePhysicalNode("window2", &windowInf),
		),
	})

	// Invalid duplicate as.
	tests = append(tests, plantest.RuleTestCase{
		Context: context.Background(),
		Name:    "AggregateWindowCountInvalidDuplicateAs",
		Rules: []plan.Rule{
			influxdb.PushDownWindowAggregateRule{},
			influxdb.PushDownWindowAggregateByTimeRule{},
		},
		Before: &plantest.PlanSpec{
			Nodes: []plan.Node{
				plan.CreateLogicalNode("ReadRange", &readRange),
				plan.CreateLogicalNode("window1", &window1m),
				plan.CreateLogicalNode("count", countProcedureSpec()),
				plan.CreateLogicalNode("duplicate", duplicate("_stop", "time")),
				plan.CreateLogicalNode("window2", &windowInf),
			},
			Edges: [][2]int{
				{0, 1},
				{1, 2},
				{2, 3},
				{3, 4},
			},
		},
		After: simpleResult("count", false,
			plan.CreatePhysicalNode("duplicate", duplicate("_stop", "time")),
			plan.CreatePhysicalNode("window2", &windowInf),
		),
	})

	// Invalid closing window.
	tests = append(tests, plantest.RuleTestCase{
		Context: context.Background(),
		Name:    "AggregateWindowCountInvalidClosingWindow",
		Rules: []plan.Rule{
			influxdb.PushDownWindowAggregateRule{},
			influxdb.PushDownWindowAggregateByTimeRule{},
		},
		Before: &plantest.PlanSpec{
			Nodes: []plan.Node{
				plan.CreateLogicalNode("ReadRange", &readRange),
				plan.CreateLogicalNode("window1", &window1m),
				plan.CreateLogicalNode("count", countProcedureSpec()),
				plan.CreateLogicalNode("duplicate", duplicate("_stop", "_time")),
				plan.CreateLogicalNode("window2", &window1m),
			},
			Edges: [][2]int{
				{0, 1},
				{1, 2},
				{2, 3},
				{3, 4},
			},
		},
		After: simpleResult("count", false,
			plan.CreatePhysicalNode("duplicate", duplicate("_stop", "_time")),
			plan.CreatePhysicalNode("window2", &window1m),
		),
	})

	// Invalid closing window with multiple problems.
	tests = append(tests, plantest.RuleTestCase{
		Context: context.Background(),
		Name:    "AggregateWindowCountInvalidClosingWindowMultiple",
		Rules: []plan.Rule{
			influxdb.PushDownWindowAggregateRule{},
			influxdb.PushDownWindowAggregateByTimeRule{},
		},
		Before: &plantest.PlanSpec{
			Nodes: []plan.Node{
				plan.CreateLogicalNode("ReadRange", &readRange),
				plan.CreateLogicalNode("window1", &window1m),
				plan.CreateLogicalNode("count", countProcedureSpec()),
				plan.CreateLogicalNode("duplicate", duplicate("_stop", "_time")),
				plan.CreateLogicalNode("window2", &badWindow3),
			},
			Edges: [][2]int{
				{0, 1},
				{1, 2},
				{2, 3},
				{3, 4},
			},
		},
		After: simpleResult("count", false,
			plan.CreatePhysicalNode("duplicate", duplicate("_stop", "_time")),
			plan.CreatePhysicalNode("window2", &badWindow3),
		),
	})

	// Invalid closing window with multiple problems.
	tests = append(tests, plantest.RuleTestCase{
		Context: context.Background(),
		Name:    "AggregateWindowCountInvalidClosingWindowCreateEmpty",
		Rules: []plan.Rule{
			influxdb.PushDownWindowAggregateRule{},
			influxdb.PushDownWindowAggregateByTimeRule{},
		},
		Before: &plantest.PlanSpec{
			Nodes: []plan.Node{
				plan.CreateLogicalNode("ReadRange", &readRange),
				plan.CreateLogicalNode("window1", &window1m),
				plan.CreateLogicalNode("count", countProcedureSpec()),
				plan.CreateLogicalNode("duplicate", duplicate("_stop", "_time")),
				plan.CreateLogicalNode("window2", &windowInfCreateEmpty),
			},
			Edges: [][2]int{
				{0, 1},
				{1, 2},
				{2, 3},
				{3, 4},
			},
		},
		After: simpleResult("count", false,
			plan.CreatePhysicalNode("duplicate", duplicate("_stop", "_time")),
			plan.CreatePhysicalNode("window2", &windowInfCreateEmpty),
		),
	})

	// Multiple matching patterns.
	tests = append(tests, plantest.RuleTestCase{
		Context: context.Background(),
		Name:    "AggregateWindowCountMultipleMatches",
		Rules: []plan.Rule{
			influxdb.PushDownWindowAggregateRule{},
			influxdb.PushDownWindowAggregateByTimeRule{},
		},
		Before: &plantest.PlanSpec{
			Nodes: []plan.Node{
				plan.CreateLogicalNode("ReadRange", &readRange),
				plan.CreateLogicalNode("window1", &window1m),
				plan.CreateLogicalNode("count", countProcedureSpec()),
				plan.CreateLogicalNode("duplicate", duplicate("_stop", "_time")),
				plan.CreateLogicalNode("window2", &windowInf),
				plan.CreateLogicalNode("duplicate2", duplicate("_stop", "_time")),
				plan.CreateLogicalNode("window3", &windowInf),
			},
			Edges: [][2]int{
				{0, 1},
				{1, 2},
				{2, 3},
				{3, 4},
				{4, 5},
				{5, 6},
			},
		},
		After: aggregateWindowResult("count", false, "_stop",
			plan.CreatePhysicalNode("duplicate2", duplicate("_stop", "_time")),
			plan.CreatePhysicalNode("window3", &windowInf),
		),
	})

	rename := universe.SchemaMutationProcedureSpec{
		Mutations: []universe.SchemaMutation{
			&universe.RenameOpSpec{
				Columns: map[string]string{"_time": "time"},
			},
		},
	}

	// Wrong schema mutator.
	tests = append(tests, plantest.RuleTestCase{
		Context: context.Background(),
		Name:    "AggregateWindowCountWrongSchemaMutator",
		Rules: []plan.Rule{
			influxdb.PushDownWindowAggregateRule{},
			influxdb.PushDownWindowAggregateByTimeRule{},
		},
		Before: &plantest.PlanSpec{
			Nodes: []plan.Node{
				plan.CreateLogicalNode("ReadRange", &readRange),
				plan.CreateLogicalNode("window1", &window1m),
				plan.CreateLogicalNode("count", countProcedureSpec()),
				plan.CreateLogicalNode("rename", &rename),
				plan.CreateLogicalNode("window2", &windowInf),
			},
			Edges: [][2]int{
				{0, 1},
				{1, 2},
				{2, 3},
				{3, 4},
			},
		},
		After: simpleResult("count", false,
			plan.CreatePhysicalNode("rename", &rename),
			plan.CreatePhysicalNode("window2", &windowInf),
		),
	})

	for _, tc := range tests {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			plantest.PhysicalRuleTestHelper(t, &tc)
		})
	}
}

func TestPushDownBareAggregateRule(t *testing.T) {
	readRange := &influxdb.ReadRangePhysSpec{
		Bucket: "my-bucket",
		Bounds: flux.Bounds{
			Start: fluxTime(5),
			Stop:  fluxTime(10),
		},
	}

	readWindowAggregate := func(proc plan.ProcedureKind) *influxdb.ReadWindowAggregatePhysSpec {
		return &influxdb.ReadWindowAggregatePhysSpec{
			ReadRangePhysSpec: *(readRange.Copy().(*influxdb.ReadRangePhysSpec)),
			WindowEvery:       flux.ConvertDuration(math.MaxInt64 * time.Nanosecond),
			Aggregates:        []plan.ProcedureKind{proc},
		}
	}

	testcases := []plantest.RuleTestCase{
		{
			// ReadRange -> count => ReadWindowAggregate
			Context: context.Background(),
			Name:    "push down count",
			Rules:   []plan.Rule{influxdb.PushDownBareAggregateRule{}},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadRange", readRange),
					plan.CreatePhysicalNode("count", countProcedureSpec()),
				},
				Edges: [][2]int{
					{0, 1},
				},
			},
			After: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadWindowAggregate", readWindowAggregate(universe.CountKind)),
				},
			},
		},
		{
			// ReadRange -> sum => ReadWindowAggregate
			Context: context.Background(),
			Name:    "push down sum",
			Rules:   []plan.Rule{influxdb.PushDownBareAggregateRule{}},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadRange", readRange),
					plan.CreatePhysicalNode("sum", sumProcedureSpec()),
				},
				Edges: [][2]int{
					{0, 1},
				},
			},
			After: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadWindowAggregate", readWindowAggregate(universe.SumKind)),
				},
			},
		},
		{
			// ReadRange -> first => ReadWindowAggregate
			Context: context.Background(),
			Name:    "push down first",
			Rules:   []plan.Rule{influxdb.PushDownBareAggregateRule{}},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadRange", readRange),
					plan.CreatePhysicalNode("first", firstProcedureSpec()),
				},
				Edges: [][2]int{
					{0, 1},
				},
			},
			After: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadWindowAggregate", readWindowAggregate(universe.FirstKind)),
				},
			},
		},
		{
			// ReadRange -> last => ReadWindowAggregate
			Context: context.Background(),
			Name:    "push down last",
			Rules:   []plan.Rule{influxdb.PushDownBareAggregateRule{}},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadRange", readRange),
					plan.CreatePhysicalNode("last", lastProcedureSpec()),
				},
				Edges: [][2]int{
					{0, 1},
				},
			},
			After: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadWindowAggregate", readWindowAggregate(universe.LastKind)),
				},
			},
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			plantest.PhysicalRuleTestHelper(t, &tc)
		})
	}
}
