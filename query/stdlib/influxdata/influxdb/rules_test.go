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
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/plan/plantest"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/stdlib/universe"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb/v2/kit/feature"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/query/stdlib/influxdata/influxdb"
	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
)

// A small mock reader so we can indicate if rule-related capabilities are
// present
type mockReaderCaps struct {
	query.StorageReader
	Have bool
}

func (caps mockReaderCaps) GetWindowAggregateCapability(ctx context.Context) query.WindowAggregateCapability {
	return mockWAC{Have: caps.Have}
}

func (caps mockReaderCaps) ReadWindowAggregate(ctx context.Context, spec query.ReadWindowAggregateSpec, alloc *memory.Allocator) (query.TableIterator, error) {
	return nil, nil
}

// Mock Window Aggregate Capability
type mockWAC struct {
	Have bool
}

func (m mockWAC) HaveMin() bool   { return m.Have }
func (m mockWAC) HaveMax() bool   { return m.Have }
func (m mockWAC) HaveMean() bool  { return m.Have }
func (m mockWAC) HaveCount() bool { return m.Have }
func (m mockWAC) HaveSum() bool   { return m.Have }

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
			Scope: nil,
			Fn:    expr,
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
						Bounds: bounds,
						Filter: toStoragePredicate(pushableFn1),
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
						Bounds: bounds,
						Filter: toStoragePredicate(pushableFn1and2),
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
						Bounds: bounds,
						Filter: toStoragePredicate(pushableFn1),
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
						Bounds: bounds,
						Filter: toStoragePredicate(pushableFn1),
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
						Bounds: bounds,
						Filter: toStoragePredicate(executetest.FunctionExpression(t, `(r) => r.host != ""`)),
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
						Bounds: bounds,
						Filter: toStoragePredicate(executetest.FunctionExpression(t, `(r) => r.host == ""`)),
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
						Bounds: bounds,
						Filter: toStoragePredicate(executetest.FunctionExpression(t, `(r) => r.host != ""`)),
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
						Bounds: bounds,
						Filter: toStoragePredicate(executetest.FunctionExpression(t, `(r) => r._value == ""`)),
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
						Bounds: bounds,
						Filter: toStoragePredicate(executetest.FunctionExpression(t, `(r) => r.host == "cpu" and r.host != ""`)),
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
			s.Filter, _ = influxdb.ToStoragePredicate(bodyExpr, "r")
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
			s.Filter, _ = influxdb.ToStoragePredicate(bodyExpr, "r")
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

//
// Window Aggregate Testing
//
func TestPushDownWindowAggregateRule(t *testing.T) {
	// Turn on all variants.
	flagger := mock.NewFlagger(map[feature.Flag]interface{}{
		feature.PushDownWindowAggregateCount(): true,
		feature.PushDownWindowAggregateRest():  true,
	})

	withFlagger, _ := feature.Annotate(context.Background(), flagger)

	// Construct dependencies either with or without aggregate window caps.
	deps := func(have bool) influxdb.StorageDependencies {
		return influxdb.StorageDependencies{
			FromDeps: influxdb.FromDependencies{
				Reader:  mockReaderCaps{Have: have},
				Metrics: influxdb.NewMetrics(nil),
			},
		}
	}

	haveCaps := deps(true).Inject(withFlagger)
	noCaps := deps(false).Inject(withFlagger)

	readRange := influxdb.ReadRangePhysSpec{
		Bucket: "my-bucket",
		Bounds: flux.Bounds{
			Start: fluxTime(5),
			Stop:  fluxTime(10),
		},
	}

	dur1m := values.ConvertDuration(60 * time.Second)
	dur2m := values.ConvertDuration(120 * time.Second)
	dur0 := values.ConvertDuration(0)
	durNeg, _ := values.ParseDuration("-60s")
	dur1y, _ := values.ParseDuration("1y")

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
	simpleResult := func(proc plan.ProcedureKind) *plantest.PlanSpec {
		return &plantest.PlanSpec{
			Nodes: []plan.Node{
				plan.CreatePhysicalNode("ReadWindowAggregate", &influxdb.ReadWindowAggregatePhysSpec{
					ReadRangePhysSpec: readRange,
					Aggregates:        []plan.ProcedureKind{proc},
					WindowEvery:       60000000000,
				}),
			},
		}
	}

	minProcedureSpec := func() *universe.MinProcedureSpec {
		return &universe.MinProcedureSpec{
			SelectorConfig: execute.SelectorConfig{Column: "_value"},
		}
	}
	maxProcedureSpec := func() *universe.MaxProcedureSpec {
		return &universe.MaxProcedureSpec{
			SelectorConfig: execute.SelectorConfig{Column: "_value"},
		}
	}
	meanProcedureSpec := func() *universe.MeanProcedureSpec {
		return &universe.MeanProcedureSpec{
			AggregateConfig: execute.AggregateConfig{Columns: []string{"_value"}},
		}
	}
	countProcedureSpec := func() *universe.CountProcedureSpec {
		return &universe.CountProcedureSpec{
			AggregateConfig: execute.AggregateConfig{Columns: []string{"_value"}},
		}
	}
	sumProcedureSpec := func() *universe.SumProcedureSpec {
		return &universe.SumProcedureSpec{
			AggregateConfig: execute.AggregateConfig{Columns: []string{"_value"}},
		}
	}

	// ReadRange -> window -> min => ReadWindowAggregate
	tests = append(tests, plantest.RuleTestCase{
		Context: haveCaps,
		Name:    "SimplePassMin",
		Rules:   []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
		Before:  simplePlanWithWindowAgg(window1m, "min", minProcedureSpec()),
		After:   simpleResult("min"),
	})

	// ReadRange -> window -> max => ReadWindowAggregate
	tests = append(tests, plantest.RuleTestCase{
		Context: haveCaps,
		Name:    "SimplePassMax",
		Rules:   []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
		Before:  simplePlanWithWindowAgg(window1m, "max", maxProcedureSpec()),
		After:   simpleResult("max"),
	})

	// ReadRange -> window -> mean => ReadWindowAggregate
	tests = append(tests, plantest.RuleTestCase{
		Context: haveCaps,
		Name:    "SimplePassMean",
		Rules:   []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
		Before:  simplePlanWithWindowAgg(window1m, "mean", meanProcedureSpec()),
		After:   simpleResult("mean"),
	})

	// ReadRange -> window -> count => ReadWindowAggregate
	tests = append(tests, plantest.RuleTestCase{
		Context: haveCaps,
		Name:    "SimplePassCount",
		Rules:   []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
		Before:  simplePlanWithWindowAgg(window1m, "count", countProcedureSpec()),
		After:   simpleResult("count"),
	})

	// ReadRange -> window -> sum => ReadWindowAggregate
	tests = append(tests, plantest.RuleTestCase{
		Context: haveCaps,
		Name:    "SimplePassSum",
		Rules:   []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
		Before:  simplePlanWithWindowAgg(window1m, "sum", sumProcedureSpec()),
		After:   simpleResult("sum"),
	})

	// Rewrite with successors
	// ReadRange -> window -> min -> count {2} => ReadWindowAggregate -> count {2}
	tests = append(tests, plantest.RuleTestCase{
		Context: haveCaps,
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
					WindowEvery:       60000000000,
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

	// Helper that adds a test with a simple plan that does not pass due to a
	// specified bad window
	simpleMinUnchanged := func(name string, window universe.WindowProcedureSpec) {
		// Note: NoChange is not working correctly for these tests. It is
		// expecting empty time, start, and stop column fields.
		tests = append(tests, plantest.RuleTestCase{
			Name:     name,
			Context:  haveCaps,
			Rules:    []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
			Before:   simplePlanWithWindowAgg(window, "min", countProcedureSpec()),
			NoChange: true,
		})
	}

	// Condition not met: period not equal to every
	badWindow1 := window1m
	badWindow1.Window.Period = dur2m
	simpleMinUnchanged("BadPeriod", badWindow1)

	// Condition not met: offset non-zero
	badWindow2 := window1m
	badWindow2.Window.Offset = dur1m
	simpleMinUnchanged("BadOffset", badWindow2)

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

	// Condition not met: createEmpty is not false
	badWindow6 := window1m
	badWindow6.CreateEmpty = true
	simpleMinUnchanged("BadCreateEmpty", badWindow6)

	// Condition not met: duration too long.
	simpleMinUnchanged("WindowTooLarge", window1y)

	// Condition not met: neg duration.
	simpleMinUnchanged("WindowNeg", windowNeg)

	// Bad min column
	// ReadRange -> window -> min => NO-CHANGE
	tests = append(tests, plantest.RuleTestCase{
		Name:    "BadMinCol",
		Context: haveCaps,
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
		Context: haveCaps,
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
		Context: haveCaps,
		Rules:   []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
		Before: simplePlanWithWindowAgg(window1m, "mean", &universe.MeanProcedureSpec{
			AggregateConfig: execute.AggregateConfig{Columns: []string{"_valmoo"}},
		}),
		NoChange: true,
	})
	tests = append(tests, plantest.RuleTestCase{
		Name:    "BadMeanCol2",
		Context: haveCaps,
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
		Context: haveCaps,
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
		Context: haveCaps,
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
		Context:  haveCaps,
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
		Context:  haveCaps,
		Rules:    []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
		Before:   noPatternMatch2(),
		NoChange: true,
	})

	// Fail due to no capabilities present.
	tests = append(tests, plantest.RuleTestCase{
		Context:  noCaps,
		Name:     "FailNoCaps",
		Rules:    []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
		Before:   simplePlanWithWindowAgg(window1m, "count", countProcedureSpec()),
		After:    simpleResult("count"),
		NoChange: true,
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
	// Turn on support for window aggregate count
	flagger := mock.NewFlagger(map[feature.Flag]interface{}{
		feature.PushDownWindowAggregateCount(): true,
	})

	withFlagger, _ := feature.Annotate(context.Background(), flagger)

	// Construct dependencies either with or without aggregate window caps.
	deps := func(have bool) influxdb.StorageDependencies {
		return influxdb.StorageDependencies{
			FromDeps: influxdb.FromDependencies{
				Reader:  mockReaderCaps{Have: have},
				Metrics: influxdb.NewMetrics(nil),
			},
		}
	}

	haveCaps := deps(true).Inject(withFlagger)
	noCaps := deps(false).Inject(withFlagger)

	readRange := &influxdb.ReadRangePhysSpec{
		Bucket: "my-bucket",
		Bounds: flux.Bounds{
			Start: fluxTime(5),
			Stop:  fluxTime(10),
		},
	}

	readWindowAggregate := &influxdb.ReadWindowAggregatePhysSpec{
		ReadRangePhysSpec: *(readRange.Copy().(*influxdb.ReadRangePhysSpec)),
		WindowEvery:       math.MaxInt64,
		Aggregates:        []plan.ProcedureKind{universe.CountKind},
	}

	minProcedureSpec := func() *universe.MinProcedureSpec {
		return &universe.MinProcedureSpec{
			SelectorConfig: execute.SelectorConfig{Column: "_value"},
		}
	}
	countProcedureSpec := func() *universe.CountProcedureSpec {
		return &universe.CountProcedureSpec{
			AggregateConfig: execute.AggregateConfig{Columns: []string{"_value"}},
		}
	}

	testcases := []plantest.RuleTestCase{
		{
			// successful push down
			Context: haveCaps,
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
					plan.CreatePhysicalNode("ReadWindowAggregate", readWindowAggregate),
				},
			},
		},
		{
			// capability not provided in storage layer
			Context: noCaps,
			Name:    "no caps",
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
			NoChange: true,
		},
		{
			// unsupported aggregate
			Context: haveCaps,
			Name:    "no push down min",
			Rules:   []plan.Rule{influxdb.PushDownBareAggregateRule{}},
			Before: &plantest.PlanSpec{
				Nodes: []plan.Node{
					plan.CreatePhysicalNode("ReadRange", readRange),
					plan.CreatePhysicalNode("count", minProcedureSpec()),
				},
				Edges: [][2]int{
					{0, 1},
				},
			},
			NoChange: true,
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

//
// Group Aggregate Testing
//
func TestPushDownGroupAggregateRule(t *testing.T) {
	// Turn on all flags
	flagger := mock.NewFlagger(map[feature.Flag]interface{}{
		feature.PushDownGroupAggregateCount(): true,
	})

	ctx, _ := feature.Annotate(context.Background(), flagger)

	readGroupAgg := func(aggregateMethod string) *influxdb.ReadGroupPhysSpec {
		return &influxdb.ReadGroupPhysSpec{
			ReadRangePhysSpec: influxdb.ReadRangePhysSpec{
				Bucket: "my-bucket",
				Bounds: flux.Bounds{
					Start: fluxTime(5),
					Stop:  fluxTime(10),
				},
			},
			GroupMode:       flux.GroupModeBy,
			GroupKeys:       []string{"_measurement", "tag0", "tag1"},
			AggregateMethod: aggregateMethod,
		}
	}
	readGroup := func() *influxdb.ReadGroupPhysSpec {
		return readGroupAgg("")
	}

	tests := make([]plantest.RuleTestCase, 0)

	// construct a simple plan with a specific aggregate
	simplePlanWithAgg := func(agg plan.NodeID, spec plan.ProcedureSpec) *plantest.PlanSpec {
		return &plantest.PlanSpec{
			Nodes: []plan.Node{
				plan.CreateLogicalNode("ReadGroup", readGroup()),
				plan.CreateLogicalNode(agg, spec),
			},
			Edges: [][2]int{
				{0, 1},
			},
		}
	}

	// construct a simple result
	simpleResult := func(proc string) *plantest.PlanSpec {
		return &plantest.PlanSpec{
			Nodes: []plan.Node{
				plan.CreatePhysicalNode("ReadGroup", readGroupAgg(proc)),
			},
		}
	}

	minProcedureSpec := func() *universe.MinProcedureSpec {
		return &universe.MinProcedureSpec{
			SelectorConfig: execute.SelectorConfig{Column: "_value"},
		}
	}
	countProcedureSpec := func() *universe.CountProcedureSpec {
		return &universe.CountProcedureSpec{
			AggregateConfig: execute.AggregateConfig{Columns: []string{"_value"}},
		}
	}
	sumProcedureSpec := func() *universe.SumProcedureSpec {
		return &universe.SumProcedureSpec{
			AggregateConfig: execute.AggregateConfig{Columns: []string{"_value"}},
		}
	}

	// ReadGroup -> count => ReadGroup
	tests = append(tests, plantest.RuleTestCase{
		Context: ctx,
		Name:    "SimplePassCount",
		Rules:   []plan.Rule{influxdb.PushDownGroupAggregateRule{}},
		Before:  simplePlanWithAgg("count", countProcedureSpec()),
		After:   simpleResult("count"),
	})

	// Rewrite with successors
	// ReadGroup -> count -> sum {2} => ReadGroup -> count {2}
	tests = append(tests, plantest.RuleTestCase{
		Context: ctx,
		Name:    "WithSuccessor1",
		Rules:   []plan.Rule{influxdb.PushDownGroupAggregateRule{}},
		Before: &plantest.PlanSpec{
			Nodes: []plan.Node{
				plan.CreateLogicalNode("ReadGroup", readGroup()),
				plan.CreateLogicalNode("count", countProcedureSpec()),
				plan.CreateLogicalNode("sum", sumProcedureSpec()),
				plan.CreateLogicalNode("sum", sumProcedureSpec()),
			},
			Edges: [][2]int{
				{0, 1},
				{1, 2},
				{1, 3},
			},
		},
		After: &plantest.PlanSpec{
			Nodes: []plan.Node{
				plan.CreatePhysicalNode("ReadGroup", readGroupAgg("count")),
				plan.CreateLogicalNode("sum", sumProcedureSpec()),
				plan.CreateLogicalNode("sum", sumProcedureSpec()),
			},
			Edges: [][2]int{
				{0, 1},
				{0, 2},
			},
		},
	})

	// Cannot replace a ReadGroup that already has an aggregate. This exercises
	// the check that ReadGroup aggregate is not set.
	// ReadGroup -> count -> count => ReadGroup -> count
	tests = append(tests, plantest.RuleTestCase{
		Context: ctx,
		Name:    "WithSuccessor2",
		Rules:   []plan.Rule{influxdb.PushDownGroupAggregateRule{}},
		Before: &plantest.PlanSpec{
			Nodes: []plan.Node{
				plan.CreateLogicalNode("ReadGroup", readGroup()),
				plan.CreateLogicalNode("count", countProcedureSpec()),
				plan.CreateLogicalNode("count", countProcedureSpec()),
			},
			Edges: [][2]int{
				{0, 1},
				{1, 2},
			},
		},
		After: &plantest.PlanSpec{
			Nodes: []plan.Node{
				plan.CreatePhysicalNode("ReadGroup", readGroupAgg("count")),
				plan.CreateLogicalNode("count", countProcedureSpec()),
			},
			Edges: [][2]int{
				{0, 1},
			},
		},
	})

	// Bad count column
	// ReadGroup -> count => NO-CHANGE
	tests = append(tests, plantest.RuleTestCase{
		Name:    "BadCountCol",
		Context: ctx,
		Rules:   []plan.Rule{influxdb.PushDownGroupAggregateRule{}},
		Before: simplePlanWithAgg("count", &universe.CountProcedureSpec{
			AggregateConfig: execute.AggregateConfig{Columns: []string{"_valmoo"}},
		}),
		NoChange: true,
	})

	// No match due to a collapsed node having a successor
	// ReadGroup -> count
	//          \-> min
	tests = append(tests, plantest.RuleTestCase{
		Name:    "CollapsedWithSuccessor",
		Context: ctx,
		Rules:   []plan.Rule{influxdb.PushDownGroupAggregateRule{}},
		Before: &plantest.PlanSpec{
			Nodes: []plan.Node{
				plan.CreateLogicalNode("ReadGroup", readGroup()),
				plan.CreateLogicalNode("count", countProcedureSpec()),
				plan.CreateLogicalNode("min", minProcedureSpec()),
			},
			Edges: [][2]int{
				{0, 1},
				{0, 2},
			},
		},
		NoChange: true,
	})

	// No pattern match
	// ReadGroup -> filter -> min -> NO-CHANGE
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
				plan.CreateLogicalNode("ReadGroup", readGroup()),
				plan.CreatePhysicalNode("filter", &universe.FilterProcedureSpec{
					Fn: makeResolvedFilterFn(pushableFn1),
				}),
				plan.CreateLogicalNode("count", countProcedureSpec()),
			},
			Edges: [][2]int{
				{0, 1},
				{1, 2},
			},
		}
	}
	tests = append(tests, plantest.RuleTestCase{
		Name:     "NoPatternMatch",
		Context:  ctx,
		Rules:    []plan.Rule{influxdb.PushDownWindowAggregateRule{}},
		Before:   noPatternMatch1(),
		NoChange: true,
	})

	for _, tc := range tests {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			plantest.PhysicalRuleTestHelper(t, &tc)
		})
	}
}
