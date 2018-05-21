package plantest

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/ifql/query/plan"
	"github.com/influxdata/ifql/semantic/semantictest"
)

var CmpOptions = []cmp.Option{
	cmpopts.IgnoreUnexported(plan.Procedure{}),
}

func PhysicalPlan_PushDown_Match_TestHelper(t *testing.T, spec plan.PushDownProcedureSpec, matchSpec plan.ProcedureSpec, want []bool) {
	t.Helper()

	rules := spec.PushDownRules()
	if len(want) != len(rules) {
		t.Fatalf("unexpected number of rules, want:%d got:%d", len(want), len(rules))
	}
	for i, rule := range rules {
		got := rule.Match(matchSpec)
		if got != want[i] {
			t.Errorf("unexpected push down rule matching: got: %t want: %t", got, want[i])
		}
	}
}

func PhysicalPlan_PushDown_TestHelper(t *testing.T, spec plan.PushDownProcedureSpec, root *plan.Procedure, wantDuplicated bool, want *plan.Procedure) {
	t.Helper()

	var duplicate *plan.Procedure
	spec.PushDown(root, func() *plan.Procedure {
		duplicate = root.Copy()
		return duplicate
	})
	got := root

	if wantDuplicated {
		if duplicate == nil {
			t.Fatal("expected push down to duplicate")
		}
		got = duplicate
	} else {
		if duplicate != nil {
			t.Fatal("unexpected push down duplication")
		}
	}

	opts := append(CmpOptions, cmpopts.EquateEmpty())
	opts = append(opts, semantictest.CmpOptions...)
	if !cmp.Equal(got, want, opts...) {
		t.Errorf("unexpected PushDown: -want/+got:\n%s", cmp.Diff(want, got, opts...))
	}
}
