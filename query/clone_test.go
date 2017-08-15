package query_test

import (
	"testing"
)

func TestClone(t *testing.T) {
	/*
		merge := &query.Merge{}
		for i := 0; i < 3; i++ {
			call := &query.FunctionCall{Name: "count"}
			_, call.Input = query.AddEdge(nil, call)
			call.Output = merge.AddInput(call)
		}

		sum := &query.FunctionCall{Name: "sum"}
		merge.Output, sum.Input = query.AddEdge(merge, sum)

		var out *query.ReadEdge
		sum.Output, out = query.NewEdge(sum)

		newOut := query.Clone(out)
		spew.Dump(newOut)
	*/
}
