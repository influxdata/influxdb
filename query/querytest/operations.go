package querytest

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/semantic/semantictest"
)

func OperationMarshalingTestHelper(t *testing.T, data []byte, expOp *query.Operation) {
	t.Helper()

	opts := append(
		semantictest.CmpOptions,
		cmp.AllowUnexported(functions.JoinOpSpec{}),
		cmpopts.IgnoreUnexported(functions.JoinOpSpec{}),
	)

	// Ensure we can properly unmarshal a spec
	gotOp := new(query.Operation)
	if err := json.Unmarshal(data, gotOp); err != nil {
		t.Fatal(err)
	}
	if !cmp.Equal(gotOp, expOp, opts...) {
		t.Errorf("unexpected operation -want/+got %s", cmp.Diff(expOp, gotOp, opts...))
	}

	// Marshal the spec and ensure we can unmarshal it again.
	data, err := json.Marshal(expOp)
	if err != nil {
		t.Fatal(err)
	}
	gotOp = new(query.Operation)
	if err := json.Unmarshal(data, gotOp); err != nil {
		t.Fatal(err)
	}

	if !cmp.Equal(gotOp, expOp, opts...) {
		t.Errorf("unexpected operation after marshalling -want/+got %s", cmp.Diff(expOp, gotOp, opts...))
	}
}
