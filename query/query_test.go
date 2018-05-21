package query_test

import (
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/ifql/functions"
	"github.com/influxdata/ifql/query"
)

var ignoreUnexportedQuerySpec = cmpopts.IgnoreUnexported(query.Spec{})

func TestQuery_JSON(t *testing.T) {
	srcData := []byte(`
{
	"operations":[
		{
			"id": "from",
			"kind": "from",
			"spec": {
				"db":"mydb"
			}
		},
		{
			"id": "range",
			"kind": "range",
			"spec": {
				"start": "-4h",
				"stop": "now"
			}
		},
		{
			"id": "sum",
			"kind": "sum"
		}
	],
	"edges":[
		{"parent":"from","child":"range"},
		{"parent":"range","child":"sum"}
	]
}
	`)

	// Ensure we can properly unmarshal a query
	gotQ := query.Spec{}
	if err := json.Unmarshal(srcData, &gotQ); err != nil {
		t.Fatal(err)
	}
	expQ := query.Spec{
		Operations: []*query.Operation{
			{
				ID: "from",
				Spec: &functions.FromOpSpec{
					Database: "mydb",
				},
			},
			{
				ID: "range",
				Spec: &functions.RangeOpSpec{
					Start: query.Time{
						Relative:   -4 * time.Hour,
						IsRelative: true,
					},
					Stop: query.Time{
						IsRelative: true,
					},
				},
			},
			{
				ID:   "sum",
				Spec: &functions.SumOpSpec{},
			},
		},
		Edges: []query.Edge{
			{Parent: "from", Child: "range"},
			{Parent: "range", Child: "sum"},
		},
	}
	if !cmp.Equal(gotQ, expQ, ignoreUnexportedQuerySpec) {
		t.Errorf("unexpected query:\n%s", cmp.Diff(gotQ, expQ, ignoreUnexportedQuerySpec))
	}

	// Ensure we can properly marshal a query
	data, err := json.Marshal(expQ)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &gotQ); err != nil {
		t.Fatal(err)
	}
	if !cmp.Equal(gotQ, expQ, ignoreUnexportedQuerySpec) {
		t.Errorf("unexpected query after marshalling: -want/+got %s", cmp.Diff(expQ, gotQ, ignoreUnexportedQuerySpec))
	}
}

func TestQuery_Walk(t *testing.T) {
	testCases := []struct {
		query     *query.Spec
		walkOrder []query.OperationID
		err       error
	}{
		{
			query: &query.Spec{},
			err:   errors.New("query has no root nodes"),
		},
		{
			query: &query.Spec{
				Operations: []*query.Operation{
					{ID: "a"},
					{ID: "b"},
				},
				Edges: []query.Edge{
					{Parent: "a", Child: "b"},
					{Parent: "a", Child: "c"},
				},
			},
			err: errors.New("edge references unknown child operation \"c\""),
		},
		{
			query: &query.Spec{
				Operations: []*query.Operation{
					{ID: "a"},
					{ID: "b"},
					{ID: "b"},
				},
				Edges: []query.Edge{
					{Parent: "a", Child: "b"},
					{Parent: "a", Child: "b"},
				},
			},
			err: errors.New("found duplicate operation ID \"b\""),
		},
		{
			query: &query.Spec{
				Operations: []*query.Operation{
					{ID: "a"},
					{ID: "b"},
					{ID: "c"},
				},
				Edges: []query.Edge{
					{Parent: "a", Child: "b"},
					{Parent: "b", Child: "c"},
					{Parent: "c", Child: "b"},
				},
			},
			err: errors.New("found cycle in query"),
		},
		{
			query: &query.Spec{
				Operations: []*query.Operation{
					{ID: "a"},
					{ID: "b"},
					{ID: "c"},
					{ID: "d"},
				},
				Edges: []query.Edge{
					{Parent: "a", Child: "b"},
					{Parent: "b", Child: "c"},
					{Parent: "c", Child: "d"},
					{Parent: "d", Child: "b"},
				},
			},
			err: errors.New("found cycle in query"),
		},
		{
			query: &query.Spec{
				Operations: []*query.Operation{
					{ID: "a"},
					{ID: "b"},
					{ID: "c"},
					{ID: "d"},
				},
				Edges: []query.Edge{
					{Parent: "a", Child: "b"},
					{Parent: "b", Child: "c"},
					{Parent: "c", Child: "d"},
				},
			},
			walkOrder: []query.OperationID{
				"a", "b", "c", "d",
			},
		},
		{
			query: &query.Spec{
				Operations: []*query.Operation{
					{ID: "a"},
					{ID: "b"},
					{ID: "c"},
					{ID: "d"},
				},
				Edges: []query.Edge{
					{Parent: "a", Child: "b"},
					{Parent: "a", Child: "c"},
					{Parent: "b", Child: "d"},
					{Parent: "c", Child: "d"},
				},
			},
			walkOrder: []query.OperationID{
				"a", "c", "b", "d",
			},
		},
		{
			query: &query.Spec{
				Operations: []*query.Operation{
					{ID: "a"},
					{ID: "b"},
					{ID: "c"},
					{ID: "d"},
				},
				Edges: []query.Edge{
					{Parent: "a", Child: "c"},
					{Parent: "b", Child: "c"},
					{Parent: "c", Child: "d"},
				},
			},
			walkOrder: []query.OperationID{
				"b", "a", "c", "d",
			},
		},
		{
			query: &query.Spec{
				Operations: []*query.Operation{
					{ID: "a"},
					{ID: "b"},
					{ID: "c"},
					{ID: "d"},
				},
				Edges: []query.Edge{
					{Parent: "a", Child: "c"},
					{Parent: "b", Child: "d"},
				},
			},
			walkOrder: []query.OperationID{
				"b", "d", "a", "c",
			},
		},
	}
	for i, tc := range testCases {
		tc := tc
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			var gotOrder []query.OperationID
			err := tc.query.Walk(func(o *query.Operation) error {
				gotOrder = append(gotOrder, o.ID)
				return nil
			})
			if tc.err == nil {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				if err == nil {
					t.Fatalf("expected error: %q", tc.err)
				} else if got, exp := err.Error(), tc.err.Error(); got != exp {
					t.Fatalf("unexpected errors: got %q exp %q", got, exp)
				}
			}

			if !cmp.Equal(gotOrder, tc.walkOrder) {
				t.Fatalf("unexpected walk order -want/+got %s", cmp.Diff(tc.walkOrder, gotOrder))
			}
		})
	}
}
