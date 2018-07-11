package query_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/functions"
	_ "github.com/influxdata/platform/query/options"
	"github.com/influxdata/platform/query/parser"
	"github.com/influxdata/platform/query/semantic"
	"github.com/influxdata/platform/query/values"
)

func init() {
	query.FinalizeBuiltIns()
}

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

// Example_option demonstrates retrieving an option from the Flux interpreter
func Example_option() {
	// Instantiate a new Flux interpreter with pre-populated option and global scopes
	itrp := query.NewInterpreter()

	// Retrieve the default value for an option
	nowFunc := itrp.Option("now")

	// The now option is a function value whose default behavior is to return
	// the current system time when called. The function now() doesn't take
	// any arguments so can be called with nil.
	nowTime, _ := nowFunc.Function().Call(nil)
	fmt.Fprintf(os.Stderr, "The current system time (UTC) is: %v\n", nowTime)
	// Output:
}

// Example_setOption demonstrates setting an option from the Flux interpreter
func Example_setOption() {
	// Instantiate a new Flux interpreter with pre-populated option and global scopes
	itrp := query.NewInterpreter()

	// Set a new option from the interpreter
	itrp.SetOption("dummy_option", values.NewIntValue(3))

	fmt.Printf("dummy_option = %d", itrp.Option("dummy_option").Int())
	// Output: dummy_option = 3
}

// Example_overrideDefaultOptionExternally demonstrates how declaring an option
// in a Flux script will change that option's binding in the options scope of the interpreter.
func Example_overrideDefaultOptionExternally() {
	queryString := `
		now = () => 2018-07-13T00:00:00Z
		what_time_is_it = now()`

	itrp := query.NewInterpreter()
	_, declarations := query.BuiltIns()

	ast, _ := parser.NewAST(queryString)
	semanticProgram, _ := semantic.New(ast, declarations)

	// Evaluate program
	itrp.Eval(semanticProgram)

	// After evaluating the program, lookup the value of what_time_is_it
	now, _ := itrp.GlobalScope().Lookup("what_time_is_it")

	// what_time_is_it? Why it's ....
	fmt.Printf("The new current time (UTC) is: %v", now)
	// Output: The new current time (UTC) is: 2018-07-13T00:00:00.000000000Z
}

// Example_overrideDefaultOptionInternally demonstrates how one can override a default
// option that is used in a query before that query is evaluated by the interpreter.
func Example_overrideDefaultOptionInternally() {
	queryString := `what_time_is_it = now()`

	itrp := query.NewInterpreter()
	_, declarations := query.BuiltIns()

	ast, _ := parser.NewAST(queryString)
	semanticProgram, _ := semantic.New(ast, declarations)

	// Define a new now function which returns a static time value of 2018-07-13T00:00:00.000000000Z
	timeValue := time.Date(2018, 7, 13, 0, 0, 0, 0, time.UTC)
	functionName := "newTime"
	functionType := semantic.NewFunctionType(semantic.FunctionSignature{
		ReturnType: semantic.Time,
	})
	functionCall := func(args values.Object) (values.Value, error) {
		return values.NewTimeValue(values.ConvertTime(timeValue)), nil
	}
	sideEffect := false

	newNowFunc := values.NewFunction(functionName, functionType, functionCall, sideEffect)

	// Override the default now function with the new one
	itrp.SetOption("now", newNowFunc)

	// Evaluate program
	itrp.Eval(semanticProgram)

	// After evaluating the program, lookup the value of what_time_is_it
	now, _ := itrp.GlobalScope().Lookup("what_time_is_it")

	// what_time_is_it? Why it's ....
	fmt.Printf("The new current time (UTC) is: %v", now)
	// Output: The new current time (UTC) is: 2018-07-13T00:00:00.000000000Z
}
