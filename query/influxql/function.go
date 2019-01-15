package influxql

import (
	"errors"
	"fmt"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/stdlib/universe"
	"github.com/influxdata/influxql"
)

// function contains the prototype for invoking a function.
// TODO(jsternberg): This should do a lot more heavy lifting, but it mostly just
// pre-validates that we know the function exists. The cursor creation should be
// done by this struct, but it isn't at the moment.
type function struct {
	Ref  *influxql.VarRef
	call *influxql.Call
}

// parseFunction parses a call AST and creates the function for it.
func parseFunction(expr *influxql.Call) (*function, error) {
	switch expr.Name {
	case "count":
		if exp, got := 1, len(expr.Args); exp != got {
			return nil, fmt.Errorf("invalid number of arguments for %s, expected %d, got %d", expr.Name, exp, got)
		}

		switch ref := expr.Args[0].(type) {
		case *influxql.VarRef:
			return &function{
				Ref:  ref,
				call: expr,
			}, nil
		case *influxql.Call:
			if ref.Name == "distinct" {
				return nil, errors.New("unimplemented: count(distinct)")
			}
			return nil, fmt.Errorf("expected field argument in %s()", expr.Name)
		case *influxql.Distinct:
			return nil, errors.New("unimplemented: count(distinct)")
		case *influxql.Wildcard:
			return nil, errors.New("unimplemented: wildcard function")
		case *influxql.RegexLiteral:
			return nil, errors.New("unimplemented: wildcard regex function")
		default:
			return nil, fmt.Errorf("expected field argument in %s()", expr.Name)
		}
	case "min", "max", "sum", "first", "last", "mean", "median":
		if exp, got := 1, len(expr.Args); exp != got {
			return nil, fmt.Errorf("invalid number of arguments for %s, expected %d, got %d", expr.Name, exp, got)
		}

		switch ref := expr.Args[0].(type) {
		case *influxql.VarRef:
			return &function{
				Ref:  ref,
				call: expr,
			}, nil
		case *influxql.Wildcard:
			return nil, errors.New("unimplemented: wildcard function")
		case *influxql.RegexLiteral:
			return nil, errors.New("unimplemented: wildcard regex function")
		default:
			return nil, fmt.Errorf("expected field argument in %s()", expr.Name)
		}
	case "percentile":
		if exp, got := 2, len(expr.Args); exp != got {
			return nil, fmt.Errorf("invalid number of arguments for %s, expected %d, got %d", expr.Name, exp, got)
		}

		var functionRef *influxql.VarRef

		switch ref := expr.Args[0].(type) {
		case *influxql.VarRef:
			functionRef = ref
		case *influxql.Wildcard:
			return nil, errors.New("unimplemented: wildcard function")
		case *influxql.RegexLiteral:
			return nil, errors.New("unimplemented: wildcard regex function")
		default:
			return nil, fmt.Errorf("expected field argument in %s()", expr.Name)
		}

		switch expr.Args[1].(type) {
		case *influxql.IntegerLiteral:
		case *influxql.NumberLiteral:
		default:
			return nil, fmt.Errorf("expected float argument in %s()", expr.Name)
		}

		return &function{
			Ref:  functionRef,
			call: expr,
		}, nil
	default:
		return nil, fmt.Errorf("unimplemented function: %q", expr.Name)
	}

}

// createFunctionCursor creates a new cursor that calls a function on one of the columns
// and returns the result.
func createFunctionCursor(t *transpilerState, call *influxql.Call, in cursor, normalize bool) (cursor, error) {
	cur := &functionCursor{
		call:   call,
		parent: in,
	}
	switch call.Name {
	case "count":
		value, ok := in.Value(call.Args[0])
		if !ok {
			return nil, fmt.Errorf("undefined variable: %s", call.Args[0])
		}
		cur.id = t.op("count", &universe.CountOpSpec{
			AggregateConfig: execute.AggregateConfig{
				Columns: []string{value},
			},
		}, in.ID())
		cur.value = value
		cur.exclude = map[influxql.Expr]struct{}{call.Args[0]: {}}
	case "min":
		value, ok := in.Value(call.Args[0])
		if !ok {
			return nil, fmt.Errorf("undefined variable: %s", call.Args[0])
		}
		cur.id = t.op("min", &universe.MinOpSpec{
			SelectorConfig: execute.SelectorConfig{
				Column: value,
			},
		}, in.ID())
		cur.value = value
		cur.exclude = map[influxql.Expr]struct{}{call.Args[0]: {}}
	case "max":
		value, ok := in.Value(call.Args[0])
		if !ok {
			return nil, fmt.Errorf("undefined variable: %s", call.Args[0])
		}
		cur.id = t.op("max", &universe.MaxOpSpec{
			SelectorConfig: execute.SelectorConfig{
				Column: value,
			},
		}, in.ID())
		cur.value = value
		cur.exclude = map[influxql.Expr]struct{}{call.Args[0]: {}}
	case "sum":
		value, ok := in.Value(call.Args[0])
		if !ok {
			return nil, fmt.Errorf("undefined variable: %s", call.Args[0])
		}
		cur.id = t.op("sum", &universe.SumOpSpec{
			AggregateConfig: execute.AggregateConfig{
				Columns: []string{value},
			},
		}, in.ID())
		cur.value = value
		cur.exclude = map[influxql.Expr]struct{}{call.Args[0]: {}}
	case "first":
		value, ok := in.Value(call.Args[0])
		if !ok {
			return nil, fmt.Errorf("undefined variable: %s", call.Args[0])
		}
		cur.id = t.op("first", &universe.FirstOpSpec{
			SelectorConfig: execute.SelectorConfig{
				Column: value,
			},
		}, in.ID())
		cur.value = value
		cur.exclude = map[influxql.Expr]struct{}{call.Args[0]: {}}
	case "last":
		value, ok := in.Value(call.Args[0])
		if !ok {
			return nil, fmt.Errorf("undefined variable: %s", call.Args[0])
		}
		cur.id = t.op("last", &universe.LastOpSpec{
			SelectorConfig: execute.SelectorConfig{
				Column: value,
			},
		}, in.ID())
		cur.value = value
		cur.exclude = map[influxql.Expr]struct{}{call.Args[0]: {}}
	case "mean":
		value, ok := in.Value(call.Args[0])
		if !ok {
			return nil, fmt.Errorf("undefined variable: %s", call.Args[0])
		}
		cur.id = t.op("mean", &universe.MeanOpSpec{
			AggregateConfig: execute.AggregateConfig{
				Columns: []string{value},
			},
		}, in.ID())
		cur.value = value
		cur.exclude = map[influxql.Expr]struct{}{call.Args[0]: {}}
	case "median":
		value, ok := in.Value(call.Args[0])
		if !ok {
			return nil, fmt.Errorf("undefined variable: %s", call.Args[0])
		}
		cur.id = t.op("median", &universe.PercentileOpSpec{
			Percentile:  0.5,
			Compression: 0,
			Method:      "exact_mean",
			AggregateConfig: execute.AggregateConfig{
				Columns: []string{value},
			},
		}, in.ID())
		cur.value = value
		cur.exclude = map[influxql.Expr]struct{}{call.Args[0]: {}}
	case "percentile":
		if len(call.Args) != 2 {
			return nil, errors.New("percentile function requires two arguments field_key and N")
		}

		fieldName, ok := in.Value(call.Args[0])
		if !ok {
			return nil, fmt.Errorf("undefined variable: %s", call.Args[0])
		}

		var percentile float64
		switch arg := call.Args[1].(type) {
		case *influxql.NumberLiteral:
			percentile = arg.Val / 100.0
		case *influxql.IntegerLiteral:
			percentile = float64(arg.Val) / 100.0
		default:
			return nil, errors.New("argument N must be a float type")
		}

		if percentile < 0 || percentile > 1 {
			return nil, errors.New("argument N must be between 0 and 100")
		}

		cur.id = t.op("percentile", &universe.PercentileOpSpec{
			Percentile:  percentile,
			Compression: 0,
			Method:      "exact_selector",
			AggregateConfig: execute.AggregateConfig{
				Columns: []string{fieldName},
			},
		}, in.ID())
		cur.value = fieldName
		cur.exclude = map[influxql.Expr]struct{}{call.Args[0]: {}}
	default:
		return nil, fmt.Errorf("unimplemented function: %q", call.Name)
	}

	// If we have been told to normalize the time, we do it here.
	if normalize {
		if influxql.IsSelector(call) {
			cur.id = t.op("drop", &universe.DropOpSpec{
				Columns: []string{execute.DefaultTimeColLabel},
			}, cur.id)
		}
		cur.id = t.op("duplicate", &universe.DuplicateOpSpec{
			Column: execute.DefaultStartColLabel,
			As:     execute.DefaultTimeColLabel,
		}, cur.id)
	}
	return cur, nil
}

type functionCursor struct {
	id      flux.OperationID
	call    *influxql.Call
	value   string
	exclude map[influxql.Expr]struct{}
	parent  cursor
}

func (c *functionCursor) ID() flux.OperationID {
	return c.id
}

func (c *functionCursor) Keys() []influxql.Expr {
	keys := []influxql.Expr{c.call}
	if a := c.parent.Keys(); len(a) > 0 {
		for _, e := range a {
			if _, ok := c.exclude[e]; ok {
				continue
			}
			keys = append(keys, e)
		}
	}
	return keys
}

func (c *functionCursor) Value(expr influxql.Expr) (string, bool) {
	if expr == c.call {
		return c.value, true
	} else if _, ok := c.exclude[expr]; ok {
		return "", false
	}
	return c.parent.Value(expr)
}
