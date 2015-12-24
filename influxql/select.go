package influxql

import (
	"errors"
	"fmt"
	"sort"
)

// Select executes stmt against ic and returns a list of iterators to stream from.
//
// Statements should have all rewriting performed before calling select(). This
// includes wildcard and source expansion.
func Select(stmt *SelectStatement, ic IteratorCreator) ([]Iterator, error) {
	// Determine base options for iterators.
	opt, err := newIteratorOptionsStmt(stmt)
	if err != nil {
		return nil, err
	}

	// Retrieve refs for each call and var ref.
	info := newSelectInfo(stmt)
	if len(info.calls) > 1 && len(info.refs) > 0 {
		return nil, errors.New("cannot select fields when selecting multiple aggregates")
	}

	// Determine auxilary fields to be selected.
	opt.Aux = make([]string, 0, len(info.refs))
	for ref := range info.refs {
		opt.Aux = append(opt.Aux, ref.Val)
	}
	sort.Strings(opt.Aux)

	// If there are multiple auxilary fields and no calls then construct an aux iterator.
	if len(info.calls) == 0 && len(info.refs) > 0 {
		return buildAuxIterators(stmt.Fields, ic, opt)
	}

	return buildExprIterators(stmt.Fields, ic, opt)
}

// buildAuxIterators creates a set of iterators from a single combined auxilary iterator.
func buildAuxIterators(fields Fields, ic IteratorCreator, opt IteratorOptions) ([]Iterator, error) {
	// Create iteartor to read auxilary fields.
	input, err := ic.CreateIterator(opt)
	if err != nil {
		return nil, err
	}

	// Apply limit & offset.
	if opt.Limit > 0 || opt.Offset > 0 {
		input = NewLimitIterator(input, opt)
	}

	// Wrap in an auxilary iterator to separate the fields.
	aitr := NewAuxIterator(input, opt)

	// Generate iterators for each field.
	itrs := make([]Iterator, len(fields))
	for i, f := range fields {
		switch expr := f.Expr.(type) {
		case *VarRef:
			itrs[i] = aitr.Iterator(expr.Val)
		case *BinaryExpr:
			// FIXME(benbjohnson): Support binary operators.
			panic("binary expressions not currently supported")
		default:
			panic("unreachable")
		}
	}

	// Drain primary aux iterator since there is no reader for it.
	go drainIterator(aitr)

	return itrs, nil
}

// buildExprIterators creates an iterator for each field expression.
func buildExprIterators(fields Fields, ic IteratorCreator, opt IteratorOptions) ([]Iterator, error) {
	// Create iterators from fields against the iterator creator.
	itrs := make([]Iterator, 0, len(fields))
	if err := func() error {
		for _, f := range fields {
			itr, err := buildExprIterator(f.Expr, ic, opt)
			if err != nil {
				return err
			}
			itrs = append(itrs, itr)
		}
		return nil

	}(); err != nil {
		Iterators(itrs).Close()
		return nil, err
	}

	// Join iterators together on time.
	itrs = Join(itrs)

	// If there is a limit or offset then apply it.
	if opt.Limit > 0 || opt.Offset > 0 {
		for i := range itrs {
			itrs[i] = NewLimitIterator(itrs[i], opt)
		}
	}

	return itrs, nil
}

// buildExprIterator creates an iterator for an expression.
func buildExprIterator(expr Expr, ic IteratorCreator, opt IteratorOptions) (Iterator, error) {
	opt.Expr = expr

	switch expr := expr.(type) {
	case *VarRef:
		return ic.CreateIterator(opt)
	case *Call:
		// FIXME(benbjohnson): Validate that only calls with 1 arg are passed to IC.

		switch expr.Name {
		case "count":
			switch arg := expr.Args[0].(type) {
			case *Call:
				if arg.Name == "distinct" {
					input, err := buildExprIterator(arg, ic, opt)
					if err != nil {
						return nil, err
					}
					return newCountIterator(input, opt), nil
				}
			}
			return ic.CreateIterator(opt)
		case "min", "max", "sum", "first", "last":
			return ic.CreateIterator(opt)
		case "distinct":
			input, err := buildExprIterator(expr.Args[0].(*VarRef), ic, opt)
			if err != nil {
				return nil, err
			}
			return newDistinctIterator(input, opt), nil
		case "mean":
			// OPTIMIZE(benbjohnson): convert to map/reduce
			input, err := buildExprIterator(expr.Args[0].(*VarRef), ic, opt)
			if err != nil {
				return nil, err
			}
			return newMeanIterator(input, opt), nil
		case "median":
			input, err := buildExprIterator(expr.Args[0].(*VarRef), ic, opt)
			if err != nil {
				return nil, err
			}
			return newMedianIterator(input, opt), nil
		case "stddev":
			input, err := buildExprIterator(expr.Args[0].(*VarRef), ic, opt)
			if err != nil {
				return nil, err
			}
			return newStddevIterator(input, opt), nil
		case "spread":
			// OPTIMIZE(benbjohnson): convert to map/reduce
			input, err := buildExprIterator(expr.Args[0].(*VarRef), ic, opt)
			if err != nil {
				return nil, err
			}
			return newSpreadIterator(input, opt), nil
		case "top":
			panic("FIXME: wrap top reduce slice iterator over raw iterator")
		case "bottom":
			panic("FIXME: wrap bottom reduce slice iterator over raw iterator")
		case "percentile":
			input, err := buildExprIterator(expr.Args[0].(*VarRef), ic, opt)
			if err != nil {
				return nil, err
			}
			percentile := expr.Args[1].(*NumberLiteral).Val
			return newPercentileIterator(input, opt, percentile), nil
		case "derivative":
			panic("FIXME: wrap derivative reduce slice iterator over raw iterator")
		case "non_negative_derivative":
			panic("FIXME: wrap derivative reduce slice iterator over raw iterator")
		default:
			panic(fmt.Sprintf("unsupported call: %s", expr.Name))
		}
	default:
		panic(fmt.Sprintf("invalid expression type: %T", expr)) // FIXME
	}
}

// stringSetSlice returns a sorted slice of keys from a string set.
func stringSetSlice(m map[string]struct{}) []string {
	if m == nil {
		return nil
	}

	a := make([]string, 0, len(m))
	for k := range m {
		a = append(a, k)
	}
	sort.Strings(a)
	return a
}
