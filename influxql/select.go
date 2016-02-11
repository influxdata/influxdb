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

	// Determine auxiliary fields to be selected.
	opt.Aux = make([]string, 0, len(info.refs))
	for ref := range info.refs {
		opt.Aux = append(opt.Aux, ref.Val)
	}
	sort.Strings(opt.Aux)

	// If there are multiple auxilary fields and no calls then construct an aux iterator.
	if len(info.calls) == 0 && len(info.refs) > 0 {
		return buildAuxIterators(stmt.Fields, ic, opt)
	}

	// Include auxiliary fields from top() and bottom()
	extraFields := 0
	for call := range info.calls {
		if call.Name == "top" || call.Name == "bottom" {
			for i := 1; i < len(call.Args)-1; i++ {
				ref := call.Args[i].(*VarRef)
				opt.Aux = append(opt.Aux, ref.Val)
				extraFields++
			}
		}
	}

	fields := stmt.Fields
	if extraFields > 0 {
		// Rebuild the list of fields if any extra fields are being implicitly added
		fields = make([]*Field, 0, len(stmt.Fields)+extraFields)
		for _, f := range stmt.Fields {
			fields = append(fields, f)
			switch expr := f.Expr.(type) {
			case *Call:
				if expr.Name == "top" || expr.Name == "bottom" {
					for i := 1; i < len(expr.Args)-1; i++ {
						fields = append(fields, &Field{Expr: expr.Args[i]})
					}
				}
			}
		}
	}

	return buildFieldIterators(fields, ic, opt)
}

// buildAuxIterators creates a set of iterators from a single combined auxilary iterator.
func buildAuxIterators(fields Fields, ic IteratorCreator, opt IteratorOptions) ([]Iterator, error) {
	// Create iterator to read auxilary fields.
	input, err := ic.CreateIterator(opt)
	if err != nil {
		return nil, err
	}

	// Filter out duplicate rows, if required.
	if opt.Dedupe {
		input = NewDedupeIterator(input)
	}

	// Apply limit & offset.
	if opt.Limit > 0 || opt.Offset > 0 {
		input = NewLimitIterator(input, opt)
	}

	seriesKeys, err := ic.SeriesKeys(opt)
	if err != nil {
		input.Close()
		return nil, err
	}

	// Wrap in an auxilary iterator to separate the fields.
	aitr := NewAuxIterator(input, seriesKeys, opt)

	// Generate iterators for each field.
	itrs := make([]Iterator, len(fields))
	for i, f := range fields {
		expr := Reduce(f.Expr, nil)
		switch expr := expr.(type) {
		case *VarRef:
			itrs[i] = aitr.Iterator(expr.Val)
		case *BinaryExpr:
			itr, err := buildExprIterator(expr, aitr, opt)
			if err != nil {
				return nil, fmt.Errorf("error constructing iterator for field '%s': %s", f.String(), err)
			}
			itrs[i] = itr
		default:
			panic("unreachable")
		}
	}
	aitr.Start()

	// Drain primary aux iterator since there is no reader for it.
	go drainIterator(aitr)

	return itrs, nil
}

// buildFieldIterators creates an iterator for each field expression.
func buildFieldIterators(fields Fields, ic IteratorCreator, opt IteratorOptions) ([]Iterator, error) {
	// Create iterators from fields against the iterator creator.
	itrs := make([]Iterator, len(fields))

	if err := func() error {
		hasAuxFields := false

		var input Iterator
		for i, f := range fields {
			// Build iterators for calls first and save the iterator.
			// We do this so we can keep the ordering provided by the user, but
			// still build the Call's iterator first.
			if ContainsVarRef(f.Expr) {
				hasAuxFields = true
				continue
			}

			expr := Reduce(f.Expr, nil)
			itr, err := buildExprIterator(expr, ic, opt)
			if err != nil {
				return err
			}
			itrs[i] = itr
			input = itr
		}

		if input == nil || !hasAuxFields {
			return nil
		}

		seriesKeys, err := ic.SeriesKeys(opt)
		if err != nil {
			return err
		}

		// Build the aux iterators. Previous validation should ensure that only one
		// call was present so we build an AuxIterator from that input.
		aitr := NewAuxIterator(input, seriesKeys, opt)
		for i, f := range fields {
			if itrs[i] != nil {
				itrs[i] = aitr
				continue
			}

			expr := Reduce(f.Expr, nil)
			itr, err := buildExprIterator(expr, aitr, opt)
			if err != nil {
				return err
			}
			itrs[i] = itr
		}
		aitr.Start()
		return nil

	}(); err != nil {
		Iterators(Iterators(itrs).filterNonNil()).Close()
		return nil, err
	}

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

		var err error
		var itr Iterator
		switch expr.Name {
		case "count":
			switch arg := expr.Args[0].(type) {
			case *Call:
				if arg.Name == "distinct" {
					input, err := buildExprIterator(arg, ic, opt)
					if err != nil {
						return nil, err
					}
					itr = newCountIterator(input, opt)
				}
			default:
				itr, err = ic.CreateIterator(opt)
			}
		case "min", "max", "sum", "first", "last":
			itr, err = ic.CreateIterator(opt)
		case "distinct":
			input, err := buildExprIterator(expr.Args[0].(*VarRef), ic, opt)
			if err != nil {
				return nil, err
			}
			return NewDistinctIterator(input, opt), nil
		case "mean":
			// OPTIMIZE(benbjohnson): convert to map/reduce
			input, err := buildExprIterator(expr.Args[0].(*VarRef), ic, opt)
			if err != nil {
				return nil, err
			}
			itr = newMeanIterator(input, opt)
		case "median":
			input, err := buildExprIterator(expr.Args[0].(*VarRef), ic, opt)
			if err != nil {
				return nil, err
			}
			itr = newMedianIterator(input, opt)
		case "stddev":
			input, err := buildExprIterator(expr.Args[0].(*VarRef), ic, opt)
			if err != nil {
				return nil, err
			}
			itr = newStddevIterator(input, opt)
		case "spread":
			// OPTIMIZE(benbjohnson): convert to map/reduce
			input, err := buildExprIterator(expr.Args[0].(*VarRef), ic, opt)
			if err != nil {
				return nil, err
			}
			itr = newSpreadIterator(input, opt)
		case "top":
			var tags []int
			if len(expr.Args) < 2 {
				return nil, fmt.Errorf("top() requires 2 or more arguments, got %d", len(expr.Args))
			} else if len(expr.Args) > 2 {
				// We need to find the indices of where the tag values are stored in Aux
				// This section is O(n^2), but for what should be a low value.
				for i := 1; i < len(expr.Args)-1; i++ {
					ref := expr.Args[i].(*VarRef)
					for index, name := range opt.Aux {
						if name == ref.Val {
							tags = append(tags, index)
							break
						}
					}
				}
			}

			input, err := buildExprIterator(expr.Args[0].(*VarRef), ic, opt)
			if err != nil {
				return nil, err
			}
			n := expr.Args[len(expr.Args)-1].(*NumberLiteral)
			itr = newTopIterator(input, opt, n, tags)
		case "bottom":
			var tags []int
			if len(expr.Args) < 2 {
				return nil, fmt.Errorf("bottom() requires 2 or more arguments, got %d", len(expr.Args))
			} else if len(expr.Args) > 2 {
				// We need to find the indices of where the tag values are stored in Aux
				// This section is O(n^2), but for what should be a low value.
				for i := 1; i < len(expr.Args)-1; i++ {
					ref := expr.Args[i].(*VarRef)
					for index, name := range opt.Aux {
						if name == ref.Val {
							tags = append(tags, index)
							break
						}
					}
				}
			}

			input, err := buildExprIterator(expr.Args[0].(*VarRef), ic, opt)
			if err != nil {
				return nil, err
			}
			n := expr.Args[len(expr.Args)-1].(*NumberLiteral)
			itr = newBottomIterator(input, opt, n, tags)
		case "percentile":
			input, err := buildExprIterator(expr.Args[0].(*VarRef), ic, opt)
			if err != nil {
				return nil, err
			}
			percentile := expr.Args[1].(*NumberLiteral).Val
			itr = newPercentileIterator(input, opt, percentile)
		case "derivative", "non_negative_derivative":
			input, err := buildExprIterator(expr.Args[0], ic, opt)
			if err != nil {
				return nil, err
			}

			interval := opt.DerivativeInterval()
			isNonNegative := (expr.Name == "non_negative_derivative")

			// Derivatives do not use GROUP BY intervals or time constraints, so clear these options.
			opt.Interval = Interval{}
			opt.StartTime, opt.EndTime = MinTime, MaxTime
			return newDerivativeIterator(input, opt, interval, isNonNegative), nil
		default:
			panic(fmt.Sprintf("unsupported call: %s", expr.Name))
		}

		if err != nil {
			return nil, err
		}

		if !opt.Interval.IsZero() && opt.Fill != NoFill {
			itr = NewFillIterator(itr, expr, opt)
		}
		return itr, nil
	case *BinaryExpr:
		if rhs, ok := expr.RHS.(Literal); ok {
			// The right hand side is a literal. It is more common to have the RHS be a literal,
			// so we check that one first and have this be the happy path.
			if lhs, ok := expr.LHS.(Literal); ok {
				// We have two literals that couldn't be combined by Reduce.
				return nil, fmt.Errorf("unable to construct an iterator from two literals: LHS: %T, RHS: %T", lhs, rhs)
			}

			lhs, err := buildExprIterator(expr.LHS, ic, opt)
			if err != nil {
				return nil, err
			}
			return buildRHSTransformIterator(lhs, rhs, expr.Op, ic, opt)
		} else if lhs, ok := expr.LHS.(Literal); ok {
			rhs, err := buildExprIterator(expr.RHS, ic, opt)
			if err != nil {
				return nil, err
			}
			return buildLHSTransformIterator(lhs, rhs, expr.Op, ic, opt)
		} else {
			// We have two iterators. Combine them into a single iterator.
			lhs, err := buildExprIterator(expr.LHS, ic, opt)
			if err != nil {
				return nil, err
			}
			rhs, err := buildExprIterator(expr.RHS, ic, opt)
			if err != nil {
				return nil, err
			}
			return buildTransformIterator(lhs, rhs, expr.Op, ic, opt)
		}
	case *ParenExpr:
		return buildExprIterator(expr.Expr, ic, opt)
	default:
		panic(fmt.Sprintf("invalid expression type: %T", expr)) // FIXME
	}
}

func buildRHSTransformIterator(lhs Iterator, rhs Literal, op Token, ic IteratorCreator, opt IteratorOptions) (Iterator, error) {
	fn := binaryExprFunc(iteratorDataType(lhs), op)
	switch fn := fn.(type) {
	case func(float64, float64) float64:
		input, ok := lhs.(FloatIterator)
		if !ok {
			return nil, fmt.Errorf("type mismatch, expected rhs to be FloatIterator, got %T", rhs)
		}
		lit, ok := rhs.(*NumberLiteral)
		if !ok {
			return nil, fmt.Errorf("type mismatch, expected lhs to be NumberLiteral, got %T", lhs)
		}
		return &floatTransformIterator{
			input: input,
			fn: func(p *FloatPoint) *FloatPoint {
				if p == nil {
					return nil
				}
				p.Value = fn(p.Value, lit.Val)
				return p
			},
		}, nil
	case func(int64, int64) int64:
		input, ok := lhs.(IntegerIterator)
		if !ok {
			return nil, fmt.Errorf("type mismatch, expected rhs to be IntegerIterator, got %T", rhs)
		}
		lit, ok := rhs.(*NumberLiteral)
		if !ok {
			return nil, fmt.Errorf("type mismatch, expected lhs to be NumberLiteral, got %T", lhs)
		}
		return &integerTransformIterator{
			input: input,
			fn: func(p *IntegerPoint) *IntegerPoint {
				if p == nil {
					return nil
				}
				p.Value = fn(p.Value, int64(lit.Val))
				return p
			},
		}, nil
	case func(float64, float64) bool:
		input, ok := lhs.(FloatIterator)
		if !ok {
			return nil, fmt.Errorf("type mismatch, expected lhs to be FloatIterator, got %T", lhs)
		}
		lit, ok := rhs.(*NumberLiteral)
		if !ok {
			return nil, fmt.Errorf("type mismatch, expected lhs to be NumberLiteral, got %T", rhs)
		}
		return &floatBoolTransformIterator{
			input: input,
			fn: func(p *FloatPoint) *BooleanPoint {
				if p == nil {
					return nil
				}
				return &BooleanPoint{
					Name:  p.Name,
					Tags:  p.Tags,
					Time:  p.Time,
					Value: fn(p.Value, lit.Val),
					Aux:   p.Aux,
				}
			},
		}, nil
	case func(int64, int64) bool:
		input, ok := lhs.(IntegerIterator)
		if !ok {
			return nil, fmt.Errorf("type mismatch, expected lhs to be IntegerIterator, got %T", lhs)
		}
		lit, ok := rhs.(*NumberLiteral)
		if !ok {
			return nil, fmt.Errorf("type mismatch, expected lhs to be NumberLiteral, got %T", rhs)
		}
		return &integerBoolTransformIterator{
			input: input,
			fn: func(p *IntegerPoint) *BooleanPoint {
				if p == nil {
					return nil
				}
				return &BooleanPoint{
					Name:  p.Name,
					Tags:  p.Tags,
					Time:  p.Time,
					Value: fn(p.Value, int64(lit.Val)),
					Aux:   p.Aux,
				}
			},
		}, nil
	}
	return nil, fmt.Errorf("unable to construct rhs transform iterator from %T and %T", lhs, rhs)
}

func buildLHSTransformIterator(lhs Literal, rhs Iterator, op Token, ic IteratorCreator, opt IteratorOptions) (Iterator, error) {
	fn := binaryExprFunc(iteratorDataType(rhs), op)
	switch fn := fn.(type) {
	case func(float64, float64) float64:
		lit, ok := lhs.(*NumberLiteral)
		if !ok {
			return nil, fmt.Errorf("type mismatch, expected lhs to be NumberLiteral, got %T", lhs)
		}
		input, ok := rhs.(FloatIterator)
		if !ok {
			return nil, fmt.Errorf("type mismatch, expected rhs to be FloatIterator, got %T", rhs)
		}
		return &floatTransformIterator{
			input: input,
			fn: func(p *FloatPoint) *FloatPoint {
				if p == nil {
					return nil
				}
				p.Value = fn(lit.Val, p.Value)
				return p
			},
		}, nil
	case func(int64, int64) int64:
		lit, ok := lhs.(*NumberLiteral)
		if !ok {
			return nil, fmt.Errorf("type mismatch, expected lhs to be NumberLiteral, got %T", lhs)
		}
		input, ok := rhs.(IntegerIterator)
		if !ok {
			return nil, fmt.Errorf("type mismatch, expected rhs to be IntegerIterator, got %T", rhs)
		}
		return &integerTransformIterator{
			input: input,
			fn: func(p *IntegerPoint) *IntegerPoint {
				if p == nil {
					return nil
				}
				p.Value = fn(int64(lit.Val), p.Value)
				return p
			},
		}, nil
	case func(float64, float64) bool:
		lit, ok := lhs.(*NumberLiteral)
		if !ok {
			return nil, fmt.Errorf("type mismatch, expected lhs to be NumberLiteral, got %T", lhs)
		}
		input, ok := rhs.(FloatIterator)
		if !ok {
			return nil, fmt.Errorf("type mismatch, expected lhs to be FloatIterator, got %T", rhs)
		}
		return &floatBoolTransformIterator{
			input: input,
			fn: func(p *FloatPoint) *BooleanPoint {
				if p == nil {
					return nil
				}
				return &BooleanPoint{
					Name:  p.Name,
					Tags:  p.Tags,
					Time:  p.Time,
					Value: fn(lit.Val, p.Value),
					Aux:   p.Aux,
				}
			},
		}, nil
	case func(int64, int64) bool:
		lit, ok := lhs.(*NumberLiteral)
		if !ok {
			return nil, fmt.Errorf("type mismatch, expected lhs to be NumberLiteral, got %T", lhs)
		}
		input, ok := rhs.(IntegerIterator)
		if !ok {
			return nil, fmt.Errorf("type mismatch, expected lhs to be IntegerIterator, got %T", rhs)
		}
		return &integerBoolTransformIterator{
			input: input,
			fn: func(p *IntegerPoint) *BooleanPoint {
				if p == nil {
					return nil
				}
				return &BooleanPoint{
					Name:  p.Name,
					Tags:  p.Tags,
					Time:  p.Time,
					Value: fn(int64(lit.Val), p.Value),
					Aux:   p.Aux,
				}
			},
		}, nil
	}
	return nil, fmt.Errorf("unable to construct lhs transform iterator from %T and %T", lhs, rhs)
}

func buildTransformIterator(lhs Iterator, rhs Iterator, op Token, ic IteratorCreator, opt IteratorOptions) (Iterator, error) {
	fn := binaryExprFunc(iteratorDataType(lhs), op)
	switch fn := fn.(type) {
	case func(float64, float64) float64:
		left, ok := lhs.(FloatIterator)
		if !ok {
			return nil, fmt.Errorf("type mismatch, expected lhs to be FloatIterator, got %T", lhs)
		}
		right, ok := rhs.(FloatIterator)
		if !ok {
			return nil, fmt.Errorf("type mismatch, expected lhs to be FloatIterator, got %T", rhs)
		}
		return &floatTransformIterator{
			input: left,
			fn: func(p *FloatPoint) *FloatPoint {
				if p == nil {
					return nil
				}
				p2 := right.Next()
				if p2 == nil {
					return nil
				}
				p.Value = fn(p.Value, p2.Value)
				return p
			},
		}, nil
	case func(int64, int64) int64:
		left, ok := lhs.(IntegerIterator)
		if !ok {
			return nil, fmt.Errorf("type mismatch, expected lhs to be IntegerIterator, got %T", lhs)
		}
		right, ok := rhs.(IntegerIterator)
		if !ok {
			return nil, fmt.Errorf("type mismatch, expected lhs to be IntegerIterator, got %T", rhs)
		}
		return &integerTransformIterator{
			input: left,
			fn: func(p *IntegerPoint) *IntegerPoint {
				if p == nil {
					return nil
				}
				p2 := right.Next()
				if p2 == nil {
					return nil
				}
				p.Value = fn(p.Value, p2.Value)
				return p
			},
		}, nil
	case func(float64, float64) bool:
		left, ok := lhs.(FloatIterator)
		if !ok {
			return nil, fmt.Errorf("type mismatch, expected lhs to be FloatIterator, got %T", lhs)
		}
		right, ok := rhs.(FloatIterator)
		if !ok {
			return nil, fmt.Errorf("type mismatch, expected lhs to be FloatIterator, got %T", rhs)
		}
		return &floatBoolTransformIterator{
			input: left,
			fn: func(p *FloatPoint) *BooleanPoint {
				if p == nil {
					return nil
				}
				p2 := right.Next()
				if p2 == nil {
					return nil
				}
				return &BooleanPoint{
					Name:  p.Name,
					Tags:  p.Tags,
					Time:  p.Time,
					Value: fn(p.Value, p2.Value),
					Aux:   p.Aux,
				}
			},
		}, nil
	case func(int64, int64) bool:
		left, ok := lhs.(IntegerIterator)
		if !ok {
			return nil, fmt.Errorf("type mismatch, expected lhs to be IntegerIterator, got %T", lhs)
		}
		right, ok := rhs.(IntegerIterator)
		if !ok {
			return nil, fmt.Errorf("type mismatch, expected lhs to be IntegerIterator, got %T", rhs)
		}
		return &integerBoolTransformIterator{
			input: left,
			fn: func(p *IntegerPoint) *BooleanPoint {
				if p == nil {
					return nil
				}
				p2 := right.Next()
				if p2 == nil {
					return nil
				}
				return &BooleanPoint{
					Name:  p.Name,
					Tags:  p.Tags,
					Time:  p.Time,
					Value: fn(p.Value, p2.Value),
					Aux:   p.Aux,
				}
			},
		}, nil
	}
	return nil, fmt.Errorf("unable to construct transform iterator from %T and %T", lhs, rhs)
}

func iteratorDataType(itr Iterator) DataType {
	switch itr.(type) {
	case FloatIterator:
		return Float
	case IntegerIterator:
		return Integer
	case StringIterator:
		return String
	case BooleanIterator:
		return Boolean
	default:
		return Unknown
	}
}

func binaryExprFunc(typ DataType, op Token) interface{} {
	var fn interface{}
	switch typ {
	case Float:
		fn = floatBinaryExprFunc(op)
	case Integer:
		fn = integerBinaryExprFunc(op)
	}
	return fn
}

func floatBinaryExprFunc(op Token) interface{} {
	switch op {
	case ADD:
		return func(lhs, rhs float64) float64 { return lhs + rhs }
	case SUB:
		return func(lhs, rhs float64) float64 { return lhs - rhs }
	case MUL:
		return func(lhs, rhs float64) float64 { return lhs * rhs }
	case DIV:
		return func(lhs, rhs float64) float64 {
			if rhs == 0 {
				return float64(0)
			}
			return lhs / rhs
		}
	case EQ:
		return func(lhs, rhs float64) bool { return lhs == rhs }
	case NEQ:
		return func(lhs, rhs float64) bool { return lhs != rhs }
	case LT:
		return func(lhs, rhs float64) bool { return lhs < rhs }
	case LTE:
		return func(lhs, rhs float64) bool { return lhs <= rhs }
	case GT:
		return func(lhs, rhs float64) bool { return lhs > rhs }
	case GTE:
		return func(lhs, rhs float64) bool { return lhs >= rhs }
	}
	return nil
}

func integerBinaryExprFunc(op Token) interface{} {
	switch op {
	case ADD:
		return func(lhs, rhs int64) int64 { return lhs + rhs }
	case SUB:
		return func(lhs, rhs int64) int64 { return lhs - rhs }
	case MUL:
		return func(lhs, rhs int64) int64 { return lhs * rhs }
	case DIV:
		return func(lhs, rhs int64) int64 {
			if rhs == 0 {
				return int64(0)
			}
			return lhs / rhs
		}
	case EQ:
		return func(lhs, rhs int64) bool { return lhs == rhs }
	case NEQ:
		return func(lhs, rhs int64) bool { return lhs != rhs }
	case LT:
		return func(lhs, rhs int64) bool { return lhs < rhs }
	case LTE:
		return func(lhs, rhs int64) bool { return lhs <= rhs }
	case GT:
		return func(lhs, rhs int64) bool { return lhs > rhs }
	case GTE:
		return func(lhs, rhs int64) bool { return lhs >= rhs }
	}
	return nil
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
