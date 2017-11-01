package query

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"

	"github.com/influxdata/influxdb/pkg/tracing"
	"github.com/influxdata/influxql"
)

// SelectOptions are options that customize the select call.
type SelectOptions struct {
	// Authorizer is used to limit access to data
	Authorizer Authorizer

	// Node to exclusively read from.
	// If zero, all nodes are used.
	NodeID uint64

	// An optional channel that, if closed, signals that the select should be
	// interrupted.
	InterruptCh <-chan struct{}

	// Maximum number of concurrent series.
	MaxSeriesN int

	// Maximum number of buckets for a statement.
	MaxBucketsN int
}

// ShardMapper retrieves and maps shards into an IteratorCreator that can later be
// used for executing queries.
type ShardMapper interface {
	MapShards(sources influxql.Sources, t influxql.TimeRange, opt SelectOptions) (ShardGroup, error)
}

// ShardGroup represents a shard or a collection of shards that can be accessed
// for creating iterators.
// When creating iterators, the resource used for reading the iterators should be
// separate from the resource used to map the shards. When the ShardGroup is closed,
// it should not close any resources associated with the created Iterator. Those
// resources belong to the Iterator and will be closed when the Iterator itself is
// closed.
// The query engine operates under this assumption and will close the shard group
// after creating the iterators, but before the iterators are actually read.
type ShardGroup interface {
	IteratorCreator
	influxql.FieldMapper
	io.Closer
}

// Select is a prepared statement that is ready to be executed.
type PreparedStatement interface {
	// Select creates the Iterators that will be used to read the query.
	Select(ctx context.Context) ([]Iterator, []string, error)

	// Explain outputs the explain plan for this statement.
	Explain() (string, error)

	// Close closes the resources associated with this prepared statement.
	// This must be called as the mapped shards may hold open resources such
	// as network connections.
	Close() error
}

// Prepare will compile the statement with the default compile options and
// then prepare the query.
func Prepare(stmt *influxql.SelectStatement, shardMapper ShardMapper, opt SelectOptions) (PreparedStatement, error) {
	c, err := Compile(stmt, CompileOptions{})
	if err != nil {
		return nil, err
	}
	return c.Prepare(shardMapper, opt)
}

// Select compiles, prepares, and then initiates execution of the query using the
// default compile options.
func Select(ctx context.Context, stmt *influxql.SelectStatement, shardMapper ShardMapper, opt SelectOptions) ([]Iterator, []string, error) {
	s, err := Prepare(stmt, shardMapper, opt)
	if err != nil {
		return nil, nil, err
	}
	// Must be deferred so it runs after Select.
	defer s.Close()
	return s.Select(ctx)
}

type preparedStatement struct {
	stmt *influxql.SelectStatement
	opt  IteratorOptions
	ic   interface {
		IteratorCreator
		io.Closer
	}
	columns []string
}

func (p *preparedStatement) Select(ctx context.Context) ([]Iterator, []string, error) {
	itrs, err := buildIterators(ctx, p.stmt, p.ic, p.opt)
	if err != nil {
		return nil, nil, err
	}
	return itrs, p.columns, nil
}

func (p *preparedStatement) Close() error {
	return p.ic.Close()
}

func buildIterators(ctx context.Context, stmt *influxql.SelectStatement, ic IteratorCreator, opt IteratorOptions) ([]Iterator, error) {
	span := tracing.SpanFromContext(ctx)
	// Retrieve refs for each call and var ref.
	info := newSelectInfo(stmt)
	if len(info.calls) > 1 && len(info.refs) > 0 {
		return nil, errors.New("cannot select fields when selecting multiple aggregates")
	}

	// Determine auxiliary fields to be selected.
	opt.Aux = make([]influxql.VarRef, 0, len(info.refs))
	for ref := range info.refs {
		opt.Aux = append(opt.Aux, *ref)
	}
	sort.Sort(influxql.VarRefs(opt.Aux))

	// If there are multiple auxilary fields and no calls then construct an aux iterator.
	if len(info.calls) == 0 && len(info.refs) > 0 {
		if span != nil {
			span = span.StartSpan("auxiliary_iterators")
			defer span.Finish()

			span.SetLabels("statement", stmt.String())
			ctx = tracing.NewContextWithSpan(ctx, span)
		}
		return buildAuxIterators(ctx, stmt.Fields, ic, stmt.Sources, opt)
	}

	if span != nil {
		span = span.StartSpan("field_iterators")
		defer span.Finish()

		span.SetLabels("statement", stmt.String())
		ctx = tracing.NewContextWithSpan(ctx, span)
	}

	// Include auxiliary fields from top() and bottom() when not writing the results.
	fields := stmt.Fields
	if stmt.Target == nil {
		extraFields := 0
		for call := range info.calls {
			if call.Name == "top" || call.Name == "bottom" {
				for i := 1; i < len(call.Args)-1; i++ {
					ref := call.Args[i].(*influxql.VarRef)
					opt.Aux = append(opt.Aux, *ref)
					extraFields++
				}
			}
		}

		if extraFields > 0 {
			// Rebuild the list of fields if any extra fields are being implicitly added
			fields = make([]*influxql.Field, 0, len(stmt.Fields)+extraFields)
			for _, f := range stmt.Fields {
				fields = append(fields, f)
				switch expr := f.Expr.(type) {
				case *influxql.Call:
					if expr.Name == "top" || expr.Name == "bottom" {
						for i := 1; i < len(expr.Args)-1; i++ {
							fields = append(fields, &influxql.Field{Expr: expr.Args[i]})
						}
					}
				}
			}
		}
	}

	// Determine if there is one call and it is a selector.
	selector := false
	if len(info.calls) == 1 {
		for call := range info.calls {
			selector = influxql.IsSelector(call)
		}
	}

	return buildFieldIterators(ctx, fields, ic, stmt.Sources, opt, selector, stmt.Target != nil)
}

// buildAuxIterators creates a set of iterators from a single combined auxiliary iterator.
func buildAuxIterators(ctx context.Context, fields influxql.Fields, ic IteratorCreator, sources influxql.Sources, opt IteratorOptions) ([]Iterator, error) {
	// Create the auxiliary iterators for each source.
	inputs := make([]Iterator, 0, len(sources))
	if err := func() error {
		for _, source := range sources {
			switch source := source.(type) {
			case *influxql.Measurement:
				input, err := ic.CreateIterator(ctx, source, opt)
				if err != nil {
					return err
				}
				inputs = append(inputs, input)
			case *influxql.SubQuery:
				b := subqueryBuilder{
					ic:   ic,
					stmt: source.Statement,
				}

				input, err := b.buildAuxIterator(ctx, opt)
				if err != nil {
					return err
				}
				inputs = append(inputs, input)
			}
		}
		return nil
	}(); err != nil {
		Iterators(inputs).Close()
		return nil, err
	}

	// Merge iterators to read auxilary fields.
	input, err := Iterators(inputs).Merge(opt)
	if err != nil {
		Iterators(inputs).Close()
		return nil, err
	} else if input == nil {
		input = &nilFloatIterator{}
	}

	// Filter out duplicate rows, if required.
	if opt.Dedupe {
		// If there is no group by and it is a float iterator, see if we can use a fast dedupe.
		if itr, ok := input.(FloatIterator); ok && len(opt.Dimensions) == 0 {
			if sz := len(fields); sz > 0 && sz < 3 {
				input = newFloatFastDedupeIterator(itr)
			} else {
				input = NewDedupeIterator(itr)
			}
		} else {
			input = NewDedupeIterator(input)
		}
	}
	// Apply limit & offset.
	if opt.Limit > 0 || opt.Offset > 0 {
		input = NewLimitIterator(input, opt)
	}

	// Wrap in an auxiliary iterator to separate the fields.
	aitr := NewAuxIterator(input, opt)
	tryAddAuxIteratorToContext(ctx, aitr)

	// Generate iterators for each field.
	itrs := make([]Iterator, len(fields))
	if err := func() error {
		for i, f := range fields {
			expr := influxql.Reduce(f.Expr, nil)
			itr, err := buildAuxIterator(expr, aitr, opt)
			if err != nil {
				return err
			}
			itrs[i] = itr
		}
		return nil
	}(); err != nil {
		Iterators(Iterators(itrs).filterNonNil()).Close()
		aitr.Close()
		return nil, err
	}

	// Background the primary iterator since there is no reader for it.
	aitr.Background()

	return itrs, nil
}

// buildAuxIterator constructs an Iterator for an expression from an AuxIterator.
func buildAuxIterator(expr influxql.Expr, aitr AuxIterator, opt IteratorOptions) (Iterator, error) {
	switch expr := expr.(type) {
	case *influxql.VarRef:
		return aitr.Iterator(expr.Val, expr.Type), nil
	case *influxql.BinaryExpr:
		if rhs, ok := expr.RHS.(influxql.Literal); ok {
			// The right hand side is a literal. It is more common to have the RHS be a literal,
			// so we check that one first and have this be the happy path.
			if lhs, ok := expr.LHS.(influxql.Literal); ok {
				// We have two literals that couldn't be combined by Reduce.
				return nil, fmt.Errorf("unable to construct an iterator from two literals: LHS: %T, RHS: %T", lhs, rhs)
			}

			lhs, err := buildAuxIterator(expr.LHS, aitr, opt)
			if err != nil {
				return nil, err
			}
			return buildRHSTransformIterator(lhs, rhs, expr.Op, opt)
		} else if lhs, ok := expr.LHS.(influxql.Literal); ok {
			rhs, err := buildAuxIterator(expr.RHS, aitr, opt)
			if err != nil {
				return nil, err
			}
			return buildLHSTransformIterator(lhs, rhs, expr.Op, opt)
		} else {
			// We have two iterators. Combine them into a single iterator.
			lhs, err := buildAuxIterator(expr.LHS, aitr, opt)
			if err != nil {
				return nil, err
			}
			rhs, err := buildAuxIterator(expr.RHS, aitr, opt)
			if err != nil {
				return nil, err
			}
			return buildTransformIterator(lhs, rhs, expr.Op, opt)
		}
	case *influxql.ParenExpr:
		return buildAuxIterator(expr.Expr, aitr, opt)
	case *influxql.NilLiteral:
		return &nilFloatIterator{}, nil
	default:
		return nil, fmt.Errorf("invalid expression type: %T", expr)
	}
}

// buildFieldIterators creates an iterator for each field expression.
func buildFieldIterators(ctx context.Context, fields influxql.Fields, ic IteratorCreator, sources influxql.Sources, opt IteratorOptions, selector, writeMode bool) ([]Iterator, error) {
	// Create iterators from fields against the iterator creator.
	itrs := make([]Iterator, len(fields))
	span := tracing.SpanFromContext(ctx)

	if err := func() error {
		hasAuxFields := false

		var input Iterator
		for i, f := range fields {
			// Build iterators for calls first and save the iterator.
			// We do this so we can keep the ordering provided by the user, but
			// still build the Call's iterator first.
			if influxql.ContainsVarRef(f.Expr) {
				hasAuxFields = true
				continue
			}

			var localSpan *tracing.Span
			localContext := ctx

			if span != nil {
				localSpan = span.StartSpan("expression")
				localSpan.SetLabels("expr", f.Expr.String())
				localContext = tracing.NewContextWithSpan(ctx, localSpan)
			}

			expr := influxql.Reduce(f.Expr, nil)
			itr, err := buildExprIterator(localContext, expr, ic, sources, opt, selector, writeMode)

			if localSpan != nil {
				localSpan.Finish()
			}

			if err != nil {
				return err
			} else if itr == nil {
				itr = &nilFloatIterator{}
			}

			// If there is a limit or offset then apply it.
			if opt.Limit > 0 || opt.Offset > 0 {
				itr = NewLimitIterator(itr, opt)
			}
			itrs[i] = itr
			input = itr
		}

		if input == nil || !hasAuxFields {
			return nil
		}

		// Build the aux iterators. Previous validation should ensure that only one
		// call was present so we build an AuxIterator from that input.
		aitr := NewAuxIterator(input, opt)
		tryAddAuxIteratorToContext(ctx, aitr)

		for i, f := range fields {
			if itrs[i] != nil {
				itrs[i] = aitr
				continue
			}

			expr := influxql.Reduce(f.Expr, nil)
			itr, err := buildAuxIterator(expr, aitr, opt)
			if err != nil {
				return err
			} else if itr == nil {
				itr = &nilFloatIterator{}
			}
			itrs[i] = itr
		}
		aitr.Start()
		return nil

	}(); err != nil {
		Iterators(Iterators(itrs).filterNonNil()).Close()
		return nil, err
	}

	return itrs, nil
}

// buildExprIterator creates an iterator for an expression.
func buildExprIterator(ctx context.Context, expr influxql.Expr, ic IteratorCreator, sources influxql.Sources, opt IteratorOptions, selector, writeMode bool) (Iterator, error) {
	opt.Expr = expr
	b := exprIteratorBuilder{
		ic:        ic,
		sources:   sources,
		opt:       opt,
		selector:  selector,
		writeMode: writeMode,
	}

	switch expr := expr.(type) {
	case *influxql.VarRef:
		return b.buildVarRefIterator(ctx, expr)
	case *influxql.Call:
		return b.buildCallIterator(ctx, expr)
	case *influxql.BinaryExpr:
		return b.buildBinaryExprIterator(ctx, expr)
	case *influxql.ParenExpr:
		return buildExprIterator(ctx, expr.Expr, ic, sources, opt, selector, writeMode)
	case *influxql.NilLiteral:
		return &nilFloatIterator{}, nil
	default:
		return nil, fmt.Errorf("invalid expression type: %T", expr)
	}
}

type exprIteratorBuilder struct {
	ic        IteratorCreator
	sources   influxql.Sources
	opt       IteratorOptions
	selector  bool
	writeMode bool
}

func (b *exprIteratorBuilder) buildVarRefIterator(ctx context.Context, expr *influxql.VarRef) (Iterator, error) {
	inputs := make([]Iterator, 0, len(b.sources))
	if err := func() error {
		for _, source := range b.sources {
			switch source := source.(type) {
			case *influxql.Measurement:
				input, err := b.ic.CreateIterator(ctx, source, b.opt)
				if err != nil {
					return err
				}
				inputs = append(inputs, input)
			case *influxql.SubQuery:
				subquery := subqueryBuilder{
					ic:   b.ic,
					stmt: source.Statement,
				}

				input, err := subquery.buildVarRefIterator(ctx, expr, b.opt)
				if err != nil {
					return err
				}
				inputs = append(inputs, input)
			}
		}
		return nil
	}(); err != nil {
		Iterators(inputs).Close()
		return nil, err
	}

	// Variable references in this section will always go into some call
	// iterator. Combine it with a merge iterator.
	itr := NewMergeIterator(inputs, b.opt)
	if itr == nil {
		itr = &nilFloatIterator{}
	}

	if b.opt.InterruptCh != nil {
		itr = NewInterruptIterator(itr, b.opt.InterruptCh)
	}
	return itr, nil
}

func (b *exprIteratorBuilder) buildCallIterator(ctx context.Context, expr *influxql.Call) (Iterator, error) {
	// TODO(jsternberg): Refactor this. This section needs to die in a fire.
	opt := b.opt
	// Eliminate limits and offsets if they were previously set. These are handled by the caller.
	opt.Limit, opt.Offset = 0, 0
	switch expr.Name {
	case "distinct":
		opt.Ordered = true
		input, err := buildExprIterator(ctx, expr.Args[0].(*influxql.VarRef), b.ic, b.sources, opt, b.selector, false)
		if err != nil {
			return nil, err
		}
		input, err = NewDistinctIterator(input, opt)
		if err != nil {
			return nil, err
		}
		return NewIntervalIterator(input, opt), nil
	case "sample":
		opt.Ordered = true
		input, err := buildExprIterator(ctx, expr.Args[0], b.ic, b.sources, opt, b.selector, false)
		if err != nil {
			return nil, err
		}
		size := expr.Args[1].(*influxql.IntegerLiteral)

		return newSampleIterator(input, opt, int(size.Val))
	case "holt_winters", "holt_winters_with_fit":
		opt.Ordered = true
		input, err := buildExprIterator(ctx, expr.Args[0], b.ic, b.sources, opt, b.selector, false)
		if err != nil {
			return nil, err
		}
		h := expr.Args[1].(*influxql.IntegerLiteral)
		m := expr.Args[2].(*influxql.IntegerLiteral)

		includeFitData := "holt_winters_with_fit" == expr.Name

		interval := opt.Interval.Duration
		// Redefine interval to be unbounded to capture all aggregate results
		opt.StartTime = influxql.MinTime
		opt.EndTime = influxql.MaxTime
		opt.Interval = Interval{}

		return newHoltWintersIterator(input, opt, int(h.Val), int(m.Val), includeFitData, interval)
	case "derivative", "non_negative_derivative", "difference", "non_negative_difference", "moving_average", "elapsed":
		if !opt.Interval.IsZero() {
			if opt.Ascending {
				opt.StartTime -= int64(opt.Interval.Duration)
			} else {
				opt.EndTime += int64(opt.Interval.Duration)
			}
		}
		opt.Ordered = true

		input, err := buildExprIterator(ctx, expr.Args[0], b.ic, b.sources, opt, b.selector, false)
		if err != nil {
			return nil, err
		}

		switch expr.Name {
		case "derivative", "non_negative_derivative":
			interval := opt.DerivativeInterval()
			isNonNegative := (expr.Name == "non_negative_derivative")
			return newDerivativeIterator(input, opt, interval, isNonNegative)
		case "elapsed":
			interval := opt.ElapsedInterval()
			return newElapsedIterator(input, opt, interval)
		case "difference", "non_negative_difference":
			isNonNegative := (expr.Name == "non_negative_difference")
			return newDifferenceIterator(input, opt, isNonNegative)
		case "moving_average":
			n := expr.Args[1].(*influxql.IntegerLiteral)
			if n.Val > 1 && !opt.Interval.IsZero() {
				if opt.Ascending {
					opt.StartTime -= int64(opt.Interval.Duration) * (n.Val - 1)
				} else {
					opt.EndTime += int64(opt.Interval.Duration) * (n.Val - 1)
				}
			}
			return newMovingAverageIterator(input, int(n.Val), opt)
		}
		panic(fmt.Sprintf("invalid series aggregate function: %s", expr.Name))
	case "cumulative_sum":
		opt.Ordered = true
		input, err := buildExprIterator(ctx, expr.Args[0], b.ic, b.sources, opt, b.selector, false)
		if err != nil {
			return nil, err
		}
		return newCumulativeSumIterator(input, opt)
	case "integral":
		opt.Ordered = true
		input, err := buildExprIterator(ctx, expr.Args[0].(*influxql.VarRef), b.ic, b.sources, opt, false, false)
		if err != nil {
			return nil, err
		}
		interval := opt.IntegralInterval()
		return newIntegralIterator(input, opt, interval)
	case "top":
		if len(expr.Args) < 2 {
			return nil, fmt.Errorf("top() requires 2 or more arguments, got %d", len(expr.Args))
		}

		var input Iterator
		if len(expr.Args) > 2 {
			// Create a max iterator using the groupings in the arguments.
			dims := make(map[string]struct{}, len(expr.Args)-2+len(opt.GroupBy))
			for i := 1; i < len(expr.Args)-1; i++ {
				ref := expr.Args[i].(*influxql.VarRef)
				dims[ref.Val] = struct{}{}
			}
			for dim := range opt.GroupBy {
				dims[dim] = struct{}{}
			}

			call := &influxql.Call{
				Name: "max",
				Args: expr.Args[:1],
			}
			callOpt := opt
			callOpt.Expr = call
			callOpt.GroupBy = dims
			callOpt.Fill = influxql.NoFill

			builder := *b
			builder.opt = callOpt
			builder.selector = true
			builder.writeMode = false

			i, err := builder.callIterator(ctx, call, callOpt)
			if err != nil {
				return nil, err
			}
			input = i
		} else {
			// There are no arguments so do not organize the points by tags.
			builder := *b
			builder.opt.Expr = expr.Args[0]
			builder.selector = true
			builder.writeMode = false

			ref := expr.Args[0].(*influxql.VarRef)
			i, err := builder.buildVarRefIterator(ctx, ref)
			if err != nil {
				return nil, err
			}
			input = i
		}

		n := expr.Args[len(expr.Args)-1].(*influxql.IntegerLiteral)
		return newTopIterator(input, opt, int(n.Val), b.writeMode)
	case "bottom":
		if len(expr.Args) < 2 {
			return nil, fmt.Errorf("bottom() requires 2 or more arguments, got %d", len(expr.Args))
		}

		var input Iterator
		if len(expr.Args) > 2 {
			// Create a max iterator using the groupings in the arguments.
			dims := make(map[string]struct{}, len(expr.Args)-2)
			for i := 1; i < len(expr.Args)-1; i++ {
				ref := expr.Args[i].(*influxql.VarRef)
				dims[ref.Val] = struct{}{}
			}
			for dim := range opt.GroupBy {
				dims[dim] = struct{}{}
			}

			call := &influxql.Call{
				Name: "min",
				Args: expr.Args[:1],
			}
			callOpt := opt
			callOpt.Expr = call
			callOpt.GroupBy = dims
			callOpt.Fill = influxql.NoFill

			builder := *b
			builder.opt = callOpt
			builder.selector = true
			builder.writeMode = false

			i, err := builder.callIterator(ctx, call, callOpt)
			if err != nil {
				return nil, err
			}
			input = i
		} else {
			// There are no arguments so do not organize the points by tags.
			builder := *b
			builder.opt.Expr = expr.Args[0]
			builder.selector = true
			builder.writeMode = false

			ref := expr.Args[0].(*influxql.VarRef)
			i, err := builder.buildVarRefIterator(ctx, ref)
			if err != nil {
				return nil, err
			}
			input = i
		}

		n := expr.Args[len(expr.Args)-1].(*influxql.IntegerLiteral)
		return newBottomIterator(input, b.opt, int(n.Val), b.writeMode)
	}

	itr, err := func() (Iterator, error) {
		switch expr.Name {
		case "count":
			switch arg0 := expr.Args[0].(type) {
			case *influxql.Call:
				if arg0.Name == "distinct" {
					input, err := buildExprIterator(ctx, arg0, b.ic, b.sources, opt, b.selector, false)
					if err != nil {
						return nil, err
					}
					return newCountIterator(input, opt)
				}
			}
			fallthrough
		case "min", "max", "sum", "first", "last", "mean":
			return b.callIterator(ctx, expr, opt)
		case "median":
			opt.Ordered = true
			input, err := buildExprIterator(ctx, expr.Args[0].(*influxql.VarRef), b.ic, b.sources, opt, false, false)
			if err != nil {
				return nil, err
			}
			return newMedianIterator(input, opt)
		case "mode":
			input, err := buildExprIterator(ctx, expr.Args[0].(*influxql.VarRef), b.ic, b.sources, opt, false, false)
			if err != nil {
				return nil, err
			}
			return NewModeIterator(input, opt)
		case "stddev":
			input, err := buildExprIterator(ctx, expr.Args[0].(*influxql.VarRef), b.ic, b.sources, opt, false, false)
			if err != nil {
				return nil, err
			}
			return newStddevIterator(input, opt)
		case "spread":
			// OPTIMIZE(benbjohnson): convert to map/reduce
			input, err := buildExprIterator(ctx, expr.Args[0].(*influxql.VarRef), b.ic, b.sources, opt, false, false)
			if err != nil {
				return nil, err
			}
			return newSpreadIterator(input, opt)
		case "percentile":
			opt.Ordered = true
			input, err := buildExprIterator(ctx, expr.Args[0].(*influxql.VarRef), b.ic, b.sources, opt, false, false)
			if err != nil {
				return nil, err
			}
			var percentile float64
			switch arg := expr.Args[1].(type) {
			case *influxql.NumberLiteral:
				percentile = arg.Val
			case *influxql.IntegerLiteral:
				percentile = float64(arg.Val)
			}
			return newPercentileIterator(input, opt, percentile)
		default:
			return nil, fmt.Errorf("unsupported call: %s", expr.Name)
		}
	}()

	if err != nil {
		return nil, err
	}

	if !b.selector || !opt.Interval.IsZero() {
		itr = NewIntervalIterator(itr, opt)
		if !opt.Interval.IsZero() && opt.Fill != influxql.NoFill {
			itr = NewFillIterator(itr, expr, opt)
		}
	}
	if opt.InterruptCh != nil {
		itr = NewInterruptIterator(itr, opt.InterruptCh)
	}
	return itr, nil
}

func (b *exprIteratorBuilder) buildBinaryExprIterator(ctx context.Context, expr *influxql.BinaryExpr) (Iterator, error) {
	if rhs, ok := expr.RHS.(influxql.Literal); ok {
		// The right hand side is a literal. It is more common to have the RHS be a literal,
		// so we check that one first and have this be the happy path.
		if lhs, ok := expr.LHS.(influxql.Literal); ok {
			// We have two literals that couldn't be combined by Reduce.
			return nil, fmt.Errorf("unable to construct an iterator from two literals: LHS: %T, RHS: %T", lhs, rhs)
		}

		lhs, err := buildExprIterator(ctx, expr.LHS, b.ic, b.sources, b.opt, b.selector, false)
		if err != nil {
			return nil, err
		}
		return buildRHSTransformIterator(lhs, rhs, expr.Op, b.opt)
	} else if lhs, ok := expr.LHS.(influxql.Literal); ok {
		rhs, err := buildExprIterator(ctx, expr.RHS, b.ic, b.sources, b.opt, b.selector, false)
		if err != nil {
			return nil, err
		}
		return buildLHSTransformIterator(lhs, rhs, expr.Op, b.opt)
	} else {
		// We have two iterators. Combine them into a single iterator.
		lhs, err := buildExprIterator(ctx, expr.LHS, b.ic, b.sources, b.opt, false, false)
		if err != nil {
			return nil, err
		}
		rhs, err := buildExprIterator(ctx, expr.RHS, b.ic, b.sources, b.opt, false, false)
		if err != nil {
			return nil, err
		}
		return buildTransformIterator(lhs, rhs, expr.Op, b.opt)
	}
}

func (b *exprIteratorBuilder) callIterator(ctx context.Context, expr *influxql.Call, opt IteratorOptions) (Iterator, error) {
	inputs := make([]Iterator, 0, len(b.sources))
	if err := func() error {
		for _, source := range b.sources {
			switch source := source.(type) {
			case *influxql.Measurement:
				input, err := b.ic.CreateIterator(ctx, source, opt)
				if err != nil {
					return err
				}
				inputs = append(inputs, input)
			case *influxql.SubQuery:
				// Identify the name of the field we are using.
				arg0 := expr.Args[0].(*influxql.VarRef)

				input, err := buildExprIterator(ctx, arg0, b.ic, []influxql.Source{source}, opt, b.selector, false)
				if err != nil {
					return err
				}

				// Wrap the result in a call iterator.
				i, err := NewCallIterator(input, opt)
				if err != nil {
					input.Close()
					return err
				}
				inputs = append(inputs, i)
			}
		}
		return nil
	}(); err != nil {
		Iterators(inputs).Close()
		return nil, err
	}

	itr, err := Iterators(inputs).Merge(opt)
	if err != nil {
		Iterators(inputs).Close()
		return nil, err
	} else if itr == nil {
		itr = &nilFloatIterator{}
	}
	return itr, nil
}

func buildRHSTransformIterator(lhs Iterator, rhs influxql.Literal, op influxql.Token, opt IteratorOptions) (Iterator, error) {
	itrType, litType := iteratorDataType(lhs), literalDataType(rhs)
	if litType == influxql.Unsigned && itrType == influxql.Integer {
		// If the literal is unsigned but the iterator is an integer, return
		// an error since we cannot add an unsigned to an integer.
		return nil, fmt.Errorf("cannot use %s with an integer and unsigned", op)
	}

	fn := binaryExprFunc(iteratorDataType(lhs), literalDataType(rhs), op)
	switch fn := fn.(type) {
	case func(float64, float64) float64:
		var input FloatIterator
		switch lhs := lhs.(type) {
		case FloatIterator:
			input = lhs
		case IntegerIterator:
			input = &integerFloatCastIterator{input: lhs}
		case UnsignedIterator:
			input = &unsignedFloatCastIterator{input: lhs}
		default:
			return nil, fmt.Errorf("type mismatch on LHS, unable to use %T as a FloatIterator", lhs)
		}

		var val float64
		switch rhs := rhs.(type) {
		case *influxql.NumberLiteral:
			val = rhs.Val
		case *influxql.IntegerLiteral:
			val = float64(rhs.Val)
		case *influxql.UnsignedLiteral:
			val = float64(rhs.Val)
		default:
			return nil, fmt.Errorf("type mismatch on RHS, unable to use %T as a NumberLiteral", rhs)
		}
		return &floatTransformIterator{
			input: input,
			fn: func(p *FloatPoint) *FloatPoint {
				if p == nil {
					return nil
				} else if p.Nil {
					return p
				}
				p.Value = fn(p.Value, val)
				return p
			},
		}, nil
	case func(int64, int64) float64:
		input, ok := lhs.(IntegerIterator)
		if !ok {
			return nil, fmt.Errorf("type mismatch on LHS, unable to use %T as a IntegerIterator", lhs)
		}

		var val int64
		switch rhs := rhs.(type) {
		case *influxql.IntegerLiteral:
			val = rhs.Val
		default:
			return nil, fmt.Errorf("type mismatch on RHS, unable to use %T as a IntegerLiteral", rhs)
		}
		return &integerFloatTransformIterator{
			input: input,
			fn: func(p *IntegerPoint) *FloatPoint {
				if p == nil {
					return nil
				}

				fp := &FloatPoint{
					Name: p.Name,
					Tags: p.Tags,
					Time: p.Time,
					Aux:  p.Aux,
				}
				if p.Nil {
					fp.Nil = true
				} else {
					fp.Value = fn(p.Value, val)
				}
				return fp
			},
		}, nil
	case func(float64, float64) bool:
		var input FloatIterator
		switch lhs := lhs.(type) {
		case FloatIterator:
			input = lhs
		case IntegerIterator:
			input = &integerFloatCastIterator{input: lhs}
		case UnsignedIterator:
			input = &unsignedFloatCastIterator{input: lhs}
		default:
			return nil, fmt.Errorf("type mismatch on LHS, unable to use %T as a FloatIterator", lhs)
		}

		var val float64
		switch rhs := rhs.(type) {
		case *influxql.NumberLiteral:
			val = rhs.Val
		case *influxql.IntegerLiteral:
			val = float64(rhs.Val)
		case *influxql.UnsignedLiteral:
			val = float64(rhs.Val)
		default:
			return nil, fmt.Errorf("type mismatch on RHS, unable to use %T as a NumberLiteral", rhs)
		}
		return &floatBoolTransformIterator{
			input: input,
			fn: func(p *FloatPoint) *BooleanPoint {
				if p == nil {
					return nil
				}

				bp := &BooleanPoint{
					Name: p.Name,
					Tags: p.Tags,
					Time: p.Time,
					Aux:  p.Aux,
				}
				if p.Nil {
					bp.Nil = true
				} else {
					bp.Value = fn(p.Value, val)
				}
				return bp
			},
		}, nil
	case func(int64, int64) int64:
		var input IntegerIterator
		switch lhs := lhs.(type) {
		case IntegerIterator:
			input = lhs
		default:
			return nil, fmt.Errorf("type mismatch on LHS, unable to use %T as an IntegerIterator", lhs)
		}

		var val int64
		switch rhs := rhs.(type) {
		case *influxql.IntegerLiteral:
			val = rhs.Val
		default:
			return nil, fmt.Errorf("type mismatch on RHS, unable to use %T as an IntegerLiteral", rhs)
		}
		return &integerTransformIterator{
			input: input,
			fn: func(p *IntegerPoint) *IntegerPoint {
				if p == nil {
					return nil
				} else if p.Nil {
					return p
				}
				p.Value = fn(p.Value, val)
				return p
			},
		}, nil
	case func(int64, int64) bool:
		var input IntegerIterator
		switch lhs := lhs.(type) {
		case IntegerIterator:
			input = lhs
		default:
			return nil, fmt.Errorf("type mismatch on LHS, unable to use %T as an IntegerIterator", lhs)
		}

		var val int64
		switch rhs := rhs.(type) {
		case *influxql.IntegerLiteral:
			val = rhs.Val
		default:
			return nil, fmt.Errorf("type mismatch on RHS, unable to use %T as an IntegerLiteral", rhs)
		}
		return &integerBoolTransformIterator{
			input: input,
			fn: func(p *IntegerPoint) *BooleanPoint {
				if p == nil {
					return nil
				}

				bp := &BooleanPoint{
					Name: p.Name,
					Tags: p.Tags,
					Time: p.Time,
					Aux:  p.Aux,
				}
				if p.Nil {
					bp.Nil = true
				} else {
					bp.Value = fn(p.Value, val)
				}
				return bp
			},
		}, nil
	case func(uint64, uint64) uint64:
		var input UnsignedIterator
		switch lhs := lhs.(type) {
		case UnsignedIterator:
			input = lhs
		default:
			return nil, fmt.Errorf("type mismatch on LHS, unable to use %T as a FloatIterator", lhs)
		}

		var val uint64
		switch rhs := rhs.(type) {
		case *influxql.IntegerLiteral:
			if rhs.Val < 0 {
				return nil, fmt.Errorf("cannot use negative integer '%s' in math with unsigned", rhs)
			}
			val = uint64(rhs.Val)
		case *influxql.UnsignedLiteral:
			val = rhs.Val
		default:
			return nil, fmt.Errorf("type mismatch on RHS, unable to use %T as a NumberLiteral", rhs)
		}
		return &unsignedTransformIterator{
			input: input,
			fn: func(p *UnsignedPoint) *UnsignedPoint {
				if p == nil {
					return nil
				} else if p.Nil {
					return p
				}
				p.Value = fn(p.Value, val)
				return p
			},
		}, nil
	case func(uint64, uint64) bool:
		var input UnsignedIterator
		switch lhs := lhs.(type) {
		case UnsignedIterator:
			input = lhs
		default:
			return nil, fmt.Errorf("type mismatch on LHS, unable to use %T as a FloatIterator", lhs)
		}

		var val uint64
		switch rhs := rhs.(type) {
		case *influxql.IntegerLiteral:
			if rhs.Val < 0 {
				return nil, fmt.Errorf("cannot use negative integer '%s' in math with unsigned", rhs)
			}
			val = uint64(rhs.Val)
		case *influxql.UnsignedLiteral:
			val = rhs.Val
		default:
			return nil, fmt.Errorf("type mismatch on RHS, unable to use %T as a NumberLiteral", rhs)
		}
		return &unsignedBoolTransformIterator{
			input: input,
			fn: func(p *UnsignedPoint) *BooleanPoint {
				if p == nil {
					return nil
				}

				bp := &BooleanPoint{
					Name: p.Name,
					Tags: p.Tags,
					Time: p.Time,
					Aux:  p.Aux,
				}
				if p.Nil {
					bp.Nil = true
				} else {
					bp.Value = fn(p.Value, val)
				}
				return bp
			},
		}, nil
	case func(bool, bool) bool:
		var input BooleanIterator
		switch lhs := lhs.(type) {
		case BooleanIterator:
			input = lhs
		default:
			return nil, fmt.Errorf("type mismatch on LHS, unable to use %T as an BooleanIterator", lhs)
		}

		var val bool
		switch rhs := rhs.(type) {
		case *influxql.BooleanLiteral:
			val = rhs.Val
		default:
			return nil, fmt.Errorf("type mismatch on RHS, unable to use %T as an BooleanLiteral", rhs)
		}
		return &booleanTransformIterator{
			input: input,
			fn: func(p *BooleanPoint) *BooleanPoint {
				if p == nil {
					return nil
				}

				bp := &BooleanPoint{
					Name: p.Name,
					Tags: p.Tags,
					Time: p.Time,
					Aux:  p.Aux,
				}
				if p.Nil {
					bp.Nil = true
				} else {
					bp.Value = fn(p.Value, val)
				}
				return bp
			},
		}, nil
	}
	return nil, fmt.Errorf("unable to construct rhs transform iterator from %T and %T", lhs, rhs)
}

func buildLHSTransformIterator(lhs influxql.Literal, rhs Iterator, op influxql.Token, opt IteratorOptions) (Iterator, error) {
	litType, itrType := literalDataType(lhs), iteratorDataType(rhs)
	if litType == influxql.Unsigned && itrType == influxql.Integer {
		// If the literal is unsigned but the iterator is an integer, return
		// an error since we cannot add an unsigned to an integer.
		return nil, fmt.Errorf("cannot use %s with unsigned and an integer", op)
	}

	fn := binaryExprFunc(litType, itrType, op)
	switch fn := fn.(type) {
	case func(float64, float64) float64:
		var input FloatIterator
		switch rhs := rhs.(type) {
		case FloatIterator:
			input = rhs
		case IntegerIterator:
			input = &integerFloatCastIterator{input: rhs}
		case UnsignedIterator:
			input = &unsignedFloatCastIterator{input: rhs}
		default:
			return nil, fmt.Errorf("type mismatch on RHS, unable to use %T as a FloatIterator", rhs)
		}

		var val float64
		switch lhs := lhs.(type) {
		case *influxql.NumberLiteral:
			val = lhs.Val
		case *influxql.IntegerLiteral:
			val = float64(lhs.Val)
		case *influxql.UnsignedLiteral:
			val = float64(lhs.Val)
		default:
			return nil, fmt.Errorf("type mismatch on LHS, unable to use %T as a NumberLiteral", lhs)
		}
		return &floatTransformIterator{
			input: input,
			fn: func(p *FloatPoint) *FloatPoint {
				if p == nil {
					return nil
				} else if p.Nil {
					return p
				}
				p.Value = fn(val, p.Value)
				return p
			},
		}, nil
	case func(int64, int64) float64:
		input, ok := rhs.(IntegerIterator)
		if !ok {
			return nil, fmt.Errorf("type mismatch on RHS, unable to use %T as a IntegerIterator", lhs)
		}

		var val int64
		switch lhs := lhs.(type) {
		case *influxql.IntegerLiteral:
			val = lhs.Val
		default:
			return nil, fmt.Errorf("type mismatch on LHS, unable to use %T as a IntegerLiteral", rhs)
		}
		return &integerFloatTransformIterator{
			input: input,
			fn: func(p *IntegerPoint) *FloatPoint {
				if p == nil {
					return nil
				}

				fp := &FloatPoint{
					Name: p.Name,
					Tags: p.Tags,
					Time: p.Time,
					Aux:  p.Aux,
				}
				if p.Nil {
					fp.Nil = true
				} else {
					fp.Value = fn(val, p.Value)
				}
				return fp
			},
		}, nil
	case func(float64, float64) bool:
		var input FloatIterator
		switch rhs := rhs.(type) {
		case FloatIterator:
			input = rhs
		case IntegerIterator:
			input = &integerFloatCastIterator{input: rhs}
		case UnsignedIterator:
			input = &unsignedFloatCastIterator{input: rhs}
		default:
			return nil, fmt.Errorf("type mismatch on RHS, unable to use %T as a FloatIterator", rhs)
		}

		var val float64
		switch lhs := lhs.(type) {
		case *influxql.NumberLiteral:
			val = lhs.Val
		case *influxql.IntegerLiteral:
			val = float64(lhs.Val)
		case *influxql.UnsignedLiteral:
			val = float64(lhs.Val)
		default:
			return nil, fmt.Errorf("type mismatch on LHS, unable to use %T as a NumberLiteral", lhs)
		}
		return &floatBoolTransformIterator{
			input: input,
			fn: func(p *FloatPoint) *BooleanPoint {
				if p == nil {
					return nil
				}

				bp := &BooleanPoint{
					Name: p.Name,
					Tags: p.Tags,
					Time: p.Time,
					Aux:  p.Aux,
				}
				if p.Nil {
					bp.Nil = true
				} else {
					bp.Value = fn(val, p.Value)
				}
				return bp
			},
		}, nil
	case func(int64, int64) int64:
		var input IntegerIterator
		switch rhs := rhs.(type) {
		case IntegerIterator:
			input = rhs
		default:
			return nil, fmt.Errorf("type mismatch on RHS, unable to use %T as an IntegerIterator", rhs)
		}

		var val int64
		switch lhs := lhs.(type) {
		case *influxql.IntegerLiteral:
			val = lhs.Val
		default:
			return nil, fmt.Errorf("type mismatch on LHS, unable to use %T as an IntegerLiteral", lhs)
		}
		return &integerTransformIterator{
			input: input,
			fn: func(p *IntegerPoint) *IntegerPoint {
				if p == nil {
					return nil
				} else if p.Nil {
					return p
				}
				p.Value = fn(val, p.Value)
				return p
			},
		}, nil
	case func(int64, int64) bool:
		var input IntegerIterator
		switch rhs := rhs.(type) {
		case IntegerIterator:
			input = rhs
		default:
			return nil, fmt.Errorf("type mismatch on RHS, unable to use %T as an IntegerIterator", rhs)
		}

		var val int64
		switch lhs := lhs.(type) {
		case *influxql.IntegerLiteral:
			val = lhs.Val
		default:
			return nil, fmt.Errorf("type mismatch on LHS, unable to use %T as an IntegerLiteral", lhs)
		}
		return &integerBoolTransformIterator{
			input: input,
			fn: func(p *IntegerPoint) *BooleanPoint {
				if p == nil {
					return nil
				}

				bp := &BooleanPoint{
					Name: p.Name,
					Tags: p.Tags,
					Time: p.Time,
					Aux:  p.Aux,
				}
				if p.Nil {
					bp.Nil = true
				} else {
					bp.Value = fn(val, p.Value)
				}
				return bp
			},
		}, nil
	case func(uint64, uint64) uint64:
		var input UnsignedIterator
		switch rhs := rhs.(type) {
		case UnsignedIterator:
			input = rhs
		default:
			return nil, fmt.Errorf("type mismatch on LHS, unable to use %T as a FloatIterator", lhs)
		}

		var val uint64
		switch lhs := lhs.(type) {
		case *influxql.IntegerLiteral:
			if lhs.Val < 0 {
				return nil, fmt.Errorf("cannot use negative integer '%s' in math with unsigned", rhs)
			}
			val = uint64(lhs.Val)
		case *influxql.UnsignedLiteral:
			val = lhs.Val
		default:
			return nil, fmt.Errorf("type mismatch on RHS, unable to use %T as a NumberLiteral", rhs)
		}
		return &unsignedTransformIterator{
			input: input,
			fn: func(p *UnsignedPoint) *UnsignedPoint {
				if p == nil {
					return nil
				} else if p.Nil {
					return p
				}
				p.Value = fn(val, p.Value)
				return p
			},
		}, nil
	case func(uint64, uint64) bool:
		var input UnsignedIterator
		switch rhs := rhs.(type) {
		case UnsignedIterator:
			input = rhs
		default:
			return nil, fmt.Errorf("type mismatch on LHS, unable to use %T as a FloatIterator", lhs)
		}

		var val uint64
		switch lhs := lhs.(type) {
		case *influxql.IntegerLiteral:
			if lhs.Val < 0 {
				return nil, fmt.Errorf("cannot use negative integer '%s' in math with unsigned", rhs)
			}
			val = uint64(lhs.Val)
		case *influxql.UnsignedLiteral:
			val = lhs.Val
		default:
			return nil, fmt.Errorf("type mismatch on RHS, unable to use %T as a NumberLiteral", rhs)
		}
		return &unsignedBoolTransformIterator{
			input: input,
			fn: func(p *UnsignedPoint) *BooleanPoint {
				if p == nil {
					return nil
				}

				bp := &BooleanPoint{
					Name: p.Name,
					Tags: p.Tags,
					Time: p.Time,
					Aux:  p.Aux,
				}
				if p.Nil {
					bp.Nil = true
				} else {
					bp.Value = fn(val, p.Value)
				}
				return bp
			},
		}, nil
	case func(bool, bool) bool:
		var input BooleanIterator
		switch rhs := rhs.(type) {
		case BooleanIterator:
			input = rhs
		default:
			return nil, fmt.Errorf("type mismatch on RHS, unable to use %T as an BooleanIterator", rhs)
		}

		var val bool
		switch lhs := lhs.(type) {
		case *influxql.BooleanLiteral:
			val = lhs.Val
		default:
			return nil, fmt.Errorf("type mismatch on LHS, unable to use %T as a BooleanLiteral", lhs)
		}
		return &booleanTransformIterator{
			input: input,
			fn: func(p *BooleanPoint) *BooleanPoint {
				if p == nil {
					return nil
				}

				bp := &BooleanPoint{
					Name: p.Name,
					Tags: p.Tags,
					Time: p.Time,
					Aux:  p.Aux,
				}
				if p.Nil {
					bp.Nil = true
				} else {
					bp.Value = fn(val, p.Value)
				}
				return bp
			},
		}, nil
	}
	return nil, fmt.Errorf("unable to construct lhs transform iterator from %T and %T", lhs, rhs)
}

func buildTransformIterator(lhs Iterator, rhs Iterator, op influxql.Token, opt IteratorOptions) (Iterator, error) {
	lhsType, rhsType := iteratorDataType(lhs), iteratorDataType(rhs)
	if lhsType == influxql.Integer && rhsType == influxql.Unsigned {
		return nil, fmt.Errorf("cannot use %s between an integer and unsigned, an explicit cast is required", op)
	} else if lhsType == influxql.Unsigned && rhsType == influxql.Integer {
		return nil, fmt.Errorf("cannot use %s between unsigned and an integer, an explicit cast is required", op)
	}

	fn := binaryExprFunc(lhsType, rhsType, op)
	switch fn := fn.(type) {
	case func(float64, float64) float64:
		var left FloatIterator
		switch lhs := lhs.(type) {
		case FloatIterator:
			left = lhs
		case IntegerIterator:
			left = &integerFloatCastIterator{input: lhs}
		case UnsignedIterator:
			left = &unsignedFloatCastIterator{input: lhs}
		default:
			return nil, fmt.Errorf("type mismatch on LHS, unable to use %T as a FloatIterator", lhs)
		}

		var right FloatIterator
		switch rhs := rhs.(type) {
		case FloatIterator:
			right = rhs
		case IntegerIterator:
			right = &integerFloatCastIterator{input: rhs}
		case UnsignedIterator:
			right = &unsignedFloatCastIterator{input: rhs}
		default:
			return nil, fmt.Errorf("type mismatch on RHS, unable to use %T as a FloatIterator", rhs)
		}
		return newFloatExprIterator(left, right, opt, fn), nil
	case func(int64, int64) float64:
		left, ok := lhs.(IntegerIterator)
		if !ok {
			return nil, fmt.Errorf("type mismatch on LHS, unable to use %T as a IntegerIterator", lhs)
		}
		right, ok := rhs.(IntegerIterator)
		if !ok {
			return nil, fmt.Errorf("type mismatch on RHS, unable to use %T as a IntegerIterator", rhs)
		}
		return newIntegerFloatExprIterator(left, right, opt, fn), nil
	case func(int64, int64) int64:
		left, ok := lhs.(IntegerIterator)
		if !ok {
			return nil, fmt.Errorf("type mismatch on LHS, unable to use %T as a IntegerIterator", lhs)
		}
		right, ok := rhs.(IntegerIterator)
		if !ok {
			return nil, fmt.Errorf("type mismatch on RHS, unable to use %T as a IntegerIterator", rhs)
		}
		return newIntegerExprIterator(left, right, opt, fn), nil
	case func(uint64, uint64) uint64:
		left, ok := lhs.(UnsignedIterator)
		if !ok {
			return nil, fmt.Errorf("type mismatch on LHS, unable to use %T as an UnsignedIterator", lhs)
		}
		right, ok := rhs.(UnsignedIterator)
		if !ok {
			return nil, fmt.Errorf("type mismatch on RHS, unable to use %T as an UnsignedIterator", lhs)
		}
		return newUnsignedExprIterator(left, right, opt, fn), nil
	case func(float64, float64) bool:
		var left FloatIterator
		switch lhs := lhs.(type) {
		case FloatIterator:
			left = lhs
		case IntegerIterator:
			left = &integerFloatCastIterator{input: lhs}
		case UnsignedIterator:
			left = &unsignedFloatCastIterator{input: lhs}
		default:
			return nil, fmt.Errorf("type mismatch on LHS, unable to use %T as a FloatIterator", lhs)
		}

		var right FloatIterator
		switch rhs := rhs.(type) {
		case FloatIterator:
			right = rhs
		case IntegerIterator:
			right = &integerFloatCastIterator{input: rhs}
		case UnsignedIterator:
			right = &unsignedFloatCastIterator{input: rhs}
		default:
			return nil, fmt.Errorf("type mismatch on RHS, unable to use %T as a FloatIterator", rhs)
		}
		return newFloatBooleanExprIterator(left, right, opt, fn), nil
	case func(int64, int64) bool:
		left, ok := lhs.(IntegerIterator)
		if !ok {
			return nil, fmt.Errorf("type mismatch on LHS, unable to use %T as a IntegerIterator", lhs)
		}
		right, ok := rhs.(IntegerIterator)
		if !ok {
			return nil, fmt.Errorf("type mismatch on RHS, unable to use %T as a IntegerIterator", rhs)
		}
		return newIntegerBooleanExprIterator(left, right, opt, fn), nil
	case func(uint64, uint64) bool:
		left, ok := lhs.(UnsignedIterator)
		if !ok {
			return nil, fmt.Errorf("type mismatch on LHS, unable to use %T as an UnsignedIterator", lhs)
		}
		right, ok := rhs.(UnsignedIterator)
		if !ok {
			return nil, fmt.Errorf("type mismatch on RHS, unable to use %T as an UnsignedIterator", lhs)
		}
		return newUnsignedBooleanExprIterator(left, right, opt, fn), nil
	case func(bool, bool) bool:
		left, ok := lhs.(BooleanIterator)
		if !ok {
			return nil, fmt.Errorf("type mismatch on LHS, unable to use %T as a BooleanIterator", lhs)
		}
		right, ok := rhs.(BooleanIterator)
		if !ok {
			return nil, fmt.Errorf("type mismatch on RHS, unable to use %T as a BooleanIterator", rhs)
		}
		return newBooleanExprIterator(left, right, opt, fn), nil
	}
	return nil, fmt.Errorf("unable to construct transform iterator from %T and %T", lhs, rhs)
}

func iteratorDataType(itr Iterator) influxql.DataType {
	switch itr.(type) {
	case FloatIterator:
		return influxql.Float
	case IntegerIterator:
		return influxql.Integer
	case UnsignedIterator:
		return influxql.Unsigned
	case StringIterator:
		return influxql.String
	case BooleanIterator:
		return influxql.Boolean
	default:
		return influxql.Unknown
	}
}

func literalDataType(lit influxql.Literal) influxql.DataType {
	switch lit.(type) {
	case *influxql.NumberLiteral:
		return influxql.Float
	case *influxql.IntegerLiteral:
		return influxql.Integer
	case *influxql.UnsignedLiteral:
		return influxql.Unsigned
	case *influxql.StringLiteral:
		return influxql.String
	case *influxql.BooleanLiteral:
		return influxql.Boolean
	default:
		return influxql.Unknown
	}
}

func binaryExprFunc(typ1 influxql.DataType, typ2 influxql.DataType, op influxql.Token) interface{} {
	var fn interface{}
	switch typ1 {
	case influxql.Float:
		fn = floatBinaryExprFunc(op)
	case influxql.Integer:
		switch typ2 {
		case influxql.Float:
			fn = floatBinaryExprFunc(op)
		case influxql.Unsigned:
			// Special case for LT, LTE, GT, and GTE.
			fn = unsignedBinaryExprFunc(op)
		default:
			fn = integerBinaryExprFunc(op)
		}
	case influxql.Unsigned:
		switch typ2 {
		case influxql.Float:
			fn = floatBinaryExprFunc(op)
		case influxql.Integer:
			// Special case for LT, LTE, GT, and GTE.
			// Since the RHS is an integer, we need to check if it is less than
			// zero for the comparison operators to not be subject to overflow.
			switch op {
			case influxql.LT:
				return func(lhs, rhs uint64) bool {
					if int64(rhs) < 0 {
						return false
					}
					return lhs < rhs
				}
			case influxql.LTE:
				return func(lhs, rhs uint64) bool {
					if int64(rhs) < 0 {
						return false
					}
					return lhs <= rhs
				}
			case influxql.GT:
				return func(lhs, rhs uint64) bool {
					if int64(rhs) < 0 {
						return true
					}
					return lhs > rhs
				}
			case influxql.GTE:
				return func(lhs, rhs uint64) bool {
					if int64(rhs) < 0 {
						return true
					}
					return lhs >= rhs
				}
			}
			fallthrough
		default:
			fn = unsignedBinaryExprFunc(op)
		}
	case influxql.Boolean:
		fn = booleanBinaryExprFunc(op)
	}
	return fn
}

func floatBinaryExprFunc(op influxql.Token) interface{} {
	switch op {
	case influxql.ADD:
		return func(lhs, rhs float64) float64 { return lhs + rhs }
	case influxql.SUB:
		return func(lhs, rhs float64) float64 { return lhs - rhs }
	case influxql.MUL:
		return func(lhs, rhs float64) float64 { return lhs * rhs }
	case influxql.DIV:
		return func(lhs, rhs float64) float64 {
			if rhs == 0 {
				return float64(0)
			}
			return lhs / rhs
		}
	case influxql.MOD:
		return func(lhs, rhs float64) float64 { return math.Mod(lhs, rhs) }
	case influxql.EQ:
		return func(lhs, rhs float64) bool { return lhs == rhs }
	case influxql.NEQ:
		return func(lhs, rhs float64) bool { return lhs != rhs }
	case influxql.LT:
		return func(lhs, rhs float64) bool { return lhs < rhs }
	case influxql.LTE:
		return func(lhs, rhs float64) bool { return lhs <= rhs }
	case influxql.GT:
		return func(lhs, rhs float64) bool { return lhs > rhs }
	case influxql.GTE:
		return func(lhs, rhs float64) bool { return lhs >= rhs }
	}
	return nil
}

func integerBinaryExprFunc(op influxql.Token) interface{} {
	switch op {
	case influxql.ADD:
		return func(lhs, rhs int64) int64 { return lhs + rhs }
	case influxql.SUB:
		return func(lhs, rhs int64) int64 { return lhs - rhs }
	case influxql.MUL:
		return func(lhs, rhs int64) int64 { return lhs * rhs }
	case influxql.DIV:
		return func(lhs, rhs int64) float64 {
			if rhs == 0 {
				return float64(0)
			}
			return float64(lhs) / float64(rhs)
		}
	case influxql.MOD:
		return func(lhs, rhs int64) int64 {
			if rhs == 0 {
				return int64(0)
			}
			return lhs % rhs
		}
	case influxql.BITWISE_AND:
		return func(lhs, rhs int64) int64 { return lhs & rhs }
	case influxql.BITWISE_OR:
		return func(lhs, rhs int64) int64 { return lhs | rhs }
	case influxql.BITWISE_XOR:
		return func(lhs, rhs int64) int64 { return lhs ^ rhs }
	case influxql.EQ:
		return func(lhs, rhs int64) bool { return lhs == rhs }
	case influxql.NEQ:
		return func(lhs, rhs int64) bool { return lhs != rhs }
	case influxql.LT:
		return func(lhs, rhs int64) bool { return lhs < rhs }
	case influxql.LTE:
		return func(lhs, rhs int64) bool { return lhs <= rhs }
	case influxql.GT:
		return func(lhs, rhs int64) bool { return lhs > rhs }
	case influxql.GTE:
		return func(lhs, rhs int64) bool { return lhs >= rhs }
	}
	return nil
}

func unsignedBinaryExprFunc(op influxql.Token) interface{} {
	switch op {
	case influxql.ADD:
		return func(lhs, rhs uint64) uint64 { return lhs + rhs }
	case influxql.SUB:
		return func(lhs, rhs uint64) uint64 { return lhs - rhs }
	case influxql.MUL:
		return func(lhs, rhs uint64) uint64 { return lhs * rhs }
	case influxql.DIV:
		return func(lhs, rhs uint64) uint64 {
			if rhs == 0 {
				return uint64(0)
			}
			return lhs / rhs
		}
	case influxql.MOD:
		return func(lhs, rhs uint64) uint64 {
			if rhs == 0 {
				return uint64(0)
			}
			return lhs % rhs
		}
	case influxql.BITWISE_AND:
		return func(lhs, rhs uint64) uint64 { return lhs & rhs }
	case influxql.BITWISE_OR:
		return func(lhs, rhs uint64) uint64 { return lhs | rhs }
	case influxql.BITWISE_XOR:
		return func(lhs, rhs uint64) uint64 { return lhs ^ rhs }
	case influxql.EQ:
		return func(lhs, rhs uint64) bool { return lhs == rhs }
	case influxql.NEQ:
		return func(lhs, rhs uint64) bool { return lhs != rhs }
	case influxql.LT:
		return func(lhs, rhs uint64) bool { return lhs < rhs }
	case influxql.LTE:
		return func(lhs, rhs uint64) bool { return lhs <= rhs }
	case influxql.GT:
		return func(lhs, rhs uint64) bool { return lhs > rhs }
	case influxql.GTE:
		return func(lhs, rhs uint64) bool { return lhs >= rhs }
	}
	return nil
}

func booleanBinaryExprFunc(op influxql.Token) interface{} {
	switch op {
	case influxql.BITWISE_AND:
		return func(lhs, rhs bool) bool { return lhs && rhs }
	case influxql.BITWISE_OR:
		return func(lhs, rhs bool) bool { return lhs || rhs }
	case influxql.BITWISE_XOR:
		return func(lhs, rhs bool) bool { return lhs != rhs }
	}
	return nil
}
