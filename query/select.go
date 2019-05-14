package query

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/influxdata/influxdb/pkg/tracing"
	"github.com/influxdata/influxdb/query/internal/gota"
	"github.com/influxdata/influxql"
)

var DefaultTypeMapper = influxql.MultiTypeMapper(
	FunctionTypeMapper{},
	MathTypeMapper{},
)

// SelectOptions are options that customize the select call.
type SelectOptions struct {
	// Authorizer is used to limit access to data
	Authorizer Authorizer

	// Node to exclusively read from.
	// If zero, all nodes are used.
	NodeID uint64

	// Maximum number of concurrent series.
	MaxSeriesN int

	// Maximum number of points to read from the query.
	// This requires the passed in context to have a Monitor that is
	// created using WithMonitor.
	MaxPointN int

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
	Select(ctx context.Context) (Cursor, error)

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
func Select(ctx context.Context, stmt *influxql.SelectStatement, shardMapper ShardMapper, opt SelectOptions) (Cursor, error) {
	s, err := Prepare(stmt, shardMapper, opt)
	if err != nil {
		return nil, err
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
	columns   []string
	maxPointN int
	now       time.Time
}

func (p *preparedStatement) Select(ctx context.Context) (Cursor, error) {
	// TODO(jsternberg): Remove this hacky method of propagating now.
	// Each level of the query should use a time range discovered during
	// compilation, but that requires too large of a refactor at the moment.
	ctx = context.WithValue(ctx, "now", p.now)

	opt := p.opt
	opt.InterruptCh = ctx.Done()
	cur, err := buildCursor(ctx, p.stmt, p.ic, opt)
	if err != nil {
		return nil, err
	}

	// If a monitor exists and we are told there is a maximum number of points,
	// register the monitor function.
	if m := MonitorFromContext(ctx); m != nil {
		if p.maxPointN > 0 {
			monitor := PointLimitMonitor(cur, DefaultStatsInterval, p.maxPointN)
			m.Monitor(monitor)
		}
	}
	return cur, nil
}

func (p *preparedStatement) Close() error {
	return p.ic.Close()
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
				} else if input != nil {
					inputs = append(inputs, input)
				}
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
	case "derivative", "non_negative_derivative", "difference", "non_negative_difference", "moving_average", "exponential_moving_average", "double_exponential_moving_average", "triple_exponential_moving_average", "relative_strength_index", "triple_exponential_derivative", "kaufmans_efficiency_ratio", "kaufmans_adaptive_moving_average", "chande_momentum_oscillator", "elapsed":
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
		case "exponential_moving_average", "double_exponential_moving_average", "triple_exponential_moving_average", "relative_strength_index", "triple_exponential_derivative":
			n := expr.Args[1].(*influxql.IntegerLiteral)
			if n.Val > 1 && !opt.Interval.IsZero() {
				if opt.Ascending {
					opt.StartTime -= int64(opt.Interval.Duration) * (n.Val - 1)
				} else {
					opt.EndTime += int64(opt.Interval.Duration) * (n.Val - 1)
				}
			}

			nHold := -1
			if len(expr.Args) >= 3 {
				nHold = int(expr.Args[2].(*influxql.IntegerLiteral).Val)
			}

			warmupType := gota.WarmEMA
			if len(expr.Args) >= 4 {
				if warmupType, err = gota.ParseWarmupType(expr.Args[3].(*influxql.StringLiteral).Val); err != nil {
					return nil, err
				}
			}

			switch expr.Name {
			case "exponential_moving_average":
				return newExponentialMovingAverageIterator(input, int(n.Val), nHold, warmupType, opt)
			case "double_exponential_moving_average":
				return newDoubleExponentialMovingAverageIterator(input, int(n.Val), nHold, warmupType, opt)
			case "triple_exponential_moving_average":
				return newTripleExponentialMovingAverageIterator(input, int(n.Val), nHold, warmupType, opt)
			case "relative_strength_index":
				return newRelativeStrengthIndexIterator(input, int(n.Val), nHold, warmupType, opt)
			case "triple_exponential_derivative":
				return newTripleExponentialDerivativeIterator(input, int(n.Val), nHold, warmupType, opt)
			}
		case "kaufmans_efficiency_ratio", "kaufmans_adaptive_moving_average":
			n := expr.Args[1].(*influxql.IntegerLiteral)
			if n.Val > 1 && !opt.Interval.IsZero() {
				if opt.Ascending {
					opt.StartTime -= int64(opt.Interval.Duration) * (n.Val - 1)
				} else {
					opt.EndTime += int64(opt.Interval.Duration) * (n.Val - 1)
				}
			}

			nHold := -1
			if len(expr.Args) >= 3 {
				nHold = int(expr.Args[2].(*influxql.IntegerLiteral).Val)
			}

			switch expr.Name {
			case "kaufmans_efficiency_ratio":
				return newKaufmansEfficiencyRatioIterator(input, int(n.Val), nHold, opt)
			case "kaufmans_adaptive_moving_average":
				return newKaufmansAdaptiveMovingAverageIterator(input, int(n.Val), nHold, opt)
			}
		case "chande_momentum_oscillator":
			n := expr.Args[1].(*influxql.IntegerLiteral)
			if n.Val > 1 && !opt.Interval.IsZero() {
				if opt.Ascending {
					opt.StartTime -= int64(opt.Interval.Duration) * (n.Val - 1)
				} else {
					opt.EndTime += int64(opt.Interval.Duration) * (n.Val - 1)
				}
			}

			nHold := -1
			if len(expr.Args) >= 3 {
				nHold = int(expr.Args[2].(*influxql.IntegerLiteral).Val)
			}

			warmupType := gota.WarmupType(-1)
			if len(expr.Args) >= 4 {
				wt := expr.Args[3].(*influxql.StringLiteral).Val
				if wt != "none" {
					if warmupType, err = gota.ParseWarmupType(wt); err != nil {
						return nil, err
					}
				}
			}

			return newChandeMomentumOscillatorIterator(input, int(n.Val), nHold, warmupType, opt)
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

				opt.Ordered = false
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

func buildCursor(ctx context.Context, stmt *influxql.SelectStatement, ic IteratorCreator, opt IteratorOptions) (Cursor, error) {
	span := tracing.SpanFromContext(ctx)
	if span != nil {
		span = span.StartSpan("build_cursor")
		defer span.Finish()

		span.SetLabels("statement", stmt.String())
		ctx = tracing.NewContextWithSpan(ctx, span)
	}

	switch opt.Fill {
	case influxql.NumberFill:
		if v, ok := opt.FillValue.(int); ok {
			opt.FillValue = int64(v)
		}
	case influxql.PreviousFill:
		opt.FillValue = SkipDefault
	}

	fields := make([]*influxql.Field, 0, len(stmt.Fields)+1)
	if !stmt.OmitTime {
		// Add a field with the variable "time" if we have not omitted time.
		fields = append(fields, &influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "time",
				Type: influxql.Time,
			},
		})
	}

	// Iterate through each of the fields to add them to the value mapper.
	valueMapper := newValueMapper()
	for _, f := range stmt.Fields {
		fields = append(fields, valueMapper.Map(f))

		// If the field is a top() or bottom() call, we need to also add
		// the extra variables if we are not writing into a target.
		if stmt.Target != nil {
			continue
		}

		switch expr := f.Expr.(type) {
		case *influxql.Call:
			if expr.Name == "top" || expr.Name == "bottom" {
				for i := 1; i < len(expr.Args)-1; i++ {
					nf := influxql.Field{Expr: expr.Args[i]}
					fields = append(fields, valueMapper.Map(&nf))
				}
			}
		}
	}

	// Set the aliases on each of the columns to what the final name should be.
	columns := stmt.ColumnNames()
	for i, f := range fields {
		f.Alias = columns[i]
	}

	// Retrieve the refs to retrieve the auxiliary fields.
	var auxKeys []influxql.VarRef
	if len(valueMapper.refs) > 0 {
		opt.Aux = make([]influxql.VarRef, 0, len(valueMapper.refs))
		for ref := range valueMapper.refs {
			opt.Aux = append(opt.Aux, *ref)
		}
		sort.Sort(influxql.VarRefs(opt.Aux))

		auxKeys = make([]influxql.VarRef, len(opt.Aux))
		for i, ref := range opt.Aux {
			auxKeys[i] = valueMapper.symbols[ref.String()]
		}
	}

	// If there are no calls, then produce an auxiliary cursor.
	if len(valueMapper.calls) == 0 {
		// If all of the auxiliary keys are of an unknown type,
		// do not construct the iterator and return a null cursor.
		if !hasValidType(auxKeys) {
			return newNullCursor(fields), nil
		}

		itr, err := buildAuxIterator(ctx, ic, stmt.Sources, opt)
		if err != nil {
			return nil, err
		}

		// Create a slice with an empty first element.
		keys := []influxql.VarRef{{}}
		keys = append(keys, auxKeys...)

		scanner := NewIteratorScanner(itr, keys, opt.FillValue)
		return newScannerCursor(scanner, fields, opt), nil
	}

	// Check to see if this is a selector statement.
	// It is a selector if it is the only selector call and the call itself
	// is a selector.
	selector := len(valueMapper.calls) == 1
	if selector {
		for call := range valueMapper.calls {
			if !influxql.IsSelector(call) {
				selector = false
			}
		}
	}

	// Produce an iterator for every single call and create an iterator scanner
	// associated with it.
	scanners := make([]IteratorScanner, 0, len(valueMapper.calls))
	for call := range valueMapper.calls {
		driver := valueMapper.table[call]
		if driver.Type == influxql.Unknown {
			// The primary driver of this call is of unknown type, so skip this.
			continue
		}

		itr, err := buildFieldIterator(ctx, call, ic, stmt.Sources, opt, selector, stmt.Target != nil)
		if err != nil {
			for _, s := range scanners {
				s.Close()
			}
			return nil, err
		}

		keys := make([]influxql.VarRef, 0, len(auxKeys)+1)
		keys = append(keys, driver)
		keys = append(keys, auxKeys...)

		scanner := NewIteratorScanner(itr, keys, opt.FillValue)
		scanners = append(scanners, scanner)
	}

	if len(scanners) == 0 {
		return newNullCursor(fields), nil
	} else if len(scanners) == 1 {
		return newScannerCursor(scanners[0], fields, opt), nil
	}
	return newMultiScannerCursor(scanners, fields, opt), nil
}

func buildAuxIterator(ctx context.Context, ic IteratorCreator, sources influxql.Sources, opt IteratorOptions) (Iterator, error) {
	span := tracing.SpanFromContext(ctx)
	if span != nil {
		span = span.StartSpan("iterator_scanner")
		defer span.Finish()

		auxFieldNames := make([]string, len(opt.Aux))
		for i, ref := range opt.Aux {
			auxFieldNames[i] = ref.String()
		}
		span.SetLabels("auxiliary_fields", strings.Join(auxFieldNames, ", "))
		ctx = tracing.NewContextWithSpan(ctx, span)
	}

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
				} else if input != nil {
					inputs = append(inputs, input)
				}
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
			if sz := len(opt.Aux); sz > 0 && sz < 3 {
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
	return input, nil
}

func buildFieldIterator(ctx context.Context, expr influxql.Expr, ic IteratorCreator, sources influxql.Sources, opt IteratorOptions, selector, writeMode bool) (Iterator, error) {
	span := tracing.SpanFromContext(ctx)
	if span != nil {
		span = span.StartSpan("iterator_scanner")
		defer span.Finish()

		labels := []string{"expr", expr.String()}
		if len(opt.Aux) > 0 {
			auxFieldNames := make([]string, len(opt.Aux))
			for i, ref := range opt.Aux {
				auxFieldNames[i] = ref.String()
			}
			labels = append(labels, "auxiliary_fields", strings.Join(auxFieldNames, ", "))
		}
		span.SetLabels(labels...)
		ctx = tracing.NewContextWithSpan(ctx, span)
	}

	input, err := buildExprIterator(ctx, expr, ic, sources, opt, selector, writeMode)
	if err != nil {
		return nil, err
	}

	// Apply limit & offset.
	if opt.Limit > 0 || opt.Offset > 0 {
		input = NewLimitIterator(input, opt)
	}
	return input, nil
}

type valueMapper struct {
	// An index that maps a node's string output to its symbol so that all
	// nodes with the same signature are mapped the same.
	symbols map[string]influxql.VarRef
	// An index that maps a specific expression to a symbol. This ensures that
	// only expressions that were mapped get symbolized.
	table map[influxql.Expr]influxql.VarRef
	// A collection of all of the calls in the table.
	calls map[*influxql.Call]struct{}
	// A collection of all of the calls in the table.
	refs map[*influxql.VarRef]struct{}
	i    int
}

func newValueMapper() *valueMapper {
	return &valueMapper{
		symbols: make(map[string]influxql.VarRef),
		table:   make(map[influxql.Expr]influxql.VarRef),
		calls:   make(map[*influxql.Call]struct{}),
		refs:    make(map[*influxql.VarRef]struct{}),
	}
}

func (v *valueMapper) Map(field *influxql.Field) *influxql.Field {
	clone := *field
	clone.Expr = influxql.CloneExpr(field.Expr)

	influxql.Walk(v, clone.Expr)
	clone.Expr = influxql.RewriteExpr(clone.Expr, v.rewriteExpr)
	return &clone
}

func (v *valueMapper) Visit(n influxql.Node) influxql.Visitor {
	expr, ok := n.(influxql.Expr)
	if !ok {
		return v
	}

	key := expr.String()
	symbol, ok := v.symbols[key]
	if !ok {
		// This symbol has not been assigned yet.
		// If this is a call or expression, mark the node
		// as stored in the symbol table.
		switch n := n.(type) {
		case *influxql.Call:
			if isMathFunction(n) {
				return v
			}
			v.calls[n] = struct{}{}
		case *influxql.VarRef:
			v.refs[n] = struct{}{}
		default:
			return v
		}

		// Determine the symbol name and the symbol type.
		symbolName := fmt.Sprintf("val%d", v.i)
		valuer := influxql.TypeValuerEval{
			TypeMapper: DefaultTypeMapper,
		}
		typ, _ := valuer.EvalType(expr)

		symbol = influxql.VarRef{
			Val:  symbolName,
			Type: typ,
		}

		// Assign this symbol to the symbol table if it is not presently there
		// and increment the value index number.
		v.symbols[key] = symbol
		v.i++
	}
	// Store the symbol for this expression so we can later rewrite
	// the query correctly.
	v.table[expr] = symbol
	return nil
}

func (v *valueMapper) rewriteExpr(expr influxql.Expr) influxql.Expr {
	symbol, ok := v.table[expr]
	if !ok {
		return expr
	}
	return &symbol
}

func validateTypes(stmt *influxql.SelectStatement) error {
	valuer := influxql.TypeValuerEval{
		TypeMapper: influxql.MultiTypeMapper(
			FunctionTypeMapper{},
			MathTypeMapper{},
		),
	}
	for _, f := range stmt.Fields {
		if _, err := valuer.EvalType(f.Expr); err != nil {
			return err
		}
	}
	return nil
}

// hasValidType returns true if there is at least one non-unknown type
// in the slice.
func hasValidType(refs []influxql.VarRef) bool {
	for _, ref := range refs {
		if ref.Type != influxql.Unknown {
			return true
		}
	}
	return false
}
