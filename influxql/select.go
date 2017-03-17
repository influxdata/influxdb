package influxql

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"
)

// SelectOptions are options that customize the select call.
type SelectOptions struct {
	// The lower bound for a select call.
	MinTime time.Time

	// The upper bound for a select call.
	MaxTime time.Time

	// Node to exclusively read from.
	// If zero, all nodes are used.
	NodeID uint64

	// An optional channel that, if closed, signals that the select should be
	// interrupted.
	InterruptCh <-chan struct{}

	// Maximum number of concurrent series.
	MaxSeriesN int
}

// Select executes stmt against ic and returns a list of iterators to stream from.
//
// Statements should have all rewriting performed before calling select(). This
// includes wildcard and source expansion.
func Select(stmt *SelectStatement, ic IteratorCreator, sopt *SelectOptions) ([]Iterator, error) {
	// Determine base options for iterators.
	opt, err := newIteratorOptionsStmt(stmt, sopt)
	if err != nil {
		return nil, err
	}
	return buildIterators(stmt, ic, opt)
}

func buildIterators(stmt *SelectStatement, ic IteratorCreator, opt IteratorOptions) ([]Iterator, error) {
	// Retrieve refs for each call and var ref.
	info := newSelectInfo(stmt)
	if len(info.calls) > 1 && len(info.refs) > 0 {
		return nil, errors.New("cannot select fields when selecting multiple aggregates")
	}

	// Determine auxiliary fields to be selected.
	opt.Aux = make([]VarRef, 0, len(info.refs))
	for ref := range info.refs {
		opt.Aux = append(opt.Aux, *ref)
	}
	sort.Sort(VarRefs(opt.Aux))

	// If there are multiple auxilary fields and no calls then construct an aux iterator.
	if len(info.calls) == 0 && len(info.refs) > 0 {
		return buildAuxIterators(stmt.Fields, ic, stmt.Sources, opt)
	}

	// Include auxiliary fields from top() and bottom()
	extraFields := 0
	for call := range info.calls {
		if call.Name == "top" || call.Name == "bottom" {
			for i := 1; i < len(call.Args)-1; i++ {
				ref := call.Args[i].(*VarRef)
				opt.Aux = append(opt.Aux, *ref)
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

	// Determine if there is one call and it is a selector.
	selector := false
	if len(info.calls) == 1 {
		for call := range info.calls {
			selector = IsSelector(call)
		}
	}

	return buildFieldIterators(fields, ic, stmt.Sources, opt, selector)
}

// buildAuxIterators creates a set of iterators from a single combined auxiliary iterator.
func buildAuxIterators(fields Fields, ic IteratorCreator, sources Sources, opt IteratorOptions) ([]Iterator, error) {
	// Create the auxiliary iterators for each source.
	inputs := make([]Iterator, 0, len(sources))
	if err := func() error {
		for _, source := range sources {
			switch source := source.(type) {
			case *Measurement:
				input, err := ic.CreateIterator(source, opt)
				if err != nil {
					return err
				}
				inputs = append(inputs, input)
			case *SubQuery:
				fields := make([]*Field, 0, len(opt.Aux))
				indexes := make([]IteratorMap, len(opt.Aux))
				offset := 0
			AUX:
				for i, name := range opt.Aux {
					// Search through the fields to find one that matches this auxiliary field.
					var match *Field
				FIELDS:
					for _, f := range source.Statement.Fields {
						if f.Name() == name.Val {
							match = f
							break
						} else if call, ok := f.Expr.(*Call); ok && (call.Name == "top" || call.Name == "bottom") {
							// We may match one of the arguments in "top" or "bottom".
							if len(call.Args) > 2 {
								for j, arg := range call.Args[1 : len(call.Args)-1] {
									if arg, ok := arg.(*VarRef); ok && arg.Val == name.Val {
										match = f
										// Increment the offset since we are looking for the tag
										// associated with this value rather than the value itself.
										offset += j + 1
										break FIELDS
									}
								}
							}
						}
					}

					// Look within the dimensions and create a field if we find it.
					if match == nil {
						for _, d := range source.Statement.Dimensions {
							if d, ok := d.Expr.(*VarRef); ok && name.Val == d.Val {
								fields = append(fields, &Field{
									Expr: &VarRef{
										Val:  d.Val,
										Type: Tag,
									},
								})
								indexes[i] = TagMap(d.Val)
								continue AUX
							}
						}
					}

					// There is no field that matches this name so signal this
					// should be a nil iterator.
					if match == nil {
						match = &Field{Expr: (*nilLiteral)(nil)}
					}
					fields = append(fields, match)
					indexes[i] = FieldMap(len(fields) + offset - 1)
				}

				// Check if we need any selectors within the selected fields.
				// If we have an expression that relies on the selector, we
				// need to include that even if it isn't referenced directly.
				var selector *Field
				for _, f := range source.Statement.Fields {
					if IsSelector(f.Expr) {
						selector = f
						break
					}
				}

				// There is a selector in the inner query. Now check if we have that selector
				// in the constructed fields list.
				if selector != nil {
					hasSelector := false
					for _, f := range fields {
						if _, ok := f.Expr.(*Call); ok {
							hasSelector = true
							break
						}
					}

					if !hasSelector {
						// Append the selector to the statement fields.
						fields = append(fields, selector)
					}
				}

				// If there are no fields, then we have nothing driving the iterator.
				// Skip this subquery since it only references tags.
				if len(fields) == 0 {
					continue
				}

				// Clone the statement and replace the fields with our custom ordering.
				stmt := source.Statement.Clone()
				stmt.Fields = fields

				subOpt, err := newIteratorOptionsSubstatement(stmt, opt)
				if err != nil {
					return err
				}

				itrs, err := buildIterators(stmt, ic, subOpt)
				if err != nil {
					return err
				}

				// Construct the iterators for the subquery.
				input := NewIteratorMapper(itrs, indexes, opt)
				// If there is a condition, filter it now.
				if opt.Condition != nil {
					input = NewFilterIterator(input, opt.Condition, opt)
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

	// Generate iterators for each field.
	itrs := make([]Iterator, len(fields))
	if err := func() error {
		for i, f := range fields {
			expr := Reduce(f.Expr, nil)
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
func buildAuxIterator(expr Expr, aitr AuxIterator, opt IteratorOptions) (Iterator, error) {
	switch expr := expr.(type) {
	case *VarRef:
		return aitr.Iterator(expr.Val, expr.Type), nil
	case *BinaryExpr:
		if rhs, ok := expr.RHS.(Literal); ok {
			// The right hand side is a literal. It is more common to have the RHS be a literal,
			// so we check that one first and have this be the happy path.
			if lhs, ok := expr.LHS.(Literal); ok {
				// We have two literals that couldn't be combined by Reduce.
				return nil, fmt.Errorf("unable to construct an iterator from two literals: LHS: %T, RHS: %T", lhs, rhs)
			}

			lhs, err := buildAuxIterator(expr.LHS, aitr, opt)
			if err != nil {
				return nil, err
			}
			return buildRHSTransformIterator(lhs, rhs, expr.Op, opt)
		} else if lhs, ok := expr.LHS.(Literal); ok {
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
	case *ParenExpr:
		return buildAuxIterator(expr.Expr, aitr, opt)
	case *nilLiteral:
		return &nilFloatIterator{}, nil
	default:
		return nil, fmt.Errorf("invalid expression type: %T", expr)
	}
}

// buildFieldIterators creates an iterator for each field expression.
func buildFieldIterators(fields Fields, ic IteratorCreator, sources Sources, opt IteratorOptions, selector bool) ([]Iterator, error) {
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
			itr, err := buildExprIterator(expr, ic, sources, opt, selector)
			if err != nil {
				return err
			} else if itr == nil {
				itr = &nilFloatIterator{}
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
		for i, f := range fields {
			if itrs[i] != nil {
				itrs[i] = aitr
				continue
			}

			expr := Reduce(f.Expr, nil)
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

	// If there is a limit or offset then apply it.
	if opt.Limit > 0 || opt.Offset > 0 {
		for i := range itrs {
			itrs[i] = NewLimitIterator(itrs[i], opt)
		}
	}

	return itrs, nil
}

// buildExprIterator creates an iterator for an expression.
func buildExprIterator(expr Expr, ic IteratorCreator, sources Sources, opt IteratorOptions, selector bool) (Iterator, error) {
	opt.Expr = expr
	b := exprIteratorBuilder{
		ic:       ic,
		sources:  sources,
		opt:      opt,
		selector: selector,
	}

	switch expr := expr.(type) {
	case *VarRef:
		return b.buildVarRefIterator(expr)
	case *Call:
		return b.buildCallIterator(expr)
	case *BinaryExpr:
		return b.buildBinaryExprIterator(expr)
	case *ParenExpr:
		return buildExprIterator(expr.Expr, ic, sources, opt, selector)
	case *nilLiteral:
		return &nilFloatIterator{}, nil
	default:
		return nil, fmt.Errorf("invalid expression type: %T", expr)
	}
}

type exprIteratorBuilder struct {
	ic       IteratorCreator
	sources  Sources
	opt      IteratorOptions
	selector bool
}

func (b *exprIteratorBuilder) buildVarRefIterator(expr *VarRef) (Iterator, error) {
	inputs := make([]Iterator, 0, len(b.sources))
	if err := func() error {
		for _, source := range b.sources {
			switch source := source.(type) {
			case *Measurement:
				input, err := b.ic.CreateIterator(source, b.opt)
				if err != nil {
					return err
				}
				inputs = append(inputs, input)
			case *SubQuery:
				info := newSelectInfo(source.Statement)
				if len(info.calls) > 1 && len(info.refs) > 0 {
					return errors.New("cannot select fields when selecting multiple aggregates from subquery")
				}

				if input, err := func() (Iterator, error) {
					// Look for the field that matches this name.
					i, e := source.Statement.FieldExprByName(expr.Val)
					if e == nil {
						return nil, nil
					}
					f := source.Statement.Fields[i]

					// Retrieve the select info for the substatement.
					info := newSelectInfo(source.Statement)
					if len(info.calls) == 0 && len(info.refs) > 0 {
						// There are no aggregates in the subquery, so
						// it is just a raw query. Match the auxiliary
						// fields to the other fields and pass as-is.
						subOpt, err := newIteratorOptionsSubstatement(source.Statement, b.opt)
						if err != nil {
							return nil, err
						}

						subOpt.Aux = make([]VarRef, len(b.opt.Aux))
						for i, ref := range b.opt.Aux {
							if ref.Type != Tag {
								for _, f := range source.Statement.Fields {
									if f.Name() == ref.Val {
										subOpt.Aux[i] = *(e.(*VarRef))
										break
									}
								}
							}

							// Look in the dimensions.
							if subOpt.Aux[i].Val == "" && (ref.Type == Unknown || ref.Type == Tag) {
								for _, d := range source.Statement.Dimensions {
									if d, ok := d.Expr.(*VarRef); ok && ref.Val == d.Val {
										subOpt.Aux[i] = VarRef{
											Val:  d.Val,
											Type: Tag,
										}
										break
									}
								}
							}
						}
						itr, err := buildExprIterator(e, b.ic, source.Statement.Sources, subOpt, false)
						if err != nil {
							return nil, err
						}

						if b.opt.Condition != nil {
							itr = NewFilterIterator(itr, b.opt.Condition, subOpt)
						}
						return itr, nil
					}

					// Reduce the expression to remove parenthesis.
					e = Reduce(e, nil)

					switch e := e.(type) {
					case *VarRef:
						// If the field we selected is a variable
						// reference, then we need to find the associated
						// selector (and ensure it is actually a selector)
						// and build the iterator off of that.
						selector := info.FindSelector()
						if selector == nil {
							return nil, nil
						}

						subOpt, err := newIteratorOptionsSubstatement(source.Statement, b.opt)
						if err != nil {
							return nil, err
						}

						// If we have top() or bottom(), we need to
						// fill the aux fields with what is in the
						// function even if we aren't using the result.
						if call, ok := f.Expr.(*Call); ok && (call.Name == "top" || call.Name == "bottom") {
							// Prepare the auxiliary fields for this call.
							subOpt.Aux = make([]VarRef, 0, len(call.Args)-1)

							// Look for the auxiliary field inside of the call.
							// If we can't find it, then add it to the end.
							hasVarRef := false
							for _, arg := range call.Args[1 : len(call.Args)-1] {
								if arg, ok := arg.(*VarRef); ok {
									subOpt.Aux = append(subOpt.Aux, *arg)
									if arg.Val == e.Val {
										hasVarRef = true
									}
								}
							}

							// We need to attach the actual auxiliary field we're looking
							// for if it wasn't in the argument list.
							// This is for SELECT top(value, 1), host.
							if !hasVarRef {
								subOpt.Aux = append(subOpt.Aux, *e)
							}
						} else {
							subOpt.Aux = []VarRef{*e}
						}

						// Construct the selector iterator.
						input, err := buildExprIterator(selector, b.ic, source.Statement.Sources, subOpt, true)
						if err != nil {
							return nil, err
						}

						// Filter the iterator.
						if b.opt.Condition != nil {
							input = NewFilterIterator(input, b.opt.Condition, subOpt)
						}

						// Create an auxiliary iterator.
						aitr := NewAuxIterator(input, subOpt)
						itr := aitr.Iterator(e.Val, e.Type)
						aitr.Background()
						return itr, nil
					case *Call:
						subOpt, err := newIteratorOptionsSubstatement(source.Statement, b.opt)
						if err != nil {
							return nil, err
						}

						if len(b.opt.Aux) > 0 {
							subOpt.Aux = make([]VarRef, len(b.opt.Aux))
							for i, ref := range b.opt.Aux {
								_, expr := source.Statement.FieldExprByName(ref.Val)
								if expr != nil {
									v, ok := expr.(*VarRef)
									if ok {
										subOpt.Aux[i] = *v
										continue
									}
								}

								if ref.Type == Unknown || ref.Type == Tag {
									for _, d := range source.Statement.Dimensions {
										if d, ok := d.Expr.(*VarRef); ok && ref.Val == d.Val {
											subOpt.Aux[i] = VarRef{
												Val:  d.Val,
												Type: Tag,
											}
											break
										}
									}
								}
							}
						}

						// Check if this is a selector or not and
						// create the iterator directly.
						selector := len(info.calls) == 1 && IsSelector(e)
						itr, err := buildExprIterator(e, b.ic, source.Statement.Sources, subOpt, selector)
						if err != nil {
							return nil, err
						}

						if b.opt.Condition != nil {
							itr = NewFilterIterator(itr, b.opt.Condition, subOpt)
						}
						return itr, nil
					case *BinaryExpr:
						// Retrieve the calls and references for this binary expression.
						// There should be no mixing of calls and refs.
						i := selectInfo{
							calls: make(map[*Call]struct{}),
							refs:  make(map[*VarRef]struct{}),
						}
						Walk(&i, e)

						opt, err := newIteratorOptionsSubstatement(source.Statement, b.opt)
						if err != nil {
							return nil, err
						}

						if len(i.refs) > 0 {
							if len(b.opt.Aux) > 0 {
								// Catch this so we don't cause a panic. This
								// is too difficult to implement now though.
								// TODO(jsternberg): Implement this.
								return nil, errors.New("unsupported")
							}

							selector := info.FindSelector()
							if selector == nil {
								return nil, nil
							}

							// Prepare the auxiliary iterators with the refs we care about.
							opt.Aux = make([]VarRef, 0, len(i.refs))
							for ref := range i.refs {
								opt.Aux = append(opt.Aux, *ref)
							}

							input, err := buildExprIterator(selector, b.ic, source.Statement.Sources, opt, true)
							if err != nil {
								return nil, err
							}

							aitr := NewAuxIterator(input, opt)
							itr, err := buildAuxIterator(e, aitr, opt)
							if err != nil {
								aitr.Close()
								return nil, err
							}
							aitr.Background()
							return itr, nil
						}

						// Determine if this expression is a selector or not.
						selector := len(i.calls) == 1 && len(info.calls) == 1
						// Prepare the auxiliary fields we need.
						if len(b.opt.Aux) > 0 {
							opt.Aux = make([]VarRef, len(b.opt.Aux))
							for i, ref := range b.opt.Aux {
								_, expr := source.Statement.FieldExprByName(ref.Val)
								if v, ok := expr.(*VarRef); ok {
									opt.Aux[i] = *v
								}
							}
						}

						// Build the iterator using the options we created.
						return buildExprIterator(e, b.ic, source.Statement.Sources, opt, selector)
					default:
						panic(fmt.Sprintf("unsupported use of %T in a subquery", e))
					}
				}(); err != nil {
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

func (b *exprIteratorBuilder) buildCallIterator(expr *Call) (Iterator, error) {
	// TODO(jsternberg): Refactor this. This section needs to die in a fire.
	switch expr.Name {
	case "distinct":
		input, err := buildExprIterator(expr.Args[0].(*VarRef), b.ic, b.sources, b.opt, b.selector)
		if err != nil {
			return nil, err
		}
		input, err = NewDistinctIterator(input, b.opt)
		if err != nil {
			return nil, err
		}
		return NewIntervalIterator(input, b.opt), nil
	case "sample":
		input, err := buildExprIterator(expr.Args[0], b.ic, b.sources, b.opt, b.selector)
		if err != nil {
			return nil, err
		}
		size := expr.Args[1].(*IntegerLiteral)

		return newSampleIterator(input, b.opt, int(size.Val))
	case "holt_winters", "holt_winters_with_fit":
		input, err := buildExprIterator(expr.Args[0], b.ic, b.sources, b.opt, b.selector)
		if err != nil {
			return nil, err
		}
		h := expr.Args[1].(*IntegerLiteral)
		m := expr.Args[2].(*IntegerLiteral)

		includeFitData := "holt_winters_with_fit" == expr.Name

		interval := b.opt.Interval.Duration
		// Redefine interval to be unbounded to capture all aggregate results
		opt := b.opt
		opt.StartTime = MinTime
		opt.EndTime = MaxTime
		opt.Interval = Interval{}

		return newHoltWintersIterator(input, opt, int(h.Val), int(m.Val), includeFitData, interval)
	case "derivative", "non_negative_derivative", "difference", "moving_average", "elapsed":
		opt := b.opt
		if !opt.Interval.IsZero() {
			if opt.Ascending {
				opt.StartTime -= int64(opt.Interval.Duration)
			} else {
				opt.EndTime += int64(opt.Interval.Duration)
			}
		}

		input, err := buildExprIterator(expr.Args[0], b.ic, b.sources, opt, b.selector)
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
		case "difference":
			return newDifferenceIterator(input, opt)
		case "moving_average":
			n := expr.Args[1].(*IntegerLiteral)
			if n.Val > 1 && !b.opt.Interval.IsZero() {
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
		input, err := buildExprIterator(expr.Args[0], b.ic, b.sources, b.opt, b.selector)
		if err != nil {
			return nil, err
		}
		return newCumulativeSumIterator(input, b.opt)
	}

	itr, err := func() (Iterator, error) {
		switch expr.Name {
		case "count":
			switch arg0 := expr.Args[0].(type) {
			case *Call:
				if arg0.Name == "distinct" {
					input, err := buildExprIterator(arg0, b.ic, b.sources, b.opt, b.selector)
					if err != nil {
						return nil, err
					}
					return newCountIterator(input, b.opt)
				}
			}
			fallthrough
		case "min", "max", "sum", "first", "last", "mean":
			inputs := make([]Iterator, 0, len(b.sources))
			if err := func() error {
				for _, source := range b.sources {
					switch source := source.(type) {
					case *Measurement:
						input, err := b.ic.CreateIterator(source, b.opt)
						if err != nil {
							return err
						}
						inputs = append(inputs, input)
					case *SubQuery:
						// Identify the name of the field we are using.
						arg0 := expr.Args[0].(*VarRef)

						input, err := buildExprIterator(arg0, b.ic, []Source{source}, b.opt, b.selector)
						if err != nil {
							return err
						}

						if b.opt.Condition != nil {
							input = NewFilterIterator(input, b.opt.Condition, b.opt)
						}

						// Wrap the result in a call iterator.
						i, err := NewCallIterator(input, b.opt)
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

			itr, err := Iterators(inputs).Merge(b.opt)
			if err != nil {
				Iterators(inputs).Close()
				return nil, err
			} else if itr == nil {
				itr = &nilFloatIterator{}
			}
			return itr, nil
		case "median":
			opt := b.opt
			opt.Ordered = true
			input, err := buildExprIterator(expr.Args[0].(*VarRef), b.ic, b.sources, opt, false)
			if err != nil {
				return nil, err
			}
			return newMedianIterator(input, opt)
		case "mode":
			input, err := buildExprIterator(expr.Args[0].(*VarRef), b.ic, b.sources, b.opt, false)
			if err != nil {
				return nil, err
			}
			return NewModeIterator(input, b.opt)
		case "stddev":
			input, err := buildExprIterator(expr.Args[0].(*VarRef), b.ic, b.sources, b.opt, false)
			if err != nil {
				return nil, err
			}
			return newStddevIterator(input, b.opt)
		case "spread":
			// OPTIMIZE(benbjohnson): convert to map/reduce
			input, err := buildExprIterator(expr.Args[0].(*VarRef), b.ic, b.sources, b.opt, false)
			if err != nil {
				return nil, err
			}
			return newSpreadIterator(input, b.opt)
		case "top":
			var tags []int
			if len(expr.Args) < 2 {
				return nil, fmt.Errorf("top() requires 2 or more arguments, got %d", len(expr.Args))
			} else if len(expr.Args) > 2 {
				// We need to find the indices of where the tag values are stored in Aux
				// This section is O(n^2), but for what should be a low value.
				for i := 1; i < len(expr.Args)-1; i++ {
					ref := expr.Args[i].(*VarRef)
					for index, aux := range b.opt.Aux {
						if aux.Val == ref.Val {
							tags = append(tags, index)
							break
						}
					}
				}
			}

			input, err := buildExprIterator(expr.Args[0].(*VarRef), b.ic, b.sources, b.opt, false)
			if err != nil {
				return nil, err
			}
			n := expr.Args[len(expr.Args)-1].(*IntegerLiteral)
			return newTopIterator(input, b.opt, n, tags)
		case "bottom":
			var tags []int
			if len(expr.Args) < 2 {
				return nil, fmt.Errorf("bottom() requires 2 or more arguments, got %d", len(expr.Args))
			} else if len(expr.Args) > 2 {
				// We need to find the indices of where the tag values are stored in Aux
				// This section is O(n^2), but for what should be a low value.
				for i := 1; i < len(expr.Args)-1; i++ {
					ref := expr.Args[i].(*VarRef)
					for index, aux := range b.opt.Aux {
						if aux.Val == ref.Val {
							tags = append(tags, index)
							break
						}
					}
				}
			}

			input, err := buildExprIterator(expr.Args[0].(*VarRef), b.ic, b.sources, b.opt, false)
			if err != nil {
				return nil, err
			}
			n := expr.Args[len(expr.Args)-1].(*IntegerLiteral)
			return newBottomIterator(input, b.opt, n, tags)
		case "percentile":
			opt := b.opt
			opt.Ordered = true
			input, err := buildExprIterator(expr.Args[0].(*VarRef), b.ic, b.sources, opt, false)
			if err != nil {
				return nil, err
			}
			var percentile float64
			switch arg := expr.Args[1].(type) {
			case *NumberLiteral:
				percentile = arg.Val
			case *IntegerLiteral:
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

	if !b.selector || !b.opt.Interval.IsZero() {
		if expr.Name != "top" && expr.Name != "bottom" {
			itr = NewIntervalIterator(itr, b.opt)
		}
		if !b.opt.Interval.IsZero() && b.opt.Fill != NoFill {
			itr = NewFillIterator(itr, expr, b.opt)
		}
	}
	if b.opt.InterruptCh != nil {
		itr = NewInterruptIterator(itr, b.opt.InterruptCh)
	}
	return itr, nil
}

func (b *exprIteratorBuilder) buildBinaryExprIterator(expr *BinaryExpr) (Iterator, error) {
	if rhs, ok := expr.RHS.(Literal); ok {
		// The right hand side is a literal. It is more common to have the RHS be a literal,
		// so we check that one first and have this be the happy path.
		if lhs, ok := expr.LHS.(Literal); ok {
			// We have two literals that couldn't be combined by Reduce.
			return nil, fmt.Errorf("unable to construct an iterator from two literals: LHS: %T, RHS: %T", lhs, rhs)
		}

		lhs, err := buildExprIterator(expr.LHS, b.ic, b.sources, b.opt, IsSelector(expr.LHS))
		if err != nil {
			return nil, err
		}
		return buildRHSTransformIterator(lhs, rhs, expr.Op, b.opt)
	} else if lhs, ok := expr.LHS.(Literal); ok {
		rhs, err := buildExprIterator(expr.RHS, b.ic, b.sources, b.opt, IsSelector(expr.RHS))
		if err != nil {
			return nil, err
		}
		return buildLHSTransformIterator(lhs, rhs, expr.Op, b.opt)
	} else {
		// We have two iterators. Combine them into a single iterator.
		lhs, err := buildExprIterator(expr.LHS, b.ic, b.sources, b.opt, false)
		if err != nil {
			return nil, err
		}
		rhs, err := buildExprIterator(expr.RHS, b.ic, b.sources, b.opt, false)
		if err != nil {
			return nil, err
		}
		return buildTransformIterator(lhs, rhs, expr.Op, b.opt)
	}
}

func buildRHSTransformIterator(lhs Iterator, rhs Literal, op Token, opt IteratorOptions) (Iterator, error) {
	fn := binaryExprFunc(iteratorDataType(lhs), literalDataType(rhs), op)
	switch fn := fn.(type) {
	case func(float64, float64) float64:
		var input FloatIterator
		switch lhs := lhs.(type) {
		case FloatIterator:
			input = lhs
		case IntegerIterator:
			input = &integerFloatCastIterator{input: lhs}
		default:
			return nil, fmt.Errorf("type mismatch on LHS, unable to use %T as a FloatIterator", lhs)
		}

		var val float64
		switch rhs := rhs.(type) {
		case *NumberLiteral:
			val = rhs.Val
		case *IntegerLiteral:
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
		case *IntegerLiteral:
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
		default:
			return nil, fmt.Errorf("type mismatch on LHS, unable to use %T as a FloatIterator", lhs)
		}

		var val float64
		switch rhs := rhs.(type) {
		case *NumberLiteral:
			val = rhs.Val
		case *IntegerLiteral:
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
		case *IntegerLiteral:
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
		case *IntegerLiteral:
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
	}
	return nil, fmt.Errorf("unable to construct rhs transform iterator from %T and %T", lhs, rhs)
}

func buildLHSTransformIterator(lhs Literal, rhs Iterator, op Token, opt IteratorOptions) (Iterator, error) {
	fn := binaryExprFunc(literalDataType(lhs), iteratorDataType(rhs), op)
	switch fn := fn.(type) {
	case func(float64, float64) float64:
		var input FloatIterator
		switch rhs := rhs.(type) {
		case FloatIterator:
			input = rhs
		case IntegerIterator:
			input = &integerFloatCastIterator{input: rhs}
		default:
			return nil, fmt.Errorf("type mismatch on RHS, unable to use %T as a FloatIterator", rhs)
		}

		var val float64
		switch lhs := lhs.(type) {
		case *NumberLiteral:
			val = lhs.Val
		case *IntegerLiteral:
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
		case *IntegerLiteral:
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
		default:
			return nil, fmt.Errorf("type mismatch on RHS, unable to use %T as a FloatIterator", rhs)
		}

		var val float64
		switch lhs := lhs.(type) {
		case *NumberLiteral:
			val = lhs.Val
		case *IntegerLiteral:
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
		case *IntegerLiteral:
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
		case *IntegerLiteral:
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
	}
	return nil, fmt.Errorf("unable to construct lhs transform iterator from %T and %T", lhs, rhs)
}

func buildTransformIterator(lhs Iterator, rhs Iterator, op Token, opt IteratorOptions) (Iterator, error) {
	fn := binaryExprFunc(iteratorDataType(lhs), iteratorDataType(rhs), op)
	switch fn := fn.(type) {
	case func(float64, float64) float64:
		var left FloatIterator
		switch lhs := lhs.(type) {
		case FloatIterator:
			left = lhs
		case IntegerIterator:
			left = &integerFloatCastIterator{input: lhs}
		default:
			return nil, fmt.Errorf("type mismatch on LHS, unable to use %T as a FloatIterator", lhs)
		}

		var right FloatIterator
		switch rhs := rhs.(type) {
		case FloatIterator:
			right = rhs
		case IntegerIterator:
			right = &integerFloatCastIterator{input: rhs}
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
	case func(float64, float64) bool:
		var left FloatIterator
		switch lhs := lhs.(type) {
		case FloatIterator:
			left = lhs
		case IntegerIterator:
			left = &integerFloatCastIterator{input: lhs}
		default:
			return nil, fmt.Errorf("type mismatch on LHS, unable to use %T as a FloatIterator", lhs)
		}

		var right FloatIterator
		switch rhs := rhs.(type) {
		case FloatIterator:
			right = rhs
		case IntegerIterator:
			right = &integerFloatCastIterator{input: rhs}
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
			return nil, fmt.Errorf("type mismatch on LHS, unable to use %T as a IntegerIterator", rhs)
		}
		return newIntegerBooleanExprIterator(left, right, opt, fn), nil
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

func literalDataType(lit Literal) DataType {
	switch lit.(type) {
	case *NumberLiteral:
		return Float
	case *IntegerLiteral:
		return Integer
	case *StringLiteral:
		return String
	case *BooleanLiteral:
		return Boolean
	default:
		return Unknown
	}
}

func binaryExprFunc(typ1 DataType, typ2 DataType, op Token) interface{} {
	var fn interface{}
	switch typ1 {
	case Float:
		fn = floatBinaryExprFunc(op)
	case Integer:
		switch typ2 {
		case Float:
			fn = floatBinaryExprFunc(op)
		default:
			fn = integerBinaryExprFunc(op)
		}
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
	case MOD:
		return func(lhs, rhs float64) float64 { return math.Mod(lhs, rhs) }
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
		return func(lhs, rhs int64) float64 {
			if rhs == 0 {
				return float64(0)
			}
			return float64(lhs) / float64(rhs)
		}
	case MOD:
		return func(lhs, rhs int64) int64 {
			if rhs == 0 {
				return int64(0)
			}
			return lhs % rhs
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
