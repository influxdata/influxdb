package functions

import (
	"fmt"
	"log"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/ast"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/interpreter"
	"github.com/influxdata/platform/query/plan"
	"github.com/influxdata/platform/query/semantic"
)

const FilterKind = "filter"

type FilterOpSpec struct {
	Fn *semantic.FunctionExpression `json:"fn"`
}

var filterSignature = query.DefaultFunctionSignature()

func init() {
	//TODO(nathanielc): Use complete function signature here, or formalize soft kind validation instead of complete function validation.
	filterSignature.Params["fn"] = semantic.Function

	query.RegisterFunction(FilterKind, createFilterOpSpec, filterSignature)
	query.RegisterOpSpec(FilterKind, newFilterOp)
	plan.RegisterProcedureSpec(FilterKind, newFilterProcedure, FilterKind)
	execute.RegisterTransformation(FilterKind, createFilterTransformation)
}

func createFilterOpSpec(args query.Arguments, a *query.Administration) (query.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}
	f, err := args.GetRequiredFunction("fn")
	if err != nil {
		return nil, err
	}

	fn, err := interpreter.ResolveFunction(f)
	if err != nil {
		return nil, err
	}

	return &FilterOpSpec{
		Fn: fn,
	}, nil
}
func newFilterOp() query.OperationSpec {
	return new(FilterOpSpec)
}

func (s *FilterOpSpec) Kind() query.OperationKind {
	return FilterKind
}

type FilterProcedureSpec struct {
	Fn *semantic.FunctionExpression
}

func newFilterProcedure(qs query.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*FilterOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}

	return &FilterProcedureSpec{
		Fn: spec.Fn,
	}, nil
}

func (s *FilterProcedureSpec) Kind() plan.ProcedureKind {
	return FilterKind
}
func (s *FilterProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(FilterProcedureSpec)
	ns.Fn = s.Fn.Copy().(*semantic.FunctionExpression)
	return ns
}

func (s *FilterProcedureSpec) PushDownRules() []plan.PushDownRule {
	return []plan.PushDownRule{
		{
			Root:    FromKind,
			Through: []plan.ProcedureKind{GroupKind, LimitKind, RangeKind},
			Match: func(spec plan.ProcedureSpec) bool {
				// TODO(nathanielc): Remove once row functions support calling functions
				if _, ok := s.Fn.Body.(semantic.Expression); !ok {
					return false
				}
				fs := spec.(*FromProcedureSpec)
				if fs.Filter != nil {
					if _, ok := fs.Filter.Body.(semantic.Expression); !ok {
						return false
					}
				}
				return true
			},
		},
		{
			Root:    FilterKind,
			Through: []plan.ProcedureKind{GroupKind, LimitKind, RangeKind},
			Match: func(spec plan.ProcedureSpec) bool {
				// TODO(nathanielc): Remove once row functions support calling functions
				if _, ok := s.Fn.Body.(semantic.Expression); !ok {
					return false
				}
				fs := spec.(*FilterProcedureSpec)
				if _, ok := fs.Fn.Body.(semantic.Expression); !ok {
					return false
				}
				return true
			},
		},
	}
}

func (s *FilterProcedureSpec) PushDown(root *plan.Procedure, dup func() *plan.Procedure) {
	switch spec := root.Spec.(type) {
	case *FromProcedureSpec:
		if spec.FilterSet {
			spec.Filter = mergeArrowFunction(spec.Filter, s.Fn)
			return
		}
		spec.FilterSet = true
		spec.Filter = s.Fn
	case *FilterProcedureSpec:
		spec.Fn = mergeArrowFunction(spec.Fn, s.Fn)
	}
}

func mergeArrowFunction(a, b *semantic.FunctionExpression) *semantic.FunctionExpression {
	fn := a.Copy().(*semantic.FunctionExpression)

	aExp, aOK := a.Body.(semantic.Expression)
	bExp, bOK := b.Body.(semantic.Expression)

	if aOK && bOK {
		fn.Body = &semantic.LogicalExpression{
			Operator: ast.AndOperator,
			Left:     aExp,
			Right:    bExp,
		}
		return fn
	}

	// TODO(nathanielc): This code is unreachable while the current PushDownRule Match function is inplace.

	and := &semantic.LogicalExpression{
		Operator: ast.AndOperator,
		Left:     aExp,
		Right:    bExp,
	}

	// Create pass through arguments expression
	passThroughArgs := &semantic.ObjectExpression{
		Properties: make([]*semantic.Property, len(a.Params)),
	}
	for i, p := range a.Params {
		passThroughArgs.Properties[i] = &semantic.Property{
			Key: p.Key,
			//TODO(nathanielc): Construct valid IdentifierExpression with Declaration for the value.
			//Value: p.Key,
		}
	}

	if !aOK {
		// Rewrite left expression as a function call.
		and.Left = &semantic.CallExpression{
			Callee:    a.Copy().(*semantic.FunctionExpression),
			Arguments: passThroughArgs.Copy().(*semantic.ObjectExpression),
		}
	}
	if !bOK {
		// Rewrite right expression as a function call.
		and.Right = &semantic.CallExpression{
			Callee:    b.Copy().(*semantic.FunctionExpression),
			Arguments: passThroughArgs.Copy().(*semantic.ObjectExpression),
		}
	}
	return fn
}

func createFilterTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*FilterProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}
	cache := execute.NewTableBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)
	t, err := NewFilterTransformation(d, cache, s)
	if err != nil {
		return nil, nil, err
	}
	return t, d, nil
}

type filterTransformation struct {
	d     execute.Dataset
	cache execute.TableBuilderCache

	fn *execute.RowPredicateFn
}

func NewFilterTransformation(d execute.Dataset, cache execute.TableBuilderCache, spec *FilterProcedureSpec) (*filterTransformation, error) {
	fn, err := execute.NewRowPredicateFn(spec.Fn)
	if err != nil {
		return nil, err
	}

	return &filterTransformation{
		d:     d,
		cache: cache,
		fn:    fn,
	}, nil
}

func (t *filterTransformation) RetractTable(id execute.DatasetID, key query.GroupKey) error {
	return t.d.RetractTable(key)
}

func (t *filterTransformation) Process(id execute.DatasetID, b query.Table) error {
	builder, created := t.cache.TableBuilder(b.Key())
	if !created {
		return fmt.Errorf("filter found duplicate table with key: %v", b.Key())
	}
	execute.AddTableCols(b, builder)

	// Prepare the function for the column types.
	cols := b.Cols()
	if err := t.fn.Prepare(cols); err != nil {
		// TODO(nathanielc): Should we not fail the query for failed compilation?
		return err
	}

	// Append only matching rows to table
	return b.Do(func(cr query.ColReader) error {
		l := cr.Len()
		for i := 0; i < l; i++ {
			if pass, err := t.fn.Eval(i, cr); err != nil {
				log.Printf("failed to evaluate filter expression: %v", err)
				continue
			} else if !pass {
				// No match, skipping
				continue
			}
			execute.AppendRecord(i, cr, builder)
		}
		return nil
	})
}

func (t *filterTransformation) UpdateWatermark(id execute.DatasetID, mark execute.Time) error {
	return t.d.UpdateWatermark(mark)
}
func (t *filterTransformation) UpdateProcessingTime(id execute.DatasetID, pt execute.Time) error {
	return t.d.UpdateProcessingTime(pt)
}
func (t *filterTransformation) Finish(id execute.DatasetID, err error) {
	t.d.Finish(err)
}
