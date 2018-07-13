package functions

import (
	"fmt"
	"math"
	"time"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/interpreter"
	"github.com/influxdata/platform/query/plan"
	"github.com/influxdata/platform/query/semantic"
)

const DerivativeKind = "derivative"

type DerivativeOpSpec struct {
	Unit        query.Duration `json:"unit"`
	NonNegative bool           `json:"non_negative"`
	Columns     []string       `json:"columns"`
	TimeSrc     string         `json:"time_src"`
}

var derivativeSignature = query.DefaultFunctionSignature()

func init() {
	derivativeSignature.Params["unit"] = semantic.Duration
	derivativeSignature.Params["nonNegative"] = semantic.Bool
	derivativeSignature.Params["columns"] = semantic.NewArrayType(semantic.String)
	derivativeSignature.Params["timeSrc"] = semantic.String

	query.RegisterFunction(DerivativeKind, createDerivativeOpSpec, derivativeSignature)
	query.RegisterOpSpec(DerivativeKind, newDerivativeOp)
	plan.RegisterProcedureSpec(DerivativeKind, newDerivativeProcedure, DerivativeKind)
	execute.RegisterTransformation(DerivativeKind, createDerivativeTransformation)
}

func createDerivativeOpSpec(args query.Arguments, a *query.Administration) (query.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}

	spec := new(DerivativeOpSpec)

	if unit, ok, err := args.GetDuration("unit"); err != nil {
		return nil, err
	} else if ok {
		spec.Unit = unit
	} else {
		//Default is 1s
		spec.Unit = query.Duration(time.Second)
	}

	if nn, ok, err := args.GetBool("nonNegative"); err != nil {
		return nil, err
	} else if ok {
		spec.NonNegative = nn
	}
	if timeCol, ok, err := args.GetString("timeSrc"); err != nil {
		return nil, err
	} else if ok {
		spec.TimeSrc = timeCol
	} else {
		spec.TimeSrc = execute.DefaultTimeColLabel
	}

	if cols, ok, err := args.GetArray("columns", semantic.String); err != nil {
		return nil, err
	} else if ok {
		columns, err := interpreter.ToStringArray(cols)
		if err != nil {
			return nil, err
		}
		spec.Columns = columns
	} else {
		spec.Columns = []string{execute.DefaultValueColLabel}
	}
	return spec, nil
}

func newDerivativeOp() query.OperationSpec {
	return new(DerivativeOpSpec)
}

func (s *DerivativeOpSpec) Kind() query.OperationKind {
	return DerivativeKind
}

type DerivativeProcedureSpec struct {
	Unit        query.Duration `json:"unit"`
	NonNegative bool           `json:"non_negative"`
	Columns     []string       `json:"columns"`
	TimeCol     string         `json:"time_col"`
}

func newDerivativeProcedure(qs query.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*DerivativeOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}

	return &DerivativeProcedureSpec{
		Unit:        spec.Unit,
		NonNegative: spec.NonNegative,
		Columns:     spec.Columns,
		TimeCol:     spec.TimeSrc,
	}, nil
}

func (s *DerivativeProcedureSpec) Kind() plan.ProcedureKind {
	return DerivativeKind
}
func (s *DerivativeProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(DerivativeProcedureSpec)
	*ns = *s
	if s.Columns != nil {
		ns.Columns = make([]string, len(s.Columns))
		copy(ns.Columns, s.Columns)
	}
	return ns
}

func createDerivativeTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*DerivativeProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}
	cache := execute.NewTableBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)
	t := NewDerivativeTransformation(d, cache, s)
	return t, d, nil
}

type derivativeTransformation struct {
	d     execute.Dataset
	cache execute.TableBuilderCache

	unit        time.Duration
	nonNegative bool
	columns     []string
	timeCol     string
}

func NewDerivativeTransformation(d execute.Dataset, cache execute.TableBuilderCache, spec *DerivativeProcedureSpec) *derivativeTransformation {
	return &derivativeTransformation{
		d:           d,
		cache:       cache,
		unit:        time.Duration(spec.Unit),
		nonNegative: spec.NonNegative,
		columns:     spec.Columns,
		timeCol:     spec.TimeCol,
	}
}

func (t *derivativeTransformation) RetractTable(id execute.DatasetID, key query.GroupKey) error {
	return t.d.RetractTable(key)
}

func (t *derivativeTransformation) Process(id execute.DatasetID, tbl query.Table) error {
	builder, created := t.cache.TableBuilder(tbl.Key())
	if !created {
		return fmt.Errorf("derivative found duplicate table with key: %v", tbl.Key())
	}
	cols := tbl.Cols()
	derivatives := make([]*derivative, len(cols))
	timeIdx := -1
	for j, c := range cols {
		found := false
		for _, label := range t.columns {
			if c.Label == label {
				found = true
				break
			}
		}
		if c.Label == t.timeCol {
			timeIdx = j
		}

		if found {
			dc := c
			// Derivative always results in a float
			dc.Type = query.TFloat
			builder.AddCol(dc)
			derivatives[j] = newDerivative(j, t.unit, t.nonNegative)
		} else {
			builder.AddCol(c)
		}
	}
	if timeIdx < 0 {
		return fmt.Errorf("no column %q exists", t.timeCol)
	}

	// We need to drop the first row since its derivative is undefined
	firstIdx := 1
	return tbl.Do(func(cr query.ColReader) error {
		l := cr.Len()
		for j, c := range cols {
			d := derivatives[j]
			switch c.Type {
			case query.TBool:
				builder.AppendBools(j, cr.Bools(j)[firstIdx:])
			case query.TInt:
				if d != nil {
					for i := 0; i < l; i++ {
						time := cr.Times(timeIdx)[i]
						v := d.updateInt(time, cr.Ints(j)[i])
						if i != 0 || firstIdx == 0 {
							builder.AppendFloat(j, v)
						}
					}
				} else {
					builder.AppendInts(j, cr.Ints(j)[firstIdx:])
				}
			case query.TUInt:
				if d != nil {
					for i := 0; i < l; i++ {
						time := cr.Times(timeIdx)[i]
						v := d.updateUInt(time, cr.UInts(j)[i])
						if i != 0 || firstIdx == 0 {
							builder.AppendFloat(j, v)
						}
					}
				} else {
					builder.AppendUInts(j, cr.UInts(j)[firstIdx:])
				}
			case query.TFloat:
				if d != nil {
					for i := 0; i < l; i++ {
						time := cr.Times(timeIdx)[i]
						v := d.updateFloat(time, cr.Floats(j)[i])
						if i != 0 || firstIdx == 0 {
							builder.AppendFloat(j, v)
						}
					}
				} else {
					builder.AppendFloats(j, cr.Floats(j)[firstIdx:])
				}
			case query.TString:
				builder.AppendStrings(j, cr.Strings(j)[firstIdx:])
			case query.TTime:
				builder.AppendTimes(j, cr.Times(j)[firstIdx:])
			}
		}
		// Now that we skipped the first row, start at 0 for the rest of the batches
		firstIdx = 0
		return nil
	})
}

func (t *derivativeTransformation) UpdateWatermark(id execute.DatasetID, mark execute.Time) error {
	return t.d.UpdateWatermark(mark)
}
func (t *derivativeTransformation) UpdateProcessingTime(id execute.DatasetID, pt execute.Time) error {
	return t.d.UpdateProcessingTime(pt)
}
func (t *derivativeTransformation) Finish(id execute.DatasetID, err error) {
	t.d.Finish(err)
}

func newDerivative(col int, unit time.Duration, nonNegative bool) *derivative {
	return &derivative{
		col:         col,
		first:       true,
		unit:        float64(unit),
		nonNegative: nonNegative,
	}
}

type derivative struct {
	col         int
	first       bool
	unit        float64
	nonNegative bool

	pIntValue   int64
	pUIntValue  uint64
	pFloatValue float64
	pTime       execute.Time
}

func (d *derivative) updateInt(t execute.Time, v int64) float64 {
	if d.first {
		d.pTime = t
		d.pIntValue = v
		d.first = false
		return math.NaN()
	}

	diff := float64(v - d.pIntValue)
	if d.nonNegative && diff < 0 {
		//TODO(nathanielc): Should we return null when we have null support
		// Or should we assume the previous is 0?
		diff = float64(v)
	}
	elapsed := float64(time.Duration(t-d.pTime)) / d.unit

	d.pTime = t
	d.pIntValue = v

	if d.nonNegative && diff < 0 {
		//TODO(nathanielc): Should we return null when we have null support
		// Or should we assume the previous is 0?
		return float64(v)
	}

	return diff / elapsed
}
func (d *derivative) updateUInt(t execute.Time, v uint64) float64 {
	if d.first {
		d.pTime = t
		d.pUIntValue = v
		d.first = false
		return math.NaN()
	}

	var diff float64
	if d.pUIntValue > v {
		// Prevent uint64 overflow by applying the negative sign after the conversion to a float64.
		diff = float64(d.pUIntValue-v) * -1
	} else {
		diff = float64(v - d.pUIntValue)
	}
	if d.nonNegative && diff < 0 {
		//TODO(nathanielc): Should we return null when we have null support
		// Or should we assume the previous is 0?
		diff = float64(v)
	}
	elapsed := float64(time.Duration(t-d.pTime)) / d.unit

	d.pTime = t
	d.pUIntValue = v

	return diff / elapsed
}
func (d *derivative) updateFloat(t execute.Time, v float64) float64 {
	if d.first {
		d.pTime = t
		d.pFloatValue = v
		d.first = false
		return math.NaN()
	}

	diff := v - d.pFloatValue
	if d.nonNegative && diff < 0 {
		//TODO(nathanielc): Should we return null when we have null support
		// Or should we assume the previous is 0?
		diff = v
	}
	elapsed := float64(time.Duration(t-d.pTime)) / d.unit

	d.pTime = t
	d.pFloatValue = v

	return diff / elapsed
}
