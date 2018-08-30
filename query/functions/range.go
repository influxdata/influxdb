package functions

import (
	"fmt"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/plan"
	"github.com/influxdata/platform/query/semantic"
	"github.com/influxdata/platform/query/values"
	"github.com/pkg/errors"
)

const RangeKind = "range"

type RangeOpSpec struct {
	Start    query.Time `json:"start"`
	Stop     query.Time `json:"stop"`
	TimeCol  string     `json:"timeCol"`
	StartCol string     `json:"startCol"`
	StopCol  string     `json:"stopCol"`
}

var rangeSignature = query.DefaultFunctionSignature()

func init() {
	rangeSignature.Params["start"] = semantic.Time
	rangeSignature.Params["stop"] = semantic.Time
	rangeSignature.Params["timeCol"] = semantic.String
	rangeSignature.Params["startCol"] = semantic.String
	rangeSignature.Params["stopCol"] = semantic.String

	query.RegisterFunction(RangeKind, createRangeOpSpec, rangeSignature)
	query.RegisterOpSpec(RangeKind, newRangeOp)
	plan.RegisterProcedureSpec(RangeKind, newRangeProcedure, RangeKind)
	// TODO register a range transformation. Currently range is only supported if it is pushed down into a select procedure.
	execute.RegisterTransformation(RangeKind, createRangeTransformation)
}

func createRangeOpSpec(args query.Arguments, a *query.Administration) (query.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}
	start, err := args.GetRequiredTime("start")
	if err != nil {
		return nil, err
	}
	spec := &RangeOpSpec{
		Start: start,
	}

	if stop, ok, err := args.GetTime("stop"); err != nil {
		return nil, err
	} else if ok {
		spec.Stop = stop
	} else {
		// Make stop time implicit "now"
		spec.Stop = query.Now
	}

	if col, ok, err := args.GetString("timeCol"); err != nil {
		return nil, err
	} else if ok {
		spec.TimeCol = col
	} else {
		spec.TimeCol = execute.DefaultTimeColLabel
	}

	if label, ok, err := args.GetString("startCol"); err != nil {
		return nil, err
	} else if ok {
		spec.StartCol = label
	} else {
		spec.StartCol = execute.DefaultStartColLabel
	}

	if label, ok, err := args.GetString("stopCol"); err != nil {
		return nil, err
	} else if ok {
		spec.StopCol = label
	} else {
		spec.StopCol = execute.DefaultStopColLabel
	}

	return spec, nil
}

func newRangeOp() query.OperationSpec {
	return new(RangeOpSpec)
}

func (s *RangeOpSpec) Kind() query.OperationKind {
	return RangeKind
}

type RangeProcedureSpec struct {
	Bounds   query.Bounds
	TimeCol  string
	StartCol string
	StopCol  string
}

func newRangeProcedure(qs query.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*RangeOpSpec)

	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}

	if spec.TimeCol == "" {
		spec.TimeCol = execute.DefaultTimeColLabel
	}

	return &RangeProcedureSpec{
		Bounds: query.Bounds{
			Start: spec.Start,
			Stop:  spec.Stop,
		},
		TimeCol:  spec.TimeCol,
		StartCol: spec.StartCol,
		StopCol:  spec.StopCol,
	}, nil
}

func (s *RangeProcedureSpec) Kind() plan.ProcedureKind {
	return RangeKind
}
func (s *RangeProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(RangeProcedureSpec)
	ns.Bounds = s.Bounds
	return ns
}

func (s *RangeProcedureSpec) PushDownRules() []plan.PushDownRule {
	return []plan.PushDownRule{{
		Root:    FromKind,
		Through: []plan.ProcedureKind{GroupKind, LimitKind, FilterKind},
		Match: func(spec plan.ProcedureSpec) bool {
			return s.TimeCol == "_time"
		},
	}}
}

func (s *RangeProcedureSpec) PushDown(root *plan.Procedure, dup func() *plan.Procedure) {
	selectSpec := root.Spec.(*FromProcedureSpec)
	if selectSpec.BoundsSet {
		// Example case where this matters
		//    var data = select(database: "mydb")
		//    var past = data.range(start:-2d,stop:-1d)
		//    var current = data.range(start:-1d,stop:now)
		root = dup()
		selectSpec = root.Spec.(*FromProcedureSpec)
		selectSpec.BoundsSet = false
		selectSpec.Bounds = query.Bounds{}
		return
	}
	selectSpec.BoundsSet = true
	selectSpec.Bounds = s.Bounds
}

func (s *RangeProcedureSpec) TimeBounds() query.Bounds {
	return s.Bounds
}

func createRangeTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*RangeProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}
	cache := execute.NewTableBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)

	bounds := a.StreamContext().Bounds()
	if bounds == nil {
		return nil, nil, errors.New("nil bounds supplied to range")
	}

	t, err := NewRangeTransformation(d, cache, s, *bounds)
	if err != nil {
		return nil, nil, err
	}
	return t, d, nil
}

type rangeTransformation struct {
	d        execute.Dataset
	cache    execute.TableBuilderCache
	bounds   execute.Bounds
	timeCol  string
	startCol string
	stopCol  string
}

func NewRangeTransformation(d execute.Dataset, cache execute.TableBuilderCache, spec *RangeProcedureSpec, absolute execute.Bounds) (*rangeTransformation, error) {
	return &rangeTransformation{
		d:        d,
		cache:    cache,
		bounds:   absolute,
		timeCol:  spec.TimeCol,
		startCol: spec.StartCol,
		stopCol:  spec.StopCol,
	}, nil
}

func (t *rangeTransformation) RetractTable(id execute.DatasetID, key query.GroupKey) error {
	return t.d.RetractTable(key)
}

func (t *rangeTransformation) Process(id execute.DatasetID, tbl query.Table) error {
	// Determine index of start and stop columns in group key
	startColIdx := execute.ColIdx(t.startCol, tbl.Cols())
	stopColIdx := execute.ColIdx(t.stopCol, tbl.Cols())

	// Determine index of start and stop columns in table
	startKeyColIdx := execute.ColIdx(t.startCol, tbl.Key().Cols())
	stopKeyColIdx := execute.ColIdx(t.stopCol, tbl.Key().Cols())

	builder, created := t.cache.TableBuilder(tbl.Key())
	if !created {
		return fmt.Errorf("range found duplicate table with key: %v", tbl.Key())
	}

	execute.AddTableCols(tbl, builder)

	timeIdx := execute.ColIdx(t.timeCol, tbl.Cols())
	if timeIdx < 0 {
		return fmt.Errorf("range error: supplied time column %s doesn't exist", t.timeCol)
	}

	if builder.Cols()[timeIdx].Type != query.TTime {
		return fmt.Errorf("range error: provided column %s is not of type time", t.timeCol)
	}

	forwardTable := false
	if startKeyColIdx > 0 && stopKeyColIdx > 0 {
		// Check group key for start and stop vaues.

		keyStart := tbl.Key().Value(startKeyColIdx).Time()
		keyStop := tbl.Key().Value(stopKeyColIdx).Time()
		keyBounds := execute.Bounds{
			Start: keyStart,
			Stop:  keyStop,
		}
		// If there is no overlap between the bounds in the group key and the bounds in the range transformation,
		// no further processing is needed.
		if !t.bounds.Overlaps(keyBounds) {
			return nil
		}

		// If [start, stop) (where start <= stop) from the group key is contained in the
		// range transformation bounds [keyStart, keyStop], we can skip the whole table.
		// Still want to skip if start >= keyStart and t.bounds.Stop == keyStop
		forwardTable = t.bounds.Contains(keyStart) && (t.bounds.Contains(keyStop) || t.bounds.Stop == keyStop)
	}

	if forwardTable {
		execute.AppendTable(tbl, builder)
		return nil
	}

	// If the start and/or stop columns don't exist,
	// They must be added to the table
	startAdded, stopAdded := false, false
	if startColIdx < 0 {
		startColIdx = builder.NCols()

		c := query.ColMeta{
			Label: t.startCol,
			Type:  query.TTime,
		}
		builder.AddCol(c)
		startAdded = true
	}

	if stopColIdx < 0 {
		stopColIdx = builder.NCols()
		c := query.ColMeta{
			Label: t.stopCol,
			Type:  query.TTime,
		}
		builder.AddCol(c)
		stopAdded = true
	}

	err := tbl.Do(func(cr query.ColReader) error {
		l := cr.Len()
		for i := 0; i < l; i++ {
			tVal := cr.Times(timeIdx)[i]
			if !t.bounds.Contains(tVal) {
				continue
			}
			for j, c := range builder.Cols() {
				switch c.Label {
				case t.startCol:
					var start values.Time
					// If we just inserted a start column with no values populated
					if startAdded {
						start = t.bounds.Start
					} else {
						start = cr.Times(j)[i]
					}

					if start < t.bounds.Start {
						start = t.bounds.Start
					}
					builder.AppendTime(j, start)
				case t.stopCol:
					var stop values.Time
					// If we just inserted a stop column with no values populated
					if stopAdded {
						stop = t.bounds.Stop
					} else {
						stop = cr.Times(j)[i]
					}

					if stop > t.bounds.Stop {
						stop = t.bounds.Stop
					}
					builder.AppendTime(j, stop)
				default:
					switch c.Type {
					case query.TBool:
						builder.AppendBool(j, cr.Bools(j)[i])
					case query.TInt:
						builder.AppendInt(j, cr.Ints(j)[i])
					case query.TUInt:
						builder.AppendUInt(j, cr.UInts(j)[i])
					case query.TFloat:
						builder.AppendFloat(j, cr.Floats(j)[i])
					case query.TString:
						builder.AppendString(j, cr.Strings(j)[i])
					case query.TTime:
						builder.AppendTime(j, cr.Times(j)[i])
					default:
						execute.PanicUnknownType(c.Type)
					}
				}
			}
		}
		return nil
	})
	return err
}

func (t *rangeTransformation) UpdateWatermark(id execute.DatasetID, mark execute.Time) error {
	return t.d.UpdateWatermark(mark)
}
func (t *rangeTransformation) UpdateProcessingTime(id execute.DatasetID, pt execute.Time) error {
	return t.d.UpdateProcessingTime(pt)
}
func (t *rangeTransformation) Finish(id execute.DatasetID, err error) {
	t.d.Finish(err)
}
