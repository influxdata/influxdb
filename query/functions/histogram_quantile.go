package functions

import (
	"fmt"
	"math"
	"sort"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/plan"
	"github.com/influxdata/platform/query/semantic"
	"github.com/pkg/errors"
)

const HistogramQuantileKind = "histogramQuantile"

const DefaultUpperBoundColumnLabel = "le"

type HistogramQuantileOpSpec struct {
	Quantile         float64 `json:"quantile"`
	CountColumn      string  `json:"countColumn"`
	UpperBoundColumn string  `json:"upperBoundColumn"`
	ValueColumn      string  `json:"valueColumn"`
	MinValue         float64 `json:"minValue"`
}

var histogramQuantileSignature = query.DefaultFunctionSignature()

func init() {
	histogramQuantileSignature.Params["quantile"] = semantic.Float
	histogramQuantileSignature.Params["countColumn"] = semantic.String
	histogramQuantileSignature.Params["upperBoundColumn"] = semantic.String
	histogramQuantileSignature.Params["valueColumn"] = semantic.String
	histogramQuantileSignature.Params["minValue"] = semantic.Float

	query.RegisterFunction(HistogramQuantileKind, createHistogramQuantileOpSpec, histogramQuantileSignature)
	query.RegisterOpSpec(HistogramQuantileKind, newHistogramQuantileOp)
	plan.RegisterProcedureSpec(HistogramQuantileKind, newHistogramQuantileProcedure, HistogramQuantileKind)
	execute.RegisterTransformation(HistogramQuantileKind, createHistogramQuantileTransformation)
}
func createHistogramQuantileOpSpec(args query.Arguments, a *query.Administration) (query.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}

	s := new(HistogramQuantileOpSpec)
	q, err := args.GetRequiredFloat("quantile")
	if err != nil {
		return nil, err
	}
	s.Quantile = q

	if col, ok, err := args.GetString("countColumn"); err != nil {
		return nil, err
	} else if ok {
		s.CountColumn = col
	} else {
		s.CountColumn = execute.DefaultValueColLabel
	}

	if col, ok, err := args.GetString("upperBoundColumn"); err != nil {
		return nil, err
	} else if ok {
		s.UpperBoundColumn = col
	} else {
		s.UpperBoundColumn = DefaultUpperBoundColumnLabel
	}

	if col, ok, err := args.GetString("valueColumn"); err != nil {
		return nil, err
	} else if ok {
		s.ValueColumn = col
	} else {
		s.ValueColumn = execute.DefaultValueColLabel
	}

	if min, ok, err := args.GetFloat("minValue"); err != nil {
		return nil, err
	} else if ok {
		s.MinValue = min
	}

	return s, nil
}

func newHistogramQuantileOp() query.OperationSpec {
	return new(HistogramQuantileOpSpec)
}

func (s *HistogramQuantileOpSpec) Kind() query.OperationKind {
	return HistogramQuantileKind
}

type HistogramQuantileProcedureSpec struct {
	Quantile         float64 `json:"quantile"`
	CountColumn      string  `json:"countColumn"`
	UpperBoundColumn string  `json:"upperBoundColumn"`
	ValueColumn      string  `json:"valueColumn"`
	MinValue         float64 `json:"minValue"`
}

func newHistogramQuantileProcedure(qs query.OperationSpec, a plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*HistogramQuantileOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}
	return &HistogramQuantileProcedureSpec{
		Quantile:         spec.Quantile,
		CountColumn:      spec.CountColumn,
		UpperBoundColumn: spec.UpperBoundColumn,
		ValueColumn:      spec.ValueColumn,
		MinValue:         spec.MinValue,
	}, nil
}

func (s *HistogramQuantileProcedureSpec) Kind() plan.ProcedureKind {
	return HistogramQuantileKind
}
func (s *HistogramQuantileProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(HistogramQuantileProcedureSpec)
	*ns = *s
	return ns
}

type histogramQuantileTransformation struct {
	d     execute.Dataset
	cache execute.TableBuilderCache

	spec HistogramQuantileProcedureSpec
}

type bucket struct {
	count      float64
	upperBound float64
}

func createHistogramQuantileTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*HistogramQuantileProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}
	cache := execute.NewTableBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)
	t := NewHistorgramQuantileTransformation(d, cache, s)
	return t, d, nil
}

func NewHistorgramQuantileTransformation(
	d execute.Dataset,
	cache execute.TableBuilderCache,
	spec *HistogramQuantileProcedureSpec,
) execute.Transformation {
	return &histogramQuantileTransformation{
		d:     d,
		cache: cache,
		spec:  *spec,
	}
}

func (t histogramQuantileTransformation) RetractTable(id execute.DatasetID, key query.GroupKey) error {
	// TODO
	return nil
}

func (t histogramQuantileTransformation) Process(id execute.DatasetID, tbl query.Table) error {
	builder, created := t.cache.TableBuilder(tbl.Key())
	if !created {
		return fmt.Errorf("histogramQuantile found duplicate table with key: %v", tbl.Key())
	}

	execute.AddTableKeyCols(tbl.Key(), builder)
	valueIdx := builder.AddCol(query.ColMeta{
		Label: t.spec.ValueColumn,
		Type:  query.TFloat,
	})

	countIdx := execute.ColIdx(t.spec.CountColumn, tbl.Cols())
	if countIdx < 0 {
		return fmt.Errorf("table is missing count column %q", t.spec.CountColumn)
	}
	if tbl.Cols()[countIdx].Type != query.TFloat {
		return fmt.Errorf("count column %q must be of type float", t.spec.CountColumn)
	}
	upperBoundIdx := execute.ColIdx(t.spec.UpperBoundColumn, tbl.Cols())
	if upperBoundIdx < 0 {
		return fmt.Errorf("table is missing upper bound column %q", t.spec.UpperBoundColumn)
	}
	if tbl.Cols()[upperBoundIdx].Type != query.TFloat {
		return fmt.Errorf("upper bound column %q must be of type float", t.spec.UpperBoundColumn)
	}
	// Read buckets
	var cdf []bucket
	sorted := true //track if the cdf was naturally sorted
	err := tbl.Do(func(cr query.ColReader) error {
		offset := len(cdf)
		// Grow cdf by number of rows
		l := offset + cr.Len()
		if cap(cdf) < l {
			cpy := make([]bucket, l, l*2)
			// Copy existing buckets to new slice
			copy(cpy, cdf)
			cdf = cpy
		} else {
			cdf = cdf[:l]
		}
		for i := 0; i < cr.Len(); i++ {
			curr := i + offset
			prev := curr - 1
			cdf[curr] = bucket{
				count:      cr.Floats(countIdx)[i],
				upperBound: cr.Floats(upperBoundIdx)[i],
			}
			if prev >= 0 {
				sorted = sorted && cdf[prev].upperBound <= cdf[curr].upperBound
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	if !sorted {
		sort.Slice(cdf, func(i, j int) bool {
			return cdf[i].upperBound < cdf[j].upperBound
		})
	}

	q, err := t.computeQuantile(cdf)
	if err != nil {
		return err
	}
	execute.AppendKeyValues(tbl.Key(), builder)
	builder.AppendFloat(valueIdx, q)
	return nil
}

func (t *histogramQuantileTransformation) computeQuantile(cdf []bucket) (float64, error) {
	if len(cdf) == 0 {
		return 0, errors.New("histogram is empty")
	}
	// Find rank index and check counts are monotonic
	prevCount := 0.0
	totalCount := cdf[len(cdf)-1].count
	rank := t.spec.Quantile * totalCount
	rankIdx := -1
	for i, b := range cdf {
		if b.count < prevCount {
			return 0, errors.New("histogram records counts are not monotonic")
		}
		prevCount = b.count

		if rank >= b.count {
			rankIdx = i
		}
	}
	var (
		lowerCount,
		lowerBound,
		upperCount,
		upperBound float64
	)
	switch rankIdx {
	case -1:
		// Quantile is below the lowest upper bound, interpolate using the min value
		lowerCount = 0
		lowerBound = t.spec.MinValue
		upperCount = cdf[0].count
		upperBound = cdf[0].upperBound
	case len(cdf) - 1:
		// Quantile is above the highest upper bound, simply return it as it must be finite
		return cdf[len(cdf)-1].upperBound, nil
	default:
		lowerCount = cdf[rankIdx].count
		lowerBound = cdf[rankIdx].upperBound
		upperCount = cdf[rankIdx+1].count
		upperBound = cdf[rankIdx+1].upperBound
	}
	if rank == lowerCount {
		// No need to interpolate
		return lowerBound, nil
	}
	if math.IsInf(lowerBound, -1) {
		// We cannot interpolate with infinity
		return upperBound, nil
	}
	if math.IsInf(upperBound, 1) {
		// We cannot interpolate with infinity
		return lowerBound, nil
	}
	// Compute quantile using linear interpolation
	scale := (rank - lowerCount) / (upperCount - lowerCount)
	return lowerBound + (upperBound-lowerBound)*scale, nil
}

func (t histogramQuantileTransformation) UpdateWatermark(id execute.DatasetID, mark execute.Time) error {
	return t.d.UpdateWatermark(mark)
}

func (t histogramQuantileTransformation) UpdateProcessingTime(id execute.DatasetID, pt execute.Time) error {
	return t.d.UpdateProcessingTime(pt)
}

func (t histogramQuantileTransformation) Finish(id execute.DatasetID, err error) {
	t.d.Finish(err)
}
