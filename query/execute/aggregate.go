package execute

import (
	"fmt"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/interpreter"
	"github.com/influxdata/platform/query/semantic"
	"github.com/pkg/errors"
)

type aggregateTransformation struct {
	d     Dataset
	cache BlockBuilderCache
	agg   Aggregate

	config AggregateConfig
}

type AggregateConfig struct {
	Columns []string `json:"columns"`
	TimeSrc string   `json:"time_src"`
	TimeDst string   `json:"time_dst"`
}

var DefaultAggregateConfig = AggregateConfig{
	Columns: []string{DefaultValueColLabel},
	TimeSrc: DefaultStopColLabel,
	TimeDst: DefaultTimeColLabel,
}

func (c AggregateConfig) Copy() AggregateConfig {
	nc := c
	if c.Columns != nil {
		nc.Columns = make([]string, len(c.Columns))
		copy(nc.Columns, c.Columns)
	}
	return nc
}

func (c *AggregateConfig) ReadArgs(args query.Arguments) error {
	if label, ok, err := args.GetString("timeDst"); err != nil {
		return err
	} else if ok {
		c.TimeDst = label
	} else {
		c.TimeDst = DefaultAggregateConfig.TimeDst
	}

	if timeValue, ok, err := args.GetString("timeSrc"); err != nil {
		return err
	} else if ok {
		c.TimeSrc = timeValue
	} else {
		c.TimeSrc = DefaultAggregateConfig.TimeSrc
	}

	if cols, ok, err := args.GetArray("columns", semantic.String); err != nil {
		return err
	} else if ok {
		columns, err := interpreter.ToStringArray(cols)
		if err != nil {
			return err
		}
		c.Columns = columns
	} else {
		c.Columns = DefaultAggregateConfig.Columns
	}
	return nil
}

func NewAggregateTransformation(d Dataset, c BlockBuilderCache, agg Aggregate, config AggregateConfig) *aggregateTransformation {
	return &aggregateTransformation{
		d:      d,
		cache:  c,
		agg:    agg,
		config: config,
	}
}

func NewAggregateTransformationAndDataset(id DatasetID, mode AccumulationMode, agg Aggregate, config AggregateConfig, a *Allocator) (*aggregateTransformation, Dataset) {
	cache := NewBlockBuilderCache(a)
	d := NewDataset(id, mode, cache)
	return NewAggregateTransformation(d, cache, agg, config), d
}

func (t *aggregateTransformation) RetractBlock(id DatasetID, key query.PartitionKey) error {
	//TODO(nathanielc): Store intermediate state for retractions
	return t.d.RetractBlock(key)
}

func (t *aggregateTransformation) Process(id DatasetID, b query.Block) error {
	builder, new := t.cache.BlockBuilder(b.Key())
	if !new {
		return fmt.Errorf("aggregate found duplicate block with key: %v", b.Key())
	}

	AddBlockKeyCols(b.Key(), builder)
	builder.AddCol(query.ColMeta{
		Label: t.config.TimeDst,
		Type:  query.TTime,
	})

	builderColMap := make([]int, len(t.config.Columns))
	blockColMap := make([]int, len(t.config.Columns))
	aggregates := make([]ValueFunc, len(t.config.Columns))

	cols := b.Cols()
	for j, label := range t.config.Columns {
		idx := -1
		for bj, bc := range cols {
			if bc.Label == label {
				idx = bj
				break
			}
		}
		if idx < 0 {
			return fmt.Errorf("column %q does not exist", label)
		}
		c := cols[idx]
		if b.Key().HasCol(c.Label) {
			return errors.New("cannot aggregate columns that are part of the partition key")
		}
		var vf ValueFunc
		switch c.Type {
		case query.TBool:
			vf = t.agg.NewBoolAgg()
		case query.TInt:
			vf = t.agg.NewIntAgg()
		case query.TUInt:
			vf = t.agg.NewUIntAgg()
		case query.TFloat:
			vf = t.agg.NewFloatAgg()
		case query.TString:
			vf = t.agg.NewStringAgg()
		default:
			return fmt.Errorf("unsupported aggregate column type %v", c.Type)
		}
		aggregates[j] = vf
		builderColMap[j] = builder.AddCol(query.ColMeta{
			Label: c.Label,
			Type:  vf.Type(),
		})
		blockColMap[j] = idx
	}
	if err := AppendAggregateTime(t.config.TimeSrc, t.config.TimeDst, b.Key(), builder); err != nil {
		return err
	}

	b.Do(func(cr query.ColReader) error {
		for j := range t.config.Columns {
			vf := aggregates[j]

			tj := blockColMap[j]
			c := b.Cols()[tj]

			switch c.Type {
			case query.TBool:
				vf.(DoBoolAgg).DoBool(cr.Bools(tj))
			case query.TInt:
				vf.(DoIntAgg).DoInt(cr.Ints(tj))
			case query.TUInt:
				vf.(DoUIntAgg).DoUInt(cr.UInts(tj))
			case query.TFloat:
				vf.(DoFloatAgg).DoFloat(cr.Floats(tj))
			case query.TString:
				vf.(DoStringAgg).DoString(cr.Strings(tj))
			default:
				return fmt.Errorf("unsupport aggregate type %v", c.Type)
			}
		}
		return nil
	})
	for j, vf := range aggregates {
		bj := builderColMap[j]
		// Append aggregated value
		switch vf.Type() {
		case query.TBool:
			builder.AppendBool(bj, vf.(BoolValueFunc).ValueBool())
		case query.TInt:
			builder.AppendInt(bj, vf.(IntValueFunc).ValueInt())
		case query.TUInt:
			builder.AppendUInt(bj, vf.(UIntValueFunc).ValueUInt())
		case query.TFloat:
			builder.AppendFloat(bj, vf.(FloatValueFunc).ValueFloat())
		case query.TString:
			builder.AppendString(bj, vf.(StringValueFunc).ValueString())
		}
	}

	AppendKeyValues(b.Key(), builder)

	return nil
}

func (t *aggregateTransformation) UpdateWatermark(id DatasetID, mark Time) error {
	return t.d.UpdateWatermark(mark)
}
func (t *aggregateTransformation) UpdateProcessingTime(id DatasetID, pt Time) error {
	return t.d.UpdateProcessingTime(pt)
}
func (t *aggregateTransformation) Finish(id DatasetID, err error) {
	t.d.Finish(err)
}

func AppendAggregateTime(srcTime, dstTime string, key query.PartitionKey, builder BlockBuilder) error {
	srcTimeIdx := ColIdx(srcTime, key.Cols())
	if srcTimeIdx < 0 {
		return fmt.Errorf("timeValue column %q does not exist", srcTime)
	}
	srcTimeCol := key.Cols()[srcTimeIdx]
	if srcTimeCol.Type != query.TTime {
		return fmt.Errorf("timeValue column %q does not have type time", srcTime)
	}

	dstTimeIdx := ColIdx(dstTime, builder.Cols())
	if dstTimeIdx < 0 {
		return fmt.Errorf("timeValue column %q does not exist", dstTime)
	}
	dstTimeCol := builder.Cols()[dstTimeIdx]
	if dstTimeCol.Type != query.TTime {
		return fmt.Errorf("timeValue column %q does not have type time", dstTime)
	}

	builder.AppendTime(dstTimeIdx, key.ValueTime(srcTimeIdx))
	return nil
}

type Aggregate interface {
	NewBoolAgg() DoBoolAgg
	NewIntAgg() DoIntAgg
	NewUIntAgg() DoUIntAgg
	NewFloatAgg() DoFloatAgg
	NewStringAgg() DoStringAgg
}

type ValueFunc interface {
	Type() query.DataType
}
type DoBoolAgg interface {
	ValueFunc
	DoBool([]bool)
}
type DoFloatAgg interface {
	ValueFunc
	DoFloat([]float64)
}
type DoIntAgg interface {
	ValueFunc
	DoInt([]int64)
}
type DoUIntAgg interface {
	ValueFunc
	DoUInt([]uint64)
}
type DoStringAgg interface {
	ValueFunc
	DoString([]string)
}

type BoolValueFunc interface {
	ValueBool() bool
}
type FloatValueFunc interface {
	ValueFloat() float64
}
type IntValueFunc interface {
	ValueInt() int64
}
type UIntValueFunc interface {
	ValueUInt() uint64
}
type StringValueFunc interface {
	ValueString() string
}
