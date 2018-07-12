package execute

import (
	"fmt"

	"github.com/influxdata/platform/query"
)

type selectorTransformation struct {
	d     Dataset
	cache BlockBuilderCache

	config SelectorConfig
}

type SelectorConfig struct {
	Column string `json:"column"`
}

func (c *SelectorConfig) ReadArgs(args query.Arguments) error {
	if col, ok, err := args.GetString("column"); err != nil {
		return err
	} else if ok {
		c.Column = col
	}
	return nil
}

type rowSelectorTransformation struct {
	selectorTransformation
	selector RowSelector
}
type indexSelectorTransformation struct {
	selectorTransformation
	selector IndexSelector
}

func NewRowSelectorTransformationAndDataset(id DatasetID, mode AccumulationMode, selector RowSelector, config SelectorConfig, a *Allocator) (*rowSelectorTransformation, Dataset) {
	cache := NewBlockBuilderCache(a)
	d := NewDataset(id, mode, cache)
	return NewRowSelectorTransformation(d, cache, selector, config), d
}
func NewRowSelectorTransformation(d Dataset, c BlockBuilderCache, selector RowSelector, config SelectorConfig) *rowSelectorTransformation {
	return &rowSelectorTransformation{
		selectorTransformation: newSelectorTransformation(d, c, config),
		selector:               selector,
	}
}

func NewIndexSelectorTransformationAndDataset(id DatasetID, mode AccumulationMode, selector IndexSelector, config SelectorConfig, a *Allocator) (*indexSelectorTransformation, Dataset) {
	cache := NewBlockBuilderCache(a)
	d := NewDataset(id, mode, cache)
	return NewIndexSelectorTransformation(d, cache, selector, config), d
}
func NewIndexSelectorTransformation(d Dataset, c BlockBuilderCache, selector IndexSelector, config SelectorConfig) *indexSelectorTransformation {
	return &indexSelectorTransformation{
		selectorTransformation: newSelectorTransformation(d, c, config),
		selector:               selector,
	}
}

func newSelectorTransformation(d Dataset, c BlockBuilderCache, config SelectorConfig) selectorTransformation {
	if config.Column == "" {
		config.Column = DefaultValueColLabel
	}
	return selectorTransformation{
		d:      d,
		cache:  c,
		config: config,
	}
}

func (t *selectorTransformation) RetractBlock(id DatasetID, key query.GroupKey) error {
	//TODO(nathanielc): Store intermediate state for retractions
	return t.d.RetractBlock(key)
}
func (t *selectorTransformation) UpdateWatermark(id DatasetID, mark Time) error {
	return t.d.UpdateWatermark(mark)
}
func (t *selectorTransformation) UpdateProcessingTime(id DatasetID, pt Time) error {
	return t.d.UpdateProcessingTime(pt)
}
func (t *selectorTransformation) Finish(id DatasetID, err error) {
	t.d.Finish(err)
}

func (t *selectorTransformation) setupBuilder(b query.Block) (BlockBuilder, int, error) {
	builder, new := t.cache.BlockBuilder(b.Key())
	if !new {
		return nil, 0, fmt.Errorf("found duplicate block with key: %v", b.Key())
	}
	AddBlockCols(b, builder)

	cols := builder.Cols()
	valueIdx := ColIdx(t.config.Column, cols)
	if valueIdx < 0 {
		return nil, 0, fmt.Errorf("no column %q exists", t.config.Column)
	}
	return builder, valueIdx, nil
}

func (t *indexSelectorTransformation) Process(id DatasetID, b query.Block) error {
	builder, valueIdx, err := t.setupBuilder(b)
	if err != nil {
		return err
	}
	valueCol := builder.Cols()[valueIdx]

	var s interface{}
	switch valueCol.Type {
	case query.TBool:
		s = t.selector.NewBoolSelector()
	case query.TInt:
		s = t.selector.NewIntSelector()
	case query.TUInt:
		s = t.selector.NewUIntSelector()
	case query.TFloat:
		s = t.selector.NewFloatSelector()
	case query.TString:
		s = t.selector.NewStringSelector()
	default:
		return fmt.Errorf("unsupported selector type %v", valueCol.Type)
	}

	return b.Do(func(cr query.ColReader) error {
		switch valueCol.Type {
		case query.TBool:
			selected := s.(DoBoolIndexSelector).DoBool(cr.Bools(valueIdx))
			t.appendSelected(selected, builder, cr)
		case query.TInt:
			selected := s.(DoIntIndexSelector).DoInt(cr.Ints(valueIdx))
			t.appendSelected(selected, builder, cr)
		case query.TUInt:
			selected := s.(DoUIntIndexSelector).DoUInt(cr.UInts(valueIdx))
			t.appendSelected(selected, builder, cr)
		case query.TFloat:
			selected := s.(DoFloatIndexSelector).DoFloat(cr.Floats(valueIdx))
			t.appendSelected(selected, builder, cr)
		case query.TString:
			selected := s.(DoStringIndexSelector).DoString(cr.Strings(valueIdx))
			t.appendSelected(selected, builder, cr)
		default:
			return fmt.Errorf("unsupported selector type %v", valueCol.Type)
		}
		return nil
	})
}

func (t *rowSelectorTransformation) Process(id DatasetID, b query.Block) error {
	builder, valueIdx, err := t.setupBuilder(b)
	if err != nil {
		return err
	}
	valueCol := builder.Cols()[valueIdx]

	var rower Rower

	switch valueCol.Type {
	case query.TBool:
		rower = t.selector.NewBoolSelector()
	case query.TInt:
		rower = t.selector.NewIntSelector()
	case query.TUInt:
		rower = t.selector.NewUIntSelector()
	case query.TFloat:
		rower = t.selector.NewFloatSelector()
	case query.TString:
		rower = t.selector.NewStringSelector()
	default:
		return fmt.Errorf("unsupported selector type %v", valueCol.Type)
	}

	// if rower has a nil value, this means that the row selector doesn't
	// yet have an implementation

	if rower == nil {
		return fmt.Errorf("invalid use of function: %T has no implementation for type %v", t.selector, valueCol.Type)
	}

	b.Do(func(cr query.ColReader) error {
		switch valueCol.Type {
		case query.TBool:
			rower.(DoBoolRowSelector).DoBool(cr.Bools(valueIdx), cr)
		case query.TInt:
			rower.(DoIntRowSelector).DoInt(cr.Ints(valueIdx), cr)
		case query.TUInt:
			rower.(DoUIntRowSelector).DoUInt(cr.UInts(valueIdx), cr)
		case query.TFloat:
			rower.(DoFloatRowSelector).DoFloat(cr.Floats(valueIdx), cr)
		case query.TString:
			rower.(DoStringRowSelector).DoString(cr.Strings(valueIdx), cr)
		default:
			return fmt.Errorf("unsupported selector type %v", valueCol.Type)
		}
		return nil
	})
	rows := rower.Rows()
	t.appendRows(builder, rows)
	return nil
}

func (t *indexSelectorTransformation) appendSelected(selected []int, builder BlockBuilder, cr query.ColReader) {
	if len(selected) == 0 {
		return
	}
	cols := builder.Cols()
	for j, c := range cols {
		for _, i := range selected {
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
				PanicUnknownType(c.Type)
			}
		}
	}
}

func (t *rowSelectorTransformation) appendRows(builder BlockBuilder, rows []Row) {
	cols := builder.Cols()
	for j, c := range cols {
		for _, row := range rows {
			v := row.Values[j]
			switch c.Type {
			case query.TBool:
				builder.AppendBool(j, v.(bool))
			case query.TInt:
				builder.AppendInt(j, v.(int64))
			case query.TUInt:
				builder.AppendUInt(j, v.(uint64))
			case query.TFloat:
				builder.AppendFloat(j, v.(float64))
			case query.TString:
				builder.AppendString(j, v.(string))
			case query.TTime:
				builder.AppendTime(j, v.(Time))
			default:
				PanicUnknownType(c.Type)
			}
		}
	}
}

type IndexSelector interface {
	NewBoolSelector() DoBoolIndexSelector
	NewIntSelector() DoIntIndexSelector
	NewUIntSelector() DoUIntIndexSelector
	NewFloatSelector() DoFloatIndexSelector
	NewStringSelector() DoStringIndexSelector
}
type DoBoolIndexSelector interface {
	DoBool([]bool) []int
}
type DoIntIndexSelector interface {
	DoInt([]int64) []int
}
type DoUIntIndexSelector interface {
	DoUInt([]uint64) []int
}
type DoFloatIndexSelector interface {
	DoFloat([]float64) []int
}
type DoStringIndexSelector interface {
	DoString([]string) []int
}

type RowSelector interface {
	NewBoolSelector() DoBoolRowSelector
	NewIntSelector() DoIntRowSelector
	NewUIntSelector() DoUIntRowSelector
	NewFloatSelector() DoFloatRowSelector
	NewStringSelector() DoStringRowSelector
}

type Rower interface {
	Rows() []Row
}

type DoBoolRowSelector interface {
	Rower
	DoBool(vs []bool, cr query.ColReader)
}
type DoIntRowSelector interface {
	Rower
	DoInt(vs []int64, cr query.ColReader)
}
type DoUIntRowSelector interface {
	Rower
	DoUInt(vs []uint64, cr query.ColReader)
}
type DoFloatRowSelector interface {
	Rower
	DoFloat(vs []float64, cr query.ColReader)
}
type DoStringRowSelector interface {
	Rower
	DoString(vs []string, cr query.ColReader)
}

type Row struct {
	Values []interface{}
}

func ReadRow(i int, cr query.ColReader) (row Row) {
	cols := cr.Cols()
	row.Values = make([]interface{}, len(cols))
	for j, c := range cols {
		switch c.Type {
		case query.TBool:
			row.Values[j] = cr.Bools(j)[i]
		case query.TInt:
			row.Values[j] = cr.Ints(j)[i]
		case query.TUInt:
			row.Values[j] = cr.UInts(j)[i]
		case query.TFloat:
			row.Values[j] = cr.Floats(j)[i]
		case query.TString:
			row.Values[j] = cr.Strings(j)[i]
		case query.TTime:
			row.Values[j] = cr.Times(j)[i]
		}
	}
	return
}
