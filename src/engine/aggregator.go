package engine

import (
	"common"
	"fmt"
	"math"
	"parser"
	"protocol"
	"sort"
	"strconv"
	"time"
)

type PointSlice []protocol.Point

type Aggregator interface {
	AggregatePoint(series string, group interface{}, p *protocol.Point) error
	InitializeFieldsMetadata(series *protocol.Series) error
	GetValue(series string, group interface{}) *protocol.FieldValue
	ColumnName() string
	ColumnType() protocol.FieldDefinition_Type
}

type AggregatorInitializer func(*parser.Query, *parser.Value) (Aggregator, error)

var registeredAggregators = make(map[string]AggregatorInitializer)

func init() {
	registeredAggregators["count"] = NewCountAggregator
	registeredAggregators["max"] = NewMaxAggregator
	registeredAggregators["min"] = NewMinAggregator
	registeredAggregators["percentile"] = NewPercentileAggregator
	registeredAggregators["median"] = NewMedianAggregator
	registeredAggregators["mean"] = NewMeanAggregator
	registeredAggregators["mode"] = NewModeAggregator
	registeredAggregators["__timestamp_aggregator"] = NewTimestampAggregator
}

//
// Count Aggregator
//

type CountAggregator struct {
	counts map[string]map[interface{}]int32
}

func (self *CountAggregator) AggregatePoint(series string, group interface{}, p *protocol.Point) error {
	counts := self.counts[series]
	if counts == nil {
		counts = make(map[interface{}]int32)
		self.counts[series] = counts
	}
	counts[group]++
	return nil
}

func (self *CountAggregator) ColumnName() string {
	return "count"
}

func (self *CountAggregator) ColumnType() protocol.FieldDefinition_Type {
	return protocol.FieldDefinition_INT32
}

func (self *CountAggregator) GetValue(series string, group interface{}) *protocol.FieldValue {
	value := self.counts[series][group]
	return &protocol.FieldValue{IntValue: &value}
}

func (self *CountAggregator) InitializeFieldsMetadata(series *protocol.Series) error { return nil }

func NewCountAggregator(*parser.Query, *parser.Value) (Aggregator, error) {
	return &CountAggregator{make(map[string]map[interface{}]int32)}, nil
}

//
// Timestamp Aggregator
//

type TimestampAggregator struct {
	duration   *time.Duration
	timestamps map[string]map[interface{}]int64
}

func (self *TimestampAggregator) AggregatePoint(series string, group interface{}, p *protocol.Point) error {
	timestamps := self.timestamps[series]
	if timestamps == nil {
		timestamps = make(map[interface{}]int64)
		self.timestamps[series] = timestamps
	}
	if self.duration != nil {
		timestamps[group] = time.Unix(*p.Timestamp, 0).Round(*self.duration).Unix()
	} else {
		timestamps[group] = *p.Timestamp
	}
	return nil
}

func (self *TimestampAggregator) ColumnName() string {
	return "count"
}

func (self *TimestampAggregator) ColumnType() protocol.FieldDefinition_Type {
	return protocol.FieldDefinition_INT32
}

func (self *TimestampAggregator) GetValue(series string, group interface{}) *protocol.FieldValue {
	value := self.timestamps[series][group]
	return &protocol.FieldValue{Int64Value: &value}
}

func (self *TimestampAggregator) InitializeFieldsMetadata(series *protocol.Series) error { return nil }

func NewTimestampAggregator(query *parser.Query, _ *parser.Value) (Aggregator, error) {
	duration, err := query.GetGroupByClause().GetGroupByTime()
	if err != nil {
		return nil, err
	}

	return &TimestampAggregator{
		timestamps: make(map[string]map[interface{}]int64),
		duration:   duration,
	}, nil
}

//
// Mean Aggregator
//

type MeanAggregator struct {
	fieldName  string
	fieldIndex int
	fieldType  protocol.FieldDefinition_Type
	means      map[string]map[interface{}]float64
	counts     map[string]map[interface{}]int
}

func (self *MeanAggregator) AggregatePoint(series string, group interface{}, p *protocol.Point) error {
	means := self.means[series]
	counts := self.counts[series]

	if means == nil && counts == nil {
		means = make(map[interface{}]float64)
		self.means[series] = means

		counts = make(map[interface{}]int)
		self.counts[series] = counts
	}

	currentMean := means[group]
	currentCount := counts[group] + 1

	var value float64
	switch self.fieldType {
	case protocol.FieldDefinition_INT64:
		value = float64(*p.Values[self.fieldIndex].Int64Value)
	case protocol.FieldDefinition_INT32:
		value = float64(*p.Values[self.fieldIndex].IntValue)
	case protocol.FieldDefinition_DOUBLE:
		value = *p.Values[self.fieldIndex].DoubleValue
	}

	currentMean = currentMean*float64(currentCount-1)/float64(currentCount) + value/float64(currentCount)

	means[group] = currentMean
	counts[group] = currentCount
	return nil
}

func (self *MeanAggregator) ColumnName() string {
	return "mean"
}

func (self *MeanAggregator) ColumnType() protocol.FieldDefinition_Type {
	return protocol.FieldDefinition_DOUBLE
}

func (self *MeanAggregator) GetValue(series string, group interface{}) *protocol.FieldValue {
	mean := self.means[series][group]
	return &protocol.FieldValue{DoubleValue: &mean}
}

func (self *MeanAggregator) InitializeFieldsMetadata(series *protocol.Series) error {
	for idx, field := range series.Fields {
		if *field.Name == self.fieldName {
			self.fieldIndex = idx
			self.fieldType = *field.Type

			switch self.fieldType {
			case protocol.FieldDefinition_INT32,
				protocol.FieldDefinition_INT64,
				protocol.FieldDefinition_DOUBLE:
				// that's fine
			default:
				return common.NewQueryError(common.InvalidArgument, fmt.Sprintf("Field %s has invalid type %v", self.fieldName, self.fieldType))
			}

			return nil
		}
	}

	return common.NewQueryError(common.InvalidArgument, fmt.Sprintf("Unknown column name %s", self.fieldName))
}

func NewMeanAggregator(_ *parser.Query, value *parser.Value) (Aggregator, error) {
	if len(value.Elems) != 1 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function mean() requires exactly one argument")
	}

	return &MeanAggregator{
		fieldName: value.Elems[0].Name,
		means:     make(map[string]map[interface{}]float64),
		counts:    make(map[string]map[interface{}]int),
	}, nil
}

//
// Median Aggregator
//

type MedianAggregator struct {
	fieldName    string
	fieldIndex   int
	fieldType    protocol.FieldDefinition_Type
	int_values   map[string]map[interface{}][]int
	float_values map[string]map[interface{}][]float64
}

func (self *MedianAggregator) AggregatePoint(series string, group interface{}, p *protocol.Point) error {
	switch self.fieldType {
	case protocol.FieldDefinition_INT32:
		int_values := self.int_values[series]
		if int_values == nil {
			int_values = make(map[interface{}][]int)
			self.int_values[series] = int_values
		}

		points := int_values[group]
		if points == nil {
			points = make([]int, 0)
		}

		points = append(points, int(*p.Values[self.fieldIndex].IntValue))
		int_values[group] = points
	case protocol.FieldDefinition_INT64:
		int_values := self.int_values[series]
		if int_values == nil {
			int_values = make(map[interface{}][]int)
			self.int_values[series] = int_values
		}

		points := int_values[group]
		if points == nil {
			points = make([]int, 0)
		}

		points = append(points, int(*p.Values[self.fieldIndex].Int64Value))
		int_values[group] = points
	case protocol.FieldDefinition_DOUBLE:
		float_values := self.float_values[series]
		if float_values == nil {
			float_values = make(map[interface{}][]float64)
			self.float_values[series] = float_values
		}

		points := float_values[group]
		if points == nil {
			points = make([]float64, 0)
		}

		points = append(points, *p.Values[self.fieldIndex].DoubleValue)
		float_values[group] = points
	default:
		return common.NewQueryError(common.InvalidArgument, fmt.Sprintf("Field %s has invalid type %v", self.fieldName, self.fieldType))
	}

	return nil
}

func (self *MedianAggregator) ColumnName() string {
	return "median"
}

func (self *MedianAggregator) ColumnType() protocol.FieldDefinition_Type {
	return self.fieldType
}

func (self *MedianAggregator) GetValue(series string, group interface{}) *protocol.FieldValue {
	switch self.fieldType {
	case protocol.FieldDefinition_INT32:
		sort.Ints(self.int_values[series][group])
		length := len(self.int_values[series][group])
		index := int(math.Floor(float64(length)*0.5+0.5)) - 1
		point := int32(self.int_values[series][group][index])
		return &protocol.FieldValue{IntValue: &point}
	case protocol.FieldDefinition_INT64:
		sort.Ints(self.int_values[series][group])
		length := len(self.int_values[series][group])
		index := int(math.Floor(float64(length)*0.5+0.5)) - 1
		point := int64(self.int_values[series][group][index])
		return &protocol.FieldValue{Int64Value: &point}
	case protocol.FieldDefinition_DOUBLE:
		sort.Float64s(self.float_values[series][group])
		length := len(self.float_values[series][group])
		index := int(math.Floor(float64(length)*0.5+0.5)) - 1
		point := self.float_values[series][group][index]
		return &protocol.FieldValue{DoubleValue: &point}
	}
	return &protocol.FieldValue{}
}

func (self *MedianAggregator) InitializeFieldsMetadata(series *protocol.Series) error {
	for idx, field := range series.Fields {
		if *field.Name == self.fieldName {
			self.fieldIndex = idx
			self.fieldType = *field.Type

			switch self.fieldType {
			case protocol.FieldDefinition_INT32,
				protocol.FieldDefinition_INT64,
				protocol.FieldDefinition_DOUBLE:
				// that's fine
			default:
				return common.NewQueryError(common.InvalidArgument, fmt.Sprintf("Field %s has invalid type %v", self.fieldName, self.fieldType))
			}

			return nil
		}
	}

	return common.NewQueryError(common.InvalidArgument, fmt.Sprintf("Unknown column name %s", self.fieldName))
}

func NewMedianAggregator(_ *parser.Query, value *parser.Value) (Aggregator, error) {
	if len(value.Elems) != 1 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function median() requires exactly one argument")
	}

	return &MedianAggregator{
		fieldName:    value.Elems[0].Name,
		int_values:   make(map[string]map[interface{}][]int),
		float_values: make(map[string]map[interface{}][]float64),
	}, nil
}

//
// Percentile Aggregator
//

type PercentileAggregator struct {
	fieldName    string
	fieldIndex   int
	fieldType    protocol.FieldDefinition_Type
	percentile   float64
	int_values   map[string]map[interface{}][]int
	float_values map[string]map[interface{}][]float64
}

func (self *PercentileAggregator) AggregatePoint(series string, group interface{}, p *protocol.Point) error {
	switch self.fieldType {
	case protocol.FieldDefinition_INT32:
		int_values := self.int_values[series]
		if int_values == nil {
			int_values = make(map[interface{}][]int)
			self.int_values[series] = int_values
		}

		points := int_values[group]
		if points == nil {
			points = make([]int, 0)
		}

		points = append(points, int(*p.Values[self.fieldIndex].IntValue))
		int_values[group] = points
	case protocol.FieldDefinition_INT64:
		int_values := self.int_values[series]
		if int_values == nil {
			int_values = make(map[interface{}][]int)
			self.int_values[series] = int_values
		}

		points := int_values[group]
		if points == nil {
			points = make([]int, 0)
		}

		points = append(points, int(*p.Values[self.fieldIndex].Int64Value))
		int_values[group] = points
	case protocol.FieldDefinition_DOUBLE:
		float_values := self.float_values[series]
		if float_values == nil {
			float_values = make(map[interface{}][]float64)
			self.float_values[series] = float_values
		}

		points := float_values[group]
		if points == nil {
			points = make([]float64, 0)
		}

		points = append(points, *p.Values[self.fieldIndex].DoubleValue)
		float_values[group] = points
	default:
		return common.NewQueryError(common.InvalidArgument, fmt.Sprintf("Field %s has invalid type %v", self.fieldName, self.fieldType))
	}

	return nil
}

func (self *PercentileAggregator) ColumnName() string {
	return "percentile"
}

func (self *PercentileAggregator) ColumnType() protocol.FieldDefinition_Type {
	return self.fieldType
}

func (self *PercentileAggregator) GetValue(series string, group interface{}) *protocol.FieldValue {
	switch self.fieldType {
	case protocol.FieldDefinition_INT32:
		sort.Ints(self.int_values[series][group])
		length := len(self.int_values[series][group])
		index := int(math.Floor(float64(length)*self.percentile/100.0+0.5)) - 1
		point := int32(self.int_values[series][group][index])
		return &protocol.FieldValue{IntValue: &point}
	case protocol.FieldDefinition_INT64:
		sort.Ints(self.int_values[series][group])
		length := len(self.int_values[series][group])
		index := int(math.Floor(float64(length)*self.percentile/100.0+0.5)) - 1
		point := int64(self.int_values[series][group][index])
		return &protocol.FieldValue{Int64Value: &point}
	case protocol.FieldDefinition_DOUBLE:
		sort.Float64s(self.float_values[series][group])
		length := len(self.float_values[series][group])
		index := int(math.Floor(float64(length)*self.percentile/100.0+0.5)) - 1
		point := self.float_values[series][group][index]
		return &protocol.FieldValue{DoubleValue: &point}
	}
	return &protocol.FieldValue{}
}

func (self *PercentileAggregator) InitializeFieldsMetadata(series *protocol.Series) error {
	for idx, field := range series.Fields {
		if *field.Name == self.fieldName {
			self.fieldIndex = idx
			self.fieldType = *field.Type

			switch self.fieldType {
			case protocol.FieldDefinition_INT32,
				protocol.FieldDefinition_INT64,
				protocol.FieldDefinition_DOUBLE:
				// that's fine
			default:
				return common.NewQueryError(common.InvalidArgument, fmt.Sprintf("Field %s has invalid type %v", self.fieldName, self.fieldType))
			}

			return nil
		}
	}

	return common.NewQueryError(common.InvalidArgument, fmt.Sprintf("Unknown column name %s", self.fieldName))
}

func NewPercentileAggregator(_ *parser.Query, value *parser.Value) (Aggregator, error) {
	if len(value.Elems) != 2 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function percentile() requires exactly two arguments")
	}
	percentile, err := strconv.ParseFloat(value.Elems[1].Name, 64)

	if err != nil || percentile <= 0 || percentile >= 100 {
		return nil, common.NewQueryError(common.InvalidArgument, "function percentile() requires a numeric second argument between 0 and 100")
	}

	return &PercentileAggregator{
		fieldName:    value.Elems[0].Name,
		percentile:   percentile,
		int_values:   make(map[string]map[interface{}][]int),
		float_values: make(map[string]map[interface{}][]float64),
	}, nil
}

//
// Mode Aggregator
//

type ModeAggregator struct {
	fieldName  string
	fieldIndex int
	fieldType  protocol.FieldDefinition_Type
	counts     map[string]map[interface{}]map[interface{}]int
}

func (self *ModeAggregator) AggregatePoint(series string, group interface{}, p *protocol.Point) error {
	seriesCounts := self.counts[series]
	if seriesCounts == nil {
		seriesCounts = make(map[interface{}]map[interface{}]int)
		self.counts[series] = seriesCounts
	}

	groupCounts := seriesCounts[group]
	if groupCounts == nil {
		groupCounts = make(map[interface{}]int)
	}

	var value interface{}
	switch self.fieldType {
	case protocol.FieldDefinition_INT32:
		value = *p.Values[self.fieldIndex].IntValue
	case protocol.FieldDefinition_INT64:
		value = *p.Values[self.fieldIndex].Int64Value
	case protocol.FieldDefinition_DOUBLE:
		value = *p.Values[self.fieldIndex].DoubleValue
	}

	count := groupCounts[value]
	count += 1
	groupCounts[value] = count
	seriesCounts[group] = groupCounts

	return nil
}

func (self *ModeAggregator) ColumnName() string {
	return "mode"
}

func (self *ModeAggregator) ColumnType() protocol.FieldDefinition_Type {
	return self.fieldType
}

func (self *ModeAggregator) GetValue(series string, group interface{}) *protocol.FieldValue {
	values := make([]interface{}, 0)
	currentCount := 1

	for value, count := range self.counts[series][group] {
		if count == currentCount {
			values = append(values, value)
		} else if count > currentCount {
			values = make([]interface{}, 0)
			values = append(values, value)
			currentCount = count
		}
	}

	switch self.fieldType {
	case protocol.FieldDefinition_INT32:
		value := values[0].(int32)
		return &protocol.FieldValue{IntValue: &value}
	case protocol.FieldDefinition_INT64:
		value := values[0].(int64)
		return &protocol.FieldValue{Int64Value: &value}
	case protocol.FieldDefinition_DOUBLE:
		value := values[0].(float64)
		return &protocol.FieldValue{DoubleValue: &value}
	}

	return &protocol.FieldValue{}
}

func (self *ModeAggregator) InitializeFieldsMetadata(series *protocol.Series) error {
	for idx, field := range series.Fields {
		if *field.Name == self.fieldName {
			self.fieldIndex = idx
			self.fieldType = *field.Type

			switch self.fieldType {
			case protocol.FieldDefinition_INT32,
				protocol.FieldDefinition_INT64,
				protocol.FieldDefinition_DOUBLE:
				// that's fine
			default:
				return common.NewQueryError(common.InvalidArgument, fmt.Sprintf("Field %s has invalid type %v", self.fieldName, self.fieldType))
			}

			return nil
		}
	}

	return common.NewQueryError(common.InvalidArgument, fmt.Sprintf("Unknown column name %s", self.fieldName))
}

func NewModeAggregator(_ *parser.Query, value *parser.Value) (Aggregator, error) {
	if len(value.Elems) != 1 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function mode() requires exactly one argument")
	}

	return &ModeAggregator{
		fieldName: value.Elems[0].Name,
		counts:    make(map[string]map[interface{}]map[interface{}]int),
	}, nil
}

//
// Max Aggregator
//

type MaxAggregator struct {
	fieldName  string
	fieldIndex int
	fieldType  protocol.FieldDefinition_Type
	values     map[string]map[interface{}]protocol.FieldValue
}

func (self *MaxAggregator) AggregatePoint(series string, group interface{}, p *protocol.Point) error {
	values := self.values[series]
	if values == nil {
		values = make(map[interface{}]protocol.FieldValue)
		self.values[series] = values
	}

	currentValue := values[group]

	switch self.fieldType {
	case protocol.FieldDefinition_INT64:
		if value := *p.Values[self.fieldIndex].Int64Value; currentValue.Int64Value == nil || *currentValue.Int64Value < value {
			currentValue.Int64Value = &value
		}
	case protocol.FieldDefinition_INT32:
		if value := *p.Values[self.fieldIndex].IntValue; currentValue.IntValue == nil || *currentValue.IntValue < value {
			currentValue.IntValue = &value
		}
	case protocol.FieldDefinition_DOUBLE:
		if value := *p.Values[self.fieldIndex].DoubleValue; currentValue.DoubleValue == nil || *currentValue.DoubleValue < value {
			currentValue.DoubleValue = &value
		}
	}

	values[group] = currentValue
	return nil
}

func (self *MaxAggregator) ColumnName() string {
	return "max"
}

func (self *MaxAggregator) ColumnType() protocol.FieldDefinition_Type {
	return self.fieldType
}

func (self *MaxAggregator) GetValue(series string, group interface{}) *protocol.FieldValue {
	value := self.values[series][group]
	return &value
}

func (self *MaxAggregator) InitializeFieldsMetadata(series *protocol.Series) error {
	for idx, field := range series.Fields {
		if *field.Name == self.fieldName {
			self.fieldIndex = idx
			self.fieldType = *field.Type

			switch self.fieldType {
			case protocol.FieldDefinition_INT32,
				protocol.FieldDefinition_INT64,
				protocol.FieldDefinition_DOUBLE:
				// that's fine
			default:
				return common.NewQueryError(common.InvalidArgument, fmt.Sprintf("Field %s has invalid type %v", self.fieldName, self.fieldType))
			}

			return nil
		}
	}

	return common.NewQueryError(common.InvalidArgument, fmt.Sprintf("Unknown column name %s", self.fieldName))
}

func NewMaxAggregator(_ *parser.Query, value *parser.Value) (Aggregator, error) {
	if len(value.Elems) != 1 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function max() requires only one argument")
	}

	return &MaxAggregator{
		fieldName: value.Elems[0].Name,
		values:    make(map[string]map[interface{}]protocol.FieldValue),
	}, nil
}

//
// Min Aggregator
//

type MinAggregator struct {
	fieldName  string
	fieldIndex int
	fieldType  protocol.FieldDefinition_Type
	values     map[string]map[interface{}]protocol.FieldValue
}

func (self *MinAggregator) AggregatePoint(series string, group interface{}, p *protocol.Point) error {
	values := self.values[series]
	if values == nil {
		values = make(map[interface{}]protocol.FieldValue)
		self.values[series] = values
	}

	currentValue := values[group]

	switch self.fieldType {
	case protocol.FieldDefinition_INT64:
		if value := *p.Values[self.fieldIndex].Int64Value; currentValue.Int64Value == nil || *currentValue.Int64Value > value {
			currentValue.Int64Value = &value
		}
	case protocol.FieldDefinition_INT32:
		if value := *p.Values[self.fieldIndex].IntValue; currentValue.IntValue == nil || *currentValue.IntValue > value {
			currentValue.IntValue = &value
		}
	case protocol.FieldDefinition_DOUBLE:
		if value := *p.Values[self.fieldIndex].DoubleValue; currentValue.DoubleValue == nil || *currentValue.DoubleValue > value {
			currentValue.DoubleValue = &value
		}
	}

	values[group] = currentValue
	return nil
}

func (self *MinAggregator) ColumnName() string {
	return "min"
}

func (self *MinAggregator) ColumnType() protocol.FieldDefinition_Type {
	return self.fieldType
}

func (self *MinAggregator) GetValue(series string, group interface{}) *protocol.FieldValue {
	value := self.values[series][group]
	return &value
}

func (self *MinAggregator) InitializeFieldsMetadata(series *protocol.Series) error {
	for idx, field := range series.Fields {
		if *field.Name == self.fieldName {
			self.fieldIndex = idx
			self.fieldType = *field.Type

			switch self.fieldType {
			case protocol.FieldDefinition_INT32,
				protocol.FieldDefinition_INT64,
				protocol.FieldDefinition_DOUBLE:
				// that's fine
			default:
				return common.NewQueryError(common.InvalidArgument, fmt.Sprintf("Field %s has invalid type %v", self.fieldName, self.fieldType))
			}

			return nil
		}
	}

	return common.NewQueryError(common.InvalidArgument, fmt.Sprintf("Unknown column name %s", self.fieldName))
}

func NewMinAggregator(_ *parser.Query, value *parser.Value) (Aggregator, error) {
	if len(value.Elems) != 1 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function min() requires only one argument")
	}

	return &MinAggregator{
		fieldName: value.Elems[0].Name,
		values:    make(map[string]map[interface{}]protocol.FieldValue),
	}, nil
}
