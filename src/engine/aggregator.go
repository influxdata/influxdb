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
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function percentile(...) requires exactly two arguments")
	}
	percentile, err := strconv.ParseFloat(value.Elems[1].Name, 64)

	if err != nil {
		return nil, common.NewQueryError(common.InvalidArgument, "not a valid number")
	}

	return &PercentileAggregator{
		fieldName:    value.Elems[0].Name,
		percentile:   percentile,
		int_values:   make(map[string]map[interface{}][]int),
		float_values: make(map[string]map[interface{}][]float64),
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
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "max take one argument only")
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
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "max take one argument only")
	}

	return &MinAggregator{
		fieldName: value.Elems[0].Name,
		values:    make(map[string]map[interface{}]protocol.FieldValue),
	}, nil
}
