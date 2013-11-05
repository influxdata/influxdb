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
	GetValue(series string, group interface{}) []*protocol.FieldValue
	ColumnName() string
}

type AggregatorInitializer func(*parser.Query, *parser.Value) (Aggregator, error)

var registeredAggregators = make(map[string]AggregatorInitializer)

func init() {
	registeredAggregators["count"] = NewCountAggregator
	registeredAggregators["derivative"] = NewDerivativeAggregator
	registeredAggregators["max"] = NewMaxAggregator
	registeredAggregators["min"] = NewMinAggregator
	registeredAggregators["sum"] = NewSumAggregator
	registeredAggregators["percentile"] = NewPercentileAggregator
	registeredAggregators["median"] = NewMedianAggregator
	registeredAggregators["mean"] = NewMeanAggregator
	registeredAggregators["mode"] = NewModeAggregator
	registeredAggregators["distinct"] = NewDistinctAggregator
}

//
// Composite Aggregator
//

type CompositeAggregator struct {
	left  Aggregator
	right Aggregator
}

func (self *CompositeAggregator) AggregatePoint(series string, group interface{}, p *protocol.Point) error {
	return self.right.AggregatePoint(series, group, p)
}

func (self *CompositeAggregator) ColumnName() string {
	return self.left.ColumnName()
}

func (self *CompositeAggregator) GetValue(series string, group interface{}) []*protocol.FieldValue {
	values := self.right.GetValue(series, group)
	for _, v := range values {
		point := &protocol.Point{Values: []*protocol.FieldValue{v}}
		self.left.AggregatePoint(series, group, point)
	}
	return self.left.GetValue(series, group)
}

func (self *CompositeAggregator) InitializeFieldsMetadata(series *protocol.Series) error {
	return self.right.InitializeFieldsMetadata(series)
}

func NewCompositeAggregator(left, right Aggregator) (Aggregator, error) {
	return &CompositeAggregator{left, right}, nil
}

//
// Derivative Aggregator
//

type DerivativeAggregator struct {
	fieldIndex int
	fieldName  string
	lastValues map[string]map[interface{}]*protocol.Point
	points     map[string]map[interface{}][]*protocol.FieldValue
}

func (self *DerivativeAggregator) AggregatePoint(series string, group interface{}, p *protocol.Point) error {
	lastValues := self.lastValues[series]
	if lastValues == nil {
		lastValues = make(map[interface{}]*protocol.Point)
		self.lastValues[series] = lastValues
	}

	var value float64
	if ptr := p.Values[self.fieldIndex].Int64Value; ptr != nil {
		value = float64(*ptr)
	} else if ptr := p.Values[self.fieldIndex].DoubleValue; ptr != nil {
		value = *ptr
	} else {
		// else ignore this point
		return nil
	}

	newValue := &protocol.Point{
		Timestamp:      p.Timestamp,
		SequenceNumber: p.SequenceNumber,
		Values:         []*protocol.FieldValue{&protocol.FieldValue{DoubleValue: &value}},
	}

	var oldValue *protocol.Point
	oldValue, lastValues[group] = lastValues[group], newValue
	if oldValue == nil {
		return nil
	}

	// if an old value exist, then compute the derivative and insert it in the points slice
	deltaT := float64(*newValue.Timestamp-*oldValue.Timestamp) / float64(time.Second/time.Microsecond)
	deltaV := *newValue.Values[self.fieldIndex].DoubleValue - *oldValue.Values[self.fieldIndex].DoubleValue
	derivative := deltaV / deltaT
	points := self.points[series]
	if points == nil {
		points = make(map[interface{}][]*protocol.FieldValue)
		self.points[series] = points
	}
	points[group] = append(points[group], &protocol.FieldValue{DoubleValue: &derivative})
	return nil
}

func (self *DerivativeAggregator) ColumnName() string {
	return "derivative"
}

func (self *DerivativeAggregator) GetValue(series string, group interface{}) []*protocol.FieldValue {
	return self.points[series][group]
}

func (self *DerivativeAggregator) InitializeFieldsMetadata(series *protocol.Series) error {
	for idx, field := range series.Fields {
		if field == self.fieldName {
			self.fieldIndex = idx
			return nil
		}
	}

	return common.NewQueryError(common.InvalidArgument, fmt.Sprintf("Unknown column name %s", self.fieldName))
}

func NewDerivativeAggregator(q *parser.Query, v *parser.Value) (Aggregator, error) {
	if len(v.Elems) != 1 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function derivative() requires exactly one argument")
	}

	if v.Elems[0].Type == parser.ValueWildcard {
		return nil, common.NewQueryError(common.InvalidArgument, "function derivative() doesn't work with wildcards")
	}

	return &DerivativeAggregator{
		fieldName:  v.Elems[0].Name,
		lastValues: make(map[string]map[interface{}]*protocol.Point),
		points:     make(map[string]map[interface{}][]*protocol.FieldValue),
	}, nil
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

func (self *CountAggregator) GetValue(series string, group interface{}) []*protocol.FieldValue {
	returnValues := []*protocol.FieldValue{}
	value := int64(self.counts[series][group])
	returnValues = append(returnValues, &protocol.FieldValue{Int64Value: &value})

	return returnValues
}

func (self *CountAggregator) InitializeFieldsMetadata(series *protocol.Series) error { return nil }

func NewCountAggregator(q *parser.Query, v *parser.Value) (Aggregator, error) {
	if len(v.Elems) != 1 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function count() requires exactly one argument")
	}

	if v.Elems[0].Type == parser.ValueWildcard {
		return nil, common.NewQueryError(common.InvalidArgument, "function count() doesn't work with wildcards")
	}

	if v.Elems[0].Type != parser.ValueSimpleName {
		innerName := v.Elems[0].Name
		init := registeredAggregators[innerName]
		if init == nil {
			return nil, common.NewQueryError(common.InvalidArgument, fmt.Sprintf("Unknown function %s", innerName))
		}
		inner, err := init(q, v.Elems[0])
		if err != nil {
			return nil, err
		}
		return NewCompositeAggregator(&CountAggregator{make(map[string]map[interface{}]int32)}, inner)
	}

	return &CountAggregator{make(map[string]map[interface{}]int32)}, nil
}

//
// Timestamp Aggregator
//

type TimestampAggregator struct {
	duration   *int64
	timestamps map[string]map[interface{}]int64
}

func (self *TimestampAggregator) AggregatePoint(series string, group interface{}, p *protocol.Point) error {
	timestamps := self.timestamps[series]
	if timestamps == nil {
		timestamps = make(map[interface{}]int64)
		self.timestamps[series] = timestamps
	}
	if self.duration != nil {
		timestamps[group] = *p.GetTimestampInMicroseconds() / *self.duration * *self.duration
	} else {
		timestamps[group] = *p.GetTimestampInMicroseconds()
	}
	return nil
}

func (self *TimestampAggregator) ColumnName() string {
	return "count"
}

func (self *TimestampAggregator) GetValue(series string, group interface{}) []*protocol.FieldValue {
	returnValues := []*protocol.FieldValue{}
	value := self.timestamps[series][group]
	returnValues = append(returnValues, &protocol.FieldValue{Int64Value: &value})

	return returnValues
}

func (self *TimestampAggregator) InitializeFieldsMetadata(series *protocol.Series) error { return nil }

func NewTimestampAggregator(query *parser.Query, _ *parser.Value) (Aggregator, error) {
	duration, err := query.GetGroupByClause().GetGroupByTime()
	if err != nil {
		return nil, err
	}

	var durationPtr *int64

	if duration != nil {
		newDuration := int64(*duration / time.Microsecond)
		durationPtr = &newDuration
	}

	return &TimestampAggregator{
		timestamps: make(map[string]map[interface{}]int64),
		duration:   durationPtr,
	}, nil
}

//
// Mean Aggregator
//

type MeanAggregator struct {
	fieldName  string
	fieldIndex int
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
	if ptr := p.Values[self.fieldIndex].Int64Value; ptr != nil {
		value = float64(*ptr)
	} else if ptr := p.Values[self.fieldIndex].DoubleValue; ptr != nil {
		value = *ptr
	}

	currentMean = currentMean*float64(currentCount-1)/float64(currentCount) + value/float64(currentCount)

	means[group] = currentMean
	counts[group] = currentCount
	return nil
}

func (self *MeanAggregator) ColumnName() string {
	return "mean"
}

func (self *MeanAggregator) GetValue(series string, group interface{}) []*protocol.FieldValue {
	returnValues := []*protocol.FieldValue{}
	mean := self.means[series][group]
	returnValues = append(returnValues, &protocol.FieldValue{DoubleValue: &mean})

	return returnValues
}

func (self *MeanAggregator) InitializeFieldsMetadata(series *protocol.Series) error {
	for idx, field := range series.Fields {
		if field == self.fieldName {
			self.fieldIndex = idx
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

func NewMedianAggregator(_ *parser.Query, value *parser.Value) (Aggregator, error) {
	if len(value.Elems) != 1 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function median() requires exactly one argument")
	}

	return &PercentileAggregator{
		functionName: "median",
		fieldName:    value.Elems[0].Name,
		percentile:   50.0,
		float_values: make(map[string]map[interface{}][]float64),
	}, nil
}

//
// Percentile Aggregator
//

type PercentileAggregator struct {
	functionName string
	fieldName    string
	fieldIndex   int
	percentile   float64
	float_values map[string]map[interface{}][]float64
}

func (self *PercentileAggregator) AggregatePoint(series string, group interface{}, p *protocol.Point) error {
	value := 0.0
	v := p.Values[self.fieldIndex]
	if v.Int64Value != nil {
		value = float64(*v.Int64Value)
	} else if v.DoubleValue != nil {
		value = *v.DoubleValue
	} else {
		return nil
	}

	values := self.float_values[series]
	if values == nil {
		values = map[interface{}][]float64{}
		self.float_values[series] = values
	}

	values[group] = append(values[group], value)

	return nil
}

func (self *PercentileAggregator) ColumnName() string {
	return self.functionName
}

func (self *PercentileAggregator) GetValue(series string, group interface{}) []*protocol.FieldValue {
	returnValues := []*protocol.FieldValue{}

	sort.Float64s(self.float_values[series][group])
	length := len(self.float_values[series][group])
	index := int(math.Floor(float64(length)*self.percentile/100.0+0.5)) - 1
	point := self.float_values[series][group][index]
	returnValues = append(returnValues, &protocol.FieldValue{DoubleValue: &point})

	return returnValues
}

func (self *PercentileAggregator) InitializeFieldsMetadata(series *protocol.Series) error {
	for idx, field := range series.Fields {
		if field == self.fieldName {
			self.fieldIndex = idx
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
		functionName: "percentile",
		fieldName:    value.Elems[0].Name,
		percentile:   percentile,
		float_values: make(map[string]map[interface{}][]float64),
	}, nil
}

//
// Mode Aggregator
//

type ModeAggregator struct {
	fieldName  string
	fieldIndex int
	counts     map[string]map[interface{}]map[float64]int
}

func (self *ModeAggregator) AggregatePoint(series string, group interface{}, p *protocol.Point) error {
	seriesCounts := self.counts[series]
	if seriesCounts == nil {
		seriesCounts = make(map[interface{}]map[float64]int)
		self.counts[series] = seriesCounts
	}

	groupCounts := seriesCounts[group]
	if groupCounts == nil {
		groupCounts = make(map[float64]int)
	}

	var value float64
	point := p.Values[self.fieldIndex]
	if point.Int64Value != nil {
		value = float64(*point.Int64Value)
	} else if point.DoubleValue != nil {
		value = *point.DoubleValue
	} else {
		return nil
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

func (self *ModeAggregator) GetValue(series string, group interface{}) []*protocol.FieldValue {
	returnValues := []*protocol.FieldValue{}

	values := []float64{}
	currentCount := 1

	for value, count := range self.counts[series][group] {
		if count == currentCount {
			values = append(values, value)
		} else if count > currentCount {
			values = nil
			values = append(values, value)
			currentCount = count
		}
	}

	for _, value := range values {
		// we can't use value since we need a pointer to a variable that won't change,
		// while value will change the next iteration
		v := value
		returnValues = append(returnValues, &protocol.FieldValue{DoubleValue: &v})
	}

	return returnValues
}

func (self *ModeAggregator) InitializeFieldsMetadata(series *protocol.Series) error {
	for idx, field := range series.Fields {
		if field == self.fieldName {
			self.fieldIndex = idx

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
		counts:    make(map[string]map[interface{}]map[float64]int),
	}, nil
}

//
// Distinct Aggregator
//

type DistinctAggregator struct {
	fieldName  string
	fieldIndex int
	counts     map[string]map[interface{}]map[interface{}]int
}

func (self *DistinctAggregator) AggregatePoint(series string, group interface{}, p *protocol.Point) error {
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
	point := p.Values[self.fieldIndex]
	if point.Int64Value != nil {
		value = float64(*point.Int64Value)
	} else if point.DoubleValue != nil {
		value = *point.DoubleValue
	} else if point.BoolValue != nil {
		value = *point.BoolValue
	} else if point.StringValue != nil {
		value = *point.StringValue
	} else {
		return nil
	}

	count := groupCounts[value]
	count += 1
	groupCounts[value] = count
	seriesCounts[group] = groupCounts

	return nil
}

func (self *DistinctAggregator) ColumnName() string {
	return "distinct"
}

func (self *DistinctAggregator) GetValue(series string, group interface{}) []*protocol.FieldValue {
	returnValues := []*protocol.FieldValue{}

	for value, _ := range self.counts[series][group] {
		switch v := value.(type) {
		case int:
			i := int64(v)
			returnValues = append(returnValues, &protocol.FieldValue{Int64Value: &i})
		case string:
			returnValues = append(returnValues, &protocol.FieldValue{StringValue: &v})
		case bool:
			returnValues = append(returnValues, &protocol.FieldValue{BoolValue: &v})
		case float64:
			returnValues = append(returnValues, &protocol.FieldValue{DoubleValue: &v})
		}
	}

	return returnValues
}

func (self *DistinctAggregator) InitializeFieldsMetadata(series *protocol.Series) error {
	for idx, field := range series.Fields {
		if field == self.fieldName {
			self.fieldIndex = idx
			return nil
		}
	}

	return common.NewQueryError(common.InvalidArgument, fmt.Sprintf("Unknown column name %s", self.fieldName))
}

func NewDistinctAggregator(_ *parser.Query, value *parser.Value) (Aggregator, error) {
	return &DistinctAggregator{
		fieldName: value.Elems[0].Name,
		counts:    make(map[string]map[interface{}]map[interface{}]int),
	}, nil
}

//
// Max, Min and Sum Aggregators
//

type Operation func(currentValue float64, newValue *protocol.FieldValue) float64

type CumulativeArithmeticAggregator struct {
	name         string
	fieldName    string
	fieldIndex   int
	values       map[string]map[interface{}]float64
	operation    Operation
	initialValue float64
}

func (self *CumulativeArithmeticAggregator) AggregatePoint(series string, group interface{}, p *protocol.Point) error {
	values := self.values[series]
	if values == nil {
		values = make(map[interface{}]float64)
		self.values[series] = values
	}
	currentValue, ok := values[group]
	if !ok {
		currentValue = self.initialValue
	}
	values[group] = self.operation(currentValue, p.Values[self.fieldIndex])
	return nil
}

func (self *CumulativeArithmeticAggregator) ColumnName() string {
	return self.name
}

func (self *CumulativeArithmeticAggregator) GetValue(series string, group interface{}) []*protocol.FieldValue {
	returnValues := []*protocol.FieldValue{}
	value := self.values[series][group]
	returnValues = append(returnValues, &protocol.FieldValue{DoubleValue: &value})
	return returnValues
}

func (self *CumulativeArithmeticAggregator) InitializeFieldsMetadata(series *protocol.Series) error {
	for idx, field := range series.Fields {
		if field == self.fieldName {
			self.fieldIndex = idx
			return nil
		}
	}

	return common.NewQueryError(common.InvalidArgument, fmt.Sprintf("Unknown column name %s", self.fieldName))
}

func NewCumulativeArithmeticAggregator(name string, value *parser.Value, initialValue float64, operation Operation) (Aggregator, error) {
	if len(value.Elems) != 1 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function max() requires only one argument")
	}

	return &CumulativeArithmeticAggregator{
		name:         name,
		fieldName:    value.Elems[0].Name,
		values:       make(map[string]map[interface{}]float64),
		operation:    operation,
		initialValue: initialValue,
	}, nil
}

func NewMaxAggregator(_ *parser.Query, value *parser.Value) (Aggregator, error) {
	return NewCumulativeArithmeticAggregator("max", value, -math.MaxFloat64, func(currentValue float64, p *protocol.FieldValue) float64 {
		if p.Int64Value != nil {
			if fv := float64(*p.Int64Value); fv > currentValue {
				return fv
			}
		} else if p.DoubleValue != nil {
			if fv := *p.DoubleValue; fv > currentValue {
				return fv
			}
		}
		return currentValue
	})
}

func NewMinAggregator(_ *parser.Query, value *parser.Value) (Aggregator, error) {
	return NewCumulativeArithmeticAggregator("min", value, math.MaxFloat64, func(currentValue float64, p *protocol.FieldValue) float64 {
		if p.Int64Value != nil {
			if fv := float64(*p.Int64Value); fv < currentValue {
				return fv
			}
		} else if p.DoubleValue != nil {
			if fv := *p.DoubleValue; fv < currentValue {
				return fv
			}
		}
		return currentValue
	})
}

func NewSumAggregator(_ *parser.Query, value *parser.Value) (Aggregator, error) {
	return NewCumulativeArithmeticAggregator("sum", value, 0, func(currentValue float64, p *protocol.FieldValue) float64 {
		var fv float64
		if p.Int64Value != nil {
			fv = float64(*p.Int64Value)
		} else if p.DoubleValue != nil {
			fv = *p.DoubleValue
		}
		return currentValue + fv
	})
}
