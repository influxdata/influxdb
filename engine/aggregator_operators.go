package engine

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/influxdb/influxdb/common"
	"github.com/influxdb/influxdb/parser"
	"github.com/influxdb/influxdb/protocol"
)

type PointSlice []protocol.Point

type Aggregator interface {
	AggregatePoint(state interface{}, p *protocol.Point) (interface{}, error)
	InitializeFieldsMetadata(series *protocol.Series) error
	GetValues(state interface{}) [][]*protocol.FieldValue
	CalculateSummaries(state interface{})
	ColumnNames() []string
}

// Initialize a new aggregator given the query, the function call of
// the aggregator and the default value that should be returned if
// the bucket doesn't have any points
type AggregatorInitializer func(*parser.SelectQuery, *parser.Value, *parser.Value) (Aggregator, error)

var registeredAggregators = make(map[string]AggregatorInitializer)

func init() {
	registeredAggregators["max"] = NewMaxAggregator
	registeredAggregators["count"] = NewCountAggregator
	registeredAggregators["histogram"] = NewHistogramAggregator
	registeredAggregators["derivative"] = NewDerivativeAggregator
	registeredAggregators["difference"] = NewDifferenceAggregator
	registeredAggregators["stddev"] = NewStandardDeviationAggregator
	registeredAggregators["min"] = NewMinAggregator
	registeredAggregators["sum"] = NewSumAggregator
	registeredAggregators["percentile"] = NewPercentileAggregator
	registeredAggregators["median"] = NewMedianAggregator
	registeredAggregators["mean"] = NewMeanAggregator
	registeredAggregators["mode"] = NewModeAggregator
	registeredAggregators["distinct"] = NewDistinctAggregator
	registeredAggregators["first"] = NewFirstAggregator
	registeredAggregators["last"] = NewLastAggregator
	registeredAggregators["top"] = NewTopAggregator
	registeredAggregators["bottom"] = NewBottomAggregator
}

// used in testing to get a list of all aggregators
func GetRegisteredAggregators() (names []string) {
	for n := range registeredAggregators {
		names = append(names, n)
	}
	return
}

type AbstractAggregator struct {
	Aggregator
	value   *parser.Value
	columns []string
}

func (self *AbstractAggregator) InitializeFieldsMetadata(series *protocol.Series) error {
	self.columns = series.Fields
	return nil
}

func (self *AbstractAggregator) CalculateSummaries(state interface{}) {
}

func wrapDefaultValue(defaultValue *parser.Value) (*protocol.FieldValue, error) {
	if defaultValue == nil {
		return nil, nil
	}

	switch defaultValue.Type {
	case parser.ValueInt:
		v, _ := strconv.Atoi(defaultValue.Name)
		value := int64(v)
		return &protocol.FieldValue{Int64Value: &value}, nil
	case parser.ValueSimpleName:
		if defaultValue.Name != "null" {
			return nil, fmt.Errorf("Unsupported fill value %s", defaultValue.Name)
		}
		return nil, nil
	default:
		return nil, fmt.Errorf("Unknown type %s", defaultValue.Type)
	}
}

type Operation func(currentValue float64, newValue *protocol.FieldValue) float64

type CumulativeArithmeticAggregatorState float64

type CumulativeArithmeticAggregator struct {
	AbstractAggregator
	name         string
	operation    Operation
	initialValue float64
	defaultValue *protocol.FieldValue
}

func (self *CumulativeArithmeticAggregator) AggregatePoint(state interface{}, p *protocol.Point) (interface{}, error) {
	if state == nil {
		state = self.initialValue
	}

	value, err := GetValue(self.value, self.columns, p)
	if err != nil {
		return nil, err
	}
	return self.operation(state.(float64), value), nil
}

func (self *CumulativeArithmeticAggregator) ColumnNames() []string {
	return []string{self.name}
}

func (self *CumulativeArithmeticAggregator) GetValues(state interface{}) [][]*protocol.FieldValue {
	if state == nil {
		return [][]*protocol.FieldValue{
			{
				{DoubleValue: &self.initialValue},
			},
		}
	}

	return [][]*protocol.FieldValue{
		{
			{
				DoubleValue: protocol.Float64(state.(float64)),
			},
		},
	}
}

func NewCumulativeArithmeticAggregator(name string, value *parser.Value, initialValue float64, defaultValue *parser.Value, operation Operation) (Aggregator, error) {
	if len(value.Elems) != 1 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function max() requires only one argument")
	}

	wrappedDefaultValue, err := wrapDefaultValue(defaultValue)
	if err != nil {
		return nil, err
	}

	if value.Alias != "" {
		name = value.Alias
	}

	return &CumulativeArithmeticAggregator{
		AbstractAggregator: AbstractAggregator{
			value: value.Elems[0],
		},
		name:         name,
		operation:    operation,
		initialValue: initialValue,
		defaultValue: wrappedDefaultValue,
	}, nil
}

func NewMaxAggregator(_ *parser.SelectQuery, value *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	return NewCumulativeArithmeticAggregator("max", value, -math.MaxFloat64, defaultValue, func(currentValue float64, p *protocol.FieldValue) float64 {
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

func NewMinAggregator(_ *parser.SelectQuery, value *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	return NewCumulativeArithmeticAggregator("min", value, math.MaxFloat64, defaultValue, func(currentValue float64, p *protocol.FieldValue) float64 {
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

func NewSumAggregator(_ *parser.SelectQuery, value *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	return NewCumulativeArithmeticAggregator("sum", value, 0, defaultValue, func(currentValue float64, p *protocol.FieldValue) float64 {
		var fv float64
		if p.Int64Value != nil {
			fv = float64(*p.Int64Value)
		} else if p.DoubleValue != nil {
			fv = *p.DoubleValue
		}
		return currentValue + fv
	})
}

//
// Composite Aggregator
//

type CompositeAggregatorState struct {
	rightState interface{}
	leftState  interface{}
}

type CompositeAggregator struct {
	left  Aggregator
	right Aggregator
}

func (self *CompositeAggregator) AggregatePoint(state interface{}, p *protocol.Point) (interface{}, error) {
	s, ok := state.(*CompositeAggregatorState)
	if !ok {
		s = &CompositeAggregatorState{}
	}
	var err error
	s.rightState, err = self.right.AggregatePoint(s.rightState, p)
	return s, err
}

func (self *CompositeAggregator) ColumnNames() []string {
	return self.left.ColumnNames()
}

func (self *CompositeAggregator) CalculateSummaries(state interface{}) {
	s := state.(*CompositeAggregatorState)
	self.right.CalculateSummaries(s.rightState)
	values := self.right.GetValues(s.rightState)
	for _, v := range values {
		point := &protocol.Point{Values: v}
		var err error
		s.leftState, err = self.left.AggregatePoint(s.leftState, point)
		if err != nil {
			panic(fmt.Errorf("Error returned from aggregator: %s", err))
		}
	}
	s.rightState = nil
	self.left.CalculateSummaries(s.leftState)
}

func (self *CompositeAggregator) GetValues(state interface{}) [][]*protocol.FieldValue {
	s, ok := state.(*CompositeAggregatorState)
	if !ok {
		return self.left.GetValues(nil)
	}
	return self.left.GetValues(s.leftState)
}

func (self *CompositeAggregator) InitializeFieldsMetadata(series *protocol.Series) error {
	return self.right.InitializeFieldsMetadata(series)
}

func NewCompositeAggregator(left, right Aggregator) (Aggregator, error) {
	return &CompositeAggregator{left, right}, nil
}

// StandardDeviation Aggregator

type StandardDeviationRunning struct {
	count   int
	totalX2 float64
	totalX  float64
}

type StandardDeviationAggregator struct {
	AbstractAggregator
	defaultValue *protocol.FieldValue
	alias        string
}

func (self *StandardDeviationAggregator) AggregatePoint(state interface{}, p *protocol.Point) (interface{}, error) {
	fieldValue, err := GetValue(self.value, self.columns, p)
	if err != nil {
		return nil, err
	}

	var value float64
	if ptr := fieldValue.Int64Value; ptr != nil {
		value = float64(*ptr)
	} else if ptr := fieldValue.DoubleValue; ptr != nil {
		value = *ptr
	} else {
		// else ignore this point
		return state, nil
	}

	running, ok := state.(*StandardDeviationRunning)
	if !ok {
		running = &StandardDeviationRunning{}
	}

	running.count++
	running.totalX += value
	running.totalX2 += value * value
	return running, nil
}

func (self *StandardDeviationAggregator) ColumnNames() []string {
	if self.alias != "" {
		return []string{self.alias}
	}

	return []string{"stddev"}
}

func (self *StandardDeviationAggregator) GetValues(state interface{}) [][]*protocol.FieldValue {
	r, ok := state.(*StandardDeviationRunning)
	if !ok {
		return nil
	}

	eX := r.totalX / float64(r.count)
	eX *= eX
	eX2 := r.totalX2 / float64(r.count)
	standardDeviation := math.Sqrt(eX2 - eX)

	return [][]*protocol.FieldValue{
		{
			{DoubleValue: &standardDeviation},
		},
	}
}

func NewStandardDeviationAggregator(q *parser.SelectQuery, v *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	if len(v.Elems) != 1 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function stddev() requires exactly one argument")
	}

	if v.Elems[0].Type == parser.ValueWildcard {
		return nil, common.NewQueryError(common.InvalidArgument, "function stddev() doesn't work with wildcards")
	}

	value, err := wrapDefaultValue(defaultValue)
	if err != nil {
		return nil, err
	}
	return &StandardDeviationAggregator{
		AbstractAggregator: AbstractAggregator{
			value: v.Elems[0],
		},
		defaultValue: value,
		alias:        v.Alias,
	}, nil
}

//
// Derivative Aggregator
//

type DerivativeAggregatorState struct {
	firstValue *protocol.Point
	lastValue  *protocol.Point
}

type DerivativeAggregator struct {
	AbstractAggregator
	defaultValue *protocol.FieldValue
	alias        string
}

func (self *DerivativeAggregator) AggregatePoint(state interface{}, p *protocol.Point) (interface{}, error) {
	fieldValue, err := GetValue(self.value, self.columns, p)
	if err != nil {
		return nil, err
	}

	var value float64
	if ptr := fieldValue.Int64Value; ptr != nil {
		value = float64(*ptr)
	} else if ptr := fieldValue.DoubleValue; ptr != nil {
		value = *ptr
	} else {
		// else ignore this point
		return state, nil
	}

	newValue := &protocol.Point{
		Timestamp: p.Timestamp,
		Values:    []*protocol.FieldValue{{DoubleValue: &value}},
	}

	s, ok := state.(*DerivativeAggregatorState)
	if !ok {
		s = &DerivativeAggregatorState{}
	}

	if s.firstValue == nil {
		s.firstValue = newValue
		return s, nil
	}

	s.lastValue = newValue
	return s, nil
}

func (self *DerivativeAggregator) ColumnNames() []string {
	if self.alias != "" {
		return []string{self.alias}
	}
	return []string{"derivative"}
}

func (self *DerivativeAggregator) GetValues(state interface{}) [][]*protocol.FieldValue {
	s, ok := state.(*DerivativeAggregatorState)

	if !(ok && s.firstValue != nil && s.lastValue != nil) {
		return nil
	}

	// if an old value exist, then compute the derivative and insert it in the points slice
	deltaT := float64(*s.lastValue.Timestamp-*s.firstValue.Timestamp) / float64(time.Second/time.Microsecond)
	deltaV := *s.lastValue.Values[0].DoubleValue - *s.firstValue.Values[0].DoubleValue
	derivative := deltaV / deltaT
	return [][]*protocol.FieldValue{
		{
			{DoubleValue: &derivative},
		},
	}
}

func NewDerivativeAggregator(q *parser.SelectQuery, v *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	if len(v.Elems) != 1 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function derivative() requires exactly one argument")
	}

	if v.Elems[0].Type == parser.ValueWildcard {
		return nil, common.NewQueryError(common.InvalidArgument, "function derivative() doesn't work with wildcards")
	}

	wrappedDefaultValue, err := wrapDefaultValue(defaultValue)
	if err != nil {
		return nil, err
	}

	return &DerivativeAggregator{
		AbstractAggregator: AbstractAggregator{
			value: v.Elems[0],
		},
		defaultValue: wrappedDefaultValue,
		alias:        v.Alias,
	}, nil
}

//
// Difference Aggregator
//

type DifferenceAggregatorState struct {
	firstValue *protocol.Point
	lastValue  *protocol.Point
}

type DifferenceAggregator struct {
	AbstractAggregator
	defaultValue *protocol.FieldValue
	alias        string
}

func (self *DifferenceAggregator) AggregatePoint(state interface{}, p *protocol.Point) (interface{}, error) {
	fieldValue, err := GetValue(self.value, self.columns, p)
	if err != nil {
		return nil, err
	}

	var value float64
	if ptr := fieldValue.Int64Value; ptr != nil {
		value = float64(*ptr)
	} else if ptr := fieldValue.DoubleValue; ptr != nil {
		value = *ptr
	} else {
		// else ignore this point
		return state, nil
	}

	newValue := &protocol.Point{
		Timestamp: p.Timestamp,
		Values:    []*protocol.FieldValue{{DoubleValue: &value}},
	}

	s, ok := state.(*DifferenceAggregatorState)
	if !ok {
		s = &DifferenceAggregatorState{}
	}

	if s.firstValue == nil {
		s.firstValue = newValue
		return s, nil
	}

	s.lastValue = newValue
	return s, nil
}

func (self *DifferenceAggregator) ColumnNames() []string {
	if self.alias != "" {
		return []string{self.alias}
	}
	return []string{"difference"}
}

func (self *DifferenceAggregator) GetValues(state interface{}) [][]*protocol.FieldValue {
	s, ok := state.(*DifferenceAggregatorState)

	if !(ok && s.firstValue != nil && s.lastValue != nil) {
		return nil
	}

	difference := *s.lastValue.Values[0].DoubleValue - *s.firstValue.Values[0].DoubleValue
	return [][]*protocol.FieldValue{
		{
			{DoubleValue: &difference},
		},
	}
}

func NewDifferenceAggregator(q *parser.SelectQuery, v *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	if len(v.Elems) != 1 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function difference() requires exactly one argument")
	}

	if v.Elems[0].Type == parser.ValueWildcard {
		return nil, common.NewQueryError(common.InvalidArgument, "function difference() doesn't work with wildcards")
	}

	wrappedDefaultValue, err := wrapDefaultValue(defaultValue)
	if err != nil {
		return nil, err
	}

	return &DifferenceAggregator{
		AbstractAggregator: AbstractAggregator{
			value: v.Elems[0],
		},
		defaultValue: wrappedDefaultValue,
		alias:        v.Alias,
	}, nil
}

//
// Histogram Aggregator
//

type HistogramAggregatorState map[int]int

type HistogramAggregator struct {
	AbstractAggregator
	bucketSize  float64
	columnNames []string
}

func (self *HistogramAggregator) AggregatePoint(state interface{}, p *protocol.Point) (interface{}, error) {
	buckets, ok := state.(HistogramAggregatorState)
	if !ok {
		buckets = make(map[int]int)
	}

	fieldValue, err := GetValue(self.value, self.columns, p)
	if err != nil {
		return nil, err
	}

	var value float64
	if ptr := fieldValue.Int64Value; ptr != nil {
		value = float64(*ptr)
	} else if ptr := fieldValue.DoubleValue; ptr != nil {
		value = *ptr
	}

	bucket := int(value / self.bucketSize)
	buckets[bucket] += 1

	return buckets, nil
}

func (self *HistogramAggregator) ColumnNames() []string {
	return self.columnNames
}

func (self *HistogramAggregator) GetValues(state interface{}) [][]*protocol.FieldValue {
	returnValues := [][]*protocol.FieldValue{}
	buckets := state.(HistogramAggregatorState)
	for bucket, size := range buckets {
		_bucket := float64(bucket) * self.bucketSize
		_size := int64(size)

		returnValues = append(returnValues, []*protocol.FieldValue{
			{DoubleValue: &_bucket},
			{Int64Value: &_size},
		})
	}

	return returnValues
}

func NewHistogramAggregator(q *parser.SelectQuery, v *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	if len(v.Elems) < 1 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function histogram() requires at least one arguments")
	}

	if len(v.Elems) > 2 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function histogram() takes at most two arguments")
	}

	if v.Elems[0].Type == parser.ValueWildcard {
		return nil, common.NewQueryError(common.InvalidArgument, "function histogram() doesn't work with wildcards")
	}

	bucketSize := 1.0

	if len(v.Elems) == 2 {
		switch v.Elems[1].Type {
		case parser.ValueInt, parser.ValueFloat:
			var err error
			bucketSize, err = strconv.ParseFloat(v.Elems[1].Name, 64)
			if err != nil {
				return nil, common.NewQueryError(common.InvalidArgument, "Cannot parse %s into a float", v.Elems[1].Name)
			}
		default:
			return nil, common.NewQueryError(common.InvalidArgument, "Cannot parse %s into a float", v.Elems[1].Name)
		}
	}

	columnNames := []string{"bucket_start", "count"}
	if v.Alias != "" {
		columnNames[0] = fmt.Sprintf("%s_bucket_start", v.Alias)
		columnNames[1] = fmt.Sprintf("%s_count", v.Alias)
	}

	return &HistogramAggregator{
		AbstractAggregator: AbstractAggregator{
			value: v.Elems[0],
		},
		bucketSize:  bucketSize,
		columnNames: columnNames,
	}, nil
}

//
// Count Aggregator
//

type CountAggregator struct {
	AbstractAggregator
	defaultValue *protocol.FieldValue
	alias        string
}

type CountAggregatorState int64

func (self *CountAggregator) AggregatePoint(state interface{}, p *protocol.Point) (interface{}, error) {
	if state == nil {
		return int64(1), nil
	}
	return state.(int64) + 1, nil
}

func (self *CountAggregator) ColumnNames() []string {
	if self.alias != "" {
		return []string{self.alias}
	}
	return []string{"count"}
}

func (self *CountAggregator) GetValues(state interface{}) [][]*protocol.FieldValue {
	returnValues := [][]*protocol.FieldValue{}
	if state == nil {
		returnValues = append(returnValues, []*protocol.FieldValue{self.defaultValue})
	} else {
		value := state.(int64)
		returnValues = append(returnValues, []*protocol.FieldValue{
			{Int64Value: &value},
		})
	}

	return returnValues
}

func (self *CountAggregator) InitializeFieldsMetadata(series *protocol.Series) error { return nil }

func NewCountAggregator(q *parser.SelectQuery, v *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	if len(v.Elems) != 1 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function count() requires exactly one argument")
	}

	if v.Elems[0].Type == parser.ValueWildcard {
		return nil, common.NewQueryError(common.InvalidArgument, "function count() doesn't work with wildcards")
	}

	wrappedDefaultValue, err := wrapDefaultValue(defaultValue)
	if err != nil {
		return nil, err
	}

	if v.Elems[0].Type != parser.ValueSimpleName {
		innerName := strings.ToLower(v.Elems[0].Name)
		init := registeredAggregators[innerName]
		if init == nil {
			return nil, common.NewQueryError(common.InvalidArgument, fmt.Sprintf("Unknown function %s", innerName))
		}
		inner, err := init(q, v.Elems[0], defaultValue)
		if err != nil {
			return nil, err
		}
		return NewCompositeAggregator(&CountAggregator{AbstractAggregator{}, wrappedDefaultValue, v.Alias}, inner)
	}

	return &CountAggregator{AbstractAggregator{}, wrappedDefaultValue, v.Alias}, nil
}

//
// Mean Aggregator
//

type MeanAggregatorState struct {
	mean  float64
	count float64
}

type MeanAggregator struct {
	AbstractAggregator
	defaultValue *protocol.FieldValue
	alias        string
}

func (self *MeanAggregator) AggregatePoint(state interface{}, p *protocol.Point) (interface{}, error) {
	s, ok := state.(*MeanAggregatorState)
	if !ok {
		s = &MeanAggregatorState{}
	}

	fieldValue, err := GetValue(self.value, self.columns, p)
	if err != nil {
		return nil, err
	}

	var value float64
	if ptr := fieldValue.Int64Value; ptr != nil {
		value = float64(*ptr)
	} else if ptr := fieldValue.DoubleValue; ptr != nil {
		value = *ptr
	}

	s.count++
	s.mean = s.mean*(s.count-1)/s.count + value/s.count

	return s, nil
}

func (self *MeanAggregator) ColumnNames() []string {
	if self.alias != "" {
		return []string{self.alias}
	}
	return []string{"mean"}
}

func (self *MeanAggregator) GetValues(state interface{}) [][]*protocol.FieldValue {
	returnValues := [][]*protocol.FieldValue{}
	s, ok := state.(*MeanAggregatorState)
	if !ok {
		returnValues = append(returnValues, []*protocol.FieldValue{self.defaultValue})
	} else {
		returnValues = append(returnValues, []*protocol.FieldValue{
			{DoubleValue: &s.mean},
		})
	}

	return returnValues
}

func NewMeanAggregator(_ *parser.SelectQuery, value *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	if len(value.Elems) != 1 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function mean() requires exactly one argument")
	}

	wrappedDefaultValue, err := wrapDefaultValue(defaultValue)
	if err != nil {
		return nil, err
	}

	return &MeanAggregator{
		AbstractAggregator: AbstractAggregator{
			value: value.Elems[0],
		},
		defaultValue: wrappedDefaultValue,
		alias:        value.Alias,
	}, nil
}

func NewMedianAggregator(_ *parser.SelectQuery, value *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	if len(value.Elems) != 1 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function median() requires exactly one argument")
	}

	wrappedDefaultValue, err := wrapDefaultValue(defaultValue)
	if err != nil {
		return nil, err
	}

	functionName := "median"
	if value.Alias != "" {
		functionName = value.Alias
	}

	aggregator := &PercentileAggregator{
		AbstractAggregator: AbstractAggregator{
			value: value.Elems[0],
		},
		functionName: functionName,
		percentile:   50.0,
		defaultValue: wrappedDefaultValue,
		alias:        value.Alias,
	}
	return aggregator, nil
}

//
// Percentile Aggregator
//

type PercentileAggregatorState struct {
	values          []float64
	percentileValue float64
}

type PercentileAggregator struct {
	AbstractAggregator
	functionName string
	percentile   float64
	defaultValue *protocol.FieldValue
	alias        string
}

func (self *PercentileAggregator) AggregatePoint(state interface{}, p *protocol.Point) (interface{}, error) {
	v, err := GetValue(self.value, self.columns, p)
	if err != nil {
		return nil, err
	}

	value := 0.0
	if v.Int64Value != nil {
		value = float64(*v.Int64Value)
	} else if v.DoubleValue != nil {
		value = *v.DoubleValue
	} else {
		return state, nil
	}

	s, ok := state.(*PercentileAggregatorState)
	if !ok {
		s = &PercentileAggregatorState{}
	}

	s.values = append(s.values, value)

	return s, nil
}

func (self *PercentileAggregator) ColumnNames() []string {
	if self.alias != "" {
		return []string{self.alias}
	}
	return []string{self.functionName}
}

func (self *PercentileAggregator) GetValues(state interface{}) [][]*protocol.FieldValue {
	s, ok := state.(*PercentileAggregatorState)
	if !ok {
		return [][]*protocol.FieldValue{
			{self.defaultValue},
		}
	}
	return [][]*protocol.FieldValue{
		{{DoubleValue: &s.percentileValue}},
	}
}

func (self *PercentileAggregator) CalculateSummaries(state interface{}) {
	s := state.(*PercentileAggregatorState)
	sort.Float64s(s.values)
	length := len(s.values)
	index := int(math.Floor(float64(length)*self.percentile/100.0+0.5)) - 1

	if index < 0 || index >= len(s.values) {
		return
	}

	s.percentileValue = s.values[index]
	s.values = nil
}

func NewPercentileAggregator(_ *parser.SelectQuery, value *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	if len(value.Elems) != 2 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function percentile() requires exactly two arguments")
	}

	if value.Elems[0].Type == parser.ValueWildcard {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "wildcard cannot be used with percentile")
	}

	percentile, err := strconv.ParseFloat(value.Elems[1].Name, 64)

	if err != nil || percentile <= 0 || percentile >= 100 {
		return nil, common.NewQueryError(common.InvalidArgument, "function percentile() requires a numeric second argument between 0 and 100")
	}

	wrappedDefaultValue, err := wrapDefaultValue(defaultValue)
	if err != nil {
		return nil, err
	}

	functionName := "percentile"
	if value.Alias != "" {
		functionName = value.Alias
	}

	return &PercentileAggregator{
		AbstractAggregator: AbstractAggregator{
			value: value.Elems[0],
		},
		functionName: functionName,
		percentile:   percentile,
		defaultValue: wrappedDefaultValue,
	}, nil
}

//
// Mode Aggregator
//

type ModeAggregatorState struct {
	counts map[interface{}]int
}

type ModeAggregator struct {
	AbstractAggregator
	defaultValue *protocol.FieldValue
	alias        string
	size         int
}

func (self *ModeAggregator) AggregatePoint(state interface{}, p *protocol.Point) (interface{}, error) {
	s, ok := state.(*ModeAggregatorState)
	if !ok {
		s = &ModeAggregatorState{make(map[interface{}]int)}
	}

	point, err := GetValue(self.value, self.columns, p)
	if err != nil {
		return nil, err
	}

	var value interface{}
	if point.Int64Value != nil {
		value = float64(*point.Int64Value)
	} else if point.DoubleValue != nil {
		value = *point.DoubleValue
	} else if point.BoolValue != nil {
		value = *point.BoolValue
	} else if point.StringValue != nil {
		value = *point.StringValue
	} else {
		value = nil
	}

	s.counts[value]++
	return s, nil
}

func (self *ModeAggregator) ColumnNames() []string {
	if self.alias != "" {
		return []string{self.alias}
	}
	return []string{"mode"}
}

func (self *ModeAggregator) GetValues(state interface{}) [][]*protocol.FieldValue {
	s := state.(*ModeAggregatorState)

	counts := make([]int, len(s.counts))
	countMap := make(map[int][]interface{}, len(s.counts))
	for value, count := range s.counts {
		counts = append(counts, count)
		countMap[count] = append(countMap[count], value)
	}

	// descending sort
	sort.Sort(sort.Reverse(sort.IntSlice(counts)))

	returnValues := [][]*protocol.FieldValue{}

	last := 0
	for _, count := range counts {
		// counts can contain duplicates, but we only want to append each count-set once
		if count == last {
			continue
		}
		last = count
		for _, value := range countMap[count] {
			switch v := value.(type) {
			case int:
				n := int64(v)
				returnValues = append(returnValues, []*protocol.FieldValue{{Int64Value: &n}})
			case string:
				returnValues = append(returnValues, []*protocol.FieldValue{{StringValue: &v}})
			case bool:
				returnValues = append(returnValues, []*protocol.FieldValue{{BoolValue: &v}})
			case float64:
				returnValues = append(returnValues, []*protocol.FieldValue{{DoubleValue: &v}})
			case nil:
				returnValues = append(returnValues, []*protocol.FieldValue{{IsNull: &TRUE}})
			}
		}
		// size is really "minimum size"
		if len(returnValues) >= self.size {
			break
		}
	}

	return returnValues
}

func NewModeAggregator(_ *parser.SelectQuery, value *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	if len(value.Elems) < 1 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function mode() requires at least one argument")
	}

	// TODO: Mode can in fact take two argument, the second specifies
	// the "size", but it's not clear if size is set to 2 whether to
	// return at least 2 elements, or return the most common values and
	// the second most common values. The difference will be apparent if
	// the data set is multimodel and there are two most common
	// values. In the first case, the two most common values will be
	// returned, but in the second case the two most common values and
	// the second most common values will be returned
	if len(value.Elems) > 1 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function mode() takes at most one arguments")
	}

	size := 1
	if len(value.Elems) == 2 {
		switch value.Elems[1].Type {
		case parser.ValueInt:
			var err error
			size, err = strconv.Atoi(value.Elems[1].Name)
			if err != nil {
				return nil, common.NewQueryError(common.InvalidArgument, "Cannot parse %s into an int", value.Elems[1].Name)
			}
		default:
			return nil, common.NewQueryError(common.InvalidArgument, "Cannot parse %s into a int", value.Elems[1].Name)
		}
	}

	wrappedDefaultValue, err := wrapDefaultValue(defaultValue)
	if err != nil {
		return nil, err
	}

	return &ModeAggregator{
		AbstractAggregator: AbstractAggregator{
			value: value.Elems[0],
		},
		defaultValue: wrappedDefaultValue,
		alias:        value.Alias,
		size:         size,
	}, nil
}

//
// Distinct Aggregator
//

type DistinctAggregatorState struct {
	counts map[interface{}]struct{}
}

type DistinctAggregator struct {
	AbstractAggregator
	defaultValue *protocol.FieldValue
	alias        string
}

func (self *DistinctAggregator) AggregatePoint(state interface{}, p *protocol.Point) (interface{}, error) {
	s, ok := state.(*DistinctAggregatorState)
	if !ok {
		s = &DistinctAggregatorState{make(map[interface{}]struct{})}
	}

	point, err := GetValue(self.value, self.columns, p)
	if err != nil {
		return nil, err
	}

	var value interface{}
	if point.Int64Value != nil {
		value = float64(*point.Int64Value)
	} else if point.DoubleValue != nil {
		value = *point.DoubleValue
	} else if point.BoolValue != nil {
		value = *point.BoolValue
	} else if point.StringValue != nil {
		value = *point.StringValue
	} else {
		value = nil
	}

	s.counts[value] = struct{}{}

	return s, nil
}

func (self *DistinctAggregator) ColumnNames() []string {
	if self.alias != "" {
		return []string{self.alias}
	}
	return []string{"distinct"}
}

func (self *DistinctAggregator) GetValues(state interface{}) [][]*protocol.FieldValue {
	returnValues := [][]*protocol.FieldValue{}
	s, ok := state.(*DistinctAggregatorState)
	if !ok || len(s.counts) == 0 {
		returnValues = append(returnValues, []*protocol.FieldValue{self.defaultValue})
	}

	for value := range s.counts {
		switch v := value.(type) {
		case int:
			i := int64(v)
			returnValues = append(returnValues, []*protocol.FieldValue{{Int64Value: &i}})
		case string:
			returnValues = append(returnValues, []*protocol.FieldValue{{StringValue: &v}})
		case bool:
			returnValues = append(returnValues, []*protocol.FieldValue{{BoolValue: &v}})
		case float64:
			returnValues = append(returnValues, []*protocol.FieldValue{{DoubleValue: &v}})
		}
	}

	return returnValues
}

func NewDistinctAggregator(_ *parser.SelectQuery, value *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	wrappedDefaultValue, err := wrapDefaultValue(defaultValue)
	if err != nil {
		return nil, err
	}

	return &DistinctAggregator{
		AbstractAggregator: AbstractAggregator{
			value: value.Elems[0],
		},
		defaultValue: wrappedDefaultValue,
		alias:        value.Alias,
	}, nil
}

//
// Max, Min and Sum Aggregators
//

type FirstOrLastAggregatorState *protocol.FieldValue

type FirstOrLastAggregator struct {
	AbstractAggregator
	name         string
	isFirst      bool
	defaultValue *protocol.FieldValue
}

func (self *FirstOrLastAggregator) AggregatePoint(state interface{}, p *protocol.Point) (interface{}, error) {
	value, err := GetValue(self.value, self.columns, p)
	if err != nil {
		return nil, err
	}

	if state == nil || !self.isFirst {
		state = FirstOrLastAggregatorState(value)
	}
	return state, nil
}

func (self *FirstOrLastAggregator) ColumnNames() []string {
	return []string{self.name}
}

func (self *FirstOrLastAggregator) GetValues(state interface{}) [][]*protocol.FieldValue {
	s := state.(FirstOrLastAggregatorState)
	return [][]*protocol.FieldValue{
		{
			s,
		},
	}
}

func NewFirstOrLastAggregator(name string, v *parser.Value, isFirst bool, defaultValue *parser.Value) (Aggregator, error) {
	if len(v.Elems) != 1 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, "function max() requires only one argument")
	}

	wrappedDefaultValue, err := wrapDefaultValue(defaultValue)
	if err != nil {
		return nil, err
	}

	if v.Alias != "" {
		name = v.Alias
	}

	return &FirstOrLastAggregator{
		AbstractAggregator: AbstractAggregator{
			value: v.Elems[0],
		},
		name:         name,
		isFirst:      isFirst,
		defaultValue: wrappedDefaultValue,
	}, nil
}

func NewFirstAggregator(_ *parser.SelectQuery, value *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	return NewFirstOrLastAggregator("first", value, true, defaultValue)
}

func NewLastAggregator(_ *parser.SelectQuery, value *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	return NewFirstOrLastAggregator("last", value, false, defaultValue)
}

//
// Top, Bottom aggregators
//
type ByPointColumnDesc struct {
	protocol.PointsCollection
}
type ByPointColumnAsc struct {
	protocol.PointsCollection
}

func (s ByPointColumnDesc) Less(i, j int) bool {
	if s.PointsCollection[i] == nil || s.PointsCollection[j] == nil {
		return false
	}

	if s.PointsCollection[i].Values[0].Int64Value != nil && s.PointsCollection[j].Values[0].Int64Value != nil {
		return *s.PointsCollection[i].Values[0].Int64Value > *s.PointsCollection[j].Values[0].Int64Value
	} else if s.PointsCollection[i].Values[0].DoubleValue != nil && s.PointsCollection[j].Values[0].DoubleValue != nil {
		return *s.PointsCollection[i].Values[0].DoubleValue > *s.PointsCollection[j].Values[0].DoubleValue
	} else if s.PointsCollection[i].Values[0].StringValue != nil && s.PointsCollection[j].Values[0].StringValue != nil {
		return *s.PointsCollection[i].Values[0].StringValue > *s.PointsCollection[j].Values[0].StringValue
	}

	return false
}

func (s ByPointColumnAsc) Less(i, j int) bool {
	if s.PointsCollection[i] == nil || s.PointsCollection[j] == nil {
		return false
	}

	return comparePointValue(s.PointsCollection[i], s.PointsCollection[j])
}

type TopOrBottomAggregatorState struct {
	values  protocol.PointsCollection
	counter int64
}

type TopOrBottomAggregator struct {
	AbstractAggregator
	name         string
	isTop        bool
	defaultValue *protocol.FieldValue
	alias        string
	limit        int64
	target       string
}

func comparePointValue(a, b *protocol.Point) bool {
	if a.Values[0].Int64Value != nil && b.Values[0].Int64Value != nil {
		return *a.Values[0].Int64Value < *b.Values[0].Int64Value
	} else if a.Values[0].DoubleValue != nil && b.Values[0].DoubleValue != nil {
		return *a.Values[0].DoubleValue < *b.Values[0].DoubleValue
	} else if a.Values[0].StringValue != nil && b.Values[0].StringValue != nil {
		return *a.Values[0].StringValue < *b.Values[0].StringValue
	}

	return false
}

func (self *TopOrBottomAggregator) comparePoint(a, b *protocol.Point, greater bool) bool {
	result := comparePointValue(a, b)

	if !greater {
		return !result
	} else {
		return result
	}
}

func (self *TopOrBottomAggregator) AggregatePoint(state interface{}, p *protocol.Point) (interface{}, error) {
	var s *TopOrBottomAggregatorState
	fieldValue, err := GetValue(self.value, self.columns, p)
	if err != nil {
		return nil, err
	}

	if state == nil {
		s = &TopOrBottomAggregatorState{values: protocol.PointsCollection{}}
	} else {
		s = state.(*TopOrBottomAggregatorState)
	}

	sorter := func(values protocol.PointsCollection, isTop bool) {
		if isTop {
			sort.Sort(ByPointColumnDesc{values})
		} else {
			sort.Sort(ByPointColumnAsc{values})
		}
	}

	asFieldValue := func(p *protocol.Point) *protocol.FieldValue {
		pp := &protocol.FieldValue{}
		if fieldValue.Int64Value != nil {
			pp.Int64Value = fieldValue.Int64Value
		} else if fieldValue.DoubleValue != nil {
			pp.DoubleValue = fieldValue.DoubleValue
		} else if fieldValue.StringValue != nil {
			pp.StringValue = fieldValue.StringValue
		}
		return pp
	}
	if s.counter < self.limit {
		newvalue := &protocol.Point{
			Values: []*protocol.FieldValue{asFieldValue(p)},
		}
		s.values = append(s.values, newvalue)
		sorter(s.values, self.isTop)
		s.counter++
	} else {
		newvalue := &protocol.Point{
			Values: []*protocol.FieldValue{asFieldValue(p)},
		}
		if self.comparePoint(s.values[s.counter-1], newvalue, self.isTop) {
			s.values = append(s.values, p)
			sorter(s.values, self.isTop)
			s.values = s.values[0:self.limit]
		}
	}

	return s, nil
}

func (self *TopOrBottomAggregator) ColumnNames() []string {
	if self.alias != "" {
		return []string{self.alias}
	} else {
		return []string{self.name}
	}
}

func (self *TopOrBottomAggregator) GetValues(state interface{}) [][]*protocol.FieldValue {
	returnValues := [][]*protocol.FieldValue{}
	if state == nil {
		returnValues = append(returnValues, []*protocol.FieldValue{self.defaultValue})
	} else {
		s := state.(*TopOrBottomAggregatorState)
		for _, values := range s.values {
			returnValues = append(returnValues, []*protocol.FieldValue{values.Values[0]})
		}
	}

	return returnValues
}

func (self *TopOrBottomAggregator) InitializeFieldsMetadata(series *protocol.Series) error {
	self.columns = series.Fields
	return nil
}

func NewTopOrBottomAggregator(name string, v *parser.Value, isTop bool, defaultValue *parser.Value) (Aggregator, error) {
	if len(v.Elems) != 2 {
		return nil, common.NewQueryError(common.WrongNumberOfArguments, fmt.Sprintf("function %s() requires at exactly 2 arguments", name))
	}

	if v.Elems[1].Type != parser.ValueInt {
		return nil, common.NewQueryError(common.InvalidArgument, fmt.Sprintf("function %s() second parameter expect int", name))
	}

	wrappedDefaultValue, err := wrapDefaultValue(defaultValue)
	if err != nil {
		return nil, err
	}

	limit, err := strconv.ParseInt(v.Elems[1].Name, 10, 64)
	if err != nil {
		return nil, err
	}

	return &TopOrBottomAggregator{
		AbstractAggregator: AbstractAggregator{
			value: v.Elems[0],
		},
		name:         name,
		isTop:        isTop,
		defaultValue: wrappedDefaultValue,
		alias:        v.Alias,
		limit:        limit}, nil
}

func NewTopAggregator(_ *parser.SelectQuery, value *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	return NewTopOrBottomAggregator("top", value, true, defaultValue)
}

func NewBottomAggregator(_ *parser.SelectQuery, value *parser.Value, defaultValue *parser.Value) (Aggregator, error) {
	return NewTopOrBottomAggregator("bottom", value, false, defaultValue)
}
