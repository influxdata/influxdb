package engine

import (
	"parser"
	"protocol"
	"time"
)

type Aggregator interface {
	AggregatePoint(series string, group interface{}, p *protocol.Point) error
	GetValue(series string, group interface{}) *protocol.FieldValue
	ColumnName() string
	ColumnType() protocol.FieldDefinition_Type
}

type AggregatorIniitializer func(*parser.Query) (Aggregator, error)

var registeredAggregators = make(map[string]AggregatorIniitializer)

func init() {
	registeredAggregators["count"] = NewCountAggregator
	registeredAggregators["__timestamp_aggregator"] = NewTimestampAggregator
}

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

func NewCountAggregator(query *parser.Query) (Aggregator, error) {
	return &CountAggregator{make(map[string]map[interface{}]int32)}, nil
}

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

func NewTimestampAggregator(query *parser.Query) (Aggregator, error) {
	duration, err := query.GetGroupByClause().GetGroupByTime()
	if err != nil {
		return nil, err
	}

	return &TimestampAggregator{
		timestamps: make(map[string]map[interface{}]int64),
		duration:   duration,
	}, nil
}
