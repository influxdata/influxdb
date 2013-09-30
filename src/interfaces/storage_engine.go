package interfaces

import (
	"time"
)

type FieldType int

const (
	StringType FieldType = iota
	IntType
	FloatType
)

type Field struct {
	Name  string
	Value interface{}
	Type  FieldType
}

type Point struct {
	Fields []*Field
}

type Timeseries struct {
	Name   string
	Points []*Point
}

type WriteRequest struct {
	Timeseries []*Timeseries
}

type Operation int

const (
	EqualOperation Operation = iota
	NotEqualOperation
	GreaterThanOperation
	GreaterThanOrEqualOperation
	LessThanOperation
	LessThanOrEqualOperation
)

type Condition interface {
	IsCondition()
}

type CombineOperation int

// definition of combining operators with order of precedence
const (
	NotOperation CombineOperation = iota // First has to be nil
	AndOperation
	OrOperation
)

type CombiningCondition struct {
	First     *Condition
	CombineOp CombineOperation
	Second    *Condition
}

type ComparisonCondition struct {
	FieldName string
	Op        Operation
	Value     interface{}
}

// TODO: applying functions and joining time series
type ReadRequest struct {
	Timeseries   string
	IsRegex      bool // is the timeseries name a regex?
	StartTime    time.Time
	EndTime      time.Time
	IsContinuous bool
	Conditions   []*Condition
}

type StorageEngineProcessingI interface {
	WritePoints(request *WriteRequest) error
	ReadPoints(request *ReadRequest, yield func(pts []*Point) error) error
}

type StorageEngineConsensusI interface {
	// TODO: figure out the requirements of this interface. Probably the following
	// 1. Transfer part(s) of the ring to other node(s)
	// 2. Give up ownership of part(s) of the ring
	// 3. Take ownership of part(s) of the ring
}
