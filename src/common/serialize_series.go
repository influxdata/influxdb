package common

import (
	"fmt"
	"protocol"
)

var (
	TRUE  = true
	FALSE = false
)

type TimePrecision int

const (
	MicrosecondPrecision TimePrecision = iota
	MillisecondPrecision
	SecondPrecision
)

func init() {
}

func removeField(fields []string, name string) []string {
	index := -1
	for idx, field := range fields {
		if field == name {
			index = idx
			break
		}
	}

	if index == -1 {
		return fields
	}

	return append(fields[:index], fields[index+1:]...)
}

func removeTimestampFieldDefinition(fields []string) []string {
	fields = removeField(fields, "time")
	return removeField(fields, "sequence_number")
}

type ApiSeries interface {
	GetName() string
	GetColumns() []string
	GetPoints() [][]interface{}
}

func ConvertToDataStoreSeries(s ApiSeries, precision TimePrecision) (*protocol.Series, error) {
	points := []*protocol.Point{}
	for _, point := range s.GetPoints() {
		values := []*protocol.FieldValue{}
		var timestamp *int64
		var sequence *uint64

		for idx, field := range s.GetColumns() {
			value := point[idx]
			if field == "time" {
				switch value.(type) {
				case float64:
					_timestamp := int64(value.(float64))
					switch precision {
					case SecondPrecision:
						_timestamp *= 1000
						fallthrough
					case MillisecondPrecision:
						_timestamp *= 1000
					}

					timestamp = &_timestamp
					continue
				default:
					return nil, fmt.Errorf("time field must be float but is %T (%v)", value, value)
				}
			}

			if field == "sequence_number" {
				switch value.(type) {
				case float64:
					_sequenceNumber := uint64(value.(float64))
					sequence = &_sequenceNumber
					continue
				default:
					return nil, fmt.Errorf("sequence_number field must be float but is %T (%v)", value, value)
				}
			}

			switch v := value.(type) {
			case string:
				values = append(values, &protocol.FieldValue{StringValue: &v})
			case float64:
				if i := int64(v); float64(i) == v {
					values = append(values, &protocol.FieldValue{Int64Value: &i})
				} else {
					values = append(values, &protocol.FieldValue{DoubleValue: &v})
				}
			case bool:
				values = append(values, &protocol.FieldValue{BoolValue: &v})
			case nil:
				values = append(values, &protocol.FieldValue{IsNull: &TRUE})
			default:
				// if we reached this line then the dynamic type didn't match
				return nil, fmt.Errorf("Unknown type %T", value)
			}
		}
		points = append(points, &protocol.Point{
			Values:         values,
			Timestamp:      timestamp,
			SequenceNumber: sequence,
		})
	}

	fields := removeTimestampFieldDefinition(s.GetColumns())

	series := &protocol.Series{
		Name:   protocol.String(s.GetName()),
		Fields: fields,
		Points: points,
	}
	return series, nil
}

// takes a slice of protobuf series and convert them to the format
// that the http api expect
func SerializeSeries(memSeries map[string]*protocol.Series, precision TimePrecision) []*SerializedSeries {
	serializedSeries := []*SerializedSeries{}

	for _, series := range memSeries {
		includeSequenceNumber := true
		if len(series.Points) > 0 && series.Points[0].SequenceNumber == nil {
			includeSequenceNumber = false
		}

		columns := []string{"time"}
		if includeSequenceNumber {
			columns = append(columns, "sequence_number")
		}
		for _, field := range series.Fields {
			columns = append(columns, field)
		}

		points := [][]interface{}{}
		for _, row := range series.Points {
			timestamp := *row.GetTimestampInMicroseconds()
			switch precision {
			case SecondPrecision:
				timestamp /= 1000
				fallthrough
			case MillisecondPrecision:
				timestamp /= 1000
			}

			rowValues := []interface{}{timestamp}
			if includeSequenceNumber {
				rowValues = append(rowValues, *row.SequenceNumber)
			}
			for _, value := range row.Values {
				if value == nil {
					rowValues = append(rowValues, nil)
					continue
				}
				rowValues = append(rowValues, value.GetValue())
			}
			points = append(points, rowValues)
		}

		serializedSeries = append(serializedSeries, &SerializedSeries{
			Name:    *series.Name,
			Columns: columns,
			Points:  points,
		})
	}
	return serializedSeries
}
