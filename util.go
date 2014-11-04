package influxdb

import (
	"encoding/json"
	"fmt"

	"github.com/influxdb/influxdb/protocol"
)

type TimePrecision int

const (
	MicrosecondPrecision TimePrecision = iota
	MillisecondPrecision
	SecondPrecision
)

type serializedSeries struct {
	Name    string          `json:"name"`
	Columns []string        `json:"columns"`
	Points  [][]interface{} `json:"points"`
}

//func SortSerializedSeries(s []*serializedSeries) {
//	sort.Sort(BySerializedSeriesNameAsc{s})
//}

func (s serializedSeries) Series(precision TimePrecision) (*protocol.Series, error) {
	points := make([]*protocol.Point, 0, len(s.Points))
	if hasDuplicates(s.Columns) {
		return nil, fmt.Errorf("Cannot have duplicate field names")
	}

	for _, point := range s.Points {
		if len(point) != len(s.Columns) {
			return nil, fmt.Errorf("invalid payload")
		}

		values := make([]*protocol.FieldValue, 0, len(point))
		var timestamp *int64
		var sequence *uint64

		for idx, field := range s.Columns {
			value := point[idx]
			if field == "time" {
				switch x := value.(type) {
				case json.Number:
					f, err := x.Float64()
					if err != nil {
						return nil, err
					}
					_timestamp := int64(f)
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
				switch x := value.(type) {
				case json.Number:
					f, err := x.Float64()
					if err != nil {
						return nil, err
					}
					_sequenceNumber := uint64(f)
					sequence = &_sequenceNumber
					continue
				default:
					return nil, fmt.Errorf("sequence_number field must be float but is %T (%v)", value, value)
				}
			}

			switch v := value.(type) {
			case string:
				values = append(values, &protocol.FieldValue{StringValue: &v})
			case json.Number:
				i, err := v.Int64()
				if err == nil {
					values = append(values, &protocol.FieldValue{Int64Value: &i})
					break
				}
				f, err := v.Float64()
				if err != nil {
					return nil, err
				}
				values = append(values, &protocol.FieldValue{DoubleValue: &f})
			case bool:
				values = append(values, &protocol.FieldValue{BoolValue: &v})
			case nil:
				trueValue := true
				values = append(values, &protocol.FieldValue{IsNull: &trueValue})
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

	fields := removeTimestampFieldDefinition(s.Columns)

	series := &protocol.Series{
		Name:   protocol.String(s.Name),
		Fields: fields,
		Points: points,
	}
	return series, nil
}

func hasDuplicates(ss []string) bool {
	m := make(map[string]struct{}, len(ss))
	for _, s := range ss {
		if _, ok := m[s]; ok {
			return true
		}
		m[s] = struct{}{}
	}
	return false
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
