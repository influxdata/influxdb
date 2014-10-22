package common

import (
	"encoding/json"
	"fmt"

	log "code.google.com/p/log4go"
	"github.com/influxdb/influxdb/protocol"
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
			timestamp := int64(0)
			if t := row.Timestamp; t != nil {
				timestamp = *row.GetTimestampInMicroseconds()
				switch precision {
				case SecondPrecision:
					timestamp /= 1000
					fallthrough
				case MillisecondPrecision:
					timestamp /= 1000
				}
			}

			rowValues := []interface{}{timestamp}
			s := uint64(0)
			if includeSequenceNumber {
				if row.SequenceNumber != nil {
					s = row.GetSequenceNumber()
				}
				rowValues = append(rowValues, s)
			}
			for _, value := range row.Values {
				if value == nil {
					rowValues = append(rowValues, nil)
					continue
				}
				v, ok := value.GetValue()
				if !ok {
					rowValues = append(rowValues, nil)
					log.Warn("Infinite or NaN value encountered")
					continue
				}
				rowValues = append(rowValues, v)
			}
			points = append(points, rowValues)
		}

		serializedSeries = append(serializedSeries, &SerializedSeries{
			Name:    *series.Name,
			Columns: columns,
			Points:  points,
		})
	}
	SortSerializedSeries(serializedSeries)
	return serializedSeries
}
