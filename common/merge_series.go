package common

import (
	"reflect"

	"github.com/influxdb/influxdb/protocol"
)

func pointMaps(s *protocol.Series) (result []map[string]*protocol.FieldValue) {
	for _, p := range s.Points {
		pointMap := map[string]*protocol.FieldValue{}
		for idx, value := range p.Values {
			pointMap[s.Fields[idx]] = value
		}
		result = append(result, pointMap)
	}
	return
}

// merges two time series making sure that the resulting series has
// the union of the two series columns and the values set
// properly. will panic if the two series don't have the same name
func MergeSeries(s1, s2 *protocol.Series) *protocol.Series {
	if s1.GetName() != s2.GetName() {
		panic("the two series don't have the same name")
	}

	// if the two series have the same columns and in the same order
	// append the points and return.
	if reflect.DeepEqual(s1.Fields, s2.Fields) {
		s1.Points = append(s1.Points, s2.Points...)
		return s1
	}

	columns := map[string]struct{}{}

	for _, cs := range [][]string{s1.Fields, s2.Fields} {
		for _, c := range cs {
			columns[c] = struct{}{}
		}
	}

	points := append(pointMaps(s1), pointMaps(s2)...)

	fieldsSlice := make([]string, 0, len(columns))
	for c := range columns {
		fieldsSlice = append(fieldsSlice, c)
	}

	resultPoints := make([]*protocol.Point, 0, len(points))
	for idx, point := range points {
		resultPoint := &protocol.Point{}
		for _, field := range fieldsSlice {
			value := point[field]
			if value == nil {
				value = &protocol.FieldValue{
					IsNull: &TRUE,
				}
			}
			resultPoint.Values = append(resultPoint.Values, value)
			if idx < len(s1.Points) {
				resultPoint.Timestamp = s1.Points[idx].Timestamp
				resultPoint.SequenceNumber = s1.Points[idx].SequenceNumber
			} else {
				resultPoint.Timestamp = s2.Points[idx-len(s1.Points)].Timestamp
				resultPoint.SequenceNumber = s2.Points[idx-len(s1.Points)].SequenceNumber
			}
		}
		resultPoints = append(resultPoints, resultPoint)
	}

	// otherwise, merge the columns
	result := &protocol.Series{
		Name:   s1.Name,
		Fields: fieldsSlice,
		Points: resultPoints,
	}

	return result
}
