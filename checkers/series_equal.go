package checkers

import (
	"fmt"
	"reflect"

	"github.com/influxdb/influxdb/protocol"
	. "launchpad.net/gocheck"
)

type SeriesEqualsChecker struct {
	*CheckerInfo
}

var SeriesEquals Checker = &SeriesEqualsChecker{
	&CheckerInfo{Name: "SeriesEquals", Params: []string{"obtained", "expected"}},
}

func (checker *SeriesEqualsChecker) Check(params []interface{}, names []string) (result bool, error string) {
	defer func() {
		if v := recover(); v != nil {
			result = false
			error = fmt.Sprint(v)
		}
	}()

	var p1, p2 []*protocol.Series

	switch x := params[0].(type) {
	case []*protocol.Series:
		p1 = x
	default:
		return false, "first parameter isn't a series"
	}

	switch x := params[1].(type) {
	case []*protocol.Series:
		p2 = x
	default:
		return false, "second parameter isn't a series"
	}

	areEqual := CheckEquality(p1, p2)
	if areEqual {
		return true, ""
	}

	return false, "series aren't equal"
}

func CheckEquality(s1, s2 []*protocol.Series) bool {
	if len(s1) != len(s2) {
		return false
	}

	for idx, series := range s1 {
		if !CheckSeriesEquality(series, s2[idx]) {
			return false
		}
	}

	return true
}

type SimplePoint [10]interface{}

// return true if the two series are equal, ignoring the order of
// points with the same timestamp. Two points can have the same
// sequence number if the user issue a query with group by time(1h)
// and some_column.
func CheckSeriesEquality(s1, s2 *protocol.Series) bool {
	if len(s1.Points) != len(s2.Points) {
		return false
	}

	if !reflect.DeepEqual(s1.Fields, s2.Fields) {
		return false
	}

	firstSeriesPoints := map[int64]map[SimplePoint]bool{}
	secondSeriesPoints := map[int64]map[SimplePoint]bool{}

	for idx, point := range s1.Points {
		secondPoint := s2.Points[idx]
		// make sure that the timestamp of the points are equal
		if *point.Timestamp != *secondPoint.Timestamp {
			return false
		}

		firstSimplePoint := SimplePoint{}
		secondSimplePoint := SimplePoint{}
		for fieldIndex := range s1.Fields {
			firstSimplePoint[fieldIndex], _ = point.Values[fieldIndex].GetValue()
			secondSimplePoint[fieldIndex], _ = point.Values[fieldIndex].GetValue()
		}

		pointsMap := firstSeriesPoints[*point.Timestamp]
		if pointsMap == nil {
			pointsMap = map[SimplePoint]bool{}
			firstSeriesPoints[*point.Timestamp] = pointsMap
		}
		pointsMap[firstSimplePoint] = true

		pointsMap = secondSeriesPoints[*point.Timestamp]
		if pointsMap == nil {
			pointsMap = map[SimplePoint]bool{}
			secondSeriesPoints[*point.Timestamp] = pointsMap
		}
		pointsMap[secondSimplePoint] = true
	}

	return reflect.DeepEqual(firstSeriesPoints, secondSeriesPoints)
}
