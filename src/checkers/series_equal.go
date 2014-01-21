package checkers

import (
	"fmt"
	. "launchpad.net/gocheck"
	"protocol"
	"reflect"
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

	areEqual := checkEquality(p1, p2)
	if areEqual {
		return true, ""
	}

	return false, "series aren't equal"
}

func checkEquality(s1, s2 []*protocol.Series) bool {
	if len(s1) != len(s2) {
		return false
	}

	for idx, series := range s1 {
		if !checkSeriesEquality(series, s2[idx]) {
			return false
		}
	}

	return true
}

// return true if the two series are equal, ignoring the order of
// points with the same timestamp. Two points can have the same
// sequence number if the user issue a query with group by time(1h)
// and some_column.
func checkSeriesEquality(s1, s2 *protocol.Series) bool {
	if len(s1.Points) != len(s2.Points) {
		return false
	}

	firstSeriesPoints := map[int64]map[*protocol.Point]bool{}
	secondSeriesPoints := map[int64]map[*protocol.Point]bool{}

	for idx, point := range s1.Points {
		secondPoint := s2.Points[idx]
		// make sure that the timestamp of the points are equal
		if *point.Timestamp != *secondPoint.Timestamp {
			return false
		}

		// for each timestamp update the set of points
		for _, m := range []map[int64]map[*protocol.Point]bool{firstSeriesPoints, secondSeriesPoints} {
			pointsMap := m[*point.Timestamp]
			if pointsMap == nil {
				pointsMap = map[*protocol.Point]bool{}
				m[*point.Timestamp] = pointsMap
			}
			pointsMap[point] = true
		}
	}

	return reflect.DeepEqual(firstSeriesPoints, secondSeriesPoints)
}
