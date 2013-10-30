package engine

import (
	"protocol"
)

func getJoinYield(table1, table2 string, yield func(*protocol.Series) error) func(*protocol.Series) error {
	var lastPoint1 *protocol.Point
	var lastPoint2 *protocol.Point
	name := table1 + "_join_" + table2

	return mergeYield(table1, table2, func(s *protocol.Series) error {
		if *s.Name == table1 {
			lastPoint1 = s.Points[len(s.Points)-1]
		}

		if *s.Name == table2 {
			lastPoint2 = s.Points[len(s.Points)-1]
		}

		if lastPoint1 == nil || lastPoint2 == nil {
			return nil
		}

		// join the last two points and yield them
		newValues := []*protocol.FieldValue{}
		for idx, value := range lastPoint1.Values {
			if value == nil {
				value = lastPoint2.Values[idx]
			}
			newValues = append(newValues, value)
		}

		newSeries := &protocol.Series{
			Name:   &name,
			Fields: s.Fields,
			Points: []*protocol.Point{
				&protocol.Point{
					Values:         newValues,
					Timestamp:      lastPoint2.Timestamp,
					SequenceNumber: lastPoint2.SequenceNumber,
				},
			},
		}

		lastPoint1 = nil
		lastPoint2 = nil

		return yield(newSeries)
	})
}

func getMergeYield(table1, table2 string, yield func(*protocol.Series) error) func(*protocol.Series) error {
	name := table1 + "_merge_" + table2

	return mergeYield(table1, table2, func(s *protocol.Series) error {
		s.Name = &name
		return yield(s)
	})
}

type mergeState struct {
	name       string
	series     []*protocol.Series
	fields     []string
	nullValues []*protocol.FieldValue
	isLeft     bool
	done       bool
}

func (self *mergeState) hasPoints() bool {
	return len(self.series) > 0 && len(self.series[0].Points) > 0
}

func (self *mergeState) isEarlier(other *mergeState) bool {
	return *self.series[0].Points[0].Timestamp < *other.series[0].Points[0].Timestamp
}

// set the fields of the other time series to null making sure that
// the order of the null values match the order of the field
// definitions, i.e. left timeseries first followed by values from the
// right timeseries
func (self *mergeState) mergeValues(other *mergeState, p *protocol.Point) {
	if self.isLeft {
		p.Values = append(p.Values, other.nullValues...)
		return
	}
	p.Values = append(other.nullValues, p.Values...)
}

// if `other` state is done (i.e. we'll receive no more points for its
// timeseries) then we know that we won't get any points that are
// older than what's in `self` so we can safely flush all `self`
// points.
func (self *mergeState) flushIfOtherStateIsEmpty(other *mergeState, fields []string, yield func(*protocol.Series) error) error {
	if other.done && len(other.series) == 0 {
		for _, s := range self.series {
			for _, p := range s.Points {
				self.mergeValues(other, p)
			}

			err := yield(&protocol.Series{
				Name:   s.Name,
				Fields: fields,
				Points: s.Points,
			})
			if err != nil {
				return err
			}
		}
		self.series = nil
	}
	return nil
}

// update the state, the points belong to this mergeState (i.e. the name of the timeseries matches)
func (self *mergeState) updateState(p *protocol.Series) {
	if *p.Name != self.name {
		return
	}

	// setup the fields
	if self.fields == nil {
		for _, f := range p.Fields {
			self.fields = append(self.fields, self.name+"."+f)
		}
		for i := 0; i < len(p.Fields); i++ {
			self.nullValues = append(self.nullValues, nil)
		}
	}

	// data for current table is exhausted
	if len(p.Points) == 0 {
		self.done = true
	} else {
		self.series = append(self.series, p)
	}
}

func (self *mergeState) removeAndGetFirstPoint() *protocol.Point {
	point := self.series[0].Points[0]
	// get rid of that point, or get rid of the entire series
	// if this is the last point
	if len(self.series[0].Points) == 1 {
		self.series = self.series[1:]
	} else {
		self.series[0].Points = self.series[0].Points[1:]
	}
	return point
}

// returns a yield function that will sort points from table1 and
// table2 no matter what the order in which they are received.
//
// FIXME: This won't work for queries that are executed in descending order.
func mergeYield(table1, table2 string, yield func(*protocol.Series) error) func(*protocol.Series) error {
	state1 := &mergeState{
		name:   table1,
		isLeft: true,
	}
	state2 := &mergeState{
		name:   table2,
		isLeft: false,
	}

	return func(p *protocol.Series) error {
		state1.updateState(p)
		state2.updateState(p)

		mergedFields := append(state1.fields, state2.fields...)

		if err := state1.flushIfOtherStateIsEmpty(state2, mergedFields, yield); err != nil {
			return err
		}
		if err := state2.flushIfOtherStateIsEmpty(state1, mergedFields, yield); err != nil {
			return err
		}

		// see which point should be returned next and remove it from the
		// series
		for state1.hasPoints() && state2.hasPoints() {
			// state is the state of the series from which the next point
			// will be fetched
			state := state1
			otherState := state2
			if state2.isEarlier(state1) {
				state = state2
				otherState = state1
			}

			p := state.removeAndGetFirstPoint()
			state.mergeValues(otherState, p)

			err := yield(&protocol.Series{
				Name:   &state.name,
				Fields: mergedFields,
				Points: []*protocol.Point{p},
			})
			if err != nil {
				return err
			}
		}
		return nil
	}
}
