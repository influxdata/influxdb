package engine

import (
	"fmt"
	"protocol"
)

func getJoinYield(table1, table2 string, yield func(*protocol.Series) error) func(*protocol.Series) error {
	var lastPoint1 *protocol.Point
	var lastPoint2 *protocol.Point
	name := table1 + "_join_" + table2

	return mergeYield(table1, table2, func(s *protocol.Series) error {
		if *s.Name == table1 {
			lastPoint1 = s.Points[len(s.Points)-1]
			fmt.Printf("values 1: %#v\n", lastPoint1.Values)
		}

		if *s.Name == table2 {
			lastPoint2 = s.Points[len(s.Points)-1]
			fmt.Printf("values 2: %#v\n", lastPoint2.Values)
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

type MergeState struct {
	name       string
	series     []*protocol.Series
	fields     []string
	nullValues []*protocol.FieldValue
	isLeft     bool
	done       bool
}

func (self *MergeState) hasPoints() bool {
	return len(self.series) > 0 && len(self.series[0].Points) > 0
}

func (self *MergeState) isEarlier(other *MergeState) bool {
	return *self.series[0].Points[0].Timestamp < *other.series[0].Points[0].Timestamp
}

func (self *MergeState) mergeValues(other *MergeState, p *protocol.Point) {
	if self.isLeft {
		p.Values = append(p.Values, other.nullValues...)
		return
	}
	p.Values = append(other.nullValues, p.Values...)
}

func (self *MergeState) flushIfOtherStateIsEmpty(other *MergeState, fields []string, yield func(*protocol.Series) error) error {
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

// update the state, the points belong to this MergeState (i.e. the name of the timeseries matches)
func (self *MergeState) updateState(p *protocol.Series) {
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

// returns a yield function that will sort points from table1 and
// table2 no matter what the order in which they are received.
//
// FIXME: This won't work for queries that are executed in descending order.
func mergeYield(table1, table2 string, yield func(*protocol.Series) error) func(*protocol.Series) error {
	state1 := &MergeState{
		name:   table1,
		isLeft: true,
	}
	state2 := &MergeState{
		name:   table2,
		isLeft: false,
	}

	return func(p *protocol.Series) error {
		state1.updateState(p)
		state2.updateState(p)

		mergedFields := append(state1.fields, state2.fields...)

		// if one of the states (let's call it s1) is done (i.e. we'll
		// receive no more points for that series) then we know that we
		// won't get any points for s1 that are older than what's in s2 so
		// we can safely flush all s2's points.
		if err := state1.flushIfOtherStateIsEmpty(state2, mergedFields, yield); err != nil {
			return err
		}
		if err := state2.flushIfOtherStateIsEmpty(state1, mergedFields, yield); err != nil {
			return err
		}

		for state1.hasPoints() && state2.hasPoints() {
			var points []*protocol.Point

			state := state1
			otherState := state2
			if state2.isEarlier(state1) {
				state = state2
				otherState = state1
			}
			points = []*protocol.Point{state.series[0].Points[0]}
			for _, p := range points {
				state.mergeValues(otherState, p)
			}
			// get rid of that point, or get rid of the entire series
			// if this is the last point
			if len(state.series[0].Points) == 1 {
				state.series = state.series[1:]
			} else {
				state.series[0].Points = state.series[0].Points[1:]
			}
			err := yield(&protocol.Series{
				Name:   &state.name,
				Fields: mergedFields,
				Points: points,
			})
			if err != nil {
				return err
			}
		}
		return nil
	}
}
