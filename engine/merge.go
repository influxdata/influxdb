package engine

import (
	"github.com/influxdb/influxdb/parser"
	"github.com/influxdb/influxdb/protocol"
)

func getJoinYield(query *parser.SelectQuery, yield func(*protocol.Series) error) func(*protocol.Series) error {
	var lastPoint1 *protocol.Point
	var lastFields1 []string
	var lastPoint2 *protocol.Point
	var lastFields2 []string

	table1 := query.GetFromClause().Names[0].GetAlias()
	table2 := query.GetFromClause().Names[1].GetAlias()
	name := table1 + "_join_" + table2

	return mergeYield(table1, table2, false, query.Ascending, func(s *protocol.Series) error {
		if *s.Name == table1 {
			lastPoint1 = s.Points[len(s.Points)-1]
			if lastFields1 == nil {
				for _, f := range s.Fields {
					lastFields1 = append(lastFields1, table1+"."+f)
				}
			}
		}

		if *s.Name == table2 {
			lastPoint2 = s.Points[len(s.Points)-1]
			if lastFields2 == nil {
				for _, f := range s.Fields {
					lastFields2 = append(lastFields2, table2+"."+f)
				}
			}
		}

		if lastPoint1 == nil || lastPoint2 == nil {
			return nil
		}

		newSeries := &protocol.Series{
			Name:   &name,
			Fields: append(lastFields1, lastFields2...),
			Points: []*protocol.Point{
				{
					Values:    append(lastPoint1.Values, lastPoint2.Values...),
					Timestamp: lastPoint2.Timestamp,
				},
			},
		}

		lastPoint1 = nil
		lastPoint2 = nil

		filteredSeries, _ := Filter(query, newSeries)
		if len(filteredSeries.Points) > 0 {
			return yield(newSeries)
		}
		return nil
	})
}

func getMergeYield(table1, table2 string, ascending bool, yield func(*protocol.Series) error) func(*protocol.Series) error {
	name := table1 + "_merge_" + table2

	return mergeYield(table1, table2, true, ascending, func(s *protocol.Series) error {
		oldName := s.Name
		s.Name = &name
		s.Fields = append(s.Fields, "_orig_series")
		for _, p := range s.Points {
			p.Values = append(p.Values, &protocol.FieldValue{StringValue: oldName})
		}
		return yield(s)
	})
}

type seriesMergeState struct {
	name   string
	series []*protocol.Series
	fields []string
	done   bool
}

func (self *seriesMergeState) hasPoints() bool {
	return len(self.series) > 0 && len(self.series[0].Points) > 0
}

func isEarlier(first *seriesMergeState, other *seriesMergeState) bool {
	return *first.series[0].Points[0].Timestamp < *other.series[0].Points[0].Timestamp
}

func isLater(first *seriesMergeState, other *seriesMergeState) bool {
	return *first.series[0].Points[0].Timestamp > *other.series[0].Points[0].Timestamp
}

func (self *seriesMergeState) flush(state *mergeState, yield func(*protocol.Series) error) error {
	for _, s := range self.series {
		s := state.mergeColumnsInSeries(s)
		err := yield(s)
		if err != nil {
			return err
		}
	}
	self.series = nil
	return nil
}

// update the state, the points belong to this seriesMergeState (i.e. the name of the timeseries matches)
func (self *seriesMergeState) updateState(p *protocol.Series) {
	if *p.Name != self.name {
		return
	}

	// setup the fields
	if self.fields == nil {
		self.fields = p.Fields
	}

	// data for current table is exhausted
	if len(p.Points) == 0 {
		self.done = true
	} else {
		self.series = append(self.series, p)
	}
}

type mergeState struct {
	leftGoFirst  func(_, _ *seriesMergeState) bool
	fields       map[string]int
	left         *seriesMergeState
	right        *seriesMergeState
	mergeColumns bool
}

// set the fields of the other time series to null making sure that
// the order of the null values match the order of the field
// definitions, i.e. left timeseries first followed by values from the
// right timeseries
func (self *mergeState) mergeColumnsInSeries(s *protocol.Series) *protocol.Series {
	if !self.mergeColumns {
		return s
	}

	newSeries := &protocol.Series{
		Name:   s.Name,
		Fields: self.getFields(),
		Points: make([]*protocol.Point, len(s.Points)),
	}

	for idx, p := range s.Points {
		newPoint := &protocol.Point{
			Timestamp:      p.Timestamp,
			SequenceNumber: p.SequenceNumber,
		}
		newValues := make([]*protocol.FieldValue, len(self.fields))
		oldValues := p.Values
		for idx, f := range s.Fields {
			newIdx := self.fields[f]
			newValues[newIdx] = oldValues[idx]
		}
		newPoint.Values = newValues
		newSeries.Points[idx] = newPoint
	}
	return newSeries
}

// set the fields of the other time series to null making sure that
// the order of the null values match the order of the field
// definitions, i.e. left timeseries first followed by values from the
// right timeseries
func (self *mergeState) getFields() []string {
	fields := make([]string, len(self.fields))
	for f, i := range self.fields {
		fields[i] = f
	}
	return fields
}

func (self *mergeState) yieldNextPoints(yield func(*protocol.Series) error) error {
	// see which point should be returned next and remove it from the
	// series
	for self.left.hasPoints() && self.right.hasPoints() {
		// state is the state of the series from which the next point
		// will be fetched
		next := self.right
		if self.leftGoFirst(self.left, self.right) {
			next = self.left
		}

		s := next.removeAndGetFirstPoint()
		err := yield(self.mergeColumnsInSeries(s))
		if err != nil {
			return err
		}
	}
	return nil
}

// if `other` state is done (i.e. we'll receive no more points for its
// timeseries) then we know that we won't get any points that are
// older than what's in `self` so we can safely flush all `self`
// points.
func (self *mergeState) flushIfNecessary(yield func(*protocol.Series) error) error {
	if self.left.done && len(self.left.series) == 0 {
		self.right.flush(self, yield)
	}
	if self.right.done && len(self.right.series) == 0 {
		self.left.flush(self, yield)
	}
	return nil
}

func (self *mergeState) updateState(p *protocol.Series) {
	self.left.updateState(p)
	self.right.updateState(p)

	// create the fields map
	if self.fields == nil && self.left.fields != nil && self.right.fields != nil {
		self.fields = make(map[string]int)

		i := 0
		for _, s := range []*seriesMergeState{self.left, self.right} {
			for _, f := range s.fields {
				if _, ok := self.fields[f]; ok {
					continue
				}
				self.fields[f] = i
				i++
			}
		}
	}
}

func (self *seriesMergeState) removeAndGetFirstPoint() *protocol.Series {
	s := &protocol.Series{
		Name:   self.series[0].Name,
		Fields: self.series[0].Fields,
		Points: []*protocol.Point{self.series[0].Points[0]},
	}
	// get rid of that point, or get rid of the entire series
	// if this is the last point
	if len(self.series[0].Points) == 1 {
		self.series = self.series[1:]
	} else {
		self.series[0].Points = self.series[0].Points[1:]
	}
	return s
}

// returns a yield function that will sort points from table1 and
// table2 no matter what the order in which they are received.
func mergeYield(table1, table2 string, mergeColumns bool, ascending bool, yield func(*protocol.Series) error) func(*protocol.Series) error {
	state1 := &seriesMergeState{
		name: table1,
	}
	state2 := &seriesMergeState{
		name: table2,
	}

	whoGoFirst := isEarlier
	if !ascending {
		whoGoFirst = isLater
	}

	state := &mergeState{
		left:         state1,
		right:        state2,
		leftGoFirst:  whoGoFirst,
		mergeColumns: mergeColumns,
	}

	return func(p *protocol.Series) error {

		state.updateState(p)

		if err := state.flushIfNecessary(yield); err != nil {
			return err
		}

		return state.yieldNextPoints(yield)
	}
}
