package engine

import "github.com/influxdb/influxdb/protocol"

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

func (self *seriesMergeState) flush(state *CommonMergeEngine) (bool, error) {
	for _, s := range self.series {
		s := state.mergeColumnsInSeries(s)
		ok, err := state.next.Yield(s)
		if !ok || err != nil {
			return ok, err
		}
	}
	self.series = nil
	return true, nil
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

type CommonMergeEngine struct {
	next         Processor
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
func (self *CommonMergeEngine) mergeColumnsInSeries(s *protocol.Series) *protocol.Series {
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
func (self *CommonMergeEngine) getFields() []string {
	fields := make([]string, len(self.fields))
	for f, i := range self.fields {
		fields[i] = f
	}
	return fields
}

func (self *CommonMergeEngine) yieldNextPoints() (bool, error) {
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
		ok, err := self.next.Yield(self.mergeColumnsInSeries(s))
		if err != nil || !ok {
			return ok, err
		}
	}
	return true, nil
}

// if `other` state is done (i.e. we'll receive no more points for its
// timeseries) then we know that we won't get any points that are
// older than what's in `self` so we can safely flush all `self`
// points.
func (self *CommonMergeEngine) flushIfNecessary() (bool, error) {
	if self.left.done && len(self.left.series) == 0 {
		if ok, err := self.right.flush(self); err != nil || !ok {
			return ok, err
		}
	}
	if self.right.done && len(self.right.series) == 0 {
		if ok, err := self.left.flush(self); err != nil || !ok {
			return ok, err
		}
	}
	return true, nil
}

func (self *CommonMergeEngine) updateState(p *protocol.Series) {
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
func NewCommonMergeEngine(table1, table2 string, mergeColumns bool, ascending bool, next Processor) *CommonMergeEngine {
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

	return &CommonMergeEngine{
		next:         next,
		left:         state1,
		right:        state2,
		leftGoFirst:  whoGoFirst,
		mergeColumns: mergeColumns,
	}
}

func (e *CommonMergeEngine) Close() error {
	e.Yield(&protocol.Series{Name: &e.left.name, Fields: []string{}})
	e.Yield(&protocol.Series{Name: &e.right.name, Fields: []string{}})
	return e.next.Close()
}

func (e *CommonMergeEngine) Name() string {
	return "CommonMergeEngine"
}

func (e *CommonMergeEngine) Yield(s *protocol.Series) (bool, error) {
	e.updateState(s)

	if ok, err := e.flushIfNecessary(); !ok || err != nil {
		return ok, err
	}

	return e.yieldNextPoints()
}
