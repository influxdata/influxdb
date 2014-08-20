package engine

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/influxdb/influxdb/common"
	"github.com/influxdb/influxdb/parser"
	"github.com/influxdb/influxdb/protocol"
)

type AggregatorEngine struct {
	// query information
	query            *parser.SelectQuery
	isAggregateQuery bool
	fields           []string
	where            *parser.WhereCondition
	fillWithZero     bool

	// was start time set in the query, e.g. time > now() - 1d
	startTimeSpecified bool
	startTime          int64
	endTime            int64

	// output fields
	next    Processor
	limiter *Limiter

	// variables for aggregate queries
	aggregators  []Aggregator
	elems        []*parser.Value // group by columns other than time()
	duration     *time.Duration  // the time by duration if any
	seriesStates map[string]*SeriesState
}

func (self *AggregatorEngine) Name() string {
	return "Aggregator Engine"
}

func (self *AggregatorEngine) yieldToNext(seriesIncoming *protocol.Series) (bool, error) {
	if ok, err := self.next.Yield(seriesIncoming); err != nil || !ok {
		return ok, err
	}
	if self.limiter.hitLimit(seriesIncoming.GetName()) {
		return false, nil
	}
	return true, nil
}

func (self *AggregatorEngine) Close() error {
	if self.isAggregateQuery {
		if _, err := self.runAggregates(); err != nil {
			return err
		}
	}

	return self.next.Close()
}

func (self *AggregatorEngine) getTimestampFromPoint(point *protocol.Point) int64 {
	return self.getTimestampBucket(uint64(*point.GetTimestampInMicroseconds()))
}

func (self *AggregatorEngine) getTimestampBucket(timestampMicroseconds uint64) int64 {
	timestampMicroseconds *= 1000 // convert to nanoseconds
	multiplier := uint64(*self.duration)
	return int64(timestampMicroseconds / multiplier * multiplier / 1000)
}

func (self *AggregatorEngine) Yield(s *protocol.Series) (bool, error) {
	if len(s.Points) == 0 {
		return true, nil
	}

	return self.aggregateValuesForSeries(s)
}

func (self *AggregatorEngine) initializeFields() {
	for _, aggregator := range self.aggregators {
		columnNames := aggregator.ColumnNames()
		self.fields = append(self.fields, columnNames...)
	}

	if self.elems == nil {
		return
	}

	for _, value := range self.elems {
		tempName := value.Name
		self.fields = append(self.fields, tempName)
	}
}

var _count = 0

func (self *AggregatorEngine) getSeriesState(name string) *SeriesState {
	state := self.seriesStates[name]
	if state == nil {
		levels := len(self.elems)
		if self.duration != nil && self.fillWithZero {
			levels++
		}

		state = &SeriesState{
			started:       false,
			trie:          NewTrie(levels, len(self.aggregators)),
			lastTimestamp: 0,
			pointsRange:   &PointRange{math.MaxInt64, math.MinInt64},
		}
		self.seriesStates[name] = state
	}
	return state
}

// We have three types of queries:
//   1. time() without fill
//   2. time() with fill
//   3. no time()
//
// For (1) we flush as soon as a new bucket start, the prefix tree
// keeps track of the other group by columns without the time
// bucket. We reset the trie once the series is yielded. For (2), we
// keep track of all group by columns with time being the last level
// in the prefix tree. At the end of the query we step through [start
// time, end time] in self.duration steps and get the state from the
// prefix tree, using default values for groups without state in the
// prefix tree. For the last case we keep the groups in the prefix
// tree and on close() we loop through the groups and flush their
// values with a timestamp equal to now()
func (self *AggregatorEngine) aggregateValuesForSeries(series *protocol.Series) (bool, error) {
	for _, aggregator := range self.aggregators {
		if err := aggregator.InitializeFieldsMetadata(series); err != nil {
			return false, err
		}
	}

	seriesState := self.getSeriesState(series.GetName())
	currentRange := seriesState.pointsRange

	includeTimestampInGroup := self.duration != nil && self.fillWithZero
	var group []*protocol.FieldValue
	if !includeTimestampInGroup {
		group = make([]*protocol.FieldValue, len(self.elems))
	} else {
		group = make([]*protocol.FieldValue, len(self.elems)+1)
	}

	for _, point := range series.Points {
		currentRange.UpdateRange(point)

		// this is a groupby with time() and no fill, flush as soon as we
		// start a new bucket
		if self.duration != nil && !self.fillWithZero {
			timestamp := self.getTimestampFromPoint(point)
			// this is the timestamp aggregator
			if seriesState.started && seriesState.lastTimestamp != timestamp {
				self.runAggregatesForTable(series.GetName())
			}
			seriesState.lastTimestamp = timestamp
			seriesState.started = true
		}

		// get the group this point belongs to
		for idx, elem := range self.elems {
			// TODO: create an index from fieldname to index
			value, err := GetValue(elem, series.Fields, point)
			if err != nil {
				return false, err
			}
			group[idx] = value
		}

		// if this is a fill() query, add the timestamp at the end
		if includeTimestampInGroup {
			timestamp := self.getTimestampFromPoint(point)
			group[len(self.elems)] = &protocol.FieldValue{Int64Value: protocol.Int64(timestamp)}
		}

		// update the state of the given group
		node := seriesState.trie.GetNode(group)
		var err error
		for idx, aggregator := range self.aggregators {
			node.states[idx], err = aggregator.AggregatePoint(node.states[idx], point)
			if err != nil {
				return false, err
			}
		}
	}

	return true, nil
}

func (self *AggregatorEngine) runAggregates() (bool, error) {
	for t := range self.seriesStates {
		if ok, err := self.runAggregatesForTable(t); !ok || err != nil {
			return ok, err
		}
	}
	return true, nil
}

func (self *AggregatorEngine) calculateSummariesForTable(table string) {
	trie := self.getSeriesState(table).trie
	err := trie.Traverse(func(group []*protocol.FieldValue, node *Node) error {
		for idx, aggregator := range self.aggregators {
			aggregator.CalculateSummaries(node.states[idx])
		}
		return nil
	})
	if err != nil {
		panic("Error while calculating summaries")
	}
}

func (self *AggregatorEngine) runAggregatesForTable(table string) (bool, error) {
	// TODO: if this is a fill query, step through [start,end] in duration
	// steps and flush the groups for the given bucket

	self.calculateSummariesForTable(table)

	state := self.getSeriesState(table)
	trie := state.trie
	points := make([]*protocol.Point, 0, trie.CountLeafNodes())
	f := func(group []*protocol.FieldValue, node *Node) error {
		points = append(points, self.getValuesForGroup(table, group, node)...)
		return nil
	}

	var err error
	if self.duration != nil && self.fillWithZero {
		timestampRange := state.pointsRange
		if self.startTimeSpecified {
			timestampRange = &PointRange{startTime: self.startTime, endTime: self.endTime}
		}

		// TODO: DRY this
		if self.query.Ascending {
			bucket := self.getTimestampBucket(uint64(timestampRange.startTime))
			for bucket <= timestampRange.endTime {
				timestamp := &protocol.FieldValue{Int64Value: protocol.Int64(bucket)}
				defaultChildNode := &Node{states: make([]interface{}, len(self.aggregators))}
				err = trie.TraverseLevel(len(self.elems), func(v []*protocol.FieldValue, node *Node) error {
					childNode := node.GetChildNode(timestamp)
					if childNode == nil {
						childNode = defaultChildNode
					}
					return f(append(v, timestamp), childNode)
				})
				bucket += self.duration.Nanoseconds() / 1000
			}
		} else {
			bucket := self.getTimestampBucket(uint64(timestampRange.endTime))
			for {
				timestamp := &protocol.FieldValue{Int64Value: protocol.Int64(bucket)}
				defaultChildNode := &Node{states: make([]interface{}, len(self.aggregators))}
				err = trie.TraverseLevel(len(self.elems), func(v []*protocol.FieldValue, node *Node) error {
					childNode := node.GetChildNode(timestamp)
					if childNode == nil {
						childNode = defaultChildNode
					}
					return f(append(v, timestamp), childNode)
				})
				if bucket <= timestampRange.startTime {
					break
				}
				bucket -= self.duration.Nanoseconds() / 1000
			}
		}
	} else {
		err = trie.Traverse(f)
	}
	if err != nil {
		panic(err)
	}
	trie.Clear()
	return self.yieldToNext(&protocol.Series{
		Name:   &table,
		Fields: self.fields,
		Points: points,
	})
}

func (self *AggregatorEngine) getValuesForGroup(table string, group []*protocol.FieldValue, node *Node) []*protocol.Point {

	values := [][][]*protocol.FieldValue{}

	var timestamp int64
	useTimestamp := false
	if self.duration != nil && !self.fillWithZero {
		// if there's a group by time(), then the timestamp is the lastTimestamp
		timestamp = self.getSeriesState(table).lastTimestamp
		useTimestamp = true
	} else if self.duration != nil && self.fillWithZero {
		// if there's no group by time(), but a fill value was specified,
		// the timestamp is the last value in the group
		timestamp = group[len(group)-1].GetInt64Value()
		useTimestamp = true
	}

	for idx, aggregator := range self.aggregators {
		values = append(values, aggregator.GetValues(node.states[idx]))
		node.states[idx] = nil
	}

	// do cross product of all the values
	var _values [][]*protocol.FieldValue
	if len(values) == 1 {
		_values = values[0]
	} else {
		_values = crossProduct(values)
	}

	points := []*protocol.Point{}

	for _, v := range _values {
		/* groupPoints := []*protocol.Point{} */
		point := &protocol.Point{
			Values: v,
		}

		if useTimestamp {
			point.SetTimestampInMicroseconds(timestamp)
		} else {
			point.SetTimestampInMicroseconds(0)
		}

		// FIXME: this should be looking at the fields slice not the group by clause
		// FIXME: we should check whether the selected columns are in the group by clause
		for idx := range self.elems {
			point.Values = append(point.Values, group[idx])
		}

		points = append(points, point)
	}
	return points
}

func (self *AggregatorEngine) init() error {
	duration, err := self.query.GetGroupByClause().GetGroupByTime()
	if err != nil {
		return err
	}

	self.isAggregateQuery = true
	self.duration = duration
	self.aggregators = []Aggregator{}

	for _, value := range self.query.GetColumnNames() {
		if !value.IsFunctionCall() {
			continue
		}
		lowerCaseName := strings.ToLower(value.Name)
		initializer := registeredAggregators[lowerCaseName]
		if initializer == nil {
			return common.NewQueryError(common.InvalidArgument, fmt.Sprintf("Unknown function %s", value.Name))
		}
		aggregator, err := initializer(self.query, value, self.query.GetGroupByClause().FillValue)
		if err != nil {
			return common.NewQueryError(common.InvalidArgument, fmt.Sprintf("%s", err))
		}
		self.aggregators = append(self.aggregators, aggregator)
	}

	for _, elem := range self.query.GetGroupByClause().Elems {
		if elem.IsFunctionCall() {
			continue
		}
		self.elems = append(self.elems, elem)
	}

	self.fillWithZero = self.query.GetGroupByClause().FillWithZero

	// This is a special case for issue #426. If the start time is
	// specified and there's a group by clause and fill with zero, then
	// we need to fill the entire range from start time to end time
	if self.query.IsStartTimeSpecified() && self.duration != nil && self.fillWithZero {
		self.startTimeSpecified = true
		self.startTime = self.query.GetStartTime().Truncate(*self.duration).UnixNano() / 1000
		self.endTime = self.query.GetEndTime().Truncate(*self.duration).UnixNano() / 1000
	}

	self.initializeFields()
	return nil
}

func NewAggregatorEngine(query *parser.SelectQuery, next Processor) (*AggregatorEngine, error) {
	queryEngine := &AggregatorEngine{
		query:   query,
		where:   query.GetWhereCondition(),
		next:    next,
		limiter: NewLimiter(query.Limit),
		// stats stuff
		duration:     nil,
		seriesStates: make(map[string]*SeriesState),
	}

	return queryEngine, queryEngine.init()
}
