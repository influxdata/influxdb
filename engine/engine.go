package engine

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	log "code.google.com/p/log4go"
	"github.com/influxdb/influxdb/common"
	"github.com/influxdb/influxdb/parser"
	"github.com/influxdb/influxdb/protocol"
)

var (
	TRUE = true
)

type SeriesState struct {
	started       bool
	trie          *Trie
	pointsRange   *PointRange
	lastTimestamp int64
}

type QueryEngine struct {
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
	responseChan   chan *protocol.Response
	limiter        *Limiter
	seriesToPoints map[string]*protocol.Series
	yield          func(*protocol.Series) error
	aggregateYield func(*protocol.Series) error

	// variables for aggregate queries
	aggregators  []Aggregator
	elems        []*parser.Value // group by columns other than time()
	duration     *time.Duration  // the time by duration if any
	seriesStates map[string]*SeriesState

	// query statistics
	explain       bool
	runStartTime  float64
	runEndTime    float64
	pointsRead    int64
	pointsWritten int64
	shardId       int
	shardLocal    bool
}

var (
	endStreamResponse    = protocol.Response_END_STREAM
	explainQueryResponse = protocol.Response_EXPLAIN_QUERY
)

const (
	POINT_BATCH_SIZE = 64
)

// distribute query and possibly do the merge/join before yielding the points
func (self *QueryEngine) distributeQuery(query *parser.SelectQuery, yield func(*protocol.Series) error) error {
	// see if this is a merge query
	fromClause := query.GetFromClause()
	if fromClause.Type == parser.FromClauseMerge {
		yield = getMergeYield(fromClause.Names[0].Name.Name, fromClause.Names[1].Name.Name, query.Ascending, yield)
	}

	if fromClause.Type == parser.FromClauseInnerJoin {
		yield = getJoinYield(query, yield)
	}

	self.yield = yield
	return nil
}

func NewQueryEngine(query *parser.SelectQuery, responseChan chan *protocol.Response) (*QueryEngine, error) {
	limit := query.Limit

	queryEngine := &QueryEngine{
		query:          query,
		where:          query.GetWhereCondition(),
		limiter:        NewLimiter(limit),
		responseChan:   responseChan,
		seriesToPoints: make(map[string]*protocol.Series),
		// stats stuff
		explain:       query.IsExplainQuery(),
		runStartTime:  0,
		runEndTime:    0,
		pointsRead:    0,
		pointsWritten: 0,
		shardId:       0,
		shardLocal:    false, //that really doesn't matter if it is not EXPLAIN query
		duration:      nil,
		seriesStates:  make(map[string]*SeriesState),
	}

	if queryEngine.explain {
		queryEngine.runStartTime = float64(time.Now().UnixNano()) / float64(time.Millisecond)
	}

	yield := func(series *protocol.Series) error {
		var response *protocol.Response

		queryEngine.limiter.calculateLimitAndSlicePoints(series)
		if len(series.Points) == 0 {
			return nil
		}
		if queryEngine.explain {
			//TODO: We may not have to send points, just count them
			queryEngine.pointsWritten += int64(len(series.Points))
		}
		response = &protocol.Response{Type: &queryResponse, Series: series}
		responseChan <- response
		return nil
	}

	var err error
	if query.HasAggregates() {
		err = queryEngine.executeCountQueryWithGroupBy(query, yield)
	} else if containsArithmeticOperators(query) {
		err = queryEngine.executeArithmeticQuery(query, yield)
	} else {
		err = queryEngine.distributeQuery(query, yield)
	}

	if err != nil {
		return nil, err
	}
	return queryEngine, nil
}

// Shard will call this method for EXPLAIN query
func (self *QueryEngine) SetShardInfo(shardId int, shardLocal bool) {
	self.shardId = shardId
	self.shardLocal = shardLocal
}

// Returns false if the query should be stopped (either because of limit or error)
func (self *QueryEngine) YieldPoint(seriesName *string, fieldNames []string, point *protocol.Point) (shouldContinue bool) {
	shouldContinue = true
	series := self.seriesToPoints[*seriesName]
	if series == nil {
		series = &protocol.Series{Name: protocol.String(*seriesName), Fields: fieldNames, Points: make([]*protocol.Point, 0, POINT_BATCH_SIZE)}
		self.seriesToPoints[*seriesName] = series
	} else if len(series.Points) >= POINT_BATCH_SIZE {
		shouldContinue = self.yieldSeriesData(series)
		series = &protocol.Series{Name: protocol.String(*seriesName), Fields: fieldNames, Points: make([]*protocol.Point, 0, POINT_BATCH_SIZE)}
		self.seriesToPoints[*seriesName] = series
	}
	series.Points = append(series.Points, point)

	if self.explain {
		self.pointsRead++
	}

	return shouldContinue
}

func (self *QueryEngine) YieldSeries(seriesIncoming *protocol.Series) (shouldContinue bool) {
	if self.explain {
		self.pointsRead += int64(len(seriesIncoming.Points))
	}
	seriesName := seriesIncoming.GetName()
	self.seriesToPoints[seriesName] = &protocol.Series{Name: &seriesName, Fields: seriesIncoming.Fields}
	return self.yieldSeriesData(seriesIncoming) && !self.limiter.hitLimit(seriesIncoming.GetName())
}

func (self *QueryEngine) yieldSeriesData(series *protocol.Series) bool {
	err := self.yield(series)
	if err != nil {
		log.Error(err)
		return false
	}
	return true
}

func (self *QueryEngine) Close() {
	for _, series := range self.seriesToPoints {
		if len(series.Points) == 0 {
			continue
		}
		self.yieldSeriesData(series)
	}

	var err error
	for _, series := range self.seriesToPoints {
		s := &protocol.Series{
			Name:   series.Name,
			Fields: series.Fields,
		}
		err = self.yield(s)
		if err != nil {
			break
		}
	}

	// make sure we yield an empty series for series without points
	fromClause := self.query.GetFromClause()
	if fromClause.Type == parser.FromClauseMerge {
		for _, s := range []string{fromClause.Names[0].Name.Name, fromClause.Names[1].Name.Name} {
			if _, ok := self.seriesToPoints[s]; ok {
				continue
			}

			err := self.yield(&protocol.Series{
				Name:   &s,
				Fields: []string{},
			})
			if err != nil {
				log.Error("Error while closing engine: %s", err)
			}
		}
	}

	if self.isAggregateQuery {
		self.runAggregates()
	}

	if self.explain {
		self.runEndTime = float64(time.Now().UnixNano()) / float64(time.Millisecond)
		log.Debug("QueryEngine: %.3f R:%d W:%d", self.runEndTime-self.runStartTime, self.pointsRead, self.pointsWritten)

		self.SendQueryStats()
	}
	response := &protocol.Response{Type: &endStreamResponse}
	if err != nil {
		message := err.Error()
		response.ErrorMessage = &message
	}
	self.responseChan <- response
}

func (self *QueryEngine) SendQueryStats() {
	timestamp := time.Now().UnixNano() / int64(time.Microsecond)

	runTime := self.runEndTime - self.runStartTime
	points := []*protocol.Point{}
	pointsRead := self.pointsRead
	pointsWritten := self.pointsWritten
	shardId := int64(self.shardId)
	shardLocal := self.shardLocal
	engineName := "QueryEngine"

	point := &protocol.Point{
		Values: []*protocol.FieldValue{
			{StringValue: &engineName},
			{Int64Value: &shardId},
			{BoolValue: &shardLocal},
			{DoubleValue: &runTime},
			{Int64Value: &pointsRead},
			{Int64Value: &pointsWritten},
		},
		Timestamp: &timestamp,
	}
	points = append(points, point)

	seriesName := "explain query"
	series := &protocol.Series{
		Name:   &seriesName,
		Fields: []string{"engine_name", "shard_id", "shard_local", "run_time", "points_read", "points_written"},
		Points: points,
	}
	response := &protocol.Response{Type: &explainQueryResponse, Series: series}
	self.responseChan <- response
}

func containsArithmeticOperators(query *parser.SelectQuery) bool {
	for _, column := range query.GetColumnNames() {
		if column.Type == parser.ValueExpression {
			return true
		}
	}
	return false
}

func (self *QueryEngine) getTimestampFromPoint(point *protocol.Point) int64 {
	return self.getTimestampBucket(uint64(*point.GetTimestampInMicroseconds()))
}

func (self *QueryEngine) getTimestampBucket(timestampMicroseconds uint64) int64 {
	timestampMicroseconds *= 1000 // convert to nanoseconds
	multiplier := uint64(*self.duration)
	return int64(timestampMicroseconds / multiplier * multiplier / 1000)
}

type PointRange struct {
	startTime int64
	endTime   int64
}

func (self *PointRange) UpdateRange(point *protocol.Point) {
	timestamp := *point.GetTimestampInMicroseconds()
	if timestamp < self.startTime {
		self.startTime = timestamp
	}
	if timestamp > self.endTime {
		self.endTime = timestamp
	}
}

func crossProduct(values [][][]*protocol.FieldValue) [][]*protocol.FieldValue {
	if len(values) == 0 {
		return [][]*protocol.FieldValue{{}}
	}

	_returnedValues := crossProduct(values[:len(values)-1])
	returnValues := [][]*protocol.FieldValue{}
	for _, v := range values[len(values)-1] {
		for _, values := range _returnedValues {
			returnValues = append(returnValues, append(values, v...))
		}
	}
	return returnValues
}

func (self *QueryEngine) executeCountQueryWithGroupBy(query *parser.SelectQuery, yield func(*protocol.Series) error) error {
	self.aggregateYield = yield
	duration, err := query.GetGroupByClause().GetGroupByTime()
	if err != nil {
		return err
	}

	self.isAggregateQuery = true
	self.duration = duration
	self.aggregators = []Aggregator{}

	for _, value := range query.GetColumnNames() {
		if !value.IsFunctionCall() {
			continue
		}
		lowerCaseName := strings.ToLower(value.Name)
		initializer := registeredAggregators[lowerCaseName]
		if initializer == nil {
			return common.NewQueryError(common.InvalidArgument, fmt.Sprintf("Unknown function %s", value.Name))
		}
		aggregator, err := initializer(query, value, query.GetGroupByClause().FillValue)
		if err != nil {
			return common.NewQueryError(common.InvalidArgument, fmt.Sprintf("%s", err))
		}
		self.aggregators = append(self.aggregators, aggregator)
	}

	for _, elem := range query.GetGroupByClause().Elems {
		if elem.IsFunctionCall() {
			continue
		}
		self.elems = append(self.elems, elem)
	}

	self.fillWithZero = query.GetGroupByClause().FillWithZero

	// This is a special case for issue #426. If the start time is
	// specified and there's a group by clause and fill with zero, then
	// we need to fill the entire range from start time to end time
	if query.IsStartTimeSpecified() && self.duration != nil && self.fillWithZero {
		self.startTimeSpecified = true
		self.startTime = query.GetStartTime().Truncate(*self.duration).UnixNano() / 1000
		self.endTime = query.GetEndTime().Truncate(*self.duration).UnixNano() / 1000
	}

	self.initializeFields()

	err = self.distributeQuery(query, func(series *protocol.Series) error {
		if len(series.Points) == 0 {
			return nil
		}

		return self.aggregateValuesForSeries(series)
	})

	return err
}

func (self *QueryEngine) initializeFields() {
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

func (self *QueryEngine) getSeriesState(name string) *SeriesState {
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
func (self *QueryEngine) aggregateValuesForSeries(series *protocol.Series) error {
	for _, aggregator := range self.aggregators {
		if err := aggregator.InitializeFieldsMetadata(series); err != nil {
			return err
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
				return err
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
				return err
			}
		}
	}

	return nil
}

func (self *QueryEngine) runAggregates() {
	for t := range self.seriesStates {
		self.runAggregatesForTable(t)
	}
}

func (self *QueryEngine) calculateSummariesForTable(table string) {
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

func (self *QueryEngine) runAggregatesForTable(table string) {
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
	self.aggregateYield(&protocol.Series{
		Name:   &table,
		Fields: self.fields,
		Points: points,
	})
}

func (self *QueryEngine) getValuesForGroup(table string, group []*protocol.FieldValue, node *Node) []*protocol.Point {

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

func (self *QueryEngine) executeArithmeticQuery(query *parser.SelectQuery, yield func(*protocol.Series) error) error {

	names := map[string]*parser.Value{}
	for idx, v := range query.GetColumnNames() {
		switch v.Type {
		case parser.ValueSimpleName:
			names[v.Name] = v
		case parser.ValueFunctionCall:
			names[v.Name] = v
		case parser.ValueExpression:
			if v.Alias != "" {
				names[v.Alias] = v
			} else {
				names["expr"+strconv.Itoa(idx)] = v
			}
		}
	}

	return self.distributeQuery(query, func(series *protocol.Series) error {
		if len(series.Points) == 0 {
			yield(series)
			return nil
		}

		newSeries := &protocol.Series{
			Name: series.Name,
		}

		// create the new column names
		for name := range names {
			newSeries.Fields = append(newSeries.Fields, name)
		}

		for _, point := range series.Points {
			newPoint := &protocol.Point{
				Timestamp:      point.Timestamp,
				SequenceNumber: point.SequenceNumber,
			}
			for _, field := range newSeries.Fields {
				value := names[field]
				v, err := GetValue(value, series.Fields, point)
				if err != nil {
					log.Error("Error in arithmetic computation: %s", err)
					return err
				}
				newPoint.Values = append(newPoint.Values, v)
			}
			newSeries.Points = append(newSeries.Points, newPoint)
		}

		yield(newSeries)

		return nil
	})
}

func (self *QueryEngine) GetName() string {
	return "QueryEngine"
}
