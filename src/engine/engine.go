package engine

import (
	"common"
	"fmt"
	"parser"
	"protocol"
	"sort"
	"strconv"
	"strings"
	"time"

	log "code.google.com/p/log4go"
)

var (
	TRUE = true
)

type QueryEngine struct {
	query          *parser.SelectQuery
	fields         []string
	where          *parser.WhereCondition
	responseChan   chan *protocol.Response
	limiter        *Limiter
	seriesToPoints map[string]*protocol.Series
	yield          func(*protocol.Series) error

	// variables for aggregate queries
	isAggregateQuery    bool
	aggregators         []Aggregator
	duration            *time.Duration
	timestampAggregator Aggregator
	groups              map[string]map[Group]bool
	buckets             map[string]int64
	pointsRange         map[string]*PointRange
	groupBy             *parser.GroupByClause
	aggregateYield      func(*protocol.Series) error
	explain             bool

	// query statistics
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
		buckets:       map[string]int64{},
		shardLocal:    false, //that really doesn't matter if it is not EXPLAIN query
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
			&protocol.FieldValue{StringValue: &engineName},
			&protocol.FieldValue{Int64Value: &shardId},
			&protocol.FieldValue{BoolValue: &shardLocal},
			&protocol.FieldValue{DoubleValue: &runTime},
			&protocol.FieldValue{Int64Value: &pointsRead},
			&protocol.FieldValue{Int64Value: &pointsWritten},
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
	multiplier := uint64(*self.duration)
	timestampNanoseconds := uint64(*point.GetTimestampInMicroseconds() * 1000)
	return int64(timestampNanoseconds / multiplier * multiplier / 1000)
}

// Mapper given a point returns a group identifier as the first return
// result and a non-time dependent group (the first group without time)
// as the second result
type Mapper func(*protocol.Point) Group

type PointRange struct {
	startTime int64
	endTime   int64
}

func (self *PointRange) UpdateRange(point *protocol.Point) {
	if *point.Timestamp < self.startTime {
		self.startTime = *point.Timestamp
	}
	if *point.Timestamp > self.endTime {
		self.endTime = *point.Timestamp
	}
}

func allGroupMapper(p *protocol.Point) Group { return ALL_GROUP_IDENTIFIER }

// Returns a mapper and inverse mapper. A mapper is a function to map
// a point to a group and return an identifier of the group that can
// be used in a map (i.e. the returned interface must be hashable).
// An inverse mapper, takes a result of the mapper identifier and
// return the column values and/or timestamp bucket that defines the
// given group.
func (self *QueryEngine) createValuesToInterface(groupBy *parser.GroupByClause, fields []string) (Mapper, error) {
	// we shouldn't get an error, this is checked earlier in the executeCountQueryWithGroupBy
	var names []string
	for _, value := range groupBy.Elems {
		if value.IsFunctionCall() {
			continue
		}
		names = append(names, value.Name)
	}

	if names == nil && self.duration == nil {
		return allGroupMapper, nil
	}

	if names == nil {
		return func(p *protocol.Point) Group {
			return ALL_GROUP_IDENTIFIER.WithTimestamp(self.getTimestampFromPoint(p))
		}, nil
	}

	sort.Sort(ReverseStringSlice(names))

	indecesMap := map[string]int{}
	for index, fieldName := range fields {
		indecesMap[fieldName] = index
	}

	mapper := func(p *protocol.Point) Group {
		var group Group = ALL_GROUP_IDENTIFIER
		for _, name := range names {
			idx := indecesMap[name]
			group = createGroup2(false, p.GetFieldValue(idx), group)
		}
		return group
	}

	if self.duration == nil {
		return mapper, nil
	}

	return func(p *protocol.Point) Group {
		return mapper(p).WithTimestamp(self.getTimestampFromPoint(p))
	}, nil
}

func crossProduct(values [][][]*protocol.FieldValue) [][]*protocol.FieldValue {
	if len(values) == 0 {
		return [][]*protocol.FieldValue{[]*protocol.FieldValue{}}
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

	timestampAggregator, err := NewTimestampAggregator(query, nil)
	if err != nil {
		return err
	}
	self.timestampAggregator = timestampAggregator
	self.groups = make(map[string]map[Group]bool)
	self.pointsRange = make(map[string]*PointRange)
	self.groupBy = query.GetGroupByClause()

	self.initializeFields()

	err = self.distributeQuery(query, func(series *protocol.Series) error {
		if len(series.Points) == 0 {
			return nil
		}

		// if we're not doing group by time() then keep all the state in
		// memory until the query finishes reading all data points
		if self.duration == nil || query.GetGroupByClause().FillWithZero {
			return self.aggregateValuesForSeries(series)
		}

		// otherwise, keep the state for the current bucket. Once ticks
		// come in for a different time bucket, we flush the state that's
		// kept in memory by the aggregators

		// split the time series by time buckets
		bucketedSeries := []*protocol.Series{}
		currentSeries := &protocol.Series{
			Name:   series.Name,
			Fields: series.Fields,
			Points: []*protocol.Point{series.Points[0]},
		}
		currentBucket := self.getTimestampFromPoint(series.Points[0])
		for _, p := range series.Points[1:] {
			bucket := self.getTimestampFromPoint(p)
			if bucket != currentBucket {
				bucketedSeries = append(bucketedSeries, currentSeries)
				currentSeries = &protocol.Series{Name: series.Name, Fields: series.Fields}
				currentBucket = bucket
			}
			currentSeries.Points = append(currentSeries.Points, p)
		}
		bucketedSeries = append(bucketedSeries, currentSeries)

		for _, s := range bucketedSeries[:len(bucketedSeries)-1] {
			if err := self.aggregateValuesForSeries(s); err != nil {
				return err
			}
			self.calculateSummariesForTable(*s.Name)
		}

		last := bucketedSeries[len(bucketedSeries)-1]
		bucket := self.getTimestampFromPoint(last.Points[0])
		if b, ok := self.buckets[*series.Name]; ok && b != bucket {
			self.calculateSummariesForTable(*last.Name)
		}

		self.buckets[*series.Name] = bucket
		return self.aggregateValuesForSeries(last)
	})

	return err
}

func (self *QueryEngine) initializeFields() {
	for _, aggregator := range self.aggregators {
		columnNames := aggregator.ColumnNames()
		self.fields = append(self.fields, columnNames...)
	}

	if self.groupBy == nil {
		return
	}

	for _, value := range self.groupBy.Elems {
		if value.IsFunctionCall() {
			continue
		}

		tempName := value.Name
		self.fields = append(self.fields, tempName)
	}
}

func (self *QueryEngine) aggregateValuesForSeries(series *protocol.Series) error {
	seriesGroups := make(map[Group]*protocol.Series)

	mapper, err := self.createValuesToInterface(self.groupBy, series.Fields)
	if err != nil {
		return err
	}

	for _, aggregator := range self.aggregators {
		if err := aggregator.InitializeFieldsMetadata(series); err != nil {
			return err
		}
	}

	currentRange := self.pointsRange[*series.Name]
	if currentRange == nil {
		currentRange = &PointRange{*series.Points[0].Timestamp, *series.Points[0].Timestamp}
		self.pointsRange[*series.Name] = currentRange
	}
	for _, point := range series.Points {
		currentRange.UpdateRange(point)
		value := mapper(point)
		seriesGroup := seriesGroups[value]
		if seriesGroup == nil {
			seriesGroup = &protocol.Series{Name: series.Name, Fields: series.Fields, Points: make([]*protocol.Point, 0)}
			seriesGroups[value] = seriesGroup
		}
		seriesGroup.Points = append(seriesGroup.Points, point)
	}

	for value, seriesGroup := range seriesGroups {
		for _, aggregator := range self.aggregators {
			err := aggregator.AggregateSeries(*series.Name, value, seriesGroup)
			if err != nil {
				return err
			}
		}
		self.timestampAggregator.AggregateSeries(*series.Name, value, seriesGroup)

		_groups := self.groups[*seriesGroup.Name]
		if _groups == nil {
			_groups = make(map[Group]bool)
			self.groups[*seriesGroup.Name] = _groups
		}
		_groups[value] = true
	}

	return nil
}

func (self *QueryEngine) runAggregates() {
	for table, _ := range self.groups {
		self.calculateSummariesForTable(table)
		self.runAggregatesForTable(table)
	}
}

func (self *QueryEngine) calculateSummariesForTable(table string) {
	tableGroups := self.groups[table]
	// delete(self.groups, table)

	for _, aggregator := range self.aggregators {
		for group, _ := range tableGroups {
			aggregator.CalculateSummaries(table, group)
		}
	}
}

func (self *QueryEngine) runAggregatesForTable(table string) {
	duration := self.duration
	query := self.query

	var _groups []Group
	tableGroups := self.groups[table]
	delete(self.groups, table)

	if len(tableGroups) == 0 {
		return
	}

	if !query.GetGroupByClause().FillWithZero || duration == nil {
		// sort the table groups by timestamp
		_groups = make([]Group, 0, len(tableGroups))
		for groupId, _ := range tableGroups {
			_groups = append(_groups, groupId)
		}
	} else {
		groupsWithTime := map[Group]bool{}
		timeRange, ok := self.pointsRange[table]
		if ok {
			first := timeRange.startTime * 1000 / int64(*duration) * int64(*duration)
			end := timeRange.endTime * 1000 / int64(*duration) * int64(*duration)
			for i := 0; ; i++ {
				timestamp := first + int64(i)*int64(*duration)
				if end < timestamp {
					break
				}
				for group, _ := range tableGroups {
					groupWithTime := group.WithoutTimestamp().WithTimestamp(timestamp / 1000)
					groupsWithTime[groupWithTime] = true
				}
			}

			for groupId, _ := range groupsWithTime {
				_groups = append(_groups, groupId)
			}
		}
	}

	self.yieldValuesForTableAndGroups(table, _groups)
}

func (self *QueryEngine) yieldValuesForTableAndGroups(table string, groups []Group) {

	points := []*protocol.Point{}

	query := self.query
	var sortedGroups SortableGroups
	fillWithZero := self.duration != nil && query.GetGroupByClause().FillWithZero
	if fillWithZero {
		if query.Ascending {
			sortedGroups = &AscendingGroupTimestampSortableGroups{CommonSortableGroups{groups, table}}
		} else {
			sortedGroups = &DescendingGroupTimestampSortableGroups{CommonSortableGroups{groups, table}}
		}
	} else {
		if self.query.Ascending {
			sortedGroups = &AscendingAggregatorSortableGroups{CommonSortableGroups{groups, table}, self.timestampAggregator}
		} else {
			sortedGroups = &DescendingAggregatorSortableGroups{CommonSortableGroups{groups, table}, self.timestampAggregator}
		}
	}
	sort.Sort(sortedGroups)

	for _, groupId := range sortedGroups.GetSortedGroups() {
		var timestamp int64
		if groupId.HasTimestamp() {
			timestamp = groupId.GetTimestamp()
		} else {
			timestamp = *self.timestampAggregator.GetValues(table, groupId)[0][0].Int64Value
		}
		values := [][][]*protocol.FieldValue{}

		for _, aggregator := range self.aggregators {
			values = append(values, aggregator.GetValues(table, groupId))
		}

		// do cross product of all the values
		_values := crossProduct(values)

		for _, v := range _values {
			/* groupPoints := []*protocol.Point{} */
			point := &protocol.Point{
				Values: v,
			}
			point.SetTimestampInMicroseconds(timestamp)

			// FIXME: this should be looking at the fields slice not the group by clause
			// FIXME: we should check whether the selected columns are in the group by clause
			for idx, _ := range self.groupBy.Elems {
				if self.duration != nil && idx == 0 {
					continue
				}

				value := groupId.GetValue(idx)

				switch x := value.(type) {
				case string:
					point.Values = append(point.Values, &protocol.FieldValue{StringValue: &x})
				case bool:
					point.Values = append(point.Values, &protocol.FieldValue{BoolValue: &x})
				case float64:
					point.Values = append(point.Values, &protocol.FieldValue{DoubleValue: &x})
				case int64:
					point.Values = append(point.Values, &protocol.FieldValue{Int64Value: &x})
				case nil:
					point.Values = append(point.Values, &protocol.FieldValue{IsNull: &TRUE})
				}
			}

			points = append(points, point)
		}
	}
	expectedData := &protocol.Series{
		Name:   &table,
		Fields: self.fields,
		Points: points,
	}
	self.aggregateYield(expectedData)
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
			names["expr"+strconv.Itoa(idx)] = v
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
		for name, _ := range names {
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
