package engine

import (
	log "code.google.com/p/log4go"
	"common"
	"fmt"
	"parser"
	"protocol"
	"sort"
	"strconv"
	"strings"
	"time"
)

type QueryEngine struct {
	query          *parser.SelectQuery
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
	// disable limit if the query has aggregates let the coordinator
	// deal with the limit
	if query.HasAggregates() {
		limit = 0
	}

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
	}

	if queryEngine.explain {
		queryEngine.runStartTime = float64(time.Now().UnixNano()) / float64(time.Millisecond)
	}

	yield := func(series *protocol.Series) error {
		var response *protocol.Response

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

func (self *QueryEngine) YieldSeries(seriesName *string, fieldNames []string, seriesIncoming *protocol.Series) (shouldContinue bool) {
	if self.explain {
		self.pointsRead += int64(len(seriesIncoming.Points))
	}
	return self.yieldSeriesData(seriesIncoming)
}

func (self *QueryEngine) yieldSeriesData(series *protocol.Series) bool {
	var err error
	if self.where != nil {
		serieses, err := self.filter(series)
		if err != nil {
			log.Error("Error while filtering points: %s\n", err)
			return false
		}
		for _, series := range serieses {
			if len(series.Points) > 0 {
				self.limiter.calculateLimitAndSlicePoints(series)
				if len(series.Points) > 0 {
					if err = self.yield(series); err != nil {
						return false
					}
				}
			}
		}
	} else {
		self.limiter.calculateLimitAndSlicePoints(series)

		if len(series.Points) > 0 {
			err = self.yield(series)
		}
	}
	if err != nil {
		log.Error(err)
		return false
	}
	return !self.limiter.hitLimit(*series.Name)
}

func (self *QueryEngine) filter(series *protocol.Series) ([]*protocol.Series, error) {
	aliases := self.query.GetTableAliases(*series.Name)
	result := make([]*protocol.Series, len(aliases), len(aliases))
	for i, alias := range aliases {
		_alias := alias
		newSeries := &protocol.Series{Name: &_alias, Points: series.Points, Fields: series.Fields}

		filteredResult := newSeries
		var err error

		// var err error
		if self.query.GetFromClause().Type != parser.FromClauseInnerJoin {
			filteredResult, err = Filter(self.query, newSeries)
			if err != nil {
				return nil, err
			}
		}
		result[i] = filteredResult
	}
	return result, nil
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

func getTimestampFromPoint(window time.Duration, point *protocol.Point) int64 {
	multiplier := int64(window / time.Microsecond)
	return *point.GetTimestampInMicroseconds() / int64(multiplier) * int64(multiplier)
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

// Returns a mapper and inverse mapper. A mapper is a function to map
// a point to a group and return an identifier of the group that can
// be used in a map (i.e. the returned interface must be hashable).
// An inverse mapper, takes a result of the mapper identifier and
// return the column values and/or timestamp bucket that defines the
// given group.
func createValuesToInterface(groupBy *parser.GroupByClause, fields []string) (Mapper, error) {
	// we shouldn't get an error, this is checked earlier in the executeCountQueryWithGroupBy
	window, _ := groupBy.GetGroupByTime()
	names := []string{}
	for _, value := range groupBy.Elems {
		if value.IsFunctionCall() {
			continue
		}
		names = append(names, value.Name)
	}

	switch len(names) {
	case 0:
		if window != nil {
			// this must be group by time
			return func(p *protocol.Point) Group {
				return createGroup1(true, getTimestampFromPoint(*window, p))
			}, nil
		}

		// this must be group by time
		return func(p *protocol.Point) Group {
			return ALL_GROUP_IDENTIFIER
		}, nil

	case 1:
		// otherwise, find the type of the column and create a mapper
		idx := -1
		for index, fieldName := range fields {
			if fieldName == names[0] {
				idx = index
				break
			}
		}

		if idx == -1 {
			return nil, common.NewQueryError(common.InvalidArgument, fmt.Sprintf("Invalid column name %s", groupBy.Elems[0].Name))
		}

		if window != nil {
			return func(p *protocol.Point) Group {
				return createGroup2(true, getTimestampFromPoint(*window, p), p.GetFieldValue(idx))
			}, nil
		}
		return func(p *protocol.Point) Group {
			return createGroup1(false, p.GetFieldValue(idx))
		}, nil

	case 2:
		idx1, idx2 := -1, -1
		for index, fieldName := range fields {
			if fieldName == names[0] {
				idx1 = index
			} else if fieldName == names[1] {
				idx2 = index
			}
			if idx1 > 0 && idx2 > 0 {
				break
			}
		}

		if idx1 == -1 {
			return nil, common.NewQueryError(common.InvalidArgument, fmt.Sprintf("Invalid column name %s", names[0]))
		}

		if idx2 == -1 {
			return nil, common.NewQueryError(common.InvalidArgument, fmt.Sprintf("Invalid column name %s", names[1]))
		}

		if window != nil {
			return func(p *protocol.Point) Group {
				return createGroup3(true, getTimestampFromPoint(*window, p), p.GetFieldValue(idx1), p.GetFieldValue(idx2))
			}, nil
		}

		return func(p *protocol.Point) Group {
			return createGroup2(false, p.GetFieldValue(idx1), p.GetFieldValue(idx2))
		}, nil

	default:
		// TODO: return an error instead of killing the entire process
		return nil, common.NewQueryError(common.InvalidArgument, "Group by currently support up to two columns and an optional group by time")
	}
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

	err = self.distributeQuery(query, func(series *protocol.Series) error {
		if len(series.Points) == 0 {
			return nil
		}

		seriesGroups := make(map[Group]*protocol.Series)

		var mapper Mapper
		mapper, err = createValuesToInterface(self.groupBy, series.Fields)
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
	})

	return err
}

func (self *QueryEngine) runAggregates() {
	duration := self.duration
	query := self.query

	fields := []string{}

	for _, aggregator := range self.aggregators {
		columnNames := aggregator.ColumnNames()
		fields = append(fields, columnNames...)
	}

	for _, value := range self.groupBy.Elems {
		if value.IsFunctionCall() {
			continue
		}

		tempName := value.Name
		fields = append(fields, tempName)
	}

	for table, tableGroups := range self.groups {
		tempTable := table
		points := []*protocol.Point{}

		var _groups []Group

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

		fillWithZero := duration != nil && query.GetGroupByClause().FillWithZero
		var sortedGroups SortableGroups
		if fillWithZero {
			if query.Ascending {
				sortedGroups = &AscendingGroupTimestampSortableGroups{CommonSortableGroups{_groups, table}}
			} else {
				sortedGroups = &DescendingGroupTimestampSortableGroups{CommonSortableGroups{_groups, table}}
			}
		} else {
			if query.Ascending {
				sortedGroups = &AscendingAggregatorSortableGroups{CommonSortableGroups{_groups, table}, self.timestampAggregator}
			} else {
				sortedGroups = &DescendingAggregatorSortableGroups{CommonSortableGroups{_groups, table}, self.timestampAggregator}
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
					if duration != nil && idx == 0 {
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
						point.Values = append(point.Values, nil)
					}
				}

				points = append(points, point)
			}
		}
		expectedData := &protocol.Series{
			Name:   &tempTable,
			Fields: fields,
			Points: points,
		}
		self.aggregateYield(expectedData)
	}
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
