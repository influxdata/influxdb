package engine

import (
	"common"
	"coordinator"
	"datastore"
	"fmt"
	"os"
	"parser"
	"protocol"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

type QueryEngine struct {
	coordinator coordinator.Coordinator
}

func (self *QueryEngine) RunQuery(user common.User, database string, queryString string, localOnly bool, yield func(*protocol.Series) error) (err error) {
	// don't let a panic pass beyond RunQuery
	defer recoverFunc(database, queryString)

	q, err := parser.ParseQuery(queryString)
	if err != nil {
		return err
	}

	for _, query := range q {
		if query.DeleteQuery != nil {
			if err := self.coordinator.DeleteSeriesData(user, database, query.DeleteQuery, localOnly); err != nil {
				return err
			}
			continue
		}

		if query.IsListQuery() {
			series, err := self.coordinator.ListSeries(user, database)
			if err != nil {
				return err
			}
			for _, s := range series {
				if err := yield(s); err != nil {
					return err
				}
			}
			continue
		}

		selectQuery := query.SelectQuery
		if isAggregateQuery(selectQuery) {
			return self.executeCountQueryWithGroupBy(user, database, selectQuery, localOnly, yield)
		} else if containsArithmeticOperators(selectQuery) {
			return self.executeArithmeticQuery(user, database, selectQuery, localOnly, yield)
		} else {
			return self.distributeQuery(user, database, selectQuery, localOnly, yield)
		}
	}
	return nil
}

func recoverFunc(database, query string) {
	if err := recover(); err != nil {
		fmt.Fprintf(os.Stderr, "********************************BUG********************************\n")
		buf := make([]byte, 1024)
		n := runtime.Stack(buf, false)
		fmt.Fprintf(os.Stderr, "Database: %s\n", database)
		fmt.Fprintf(os.Stderr, "Query: [%s]\n", query)
		fmt.Fprintf(os.Stderr, "Error: %s. Stacktrace: %s\n", err, string(buf[:n]))
		err = common.NewQueryError(common.InternalError, "Internal Error")
	}
}

// distribute query and possibly do the merge/join before yielding the points
func (self *QueryEngine) distributeQuery(user common.User, database string, query *parser.SelectQuery, localOnly bool, yield func(*protocol.Series) error) (err error) {
	// see if this is a merge query
	fromClause := query.GetFromClause()
	if fromClause.Type == parser.FromClauseMerge {
		yield = getMergeYield(fromClause.Names[0].Name.Name, fromClause.Names[1].Name.Name, query.Ascending, yield)
	}

	if fromClause.Type == parser.FromClauseInnerJoin {
		yield = getJoinYield(query, yield)
	}

	return self.coordinator.DistributeQuery(user, database, query, localOnly, yield)
}

func NewQueryEngine(c coordinator.Coordinator) (EngineI, error) {
	return &QueryEngine{c}, nil
}

func containsArithmeticOperators(query *parser.SelectQuery) bool {
	for _, column := range query.GetColumnNames() {
		if column.Type == parser.ValueExpression {
			return true
		}
	}
	return false
}

func isAggregateQuery(query *parser.SelectQuery) bool {
	for _, column := range query.GetColumnNames() {
		if column.IsFunctionCall() {
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

func (self *QueryEngine) executeCountQueryWithGroupBy(user common.User, database string, query *parser.SelectQuery, localOnly bool,
	yield func(*protocol.Series) error) error {
	duration, err := query.GetGroupByClause().GetGroupByTime()
	if err != nil {
		return err
	}

	aggregators := []Aggregator{}
	for _, value := range query.GetColumnNames() {
		if value.IsFunctionCall() {
			lowerCaseName := strings.ToLower(value.Name)
			initializer := registeredAggregators[lowerCaseName]
			if initializer == nil {
				return common.NewQueryError(common.InvalidArgument, fmt.Sprintf("Unknown function %s", value.Name))
			}
			aggregator, err := initializer(query, value, query.GetGroupByClause().FillValue)
			if err != nil {
				return err
			}
			aggregators = append(aggregators, aggregator)
		}
	}
	timestampAggregator, err := NewTimestampAggregator(query, nil)
	if err != nil {
		return err
	}

	groups := make(map[string]map[Group]bool)
	pointsRange := make(map[string]*PointRange)
	groupBy := query.GetGroupByClause()

	err = self.distributeQuery(user, database, query, localOnly, func(series *protocol.Series) error {
		if len(series.Points) == 0 {
			return nil
		}

		var mapper Mapper
		mapper, err = createValuesToInterface(groupBy, series.Fields)
		if err != nil {
			return err
		}

		for _, aggregator := range aggregators {
			if err := aggregator.InitializeFieldsMetadata(series); err != nil {
				return err
			}
		}

		currentRange := pointsRange[*series.Name]
		for _, point := range series.Points {
			value := mapper(point)
			for _, aggregator := range aggregators {
				err := aggregator.AggregatePoint(*series.Name, value, point)
				if err != nil {
					return err
				}
			}

			timestampAggregator.AggregatePoint(*series.Name, value, point)
			seriesGroups := groups[*series.Name]
			if seriesGroups == nil {
				seriesGroups = make(map[Group]bool)
				groups[*series.Name] = seriesGroups
			}
			seriesGroups[value] = true

			if currentRange == nil {
				currentRange = &PointRange{*point.Timestamp, *point.Timestamp}
				pointsRange[*series.Name] = currentRange
			} else {
				currentRange.UpdateRange(point)
			}
		}

		return nil
	})

	if err != nil {
		return err
	}

	fields := []string{}

	for _, aggregator := range aggregators {
		columnNames := aggregator.ColumnNames()
		fields = append(fields, columnNames...)
	}

	for _, value := range groupBy.Elems {
		if value.IsFunctionCall() {
			continue
		}

		tempName := value.Name
		fields = append(fields, tempName)
	}

	for table, tableGroups := range groups {
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
			timeRange, ok := pointsRange[table]
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
				sortedGroups = &AscendingAggregatorSortableGroups{CommonSortableGroups{_groups, table}, timestampAggregator}
			} else {
				sortedGroups = &DescendingAggregatorSortableGroups{CommonSortableGroups{_groups, table}, timestampAggregator}
			}
		}
		sort.Sort(sortedGroups)

		for _, groupId := range sortedGroups.GetSortedGroups() {
			var timestamp int64
			if groupId.HasTimestamp() {
				timestamp = groupId.GetTimestamp()
			} else {
				timestamp = *timestampAggregator.GetValues(table, groupId)[0][0].Int64Value
			}
			values := [][][]*protocol.FieldValue{}

			for _, aggregator := range aggregators {
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
				for idx, _ := range groupBy.Elems {
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
		yield(expectedData)
	}

	return nil
}

func (self *QueryEngine) executeArithmeticQuery(user common.User, database string, query *parser.SelectQuery, localOnly bool,
	yield func(*protocol.Series) error) error {

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

	return self.distributeQuery(user, database, query, localOnly, func(series *protocol.Series) error {
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
				v, err := datastore.GetValue(value, series.Fields, point)
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
