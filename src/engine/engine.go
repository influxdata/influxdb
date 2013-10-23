package engine

import (
	"common"
	"coordinator"
	"fmt"
	"os"
	"parser"
	"protocol"
	"runtime"
	"time"
)

const ALL_GROUP_IDENTIFIER = 1

type QueryEngine struct {
	coordinator coordinator.Coordinator
}

func (self *QueryEngine) RunQuery(database string, query string, yield func(*protocol.Series) error) (err error) {
	// don't let a panic pass beyond RunQuery
	defer func() {
		if err := recover(); err != nil {
			fmt.Fprintf(os.Stderr, "********************************BUG********************************\n")
			buf := make([]byte, 1024)
			n := runtime.Stack(buf, false)
			fmt.Fprintf(os.Stderr, "Error: %s. Stacktrace: %s\n", err, string(buf[:n]))
			err = common.NewQueryError(common.InternalError, "Internal Error")
		}
	}()

	q, err := parser.ParseQuery(query)
	if err != nil {
		return err
	}
	if isAggregateQuery(q) {
		return self.executeCountQueryWithGroupBy(database, q, yield)
	} else {
		self.coordinator.DistributeQuery(database, q, yield)
	}
	return nil
}

func NewQueryEngine(c coordinator.Coordinator) (EngineI, error) {
	return &QueryEngine{c}, nil
}

func isAggregateQuery(query *parser.Query) bool {
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

type Mapper func(*protocol.Point) interface{}
type InverseMapper func(interface{}, int) interface{}

// Returns a mapper and inverse mapper. A mapper is a function to map
// a point to a group and return an identifier of the group that can
// be used in a map (i.e. the returned interface must be hashable).
// An inverse mapper, takes a result of the mapper identifier and
// return the column values and/or timestamp bucket that defines the
// given group.
func createValuesToInterface(groupBy parser.GroupByClause, fields []string) (Mapper, InverseMapper, error) {
	// we shouldn't get an error, this is checked earlier in the executeCountQueryWithGroupBy
	window, _ := groupBy.GetGroupByTime()
	names := []string{}
	for _, value := range groupBy {
		if value.IsFunctionCall() {
			continue
		}
		names = append(names, value.Name)
	}

	switch len(names) {
	case 0:
		if window != nil {
			// this must be group by time
			return func(p *protocol.Point) interface{} {
					return getTimestampFromPoint(*window, p)
				}, func(i interface{}, idx int) interface{} {
					return i
				}, nil
		}

		// this must be group by time
		return func(p *protocol.Point) interface{} {
				return ALL_GROUP_IDENTIFIER
			}, func(i interface{}, idx int) interface{} {
				panic("This should never be called")
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
			return nil, nil, common.NewQueryError(common.InvalidArgument, fmt.Sprintf("Invalid column name %s", groupBy[0].Name))
		}

		if window != nil {
			return func(p *protocol.Point) interface{} {
					return [2]interface{}{
						getTimestampFromPoint(*window, p),
						p.GetFieldValue(idx),
					}
				}, func(i interface{}, idx int) interface{} {
					return i.([2]interface{})[idx]
				}, nil
		}
		return func(p *protocol.Point) interface{} {
				return p.GetFieldValue(idx)
			}, func(i interface{}, idx int) interface{} {
				return i
			}, nil

	case 2:
		names := []string{}
		for _, value := range groupBy {
			names = append(names, value.Name)
		}

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

		if window != nil {
			return func(p *protocol.Point) interface{} {
					return [3]interface{}{
						getTimestampFromPoint(*window, p),
						p.GetFieldValue(idx1),
						p.GetFieldValue(idx2),
					}
				}, func(i interface{}, idx int) interface{} {
					return i.([3]interface{})[idx]
				}, nil
		}

		return func(p *protocol.Point) interface{} {
				return [2]interface{}{
					p.GetFieldValue(idx1),
					p.GetFieldValue(idx2),
				}
			}, func(i interface{}, idx int) interface{} {
				return i.([2]interface{})[idx]
			}, nil

	default:
		// TODO: return an error instead of killing the entire process
		return nil, nil, common.NewQueryError(common.InvalidArgument, "Group by currently support up to two columns and an optional group by time")
	}
}

func (self *QueryEngine) executeCountQueryWithGroupBy(database string, query *parser.Query, yield func(*protocol.Series) error) error {
	duration, err := query.GetGroupByClause().GetGroupByTime()
	if err != nil {
		return err
	}

	aggregators := []Aggregator{}
	for _, value := range query.GetColumnNames() {
		if value.IsFunctionCall() {
			initializer := registeredAggregators[value.Name]
			if initializer == nil {
				return common.NewQueryError(common.InvalidArgument, fmt.Sprintf("Unknown function %s", value.Name))
			}
			aggregator, err := initializer(query, value)
			if err != nil {
				return err
			}
			aggregators = append(aggregators, aggregator)
		}
	}
	timestampAggregator, err := registeredAggregators["__timestamp_aggregator"](query, nil)
	if err != nil {
		return err
	}

	groups := make(map[string]map[interface{}]bool)
	groupBy := query.GetGroupByClause()

	var inverse InverseMapper

	err = self.coordinator.DistributeQuery(database, query, func(series *protocol.Series) error {
		var mapper Mapper
		mapper, inverse, err = createValuesToInterface(groupBy, series.Fields)
		if err != nil {
			return err
		}

		for _, aggregator := range aggregators {
			if err := aggregator.InitializeFieldsMetadata(series); err != nil {
				return err
			}
		}

		for _, point := range series.Points {
			value := mapper(point)
			for _, aggregator := range aggregators {
				aggregator.AggregatePoint(*series.Name, value, point)
			}

			timestampAggregator.AggregatePoint(*series.Name, value, point)
			seriesGroups := groups[*series.Name]
			if seriesGroups == nil {
				seriesGroups = make(map[interface{}]bool)
				groups[*series.Name] = seriesGroups
			}
			seriesGroups[value] = true
		}

		return nil
	})

	if err != nil {
		return err
	}

	var sequenceNumber uint32 = 1
	fields := []string{}

	for _, aggregator := range aggregators {
		columnName := aggregator.ColumnName()
		fields = append(fields, columnName)
	}

	for _, value := range groupBy {
		if value.IsFunctionCall() {
			continue
		}

		tempName := value.Name
		fields = append(fields, tempName)
	}

	for table, tableGroups := range groups {
		tempTable := table
		points := []*protocol.Point{}
		for groupId, _ := range tableGroups {
			timestamp := *timestampAggregator.GetValue(table, groupId)[0].Int64Value
			/* groupPoints := []*protocol.Point{} */
			point := &protocol.Point{
				SequenceNumber: &sequenceNumber,
				Values:         []*protocol.FieldValue{},
			}
			point.SetTimestampInMicroseconds(timestamp)

			for _, aggregator := range aggregators {
				// point.Values = append(point.Values, aggregator.GetValue(table, groupId)[0])
				returnValues := aggregator.GetValue(table, groupId)
				returnDepth := len(returnValues)
				for _, value := range returnValues {
					if returnDepth > 1 {
						// do some crazy shit
					} else {
						point.Values = append(point.Values, value)
					}
				}
			}

			// FIXME: this should be looking at the fields slice not the group by clause
			// FIXME: we should check whether the selected columns are in the group by clause
			for idx, _ := range groupBy {
				if duration != nil && idx == 0 {
					continue
				}

				value := inverse(groupId, idx)

				switch x := value.(type) {
				case string:
					point.Values = append(point.Values, &protocol.FieldValue{StringValue: &x})
				case bool:
					point.Values = append(point.Values, &protocol.FieldValue{BoolValue: &x})
				case float64:
					point.Values = append(point.Values, &protocol.FieldValue{DoubleValue: &x})
				case int64:
					point.Values = append(point.Values, &protocol.FieldValue{Int64Value: &x})
				}
			}

			points = append(points, point)
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
