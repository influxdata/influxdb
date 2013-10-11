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

type QueryEngine struct {
	coordinator coordinator.Coordinator
}

func (self *QueryEngine) RunQuery(query *parser.Query, yield func(*protocol.Series) error) (err error) {
	// don't let a panic pass beyond RunQuery
	defer func() {
		if err := recover(); err != nil {
			fmt.Fprintf(os.Stderr, "********************************BUG********************************\n")
			buf := make([]byte, 1024)
			n := runtime.Stack(buf, false)
			fmt.Fprintf(os.Stderr, "Stacktrace: %s\n", string(buf[:n]))
			err = common.NewQueryError(common.InternalError, "Internal Error")
		}
	}()

	if isCountQuery(query) {
		if groupBy := query.GetGroupByClause(); len(groupBy) > 0 {
			return self.executeCountQueryWithGroupBy(query, yield)
		} else {
			return self.executeCountQuery(query, yield)
		}
	} else if isMinQuery(query) {
		if groupBy := query.GetGroupByClause(); len(groupBy) > 0 {
			return self.executeMinQueryWithGroupBy(query, yield)
		} else {
			// return self.executeMinQuery(query, yield)
		}
	} else {
		self.coordinator.DistributeQuery(query, yield)
	}
	return nil
}

func NewQueryEngine(c coordinator.Coordinator) (EngineI, error) {
	return &QueryEngine{c}, nil
}

func isCountQuery(query *parser.Query) bool {
	for _, column := range query.GetColumnNames() {
		if column.IsFunctionCall() && column.Name == "count" {
			return true
		}
	}

	return false
}

func isMinQuery(query *parser.Query) bool {
	for _, column := range query.GetColumnNames() {
		if column.IsFunctionCall() && column.Name == "min" {
			return true
		}
	}

	return false
}

func (self *QueryEngine) executeCountQuery(query *parser.Query, yield func(*protocol.Series) error) error {
	count := make(map[string]int32)
	var timestamp int64 = 0

	self.coordinator.DistributeQuery(query, func(series *protocol.Series) error {
		c := count[*series.Name]
		c += int32(len(series.Points))
		count[*series.Name] = c
		if len(series.Points) > 0 {
			timestamp = series.Points[0].GetTimestamp()
		}
		return nil
	})

	expectedFieldType := protocol.FieldDefinition_INT32
	expectedName := "count"
	var sequenceNumber uint32 = 1

	for name, value := range count {
		tempValue := value
		tempName := name

		expectedData := &protocol.Series{
			Name: &tempName,
			Fields: []*protocol.FieldDefinition{
				&protocol.FieldDefinition{Name: &expectedName, Type: &expectedFieldType},
			},
			Points: []*protocol.Point{
				&protocol.Point{
					Timestamp:      &timestamp,
					SequenceNumber: &sequenceNumber,
					Values: []*protocol.FieldValue{
						&protocol.FieldValue{
							IntValue: &tempValue,
						},
					},
				},
			},
		}
		yield(expectedData)
	}
	return nil
}

func getValueFromPoint(value *protocol.FieldValue, fType protocol.FieldDefinition_Type) interface{} {
	switch fType {
	case protocol.FieldDefinition_STRING:
		return *value.StringValue
	case protocol.FieldDefinition_INT32:
		return *value.IntValue
	case protocol.FieldDefinition_BOOL:
		return *value.BoolValue
	case protocol.FieldDefinition_DOUBLE:
		return *value.DoubleValue
	default:
		panic("WTF")
	}
}

func getTimestampFromPoint(window time.Duration, point *protocol.Point) int64 {
	return time.Unix(*point.Timestamp, 0).Round(window).Unix()
}

type Mapper func(*protocol.Point) interface{}
type InverseMapper func(interface{}, int) interface{}

// Returns a mapper and inverse mapper. A mapper is a function to map
// a point to a group and return an identifier of the group that can
// be used in a map (i.e. the returned interface must be hashable).
// An inverse mapper, takes a result of the mapper identifier and
// return the column values and/or timestamp bucket that defines the
// given group.
func createValuesToInterface(groupBy parser.GroupByClause, definitions []*protocol.FieldDefinition) (Mapper, InverseMapper, error) {
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
		// this must be group by time
		return func(p *protocol.Point) interface{} {
				return getTimestampFromPoint(*window, p)
			}, func(i interface{}, idx int) interface{} {
				return i
			}, nil

	case 1:
		// otherwise, find the type of the column and create a mapper
		idx := -1
		var fType protocol.FieldDefinition_Type
		for index, definition := range definitions {
			if *definition.Name == names[0] {
				idx = index
				fType = *definition.Type
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
						getValueFromPoint(p.Values[idx], fType),
					}
				}, func(i interface{}, idx int) interface{} {
					return i.([2]interface{})[idx]
				}, nil
		}
		return func(p *protocol.Point) interface{} {
				return getValueFromPoint(p.Values[idx], fType)
			}, func(i interface{}, idx int) interface{} {
				return i
			}, nil

	case 2:
		names := []string{}
		for _, value := range groupBy {
			names = append(names, value.Name)
		}

		idx1, idx2 := -1, -1
		var fType1, fType2 protocol.FieldDefinition_Type

		for index, definition := range definitions {
			if *definition.Name == names[0] {
				idx1 = index
				fType1 = *definition.Type
			} else if *definition.Name == names[1] {
				idx2 = index
				fType2 = *definition.Type
			}
			if idx1 > 0 && idx2 > 0 {
				break
			}
		}

		if window != nil {
			return func(p *protocol.Point) interface{} {
					return [3]interface{}{
						getTimestampFromPoint(*window, p),
						getValueFromPoint(p.Values[idx1], fType1),
						getValueFromPoint(p.Values[idx2], fType2),
					}
				}, func(i interface{}, idx int) interface{} {
					return i.([3]interface{})[idx]
				}, nil
		}

		return func(p *protocol.Point) interface{} {
				return [2]interface{}{
					getValueFromPoint(p.Values[idx1], fType1),
					getValueFromPoint(p.Values[idx2], fType2),
				}
			}, func(i interface{}, idx int) interface{} {
				return i.([2]interface{})[idx]
			}, nil

	default:
		// TODO: return an error instead of killing the entire process
		return nil, nil, common.NewQueryError(common.InvalidArgument, "Group by currently support up to two columns and an optional group by time")
	}
}

func (self *QueryEngine) executeCountQueryWithGroupBy(query *parser.Query, yield func(*protocol.Series) error) error {
	counts := make(map[interface{}]int32)
	timestamps := make(map[interface{}]int64)

	groupBy := query.GetGroupByClause()

	fieldTypes := map[string]*protocol.FieldDefinition_Type{}
	var inverse InverseMapper

	duration, err := groupBy.GetGroupByTime()
	if err != nil {
		return err
	}

	err = self.coordinator.DistributeQuery(query, func(series *protocol.Series) error {
		var mapper Mapper
		mapper, inverse, err = createValuesToInterface(groupBy, series.Fields)
		if err != nil {
			return err
		}

		for _, field := range series.Fields {
			fieldTypes[*field.Name] = field.Type
		}

		for _, point := range series.Points {
			value := mapper(point)
			c := counts[value]
			counts[value] = c + 1

			if duration != nil {
				timestamps[value] = getTimestampFromPoint(*duration, point)
			} else {
				timestamps[value] = point.GetTimestamp()
			}
		}

		return nil
	})

	if err != nil {
		return err
	}

	expectedFieldType := protocol.FieldDefinition_INT32
	expectedName := "count"
	var sequenceNumber uint32 = 1

	/* fields := []*protocol.FieldDefinition{} */
	points := []*protocol.Point{}

	fields := []*protocol.FieldDefinition{
		&protocol.FieldDefinition{Name: &expectedName, Type: &expectedFieldType},
	}

	for _, value := range groupBy {
		if value.IsFunctionCall() {
			continue
		}

		tempName := value.Name
		fields = append(fields, &protocol.FieldDefinition{Name: &tempName, Type: fieldTypes[tempName]})
	}

	for key, count := range counts {
		tempKey := key
		tempCount := count

		timestamp := timestamps[tempKey]

		point := &protocol.Point{
			Timestamp:      &timestamp,
			SequenceNumber: &sequenceNumber,
			Values: []*protocol.FieldValue{
				&protocol.FieldValue{
					IntValue: &tempCount,
				},
			},
		}

		for idx, _ := range groupBy {
			value := inverse(tempKey, idx)

			switch x := value.(type) {
			case string:
				point.Values = append(point.Values, &protocol.FieldValue{StringValue: &x})
			case int32:
				point.Values = append(point.Values, &protocol.FieldValue{IntValue: &x})
			case bool:
				point.Values = append(point.Values, &protocol.FieldValue{BoolValue: &x})
			case float64:
				point.Values = append(point.Values, &protocol.FieldValue{DoubleValue: &x})
			}
		}

		points = append(points, point)
	}

	expectedData := &protocol.Series{
		Name:   &query.GetFromClause().Name,
		Fields: fields,
		Points: points,
	}
	yield(expectedData)
	return nil
}

func (self *QueryEngine) executeMinQueryWithGroupBy(query *parser.Query, yield func(*protocol.Series) error) error {
	mins := make(map[interface{}]int32)
	timestamps := make(map[interface{}]int64)
	var minField string
	var inverse InverseMapper

	groupBy := query.GetGroupByClause()

	for _, column := range query.GetColumnNames() {
		if column.IsFunctionCall() && column.Name == "min" {
			minField = column.Elems[0].Name
		}
	}

	fieldTypes := map[string]*protocol.FieldDefinition_Type{}
	duration, ok := groupBy.GetGroupByTime()

	self.coordinator.DistributeQuery(query, func(series *protocol.Series) error {
		var mapper Mapper
		mapper, inverse = createValuesToInterface(groupBy, series.Fields)
		var fieldIndex int

		for idx, field := range series.Fields {
			fieldTypes[*field.Name] = field.Type
			if *field.Name == minField {
				fieldIndex = idx
			}
		}

		for _, point := range series.Points {
			min := *point.Values[fieldIndex].IntValue
			value := mapper(point)

			if oldMin, exists := mins[value]; exists {
				if min < oldMin {
					mins[value] = min
				}
			} else {
				mins[value] = min
			}

			if ok {
				timestamps[value] = getTimestampFromPoint(duration, point)
			} else {
				timestamps[value] = point.GetTimestamp()
			}
		}

		return nil
	})

	expectedFieldType := protocol.FieldDefinition_INT32
	expectedName := "min"
	var sequenceNumber uint32 = 1

	/* fields := []*protocol.FieldDefinition{} */
	points := []*protocol.Point{}

	fields := []*protocol.FieldDefinition{
		&protocol.FieldDefinition{Name: &expectedName, Type: &expectedFieldType},
	}

	for _, value := range groupBy {
		if value.IsFunctionCall() {
			continue
		}

		tempName := value.Name
		fields = append(fields, &protocol.FieldDefinition{Name: &tempName, Type: fieldTypes[tempName]})
	}

	for key, count := range mins {
		tempKey := key
		tempCount := count

		timestamp := timestamps[tempKey]

		point := &protocol.Point{
			Timestamp:      &timestamp,
			SequenceNumber: &sequenceNumber,
			Values: []*protocol.FieldValue{
				&protocol.FieldValue{
					IntValue: &tempCount,
				},
			},
		}

		for idx, _ := range groupBy {
			value := inverse(tempKey, idx)

			switch x := value.(type) {
			case string:
				point.Values = append(point.Values, &protocol.FieldValue{StringValue: &x})
			case int32:
				point.Values = append(point.Values, &protocol.FieldValue{IntValue: &x})
			case bool:
				point.Values = append(point.Values, &protocol.FieldValue{BoolValue: &x})
			case float64:
				point.Values = append(point.Values, &protocol.FieldValue{DoubleValue: &x})
			}
		}

		points = append(points, point)
	}

	expectedData := &protocol.Series{
		Name:   &query.GetFromClause().Name,
		Fields: fields,
		Points: points,
	}
	yield(expectedData)
	return nil
}
