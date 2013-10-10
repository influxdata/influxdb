package engine

import (
	"coordinator"
	"parser"
	"protocol"
)

type QueryEngine struct {
	coordinator coordinator.Coordinator
}

func (self *QueryEngine) RunQuery(query *parser.Query, yield func(*protocol.Series) error) error {
	if isCountQuery(query) {
		if groupBy := query.GetGroupByClause(); len(groupBy) > 0 {
			return self.executeCountQueryWithGroupBy(query, yield)
		} else {
			return self.executeCountQuery(query, yield)
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

func (self *QueryEngine) executeCountQueryWithGroupBy(query *parser.Query, yield func(*protocol.Series) error) error {
	counts := make(map[interface{}]int32)
	var timestamp int64 = 0
	groupBy := query.GetGroupByClause()[0]
	var columnType protocol.FieldDefinition_Type

	self.coordinator.DistributeQuery(query, func(series *protocol.Series) error {
		var columnIndex int

		for index, value := range series.Fields {
			if *value.Name == groupBy.Name {
				columnIndex = index
				columnType = *value.Type
				break
			}
		}

		for _, point := range series.Points {
			var value interface{}

			switch columnType {
			case protocol.FieldDefinition_STRING:
				value = *point.Values[columnIndex].StringValue
			case protocol.FieldDefinition_INT32:
				value = *point.Values[columnIndex].IntValue
			case protocol.FieldDefinition_DOUBLE:
				value = *point.Values[columnIndex].DoubleValue
			case protocol.FieldDefinition_BOOL:
				value = *point.Values[columnIndex].BoolValue
			}

			c := counts[value]
			counts[value] = c + 1
		}

		if len(series.Points) > 0 {
			timestamp = series.Points[0].GetTimestamp()
		}
		return nil
	})

	expectedFieldType := protocol.FieldDefinition_INT32
	expectedName := "count"
	var sequenceNumber uint32 = 1

	/* fields := []*protocol.FieldDefinition{} */
	points := []*protocol.Point{}

	fields := []*protocol.FieldDefinition{
		&protocol.FieldDefinition{Name: &expectedName, Type: &expectedFieldType},
		&protocol.FieldDefinition{Name: &groupBy.Name, Type: &columnType},
	}

	for key, count := range counts {
		tempKey := key.(string)
		tempCount := count

		points = append(points, &protocol.Point{
			Timestamp:      &timestamp,
			SequenceNumber: &sequenceNumber,
			Values: []*protocol.FieldValue{
				&protocol.FieldValue{
					IntValue: &tempCount,
				},
				&protocol.FieldValue{
					StringValue: &tempKey,
				},
			},
		})
	}

	expectedData := &protocol.Series{
		Name:   &query.GetFromClause().Name,
		Fields: fields,
		Points: points,
	}
	yield(expectedData)
	return nil
}
