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
		return self.executeCountQuery(query, yield)
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
