package engine

import (
	"coordinator"
	"parser"
	"protocol"
	"time"
)

type QueryEngine struct {
	coordinator coordinator.Coordinator
}

func (self *QueryEngine) RunQuery(query *parser.Query, yield func(*protocol.Series) error) error {
	var count int32 = 0 
  self.coordinator.DistributeQuery(query, func(series *protocol.Series) error {
    count += series.length
    return nil
  })

	expectedFieldType := protocol.FieldDefinition_INT32
	expectedName := "count"
  timestamp := time.Now().Unix()
  var sequenceNumnber uint32 = 0

	expectedData := &protocol.Series{
		Name: &query.GetFromClause().Name,
		Fields: []*protocol.FieldDefinition{
			&protocol.FieldDefinition{Name: &expectedName, Type: &expectedFieldType},
		},
		Points: []*protocol.Point{
			&protocol.Point{
				Timestamp:      &timestamp,
				SequenceNumber: &sequenceNumber,
				Values: []*protocol.FieldValue{
					&protocol.FieldValue{
						IntValue: &count,
					},
				},
			},
		},
	}

  yield(count)
	return nil
}

func NewQueryEngine(c coordinator.Coordinator) (EngineI, error) {
	return &QueryEngine{c}, nil
}
