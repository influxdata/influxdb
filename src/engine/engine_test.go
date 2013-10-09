package engine

import (
	"coordinator"
	. "launchpad.net/gocheck"
	"parser"
	"protocol"
	"testing"
	"time"
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) {
	TestingT(t)
}

type EngineSuite struct{}

var _ = Suite(&EngineSuite{})

type MockCoordinator struct {
	series []*protocol.Series
}

func (self *MockCoordinator) DistributeQuery(query *parser.Query, yield func(*protocol.Series) error) error {
	for _, series := range self.series {
		yield(series)
	}
	return nil
}

func (self *MockCoordinator) WriteSeriesData(series *protocol.Series) error {
	return nil
}

func createMockCoordinator(series ...*protocol.Series) coordinator.Coordinator {
	return &MockCoordinator{
		series: series,
	}
}

// 1. initialize an engine
// 2. generate a query
// 3. issue query to engine
// 4. mock coordinator
// 5. verify that data is returned
func (self *EngineSuite) TestBasicQuery(c *C) {
	seriesName := "foo"
	c1 := "column_one"
	timestamp := time.Now().Unix()
	value := "some_value"
	var sequenceNumber uint32 = 1
	fieldType := protocol.FieldDefinition_STRING

	mockData := &protocol.Series{
		Name: &seriesName,
		Fields: []*protocol.FieldDefinition{
			&protocol.FieldDefinition{Name: &c1, Type: &fieldType},
		},
		Points: []*protocol.Point{
			&protocol.Point{
				Timestamp:      &timestamp,
				SequenceNumber: &sequenceNumber,
				Values: []*protocol.FieldValue{
					&protocol.FieldValue{
						StringValue: &value,
					},
				},
			},
		},
	}

	// make the mock coordinator return some data
	engine, err := NewQueryEngine(createMockCoordinator(mockData))
	c.Assert(err, IsNil)

	query, err := parser.ParseQuery("select * from foo;")
	c.Assert(err, IsNil)

	result := []*protocol.Series{}
	err = engine.RunQuery(query, func(series *protocol.Series) error {
		result = append(result, series)
		return nil
	})
	c.Assert(err, IsNil)

	c.Assert(result, DeepEquals, []*protocol.Series{mockData})
}

func (self *EngineSuite) TestCountQuery(c *C) {
	seriesName := "foo"
	c1 := "column_one"
	timestamp := time.Now().Unix()
	value := "some_value"
	var sequenceNumber uint32 = 1
	fieldType := protocol.FieldDefinition_STRING

	mockData := &protocol.Series{
		Name: &seriesName,
		Fields: []*protocol.FieldDefinition{
			&protocol.FieldDefinition{Name: &c1, Type: &fieldType},
		},
		Points: []*protocol.Point{
			&protocol.Point{
				Timestamp:      &timestamp,
				SequenceNumber: &sequenceNumber,
				Values: []*protocol.FieldValue{
					&protocol.FieldValue{
						StringValue: &value,
					},
				},
			},
		},
	}

	// make the mock coordinator return some data
	engine, err := NewQueryEngine(createMockCoordinator(mockData))
	c.Assert(err, IsNil)

	query, err := parser.ParseQuery("select count(*) from foo;")
	c.Assert(err, IsNil)

	result := []*protocol.Series{}
	err = engine.RunQuery(query, func(series *protocol.Series) error {
		result = append(result, series)
		return nil
	})

	c.Assert(err, IsNil)

	c.Assert(result, HasLen, 1)

	c.Assert(result[0].Points, HasLen, 1)

	expectedFieldType := protocol.FieldDefinition_INT32
	expectedName := "count"
	var expectedValue int32 = 1

	expectedData := &protocol.Series{
		Name: &seriesName,
		Fields: []*protocol.FieldDefinition{
			&protocol.FieldDefinition{Name: &expectedName, Type: &expectedFieldType},
		},
		Points: []*protocol.Point{
			&protocol.Point{
				Timestamp:      &timestamp,
				SequenceNumber: &sequenceNumber,
				Values: []*protocol.FieldValue{
					&protocol.FieldValue{
						IntValue: &expectedValue,
					},
				},
			},
		},
	}

	c.Assert(result, DeepEquals, []*protocol.Series{expectedData})
}
