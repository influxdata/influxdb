package engine

import (
	"encoding/json"
	"fmt"
	. "launchpad.net/gocheck"
	"os"
	"parser"
	"protocol"
	"reflect"
	"testing"
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

func stringToSeriesArray(seriesString string, c *C) []*protocol.Series {
	series := []*protocol.Series{}
	err := json.Unmarshal([]byte(seriesString), &series)
	c.Assert(err, IsNil)
	return series
}

func createEngine(c *C, seriesString string) EngineI {
	series := stringToSeriesArray(seriesString, c)

	engine, err := NewQueryEngine(&MockCoordinator{
		series: series,
	})
	c.Assert(err, IsNil)
	return engine
}

// runQuery() will run the given query on the engine and assert that
// the engine yields the expected time series given by expectedSeries
// in the order specified.
//
// expectedSeries must be a json array, e.g. time series must by
// inclosed in '[' and ']'
func runQuery(engine EngineI, query string, c *C, expectedSeries string) {
	q, err := parser.ParseQuery(query)
	c.Assert(err, IsNil)

	result := []*protocol.Series{}
	err = engine.RunQuery(q, func(series *protocol.Series) error {
		result = append(result, series)
		return nil
	})

	c.Assert(err, IsNil)

	series := stringToSeriesArray(expectedSeries, c)

	if !reflect.DeepEqual(result, series) {
		resultData, _ := json.MarshalIndent(result, "", "  ")
		seriesData, _ := json.MarshalIndent(series, "", "  ")

		fmt.Fprintf(os.Stderr,
			"===============\nThe two series aren't equal.\nExpected: %s\nActual: %s\n===============\n",
			seriesData, resultData)
	}

	c.Assert(result, DeepEquals, series)
}

// All tests do more or less the following steps
//
// 1. initialize an engine
// 2. generate a query
// 3. issue query to engine
// 4. mock coordinator
// 5. verify that data is returned

func (self *EngineSuite) TestBasicQuery(c *C) {
	mockData := `
[
 {
   "points": [
     {
       "values": [
         {
           "string_value": "some_value"
         }
       ],
       "timestamp": 1381346631,
       "sequence_number": 1
     }
   ],
   "name": "foo",
   "fields": [
     {
       "type": "STRING",
       "name": "column_one"
     }
   ]
 }
]
`

	// create an engine and assert the engine works as a passthrough if
	// the query only returns the raw data
	engine := createEngine(c, mockData)
	runQuery(engine, "select * from foo;", c, mockData)
}

func (self *EngineSuite) TestCountQuery(c *C) {
	// make the mock coordinator return some data
	engine := createEngine(c, `
[
  {
    "points": [
      {
        "values": [
          {
            "string_value": "some_value"
          }
        ],
        "timestamp": 1381346631,
        "sequence_number": 1
      },
      {
        "values": [
          {
            "string_value": "some_value"
          }
        ],
        "timestamp": 1381346631,
        "sequence_number": 2
      }
    ],
    "name": "foo",
    "fields": [
      {
        "type": "STRING",
        "name": "column_one"
      }
    ]
  }
]
`)

	runQuery(engine, "select count(*) from foo;", c, `[
  {
    "points": [
      {
        "values": [
          {
            "int_value": 2
          }
        ],
        "timestamp": 1381346631,
        "sequence_number": 1
      }
    ],
    "name": "foo",
    "fields": [
      {
        "type": "INT32",
        "name": "count"
      }
    ]
  }
]
`)

}

func (self *EngineSuite) TestCountQueryWithRegexTables(c *C) {
	// make the mock coordinator return some data
	engine := createEngine(c, `
[
  {
    "points": [
      {
        "values": [
          {
            "string_value": "some_value"
          }
        ],
        "timestamp": 1381346631,
        "sequence_number": 1
      }
    ],
    "name": "foo.bar",
    "fields": [
      {
        "type": "STRING",
        "name": "column_one"
      }
    ]
  },
  {
    "points": [
      {
        "values": [
          {
            "string_value": "some_value"
          }
        ],
        "timestamp": 1381346631,
        "sequence_number": 1
      }
    ],
    "name": "foo.baz",
    "fields": [
      {
        "type": "STRING",
        "name": "column_one"
      }
    ]
  }
]
`)

	runQuery(engine, "select count(*) from foo.*;", c, `[
  {
    "points": [
      {
        "values": [
          {
            "int_value": 1
          }
        ],
        "timestamp": 1381346631,
        "sequence_number": 1
      }
    ],
    "name": "foo.bar",
    "fields": [
      {
        "type": "INT32",
        "name": "count"
      }
    ]
  },
  {
    "points": [
      {
        "values": [
          {
            "int_value": 1
          }
        ],
        "timestamp": 1381346631,
        "sequence_number": 1
      }
    ],
    "name": "foo.baz",
    "fields": [
      {
        "type": "INT32",
        "name": "count"
      }
    ]
  }
]
`)

}

func (self *EngineSuite) TestCountQueryWithGroupByClause(c *C) {
	// make the mock coordinator return some data
	engine := createEngine(c, `
[
  {
    "points": [
      {
        "values": [
          {
            "string_value": "some_value"
          }
        ],
        "timestamp": 1381346631,
        "sequence_number": 1
      },
      {
        "values": [
          {
            "string_value": "another_value"
          }
        ],
        "timestamp": 1381346631,
        "sequence_number": 1
      }
    ],
    "name": "foo",
    "fields": [
      {
        "type": "STRING",
        "name": "column_one"
      }
    ]
  }
]
`)

	runQuery(engine, "select count(*), column_one from foo.* group by column_one;", c, `[
  {
    "points": [
      {
        "values": [
          {
            "int_value": 1
          },
          {
            "string_value": "some_value"
          }
        ],
        "timestamp": 1381346631,
        "sequence_number": 1
      },
			{
        "values": [
          {
            "int_value": 1
          },
          {
            "string_value": "some_value"
          }
        ],
        "timestamp": 1381346631,
        "sequence_number": 1
      }
    ],
    "name": "foo",
    "fields": [
      {
        "type": "INT32",
        "name": "count"
      },
      {
        "type": "STRING",
        "name": "column_one"
      }
    ]
  }
]
`)

}
