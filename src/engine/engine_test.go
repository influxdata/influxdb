package engine

import (
	"common"
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

func (self *MockCoordinator) DistributeQuery(user common.User, database string, query *parser.Query, yield func(*protocol.Series) error) error {
	for _, series := range self.series {
		if err := yield(series); err != nil {
			return err
		}
	}
	return nil
}

func (self *MockCoordinator) WriteSeriesData(user common.User, database string, series *protocol.Series) error {
	return nil
}

func (self *MockCoordinator) CreateDatabase(user common.User, db string) error {
	return nil
}

func (self *MockCoordinator) DropDatabase(user common.User, db string) error {
	return nil
}

func (self *MockCoordinator) ListDatabases(user common.User) ([]string, error) {
	return nil, nil
}

func createEngine(c *C, seriesString string) EngineI {
	series, err := common.StringToSeriesArray(seriesString)
	c.Assert(err, IsNil)

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
func runQueryRunError(engine EngineI, query string, c *C, expectedErr error) {
	err := engine.RunQuery(nil, "", query, func(series *protocol.Series) error { return nil })

	c.Assert(err, DeepEquals, expectedErr)
}

func runQuery(engine EngineI, query string, c *C, expectedSeries string) {
	runQueryExtended(engine, query, c, false, expectedSeries)
}

func runQueryExtended(engine EngineI, query string, c *C, appendPoints bool, expectedSeries string) {

	var result []*protocol.Series
	err := engine.RunQuery(nil, "", query, func(series *protocol.Series) error {
		if appendPoints && result != nil {
			result[0].Points = append(result[0].Points, series.Points...)
		} else {
			result = append(result, series)
		}
		return nil
	})

	c.Assert(err, IsNil)

	series, err := common.StringToSeriesArray(expectedSeries)
	c.Assert(err, IsNil)

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
       "timestamp": 1381346631000000,
       "sequence_number": 1
     }
   ],
   "name": "foo",
   "fields": ["column_one"]
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
        "timestamp": 1381346631000000,
        "sequence_number": 1
      },
      {
        "values": [
          {
            "string_value": "some_value"
          }
        ],
        "timestamp": 1381346631000000,
        "sequence_number": 2
      }
    ],
    "name": "foo",
    "fields": ["column_one"]
  }
]
`)

	runQuery(engine, "select count(column_one) from foo;", c, `[
  {
    "points": [
      {
        "values": [
          {
            "int64_value": 2
          }
        ],
        "timestamp": 1381346631000000,
        "sequence_number": 1
      }
    ],
    "name": "foo",
    "fields": ["count"]
  }
]
`)

}

func (self *EngineSuite) TestUpperCaseQuery(c *C) {
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
        "timestamp": 1381346631000000,
        "sequence_number": 1
      },
      {
        "values": [
          {
            "string_value": "some_value"
          }
        ],
        "timestamp": 1381346631000000,
        "sequence_number": 2
      }
    ],
    "name": "foo",
    "fields": ["column_one"]
  }
]
`)

	runQuery(engine, "select COUNT(column_one) from foo;", c, `[
  {
    "points": [
      {
        "values": [
          {
            "int64_value": 2
          }
        ],
        "timestamp": 1381346631000000,
        "sequence_number": 1
      }
    ],
    "name": "foo",
    "fields": ["count"]
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
        "timestamp": 1381346631000000,
        "sequence_number": 1
      }
    ],
    "name": "foo.bar",
    "fields": ["column_one"]
  },
  {
    "points": [
      {
        "values": [
          {
            "string_value": "some_value"
          }
        ],
        "timestamp": 1381346631000000,
        "sequence_number": 1
      }
    ],
    "name": "foo.baz",
    "fields": ["column_one"]
  }
]
`)

	runQuery(engine, "select count(column_one) from /foo.*/;", c, `[
  {
    "points": [
      {
        "values": [
          {
            "int64_value": 1
          }
        ],
        "timestamp": 1381346631000000,
        "sequence_number": 1
      }
    ],
    "name": "foo.bar",
    "fields": ["count"]
  },
  {
    "points": [
      {
        "values": [
          {
            "int64_value": 1
          }
        ],
        "timestamp": 1381346631000000,
        "sequence_number": 1
      }
    ],
    "name": "foo.baz",
    "fields": ["count"]
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
        "timestamp": 1381346631000000,
        "sequence_number": 1
      },
      {
        "values": [
          {
            "string_value": "another_value"
          }
        ],
        "timestamp": 1381346631000000,
        "sequence_number": 1
      }
    ],
    "name": "foo",
    "fields": ["column_one"]
  }
]
`)

	runQuery(engine, "select count(column_one), column_one from foo group by column_one;", c, `[
  {
    "points": [
      {
        "values": [
          {
            "int64_value": 1
          },
          {
            "string_value": "some_value"
          }
        ],
        "timestamp": 1381346631000000,
        "sequence_number": 1
      },
      {
        "values": [
          {
            "int64_value": 1
          },
          {
            "string_value": "another_value"
          }
        ],
        "timestamp": 1381346631000000,
        "sequence_number": 1
      }
    ],
    "name": "foo",
    "fields": ["count", "column_one"]
  }
]
`)

}

func (self *EngineSuite) BenchmarkFoo(c *C) {
	counts := map[interface{}]int{}
	for i := 0; i < c.N; i++ {
		foo := [4]interface{}{i % 10, i % 100, i % 20, i % 50}
		counts[foo]++
	}

	fmt.Printf("count: %d\n", counts[[4]interface{}{0, 0, 0, 0}])
}

func (self *EngineSuite) BenchmarkStringFoo(c *C) {
	counts := map[string]int{}
	for i := 0; i < c.N; i++ {
		foo := fmt.Sprintf("%d|%d|%d|%d", i%10, i%100, i%20, i%50)
		counts[foo]++
	}

	fmt.Printf("count: %d\n", counts["0|0|0|0"])
}

func (self *EngineSuite) TestCountQueryWithGroupByClauseWithMultipleColumns(c *C) {
	// make the mock coordinator return some data
	engine := createEngine(c, `
[
  {
    "points": [
      {
        "values": [
          {
            "string_value": "some_value"
          },
          {
            "int64_value": 1
          }
        ],
        "timestamp": 1381346631000000,
        "sequence_number": 1
      },
      {
        "values": [
          {
            "string_value": "some_value"
          },
          {
            "int64_value": 2
          }
        ],
        "timestamp": 1381346631000000,
        "sequence_number": 1
      },
      {
        "values": [
          {
            "string_value": "another_value"
          },
          {
            "int64_value": 1
          }
        ],
        "timestamp": 1381346631000000,
        "sequence_number": 1
      }
    ],
    "name": "foo",
    "fields": ["column_one", "column_two"]
  }
]
`)

	runQuery(engine, "select count(column_one), column_one, column_two from foo group by column_one, column_two;", c, `[
  {
    "points": [
      {
        "values": [
          {
            "int64_value": 1
          },
          {
            "string_value": "some_value"
          },
          {
            "int64_value": 1
          }
        ],
        "timestamp": 1381346631000000,
        "sequence_number": 1
      },
      {
        "values": [
          {
            "int64_value": 1
          },
          {
            "string_value": "some_value"
          },
          {
            "int64_value": 2
          }
        ],
        "timestamp": 1381346631000000,
        "sequence_number": 1
      },
			{
        "values": [
          {
            "int64_value": 1
          },
          {
            "string_value": "another_value"
          },
          {
            "int64_value": 1
          }
        ],
        "timestamp": 1381346631000000,
        "sequence_number": 1
      }
    ],
    "name": "foo",
    "fields": ["count", "column_one", "column_two"]
  }
]
`)
}

func (self *EngineSuite) TestCountQueryWithGroupByTime(c *C) {
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
        "timestamp": 1381346641000000,
        "sequence_number": 1
      },
      {
        "values": [
          {
            "string_value": "another_value"
          }
        ],
        "timestamp": 1381346701000000,
        "sequence_number": 1
      },
      {
        "values": [
          {
            "string_value": "some_value"
          }
        ],
        "timestamp": 1381346721000000,
        "sequence_number": 1
      }
    ],
    "name": "foo",
    "fields": ["column_one"]
  }
]
`)

	runQuery(engine, "select count(column_one) from foo group by time(1m);", c, `[
  {
    "points": [
      {
        "values": [
          {
            "int64_value": 1
          }
        ],
        "timestamp": 1381346640000000,
        "sequence_number": 1
      },
      {
        "values": [
          {
            "int64_value": 2
          }
        ],
        "timestamp": 1381346700000000,
        "sequence_number": 1
      }
    ],
    "name": "foo",
    "fields": ["count"]
  }
]
`)
}

func (self *EngineSuite) TestCountQueryWithGroupByTimeAndColumn(c *C) {
	engine := createEngine(c, `[
    {
      "points": [
        { "values": [{ "string_value": "some_value" }], "timestamp": 1381346641000000, "sequence_number": 1 },
        { "values": [{ "string_value": "another_value" }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "string_value": "some_value" }], "timestamp": 1381346721000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	runQuery(engine, "select count(column_one), column_one from foo group by time(1m), column_one;", c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }, { "string_value": "some_value" }], "timestamp": 1381346640000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 1 }, { "string_value": "another_value" }], "timestamp": 1381346700000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 1 }, { "string_value": "some_value" }], "timestamp": 1381346700000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["count" , "column_one"]
    }
  ]`)
}

func (self *EngineSuite) TestMinQueryWithGroupByTime(c *C) {
	engine := createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346641000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 8 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346721000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	runQuery(engine, "select min(column_one) from foo group by time(1m);", c, `[
    {
      "points": [
        { "values": [{ "double_value": 3 }], "timestamp": 1381346640000000, "sequence_number": 1 },
        { "values": [{ "double_value": 4 }], "timestamp": 1381346700000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["min"]
    }
  ]`)
}

func (self *EngineSuite) TestMaxQueryWithGroupByTime(c *C) {
	engine := createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346641000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 8 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346721000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	runQuery(engine, "select max(column_one) from foo group by time(1m);", c, `[
    {
      "points": [
        { "values": [{ "double_value": 3 }], "timestamp": 1381346640000000, "sequence_number": 1 },
        { "values": [{ "double_value": 8 }], "timestamp": 1381346700000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["max"]
    }
  ]`)
}

func (self *EngineSuite) TestMaxMinQueryWithGroupByTime(c *C) {
	// make the mock coordinator return some data
	engine := createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346641000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 8 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346721000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	runQuery(engine, "select max(column_one), min(column_one) from foo group by time(1m);", c, `[
    {
      "points": [
        { "values": [{ "double_value": 3 }, { "double_value": 3 }], "timestamp": 1381346640000000, "sequence_number": 1 },
        { "values": [{ "double_value": 8 }, { "double_value": 4 }], "timestamp": 1381346700000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["max", "min"]
    }
  ]`)
}

func (self *EngineSuite) TestPercentileQueryWithGroupByTime(c *C) {
	// make the mock coordinator return some data
	engine := createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 5 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 7 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 2 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 9 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 8 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 7 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 5 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346771000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	runQuery(engine, "select percentile(column_one, 80) from foo group by time(1m);", c, `[
    {
      "points": [
        { "values": [{ "double_value": 6 }], "timestamp": 1381346700000000, "sequence_number": 1 },
        { "values": [{ "double_value": 8 }], "timestamp": 1381346760000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["percentile"]
    }
  ]`)
}

func (self *EngineSuite) TestCountDistinct(c *C) {
	// make the mock coordinator return some data
	engine := createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 5 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 7 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 2 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 9 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 8 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 7 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 5 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346771000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	runQuery(engine, "select count(distinct(column_one)) from foo;", c, `[
    {
      "points": [
        { "values": [{ "int64_value": 9 }], "timestamp": 1381346771000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["count"]
    }
  ]`)
}

func (self *EngineSuite) TestMedianQueryWithGroupByTime(c *C) {
	engine := createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 5 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 7 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 2 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 9 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 8 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 7 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 5 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346771000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	runQuery(engine, "select median(column_one) from foo group by time(1m);", c, `[
    {
      "points": [
        { "values": [{ "double_value": 4 }], "timestamp": 1381346700000000, "sequence_number": 1 },
        { "values": [{ "double_value": 6 }], "timestamp": 1381346760000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["median"]
    }
  ]`)
}

func (self *EngineSuite) TestMeanQueryWithGroupByTime(c *C) {
	engine := createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 5 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 7 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 2 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 9 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 8 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 7 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 5 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346771000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	runQuery(engine, "select mean(column_one) from foo group by time(1m);", c, `[
    {
      "points": [
        { "values": [{ "double_value": 4 }], "timestamp": 1381346700000000, "sequence_number": 1 },
        { "values": [{ "double_value": 6 }], "timestamp": 1381346760000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["mean"]
    }
  ]`)
}

func (self *EngineSuite) TestDerivativeQuery(c *C) {
	engine := createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381347701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 2 }], "timestamp": 1381347702000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381347703000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381347704000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	runQuery(engine, "select derivative(column_one) from foo;", c, `[
    {
      "points": [
        { "values": [{ "double_value": 1 } ], "timestamp": 1381347704000000, "sequence_number": 1 },
        { "values": [{ "double_value": 4 } ], "timestamp": 1381347704000000, "sequence_number": 1 },
        { "values": [{ "double_value": -2 }], "timestamp": 1381347704000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["derivative"]
    }
  ]`)
}

func (self *EngineSuite) TestDistinctQuery(c *C) {
	engine := createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381347701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 2 }], "timestamp": 1381347702000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381347703000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 2 }], "timestamp": 1381347704000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	runQuery(engine, "select distinct(column_one) from foo;", c, `[
    {
      "points": [
        { "values": [{ "double_value": 1 }], "timestamp": 1381347704000000, "sequence_number": 1 },
        { "values": [{ "double_value": 2 }], "timestamp": 1381347704000000, "sequence_number": 1 },
        { "values": [{ "double_value": 6 }], "timestamp": 1381347704000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["distinct"]
    }
  ]`)
}

func (self *EngineSuite) TestSumQueryWithGroupByTime(c *C) {
	engine := createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 8 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 5 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346771000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	runQuery(engine, "select sum(column_one) from foo group by time(1m);", c, `[
    {
      "points": [
        { "values": [{ "double_value": 11 }], "timestamp": 1381346700000000, "sequence_number": 1 },
        { "values": [{ "double_value": 16 }], "timestamp": 1381346760000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["sum"]
    }
  ]`)
}

func (self *EngineSuite) TestModeQueryWithGroupByTime(c *C) {
	engine := createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 5 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 8 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 7 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 5 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346771000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346771000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	runQuery(engine, "select mode(column_one) from foo group by time(1m);", c, `[
    {
      "points": [
        { "values": [{ "double_value": 1 }], "timestamp": 1381346700000000, "sequence_number": 1 },
        { "values": [{ "double_value": 3 }], "timestamp": 1381346760000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["mode"]
    }
  ]`)
}

func (self *EngineSuite) TestQueryWithMergedTables(c *C) {
	engine := createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["value"]
    },
    {
      "points": [
        { "values": [{ "int64_value": 2 }], "timestamp": 1381346705000000, "sequence_number": 1 }
      ],
      "name": "bar",
      "fields": ["value"]
    },
    {
      "points": [
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346707000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["value"]
    },
    {
      "points": [
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346706000000, "sequence_number": 1 }
      ],
      "name": "bar",
      "fields": ["value"]
    },
    {
      "points": [],
      "name": "foo",
      "fields": ["value"]
    },
    {
      "points": [],
      "name": "bar",
      "fields": ["value"]
    }
  ]`)

	runQuery(engine, "select * from foo merge bar;", c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }, {"string_value": "foo"}], "timestamp": 1381346701000000, "sequence_number": 1 }
      ],
      "name": "foo_merge_bar",
      "fields": ["value", "_orig_series"]
    },
    {
      "points": [
        { "values": [{ "int64_value": 2 }, {"string_value": "bar"}], "timestamp": 1381346705000000, "sequence_number": 1 }
      ],
      "name": "foo_merge_bar",
      "fields": ["value", "_orig_series"]
    },
    {
      "points": [
        { "values": [{ "int64_value": 4 }, {"string_value": "bar"}], "timestamp": 1381346706000000, "sequence_number": 1 }
      ],
      "name": "foo_merge_bar",
      "fields": ["value", "_orig_series"]
    },
    {
      "points": [
        { "values": [{ "int64_value": 3 }, {"string_value": "foo"}], "timestamp": 1381346707000000, "sequence_number": 1 }
      ],
      "name": "foo_merge_bar",
      "fields": ["value", "_orig_series"]
    }
  ]`)
}

func (self *EngineSuite) TestQueryWithJoinedTables(c *C) {
	engine := createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["value"]
    },
    {
      "points": [
        { "values": [{ "int64_value": 2 }], "timestamp": 1381346705000000, "sequence_number": 1 }
      ],
      "name": "bar",
      "fields": ["value"]
    },
    {
      "points": [
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346706000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["value"]
    },
    {
      "points": [
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346707000000, "sequence_number": 1 }
      ],
      "name": "bar",
      "fields": ["value"]
    },
    {
      "points": [],
      "name": "foo",
      "fields": ["value"]
    },
    {
      "points": [],
      "name": "bar",
      "fields": ["value"]
    }
  ]`)

	runQuery(engine, "select * from foo inner join bar;", c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }, { "int64_value": 2 }], "timestamp": 1381346705000000, "sequence_number": 1 }
      ],
      "name": "foo_join_bar",
      "fields": ["foo.value", "bar.value"]
    },
    {
      "points": [
        { "values": [{ "int64_value": 3 }, { "int64_value": 4 }], "timestamp": 1381346707000000, "sequence_number": 1 }
      ],
      "name": "foo_join_bar",
      "fields": ["foo.value", "bar.value"]
    }
  ]`)
}

func (self *EngineSuite) TestQueryWithMergedTablesWithPointsAppend(c *C) {
	engine := createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }, { "int64_value": 1 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 1 }, { "int64_value": 4 }], "timestamp": 1381346707000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["a", "b"]
    },
    {
      "points": [],
      "name": "foo",
      "fields": ["a", "b"]
    },
    {
      "points": [
        { "values": [{ "int64_value": 1 }, { "int64_value": 2 }], "timestamp": 1381346705000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 1 }, { "int64_value": 3 }], "timestamp": 1381346706000000, "sequence_number": 1 }
      ],
      "name": "bar",
      "fields": ["a", "b"]
    },
    {
      "points": [],
      "name": "bar",
      "fields": ["a", "b"]
    }
  ]`)

	runQueryExtended(engine, "select * from foo merge bar;", c, true, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }, { "int64_value": 1 }, {"string_value": "foo"}], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 1 }, { "int64_value": 2 }, {"string_value": "bar"}], "timestamp": 1381346705000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 1 }, { "int64_value": 3 }, {"string_value": "bar"}], "timestamp": 1381346706000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 1 }, { "int64_value": 4 }, {"string_value": "foo"}], "timestamp": 1381346707000000, "sequence_number": 1 }
      ],
      "name": "foo_merge_bar",
      "fields": ["a", "b", "_orig_series"]
    }
  ]`)
}

func (self *EngineSuite) TestCountQueryWithGroupByTimeInvalidNumberOfArguments(c *C) {
	err := common.NewQueryError(common.WrongNumberOfArguments, "time function only accepts one argument")
	engine := createEngine(c, `[]`)
	runQueryRunError(engine, "select count(*) from foo group by time(1h, 1m);", c, err)
}

func (self *EngineSuite) TestCountQueryWithInvalidWildcardArgument(c *C) {
	err := common.NewQueryError(common.InvalidArgument, "function count() doesn't work with wildcards")
	engine := createEngine(c, `[]`)
	runQueryRunError(engine, "select count(*) from foo;", c, err)
}

func (self *EngineSuite) TestCountQueryWithGroupByTimeInvalidArgument(c *C) {
	err := common.NewQueryError(common.InvalidArgument, "invalid argument foobar to the time function")
	engine := createEngine(c, `[]`)
	runQueryRunError(engine, "select count(*) from foo group by time(foobar);", c, err)
}

func (self *EngineSuite) TestPercentileQueryWithInvalidNumberOfArguments(c *C) {
	err := common.NewQueryError(common.WrongNumberOfArguments, "function percentile() requires exactly two arguments")
	engine := createEngine(c, `[]`)
	runQueryRunError(engine, "select percentile(95) from foo group by time(1m);", c, err)
}

func (self *EngineSuite) TestPercentileQueryWithNonNumericArguments(c *C) {
	err := common.NewQueryError(common.InvalidArgument, "function percentile() requires a numeric second argument between 0 and 100")
	engine := createEngine(c, `[]`)
	runQueryRunError(engine, "select percentile(column_one, a95) from foo group by time(1m);", c, err)
}

func (self *EngineSuite) TestPercentileQueryWithOutOfBoundNumericArguments(c *C) {
	err := common.NewQueryError(common.InvalidArgument, "function percentile() requires a numeric second argument between 0 and 100")
	engine := createEngine(c, `[]`)
	runQueryRunError(engine, "select percentile(column_one, 0) from foo group by time(1m);", c, err)
	runQueryRunError(engine, "select percentile(column_one, 105) from foo group by time(1m);", c, err)
}
