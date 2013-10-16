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

func (self *MockCoordinator) DistributeQuery(query *parser.Query, yield func(*protocol.Series) error) error {
	for _, series := range self.series {
		if err := yield(series); err != nil {
			return err
		}
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
func runQueryRunError(engine EngineI, query string, c *C, expectedErr error) {
	q, err := parser.ParseQuery(query)
	c.Assert(err, IsNil)

	err = engine.RunQuery(q, func(series *protocol.Series) error { return nil })

	c.Assert(err, DeepEquals, expectedErr)
}

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

	runQuery(engine, "select count(*), column_one from foo group by column_one;", c, `[
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
            "int_value": 1
          }
        ],
        "timestamp": 1381346631,
        "sequence_number": 1
      },
      {
        "values": [
          {
            "string_value": "some_value"
          },
          {
            "int_value": 2
          }
        ],
        "timestamp": 1381346631,
        "sequence_number": 1
      },
      {
        "values": [
          {
            "string_value": "another_value"
          },
          {
            "int_value": 1
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
      },
      {
        "type": "INT32",
        "name": "column_two"
      }

    ]
  }
]
`)

	runQuery(engine, "select count(*), column_one, column_two from foo group by column_one, column_two;", c, `[
  {
    "points": [
      {
        "values": [
          {
            "int_value": 1
          },
          {
            "string_value": "some_value"
          },
          {
            "int_value": 1
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
          },
          {
            "int_value": 2
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
            "string_value": "another_value"
          },
          {
            "int_value": 1
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
      },
      {
        "type": "INT32",
        "name": "column_two"
      }
    ]
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
        "timestamp": 1381346641,
        "sequence_number": 1
      },
      {
        "values": [
          {
            "string_value": "another_value"
          }
        ],
        "timestamp": 1381346701,
        "sequence_number": 1
      },
      {
        "values": [
          {
            "string_value": "some_value"
          }
        ],
        "timestamp": 1381346721,
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

	runQuery(engine, "select count(*) from foo group by time(1m);", c, `[
  {
    "points": [
      {
        "values": [
          {
            "int_value": 1
          }
        ],
        "timestamp": 1381346640,
        "sequence_number": 1
      },
      {
        "values": [
          {
            "int_value": 2
          }
        ],
        "timestamp": 1381346700,
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

func (self *EngineSuite) TestCountQueryWithGroupByTimeAndColumn(c *C) {
	engine := createEngine(c, `[
    {
      "points": [
        { "values": [{ "string_value": "some_value" }], "timestamp": 1381346641, "sequence_number": 1 },
        { "values": [{ "string_value": "another_value" }], "timestamp": 1381346701, "sequence_number": 1 },
        { "values": [{ "string_value": "some_value" }], "timestamp": 1381346721, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": [
        { "type": "STRING", "name": "column_one" }
      ]
    }
  ]`)

	runQuery(engine, "select count(*), column_one from foo group by time(1m), column_one;", c, `[
    {
      "points": [
        { "values": [{ "int_value": 1 }, { "string_value": "some_value" }], "timestamp": 1381346640, "sequence_number": 1 },
        { "values": [{ "int_value": 1 }, { "string_value": "another_value" }], "timestamp": 1381346700, "sequence_number": 1 },
        { "values": [{ "int_value": 1 }, { "string_value": "some_value" }], "timestamp": 1381346700, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": [
        { "type": "INT32", "name": "count" },
        { "type": "STRING", "name": "column_one" }
      ]
    }
  ]`)
}

func (self *EngineSuite) TestMinQueryWithGroupByTime(c *C) {
	engine := createEngine(c, `[
    {
      "points": [
        { "values": [{ "int_value": 3 }], "timestamp": 1381346641, "sequence_number": 1 },
        { "values": [{ "int_value": 8 }], "timestamp": 1381346701, "sequence_number": 1 },
        { "values": [{ "int_value": 4 }], "timestamp": 1381346721, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": [
        { "type": "INT32", "name": "column_one" }
      ]
    }
  ]`)

	runQuery(engine, "select min(column_one) from foo group by time(1m);", c, `[
    {
      "points": [
        { "values": [{ "int_value": 3 }], "timestamp": 1381346640, "sequence_number": 1 },
        { "values": [{ "int_value": 4 }], "timestamp": 1381346700, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": [
        { "type": "INT32", "name": "min" }
      ]
    }
  ]`)
}

func (self *EngineSuite) TestMaxQueryWithGroupByTime(c *C) {
	engine := createEngine(c, `[
    {
      "points": [
        { "values": [{ "int_value": 3 }], "timestamp": 1381346641, "sequence_number": 1 },
        { "values": [{ "int_value": 8 }], "timestamp": 1381346701, "sequence_number": 1 },
        { "values": [{ "int_value": 4 }], "timestamp": 1381346721, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": [
        { "type": "INT32", "name": "column_one" }
      ]
    }
  ]`)

	runQuery(engine, "select max(column_one) from foo group by time(1m);", c, `[
    {
      "points": [
        { "values": [{ "int_value": 3 }], "timestamp": 1381346640, "sequence_number": 1 },
        { "values": [{ "int_value": 8 }], "timestamp": 1381346700, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": [
        { "type": "INT32", "name": "max" }
      ]
    }
  ]`)
}

func (self *EngineSuite) TestMaxMinQueryWithGroupByTime(c *C) {
	// make the mock coordinator return some data
	engine := createEngine(c, `[
    {
      "points": [
        { "values": [{ "int_value": 3 }], "timestamp": 1381346641, "sequence_number": 1 },
        { "values": [{ "int_value": 8 }], "timestamp": 1381346701, "sequence_number": 1 },
        { "values": [{ "int_value": 4 }], "timestamp": 1381346721, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": [
        { "type": "INT32", "name": "column_one" }
      ]
    }
  ]`)

	runQuery(engine, "select max(column_one), min(column_one) from foo group by time(1m);", c, `[
    {
      "points": [
        { "values": [{ "int_value": 3 }, { "int_value": 3 }], "timestamp": 1381346640, "sequence_number": 1 },
        { "values": [{ "int_value": 8 }, { "int_value": 4 }], "timestamp": 1381346700, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": [
        { "type": "INT32", "name": "max" },
        { "type": "INT32", "name": "min" }
      ]
    }
  ]`)
}

func (self *EngineSuite) TestPercentileQueryWithGroupByTime(c *C) {
	// make the mock coordinator return some data
	engine := createEngine(c, `[
    {
      "points": [
        { "values": [{ "int_value": 1 }], "timestamp": 1381346701, "sequence_number": 1 },
        { "values": [{ "int_value": 3 }], "timestamp": 1381346701, "sequence_number": 1 },
        { "values": [{ "int_value": 5 }], "timestamp": 1381346701, "sequence_number": 1 },
        { "values": [{ "int_value": 7 }], "timestamp": 1381346701, "sequence_number": 1 },
        { "values": [{ "int_value": 4 }], "timestamp": 1381346701, "sequence_number": 1 },
        { "values": [{ "int_value": 2 }], "timestamp": 1381346701, "sequence_number": 1 },
        { "values": [{ "int_value": 6 }], "timestamp": 1381346701, "sequence_number": 1 },
        { "values": [{ "int_value": 9 }], "timestamp": 1381346741, "sequence_number": 1 },
        { "values": [{ "int_value": 8 }], "timestamp": 1381346741, "sequence_number": 1 },
        { "values": [{ "int_value": 7 }], "timestamp": 1381346741, "sequence_number": 1 },
        { "values": [{ "int_value": 6 }], "timestamp": 1381346741, "sequence_number": 1 },
        { "values": [{ "int_value": 5 }], "timestamp": 1381346741, "sequence_number": 1 },
        { "values": [{ "int_value": 4 }], "timestamp": 1381346741, "sequence_number": 1 },
        { "values": [{ "int_value": 3 }], "timestamp": 1381346741, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": [
        { "type": "INT32", "name": "column_one" }
      ]
    }
  ]`)

	runQuery(engine, "select percentile(column_one, 80) from foo group by time(1m);", c, `[
    {
      "points": [
        { "values": [{ "int_value": 6 }], "timestamp": 1381346700, "sequence_number": 1 },
        { "values": [{ "int_value": 8 }], "timestamp": 1381346760, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": [
        { "type": "INT32", "name": "percentile" }
      ]
    }
  ]`)
}

func (self *EngineSuite) TestMeanQueryWithGroupByTime(c *C) {
	// make the mock coordinator return some data
	engine := createEngine(c, `[
    {
      "points": [
        { "values": [{ "int_value": 1 }], "timestamp": 1381346701, "sequence_number": 1 },
        { "values": [{ "int_value": 3 }], "timestamp": 1381346701, "sequence_number": 1 },
        { "values": [{ "int_value": 5 }], "timestamp": 1381346701, "sequence_number": 1 },
        { "values": [{ "int_value": 7 }], "timestamp": 1381346701, "sequence_number": 1 },
        { "values": [{ "int_value": 4 }], "timestamp": 1381346701, "sequence_number": 1 },
        { "values": [{ "int_value": 2 }], "timestamp": 1381346701, "sequence_number": 1 },
        { "values": [{ "int_value": 6 }], "timestamp": 1381346701, "sequence_number": 1 },
        { "values": [{ "int_value": 9 }], "timestamp": 1381346741, "sequence_number": 1 },
        { "values": [{ "int_value": 8 }], "timestamp": 1381346741, "sequence_number": 1 },
        { "values": [{ "int_value": 7 }], "timestamp": 1381346741, "sequence_number": 1 },
        { "values": [{ "int_value": 6 }], "timestamp": 1381346741, "sequence_number": 1 },
        { "values": [{ "int_value": 5 }], "timestamp": 1381346741, "sequence_number": 1 },
        { "values": [{ "int_value": 4 }], "timestamp": 1381346741, "sequence_number": 1 },
        { "values": [{ "int_value": 3 }], "timestamp": 1381346741, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": [
        { "type": "INT32", "name": "column_one" }
      ]
    }
  ]`)

	runQuery(engine, "select mean(column_one) from foo group by time(1m);", c, `[
    {
      "points": [
        { "values": [{ "double_value": 4 }], "timestamp": 1381346700, "sequence_number": 1 },
        { "values": [{ "double_value": 6 }], "timestamp": 1381346760, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": [
        { "type": "DOUBLE", "name": "mean" }
      ]
    }
  ]`)
}

func (self *EngineSuite) TestCountQueryWithGroupByTimeInvalidNumberOfArguments(c *C) {
	err := common.NewQueryError(common.WrongNumberOfArguments, "time function only accepts one argument")
	engine := createEngine(c, `[]`)
	runQueryRunError(engine, "select count(*) from foo group by time(1h, 1m);", c, err)
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
