package engine

import (
	. "checkers"
	"common"
	"coordinator"
	"encoding/json"
	"fmt"
	. "launchpad.net/gocheck"
	"os"
	"parser"
	"protocol"
	"reflect"
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
	coordinator.Coordinator
	returnedError error
	series        []*protocol.Series
}

func (self *MockCoordinator) DistributeQuery(user common.User, database string, query *parser.SelectQuery, localOnly bool, yield func(*protocol.Series) error) error {
	if self.returnedError != nil {
		return self.returnedError
	}

	for _, series := range self.series {
		if err := yield(series); err != nil {
			return err
		}
	}
	return nil
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
// enclosed in '[' and ']'
func runQueryRunError(engine EngineI, query string, c *C, expectedErr error) {
	err := engine.RunQuery(nil, "", query, false, func(series *protocol.Series) error { return nil })

	c.Assert(err, DeepEquals, expectedErr)
}

func runQuery(engine EngineI, query string, c *C, expectedSeries string) {
	runQueryExtended(engine, query, c, false, expectedSeries)
}

func runQueryExtended(engine EngineI, query string, c *C, appendPoints bool, expectedSeries string) {
	series, err := common.StringToSeriesArray(expectedSeries)
	c.Assert(err, IsNil)

	result := runQueryWithoutChecking(engine, query, c, appendPoints)

	if !reflect.DeepEqual(result, series) {
		resultData, _ := json.MarshalIndent(result, "", "  ")
		seriesData, _ := json.MarshalIndent(series, "", "  ")

		fmt.Fprintf(os.Stderr,
			"===============\nThe two series aren't equal.\nExpected: %s\nActual: %s\n===============\n",
			seriesData, resultData)
	}

	c.Assert(result, SeriesEquals, series)
}

func runQueryWithoutChecking(engine EngineI, query string, c *C, appendPoints bool) []*protocol.Series {
	var result []*protocol.Series
	err := engine.RunQuery(nil, "", query, false, func(series *protocol.Series) error {
		if appendPoints && result != nil {
			result[0].Points = append(result[0].Points, series.Points...)
		} else {
			result = append(result, series)
		}
		return nil
	})

	c.Assert(err, IsNil)
	return result
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
	runQuery(engine, "select * from foo order asc", c, mockData)
}

func (self *EngineSuite) TestBasicQueryError(c *C) {
	// create an engine and assert the engine works as a passthrough if
	// the query only returns the raw data
	engine := createEngine(c, "[]")
	engine.(*QueryEngine).coordinator.(*MockCoordinator).returnedError = fmt.Errorf("some error")
	err := engine.RunQuery(nil, "", "select * from foo", false, func(series *protocol.Series) error {
		return nil
	})

	c.Assert(err, ErrorMatches, "some error")
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
        "timestamp": 1381346631000000
      },
      {
        "values": [
          {
            "string_value": "some_value"
          }
        ],
        "timestamp": 1381346631000000
      }
    ],
    "name": "foo",
    "fields": ["column_one"]
  }
]
`)

	runQuery(engine, "select count(column_one) from foo order asc", c, `[
  {
    "points": [
      {
        "values": [
          {
            "int64_value": 2
          }
        ],
        "timestamp": 1381346631000000
      }
    ],
    "name": "foo",
    "fields": ["count"]
  }
]
`)

}

func (self *EngineSuite) TestFirstAndLastQuery(c *C) {
	// make the mock coordinator return some data
	engine := createEngine(c, `
[
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
      },
      {
        "values": [
          {
            "int64_value": 2
          }
        ],
        "timestamp": 1381346631000000,
        "sequence_number": 2
      },
      {
        "values": [
          {
            "int64_value": 3
          }
        ],
        "timestamp": 1381346631000000,
        "sequence_number": 3
      }
    ],
    "name": "foo",
    "fields": ["column_one"]
  }
]
`)

	runQuery(engine, "select first(column_one), last(column_one) from foo", c, `[
  {
    "points": [
      {
        "values": [
          {
            "int64_value": 1
          },
          {
            "int64_value": 3
          }
        ],
        "timestamp": 1381346631000000
      }
    ],
    "name": "foo",
    "fields": ["first", "last"]
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

	runQuery(engine, "select COUNT(column_one) from foo order asc", c, `[
  {
    "points": [
      {
        "values": [
          {
            "int64_value": 2
          }
        ],
        "timestamp": 1381346631000000
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

	runQuery(engine, "select count(column_one) from /foo.*/ order asc", c, `[
  {
    "points": [
      {
        "values": [
          {
            "int64_value": 1
          }
        ],
        "timestamp": 1381346631000000
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
        "timestamp": 1381346631000000
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

	runQuery(engine, "select count(column_one), column_one from foo group by column_one order asc", c, `[
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
        "timestamp": 1381346631000000
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
        "timestamp": 1381346631000000
      }
    ],
    "name": "foo",
    "fields": ["count", "column_one"]
  }
]
`)

}

// issue #27
func (self *EngineSuite) TestCountQueryWithGroupByClauseAndNullValues(c *C) {
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
            "double_value": 1.0
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
            "double_value": 2.0
				  }
        ],
        "timestamp": 1381346631000000,
        "sequence_number": 1
      },
      {
        "values": [
				  {
						"is_null": true
					},
          {
            "double_value": 3.0
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

	runQuery(engine, "select count(column_two), column_one from foo group by column_one order asc", c, `[
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
        "timestamp": 1381346631000000
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
        "timestamp": 1381346631000000
      },
      {
        "values": [
          {
            "int64_value": 1
          },
				  {
						"is_null": true
					}
        ],
        "timestamp": 1381346631000000
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

	runQuery(engine, "select count(column_one), column_one, column_two from foo group by column_one, column_two order asc", c, `[
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
        "timestamp": 1381346631000000
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
        "timestamp": 1381346631000000
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
        "timestamp": 1381346631000000
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

	runQuery(engine, "select count(column_one) from foo group by time(1m) order asc", c, `[
  {
    "points": [
      {
        "values": [
          {
            "int64_value": 1
          }
        ],
        "timestamp": 1381346640000000
      },
      {
        "values": [
          {
            "int64_value": 2
          }
        ],
        "timestamp": 1381346700000000
      }
    ],
    "name": "foo",
    "fields": ["count"]
  }
]
`)
}

func (self *EngineSuite) TestCountQueryWithGroupByTimeDescendingOrder(c *C) {
	points := `
[
  {
    "points": [
`

	expectedResponse := `
[
  {
    "points": [
`
	endTime := time.Now().Round(time.Hour)

	for i := 0; i < 3600; i++ {
		delimiter := ","
		if i == 3599 {
			delimiter = ""
		}

		points += fmt.Sprintf(`
      {
        "values": [
          {
            "string_value": "some_value"
          }
        ],
        "timestamp": %d,
        "sequence_number": 1
      }%s
`, endTime.Add(time.Duration(-i)*time.Second).Unix()*1000000, delimiter)

		expectedResponse += fmt.Sprintf(`
      {
        "values": [
          {
            "int64_value": 1
          }
        ],
        "timestamp": %d
      }%s
`, endTime.Add(time.Duration(-i)*time.Second).Unix()*1000000, delimiter)
	}

	points += `
    ],
    "name": "foo",
    "fields": ["count"]
  }
]
`

	expectedResponse += `
    ],
    "name": "foo",
    "fields": ["count"]
  }
]
`

	// make the mock coordinator return some data
	engine := createEngine(c, points)

	runQuery(engine, "select count(column_one) from foo group by time(1s);", c, expectedResponse)
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

	runQuery(engine, "select count(column_one), column_one from foo group by time(1m), column_one order asc", c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }, { "string_value": "some_value" }], "timestamp": 1381346640000000   },
        { "values": [{ "int64_value": 1 }, { "string_value": "another_value" }], "timestamp": 1381346700000000},
        { "values": [{ "int64_value": 1 }, { "string_value": "some_value" }], "timestamp": 1381346700000000   }
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

	runQuery(engine, "select min(column_one) from foo group by time(1m) order asc", c, `[
    {
      "points": [
        { "values": [{ "double_value": 3 }], "timestamp": 1381346640000000},
        { "values": [{ "double_value": 4 }], "timestamp": 1381346700000000}
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

	runQuery(engine, "select max(column_one) from foo group by time(1m) order asc", c, `[
    {
      "points": [
        { "values": [{ "double_value": 3 }], "timestamp": 1381346640000000},
        { "values": [{ "double_value": 8 }], "timestamp": 1381346700000000}
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

	runQuery(engine, "select max(column_one), min(column_one) from foo group by time(1m) order asc", c, `[
    {
      "points": [
        { "values": [{ "double_value": 3 }, { "double_value": 3 }], "timestamp": 1381346640000000},
        { "values": [{ "double_value": 8 }, { "double_value": 4 }], "timestamp": 1381346700000000}
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

	runQuery(engine, "select percentile(column_one, 80) from foo group by time(1m) order asc", c, `[
    {
      "points": [
        { "values": [{ "double_value": 6 }], "timestamp": 1381346700000000},
        { "values": [{ "double_value": 8 }], "timestamp": 1381346760000000}
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

	runQuery(engine, "select count(distinct(column_one)) from foo order asc", c, `[
    {
      "points": [
        { "values": [{ "int64_value": 9 }], "timestamp": 1381346771000000}
      ],
      "name": "foo",
      "fields": ["count"]
    }
  ]`)
}

func (self *EngineSuite) TestEmptyGroups(c *C) {
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
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346871000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 5 }], "timestamp": 1381346871000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346871000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346871000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	for _, query := range []string{
		"select count(column_one) from foo where time > 1381346701s and time < 1381346872s group by time(1m) fill(0) order asc",
		"select count(column_one) from foo group by time(1m) fill(0) order asc",
	} {
		runQuery(engine, query, c, `[
    {
      "points": [
        { "values": [{ "int64_value": 7 }], "timestamp": 1381346700000000},
        { "values": [{ "int64_value": 0 }], "timestamp": 1381346760000000},
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346820000000}
      ],
      "name": "foo",
      "fields": ["count"]
    }
  ]`)
	}
}

func (self *EngineSuite) TestEmptyGroupsWithNonZeroDefault(c *C) {
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
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346871000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 5 }], "timestamp": 1381346871000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346871000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346871000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	for _, query := range []string{
		"select count(column_one) from foo where time > 1381346701s and time < 1381346872s group by time(1m) fill(10) order asc",
		"select count(column_one) from foo group by time(1m) fill(10) order asc",
	} {
		runQuery(engine, query, c, `[
    {
      "points": [
        { "values": [{ "int64_value": 7 }], "timestamp": 1381346700000000},
        { "values": [{ "int64_value": 10}], "timestamp": 1381346760000000},
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346820000000}
      ],
      "name": "foo",
      "fields": ["count"]
    }
  ]`)
	}
}

func (self *EngineSuite) TestEmptyGroupsWithoutTime(c *C) {
	engine := createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346871000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346871000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346871000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346871000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	for _, query := range []string{
		"select count(column_one) from foo where time > 1381346701s and time < 1381346872s group by column_one fill(0) order asc",
		"select count(column_one) from foo group by column_one fill(0) order asc",
	} {
		runQuery(engine, query, c, `[
    {
      "points": [
        { "values": [{ "int64_value": 5}, { "int64_value": 1 }], "timestamp": 1381346871000000},
        { "values": [{ "int64_value": 6}, { "int64_value": 3 }], "timestamp": 1381346871000000}
      ],
      "name": "foo",
      "fields": ["count", "column_one"]
    }
  ]`)
	}
}
func (self *EngineSuite) TestEmptyGroupsWithMultipleColumns(c *C) {
	engine := createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346871000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346871000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346871000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346871000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	for _, query := range []string{
		"select count(column_one) from foo where time > 1381346701s and time < 1381346872s group by time(1m), column_one fill(0) order asc",
		"select count(column_one) from foo group by time(1m), column_one fill(0) order asc",
	} {
		runQuery(engine, query, c, `[
    {
      "points": [
        { "values": [{ "int64_value": 2}, { "int64_value": 1 }], "timestamp": 1381346700000000},
        { "values": [{ "int64_value": 5}, { "int64_value": 3 }], "timestamp": 1381346700000000},
        { "values": [{ "int64_value": 0}, { "int64_value": 1 }], "timestamp": 1381346760000000},
        { "values": [{ "int64_value": 0}, { "int64_value": 3 }], "timestamp": 1381346760000000},
        { "values": [{ "int64_value": 3}, { "int64_value": 1 }], "timestamp": 1381346820000000},
        { "values": [{ "int64_value": 1}, { "int64_value": 3 }], "timestamp": 1381346820000000}
      ],
      "name": "foo",
      "fields": ["count", "column_one"]
    }
  ]`)
	}
}

func (self *EngineSuite) TestEmptyGroupsDescending(c *C) {
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
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346871000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 5 }], "timestamp": 1381346871000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346871000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346871000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	for _, query := range []string{
		"select count(column_one) from foo where time > 1381346701s and time < 1381346872s group by time(1m) fill(0)",
		"select count(column_one) from foo group by time(1m) fill(0)",
	} {
		runQuery(engine, query, c, `[
    {
      "points": [
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346820000000},
        { "values": [{ "int64_value": 0 }], "timestamp": 1381346760000000},
        { "values": [{ "int64_value": 7 }], "timestamp": 1381346700000000}
      ],
      "name": "foo",
      "fields": ["count"]
    }
  ]`)
	}
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

	runQuery(engine, "select median(column_one) from foo group by time(1m) order asc", c, `[
    {
      "points": [
        { "values": [{ "double_value": 4 }], "timestamp": 1381346700000000},
        { "values": [{ "double_value": 6 }], "timestamp": 1381346760000000}
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

	runQuery(engine, "select mean(column_one) from foo group by time(1m) order asc", c, `[
    {
      "points": [
        { "values": [{ "double_value": 4 }], "timestamp": 1381346700000000},
        { "values": [{ "double_value": 6 }], "timestamp": 1381346760000000}
      ],
      "name": "foo",
      "fields": ["mean"]
    }
  ]`)
}

func (self *EngineSuite) TestStddevQuery(c *C) {
	engine := createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381347700000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381347700500000, "sequence_number": 1 },
        { "values": [{ "int64_value": 2 }], "timestamp": 1381347701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381347702000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381347703000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	result := runQueryWithoutChecking(engine, "select stddev(column_one) from foo group by time(2s) order asc", c, false)
	c.Assert(result, HasLen, 1)
	c.Assert(*result[0].Name, Equals, "foo")
	c.Assert(result[0].Fields, DeepEquals, []string{"stddev"})
	c.Assert(result[0].Points, HasLen, 2)
	c.Assert(*result[0].Points[0].Values[0].DoubleValue, InRange, 0.4714, 0.4715)
	c.Assert(*result[0].Points[1].Values[0].DoubleValue, InRange, 0.9999, 1.0001)
}

func (self *EngineSuite) TestDerivativeQuery(c *C) {
	engine := createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381347700000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381347700500000, "sequence_number": 1 },
        { "values": [{ "int64_value": 2 }], "timestamp": 1381347701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381347702000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381347703000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	runQuery(engine, "select derivative(column_one) from foo group by time(2s) order asc", c, `[
    {
      "points": [
        { "values": [{ "double_value": 1 } ], "timestamp": 1381347700000000},
        { "values": [{ "double_value": -2 }], "timestamp": 1381347702000000}
      ],
      "name": "foo",
      "fields": ["derivative"]
    }
  ]`)
}

func (self *EngineSuite) TestDerivativeQueryWithOnePoint(c *C) {
	engine := createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381347700000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	runQuery(engine, "select derivative(column_one) from foo", c, `[
    {
      "points": [],
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

	runQuery(engine, "select distinct(column_one) from foo order asc", c, `[
    {
      "points": [
        { "values": [{ "double_value": 1 }], "timestamp": 1381347704000000},
        { "values": [{ "double_value": 2 }], "timestamp": 1381347704000000},
        { "values": [{ "double_value": 6 }], "timestamp": 1381347704000000}
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

	runQuery(engine, "select sum(column_one) from foo group by time(1m) order asc", c, `[
    {
      "points": [
        { "values": [{ "double_value": 11 }], "timestamp": 1381346700000000},
        { "values": [{ "double_value": 16 }], "timestamp": 1381346760000000}
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

	runQuery(engine, "select mode(column_one) from foo group by time(1m) order asc", c, `[
    {
      "points": [
        { "values": [{ "double_value": 1 }], "timestamp": 1381346700000000},
        { "values": [{ "double_value": 3 }], "timestamp": 1381346760000000}
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

	runQuery(engine, "select * from foo merge bar order asc", c, `[
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

	runQuery(engine, "select * from foo inner join bar order asc", c, `[
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

func (self *EngineSuite) TestQueryWithJoinedTablesDescendingOrder(c *C) {
	engine := createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346707000000, "sequence_number": 1 }
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
        { "values": [{ "int64_value": 2 }], "timestamp": 1381346705000000, "sequence_number": 1 }
      ],
      "name": "bar",
      "fields": ["value"]
    },
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000, "sequence_number": 1 }
      ],
      "name": "foo",
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

	runQuery(engine, "select * from foo inner join bar", c, `[
    {
      "points": [
        { "values": [{ "int64_value": 3 }, { "int64_value": 4 }], "timestamp": 1381346707000000, "sequence_number": 1 }
      ],
      "name": "foo_join_bar",
      "fields": ["foo.value", "bar.value"]
    },
    {
      "points": [
        { "values": [{ "int64_value": 1 }, { "int64_value": 2 }], "timestamp": 1381346705000000, "sequence_number": 1 }
      ],
      "name": "foo_join_bar",
      "fields": ["foo.value", "bar.value"]
    }
  ]`)
}

func (self *EngineSuite) TestJoiningWithSelf(c *C) {
	engine := createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346706000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000, "sequence_number": 1 }
      ],
      "name": "foo",
      "fields": ["value"]
    },
    {
      "points": [],
      "name": "foo",
      "fields": ["value"]
    },
    {
      "points": [
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346706000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000, "sequence_number": 1 }
      ],
      "name": "bar",
      "fields": ["value"]
    },
    {
      "points": [],
      "name": "bar",
      "fields": ["value"]
    }
  ]`)

	runQuery(engine, "select * from t as foo inner join t as bar", c, `[
    {
      "points": [
        { "values": [{ "int64_value": 3 }, { "int64_value": 3 }], "timestamp": 1381346706000000, "sequence_number": 1 }
      ],
      "name": "foo_join_bar",
      "fields": ["foo.value", "bar.value"]
    },
    {
      "points": [
        { "values": [{ "int64_value": 1 }, { "int64_value": 1 }], "timestamp": 1381346701000000, "sequence_number": 1 }
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

	runQueryExtended(engine, "select * from foo merge bar order asc", c, true, `[
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

func (self *EngineSuite) TestHistogramQueryWithGroupByTime(c *C) {
	// make the mock coordinator return some data
	engine := createEngine(c, `
[
  {
    "points": [
      {
        "values": [
          {
            "int64_value": 100
          }
        ],
        "timestamp": 1381346641000000,
        "sequence_number": 1
      },
      {
        "values": [
          {
            "int64_value": 5
          }
        ],
        "timestamp": 1381346651000000,
        "sequence_number": 1
      },
      {
        "values": [
          {
            "int64_value": 200
          }
        ],
        "timestamp": 1381346701000000,
        "sequence_number": 1
      },
      {
        "values": [
          {
            "int64_value": 299
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

	runQuery(engine, "select histogram(column_one, 100) from foo group by time(1m) order asc", c, `[
  {
    "points": [
      {
        "values": [
          {
            "double_value": 100
          },
          {
            "int64_value": 1
          }
        ],
        "timestamp": 1381346640000000
      },
      {
        "values": [
          {
            "double_value": 0
          },
          {
            "int64_value": 1
          }
        ],
        "timestamp": 1381346640000000
      },
      {
        "values": [
          {
            "double_value": 200
          },
          {
            "int64_value": 2
          }
        ],
        "timestamp": 1381346700000000
      }
    ],
    "name": "foo",
    "fields": ["bucket_start", "count"]
  }
]
`)
}

func (self *EngineSuite) TestHistogramQueryWithGroupByTimeAndDefaultBucketSize(c *C) {
	// make the mock coordinator return some data
	engine := createEngine(c, `
[
  {
    "points": [
      {
        "values": [
          {
            "int64_value": 100
          }
        ],
        "timestamp": 1381346641000000,
        "sequence_number": 1
      },
      {
        "values": [
          {
            "int64_value": 5
          }
        ],
        "timestamp": 1381346651000000,
        "sequence_number": 1
      },
      {
        "values": [
          {
            "int64_value": 200
          }
        ],
        "timestamp": 1381346701000000,
        "sequence_number": 1
      },
      {
        "values": [
          {
            "int64_value": 299
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

	runQuery(engine, "select histogram(column_one) from foo group by time(1m) order asc", c, `[
  {
    "points": [
      {
        "values": [
          {
            "double_value": 100
          },
          {
            "int64_value": 1
          }
        ],
        "timestamp": 1381346640000000
      },
      {
        "values": [
          {
            "double_value": 5
          },
          {
            "int64_value": 1
          }
        ],
        "timestamp": 1381346640000000
      },
      {
        "values": [
          {
            "double_value": 200
          },
          {
            "int64_value": 1
          }
        ],
        "timestamp": 1381346700000000
      },
      {
        "values": [
          {
            "double_value": 299
          },
          {
            "int64_value": 1
          }
        ],
        "timestamp": 1381346700000000
      }
    ],
    "name": "foo",
    "fields": ["bucket_start", "count"]
  }
]
`)
}

func (self *EngineSuite) TestCountQueryWithGroupByTimeInvalidNumberOfArguments(c *C) {
	err := common.NewQueryError(common.WrongNumberOfArguments, "time function only accepts one argument")
	engine := createEngine(c, `[]`)
	runQueryRunError(engine, "select count(*) from foo group by time(1h, 1m) order asc", c, err)
}

func (self *EngineSuite) TestCountQueryWithInvalidWildcardArgument(c *C) {
	err := common.NewQueryError(common.InvalidArgument, "function count() doesn't work with wildcards")
	engine := createEngine(c, `[]`)
	runQueryRunError(engine, "select count(*) from foo order asc", c, err)
}

func (self *EngineSuite) TestCountQueryWithGroupByTimeInvalidArgument(c *C) {
	err := common.NewQueryError(common.InvalidArgument, "invalid argument foobar to the time function")
	engine := createEngine(c, `[]`)
	runQueryRunError(engine, "select count(*) from foo group by time(foobar) order asc", c, err)
}

func (self *EngineSuite) TestPercentileQueryWithInvalidNumberOfArguments(c *C) {
	err := common.NewQueryError(common.WrongNumberOfArguments, "function percentile() requires exactly two arguments")
	engine := createEngine(c, `[]`)
	runQueryRunError(engine, "select percentile(95) from foo group by time(1m) order asc", c, err)
}

func (self *EngineSuite) TestPercentileQueryWithNonNumericArguments(c *C) {
	err := common.NewQueryError(common.InvalidArgument, "function percentile() requires a numeric second argument between 0 and 100")
	engine := createEngine(c, `[]`)
	runQueryRunError(engine, "select percentile(column_one, a95) from foo group by time(1m) order asc", c, err)
}

func (self *EngineSuite) TestPercentileQueryWithOutOfBoundNumericArguments(c *C) {
	err := common.NewQueryError(common.InvalidArgument, "function percentile() requires a numeric second argument between 0 and 100")
	engine := createEngine(c, `[]`)
	runQueryRunError(engine, "select percentile(column_one, 0) from foo group by time(1m) order asc", c, err)
	runQueryRunError(engine, "select percentile(column_one, 105) from foo group by time(1m) order asc", c, err)
}
