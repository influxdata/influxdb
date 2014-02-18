package integration

import (
	. "checkers"
	. "common"
	"encoding/json"
	"fmt"
	. "launchpad.net/gocheck"
	"net/http"
	"os"
	"protocol"
	"time"
)

type EngineSuite struct {
	server *ServerProcess
}

var _ = Suite(&EngineSuite{})

func (self *EngineSuite) SetUpSuite(c *C) {
	os.RemoveAll("/tmp/influxdb/test/1")
	self.server = NewServerProcess("test_config1.toml", 60500, time.Second*4, c)
}

func (self *EngineSuite) SetUpTest(c *C) {
	resp := self.server.Post("/db?u=root&p=root", "{\"name\":\"test_db\"}", c)
	c.Assert(resp.StatusCode, Equals, http.StatusCreated)
	resp = self.server.Post("/db/test_db/users?u=root&p=root", "{\"name\":\"user\", \"password\":\"pass\", \"isAdmin\": true}", c)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	time.Sleep(100 * time.Millisecond)
}

func (self *EngineSuite) TearDownTest(c *C) {
	resp := self.server.Request("DELETE", "/db/test_db?u=root&p=root", "", c)
	c.Assert(resp.StatusCode, Equals, http.StatusNoContent)
	time.Sleep(100 * time.Millisecond)
}

func (self *EngineSuite) TearDownSuite(c *C) {
	self.server.Stop()
}

func (self *EngineSuite) createEngine(c *C, seriesString string) {
	seriesString = convertFromDataStoreSeries(seriesString, c)
	resp := self.server.Post("/db/test_db/series?u=user&p=pass&time_precision=u", seriesString, c)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
}

// convert from data store internal series format to the api format
func convertFromDataStoreSeries(seriesString string, c *C) string {
	series := []*protocol.Series{}
	c.Assert(json.Unmarshal([]byte(seriesString), &series), IsNil)
	apiSeries := []*SerializedSeries{}
	for _, s := range series {
		apiS := SerializeSeries(map[string]*protocol.Series{"": s}, MicrosecondPrecision)
		apiSeries = append(apiSeries, apiS[0])
	}
	bytes, err := json.Marshal(apiSeries)
	c.Assert(err, IsNil)
	return string(bytes)
}

// runQuery() will run the given query on the engine and assert that
// the engine yields the expected time series given by expectedSeries
// in the order specified.
//
// expectedSeries must be a json array, e.g. time series must by
// enclosed in '[' and ']'
func (self *EngineSuite) runQuery(query string, c *C, expectedSeries string) {
	result := self.server.QueryWithUsername("test_db", query, false, c, "user", "pass")
	var expected []*protocol.Series
	err := json.Unmarshal([]byte(expectedSeries), &expected)
	c.Assert(err, IsNil)
	actual := []*protocol.Series{}
	for _, s := range result.Members {
		dataStoreS, err := ConvertToDataStoreSeries(s, MillisecondPrecision)
		c.Assert(err, IsNil)
		actual = append(actual, dataStoreS)
	}

	if !CheckEquality(actual, expected) {
		actualString, _ := json.MarshalIndent(actual, "", "  ")
		expectedString, _ := json.MarshalIndent(expected, "", "  ")

		fmt.Fprintf(os.Stderr,
			"===============\nThe two series aren't equal.\nExpected: %s\nActual: %s\n===============\n",
			expectedString, actualString)
	}

	c.Assert(actual, SeriesEquals, expected)
}

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
       "timestamp": 1381346631000000
     }
   ],
   "name": "foo",
   "fields": ["column_one"]
 }
]
`

	// create an engine and assert the engine works as a passthrough if
	// the query only returns the raw data
	self.createEngine(c, mockData)
	self.runQuery("select * from foo order asc", c, mockData)
}

// func (self *EngineSuite) TestBasicQueryError(c *C) {
// 	// create an engine and assert the engine works as a passthrough if
// 	// the query only returns the raw data
// 	engine := createEngine(c, "[]")
// 	engine.coordinator.(*MockCoordinator).returnedError = fmt.Errorf("some error")
// 	err := engine.coordinator.RunQuery(nil, "", "select * from foo", func(series *protocol.Series) error {
// 		return nil
// 	})

// 	c.Assert(err, ErrorMatches, "some error")
// }

func (self *EngineSuite) TestCountQuery(c *C) {
	// make the mock coordinator return some data
	self.createEngine(c, `
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

	self.runQuery("select count(column_one) from foo order asc", c, `[
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
	self.createEngine(c, `
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

	self.runQuery("select first(column_one), last(column_one) from foo", c, `[
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
	self.createEngine(c, `
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

	self.runQuery("select COUNT(column_one) from foo order asc", c, `[
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
	self.createEngine(c, `
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
        "timestamp": 1381346631000000
      }
    ],
    "name": "foo.baz",
    "fields": ["column_one"]
  }
]
`)

	self.runQuery("select count(column_one) from /foo.*/ order asc", c, `[
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
	self.createEngine(c, `
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
            "string_value": "another_value"
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

	self.runQuery("select count(column_one), column_one from foo group by column_one order asc", c, `[
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
	self.createEngine(c, `
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
        "timestamp": 1381346631000000
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
        "timestamp": 1381346631000000
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
        "timestamp": 1381346631000000
      }
    ],
    "name": "foo",
    "fields": ["column_one", "column_two"]
  }
]
`)

	self.runQuery("select count(column_two), column_one from foo group by column_one order asc", c, `[
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
	self.createEngine(c, `
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
        "timestamp": 1381346631000000
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
        "timestamp": 1381346631000000
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
        "timestamp": 1381346631000000
      }
    ],
    "name": "foo",
    "fields": ["column_one", "column_two"]
  }
]
`)

	self.runQuery("select count(column_one), column_one, column_two from foo group by column_one, column_two order asc", c, `[
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
	self.createEngine(c, `
[
  {
    "points": [
      {
        "values": [
          {
            "string_value": "some_value"
          }
        ],
        "timestamp": 1381346641000000
      },
      {
        "values": [
          {
            "string_value": "another_value"
          }
        ],
        "timestamp": 1381346701000000
      },
      {
        "values": [
          {
            "string_value": "some_value"
          }
        ],
        "timestamp": 1381346721000000
      }
    ],
    "name": "foo",
    "fields": ["column_one"]
  }
]
`)

	self.runQuery("select count(column_one) from foo group by time(1m) order asc", c, `[
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
        "timestamp": %d
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
	self.createEngine(c, points)

	self.runQuery("select count(column_one) from foo group by time(1s);", c, expectedResponse)
}

func (self *EngineSuite) TestCountQueryWithGroupByTimeAndColumn(c *C) {
	self.createEngine(c, `[
    {
      "points": [
        { "values": [{ "string_value": "some_value" }], "timestamp": 1381346641000000 },
        { "values": [{ "string_value": "another_value" }], "timestamp": 1381346701000000 },
        { "values": [{ "string_value": "some_value" }], "timestamp": 1381346721000000 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	self.runQuery("select count(column_one), column_one from foo group by time(1m), column_one order asc", c, `[
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
	self.createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346641000000 },
        { "values": [{ "int64_value": 8 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346721000000 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	self.runQuery("select min(column_one) from foo group by time(1m) order asc", c, `[
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
	self.createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346641000000 },
        { "values": [{ "int64_value": 8 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346721000000 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	self.runQuery("select max(column_one) from foo group by time(1m) order asc", c, `[
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
	self.createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346641000000 },
        { "values": [{ "int64_value": 8 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346721000000 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	self.runQuery("select max(column_one), min(column_one) from foo group by time(1m) order asc", c, `[
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
	self.createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 5 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 7 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 2 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 9 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 8 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 7 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 5 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346771000000 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	self.runQuery("select percentile(column_one, 80) from foo group by time(1m) order asc", c, `[
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
	self.createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 5 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 7 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 2 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 9 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 8 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 7 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 5 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346771000000 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	self.runQuery("select count(distinct(column_one)) from foo order asc", c, `[
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
	self.createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 5 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 7 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 2 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346871000000 },
        { "values": [{ "int64_value": 5 }], "timestamp": 1381346871000000 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346871000000 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346871000000 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	for _, query := range []string{
		"select count(column_one) from foo where time > 1381346701s and time < 1381346872s group by time(1m) fill(0) order asc",
		"select count(column_one) from foo group by time(1m) fill(0) order asc",
	} {
		self.runQuery(query, c, `[
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
	self.createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 5 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 7 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 2 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346871000000 },
        { "values": [{ "int64_value": 5 }], "timestamp": 1381346871000000 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346871000000 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346871000000 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	for _, query := range []string{
		"select count(column_one) from foo where time > 1381346701s and time < 1381346872s group by time(1m) fill(10) order asc",
		"select count(column_one) from foo group by time(1m) fill(10) order asc",
	} {
		self.runQuery(query, c, `[
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
	self.createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346871000000 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346871000000 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346871000000 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346871000000 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	for _, query := range []string{
		"select count(column_one) from foo where time > 1381346701s and time < 1381346872s group by column_one fill(0) order asc",
		"select count(column_one) from foo group by column_one fill(0) order asc",
	} {
		self.runQuery(query, c, `[
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
	self.createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346871000000 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346871000000 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346871000000 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346871000000 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	for _, query := range []string{
		"select count(column_one) from foo where time > 1381346701s and time < 1381346872s group by time(1m), column_one fill(0) order asc",
		"select count(column_one) from foo group by time(1m), column_one fill(0) order asc",
	} {
		self.runQuery(query, c, `[
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
	self.createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 5 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 7 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 2 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346871000000 },
        { "values": [{ "int64_value": 5 }], "timestamp": 1381346871000000 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346871000000 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346871000000 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	for _, query := range []string{
		"select count(column_one) from foo where time > 1381346701s and time < 1381346872s group by time(1m) fill(0)",
		"select count(column_one) from foo group by time(1m) fill(0)",
	} {
		self.runQuery(query, c, `[
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
	self.createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 5 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 7 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 2 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 9 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 8 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 7 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 5 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346771000000 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	self.runQuery("select median(column_one) from foo group by time(1m) order asc", c, `[
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
	self.createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 5 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 7 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 2 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 9 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 8 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 7 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 5 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346771000000 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	self.runQuery("select mean(column_one) from foo group by time(1m) order asc", c, `[
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
	self.createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381347700000000 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381347700500000 },
        { "values": [{ "int64_value": 2 }], "timestamp": 1381347701000000 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381347702000000 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381347703000000 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	result := self.server.QueryWithUsername("test_db", "select stddev(column_one) from foo group by time(2s) order asc", false, c, "user", "pass").Members
	c.Assert(result, HasLen, 1)
	c.Assert(result[0].Name, Equals, "foo")
	c.Assert(result[0].Columns, DeepEquals, []string{"time", "stddev"})
	c.Assert(result[0].Points, HasLen, 2)
	c.Assert(result[0].Points[0][1], InRange, 0.4714, 0.4715)
	c.Assert(result[0].Points[1][1], InRange, 0.9999, 1.0001)
}

func (self *EngineSuite) TestDerivativeQuery(c *C) {
	self.createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381347700000000 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381347700500000 },
        { "values": [{ "int64_value": 2 }], "timestamp": 1381347701000000 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381347702000000 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381347703000000 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	self.runQuery("select derivative(column_one) from foo group by time(2s) order asc", c, `[
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
	self.createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381347700000 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	self.runQuery("select derivative(column_one) from foo", c, `[
    {
      "points": [],
      "name": "foo",
      "fields": ["derivative"]
    }
  ]`)
}

func (self *EngineSuite) TestDistinctQuery(c *C) {
	self.createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381347701000000 },
        { "values": [{ "int64_value": 2 }], "timestamp": 1381347702000000 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381347703000000 },
        { "values": [{ "int64_value": 2 }], "timestamp": 1381347704000000 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	self.runQuery("select distinct(column_one) from foo order asc", c, `[
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
	self.createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 8 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 5 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346771000000 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	self.runQuery("select sum(column_one) from foo group by time(1m) order asc", c, `[
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
	self.createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 5 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 8 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 7 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 6 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 5 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346771000000 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346771000000 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)

	self.runQuery("select mode(column_one) from foo group by time(1m) order asc", c, `[
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
	self.createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000 }
      ],
      "name": "foo",
      "fields": ["value"]
    },
    {
      "points": [
        { "values": [{ "int64_value": 2 }], "timestamp": 1381346705000000 }
      ],
      "name": "bar",
      "fields": ["value"]
    },
    {
      "points": [
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346707000000 }
      ],
      "name": "foo",
      "fields": ["value"]
    },
    {
      "points": [
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346706000000 }
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

	self.runQuery("select * from foo merge bar order asc", c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }, {"string_value": "foo"}], "timestamp": 1381346701000000 }
      ],
      "name": "foo_merge_bar",
      "fields": ["value", "_orig_series"]
    },
    {
      "points": [
        { "values": [{ "int64_value": 2 }, {"string_value": "bar"}], "timestamp": 1381346705000000 }
      ],
      "name": "foo_merge_bar",
      "fields": ["value", "_orig_series"]
    },
    {
      "points": [
        { "values": [{ "int64_value": 4 }, {"string_value": "bar"}], "timestamp": 1381346706000000 }
      ],
      "name": "foo_merge_bar",
      "fields": ["value", "_orig_series"]
    },
    {
      "points": [
        { "values": [{ "int64_value": 3 }, {"string_value": "foo"}], "timestamp": 1381346707000000 }
      ],
      "name": "foo_merge_bar",
      "fields": ["value", "_orig_series"]
    }
  ]`)
}

func (self *EngineSuite) TestQueryWithJoinedTables(c *C) {
	self.createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000 }
      ],
      "name": "foo",
      "fields": ["value"]
    },
    {
      "points": [
        { "values": [{ "int64_value": 2 }], "timestamp": 1381346705000000 }
      ],
      "name": "bar",
      "fields": ["value"]
    },
    {
      "points": [
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346706000000 }
      ],
      "name": "foo",
      "fields": ["value"]
    },
    {
      "points": [
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346707000000 }
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

	self.runQuery("select * from foo inner join bar order asc", c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }, { "int64_value": 2 }], "timestamp": 1381346705000000 }
      ],
      "name": "foo_join_bar",
      "fields": ["foo.value", "bar.value"]
    },
    {
      "points": [
        { "values": [{ "int64_value": 3 }, { "int64_value": 4 }], "timestamp": 1381346707000000 }
      ],
      "name": "foo_join_bar",
      "fields": ["foo.value", "bar.value"]
    }
  ]`)
}

func (self *EngineSuite) TestQueryWithJoinedTablesDescendingOrder(c *C) {
	self.createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346707000000 }
      ],
      "name": "bar",
      "fields": ["value"]
    },
    {
      "points": [
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346706000000 }
      ],
      "name": "foo",
      "fields": ["value"]
    },
    {
      "points": [
        { "values": [{ "int64_value": 2 }], "timestamp": 1381346705000000 }
      ],
      "name": "bar",
      "fields": ["value"]
    },
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000 }
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

	self.runQuery("select * from foo inner join bar", c, `[
    {
      "points": [
        { "values": [{ "int64_value": 3 }, { "int64_value": 4 }], "timestamp": 1381346707000000 }
      ],
      "name": "foo_join_bar",
      "fields": ["foo.value", "bar.value"]
    },
    {
      "points": [
        { "values": [{ "int64_value": 1 }, { "int64_value": 2 }], "timestamp": 1381346705000000 }
      ],
      "name": "foo_join_bar",
      "fields": ["foo.value", "bar.value"]
    }
  ]`)
}

func (self *EngineSuite) TestJoiningWithSelf(c *C) {
	self.createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346706000000 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000 }
      ],
      "name": "foo",
      "fields": ["value"]
    },
    {
      "points": [
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346706000000 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000 }
      ],
      "name": "bar",
      "fields": ["value"]
    }
  ]`)

	self.runQuery("select * from t as foo inner join t as bar", c, `[
    {
      "points": [
        { "values": [{ "int64_value": 3 }, { "int64_value": 3 }], "timestamp": 1381346706000000 }
      ],
      "name": "foo_join_bar",
      "fields": ["foo.value", "bar.value"]
    },
    {
      "points": [
        { "values": [{ "int64_value": 1 }, { "int64_value": 1 }], "timestamp": 1381346701000000 }
      ],
      "name": "foo_join_bar",
      "fields": ["foo.value", "bar.value"]
    }
  ]`)
}

func (self *EngineSuite) TestQueryWithMergedTablesWithPointsAppend(c *C) {
	self.createEngine(c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }, { "int64_value": 1 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 1 }, { "int64_value": 4 }], "timestamp": 1381346707000000 }
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
        { "values": [{ "int64_value": 1 }, { "int64_value": 2 }], "timestamp": 1381346705000000 },
        { "values": [{ "int64_value": 1 }, { "int64_value": 3 }], "timestamp": 1381346706000000 }
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

	self.runQuery("select * from foo merge bar order asc", c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }, { "int64_value": 1 }, {"string_value": "foo"}], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 1 }, { "int64_value": 2 }, {"string_value": "bar"}], "timestamp": 1381346705000000 },
        { "values": [{ "int64_value": 1 }, { "int64_value": 3 }, {"string_value": "bar"}], "timestamp": 1381346706000000 },
        { "values": [{ "int64_value": 1 }, { "int64_value": 4 }, {"string_value": "foo"}], "timestamp": 1381346707000000 }
      ],
      "name": "foo_merge_bar",
      "fields": ["a", "b", "_orig_series"]
    }
  ]`)
}

func (self *EngineSuite) TestHistogramQueryWithGroupByTime(c *C) {
	// make the mock coordinator return some data
	self.createEngine(c, `
[
  {
    "points": [
      {
        "values": [
          {
            "int64_value": 100
          }
        ],
        "timestamp": 1381346641000000
      },
      {
        "values": [
          {
            "int64_value": 5
          }
        ],
        "timestamp": 1381346651000000
      },
      {
        "values": [
          {
            "int64_value": 200
          }
        ],
        "timestamp": 1381346701000000
      },
      {
        "values": [
          {
            "int64_value": 299
          }
        ],
        "timestamp": 1381346721000000
      }
    ],
    "name": "foo",
    "fields": ["column_one"]
  }
]
`)

	self.runQuery("select histogram(column_one, 100) from foo group by time(1m) order asc", c, `[
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
	self.createEngine(c, `
[
  {
    "points": [
      {
        "values": [
          {
            "int64_value": 100
          }
        ],
        "timestamp": 1381346641000000
      },
      {
        "values": [
          {
            "int64_value": 5
          }
        ],
        "timestamp": 1381346651000000
      },
      {
        "values": [
          {
            "int64_value": 200
          }
        ],
        "timestamp": 1381346701000000
      },
      {
        "values": [
          {
            "int64_value": 299
          }
        ],
        "timestamp": 1381346721000000
      }
    ],
    "name": "foo",
    "fields": ["column_one"]
  }
]
`)

	self.runQuery("select histogram(column_one) from foo group by time(1m) order asc", c, `[
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

// func (self *EngineSuite) TestCountQueryWithGroupByTimeInvalidNumberOfArguments(c *C) {
// 	err := common.NewQueryError(common.WrongNumberOfArguments, "time function only accepts one argument")
// self.createEngine(c, `[]`)
// 	runQueryRunError(engine, "select count(*) from foo group by time(1h, 1m) order asc", c, err)
// }

// func (self *EngineSuite) TestCountQueryWithInvalidWildcardArgument(c *C) {
// 	err := common.NewQueryError(common.InvalidArgument, "function count() doesn't work with wildcards")
// self.createEngine(c, `[]`)
// 	runQueryRunError(engine, "select count(*) from foo order asc", c, err)
// }

// func (self *EngineSuite) TestCountQueryWithGroupByTimeInvalidArgument(c *C) {
// 	err := common.NewQueryError(common.InvalidArgument, "invalid argument foobar to the time function")
// self.createEngine(c, `[]`)
// 	runQueryRunError(engine, "select count(*) from foo group by time(foobar) order asc", c, err)
// }

// func (self *EngineSuite) TestPercentileQueryWithInvalidNumberOfArguments(c *C) {
// 	err := common.NewQueryError(common.WrongNumberOfArguments, "function percentile() requires exactly two arguments")
// self.createEngine(c, `[]`)
// 	runQueryRunError(engine, "select percentile(95) from foo group by time(1m) order asc", c, err)
// }

// func (self *EngineSuite) TestPercentileQueryWithNonNumericArguments(c *C) {
// 	err := common.NewQueryError(common.InvalidArgument, "function percentile() requires a numeric second argument between 0 and 100")
// self.createEngine(c, `[]`)
// 	runQueryRunError(engine, "select percentile(column_one, a95) from foo group by time(1m) order asc", c, err)
// }

// func (self *EngineSuite) TestPercentileQueryWithOutOfBoundNumericArguments(c *C) {
// 	err := common.NewQueryError(common.InvalidArgument, "function percentile() requires a numeric second argument between 0 and 100")
// self.createEngine(c, `[]`)
// 	runQueryRunError(engine, "select percentile(column_one, 0) from foo group by time(1m) order asc", c, err)
// 	runQueryRunError(engine, "select percentile(column_one, 105) from foo group by time(1m) order asc", c, err)
// }
