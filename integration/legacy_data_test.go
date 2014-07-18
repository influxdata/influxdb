package integration

// DO NOT COPY PASTE TESTS FROM THIS FILE. THESE ARE LEGACY TESTS AND
// SHOULD NOT BE FOLLOWED

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	. "github.com/influxdb/influxdb/checkers"
	influxdb "github.com/influxdb/influxdb/client"
	"github.com/influxdb/influxdb/common"
	. "github.com/influxdb/influxdb/integration/helpers"
	"github.com/influxdb/influxdb/protocol"
	. "launchpad.net/gocheck"
)

func createEngine(client Client, c *C, seriesString string) {
	series := convertFromDataStoreSeries(seriesString, c)
	client.WriteData(series, c, "u")
}

// convert from data store internal series format to the api format
func convertFromDataStoreSeries(seriesString string, c *C) []*influxdb.Series {
	series := []*protocol.Series{}
	c.Assert(json.Unmarshal([]byte(seriesString), &series), IsNil)
	apiSeries := []*influxdb.Series{}
	for _, s := range series {
		apiS := common.SerializeSeries(map[string]*protocol.Series{"": s}, common.MicrosecondPrecision)
		apiSeries = append(apiSeries, &influxdb.Series{
			Name:    apiS[0].Name,
			Columns: apiS[0].Columns,
			Points:  apiS[0].Points,
		})
	}
	return apiSeries
}

// runQuery() will run the given query on the engine and assert that
// the engine yields the expected time series given by expectedSeries
// in the order specified.
//
// expectedSeries must be a json array, e.g. time series must by
// enclosed in '[' and ']'
func runQuery(client Client, query string, c *C, expectedSeries string) {
	result := client.RunQueryWithNumbers(query, c)
	var expected []*protocol.Series
	err := json.Unmarshal([]byte(expectedSeries), &expected)
	c.Assert(err, IsNil)
	actual := []*protocol.Series{}
	for _, s := range result {
		dataStoreS, err := common.ConvertToDataStoreSeries(s, common.MillisecondPrecision)
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

func (self *DataTestSuite) BasicQuery(c *C) (Fun, Fun) {
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
	return func(client Client) {
			// create an engine and assert the engine works as a passthrough if
			// the query only returns the raw data
			createEngine(client, c, mockData)
		}, func(client Client) {
			runQuery(client, "select * from foo order asc", c, mockData)
		}
}

func (self *DataTestSuite) CountQuery(c *C) (Fun, Fun) {
	// make the mock coordinator return some data
	return func(client Client) {
			createEngine(client, c, `
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
		}, func(client Client) {
			runQuery(client, "select count(column_one) from foo order asc", c, `[
  {
    "points": [
      {
        "values": [
          {
            "int64_value": 2
          }
        ],
        "timestamp": 0
      }
    ],
    "name": "foo",
    "fields": ["count"]
  }
]
`)
		}
}

func (self *DataTestSuite) FirstAndLastQuery(c *C) (Fun, Fun) {
	return func(client Client) {
			// make the mock coordinator return some data
			createEngine(client, c, `
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
		}, func(client Client) {

			runQuery(client, "select first(column_one), last(column_one) from foo", c, `[
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
        "timestamp": 0
      }
    ],
    "name": "foo",
    "fields": ["first", "last"]
  }
]
`)
		}

}

func (self *DataTestSuite) UpperCaseQuery(c *C) (Fun, Fun) {
	return func(client Client) {
			// make the mock coordinator return some data
			createEngine(client, c, `
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
		}, func(client Client) {

			runQuery(client, "select COUNT(column_one) from foo order asc", c, `[
  {
    "points": [
      {
        "values": [
          {
            "int64_value": 2
          }
        ],
        "timestamp": 0
      }
    ],
    "name": "foo",
    "fields": ["count"]
  }
]
`)
		}

}

func (self *DataTestSuite) CountQueryWithRegexTables(c *C) (Fun, Fun) {
	return func(client Client) {
			// make the mock coordinator return some data
			createEngine(client, c, `
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
		}, func(client Client) {

			runQuery(client, "select count(column_one) from /foo.*/ order asc", c, `[
  {
    "points": [
      {
        "values": [
          {
            "int64_value": 1
          }
        ],
        "timestamp": 0
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
        "timestamp": 0
      }
    ],
    "name": "foo.baz",
    "fields": ["count"]
  }
]
`)
		}
}

func (self *DataTestSuite) CountQueryWithGroupByClause(c *C) (Fun, Fun) {
	return func(client Client) {
			// make the mock coordinator return some data
			createEngine(client, c, `
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
		}, func(client Client) {
			runQuery(client, "select count(column_one), column_one from foo group by column_one order asc", c, `[
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
        "timestamp": 0
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
        "timestamp": 0
      }
    ],
    "name": "foo",
    "fields": ["count", "column_one"]
  }
]
`)
		}
}

// issue #27
func (self *DataTestSuite) CountQueryWithGroupByClauseAndNullValues(c *C) (Fun, Fun) {
	// make the mock coordinator return some data
	return func(client Client) {
			createEngine(client, c, `
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
		}, func(client Client) {
			runQuery(client, "select count(column_two), column_one from foo group by column_one order asc", c, `[
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
        "timestamp": 0
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
        "timestamp": 0
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
        "timestamp": 0
      }
    ],
    "name": "foo",
    "fields": ["count", "column_one"]
  }
]
`)
		}
}

func (self *DataTestSuite) CountQueryWithGroupByClauseWithMultipleColumns(c *C) (Fun, Fun) {
	return func(client Client) {
			// make the mock coordinator return some data
			createEngine(client, c, `
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
		}, func(client Client) {
			runQuery(client, "select count(column_one), column_one, column_two from foo group by column_one, column_two order asc", c, `[
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
        "timestamp": 0
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
        "timestamp": 0
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
        "timestamp": 0
      }
    ],
    "name": "foo",
    "fields": ["count", "column_one", "column_two"]
  }
]
`)
		}
}

func (self *DataTestSuite) CountQueryWithGroupByTime(c *C) (Fun, Fun) {
	return func(client Client) {
			// make the mock coordinator return some data
			createEngine(client, c, `[
    {
      "points": [
        { "values": [{ "string_value": "some_value" }], "timestamp": 1381346641000000 },
        { "values": [{ "string_value": "another_value" }], "timestamp": 1381346701000000 },
        { "values": [{ "string_value": "some_value" }], "timestamp": 1381346721000000}
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)
		}, func(client Client) {
			runQuery(client, "select count(column_one) from foo group by time(1m) order asc", c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346640000000 },
        { "values": [{ "int64_value": 2 }], "timestamp": 1381346700000000 }
      ],
      "name": "foo",
      "fields": ["count"]
    }
  ]`)
		}
}

// TODO: cleanup this test
func (self *DataTestSuite) CountQueryWithGroupByTimeDescendingOrder(c *C) (Fun, Fun) {
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
	endTime := time.Now().Truncate(time.Hour)

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
          },
          {
            "int64_value": %d
          }
        ],
        "timestamp": %d
      }%s
`, i, endTime.Add(time.Duration(-i)*time.Second).Unix()*1000000, delimiter)

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
    "fields": ["column_one", "column_two"]
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
	return func(client Client) {
			// make the mock coordinator return some data
			createEngine(client, c, points)
		}, func(client Client) {
			runQuery(client, "select count(column_two) from foo group by time(1s);", c, expectedResponse)
		}
}

func (self *DataTestSuite) CountQueryWithGroupByTimeAndColumn(c *C) (Fun, Fun) {
	return func(client Client) {
			createEngine(client, c, `[
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
		}, func(client Client) {
			runQuery(client, "select count(column_one), column_one from foo group by time(1m), column_one order asc", c, `[
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
}

func (self *DataTestSuite) MinQueryWithGroupByTime(c *C) (Fun, Fun) {
	return func(client Client) {
			createEngine(client, c, `[
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
		}, func(client Client) {
			runQuery(client, "select min(column_one) from foo group by time(1m) order asc", c, `[
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
}

func (self *DataTestSuite) MaxQueryWithGroupByTime(c *C) (Fun, Fun) {
	return func(client Client) {
			createEngine(client, c, `[
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
		}, func(client Client) {
			runQuery(client, "select max(column_one) from foo group by time(1m) order asc", c, `[
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
}

func (self *DataTestSuite) MaxMinQueryWithGroupByTime(c *C) (Fun, Fun) {
	return func(client Client) {
			// make the mock coordinator return some data
			createEngine(client, c, `[
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
		}, func(client Client) {
			runQuery(client, "select max(column_one), min(column_one) from foo group by time(1m) order asc", c, `[
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
}

func (self *DataTestSuite) PercentileQueryWithGroupByTime(c *C) (Fun, Fun) {
	return func(client Client) {
			// make the mock coordinator return some data
			createEngine(client, c, `[
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
		}, func(client Client) {
			runQuery(client, "select percentile(column_one, 80) from foo group by time(1m) order asc", c, `[
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
}

// issue #405
func (self *DataTestSuite) LowPercentileForOnePointShouldNotCrash(c *C) (Fun, Fun) {
	return func(client Client) {
			// make the mock coordinator return some data
			createEngine(client, c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)
		}, func(client Client) {
			runQuery(client, "select percentile(column_one, 40) from foo group by time(1m)", c, `[
    {
      "points": [
        { "values": [{ "double_value": 0 }], "timestamp": 1381346700000000}
      ],
      "name": "foo",
      "fields": ["percentile"]
    }
  ]`)
		}
}

// issue #401
func (self *DataTestSuite) GroupBy5Columns(c *C) (Fun, Fun) {
	return func(client Client) {
			// make the mock coordinator return some data
			createEngine(client, c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 },{ "int64_value": 2 },{ "int64_value": 3 },{ "int64_value": 4 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 1 },{ "int64_value": 2 },{ "int64_value": 3 },{ "int64_value": 5 }], "timestamp": 1381346701000000 }
      ],
      "name": "foo",
      "fields": ["column1", "column2", "column3", "column4"]
    }
  ]`)
		}, func(client Client) {
			runQuery(client, "select count(column1) from foo group by time(1m), column1, column2, column3, column4", c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 },{ "int64_value": 1 },{ "int64_value": 2 },{ "int64_value": 3 },{ "int64_value": 4 }], "timestamp": 1381346700000000 },
        { "values": [{ "int64_value": 1 },{ "int64_value": 1 },{ "int64_value": 2 },{ "int64_value": 3 },{ "int64_value": 5 }], "timestamp": 1381346700000000 }
      ],
      "name": "foo",
      "fields": ["count","column1","column2","column3","column4"]
    }
  ]`)
		}
}

func (self *DataTestSuite) CountDistinct(c *C) (Fun, Fun) {
	return func(client Client) {
			// make the mock coordinator return some data
			createEngine(client, c, `[
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
		}, func(client Client) {
			for _, fun := range []string{"distinct", "DISTINCT"} {
				runQuery(client, fmt.Sprintf("select count(%s(column_one)) from foo order asc", fun), c, `[
    {
      "points": [
        { "values": [{ "int64_value": 9 }], "timestamp": 0}
      ],
      "name": "foo",
      "fields": ["count"]
    }
  ]`)
			}
		}
}

func (self *DataTestSuite) EmptyGroups(c *C) (Fun, Fun) {
	return func(client Client) {
			createEngine(client, c, `[
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
		}, func(client Client) {
			for _, query := range []string{
				"select count(column_one) from foo where time > 1381346701s and time < 1381346872s group by time(1m) fill(0) order asc",
				"select count(column_one) from foo group by time(1m) fill(0) order asc",
			} {
				runQuery(client, query, c, `[
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
}

func (self *DataTestSuite) EmptyGroupsWithNonZeroDefault(c *C) (Fun, Fun) {
	return func(client Client) {
			createEngine(client, c, `[
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
		}, func(client Client) {
			for _, query := range []string{
				"select count(column_one) from foo where time > 1381346701s and time < 1381346872s group by time(1m) fill(10) order asc",
				"select count(column_one) from foo group by time(1m) fill(10) order asc",
			} {
				runQuery(client, query, c, `[
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
}

func (self *DataTestSuite) EmptyGroupsWithoutTime(c *C) (Fun, Fun) {
	return func(client Client) {
			createEngine(client, c, `[
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
		}, func(client Client) {
			for _, query := range []string{
				"select count(column_one) from foo where time > 1381346701s and time < 1381346872s group by column_one fill(0) order asc",
				"select count(column_one) from foo group by column_one fill(0) order asc",
			} {
				runQuery(client, query, c, `[
    {
      "points": [
        { "values": [{ "int64_value": 5}, { "int64_value": 1 }], "timestamp": 0},
        { "values": [{ "int64_value": 6}, { "int64_value": 3 }], "timestamp": 0}
      ],
      "name": "foo",
      "fields": ["count", "column_one"]
    }
  ]`)
			}
		}
}

func (self *DataTestSuite) EmptyGroupsWithMultipleColumns(c *C) (Fun, Fun) {
	return func(client Client) {
			createEngine(client, c, `[
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
		}, func(client Client) {
			for _, query := range []string{
				"select count(column_one) from foo where time > 1381346701s and time < 1381346872s group by time(1m), column_one fill(0) order asc",
				"select count(column_one) from foo group by time(1m), column_one fill(0) order asc",
			} {
				runQuery(client, query, c, `[
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
}

func (self *DataTestSuite) EmptyGroupsDescending(c *C) (Fun, Fun) {
	return func(client Client) {
			createEngine(client, c, `[
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
		}, func(client Client) {
			for _, query := range []string{
				"select count(column_one) from foo where time > 1381346701s and time < 1381346872s group by time(1m) fill(0)",
				"select count(column_one) from foo group by time(1m) fill(0)",
			} {
				runQuery(client, query, c, `[
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
}

func (self *DataTestSuite) MedianQueryWithGroupByTime(c *C) (Fun, Fun) {
	return func(client Client) {
			createEngine(client, c, `[
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
		}, func(client Client) {
			runQuery(client, "select median(column_one) from foo group by time(1m) order asc", c, `[
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
}

func (self *DataTestSuite) MeanQueryWithGroupByTime(c *C) (Fun, Fun) {
	return func(client Client) {
			createEngine(client, c, `[
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
		}, func(client Client) {
			runQuery(client, "select mean(column_one) from foo group by time(1m) order asc", c, `[
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
}

func (self *DataTestSuite) StddevQuery(c *C) (Fun, Fun) {
	return func(client Client) {
			createEngine(client, c, `[
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
		}, func(client Client) {
			result := client.RunQuery("select stddev(column_one) from foo group by time(2s) order asc", c)
			c.Assert(result, HasLen, 1)
			c.Assert(result[0].Name, Equals, "foo")
			c.Assert(result[0].Columns, DeepEquals, []string{"time", "stddev"})
			c.Assert(result[0].Points, HasLen, 2)
			c.Assert(result[0].Points[0][1], InRange, 0.4714, 0.4715)
			c.Assert(result[0].Points[1][1], InRange, 0.9999, 1.0001)
		}
}

func (self *DataTestSuite) DerivativeQuery(c *C) (Fun, Fun) {
	return func(client Client) {
			createEngine(client, c, `[
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
		}, func(client Client) {
			runQuery(client, "select derivative(column_one) from foo group by time(2s) order asc", c, `[
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
}

func (self *DataTestSuite) DerivativeQueryWithOnePoint(c *C) (Fun, Fun) {
	return func(client Client) {
			createEngine(client, c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381347700000 }
      ],
      "name": "foo",
      "fields": ["column_one"]
    }
  ]`)
		}, func(client Client) {
			runQuery(client, "select derivative(column_one) from foo", c, `[]`)
		}
}

func (self *DataTestSuite) DistinctQuery(c *C) (Fun, Fun) {
	return func(client Client) {
			createEngine(client, c, `[
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
		}, func(client Client) {
			runQuery(client, "select distinct(column_one) from foo order asc", c, `[
    {
      "points": [
        { "values": [{ "double_value": 1 }], "timestamp": 0},
        { "values": [{ "double_value": 2 }], "timestamp": 0},
        { "values": [{ "double_value": 6 }], "timestamp": 0}
      ],
      "name": "foo",
      "fields": ["distinct"]
    }
  ]`)
		}
}

func (self *DataTestSuite) SumQueryWithGroupByTime(c *C) (Fun, Fun) {
	return func(client Client) {
			createEngine(client, c, `[
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
		}, func(client Client) {
			runQuery(client, "select sum(column_one) from foo group by time(1m) order asc", c, `[
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
}

func (self *DataTestSuite) ModeQueryWithGroupByTime(c *C) (Fun, Fun) {
	return func(client Client) {
			createEngine(client, c, `[
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
		}, func(client Client) {
			runQuery(client, "select mode(column_one) from foo group by time(1m) order asc", c, `[
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
}

func (self *DataTestSuite) QueryWithMergedTables(c *C) (Fun, Fun) {
	return func(client Client) {
			createEngine(client, c, `[
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
		}, func(client Client) {
			runQuery(client, "select * from foo merge bar order asc", c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }, {"string_value": "foo"}], "timestamp": 1381346701000000, "sequence_number": 1 },
        { "values": [{ "int64_value": 2 }, {"string_value": "bar"}], "timestamp": 1381346705000000, "sequence_number": 2 },
        { "values": [{ "int64_value": 4 }, {"string_value": "bar"}], "timestamp": 1381346706000000, "sequence_number": 3 },
        { "values": [{ "int64_value": 3 }, {"string_value": "foo"}], "timestamp": 1381346707000000, "sequence_number": 4 }
      ],
      "name": "foo_merge_bar",
      "fields": ["value", "_orig_series"]
    }
  ]`)
		}
}

func (self *DataTestSuite) QueryWithJoinedTablesAndArithmetic(c *C) (Fun, Fun) {
	return func(client Client) {
			createEngine(client, c, `[
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
    }
  ]`)
		}, func(client Client) {
			runQuery(client, "select bar.value - foo.value from foo inner join bar", c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346707000000 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346705000000 }
      ],
      "name": "foo_join_bar",
      "fields": ["expr0"]
    }
  ]`)
		}
}

func (self *DataTestSuite) QueryWithJoinedTables(c *C) (Fun, Fun) {
	return func(client Client) {
			createEngine(client, c, `[
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
    }
  ]`)
		}, func(client Client) {
			runQuery(client, "select * from foo inner join bar order asc", c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }, { "int64_value": 2 }], "timestamp": 1381346705000000 },
        { "values": [{ "int64_value": 3 }, { "int64_value": 4 }], "timestamp": 1381346707000000 }
      ],
      "name": "foo_join_bar",
      "fields": ["foo.value", "bar.value"]
    }
  ]`)
		}
}

func (self *DataTestSuite) QueryWithJoinedTablesDescendingOrder(c *C) (Fun, Fun) {
	return func(client Client) {
			createEngine(client, c, `[
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
		}, func(client Client) {
			runQuery(client, "select * from foo inner join bar", c, `[
    {
      "points": [
        { "values": [{ "int64_value": 3 }, { "int64_value": 4 }], "timestamp": 1381346707000000 },
        { "values": [{ "int64_value": 1 }, { "int64_value": 2 }], "timestamp": 1381346705000000 }
      ],
      "name": "foo_join_bar",
      "fields": ["foo.value", "bar.value"]
    }
  ]`)
		}
}

func (self *DataTestSuite) QueryWithJoinedTablesWithWhereClause(c *C) (Fun, Fun) {
	return func(client Client) {
			createEngine(client, c, `[
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
		}, func(client Client) {
			runQuery(client, "select * from foo inner join bar where foo.value = 3", c, `[
    {
      "points": [
        { "values": [{ "int64_value": 3 }, { "int64_value": 4 }], "timestamp": 1381346707000000 }
      ],
      "name": "foo_join_bar",
      "fields": ["foo.value", "bar.value"]
    }
  ]`)
		}
}

func (self *DataTestSuite) JoinedWithSelf(c *C) (Fun, Fun) {
	return func(client Client) {
			createEngine(client, c, `[
    {
      "points": [
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346706000000 },
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000 }
      ],
      "name": "t",
      "fields": ["value"]
    }
  ]`)
		}, func(client Client) {
			runQuery(client, "select * from t as foo inner join t as bar", c, `[
    {
      "points": [
        { "values": [{ "int64_value": 3 }, { "int64_value": 3 }], "timestamp": 1381346706000000 },
        { "values": [{ "int64_value": 1 }, { "int64_value": 1 }], "timestamp": 1381346701000000 }
      ],
      "name": "foo_join_bar",
      "fields": ["foo.value", "bar.value"]
    }
  ]`)
		}
}

func (self *DataTestSuite) QueryWithMergedTablesWithPointsAppend(c *C) (Fun, Fun) {
	return func(client Client) {
			createEngine(client, c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 4 }], "timestamp": 1381346707000000 }
      ],
      "name": "foo",
      "fields": ["a"]
    },
    {
      "points": [],
      "name": "foo",
      "fields": ["a"]
    },
    {
      "points": [
        { "values": [{ "int64_value": 2 }], "timestamp": 1381346705000000 },
        { "values": [{ "int64_value": 3 }], "timestamp": 1381346706000000 }
      ],
      "name": "bar",
      "fields": ["a"]
    },
    {
      "points": [],
      "name": "bar",
      "fields": ["a"]
    }
  ]`)
		}, func(client Client) {
			runQuery(client, "select * from foo merge bar order asc", c, `[
    {
      "points": [
        { "values": [{ "int64_value": 1 }, {"string_value": "foo"}], "timestamp": 1381346701000000 },
        { "values": [{ "int64_value": 2 }, {"string_value": "bar"}], "timestamp": 1381346705000000 },
        { "values": [{ "int64_value": 3 }, {"string_value": "bar"}], "timestamp": 1381346706000000 },
        { "values": [{ "int64_value": 4 }, {"string_value": "foo"}], "timestamp": 1381346707000000 }
      ],
      "name": "foo_merge_bar",
      "fields": ["a", "_orig_series"]
    }
  ]`)
		}
}

func (self *DataTestSuite) HistogramQueryWithGroupByTime(c *C) (Fun, Fun) {
	return func(client Client) {
			// make the mock coordinator return some data
			createEngine(client, c, `
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
		}, func(client Client) {
			runQuery(client, "select histogram(column_one, 100) from foo group by time(1m) order asc", c, `[
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
}

func (self *DataTestSuite) HistogramQueryWithGroupByTimeAndDefaultBucketSize(c *C) (Fun, Fun) {
	return func(client Client) {
			// make the mock coordinator return some data
			createEngine(client, c, `
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
		}, func(client Client) {
			runQuery(client, "select histogram(column_one) from foo group by time(1m) order asc", c, `[
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
}
