package integration

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"time"

	influxdb "github.com/influxdb/influxdb/client"
	"github.com/influxdb/influxdb/engine"
	. "github.com/influxdb/influxdb/integration/helpers"
	. "launchpad.net/gocheck"
)

type DataTestSuite struct {
	server *Server
	client *DataTestClient
	dbname string
}

var _ = Suite(&DataTestSuite{})

func (self *DataTestSuite) SetUpSuite(c *C) {
	self.dbname = "testdb"
	self.server = NewServer("integration/test_config_single.toml", c)
	// make sure the tests don't use an idle connection, otherwise the
	// server will close it
	http.DefaultTransport.(*http.Transport).CloseIdleConnections()
}

func (self *DataTestSuite) TearDownSuite(c *C) {
	if self.server == nil {
		return
	}
	self.server.Stop()
}

func (self *DataTestSuite) SetUpTest(c *C) {
	self.client = &DataTestClient{}
	self.client.CreateDatabase(self.dbname, c)
	self.client.SetDB(self.dbname)
}

func (self *DataTestSuite) TearDownTest(c *C) {
	self.client.DeleteDatabase(self.dbname, c)
	self.client = nil
}

type Fun func(client Client)

// issue #518
func (self *DataTestSuite) TestInfiniteValues(c *C) {
	// make sure we exceed the pointBatchSize, so we force a yield to
	// the filtering engine
	data := `
[
  {
    "points": [
        [1399590718, 0.0],
        [1399590718, 0.0]
    ],
    "name": "test_infinite_values",
    "columns": ["time", "value"]
  }
]`
	self.client.WriteJsonData(data, c, influxdb.Second)
	serieses := self.client.RunQuery("select derivative(value) from test_infinite_values", c, "m")
	c.Assert(serieses, HasLen, 1)
	maps := ToMap(serieses[0])
	c.Assert(maps, HasLen, 1)
	c.Assert(maps[0]["derivative"], IsNil)
}

// This test tries to write a large batch of points to a shard that is
// supposed to be dropped. This will demonstrate issue #985: while the
// data is being written, InfluxDB will close the underlying storage
// engine which will cause random errors to be thrown and could
// possibly corrupt the db.
func (self *DataTestSuite) TestWritingToExpiredShards(c *C) {
	client := self.server.GetClient(self.dbname, c)
	err := client.CreateShardSpace(self.dbname, &influxdb.ShardSpace{
		Name:            "default",
		Regex:           ".*",
		RetentionPolicy: "7d",
		ShardDuration:   "1y",
	})
	c.Assert(err, IsNil)

	data := CreatePoints("test_using_deleted_shard", 1, 1000000)
	data[0].Columns = append(data[0].Columns, "time")
	for i := range data[0].Points {
		data[0].Points[i] = append(data[0].Points[i], 0)
	}
	// This test will fail randomly without the fix submitted in the
	// same commit. 10 times is sufficient to trigger the bug.
	for i := 0; i < 10; i++ {
		self.client.WriteData(data, c, influxdb.Second)
	}
}

// test large integer values
func (self *DataTestSuite) TestLargeIntegerValues(c *C) {
	// make sure we exceed the pointBatchSize, so we force a yield to
	// the filtering engine
	i := int64(math.MaxInt64)
	data := fmt.Sprintf(`
[
  {
    "points": [
        [%d]
    ],
    "name": "test_large_integer_values",
    "columns": ["value"]
  }
]`, i)
	self.client.WriteJsonData(data, c, influxdb.Second)
	serieses := self.client.RunQueryWithNumbers("select * from test_large_integer_values", c, "m")
	c.Assert(serieses, HasLen, 1)
	maps := ToMap(serieses[0])
	c.Assert(maps, HasLen, 1)
	n := maps[0]["value"]
	actual, err := n.(json.Number).Int64()
	c.Assert(err, IsNil)
	c.Assert(actual, Equals, i)
}

// Postive case of derivative function
func (self *DataTestSuite) TestDerivativeValues(c *C) {
	// make sure we exceed the pointBatchSize, so we force a yield to
	// the filtering engine
	data := `
[
  {
	"points": [
	[1399590718, 10.0],
	[1399590719, 20.0]
	],
	"name": "test_derivative_values",
	"columns": ["time", "value"]
  }
]`
	self.client.WriteJsonData(data, c, influxdb.Second)
	serieses := self.client.RunQuery("select derivative(value) from test_derivative_values", c, "m")
	c.Assert(serieses, HasLen, 1)
	maps := ToMap(serieses[0])
	c.Assert(maps, HasLen, 1)
	c.Assert(maps[0]["derivative"], Equals, 10.0)
}

func (self *DataTestSuite) TestInvalidSeriesInSelect(c *C) {
	client := self.server.GetClient(self.dbname, c)
	_, err := client.Query("select value from some_invalid_series_name", "m")
	c.Assert(err, ErrorMatches, ".*Couldn't find series: some_invalid_series_name.*")
}

// Simple case of difference function
func (self *DataTestSuite) TestDifferenceValues(c *C) {
	// make sure we exceed the pointBatchSize, so we force a yield to
	// the filtering engine
	data := `
[
  {
	"points": [
	[1399590718, 10.0],
	[1399590719, 20.0],
	[1399590720, 30.0]
	],
	"name": "test_difference_values",
	"columns": ["time", "value"]
  }
]`
	self.client.WriteJsonData(data, c, influxdb.Second)
	serieses := self.client.RunQuery("select difference(value) from test_difference_values order asc", c, "m")
	c.Assert(serieses, HasLen, 1)
	maps := ToMap(serieses[0])
	c.Assert(maps, HasLen, 1)
	c.Assert(maps[0]["difference"], Equals, 20.0)
}

// issue #426
func (self *DataTestSuite) TestFillingEntireRange(c *C) {
	data := `
[
  {
    "name": "test_filling_range",
    "columns": ["value"],
    "points": [
      [1]
    ]
  }
 ]`
	self.client.WriteJsonData(data, c, influxdb.Millisecond)
	serieses := self.client.RunQuery("select sum(value) from test_filling_range where time > now() - 1d group by time(1h) fill(0)", c, "m")
	c.Assert(serieses, HasLen, 1)
	maps := ToMap(serieses[0])
	c.Assert(maps, HasLen, 25)
	c.Assert(maps[0]["sum"], Equals, 1.0)
	for i := 1; i < len(maps); i++ {
		c.Assert(maps[i]["sum"], Equals, 0.0)
	}
}

func (self *DataTestSuite) TestBigInts(c *C) {
	data := `
[
  {
    "name": "test_mode",
    "columns": ["value"],
    "points": [
      [7335093126128605887],
      [15028546720250474530]
    ]
  }
 ]`
	self.client.WriteJsonData(data, c, influxdb.Millisecond)
	for _, i := range []uint64{7335093126128605887, 15028546720250474530} {
		q := fmt.Sprintf("select count(value) from test_mode where value = %d", i)
		serieses := self.client.RunQuery(q, c, "m")
		c.Assert(serieses, HasLen, 1)
		maps := ToMap(serieses[0])
		c.Assert(maps, HasLen, 1)
		c.Assert(maps[0]["count"], Equals, 1.0)
	}

	q := "select count(value) from test_mode where value >= 15028546720250474530 and value <= 15028546720250474530"
	serieses := self.client.RunQuery(q, c, "m")
	c.Assert(serieses, HasLen, 1)
	maps := ToMap(serieses[0])
	c.Assert(maps, HasLen, 1)
	c.Assert(maps[0]["count"], Equals, 1.0)
}

func (self *DataTestSuite) TestModeWithInt(c *C) {
	data := `
[
  {
    "name": "test_mode",
    "columns": ["value"],
    "points": [
      [1],
      [2],
      [2],
      [3],
      [4]
    ]
  }
 ]`
	self.client.WriteJsonData(data, c, influxdb.Millisecond)
	serieses := self.client.RunQuery("select mode(value) from test_mode", c, "m")
	c.Assert(serieses, HasLen, 1)
	maps := ToMap(serieses[0])
	c.Assert(maps, HasLen, 1)
	c.Assert(maps[0]["mode"], Equals, 2.0)
}

func (self *DataTestSuite) TestModeWithString(c *C) {
	data := `
[
  {
    "name": "test_mode_string",
    "columns": ["value"],
    "points": [
      ["one"],
      ["two"],
      ["two"],
      ["two"],
      ["three"]
    ]
  }
 ]`
	self.client.WriteJsonData(data, c, influxdb.Millisecond)
	serieses := self.client.RunQuery("select mode(value) from test_mode_string", c, "m")
	c.Assert(serieses, HasLen, 1)
	maps := ToMap(serieses[0])
	c.Assert(maps, HasLen, 1)
	c.Assert(maps[0]["mode"], Equals, "two")
}

func (self *DataTestSuite) TestModeWithNils(c *C) {
	data := `
[
  {
    "name": "test_mode_nils",
    "columns": ["value", "value2"],
    "points": [
      [1, "one"],
      [1, null],
      [1, null],
      [1, null],
      [1, "three"]
    ]
  }
 ]`
	self.client.WriteJsonData(data, c, influxdb.Millisecond)
	serieses := self.client.RunQuery("select mode(value) as m1, mode(value2) as m2 from test_mode_nils", c, "m")
	c.Assert(serieses, HasLen, 1)
	maps := ToMap(serieses[0])
	c.Assert(maps, HasLen, 1)
	c.Assert(maps[0]["m2"], Equals, nil)
}

func (self *DataTestSuite) TestMergeRegexOneSeries(c *C) {
	data := `
[
  {
    "name": "test_merge_1",
    "columns": ["time", "value"],
    "points": [
      [1401321700000, "m11"],
      [1401321600000, "m12"]
    ]
  }
]`

	self.client.WriteJsonData(data, c, influxdb.Millisecond)
	serieses := self.client.RunQuery("select * from merge(/.*/)", c, "m")
	c.Assert(serieses, HasLen, 1)
	maps := ToMap(serieses[0])
	c.Assert(maps, HasLen, 2)
	c.Assert(maps[0]["value"], Equals, "m11")
	c.Assert(maps[1]["value"], Equals, "m12")
}

func (self *DataTestSuite) TestMergeRegex(c *C) {
	data := `
[
  {
    "name": "test_merge_1",
    "columns": ["time", "value"],
    "points": [
      [1401321600000, "m11"],
      [1401321800000, "m12"]
    ]
  },
  {
    "name": "test_merge_2",
    "columns": ["time", "value"],
    "points": [
      [1401321700000, "m21"],
      [1401321900000, "m22"]
    ]
  },
  {
    "name": "test_merge_3",
    "columns": ["time", "value"],
    "points": [
      [1401321500000, "m31"],
      [1401322000000, "m32"]
    ]
  }
 ]`
	self.client.WriteJsonData(data, c, influxdb.Millisecond)
	serieses := self.client.RunQuery("select * from merge(/.*/)", c, "m")
	c.Assert(serieses, HasLen, 1)
	maps := ToMap(serieses[0])
	c.Assert(maps, HasLen, 6)
	t := make([]float64, len(maps))
	for i, m := range maps {
		t[i] = m["time"].(float64)
	}
	c.Assert(t, DeepEquals, []float64{
		1401322000000,
		1401321900000,
		1401321800000,
		1401321700000,
		1401321600000,
		1401321500000,
	})
}

func (self *DataTestSuite) TestMergingOldData(c *C) {
	data := `
[
  {
    "name": "test_merge_1",
    "columns": ["time", "value"],
    "points": [
      [315532800000, "a value"]
    ]
  },
  {
    "name": "test_merge_2",
    "columns": ["time", "value"],
    "points": [
      [1401321600000, "another value"]
    ]
  }
 ]`
	self.client.WriteJsonData(data, c, influxdb.Millisecond)
	serieses := self.client.RunQuery("select * from test_merge_1 merge test_merge_2", c, "m")
	c.Assert(serieses, HasLen, 1)
	maps := ToMap(serieses[0])
	c.Assert(maps, HasLen, 2)

	serieses = self.client.RunQuery("select * from test_merge_1 merge test_merge_2 where time > '1980-01-01' and time < '1980-01-04'", c, "m")
	c.Assert(serieses, HasLen, 1)
	maps = ToMap(serieses[0])
	c.Assert(maps, HasLen, 1)
	c.Assert(maps[0]["value"], Equals, "a value")
}

// Difference function combined with group by
func (self *DataTestSuite) TestDifferenceGroupValues(c *C) {
	data := `
[
  {
	"points": [
	[1399590700,   0.0],
	[1399590710,  10.0],
	[1399590720,  20.0],
	[1399590730,  40.0],
	[1399590740,  80.0],
	[1399590750, 160.0]
	],
	"name": "test_difference_group_values",
	"columns": ["time", "value"]
  }
]`
	self.client.WriteJsonData(data, c, influxdb.Second)
	serieses := self.client.RunQuery("select difference(value) from test_difference_group_values group by time(20s) order asc", c, "m")
	c.Assert(serieses, HasLen, 1)
	maps := ToMap(serieses[0])
	c.Assert(maps, HasLen, 3)
	c.Assert(maps[0]["difference"], Equals, 10.0)
	c.Assert(maps[1]["difference"], Equals, 20.0)
	c.Assert(maps[2]["difference"], Equals, 80.0)
}

// issue #578
func (self *DataTestSuite) TestParanthesesAlias(c *C) {
	data := `
[
  {
	"points": [
	[  0.0, 1.0]
	],
	"name": "test_parantheses_aliasing",
	"columns": ["value", "one"]
  }
]`
	self.client.WriteJsonData(data, c, influxdb.Second)
	serieses := self.client.RunQuery("select (value + one) as value_plus_one from test_parantheses_aliasing", c, "m")
	c.Assert(serieses, HasLen, 1)
	maps := ToMap(serieses[0])
	c.Assert(maps, HasLen, 1)
	c.Assert(maps[0]["value_plus_one"], Equals, 1.0)
}

// Difference and group by function using a time where clause with an interval which is equal to the time of the points
// FIXME: This test still fails. For this case the group by function should include points with the end time for each bucket.
//func (self *DataTestSuite) TestDifferenceGroupSameTimeValues(c *C) {
//	// make sure we exceed the pointBatchSize, so we force a yield to
//	// the filtering engine
//			data := `
//[
//  {
//	"points": [
//	[1399590700,   0.0],
//	[1399590710,  10.0],
//	[1399590720,  20.0],
//	[1399590730,  40.0],
//	[1399590740,  80.0],
//	[1399590750, 160.0]
//	],
//	"name": "test_difference_group_same_time_values",
//	"columns": ["time", "value"]
//  }
//]`
//			self.client.WriteJsonData(data, c, influxdb.Second)
//			serieses := self.client.RunQuery("select range(value) from test_difference_group_same_time_values group by time(10s) order asc", c, "m")
//			c.Assert(serieses, HasLen, 1)
//			maps := ToMap(serieses[0])
//			c.Assert(maps, HasLen, 6)
//			c.Assert(maps[0]["difference"], Equals, 10.0)
//			c.Assert(maps[1]["difference"], Equals, 10.0)
//			c.Assert(maps[2]["difference"], Equals, 20.0)
//			c.Assert(maps[3]["difference"], Equals, 40.0)
//			c.Assert(maps[4]["difference"], Equals, 80.0)
//}

// issue #512
func (self *DataTestSuite) TestGroupByNullValues(c *C) {
	// make sure we exceed the pointBatchSize, so we force a yield to
	// the filtering engine
	data := `
[
  {
    "points": [
        ["one", null],
        ["one", 1],
        ["one", null]
    ],
    "name": "test_null_groups",
    "columns": ["column0", "column1"]
  }
]`
	self.client.WriteJsonData(data, c, influxdb.Second)
	serieses := self.client.RunQuery("select count(column0) from test_null_groups group by column1", c, "m")
	c.Assert(serieses, HasLen, 1)
	maps := ToMap(serieses[0])
	c.Assert(maps, HasLen, 2)
	c.Assert(maps[0]["count"], Equals, 1.0)
	// this is an implementation detail, but nulls come last in the
	// trie
	c.Assert(maps[1]["count"], Equals, 2.0)
}

// issue #389
func (self *DataTestSuite) TestFilteringShouldNotStopIfAllPointsDontMatch(c *C) {
	// make sure we exceed the pointBatchSize, so we force a yield to
	// the filtering engine
	numberOfPoints := 1000000
	serieses := CreatePointsFromFunc("test_filtering_shouldnt_stop", 1, numberOfPoints, func(i int) float64 { return float64(numberOfPoints - i) })
	series := serieses[0]
	series.Columns = append(series.Columns, "time")
	points := series.Points
	series.Points = nil
	now := time.Now()
	for idx, point := range points {
		point = append(point, float64(now.Add(time.Duration(-idx)*time.Second).Unix()))
		series.Points = append(series.Points, point)
	}
	self.client.WriteData([]*influxdb.Series{series}, c, influxdb.Second)
	result := self.client.RunQuery("select column0 from test_filtering_shouldnt_stop where column0 < 10", c, "m")
	c.Assert(result, HasLen, 1)
}

// issue #413
func (self *DataTestSuite) TestSmallGroupByIntervals(c *C) {
	serieses := CreatePoints("test_small_group_by", 1, 1)
	self.client.WriteData(serieses, c)
	serieses = self.client.RunQuery("select count(column0) from test_small_group_by group by time(10)", c, "m")
	c.Assert(serieses, HasLen, 1)
	c.Assert(serieses[0].Points, HasLen, 1)
	c.Assert(serieses[0].Points[0], HasLen, 2)
	c.Assert(serieses[0].Points[0][1], Equals, 1.0)
}

// issue #524
func (self *DataTestSuite) TestWhereAndArithmetic(c *C) {
	i := 0
	serieses := CreatePointsFromFunc("foo", 2, 2, func(_ int) float64 { i++; return float64(i) })
	self.client.WriteData(serieses, c)
	serieses = self.client.RunQuery("select column1 / 2 from foo where column0 > 1", c, "m")
	c.Assert(serieses, HasLen, 1)
	maps := ToMap(serieses[0])
	c.Assert(maps, HasLen, 1)
	c.Assert(maps[0]["expr0"], Equals, 2.0)
}

// issue #524
func (self *DataTestSuite) TestJoinRegex(c *C) {
	t1 := time.Now().Truncate(time.Hour).Add(-4 * time.Hour)
	t2 := t1.Add(time.Hour)
	data := fmt.Sprintf(`[
{
  "name":"foo1",
  "columns":["time", "val"],
  "points":[[%d, 1],[%d, 2]]
},
{
  "name":"foo2",
  "columns":["time", "val"],
  "points":[[%d, 3],[%d, 4]]

},
{
  "name":"foo3",
  "columns":["time", "val"],
  "points":[[%d, 5],[%d, 6]]

}]`, t1.Unix(), t2.Unix(), t1.Unix(), t2.Unix(), t1.Unix(), t2.Unix())
	self.client.WriteJsonData(data, c, "s")
	serieses := self.client.RunQuery("select * from join(/foo\\d+/)", c, "m")
	c.Assert(serieses, HasLen, 1)
	maps := ToMap(serieses[0])
	c.Assert(maps, HasLen, 2)
	c.Assert(maps[0]["foo1.val"], Equals, 2.0)
	c.Assert(maps[0]["foo2.val"], Equals, 4.0)
	c.Assert(maps[0]["foo3.val"], Equals, 6.0)
	c.Assert(maps[1]["foo1.val"], Equals, 1.0)
	c.Assert(maps[1]["foo2.val"], Equals, 3.0)
	c.Assert(maps[1]["foo3.val"], Equals, 5.0)
}

// issue #524
func (self *DataTestSuite) TestJoinAndArithmetic(c *C) {
	t1 := time.Now().Truncate(time.Hour).Add(-4 * time.Hour)
	t2 := t1.Add(time.Hour)
	t3 := t2.Add(time.Hour)
	t4 := t3.Add(time.Hour)
	data := fmt.Sprintf(`[
{
  "name":"foo",
  "columns":["time", "val"],
  "points":[[%d, 1],[%d, 2]]
},
{
  "name":"bar",
  "columns":["time", "val"],
  "points":[[%d, 3],[%d, 4]]

}]`, t1.Unix(), t3.Unix(), t2.Unix(), t4.Unix())
	self.client.WriteJsonData(data, c, "s")
	serieses := self.client.RunQuery("select foo.val + bar.val from foo inner join bar where bar.val <> 3", c, "m")
	c.Assert(serieses, HasLen, 1)
	maps := ToMap(serieses[0])
	c.Assert(maps, HasLen, 1)
	c.Assert(maps[0]["expr0"], Equals, 6.0)
}

// issue #652
func (self *DataTestSuite) TestJoinWithInvalidFilteringShouldReturnMeaningfulError(c *C) {
	t1 := time.Now().Truncate(time.Hour).Add(-4 * time.Hour)
	t2 := t1.Add(time.Hour)
	t3 := t2.Add(time.Hour)
	t4 := t3.Add(time.Hour)
	data := fmt.Sprintf(`[
{
  "name":"foo",
  "columns":["time", "val"],
  "points":[[%d, 1],[%d, 2]]
},
{
  "name":"bar",
  "columns":["time", "val"],
  "points":[[%d, 3],[%d, 4]]

}]`, t1.Unix(), t3.Unix(), t2.Unix(), t4.Unix())
	self.client.WriteJsonData(data, c, "s")
	self.client.RunInvalidQuery("select foo.val + bar.val from foo inner join bar where val <> 3", c, "m")
}

// issue #768
func (self *DataTestSuite) TestMinusOperatorWithoutSpace(c *C) {
	data := `
  [{
    "points": [
        [2, 3]
    ],
    "name": "test_minus_operator",
    "columns": ["val1", "val2"]
  }]`
	self.client.WriteJsonData(data, c)
	serieses := self.client.RunQuery("select val2-val1 from test_minus_operator ", c, "m")
	c.Assert(serieses, HasLen, 1)
	maps := ToMap(serieses[0])
	c.Assert(maps, HasLen, 1)
	c.Assert(maps[0]["expr0"], Equals, 1.0)
}

// issue #540
func (self *DataTestSuite) TestRegexMatching(c *C) {
	serieses := CreatePoints("cpu.1", 1, 1)
	self.client.WriteData(serieses, c)
	serieses = CreatePoints("cpu-2", 1, 1)
	self.client.WriteData(serieses, c)
	serieses = self.client.RunQuery("select * from /cpu\\..*/ limit 1", c, "m")
	c.Assert(serieses, HasLen, 1)
	c.Assert(serieses[0].Name, Equals, "cpu.1")
}

// issue #112
func (self *DataTestSuite) TestSelectFromMultipleSeries(c *C) {
	serieses := CreatePoints("cpu1.load_one", 1, 5)
	self.client.WriteData(serieses, c)
	serieses = CreatePoints("cpu2.load_one", 1, 3)
	self.client.WriteData(serieses, c)
	results := self.client.RunQuery("select * from cpu1.load_one, cpu2.load_one", c, "m")
	series := map[string][]map[string]interface{}{}
	for _, s := range results {
		series[s.Name] = ToMap(s)
	}
	c.Assert(series, HasLen, 2)
	c.Assert(series["cpu1.load_one"], HasLen, 5)
	c.Assert(series["cpu2.load_one"], HasLen, 3)
}

func (self *DataTestSuite) TestSelectFromMultipleSeriesWithLimit(c *C) {
	i := 0.0
	serieses := CreatePointsFromFunc("cpu1.load_one", 1, 5, func(_ int) float64 { i++; return i })
	self.client.WriteData(serieses, c)
	serieses = CreatePointsFromFunc("cpu2.load_one", 1, 3, func(_ int) float64 { i++; return i })
	self.client.WriteData(serieses, c)
	results := self.client.RunQuery("select * from cpu1.load_one, cpu2.load_one limit 2", c, "m")
	series := map[string][]map[string]interface{}{}
	for _, s := range results {
		series[s.Name] = ToMap(s)
	}
	c.Assert(series, HasLen, 2)
	c.Assert(series["cpu1.load_one"], HasLen, 2)
	c.Assert(series["cpu2.load_one"], HasLen, 2)
	c.Assert(series["cpu1.load_one"][0]["column0"], Equals, float64(5.0))
	c.Assert(series["cpu1.load_one"][1]["column0"], Equals, float64(4.0))
	c.Assert(series["cpu2.load_one"][0]["column0"], Equals, float64(8.0))
	c.Assert(series["cpu2.load_one"][1]["column0"], Equals, float64(7.0))
}

// issue #392
func (self *DataTestSuite) TestDifferentColumnsAcrossShards(c *C) {
	i := 0.0
	serieses := CreatePointsFromFunc("test_different_columns_across_shards", 1, 1, func(_ int) float64 { i++; return i })
	now := time.Now().Truncate(24 * time.Hour)
	serieses[0].Columns = []string{"column0", "time"}
	serieses[0].Points[0] = append(serieses[0].Points[0], now.Unix())
	self.client.WriteData(serieses, c, "s")
	serieses = CreatePointsFromFunc("test_different_columns_across_shards", 2, 1, func(_ int) float64 { i++; return i })
	serieses[0].Columns = []string{"column0", "column1", "time"}
	serieses[0].Points[0] = append(serieses[0].Points[0], now.Add(-8*24*time.Hour).Unix())
	self.client.WriteData(serieses, c, "s")
	serieses = self.client.RunQuery("select * from test_different_columns_across_shards", c, "s")
	c.Assert(serieses, HasLen, 1)

	maps := ToMap(serieses[0])
	c.Assert(maps[0]["column0"], Equals, 1.0)
	c.Assert(maps[0]["column1"], IsNil)
	c.Assert(maps[1]["column0"], Equals, 2.0)
	c.Assert(maps[1]["column1"], Equals, 3.0)
}

// issue #392
func (self *DataTestSuite) TestDifferentColumnsAcrossShards2(c *C) {
	i := 0.0
	serieses := CreatePointsFromFunc("test_different_columns_across_shards_2", 1, 1, func(_ int) float64 { i++; return i })
	now := time.Now().Truncate(24 * time.Hour)
	serieses[0].Columns = []string{"column1", "time"}
	serieses[0].Points[0] = append(serieses[0].Points[0], now.Add(-13*24*time.Hour).Unix())
	self.client.WriteData(serieses, c, "s")
	serieses = CreatePointsFromFunc("test_different_columns_across_shards_2", 2, 1, func(_ int) float64 { i++; return i })
	serieses[0].Columns = []string{"column1", "column0", "time"}
	serieses[0].Points[0] = append(serieses[0].Points[0], now.Unix())
	self.client.WriteData(serieses, c, "s")
	serieses = self.client.RunQuery("select * from test_different_columns_across_shards_2", c, "s")
	c.Assert(serieses, HasLen, 1)

	maps := ToMap(serieses[0])
	c.Assert(maps[0]["column0"], Equals, 3.0)
	c.Assert(maps[0]["column1"], Equals, 2.0)
	c.Assert(maps[1]["column0"], IsNil)
	c.Assert(maps[1]["column1"], Equals, 1.0)
}

// issue #455
func (self *DataTestSuite) TestNullValuesInComparison(c *C) {
	series := []*influxdb.Series{
		{
			Name:    "foo",
			Columns: []string{"foo", "bar"},
			Points: [][]interface{}{
				{1, 2},
				{2, nil},
			},
		},
	}
	self.client.WriteData(series, c, "s")
	serieses := self.client.RunQuery("select * from foo where bar < 10", c, "s")
	c.Assert(serieses, HasLen, 1)
	maps := ToMap(serieses[0])
	c.Assert(maps, HasLen, 1)
	c.Assert(maps[0]["bar"], Equals, 2.0)
}

// issue #462 (wasn't an issue, added for regression testing only)
func (self *DataTestSuite) TestRegexWithDollarSign(c *C) {
	data := `
[
  {
    "points": [
        ["one", 1]
    ],
    "name": "test.negative.in.where.clause.1",
    "columns": ["val_1", "val_2"]
  },
  {
    "points": [
        ["two", 2]
    ],
    "name": "test.negative.in.where.clause.1.2",
    "columns": ["val_1", "val_2"]
  },
  {
    "points": [
        ["three", 3]
    ],
    "name": "test.negative.in.where.clause.a",
    "columns": ["val_1", "val_2"]
  }
]`
	self.client.WriteJsonData(data, c)
	series := self.client.RunQuery(`select * from /test\.negative\.in\.where\.clause\.\d$/ limit 1`, c, "m")
	c.Assert(series, HasLen, 1)
	c.Assert(series[0].Name, Equals, "test.negative.in.where.clause.1")
	c.Assert(series[0].Columns, HasLen, 4)
	maps := ToMap(series[0])
	c.Assert(maps, HasLen, 1)
	c.Assert(maps[0]["val_1"], Equals, "one")
	c.Assert(maps[0]["val_2"], Equals, 1.0)
}

// https://groups.google.com/forum/#!searchin/influxdb/INT_VALUE%7Csort:relevance%7Cspell:false/influxdb/9bQAuWUnDf4/cp0vtmEe65oJ
func (self *DataTestSuite) TestNegativeNumbersInWhereClause(c *C) {
	data := `
  [{
    "points": [
        ["one", -1],
        ["two", 3]
    ],
    "name": "test_negative_in_where_clause",
    "columns": ["val_1", "val_2"]
  }]`
	self.client.WriteJsonData(data, c)
	series := self.client.RunQuery("select * from test_negative_in_where_clause where val_2 = -1 limit 1", c, "m")
	c.Assert(series, HasLen, 1)
	c.Assert(series[0].Name, Equals, "test_negative_in_where_clause")
	c.Assert(series[0].Columns, HasLen, 4) // 6 columns plus the time column
	maps := ToMap(series[0])
	c.Assert(maps, HasLen, 1)
	c.Assert(maps[0]["val_2"], Equals, -1.0)
	c.Assert(maps[0]["val_1"], Equals, "one")
}

func (self *DataTestSuite) TestDistinctWithLimit(c *C) {
	data := CreatePoints("test_count_distinct_limit", 1, 1000)
	self.client.WriteData(data, c)
	series := self.client.RunQuery("select distinct(column0) from test_count_distinct_limit limit 10", c, "m")
	c.Assert(series, HasLen, 1)
	c.Assert(series[0].Columns, HasLen, 2) // 6 columns plus the time column
	c.Assert(series[0].Points, HasLen, 10)
}

func (self *DataTestSuite) TestInsensitiveRegexMatching(c *C) {
	data := `[{"name":"foo","columns":["value"],"points":[["Paul"]]}]`
	self.client.WriteJsonData(data, c)
	series := self.client.RunQuery("select * from foo where value =~ /paul/i", c, "m")
	c.Assert(series, HasLen, 1)
	c.Assert(series[0].Name, Equals, "foo")
	maps := ToMap(series[0])
	c.Assert(maps, HasLen, 1)
	c.Assert(maps[0]["value"], Equals, "Paul")
}

func (self *DataTestSuite) TestDifferentColumnsInOnePost(c *C) {
	data := `[{"name":"foo","columns":["val0", "val1"],"points":[["a", 1]]},{"name":"foo","columns":["val0"],"points":[["b"]]}]`
	self.client.WriteJsonData(data, c)
	for v0, v1 := range map[string]interface{}{"a": 1.0, "b": nil} {
		series := self.client.RunQuery(fmt.Sprintf("select * from foo where val0 = '%s'", v0), c, "m")
		c.Assert(series, HasLen, 1)
		c.Assert(series[0].Name, Equals, "foo")
		maps := ToMap(series[0])
		c.Assert(maps, HasLen, 1)
		c.Assert(maps[0]["val1"], Equals, v1)
	}
}

func (self *DataTestSuite) TestFillWithCountDistinct(c *C) {
	t1 := time.Now()
	t2 := t1.Add(-2 * time.Hour)
	data := fmt.Sprintf(`[{"name":"foo","columns":["time", "val0"],"points":[[%d, "a"],[%d, "b"]]}]`, t1.Unix(), t2.Unix())
	self.client.WriteJsonData(data, c, "s")
	series := self.client.RunQuery("select count(distinct(val0)) from foo group by time(1h) fill(0)", c, "m")
	c.Assert(series, HasLen, 1)
	maps := ToMap(series[0])
	c.Assert(maps, HasLen, 3)
	c.Assert(maps[0]["count"], Equals, 1.0)
	c.Assert(maps[1]["count"], Equals, 0.0)
	c.Assert(maps[2]["count"], Equals, 1.0)
}

func (self *DataTestSuite) TestMedians(c *C) {
	for i := 0; i < 3; i++ {
		self.client.WriteJsonData(fmt.Sprintf(`
[
  {
     "name": "test_medians",
     "columns": ["cpu", "host"],
     "points": [[%d, "hosta"], [%d, "hostb"]]
  }
]
`, 60+i*10, 70+i*10), c)
	}
	data := self.client.RunQuery("select median(cpu) from test_medians group by host;", c, "m")
	c.Assert(data[0].Name, Equals, "test_medians")
	c.Assert(data[0].Columns, HasLen, 3)
	c.Assert(data[0].Points, HasLen, 2)
	medians := map[float64]string{}
	for _, point := range data[0].Points {
		medians[point[1].(float64)] = point[2].(string)
	}
	c.Assert(medians, DeepEquals, map[float64]string{70.0: "hosta", 80.0: "hostb"})
}

// issue #34
func (self *DataTestSuite) TestAscendingQueries(c *C) {
	now := time.Now().Truncate(time.Hour)
	series := &influxdb.Series{
		Name:    "test_ascending",
		Columns: []string{"host", "time"},
		Points: [][]interface{}{
			{"hosta", now.Add(-4 * time.Second).Unix()},
		},
	}
	self.client.WriteData([]*influxdb.Series{series}, c, "s")
	series = &influxdb.Series{
		Name:    "test_ascending",
		Columns: []string{"host", "time", "cpu"},
		Points: [][]interface{}{
			{"hosta", now.Add(-time.Second).Unix(), 60},
			{"hostb", now.Add(-time.Second).Unix(), 70},
		},
	}
	self.client.WriteData([]*influxdb.Series{series}, c, "s")

	data := self.client.RunQuery("select host, cpu from test_ascending order asc", c, "s")
	c.Assert(data, HasLen, 1)
	c.Assert(data[0].Name, Equals, "test_ascending")
	c.Assert(data[0].Columns, HasLen, 4)
	c.Assert(data[0].Points, HasLen, 3)
	maps := ToMap(data[0])
	for i := 1; i < 3; i++ {
		c.Assert(maps[i]["cpu"], NotNil)
	}
}

func (self *DataTestSuite) TestFilterWithInvalidCondition(c *C) {
	data := CreatePoints("test_invalid_where_condition", 1, 1)
	self.client.WriteData(data, c)
	self.client.RunInvalidQuery("select * from test_invalid_where_condition where column0 > 0.1s", c, "m")
}

// issue #55
func (self *DataTestSuite) TestFilterWithLimit(c *C) {
	now := time.Now().Truncate(time.Hour)
	series := &influxdb.Series{
		Name:    "test_ascending",
		Columns: []string{"host", "time", "cpu"},
		Points: [][]interface{}{
			{"hosta", now.Add(-time.Second).Unix(), 60},
			{"hostb", now.Add(-time.Second).Unix(), 70},
			{"hosta", now.Add(-2 * time.Second).Unix(), 70},
			{"hostb", now.Add(-2 * time.Second).Unix(), 80},
		},
	}
	self.client.WriteData([]*influxdb.Series{series}, c)
	data := self.client.RunQuery("select host, cpu from test_ascending where host = 'hostb' order asc limit 1", c, "m")
	c.Assert(data, HasLen, 1)
	maps := ToMap(data[0])
	c.Assert(maps, HasLen, 1)
	c.Assert(maps[0]["cpu"], Equals, 80.0)
}

// issue #81
func (self *DataTestSuite) TestFilterWithInClause(c *C) {
	series := &influxdb.Series{
		Name:    "test_in_clause",
		Columns: []string{"host", "cpu"},
		Points: [][]interface{}{
			{"hosta", 60},
			{"hostb", 70},
		},
	}
	self.client.WriteData([]*influxdb.Series{series}, c)
	data := self.client.RunQuery("select host, cpu from test_in_clause where host in ('hostb')", c, "m")
	c.Assert(data, HasLen, 1)
	maps := ToMap(data[0])
	c.Assert(maps, HasLen, 1)
	c.Assert(maps[0]["host"], Equals, "hostb")
}

// issue #85
// querying a time series shouldn't add non existing columns
func (self *DataTestSuite) TestIssue85(c *C) {
	data := CreatePoints("test_issue_85", 1, 1)
	self.client.WriteData(data, c)
	_ = self.client.RunInvalidQuery("select new_column from test_issue_85", c, "m")
	data = self.client.RunQuery("select * from test_issue_85", c, "m")
	c.Assert(data, HasLen, 1)
	c.Assert(data[0].Columns, HasLen, 3)
}

// issue #92
// grouping my multiple columns fails
// Assuming the following sample data
//
// time        | fr      | to       | app        | kb
// -----------------------------------------------------
//  now() - 1hr | home    | office   | ssl        | 10
//  now() - 1hr | home    | office   | ssl        | 20
//  now() - 1hr | home    | internet | http       | 30
//  now() - 1hr | home    | office   | http       | 40
//  now()       | home    | internet | skype      | 50
//  now()       | home    | office   | lotus      | 60
//  now()       | home    | internet | skype      | 70
//
// the query `select sum(kb) from test group by time(1h), to, app`
// will cause an index out of range
func (self *DataTestSuite) TestIssue92(c *C) {
	hourAgo := time.Now().Add(-1 * time.Hour).Unix()
	now := time.Now().Unix()

	self.client.WriteJsonData(fmt.Sprintf(`
[
  {
     "name": "test_issue_92",
     "columns": ["time", "fr", "to", "app", "kb"],
     "points": [
			 [%d, "home", "office", "ssl", 10],
			 [%d, "home", "office", "ssl", 20],
			 [%d, "home", "internet", "http", 30],
			 [%d, "home", "office", "http", 40],
			 [%d, "home", "internet", "skype", 50],
			 [%d, "home", "office", "lotus", 60],
			 [%d, "home", "internet", "skype", 70]
		 ]
  }
]
`, hourAgo, hourAgo, hourAgo, hourAgo, now, now, now), c, "s")
	data := self.client.RunQuery("select sum(kb) from test_issue_92 group by time(1h), to, app", c, "m")
	c.Assert(data, HasLen, 1)
	points := ToMap(data[0])
	// use a map since the order isn't guaranteed
	sumToPointsMap := map[float64][]map[string]interface{}{}
	for _, point := range points {
		sum := point["sum"].(float64)
		sumToPointsMap[sum] = append(sumToPointsMap[sum], point)
	}
	c.Assert(sumToPointsMap[120.0], HasLen, 1)
	c.Assert(sumToPointsMap[120.0][0]["to"], Equals, "internet")
	c.Assert(sumToPointsMap[120.0][0]["app"], Equals, "skype")
	c.Assert(sumToPointsMap[60.0], HasLen, 1)
	c.Assert(sumToPointsMap[40.0], HasLen, 1)
	c.Assert(sumToPointsMap[30.0], HasLen, 2)
}

// issue #89
// Group by combined with where clause doesn't work
//
// a | b | c
// ---------
// x | y | 10
// x | y | 20
// y | z | 30
// x | z | 40
//
// `select sum(c) from test group by b where a = 'x'` should return the following:
//
// time | sum | b
// --------------
// tttt | 30  | y
// tttt | 40  | z
func (self *DataTestSuite) TestIssue89(c *C) {
	self.client.WriteJsonData(`
[
  {
     "name": "test_issue_89",
     "columns": ["a", "b", "c"],
     "points": [
			 ["x", "y", 10],
			 ["x", "y", 20],
			 ["y", "z", 30],
			 ["x", "z", 40]
		 ]
  }
]`, c)
	data := self.client.RunQuery("select sum(c) from test_issue_89 group by b where a = 'x'", c, "m")
	c.Assert(data, HasLen, 1)
	points := ToMap(data[0])
	c.Assert(points, HasLen, 2)
	sums := map[string]float64{}
	for _, p := range points {
		sums[p["b"].(string)] = p["sum"].(float64)
	}
	c.Assert(sums, DeepEquals, map[string]float64{"y": 30.0, "z": 40.0})
}

// issue #306
func (self *DataTestSuite) TestNegativeTimeInterval(c *C) {
	self.client.WriteJsonData(`
[
  {
     "name": "test_negative_interval",
     "columns": ["cpu", "host", "time"],
     "points": [[60, "hosta", -1], [70, "hostb", -2]]
  }
]
`, c)
	data := self.client.RunQuery("select count(cpu) from test_negative_interval", c, "m")
	c.Assert(data, HasLen, 1)
	c.Assert(data[0].Name, Equals, "test_negative_interval")
	c.Assert(data[0].Columns, HasLen, 2)
	c.Assert(data[0].Points, HasLen, 1)
	// count should be 3
	c.Assert(data[0].Points[0][1], Equals, 2.0)
}

// issue #306
func (self *DataTestSuite) TestShardBoundaries(c *C) {
	d := `
[
  {
     "name": "test_end_time_of_shard_is_exclusive",
     "columns": ["cpu", "host", "time"],
     "points": [[60, "hosta", -1], [70, "hostb", 0]]
  }
]
`
	self.client.WriteJsonData(d, c, "s")
	for _, query := range []string{
		"select count(cpu) from test_end_time_of_shard_is_exclusive where time > 0s",
		"select count(cpu) from test_end_time_of_shard_is_exclusive where time < 0s",
	} {
		fmt.Printf("Running query: %s\n", query)
		data := self.client.RunQuery(query, c, "s")
		c.Assert(data, HasLen, 1)
		c.Assert(data[0].Name, Equals, "test_end_time_of_shard_is_exclusive")
		c.Assert(data[0].Columns, HasLen, 2)
		c.Assert(data[0].Points, HasLen, 1)
		c.Assert(data[0].Points[0][1], Equals, 1.0)
	}
}

// make sure aggregation when happen locally at the shard level don't
// get repeated at the coordinator level, otherwise unexpected
// behavior will happen
func (self *DataTestSuite) TestCountWithGroupByTimeAndLimit(c *C) {
	data := CreatePoints("test_count_with_groupby_and_limit", 1, 2)
	self.client.WriteData(data, c)
	data = self.client.RunQuery("select count(column0) from test_count_with_groupby_and_limit group by time(5m) limit 10", c, "m")
	c.Assert(data, HasLen, 1)
	c.Assert(data[0].Name, Equals, "test_count_with_groupby_and_limit")
	c.Assert(data[0].Columns, HasLen, 2)
	c.Assert(data[0].Points, HasLen, 1)
	c.Assert(data[0].Points[0][1], Equals, 2.0)
}

func (self *DataTestSuite) TestWhereQuery(c *C) {
	data := `[{"points": [[4], [10], [5]], "name": "test_where_query", "columns": ["value"]}]`
	self.client.WriteJsonData(data, c)
	collection := self.client.RunQuery("select * from test_where_query where value < 6", c)
	c.Assert(collection, HasLen, 1)
	maps := ToMap(collection[0])
	c.Assert(maps, HasLen, 2)
	c.Assert(maps[0]["value"], Equals, 5.0)
	c.Assert(maps[1]["value"], Equals, 4.0)
}

func (self *DataTestSuite) TestCountWithGroupBy(c *C) {
	series := &influxdb.Series{
		Name:    "test_count",
		Columns: []string{"host"},
	}
	for i := 0; i < 20; i++ {
		series.Points = append(series.Points, []interface{}{"hosta"})
		series.Points = append(series.Points, []interface{}{"hostb"})
	}
	self.client.WriteData([]*influxdb.Series{series}, c)
	data := self.client.RunQuery("select count(host) from test_count group by host limit 10", c, "m")
	c.Assert(data, HasLen, 1)
	maps := ToMap(data[0])
	c.Assert(maps[0]["count"], Equals, 20.0)
}

func (self *DataTestSuite) TestCountWithAlias(c *C) {
	// generate 10 points to make sure the aggregation will be calculated
	now := time.Now().Truncate(time.Hour)
	data := CreatePoints("test_aliasing", 1, 2)
	data[0].Columns = append(data[0].Columns, "time")
	data[0].Points[0] = append(data[0].Points[0], now.Unix())
	data[0].Points[1] = append(data[0].Points[1], now.Add(-time.Second).Unix())
	self.client.WriteData(data, c)
	for _, name := range engine.GetRegisteredAggregators() {
		query := fmt.Sprintf("select %s(column0) as some_alias from test_aliasing", name)
		if name == "percentile" {
			query = "select percentile(column0, 90) as some_alias from test_aliasing"
		} else if name == "top" || name == "bottom" {
			query = fmt.Sprintf("select %s(column0, 10) as some_alias from test_aliasing", name)
		}
		fmt.Printf("query: %s\n", query)
		data := self.client.RunQuery(query, c, "m")
		c.Assert(data, HasLen, 1)
		c.Assert(data[0].Name, Equals, "test_aliasing")
		if name == "histogram" {
			c.Assert(data[0].Columns, DeepEquals, []string{"time", "some_alias_bucket_start", "some_alias_count"})
			continue
		}
		c.Assert(data[0].Columns, DeepEquals, []string{"time", "some_alias"})
	}
}

// test for issue #30
func (self *DataTestSuite) TestHttpPostWithTime(c *C) {
	now := time.Now().Add(-10 * 24 * time.Hour)
	data := CreatePoints("test_post_with_time", 1, 1)
	data[0].Columns = append(data[0].Columns, "time")
	data[0].Points[0] = append(data[0].Points[0], now.Unix())
	self.client.WriteData(data, c, "s")
	data = self.client.RunQuery("select * from test_post_with_time where time > now() - 20d", c, "m")
	c.Assert(data, HasLen, 1)
	c.Assert(data[0].Columns, HasLen, 3)
	c.Assert(data[0].Points, HasLen, 1)
}

// test limit when getting data from multiple shards
func (self *DataTestSuite) TestLimitMultipleShards(c *C) {
	self.client.WriteJsonData(`
[
  {
    "name": "test_limit_with_multiple_shards",
    "columns": ["time", "a"],
    "points":[
		  [1393577978000, 1],
		  [1383577978000, 2],
		  [1373577978000, 2],
		  [1363577978000, 2],
		  [1353577978000, 2],
		  [1343577978000, 2],
		  [1333577978000, 2],
		  [1323577978000, 2],
		  [1313577978000, 2]
	  ]
  }
]`, c, "m")
	data := self.client.RunQuery("select * from test_limit_with_multiple_shards limit 1", c, "m")
	c.Assert(data, HasLen, 1)
	c.Assert(data[0].Points, HasLen, 1)
}

// test for issue #106
func (self *DataTestSuite) TestIssue106(c *C) {
	data := CreatePoints("test_issue_106", 1, 1)
	self.client.WriteData(data, c)
	data = self.client.RunQuery("select derivative(column0) from test_issue_106", c, "m")
	c.Assert(data, HasLen, 0)
}

func (self *DataTestSuite) TestIssue105(c *C) {
	self.client.WriteJsonData(`
[
  {
    "name": "test_issue_105",
    "columns": ["time", "a", "b"],
    "points":[
		  [1386262529794, 2, 1],
		  [1386262529794, 2, null]
	  ]
  }
]`, c, "m")

	data := self.client.RunQuery("select a, b from test_issue_105 where b > 0", c, "m")
	c.Assert(data, HasLen, 1)
	maps := ToMap(data[0])
	c.Assert(maps, HasLen, 1)
	c.Assert(maps[0]["b"], Equals, 1.0)
}

func (self *DataTestSuite) TestWhereConditionWithExpression(c *C) {
	self.client.WriteJsonData(`
[
  {
    "name": "test_where_expression",
    "columns": ["time", "a", "b"],
    "points":[
		  [1386262529794, 2, 1],
		  [1386262529794, 2, 0]
	  ]
  }
]`, c, "m")
	data := self.client.RunQuery("select a, b from test_where_expression where a + b >= 3", c, "m")
	c.Assert(data, HasLen, 1)
	maps := ToMap(data[0])
	c.Assert(maps, HasLen, 1)
	c.Assert(maps[0]["b"], Equals, 1.0)
}

func (self *DataTestSuite) JoinedWithSelf(c *C) (Fun, Fun) {
	return func(client Client) {
			client.WriteJsonData(`
[
  {
    "name": "t",
    "columns": ["time", "value"],
    "points":[
		  [1381346706000, 3],
		  [1381346701000, 1]
	  ]
  }
]`, c, influxdb.Millisecond)
		}, func(client Client) {
			client.RunInvalidQuery("select * from t as foo inner join t as bar", c, "m")
		}
}

// issue #740 and #781
func (self *DataTestSuite) TestJoiningDifferentFields(c *C) {
	// TODO: why do we get a different error if we remove all but the first values
	self.client.WriteJsonData(`
[
  { "name" : "totalThreads",
    "columns" : ["time",
                 "value",
                 "hostname"],
    "points" : [ [1405364100, 16, "serverA"],
                 [1405364105, 20, "serverB"],
                 [1405364115, 16, "serverA"],
                 [1405364120, 20, "serverB"],
                 [1405364130, 18, "serverA"],
                 [1405364135, 15, "serverB"],
                 [1405364145, 19, "serverA"],
                 [1405364150, 16, "serverB"] ]
  },
  { "name" : "idleThreads",
    "columns" : ["time",
                 "value",
                 "hostname"],
    "points" : [ [1405364100, 12, "serverA"],
                 [1405364105, 9, "serverB"],
                 [1405364115, 13, "serverA"],
                 [1405364120, 12, "serverB"],
                 [1405364130, 8, "serverA"],
                 [1405364135, 13, "serverB"],
                 [1405364145, 2, "serverA"],
                 [1405364150, 13, "serverB"] ]
  }
]
`, c, "ms")
	data := self.client.RunQuery("SELECT total.hostname, total.value - idle.value FROM totalThreads AS total INNER JOIN idleThreads AS idle GROUP BY time(15s);", c, "m")
	c.Assert(data, HasLen, 1)
	maps := ToMap(data[0])
	expectedValues := []float64{3, 17, 2, 10, 8, 3, 11, 4}
	for i, m := range maps {
		c.Assert(m["expr1"], Equals, expectedValues[i])
	}
}

func (self *DataTestSuite) TestAggregateWithExpression(c *C) {
	self.client.WriteJsonData(`
[
  {
    "name": "test_aggregate_expression",
    "columns": ["time", "a", "b"],
    "points":[
		  [1386262529794, 1, 1],
		  [1386262529794, 2, 2]
	  ]
  }
]`, c, "m")
	data := self.client.RunQuery("select mean(a + b) from test_aggregate_expression", c, "m")
	c.Assert(data, HasLen, 1)
	maps := ToMap(data[0])
	c.Assert(maps, HasLen, 1)
	c.Assert(maps[0]["mean"], Equals, 3.0)
}

func (self *DataTestSuite) verifyWrite(seriesName string, value interface{}, c *C) {
	series := CreatePoints(seriesName, 1, 1)
	series[0].Columns = append(series[0].Columns, "time", "sequence_number")
	now := time.Now().Truncate(time.Hour)
	series[0].Points[0] = append(series[0].Points[0], 1.0, now.Unix())
	self.client.WriteData(series, c, "s")
	series[0].Points[0][0] = value
	self.client.WriteData(series, c, "s")
	data := self.client.RunQuery("select * from "+seriesName, c, "m")
	if value == nil {
		c.Assert(data, HasLen, 0)
		return
	}
	c.Assert(data, HasLen, 1)
	maps := ToMap(data[0])
	c.Assert(maps[0]["column0"], Equals, value)
}

func (self *DataTestSuite) TestUpdatePoint(c *C) {
	self.verifyWrite("test_updating_point", 1.0, c)
}

func (self *DataTestSuite) TestDeletePoint(c *C) {
	self.verifyWrite("test_deleting_point", nil, c)
}

func (self *DataTestSuite) TestInvalidDeleteQuery(c *C) {
	data := CreatePoints("test_invalid_delete_query", 1, 1)
	self.client.WriteData(data, c)
	_ = self.client.RunInvalidQuery("delete from test_invalid_delete_query where foo = 'bar'", c, "m")
	data = self.client.RunQuery("select * from test_invalid_delete_query", c, "m")
	c.Assert(data, HasLen, 1)
	c.Assert(data[0].Points, HasLen, 1)
}

func (self *DataTestSuite) TestReadingWhenColumnHasDot(c *C) {
	self.client.WriteJsonData(`
[
  {
     "name": "test_column_names_with_dots",
     "columns": ["first.name", "last.name"],
     "points": [["paul", "dix"], ["john", "shahid"]]
  }
]`, c)
	for name, expected := range map[string]map[string]bool{
		"first.name": {"paul": true, "john": true},
		"last.name":  {"dix": true, "shahid": true},
	} {
		q := fmt.Sprintf("select %s from test_column_names_with_dots", name)

		data := self.client.RunQuery(q, c, "m")
		c.Assert(data, HasLen, 1)
		c.Assert(data[0].Columns, HasLen, 3) // time, sequence number and the requested columns
		c.Assert(data[0].Columns[2], Equals, name)
		names := map[string]bool{}
		for _, p := range data[0].Points {
			names[p[2].(string)] = true
		}
		c.Assert(names, DeepEquals, expected)
	}
}

func (self *DataTestSuite) TestSinglePointSelect(c *C) {
	data := CreatePoints("test_single_points", 1, 2)
	self.client.WriteData(data, c)

	query := "select * from test_single_points;"
	data = self.client.RunQuery(query, c, "u")
	c.Assert(data[0].Points, HasLen, 2)
	c.Assert(data, HasLen, 1)
	expected := ToMap(data[0])
	c.Assert(expected, HasLen, 2)

	for _, point := range expected {
		query := fmt.Sprintf("select * from test_single_points where time = %.0fu and sequence_number = %0.f;", point["time"], point["sequence_number"])
		data := self.client.RunQuery(query, c, "u")
		c.Assert(data, HasLen, 1)
		maps := ToMap(data[0])
		c.Assert(maps, HasLen, 1)
		actual := maps[0]
		c.Assert(actual, HasLen, 3)
		c.Assert(actual["time"], Equals, point["time"])
	}
}

func (self *DataTestSuite) TestSinglePointSelectWithNullValues(c *C) {
	self.client.WriteJsonData(`
[
  {
     "name": "test_single_points_with_nulls",
     "columns": ["name", "age"],
     "points": [["paul", 50],["john", null]]
  }
]`, c)

	query := "select * from test_single_points_with_nulls where name='john';"
	data := self.client.RunQuery(query, c, "u")
	c.Assert(data, HasLen, 1)
	maps := ToMap(data[0])

	for _, m := range maps {
		query := fmt.Sprintf("select * from test_single_points_with_nulls where time = %.0fu and sequence_number = %0.f;", m["time"], m["sequence_number"])
		data := self.client.RunQuery(query, c, "u")
		c.Assert(data, HasLen, 1)
		actualMaps := ToMap(data[0])
		c.Assert(actualMaps, HasLen, 1)
		c.Assert(actualMaps[0]["name"], Equals, maps[0]["name"])
	}
}

func (self *DataTestSuite) TestBooleanColumnsWorkWithWhereQuery(c *C) {
	self.client.WriteJsonData(`
[
  {
    "name": "test_boolean_columns_where",
    "columns": ["a"],
    "points":[[true], [false], [true]]
  }
]`, c)
	data := self.client.RunQuery("select count(a) from test_boolean_columns_where where a = true", c, "m")
	c.Assert(data, HasLen, 1)
	c.Assert(data[0].Points, HasLen, 1)
	c.Assert(data[0].Points[0][1], Equals, 2.0)
}

func (self *DataTestSuite) TestColumnsWithOnlySomeValuesWorkWithWhereQuery(c *C) {
	self.client.WriteJsonData(`
[
  {
    "name": "test_missing_column_values",
    "columns": ["a", "b"],
    "points":[["a", "b"]]
  }
]`, c)
	self.client.WriteJsonData(`
[
  {
    "name": "test_missing_column_values",
    "columns": ["a", "b", "c"],
    "points":[["a", "b", "c"]]
  }
]`, c)
	data := self.client.RunQuery("select * from test_missing_column_values where c = 'c'", c, "m")
	c.Assert(data, HasLen, 1)
	c.Assert(data[0].Points, HasLen, 1)
}

func (self *DataTestSuite) TestSeriesListing(c *C) {
	self.client.WriteJsonData(`
[
  {
     "name": "test_series_listing",
     "columns": ["cpu", "host"],
     "points": [[99.2, "hosta"], [55.6, "hostb"]]
  }
]
`, c)
	data := self.client.RunQuery("list series", c, "m")
	c.Assert(data, HasLen, 1)
	maps := ToMap(data[0])
	names := map[string]bool{}
	for _, m := range maps {
		names[m["name"].(string)] = true
	}
	c.Assert(names["test_series_listing"], Equals, true)
}

func (self *DataTestSuite) TestArithmeticOperations(c *C) {
	queries := map[string][9]float64{
		"select input + output from test_arithmetic_3.0;":       {1, 2, 3, 4, 5, 9, 6, 7, 13},
		"select input - output from \"test_arithmetic_-1.0\";":  {1, 2, -1, 4, 5, -1, 6, 7, -1},
		"select input * output from test_arithmetic_2.0;":       {1, 2, 2, 4, 5, 20, 6, 7, 42},
		"select 1.0 * input / output from test_arithmetic_0.5;": {1, 2, 0.5, 4, 5, 0.8, 6, 8, 0.75},
	}

	for query, values := range queries {

		fmt.Printf("Running query %s\n", query)

		for i := 0; i < 3; i++ {

			data := fmt.Sprintf(`
        [
          {
             "name": "test_arithmetic_%.1f",
             "columns": ["input", "output"],
             "points": [[%f, %f]]
          }
        ]
      `, values[2], values[3*i], values[3*i+1])
			self.client.WriteJsonData(data, c)
		}
	}

	for query, values := range queries {
		data := self.client.RunQuery(query, c, "m")
		c.Assert(data, HasLen, 1)
		c.Assert(data[0].Columns, HasLen, 3)
		c.Assert(data[0].Points, HasLen, 3)
		for i, p := range data[0].Points {
			idx := 2 - i
			c.Assert(p[2], Equals, values[3*idx+2])
		}
	}
}

// issue #437
func (self *DataTestSuite) TestConstantsInArithmeticQueries(c *C) {
	data := `[{"points": [[1]], "name": "test_constants", "columns": ["value"]}]`
	self.client.WriteJsonData(data, c)
	for _, query := range []string{
		"select -1 * value from test_constants",
		"select -1.0 * value from test_constants",
	} {
		collection := self.client.RunQuery(query, c)
		c.Assert(collection, HasLen, 1)
		maps := ToMap(collection[0])
		c.Assert(maps, HasLen, 1)
		c.Assert(maps[0]["expr0"], Equals, -1.0)
	}
}

func (self *DataTestSuite) TestCountQueryOnSingleShard(c *C) {
	data := `[{"points": [[4], [10], [5]], "name": "test_count_query_single_shard", "columns": ["value"]}]`
	self.client.WriteJsonData(data, c)
	t := time.Now().Add(-time.Minute)
	data = fmt.Sprintf(`[{"points": [[2, %d]], "name": "test_count_query_single_shard", "columns": ["value", "time"]}]`, t.Unix())
	self.client.WriteJsonData(data, c, "s")
	collection := self.client.RunQuery("select count(value) from test_count_query_single_shard group by time(1m)", c)
	c.Assert(collection, HasLen, 1)
	maps := ToMap(collection[0])
	c.Assert(maps, HasLen, 2)
	c.Assert(maps[0]["count"], Equals, 3.0)
	c.Assert(maps[1]["count"], Equals, 1.0)
}

// issue #714
func (self *DataTestSuite) TestWhereClauseWithFunction(c *C) {
	serieses := CreatePoints("test_where_clause_with_function", 1, 1)
	self.client.WriteData(serieses, c)
	// Make sure the query returns an error
	self.client.RunInvalidQuery("select column0 from test_where_clause_with_function where empty(column0)", c)
}

func (self *DataTestSuite) TestGroupByDay(c *C) {
	data := `[{"points": [[4], [10], [5]], "name": "test_group_by_day", "columns": ["value"]}]`
	self.client.WriteJsonData(data, c)
	t := time.Now().Add(-24 * time.Hour).Unix()
	data = fmt.Sprintf(`[{"points": [[2, %d]], "name": "test_group_by_day", "columns": ["value", "time"]}]`, t)
	self.client.WriteJsonData(data, c, "s")
	collection := self.client.RunQuery("select count(value) from test_group_by_day group by time(1d)", c)
	c.Assert(collection, HasLen, 1)
	maps := ToMap(collection[0])
	c.Assert(maps, HasLen, 2)
	c.Assert(maps[0]["count"], Equals, 3.0)
	c.Assert(maps[1]["count"], Equals, 1.0)
}

func (self *DataTestSuite) TestLogicalGroupByBoundariesForWeek(c *C) {
	tueSep30 := time.Date(2014, 9, 30, 12, 0, 0, 0, time.UTC).Unix()
	friOct03 := time.Date(2014, 10, 3, 12, 0, 0, 0, time.UTC).Unix()
	monOct06 := time.Date(2014, 10, 6, 12, 0, 0, 0, time.UTC).Unix()

	sunSep28 := time.Date(2014, 9, 28, 0, 0, 0, 0, time.UTC).UnixNano() / int64(time.Millisecond)
	sunOct05 := time.Date(2014, 10, 5, 0, 0, 0, 0, time.UTC).UnixNano() / int64(time.Millisecond)
	data := fmt.Sprintf(`
  [{
    "name": "test_group_by_week",
    "columns": ["value", "time"],
    "points": [
      [1, %d],
      [2, %d],
      [3, %d],

      [4, %d],
      [5, %d],
      [6, %d],
      [7, %d],

      [8, %d],
      [9, %d]
    ]
  }]`, tueSep30, tueSep30, tueSep30, friOct03, friOct03, friOct03, friOct03, monOct06, monOct06)

	self.client.WriteJsonData(data, c, "s")
	collection := self.client.RunQuery("select count(value) from test_group_by_week group by time(1w)", c)
	c.Assert(collection, HasLen, 1)
	maps := ToMap(collection[0])
	c.Assert(maps, HasLen, 2)
	c.Assert(maps[0], DeepEquals, map[string]interface{}{"time": float64(sunOct05), "count": 2.0})
	c.Assert(maps[1], DeepEquals, map[string]interface{}{"time": float64(sunSep28), "count": 7.0})
}

func (self *DataTestSuite) TestLogicalGroupByBoundariesForMonth(c *C) {
	aug30 := time.Date(2014, 8, 30, 12, 0, 0, 0, time.UTC).Unix()
	aug31 := time.Date(2014, 8, 31, 12, 0, 0, 0, time.UTC).Unix()
	sep01 := time.Date(2014, 9, 1, 12, 0, 0, 0, time.UTC).Unix()
	sep30 := time.Date(2014, 9, 30, 12, 0, 0, 0, time.UTC).Unix()
	oct01 := time.Date(2014, 10, 1, 12, 0, 0, 0, time.UTC).Unix()

	aug := time.Date(2014, 8, 1, 0, 0, 0, 0, time.UTC).UnixNano() / int64(time.Millisecond)
	sep := time.Date(2014, 9, 1, 0, 0, 0, 0, time.UTC).UnixNano() / int64(time.Millisecond)
	oct := time.Date(2014, 10, 1, 0, 0, 0, 0, time.UTC).UnixNano() / int64(time.Millisecond)

	data := fmt.Sprintf(`
  [{
    "name": "test_group_by_month",
    "columns": ["value", "time"],
    "points": [
      [1, %d],
      [2, %d],
      [3, %d],

      [4, %d],
      [5, %d],
      [6, %d],
      [7, %d],
      [8, %d],
      [9, %d],

      [10, %d],
      [11, %d]
    ]
  }]`, aug30, aug31, aug31, sep01, sep01, sep01, sep01, sep30, sep30, oct01, oct01)

	self.client.WriteJsonData(data, c, "s")
	collection := self.client.RunQuery("select count(value) from test_group_by_month group by time(1M)", c)
	c.Assert(collection, HasLen, 1)
	maps := ToMap(collection[0])
	c.Assert(maps, HasLen, 3)
	c.Assert(maps[0], DeepEquals, map[string]interface{}{"time": float64(oct), "count": 2.0})
	c.Assert(maps[1], DeepEquals, map[string]interface{}{"time": float64(sep), "count": 6.0})
	c.Assert(maps[2], DeepEquals, map[string]interface{}{"time": float64(aug), "count": 3.0})
}

func (self *DataTestSuite) TestLogicalGroupByBoundariesForMonthDuringLeapYear(c *C) {
	jan31 := time.Date(2012, 1, 31, 12, 0, 0, 0, time.UTC).Unix()
	feb01 := time.Date(2012, 2, 1, 12, 0, 0, 0, time.UTC).Unix()
	feb28 := time.Date(2012, 2, 28, 12, 0, 0, 0, time.UTC).Unix()
	feb29 := time.Date(2012, 2, 29, 12, 0, 0, 0, time.UTC).Unix()
	mar01 := time.Date(2012, 3, 1, 12, 0, 0, 0, time.UTC).Unix()

	jan := time.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano() / int64(time.Millisecond)
	feb := time.Date(2012, 2, 1, 0, 0, 0, 0, time.UTC).UnixNano() / int64(time.Millisecond)
	mar := time.Date(2012, 3, 1, 0, 0, 0, 0, time.UTC).UnixNano() / int64(time.Millisecond)

	data := fmt.Sprintf(`
  [{
    "name": "test_group_by_month",
    "columns": ["value", "time"],
    "points": [
      [1, %d],
      [2, %d],
      [3, %d],

      [4, %d],
      [5, %d],
      [6, %d],
      [7, %d],
      [8, %d],
      [9, %d],

      [10, %d],
      [11, %d]
    ]
  }]`, jan31, jan31, jan31, feb01, feb01, feb28, feb28, feb28, feb29, mar01, mar01)

	self.client.WriteJsonData(data, c, "s")
	collection := self.client.RunQuery("select count(value) from test_group_by_month group by time(1M)", c)
	c.Assert(collection, HasLen, 1)
	maps := ToMap(collection[0])
	c.Assert(maps, HasLen, 3)
	c.Assert(maps[0], DeepEquals, map[string]interface{}{"time": float64(mar), "count": 2.0})
	c.Assert(maps[1], DeepEquals, map[string]interface{}{"time": float64(feb), "count": 6.0})
	c.Assert(maps[2], DeepEquals, map[string]interface{}{"time": float64(jan), "count": 3.0})
}

func (self *DataTestSuite) TestLogicalGroupByBoundariesForYear(c *C) {
	dec31of2012 := time.Date(2012, 12, 31, 12, 0, 0, 0, time.UTC).Unix()
	jan01of2013 := time.Date(2013, 1, 1, 12, 0, 0, 0, time.UTC).Unix()
	feb01of2013 := time.Date(2013, 2, 1, 12, 0, 0, 0, time.UTC).Unix()
	dec31of2013 := time.Date(2013, 12, 31, 12, 0, 0, 0, time.UTC).Unix()
	jan01of2014 := time.Date(2014, 1, 1, 12, 0, 0, 0, time.UTC).Unix()

	year2012 := time.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano() / int64(time.Millisecond)
	year2013 := time.Date(2013, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano() / int64(time.Millisecond)
	year2014 := time.Date(2014, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano() / int64(time.Millisecond)

	data := fmt.Sprintf(`
  [{
    "name": "test_group_by_month",
    "columns": ["value", "time"],
    "points": [
      [1, %d],

      [2, %d],
      [3, %d],
      [4, %d],
      [5, %d],
      [6, %d],
      [7, %d],
      [8, %d],
      [9, %d],

      [10, %d],
      [11, %d]
    ]
  }]`, dec31of2012, jan01of2013, jan01of2013, feb01of2013, feb01of2013, feb01of2013, feb01of2013, dec31of2013, dec31of2013, jan01of2014, jan01of2014)

	self.client.WriteJsonData(data, c, "s")
	collection := self.client.RunQuery("select count(value) from test_group_by_month group by time(1Y)", c)
	c.Assert(collection, HasLen, 1)
	maps := ToMap(collection[0])
	c.Assert(maps, HasLen, 3)
	c.Assert(maps[0], DeepEquals, map[string]interface{}{"time": float64(year2014), "count": 2.0})
	c.Assert(maps[1], DeepEquals, map[string]interface{}{"time": float64(year2013), "count": 8.0})
	c.Assert(maps[2], DeepEquals, map[string]interface{}{"time": float64(year2012), "count": 1.0})
}

func (self *DataTestSuite) TestLimitQueryOnSingleShard(c *C) {
	data := `[{"points": [[4], [10], [5]], "name": "test_limit_query_single_shard", "columns": ["value"]}]`
	self.client.WriteJsonData(data, c)
	collection := self.client.RunQuery("select * from test_limit_query_single_shard limit 2", c)
	c.Assert(collection, HasLen, 1)
	maps := ToMap(collection[0])
	c.Assert(maps, HasLen, 2)
	c.Assert(maps[0]["value"], Equals, 5.0)
	c.Assert(maps[1]["value"], Equals, 10.0)
}

func (self *DataTestSuite) TestQueryAgainstMultipleShards(c *C) {
	data := `[{"points": [[4], [10], [5]], "name": "test_query_against_multiple_shards", "columns": ["value"]}]`
	self.client.WriteJsonData(data, c)
	t := time.Now().Add(-14 * 24 * time.Hour).Unix()
	data = fmt.Sprintf(`[{"points": [[2, %d]], "name": "test_query_against_multiple_shards", "columns": ["value", "time"]}]`, t)
	self.client.WriteJsonData(data, c, "s")
	collection := self.client.RunQuery("select count(value) from test_query_against_multiple_shards group by time(1h)", c)
	c.Assert(collection, HasLen, 1)
	maps := ToMap(collection[0])
	c.Assert(maps, HasLen, 2)
	c.Assert(maps[0]["count"], Equals, 3.0)
	c.Assert(maps[1]["count"], Equals, 1.0)
}

func (self *DataTestSuite) TestQueryAscendingAgainstMultipleShards(c *C) {
	data := `[{"points": [[4], [10]], "name": "test_ascending_against_multiple_shards", "columns": ["value"]}]`
	self.client.WriteJsonData(data, c)
	t := time.Now().Add(-14 * 24 * time.Hour).Unix()
	data = fmt.Sprintf(`[{"points": [[2, %d]], "name": "test_ascending_against_multiple_shards", "columns": ["value", "time"]}]`, t)
	self.client.WriteJsonData(data, c, "s")
	collection := self.client.RunQuery("select * from test_ascending_against_multiple_shards order asc", c)
	c.Assert(collection, HasLen, 1)
	maps := ToMap(collection[0])
	c.Assert(maps, HasLen, 3)
	c.Assert(maps[0]["value"], Equals, 2.0)
	c.Assert(maps[1]["value"], Equals, 4.0)
	c.Assert(maps[2]["value"], Equals, 10.0)
}

func (self *DataTestSuite) TestBigGroupByQueryAgainstMultipleShards(c *C) {
	duration := int64(30 * 24 * time.Hour)
	first := time.Unix(time.Now().Unix()/duration*duration, 0).Add(7 * 24 * time.Hour)

	data := fmt.Sprintf(
		`[{"points": [[4, %d], [10, %d]], "name": "test_multiple_shards_big_group_by", "columns": ["value", "time"]}]`,
		first.Unix(), first.Unix())
	self.client.WriteJsonData(data, c)
	t := first.Add(7 * 24 * time.Hour).Unix()
	data = fmt.Sprintf(`[{"points": [[2, %d]], "name": "test_multiple_shards_big_group_by", "columns": ["value", "time"]}]`, t)
	self.client.WriteJsonData(data, c, "s")
	collection := self.client.RunQuery("select count(value) from test_multiple_shards_big_group_by group by time(30d)", c)
	c.Assert(collection, HasLen, 1)
	maps := ToMap(collection[0])
	c.Assert(maps, HasLen, 1)
	c.Assert(maps[0]["count"], Equals, 3.0)
}

func (self *DataTestSuite) TestWriteSplitToMultipleShards(c *C) {
	data := `[
		{"points": [[4], [10]], "name": "test_write_multiple_shards", "columns": ["value"]},
		{"points": [["asdf"]], "name": "Test_write_multiple_shards", "columns": ["thing"]}]`
	self.client.WriteJsonData(data, c)
	collection := self.client.RunQuery("select count(value) from test_write_multiple_shards", c)
	c.Assert(collection, HasLen, 1)
	maps := ToMap(collection[0])
	c.Assert(maps, HasLen, 1)
	c.Assert(maps[0]["count"], Equals, 2.0)

	collection = self.client.RunQuery("select * from Test_write_multiple_shards", c)
	c.Assert(collection, HasLen, 1)
	maps = ToMap(collection[0])
	c.Assert(maps, HasLen, 1)
	c.Assert(maps[0]["thing"], Equals, "asdf")
}

func (self *DataTestSuite) TestCountDistinctWithNullValues(c *C) {
	data := `[{"name":"test_null_with_distinct","columns":["column"],"points":[["value1"], [null], ["value2"], ["value1"], [null]]}]`
	self.client.WriteJsonData(data, c)
	collection := self.client.RunQuery("select count(distinct(column)) from test_null_with_distinct group by time(1m)", c)
	c.Assert(collection, HasLen, 1)
	maps := ToMap(collection[0])
	c.Assert(maps, HasLen, 1)
	c.Assert(maps[0]["count"], Equals, 2.0)
}

// issue #147
func (self *DataTestSuite) TestExtraSequenceNumberColumns(c *C) {
	data := `
  [{
    "points": [
        ["foo", 1390852524, 1234]
    ],
    "name": "test_extra_sequence_number_column",
    "columns": ["val_1", "time", "sequence_number"]
  }]`
	self.client.WriteJsonData(data, c)
	collection := self.client.RunQuery("select * from test_extra_sequence_number_column", c)
	c.Assert(collection, HasLen, 1)
	maps := ToMap(collection[0])
	c.Assert(maps, HasLen, 1)
	c.Assert(maps[0]["sequence_number"], Equals, 1234.0)
}

// issue #206
func (self *DataTestSuite) TestUnicodeSupport(c *C) {
	data := `
  [{
    "points": [
        ["山田太郎", "中文", "⚑"]
    ],
    "name": "test_unicode",
    "columns": ["val_1", "val_2", "val_3"]
  }]`
	self.client.WriteJsonData(data, c)
	collection := self.client.RunQuery("select * from test_unicode", c)
	c.Assert(collection, HasLen, 1)
	maps := ToMap(collection[0])
	c.Assert(maps[0]["val_1"], Equals, "山田太郎")
	c.Assert(maps[0]["val_2"], Equals, "中文")
	c.Assert(maps[0]["val_3"], Equals, "⚑")
}
func (self *DataTestSuite) TestSelectingTimeColumn(c *C) {
	data := `[{
		"name": "selecting_time_column",
		"columns": ["val1"],
		"points": [[1]]
		}]`
	self.client.WriteJsonData(data, c)
	collection := self.client.RunQuery("select val1, time from selecting_time_column", c)
	c.Assert(collection, HasLen, 1)
	c.Assert(collection[0].Columns, HasLen, 3)
	c.Assert(collection[0].Points, HasLen, 1)
}

// For issue #130 https://github.com/influxdb/influxdb/issues/130
func (self *DataTestSuite) TestColumnNamesReturnInDistributedQuery(c *C) {
	data := `[{
		"name": "cluster_query_with_columns",
		"columns": ["col1"],
		"points": [[1], [2]]
		}]`
	self.client.WriteJsonData(data, c)
	collection := self.client.RunQuery("select * from cluster_query_with_columns", c)
	c.Assert(collection, HasLen, 1)
	maps := ToMap(collection[0])
	set := map[float64]bool{}
	for idx := range maps {
		set[maps[idx]["col1"].(float64)] = true
	}
	c.Assert(set, DeepEquals, map[float64]bool{1: true, 2: true})
}
func (self *DataTestSuite) TestLimitWithRegex(c *C) {
	batchSize := []int{100, 1000}
	// run the test once with less than POINT_BATCH_SIZE points and once
	// with more than POINT_BATCH_SIZE points
	series := []*influxdb.Series{}
	for _, numberOfPoints := range batchSize {
		for i := 0; i < 100; i++ {
			series = append(series, CreatePointsFromFunc(fmt.Sprintf("limit_with_regex_%d_%d", numberOfPoints, i), 1, numberOfPoints,
				func(i int) float64 { return float64(i) },
			)...)
		}
	}
	self.client.WriteData(series, c)
	for _, numberOfPoints := range batchSize {
		query := fmt.Sprintf("select * from /.*limit_with_regex_%d.*/ limit 1", numberOfPoints)
		collection := self.client.RunQuery(query, c)
		// make sure all series get back 1 point only
		series := map[string][]map[string]interface{}{}
		for _, s := range collection {
			series[s.Name] = ToMap(s)
		}
		for i := 0; i < 100; i++ {
			table := fmt.Sprintf("limit_with_regex_%d_%d", numberOfPoints, i)
			c.Assert(series[table], HasLen, 1)
			c.Assert(series[table][0]["column0"], Equals, float64(numberOfPoints-1))
		}
	}
}

// For issue #131 https://github.com/influxdb/influxdb/issues/131
func (self *DataTestSuite) TestSelectFromRegexInCluster(c *C) {
	data := `[{
		"name": "cluster_regex_query",
		"columns": ["col1", "col2"],
		"points": [[1, "foo"], [23, "bar"]]
		},{
			"name": "cluster_regex_query_2",
			"columns": ["blah"],
			"points": [[true]]
		},{
			"name": "cluster_regex_query_3",
			"columns": ["foobar"],
			"points": [["asdf"]]
			}]`
	self.client.WriteJsonData(data, c)
	collection := self.client.RunQuery("select * from /.*/ limit 1", c)
	c.Assert(collection, HasLen, 3)
	series := map[string][]map[string]interface{}{}
	for _, s := range collection {
		series[s.Name] = ToMap(s)
	}
	c.Assert(series["cluster_regex_query"], HasLen, 1)
	c.Assert(series["cluster_regex_query"][0]["col1"], Equals, 23.0)
	c.Assert(series["cluster_regex_query"][0]["col2"], Equals, "bar")
	c.Assert(series["cluster_regex_query_2"], HasLen, 1)
	c.Assert(series["cluster_regex_query_2"][0]["blah"], Equals, true)
	c.Assert(series["cluster_regex_query_3"], HasLen, 1)
	c.Assert(series["cluster_regex_query_3"][0]["foobar"], Equals, "asdf")
}

func (self *DataTestSuite) TestListSeries(c *C) {
	data := `[
        {"points": [[1]], "name": "cluster_query", "columns": ["value"]},
			  {"points": [[2]], "name": "another_query", "columns": ["value"]}
      ]`
	self.client.WriteJsonData(data, c, "s")
	collection := self.client.RunQuery("list series", c)
	c.Assert(collection, HasLen, 1)
	maps := ToMap(collection[0])
	names := map[string]bool{}
	for _, m := range maps {
		names[m["name"].(string)] = true
	}
	c.Assert(names["cluster_query"], Equals, true)
	c.Assert(names["another_query"], Equals, true)
}

// For issue #267 - allow all characters in series name - https://github.com/influxdb/influxdb/issues/267
func (self *DataTestSuite) TestSeriesNameWithWeirdCharacters(c *C) {
	data := `[
  	  	{
	      	"name": "/blah ( ) ; : ! @ # $ \n \t,foo\"=bar/baz",
      		"columns": ["value"],
    	  	"points": [[1]]
  	  	}
	  	]`
	self.client.WriteJsonData(data, c, "s")
	result := self.client.RunQuery("select value from \"/blah ( ) ; : ! @ # $ \n \t,foo\\\"=bar/baz\"", c)
	c.Assert(result, HasLen, 1)
	c.Assert(result[0].Points, HasLen, 1)
	c.Assert(result[0].Name, Equals, "/blah ( ) ; : ! @ # $ \n \t,foo\"=bar/baz")
}

// For issue #466 - allow all characters in column names - https://github.com/influxdb/influxdb/issues/267
func (self *DataTestSuite) TestColumnNameWithWeirdCharacters(c *C) {
	data := `[
  	  	{
	      	"name": "foo",
      		"columns": ["baz.-239(*@&#$!#)(* #$@"],
    	  	"points": [[1]]
  	  	}
	  	]`
	self.client.WriteJsonData(data, c, "s")
	result := self.client.RunQuery(`select "baz.-239(*@&#$!#)(* #$@" from foo`, c)
	c.Assert(result, HasLen, 1)
	c.Assert(result[0].Points, HasLen, 1)
	c.Assert(result[0].Name, Equals, "foo")
	c.Assert(result[0].Columns, HasLen, 3)
	c.Assert(result[0].Columns[2], Equals, `baz.-239(*@&#$!#)(* #$@`)
}

// For issue #551 - add aggregate function top and bottom - https://github.com/influxdb/influxdb/issues/551
func (self *DataTestSuite) TestTop(c *C) {
	for i := 0; i < 3; i++ {
		self.client.WriteJsonData(fmt.Sprintf(`
[
  {
     "name": "test_top",
     "columns": ["cpu", "host"],
     "points": [[%d, "hosta"], [%d, "hostb"]]
  }
]
`, 60+i*10, 70+i*10), c)
	}
	data := self.client.RunQuery("select top(cpu, 5) from test_top;", c, "m")
	c.Assert(data[0].Name, Equals, "test_top")
	c.Assert(data[0].Columns, HasLen, 2)
	c.Assert(data[0].Points, HasLen, 5)

	tops := []float64{}
	for _, point := range data[0].Points {
		tops = append(tops, point[1].(float64))
	}
	c.Assert(tops, DeepEquals, []float64{90, 80, 80, 70, 70})
}

func (self *DataTestSuite) TestTopWithStringColumn(c *C) {
	for i := 0; i < 3; i++ {
		self.client.WriteJsonData(fmt.Sprintf(`
[
  {
     "name": "test_top",
     "columns": ["cpu", "host"],
     "points": [[%d, "hosta"], [%d, "hostb"]]
  }
]
`, 60+i*10, 70+i*10), c)
	}
	data := self.client.RunQuery("select top(host, 5) from test_top;", c, "m")
	c.Assert(data[0].Name, Equals, "test_top")
	c.Assert(data[0].Columns, HasLen, 2)
	c.Assert(data[0].Points, HasLen, 5)

	tops := []string{}
	for _, point := range data[0].Points {
		tops = append(tops, point[1].(string))
	}
	c.Assert(tops, DeepEquals, []string{"hostb", "hostb", "hostb", "hosta", "hosta"})
}

func (self *DataTestSuite) TestTopWithGroupBy(c *C) {
	for i := 0; i < 3; i++ {
		self.client.WriteJsonData(fmt.Sprintf(`
[
  {
     "name": "test_top",
     "columns": ["cpu", "host"],
     "points": [[%d, "hosta"], [%d, "hostb"]]
  }
]
`, 60+i*10, 70+i*10), c)
	}
	data := self.client.RunQuery("select top(cpu, 2), host from test_top group by host;", c, "m")
	c.Assert(data[0].Name, Equals, "test_top")
	c.Assert(data[0].Columns, HasLen, 3)
	c.Assert(data[0].Points, HasLen, 4)

	type tmp struct {
		cpu  float64
		host string
	}
	tops := []tmp{}
	for _, point := range data[0].Points {
		tops = append(tops, tmp{point[1].(float64), point[2].(string)})
	}
	c.Assert(tops, DeepEquals, []tmp{{80, "hosta"}, {70, "hosta"}, {90, "hostb"}, {80, "hostb"}})
}

func (self *DataTestSuite) TestTopWithMultipleGroupBy(c *C) {
	for i := 0; i < 3; i++ {
		self.client.WriteJsonData(fmt.Sprintf(`
[
  {
     "name": "test_top",
     "columns": ["time", "cpu", "host"],
     "points": [[%d, %d, "hosta"], [%d, %d, "hostb"]]
  }
]
`, 1400504400+i*60, 60+i*10, 1400504400+i*60, 70+i*10), c, "s")
	}
	data := self.client.RunQuery("select top(cpu, 2), host from test_top group by time(1d), host;", c, "m")
	c.Assert(data[0].Name, Equals, "test_top")
	c.Assert(data[0].Columns, HasLen, 3)

	type tmp struct {
		// TODO(chobie): add time column
		cpu  float64
		host string
	}
	tops := []tmp{}
	for _, point := range data[0].Points {
		tops = append(tops, tmp{point[1].(float64), point[2].(string)})
	}
	c.Assert(tops, DeepEquals, []tmp{{80, "hosta"}, {70, "hosta"}, {90, "hostb"}, {80, "hostb"}})
}

func (self *DataTestSuite) TestTopWithLessResult(c *C) {
	for i := 0; i < 5; i++ {
		self.client.WriteJsonData(fmt.Sprintf(`
[
  {
     "name": "test_top",
     "columns": ["cpu", "host"],
     "points": [[%d, "hosta"], [%d, "hostb"]]
  }
]
`, 40+i*10, 50+i*10), c)
	}
	data := self.client.RunQuery("select top(cpu, 20) from test_top;", c, "m")
	c.Assert(data[0].Name, Equals, "test_top")
	c.Assert(data[0].Columns, HasLen, 2)
	// top collects result as possible
	c.Assert(data[0].Points, HasLen, 10)

	tops := []float64{}
	for _, point := range data[0].Points {
		tops = append(tops, point[1].(float64))
	}
	c.Assert(tops, DeepEquals, []float64{90, 80, 80, 70, 70, 60, 60, 50, 50, 40})
}

func (self *DataTestSuite) TestBottom(c *C) {
	for i := 0; i < 3; i++ {
		self.client.WriteJsonData(fmt.Sprintf(`
[
  {
     "name": "test_bottom",
     "columns": ["cpu", "host"],
     "points": [[%d, "hosta"], [%d, "hostb"]]
  }
]
`, 60+i*10, 70+i*10), c)
	}
	data := self.client.RunQuery("select bottom(cpu, 5) from test_bottom;", c, "m")
	c.Assert(data[0].Name, Equals, "test_bottom")
	c.Assert(data[0].Columns, HasLen, 2)
	c.Assert(data[0].Points, HasLen, 5)

	tops := []float64{}
	for _, point := range data[0].Points {
		tops = append(tops, point[1].(float64))
	}
	c.Assert(tops, DeepEquals, []float64{60, 70, 70, 80, 80})
}

func (self *DataTestSuite) TestBottomWithStringColumn(c *C) {
	for i := 0; i < 3; i++ {
		self.client.WriteJsonData(fmt.Sprintf(`
[
  {
     "name": "test_bottom",
     "columns": ["cpu", "host"],
     "points": [[%d, "hosta"], [%d, "hostb"]]
  }
]
`, 60+i*10, 70+i*10), c)
	}
	data := self.client.RunQuery("select bottom(host, 5) from test_bottom;", c, "m")
	c.Assert(data[0].Name, Equals, "test_bottom")
	c.Assert(data[0].Columns, HasLen, 2)
	c.Assert(data[0].Points, HasLen, 5)

	tops := []string{}
	for _, point := range data[0].Points {
		tops = append(tops, point[1].(string))
	}
	c.Assert(tops, DeepEquals, []string{"hosta", "hosta", "hosta", "hostb", "hostb"})
}

func (self *DataTestSuite) TestBottomWithGroupBy(c *C) {
	for i := 0; i < 3; i++ {
		self.client.WriteJsonData(fmt.Sprintf(`
[
  {
     "name": "test_bottom",
     "columns": ["cpu", "host"],
     "points": [[%d, "hosta"], [%d, "hostb"]]
  }
]
`, 60+i*10, 70+i*10), c)
	}
	data := self.client.RunQuery("select bottom(cpu, 2), host from test_bottom group by host;", c, "m")
	c.Assert(data[0].Name, Equals, "test_bottom")
	c.Assert(data[0].Columns, HasLen, 3)

	type tmp struct {
		cpu  float64
		host string
	}
	tops := []tmp{}
	for _, point := range data[0].Points {
		tops = append(tops, tmp{point[1].(float64), point[2].(string)})
	}
	c.Assert(tops, DeepEquals, []tmp{{60, "hosta"}, {70, "hosta"}, {70, "hostb"}, {80, "hostb"}})
}

func (self *DataTestSuite) TestBottomWithLessResult(c *C) {
	for i := 0; i < 5; i++ {
		self.client.WriteJsonData(fmt.Sprintf(`
[
  {
     "name": "test_bottom",
     "columns": ["cpu", "host"],
     "points": [[%d, "hosta"], [%d, "hostb"]]
  }
]
`, 40+i*10, 50+i*10), c)
	}
	data := self.client.RunQuery("select bottom(cpu, 20) from test_bottom;", c, "m")
	c.Assert(data[0].Name, Equals, "test_bottom")
	c.Assert(data[0].Columns, HasLen, 2)
	// bottom collects result as possible
	c.Assert(data[0].Points, HasLen, 10)

	bottoms := []float64{}
	for _, point := range data[0].Points {
		bottoms = append(bottoms, point[1].(float64))
	}
	c.Assert(bottoms, DeepEquals, []float64{40, 50, 50, 60, 60, 70, 70, 80, 80, 90})
}

func (self *DataTestSuite) TestBottomWithMultipleGroupBy(c *C) {
	for i := 0; i < 3; i++ {
		self.client.WriteJsonData(fmt.Sprintf(`
[
  {
     "name": "test_bottom",
     "columns": ["time", "cpu", "host"],
     "points": [[%d, %d, "hosta"], [%d, %d, "hostb"]]
  }
]
`, 1400504400+i*60, 60+i*10, 1400504400+i*60, 70+i*10), c)
	}
	data := self.client.RunQuery("select bottom(cpu, 2), host from test_bottom group by time(1d), host;", c, "m")
	c.Assert(data[0].Name, Equals, "test_bottom")
	c.Assert(data[0].Columns, HasLen, 3)

	type tmp struct {
		// TODO(chobie): add time column
		cpu  float64
		host string
	}
	tops := []tmp{}
	for _, point := range data[0].Points {
		tops = append(tops, tmp{point[1].(float64), point[2].(string)})
	}
	c.Assert(tops, DeepEquals, []tmp{{60, "hosta"}, {70, "hosta"}, {70, "hostb"}, {80, "hostb"}})
}

// issue #557
func (self *DataTestSuite) TestGroupByYear(c *C) {
	data := `[{"points": [[4], [10], [5]], "name": "test_group_by_day", "columns": ["value"]}]`
	self.client.WriteJsonData(data, c)
	t := time.Now().Truncate(time.Hour).Add(-24 * 365 * time.Hour).Unix()
	data = fmt.Sprintf(`[{"points": [[2, %d]], "name": "test_group_by_day", "columns": ["value", "time"]}]`, t)
	self.client.WriteJsonData(data, c, "s")
	collection := self.client.RunQuery("select count(value) from test_group_by_day group by time(1y)", c)
	c.Assert(collection, HasLen, 1)
	maps := ToMap(collection[0])
	c.Assert(maps, HasLen, 2)
	c.Assert(maps[0]["count"], Equals, 3.0)
	c.Assert(maps[1]["count"], Equals, 1.0)
}

// Issue #713
// Test various fill options
func (self *DataTestSuite) TestMeanAggregateFillWithZero(c *C) {
	data := `
[
  {
    "points": [
    [1304378375, 10.0],
    [1304378380, 20.0],
    [1304378400, 60.0]
    ],
    "name": "test_fill_zero",
    "columns": ["time", "value"]
  }
]`
	self.client.WriteJsonData(data, c, influxdb.Second)
	queries := map[string]interface{}{
		"select mean(value) from test_fill_zero group by time(10s)":            math.Inf(1),
		"select mean(value) from test_fill_zero group by time(10s) fill(0)":    0.0,
		"select mean(value) from test_fill_zero group by time(10s) fill(-42)":  -42.0,
		"select mean(value) from test_fill_zero group by time(10s) fill(42)":   42.0,
		"select mean(value) from test_fill_zero group by time(10s) fill(null)": nil,
	}

	for query, expectedValue := range queries {
		serieses := self.client.RunQuery(query, c)
		c.Assert(serieses, HasLen, 1)
		maps := ToMap(serieses[0])
		c.Assert(maps[0], DeepEquals, map[string]interface{}{"time": 1304378400000.0, "mean": 60.0})
		v, ok := expectedValue.(float64)
		// we assign math.Inf for the no fill() case
		if ok && math.IsInf(v, 1) {
			c.Assert(maps[1], DeepEquals, map[string]interface{}{"time": 1304378380000.0, "mean": 20.0})
			c.Assert(maps[2], DeepEquals, map[string]interface{}{"time": 1304378370000.0, "mean": 10.0})
			continue
		}
		c.Assert(maps[1], DeepEquals, map[string]interface{}{"time": 1304378390000.0, "mean": expectedValue})
		c.Assert(maps[2], DeepEquals, map[string]interface{}{"time": 1304378380000.0, "mean": 20.0})
		c.Assert(maps[3], DeepEquals, map[string]interface{}{"time": 1304378370000.0, "mean": 10.0})
	}
}

// issue #669
func HistogramHelper(c *C, client Client, query string, expected map[float64]float64) {
	//Test basic histogram
	collection := client.RunQuery(query, c)

	c.Assert(collection, HasLen, 1)
	maps := ToMap(collection[0])
	actual := make(map[float64]float64, len(maps))
	for key := range maps {
		c.Logf(fmt.Sprintf(`%d: bucket_start: %f count: %f`, key, maps[key]["bucket_start"], maps[key]["count"]))
		actual[maps[key]["bucket_start"].(float64)] = maps[key]["count"].(float64)
	}
	c.Assert(actual, HasLen, len(expected))
	for bucket, count := range expected {
		c.Assert(actual[bucket], Equals, count)
	}
}

func (self *DataTestSuite) TestHistogram(c *C) {
	c.Logf("Running Histogram test")
	data := `[{"points": [[-3], [-2], [-1], [0], [1], [2], [3]], "name": "test_histogram", "columns": ["value"]}]`
	self.client.WriteJsonData(data, c)
	//Test basic histogram
	expected := make(map[float64]float64, 7)
	expected[-3.0] = 1.0
	expected[-2.0] = 1.0
	expected[-1.0] = 1.0
	expected[0.0] = 1.0
	expected[1.0] = 1.0
	expected[2.0] = 1.0
	expected[3.0] = 1.0
	HistogramHelper(c, self.client, "select Histogram(value, 1.0) from test_histogram", expected)

	// Test specifying start and stop
	HistogramHelper(c, self.client, "select Histogram(value, 1.0, -3, 3) from test_histogram", expected)

	// Test specifying start and stop outside domain of data
	expected = make(map[float64]float64, 21)
	expected[-10.0] = 0.0
	expected[-9.0] = 0.0
	expected[-8.0] = 0.0
	expected[-7.0] = 0.0
	expected[-6.0] = 0.0
	expected[-5.0] = 0.0
	expected[-4.0] = 0.0
	expected[-3.0] = 1.0
	expected[-2.0] = 1.0
	expected[-1.0] = 1.0
	expected[0.0] = 1.0
	expected[1.0] = 1.0
	expected[2.0] = 1.0
	expected[3.0] = 1.0
	expected[4.0] = 0.0
	expected[5.0] = 0.0
	expected[6.0] = 0.0
	expected[7.0] = 0.0
	expected[8.0] = 0.0
	expected[9.0] = 0.0
	expected[10.0] = 0.0
	HistogramHelper(c, self.client, "select Histogram(value, 1.0, -10, 10) from test_histogram", expected)

	// Test specifying start and stop inside domain of data
	expected = make(map[float64]float64, 2)
	expected[-1.0] = 1.0
	expected[0.0] = 1.0
	HistogramHelper(c, self.client, "select Histogram(value, 1.0, -1, 0) from test_histogram", expected)

	// Test specifying step and start that don't align with 0
	expected = make(map[float64]float64, 4)
	expected[-3.0] = 2.0
	expected[-1.0] = 2.0
	expected[1.0] = 2.0
	expected[3.0] = 1.0
	HistogramHelper(c, self.client, "select Histogram(value, 2.0, -3) from test_histogram", expected)
	HistogramHelper(c, self.client, "select Histogram(value, 2.0, -3, 3) from test_histogram", expected)

	// Test specifying step, start and stop that don't align with 0 inside the domain
	expected = make(map[float64]float64, 3)
	expected[-3.0] = 2.0
	expected[-1.0] = 2.0
	expected[1.0] = 2.0
	HistogramHelper(c, self.client, "select Histogram(value, 2.0, -3, 1) from test_histogram", expected)

	// Test specifying step and start that don't align with stop
	expected = make(map[float64]float64, 2)
	expected[-1.0] = 2.0
	expected[1.0] = 2.0
	HistogramHelper(c, self.client, "select Histogram(value, 2.0, -1, 2) from test_histogram", expected)
}

// Test data and expected result data
var (
	aggTstData = `
[
  {
    "points": [
    [300000, 30.0],
    [240000, null],
    [120000, 20.0],
    [60000, 10.0]
    ],
    "name": "data",
    "columns": ["time", "value"]
  }
]`

	aggTstExpect_FillWithNil = []tv{{300000.0, 30.0}, {240000.0, nil}, {180000.0, nil}, {120000.0, 20.0}, {60000.0, 10.0}}
	aggTstExpect_FillWith0   = []tv{{300000.0, 30.0}, {240000.0, 0.0}, {180000.0, 0.0}, {120000.0, 20.0}, {60000.0, 10.0}}

	aggTstExpect_ZerosAndFillWithNil = []tv{{300000.0, 0.0}, {240000.0, nil}, {180000.0, nil}, {120000.0, 0.0}, {60000.0, 0.0}}
	aggTstExpect_ZerosAndFillWith0   = []tv{{300000.0, 0.0}, {240000.0, 0.0}, {180000.0, 0.0}, {120000.0, 0.0}, {60000.0, 0.0}}

	aggTstData2 = `
[
  {
    "points": [
    [310000, 400.0],
    [300000, 30.0],
    [120000, 20.0],
    [60000, 5.0]
    ],
    "name": "data",
    "columns": ["time", "value"]
  }
]`

	aggTstData_Issue939 = `
[
  {
    "points": [
    [300000, 30.0],
    [240000, null],
    [180000, "foo"],
    [120000, 20.0],
    [60000, 10.0]
    ],
    "name": "data",
    "columns": ["time", "value"]
  }
]`
)

// code that's common to many of the folling Test*AggregateFillWith* tests
func (self *DataTestSuite) tstAggregateFill(tstData, aggregate, fill string, aggArgs []interface{}, expVals []tv, c *C) {
	// write test data to the database
	self.client.WriteJsonData(tstData, c, influxdb.Millisecond)
	// build the test query string
	query := fmtQuery(aggregate, aggArgs, "data", fill)
	// run the query
	series := self.client.RunQuery(query, c)
	// check that we only got one result series
	c.Assert(len(series), Equals, 1)
	// convert result series to a map of strings to values
	maps := ToMap(series[0])
	fmt.Println(maps)
	// check that the result has the expected number of records
	c.Assert(len(maps), Equals, len(expVals))
	// check each result value
	for i, ev := range expVals {
		expVal := map[string]interface{}{"time": ev.Time, aggregate: ev.Value}
		c.Assert(maps[i], DeepEquals, expVal)
	}
}

func fmtQuery(aggregate string, aggArgs []interface{}, series, fill string) string {
	args := "value"
	for _, arg := range aggArgs {
		args = fmt.Sprintf("%s, %v", args, arg)
	}

	if fill != "" {
		return fmt.Sprintf("select %s(%s) from %s group by time(60s) fill(%s) where time > 60s and time < 320s", aggregate, args, series, fill)
	}

	return fmt.Sprintf("select %s(%s) from %s group by time(60s) where time > 60s and time < 320s", aggregate, args, series)
}

var emptyAggArgs []interface{}

// tv holds a time / value pair (at this time, the value was)
type tv struct {
	Time  float64
	Value interface{}
}

// count aggregate filling with null
func (self *DataTestSuite) TestCountAggregateFillWithNull(c *C) {
	expVals := []tv{{300000.0, 1.0}, {240000.0, nil}, {180000.0, nil}, {120000.0, 1.0}, {60000.0, 1.0}}
	self.tstAggregateFill(aggTstData, "count", "null", emptyAggArgs, expVals, c)
}

// count aggregate filling with 0
func (self *DataTestSuite) TestCountAggregateFillWith0(c *C) {
	expVals := []tv{{300000.0, 1.0}, {240000.0, 0.0}, {180000.0, 0.0}, {120000.0, 1.0}, {60000.0, 1.0}}
	self.tstAggregateFill(aggTstData, "count", "0", emptyAggArgs, expVals, c)
}

// min aggregate filling with null
func (self *DataTestSuite) TestMinAggregateFillWithNull(c *C) {
	self.tstAggregateFill(aggTstData, "min", "null", emptyAggArgs, aggTstExpect_FillWithNil, c)
}

// min aggregate filling with 0
func (self *DataTestSuite) TestMinAggregateFillWith0(c *C) {
	self.tstAggregateFill(aggTstData, "min", "0", emptyAggArgs, aggTstExpect_FillWith0, c)
}

// max aggregate filling with null
func (self *DataTestSuite) TestMaxAggregateFillWithNull(c *C) {
	self.tstAggregateFill(aggTstData, "max", "null", emptyAggArgs, aggTstExpect_FillWithNil, c)
}

// max aggregate filling with 0
func (self *DataTestSuite) TestMaxAggregateFillWith0(c *C) {
	self.tstAggregateFill(aggTstData, "max", "0", emptyAggArgs, aggTstExpect_FillWith0, c)
}

// mode aggregate filling with null
func (self *DataTestSuite) TestModeAggregateFillWithNull(c *C) {
	self.tstAggregateFill(aggTstData, "mode", "null", emptyAggArgs, aggTstExpect_FillWithNil, c)
}

// mode aggregate filling with 0
func (self *DataTestSuite) TestModeAggregateFillWith0(c *C) {
	self.tstAggregateFill(aggTstData, "mode", "0", emptyAggArgs, aggTstExpect_FillWith0, c)
}

// median aggregate filling with null
func (self *DataTestSuite) TestMedianAggregateFillWithNull(c *C) {
	self.tstAggregateFill(aggTstData, "median", "null", emptyAggArgs, aggTstExpect_FillWithNil, c)
}

// median aggregate filling with 0
func (self *DataTestSuite) TestMedianAggregateFillWith0(c *C) {
	self.tstAggregateFill(aggTstData, "median", "0", emptyAggArgs, aggTstExpect_FillWith0, c)
}

// issue #939 - median panics with null values in column
func (self *DataTestSuite) Test_Issue939_MedianAggregateFillWithNull(c *C) {
	self.tstAggregateFill(aggTstData_Issue939, "median", "null", emptyAggArgs, aggTstExpect_FillWithNil, c)
}

// issue #939 - median panics with null values in column
func (self *DataTestSuite) Test_Issue939_MedianAggregateFillWith0(c *C) {
	self.tstAggregateFill(aggTstData_Issue939, "median", "0", emptyAggArgs, aggTstExpect_FillWith0, c)
}

// distinct aggregate filling with null
func (self *DataTestSuite) TestDistinctAggregateFillWithNull(c *C) {
	self.tstAggregateFill(aggTstData, "distinct", "null", emptyAggArgs, aggTstExpect_FillWithNil, c)
}

// distinct aggregate filling with 0
func (self *DataTestSuite) TestDistinctAggregateFillWith0(c *C) {
	self.tstAggregateFill(aggTstData, "distinct", "0", emptyAggArgs, aggTstExpect_FillWith0, c)
}

// percentile aggregate filling with null
func (self *DataTestSuite) TestPercentileAggregateFillWithNull(c *C) {
	self.tstAggregateFill(aggTstData, "percentile", "null", []interface{}{10}, aggTstExpect_ZerosAndFillWithNil, c)
}

// percentile aggregate filling with 0
func (self *DataTestSuite) TestPercentileAggregateFillWith0(c *C) {
	self.tstAggregateFill(aggTstData, "percentile", "0", []interface{}{10}, aggTstExpect_ZerosAndFillWith0, c)
}

// sum aggregate filling with null
func (self *DataTestSuite) TestSumAggregateFillWithNull(c *C) {
	self.tstAggregateFill(aggTstData, "sum", "null", emptyAggArgs, aggTstExpect_FillWithNil, c)
}

// sum aggregate filling with 0
func (self *DataTestSuite) TestSumAggregateFillWith0(c *C) {
	self.tstAggregateFill(aggTstData, "sum", "0", emptyAggArgs, aggTstExpect_FillWith0, c)
}

// stddev aggregate filling with null
func (self *DataTestSuite) TestStddevAggregateFillWithNull(c *C) {
	self.tstAggregateFill(aggTstData, "stddev", "null", emptyAggArgs, aggTstExpect_ZerosAndFillWithNil, c)
}

// stddev aggregate filling with 0
func (self *DataTestSuite) TestStddevAggregateFillWith0(c *C) {
	self.tstAggregateFill(aggTstData, "stddev", "0", emptyAggArgs, aggTstExpect_ZerosAndFillWith0, c)
}

// first aggregate filling with null
func (self *DataTestSuite) TestFirstAggregateFillWithNull(c *C) {
	self.tstAggregateFill(aggTstData, "first", "null", emptyAggArgs, aggTstExpect_FillWithNil, c)
}

// first aggregate filling with 0
func (self *DataTestSuite) TestFirstAggregateFillWith0(c *C) {
	self.tstAggregateFill(aggTstData, "first", "0", emptyAggArgs, aggTstExpect_FillWith0, c)
}

// last aggregate filling with null
func (self *DataTestSuite) TestLastAggregateFillWithNull(c *C) {
	self.tstAggregateFill(aggTstData, "last", "null", emptyAggArgs, aggTstExpect_FillWithNil, c)
}

// last aggregate filling with 0
func (self *DataTestSuite) TestLastAggregateFillWith0(c *C) {
	self.tstAggregateFill(aggTstData, "last", "0", emptyAggArgs, aggTstExpect_FillWith0, c)
}

// top 1 aggregate filling with null
func (self *DataTestSuite) TestTop1AggregateFillWithNull(c *C) {
	self.tstAggregateFill(aggTstData, "top", "null", []interface{}{1}, aggTstExpect_FillWithNil, c)
}

// top 1 aggregate filling with 0
func (self *DataTestSuite) TestTop1AggregateFillWith0(c *C) {
	self.tstAggregateFill(aggTstData, "top", "0", []interface{}{1}, aggTstExpect_FillWith0, c)
}

// top 10 aggregate filling with null
func (self *DataTestSuite) TestTop10AggregateFillWithNull(c *C) {
	self.tstAggregateFill(aggTstData, "top", "null", []interface{}{10}, aggTstExpect_FillWithNil, c)
}

// top 10 aggregate filling with 0
func (self *DataTestSuite) TestTop10AggregateFillWith0(c *C) {
	self.tstAggregateFill(aggTstData, "top", "0", []interface{}{10}, aggTstExpect_FillWith0, c)
}

// bottom 1 aggregate filling with null
func (self *DataTestSuite) TestBottom1AggregateFillWithNull(c *C) {
	self.tstAggregateFill(aggTstData, "bottom", "null", []interface{}{1}, aggTstExpect_FillWithNil, c)
}

// bottom 1 aggregate filling with 0
func (self *DataTestSuite) TestBottom1AggregateFillWith0(c *C) {
	self.tstAggregateFill(aggTstData, "bottom", "0", []interface{}{1}, aggTstExpect_FillWith0, c)
}

// bottom 10 aggregate filling with null
func (self *DataTestSuite) TestBottom10AggregateFillWithNull(c *C) {
	self.tstAggregateFill(aggTstData, "bottom", "null", []interface{}{10}, aggTstExpect_FillWithNil, c)
}

// bottom 10 aggregate filling with 0
func (self *DataTestSuite) TestBottom10AggregateFillWith0(c *C) {
	self.tstAggregateFill(aggTstData, "bottom", "0", []interface{}{10}, aggTstExpect_FillWith0, c)
}

// derivative aggregate filling with null
func (self *DataTestSuite) TestDerivativeAggregateFillWithNull(c *C) {
	expVals := []tv{{300000.0, 37.0}, {240000.0, nil}, {180000.0, nil}}
	self.tstAggregateFill(aggTstData2, "derivative", "null", emptyAggArgs, expVals, c)
}

// derivative aggregate filling with 0
func (self *DataTestSuite) TestDerivativeAggregateFillWith0(c *C) {
	expVals := []tv{{300000.0, 37.0}, {240000.0, 0.0}, {180000.0, 0.0}}
	self.tstAggregateFill(aggTstData2, "derivative", "0", emptyAggArgs, expVals, c)
}

// difference aggregate filling with null
func (self *DataTestSuite) TestDifferenceAggregateFillWithNull(c *C) {
	expVals := []tv{{300000.0, -370.0}, {240000.0, nil}, {180000.0, nil}, {120000.0, nil}, {60000.0, nil}}
	self.tstAggregateFill(aggTstData2, "difference", "null", emptyAggArgs, expVals, c)
}

// difference aggregate filling with 0
func (self *DataTestSuite) TestDifferenceAggregateFillWith0(c *C) {
	expVals := []tv{{300000.0, -370.0}, {240000.0, 0.0}, {180000.0, 0.0}, {120000.0, 0.0}, {60000.0, 0.0}}
	self.tstAggregateFill(aggTstData2, "difference", "0", emptyAggArgs, expVals, c)
}

// histogram aggregate filling with null
func (self *DataTestSuite) TestHistogramAggregateFillWithNull(c *C) {
	self.client.WriteJsonData(aggTstData2, c, influxdb.Millisecond)
	series := self.client.RunQuery(fmtQuery("histogram", []interface{}{}, "data", "null"), c)
	c.Assert(len(series), Equals, 1)
	maps := ToMap(series[0])
	c.Assert(len(maps), Equals, 6)
	// FIXME: Can't test return values because the order of the returned data is randomized.
	//        Add some asserts here once engine/aggregator_operators.go
	//        func(self *HistogramAggregator) GetValues(...) is modified to sort data.
}

// histogram aggregate filling with 0
func (self *DataTestSuite) TestHistogramAggregateFillWith0(c *C) {
	self.client.WriteJsonData(aggTstData2, c, influxdb.Millisecond)
	series := self.client.RunQuery(fmtQuery("histogram", []interface{}{}, "data", "0"), c)
	c.Assert(len(series), Equals, 1)
	maps := ToMap(series[0])
	c.Assert(len(maps), Equals, 6)
	// FIXME: Can't test return values because the order of the returned data is randomized.
	//        Add some asserts here once engine/aggregator_operators.go
	//        func(self *HistogramAggregator) GetValues(...) is modified to sort data.
}

// Test issue #996: fill() does not fill empty series / timespan
func (self *DataTestSuite) TestIssue996FillEmptyTimespan(c *C) {
	data := `
[
  {
	"name": "data",
    "columns": ["time", "value"],
    "points": [
    [10000, 10.0]
    ]
  }
]`

	expect := []tv{{300000.0, nil}, {240000.0, nil}, {180000.0, nil}, {120000.0, nil}, {60000.0, nil}}
	self.tstAggregateFill(data, "sum", "null", emptyAggArgs, expect, c)
}
