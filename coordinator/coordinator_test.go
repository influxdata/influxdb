package coordinator

import (
	"fmt"
	"time"

	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/configuration"
	"github.com/influxdb/influxdb/parser"
	. "launchpad.net/gocheck"
)

type CoordinatorSuite struct{}

var _ = Suite(&CoordinatorSuite{})

func (self *CoordinatorSuite) TestShouldQuerySequentially(c *C) {
	end := time.Now().Truncate(24 * time.Hour)
	start := end.Add(-7 * 24 * time.Hour)
	shard := cluster.NewShard(1, start, end, "", "", nil)
	shards := []*cluster.ShardData{shard}
	coordinator := NewCoordinatorImpl(&configuration.Configuration{
		ClusterMaxResponseBufferSize: 1000,
	}, nil, nil, nil)
	queries := map[string]bool{
		"list series": false,
		"select count(foo) from /.*bar.*/ group by time(1d)": true,
		"select count(foo) from bar":                         true,
		"select foo from bar":                                true,
		"select count(foo) from bar group by baz":            true,
		"select count(foo) from bar group by time(1d)":       false,
		"select count(foo) from bar group by time(3d)":       true,
	}

	for query, result := range queries {
		fmt.Printf("Testing %s\n", query)
		parsedQuery, err := parser.ParseQuery(query)
		c.Assert(err, IsNil)
		c.Assert(parsedQuery, HasLen, 1)
		querySpec := parser.NewQuerySpec(nil, "", parsedQuery[0])
		c.Assert(coordinator.shouldQuerySequentially(shards, querySpec), Equals, result)
	}
}
