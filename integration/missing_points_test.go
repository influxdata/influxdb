package integration

import (
	"fmt"
	"os"
	"sync"
	"time"

	influxdb "github.com/influxdb/influxdb/client"
	. "github.com/influxdb/influxdb/integration/helpers"
	. "launchpad.net/gocheck"
)

type MissingPointsSuite struct {
	serverProcesses []*Server
}

var _ = Suite(&MissingPointsSuite{})

func (self *MissingPointsSuite) SetUpSuite(c *C) {
	err := os.RemoveAll("/tmp/influxdb/test")
	c.Assert(err, IsNil)
	self.serverProcesses = []*Server{
		NewServer("integration/test_missing_points1.toml", c),
		NewServer("integration/test_missing_points2.toml", c),
		NewServer("integration/test_missing_points3.toml", c),
	}
	self.serverProcesses[0].SetSslOnly(true)
	client := self.serverProcesses[0].GetClient("", c)
	dbs := []string{"full_rep", "test_rep", "single_rep", "test_cq", "test_cq_null", "drop_db"}
	for _, db := range dbs {
		c.Assert(client.CreateDatabase(db), IsNil)
	}
	for _, db := range dbs {
		c.Assert(client.CreateDatabaseUser(db, "paul", "pass"), IsNil)
		c.Assert(client.AlterDatabasePrivilege(db, "paul", true), IsNil)
		c.Assert(client.CreateDatabaseUser(db, "weakpaul", "pass"), IsNil)
	}
	for _, s := range self.serverProcesses {
		s.WaitForServerToSync()
	}
}

func (self *MissingPointsSuite) TearDownSuite(c *C) {
	for _, s := range self.serverProcesses {
		s.Stop()
	}
}

// test missing points when we have a split
func (self *MissingPointsSuite) TestMissingPoints(c *C) {
	numberOfSeries := 25000
	batchSize := 100
	numberOfPoints := 5
	parallelism := 10
	timeBase := 1399035078

	client := self.serverProcesses[0].GetClient("", c)
	client.CreateDatabase("test_missing_points")
	for _, s := range self.serverProcesses {
		s.WaitForServerToSync()
	}

	batches := make(chan []*influxdb.Series)

	// write points
	wg := sync.WaitGroup{}
	wg.Add(parallelism)
	for i := 0; i < parallelism; i++ {
		server := self.serverProcesses[i%len(self.serverProcesses)]
		client := server.GetClient("test_missing_points", c)
		go func() {
			defer wg.Done()
			for batch := range batches {
				c.Assert(client.WriteSeries(batch), IsNil)
			}
		}()
	}

	fmt.Printf("Generating data\n")
	// generate points
	batch := make([]*influxdb.Series, 0, batchSize)
	columns := []string{"value", "time"}
	for p := 0; p < numberOfPoints; p++ {
		for s := 0; s < numberOfSeries; s++ {
			name := fmt.Sprintf("series.%d", s)
			i := (p*numberOfSeries + s)
			t := (timeBase + i) * 1000
			batch = append(batch, &influxdb.Series{name, columns, [][]interface{}{{p, t}}})
			if len(batch) >= batchSize {
				batches <- batch
				batch = make([]*influxdb.Series, 0, batchSize)
			}
		}
	}
	close(batches)

	fmt.Printf("Waiting for data to be written\n")
	wg.Wait()

	fmt.Printf("Waiting for servers to sync\n")
	for _, s := range self.serverProcesses {
		s.WaitForServerToSync()
	}

	client = self.serverProcesses[0].GetClient("test_missing_points", c)
	fmt.Printf("Running query\n")
	ss, err := client.Query(`select * from /^series\.[0-9]+$/`, "m")
	c.Assert(err, IsNil)
	c.Assert(len(ss), Equals, numberOfSeries)
	names := map[string]struct{}{}
	for _, s := range ss {
		names[s.Name] = struct{}{}
		if len(s.Points) != numberOfPoints {
			fmt.Printf("series %s is missing some points ", s.Name)
			for _, v := range s.Points {
				fmt.Printf(" t: %v", time.Unix(int64(v[0].(float64)/1000.0), 0))
			}
			fmt.Println()
		}
		c.Assert(s.Points, HasLen, numberOfPoints)
	}
	c.Assert(len(names), Equals, numberOfSeries)
}
