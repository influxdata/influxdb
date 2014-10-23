package integration

import (
	"fmt"
	"net/http"
	"os"
	"time"

	. "github.com/influxdb/influxdb/integration/helpers"
	. "launchpad.net/gocheck"
)

type ContinuousQueriesSuite struct {
	serverProcesses []*Server
}

var _ = Suite(&ContinuousQueriesSuite{})

func (self *ContinuousQueriesSuite) SetUpSuite(c *C) {
	err := os.RemoveAll("/tmp/influxdb/test")
	c.Assert(err, IsNil)
	self.serverProcesses = []*Server{
		NewServer("integration/test_rf_1.toml", c),
		NewServer("integration/test_rf_2.toml", c),
	}
	client := self.serverProcesses[0].GetClient("", c)
	c.Assert(client.CreateDatabase("test"), IsNil)
	for _, s := range self.serverProcesses {
		s.WaitForServerToSync()
	}
}

func (self *ContinuousQueriesSuite) TearDownSuite(c *C) {
	for _, s := range self.serverProcesses {
		s.Stop()
	}
}

func (self *ContinuousQueriesSuite) TestFirstBackfill(c *C) {
	defer self.serverProcesses[0].RemoveAllContinuousQueries("test_cq", c)
	client := self.serverProcesses[0].GetClient("", c)
	c.Assert(client.CreateDatabase("test_no_backfill"), IsNil)

	data := fmt.Sprintf(`[
  {
    "name": "cqbackfilltest",
    "columns": ["time", "reqtime", "url"],
    "points": [
        [0, 8.0, "/login"],
        [0, 3.0, "/list"],
        [0, 4.0, "/register"],
        [5, 9.0, "/login"],
        [5, 4.0, "/list"],
        [5, 5.0, "/register"]
    ]
  }
  ]`)

	resp := self.serverProcesses[0].Post("/db/test_no_backfill/series?u=root&p=root&time_precision=s", data, c)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	// wait for the data to get written

	for _, s := range self.serverProcesses {
		s.WaitForServerToSync()
	}

	cq := "select count(reqtime), url from cqbackfilltest group by time(10s), url into cqbackfill_off.10s backfill(false)"
	self.serverProcesses[0].QueryAsRoot("test_no_backfill", cq, false, c)

	// wait for the continuous query to run
	time.Sleep(5 * time.Second)

	self.serverProcesses[0].RemoveAllContinuousQueries("test_no_backfill", c)

	for _, s := range self.serverProcesses {
		s.WaitForServerToSync()
	}

	// check backfill_off query results
	body, _ := self.serverProcesses[0].GetErrorBody("test_no_backfill", "select * from cqbackfill_off.10s", "root", "root", false, c)
	c.Assert(body, Matches, "Couldn't find series.*")
}
