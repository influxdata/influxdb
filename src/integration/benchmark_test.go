package integration

import (
	h "api/http"
	"bytes"
	"checkers"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	. "launchpad.net/gocheck"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

const (
	BATCH_SIZE       = 1
	NUMBER_OF_POINTS = 1000000
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) {
	TestingT(t)
}

var benchmark = flag.Bool("benchmark", false, "Run the benchmarks")
var batchSize = flag.Int("batch-size", BATCH_SIZE, "The batch size per write")

type IntegrationSuite struct {
	server *Server
}

var _ = Suite(&IntegrationSuite{})

type Server struct {
	p *os.Process
}

func (self *Server) WriteData(data interface{}) error {
	bs := []byte{}
	switch x := data.(type) {
	case string:
		bs = []byte(x)
	default:
		var err error
		bs, err = json.Marshal(x)
		if err != nil {
			return err
		}
	}

	resp, err := http.Post("http://localhost:8086/db/db1/series?u=user&p=pass", "application/json", bytes.NewBuffer(bs))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("Status code = %d. Body = %s", resp.StatusCode, string(body))
	}
	return nil
}
func (self *Server) RunQuery(query string) ([]byte, error) {
	encodedQuery := url.QueryEscape(query)
	resp, err := http.Get(fmt.Sprintf("http://localhost:8086/db/db1/series?u=user&p=pass&q=%s", encodedQuery))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Status code = %d. Body = %s", resp.StatusCode, string(body))
	}
	return body, nil
}

func (self *Server) start() error {
	if self.p != nil {
		return fmt.Errorf("Server is already running with pid %d", self.p.Pid)
	}

	fmt.Printf("Starting server")
	dir, err := os.Getwd()
	if err != nil {
		return err
	}

	root := filepath.Join(dir, "..", "..")
	filename := filepath.Join(root, "server")
	p, err := os.StartProcess(filename, []string{filename, "-cpuprofile", "/tmp/cpuprofile"}, &os.ProcAttr{
		Dir:   root,
		Env:   os.Environ(),
		Files: []*os.File{os.Stdin, os.Stdout, os.Stderr},
	})
	if err != nil {
		return err
	}
	self.p = p
	time.Sleep(2 * time.Second)
	return nil
}

func (self *Server) stop() {
	if self.p == nil {
		return
	}

	self.p.Signal(syscall.SIGTERM)
	self.p.Wait()
}

func (self *IntegrationSuite) createUser() error {
	resp, err := http.Post("http://localhost:8086/db/db1/users?u=root&p=root", "application/json",
		bytes.NewBufferString(`{"username": "user", "password": "pass"}`))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Statuscode is %d", resp.StatusCode)
	}
	return nil
}

var cleanData = flag.Bool("clean-data", false, "Clean data before running the benchmark tests")
var wroteData = true

func (self *IntegrationSuite) SetUpSuite(c *C) {

	if *cleanData {
		wroteData = false
		err := os.RemoveAll("/tmp/influxdb")
		c.Assert(err, IsNil)
	}

	self.server = &Server{}
	err := self.server.start()
	c.Assert(err, IsNil)

	if *cleanData {
		err = self.createUser()
		c.Assert(err, IsNil)
	}
}

func (self *IntegrationSuite) TearDownSuite(c *C) {
	self.server.stop()
}

func (self *IntegrationSuite) createPoints(name string, numOfColumns, numOfPoints int) interface{} {
	series := &h.SerializedSeries{}

	series.Name = name
	for i := 0; i < numOfColumns; i++ {
		series.Columns = append(series.Columns, fmt.Sprintf("column%d", i))
	}

	for i := 0; i < numOfPoints; i++ {
		point := []interface{}{}
		for j := 0; j < numOfColumns; j++ {
			point = append(point, rand.Float64())
		}
		series.Points = append(series.Points, point)
	}

	return []*h.SerializedSeries{series}
}

func (self *IntegrationSuite) writeData(c *C) {
	if wroteData {
		return
	}

	for _, numberOfColumns := range []int{1, 5, 10} {
		startTime := time.Now()
		seriesName := fmt.Sprintf("foo%d", numberOfColumns)

		bSize := *batchSize
		for batch := 0; batch < NUMBER_OF_POINTS/bSize; batch++ {
			err := self.server.WriteData(self.createPoints(seriesName, numberOfColumns, bSize))
			c.Assert(err, IsNil)
			if (batch*bSize+bSize)%1000 == 0 {
				fmt.Print(".")
			}
		}
		fmt.Println()
		fmt.Printf("Writing %d points (containing %d columns) in %d batches took %s\n", NUMBER_OF_POINTS, numberOfColumns, BATCH_SIZE,
			time.Now().Sub(startTime))
	}
	wroteData = true
}

func (self *IntegrationSuite) TestWriting(c *C) {
	if !*benchmark {
		c.Skip("Benchmarks are disabled")
	}

	self.writeData(c)
}

func (self *IntegrationSuite) TestReading(c *C) {
	if !*benchmark {
		c.Skip("Benchmarking is disabled")
	}

	queries := map[string][]int{
		"select column0 from foo1 where column0 > 0.5 and column0 < 0.6;":                                     []int{1, 80000, 120000},
		"select column0 from foo1;":                                                                           []int{1, 1000000 - 1, 1000000 + 1},
		"select column0 from foo5 where column0 > 0.5 and column0 < 0.6 and column1 > 0.5 and column1 < 0.6;": []int{1, 8000, 12000},
		"select column0, column1, column2, column3, column4 from foo5;":                                       []int{5, 1000000 - 1, 1000000 + 1},
	}

	for q, r := range queries {
		self.writeData(c)

		startTime := time.Now()
		bs, err := self.server.RunQuery(q)
		c.Assert(err, IsNil)
		elapsedTime := time.Now().Sub(startTime)

		data := []*h.SerializedSeries{}
		err = json.Unmarshal(bs, &data)
		c.Assert(err, IsNil)

		c.Assert(data, HasLen, 1)
		c.Assert(data[0].Columns, HasLen, r[0]+2)                   // time, sequence nuber and the requested columns
		c.Assert(len(data[0].Points), checkers.InRange, r[1], r[2]) // values between 0.5 and 0.65 should be about 100,000

		fmt.Printf("Took %s to execute %s\n", elapsedTime, q)
	}
}
