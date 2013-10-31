package integration

import (
	h "api/http"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	. "launchpad.net/gocheck"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const (
	BATCH_SIZE       = 1000
	NUMBER_OF_POINTS = BATCH_SIZE * 1000
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) {
	TestingT(t)
}

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
func (self *Server) RunQuery(query string) (string, error) {
	encodedQuery := url.QueryEscape(query)
	resp, err := http.Get(fmt.Sprintf("http://localhost:8086/db/db1/series?u=user&p=pass&q=%s", encodedQuery))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("Status code = %d. Body = %s", resp.StatusCode, string(body))
	}
	return string(body), nil
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

	self.p.Kill()
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

func (self *IntegrationSuite) SetUpSuite(c *C) {
	err := os.RemoveAll("/tmp/influxdb")
	c.Assert(err, IsNil)

	self.server = &Server{}
	err = self.server.start()
	c.Assert(err, IsNil)
	err = self.createUser()
	c.Assert(err, IsNil)
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

func (self *IntegrationSuite) TestWriting(c *C) {
	// for _, numberOfColumns := range []int{1, 5, 10, 20} {
	for _, numberOfColumns := range []int{1} {
		startTime := time.Now()
		seriesName := fmt.Sprintf("foo%d", numberOfColumns)

		for batch := 0; batch < NUMBER_OF_POINTS/BATCH_SIZE; batch++ {
			err := self.server.WriteData(self.createPoints(seriesName, numberOfColumns, BATCH_SIZE))
			c.Assert(err, IsNil)
			fmt.Printf("Finished batch %d\n", batch)
		}

		fmt.Printf("Writing %d points (containgin %d columns) in %d batches took %s\n", NUMBER_OF_POINTS, numberOfColumns, BATCH_SIZE,
			time.Now().Sub(startTime))
	}
}
