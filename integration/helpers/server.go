package helpers

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"syscall"
	"time"

	influxdb "github.com/influxdb/influxdb/client"
	"github.com/influxdb/influxdb/common"
	"github.com/influxdb/influxdb/configuration"
	. "launchpad.net/gocheck"
)

type Server struct {
	p          *os.Process
	configFile string
	sslOnly    bool
	apiPort    int
	sslApiPort int
	args       []string
}

func NewSslServer(configFile string, c *C) *Server {
	return newServerCommon(configFile, true, true, c)
}

func NewServerWithArgs(configFile string, c *C, args ...string) *Server {
	return newServerCommon(configFile, false, false, c, args...)
}

func NewServer(configFile string, c *C) *Server {
	return newServerCommon(configFile, true, false, c)
}

func newServerCommon(configFile string, deleteData, ssl bool, c *C, args ...string) *Server {
	config := configuration.LoadConfiguration("../" + configFile)
	s := &Server{configFile: configFile, apiPort: config.ApiHttpPort, sslApiPort: config.ApiHttpSslPort, sslOnly: ssl, args: args}
	if deleteData {
		c.Assert(os.RemoveAll(config.DataDir), IsNil)
		c.Assert(os.RemoveAll(config.WalDir), IsNil)
		c.Assert(os.RemoveAll(config.RaftDir), IsNil)
	}
	err := s.Start()
	c.Assert(err, IsNil)
	s.WaitForServerToStart()
	return s
}

func (self *Server) WaitForServerToStart() {
	url := fmt.Sprintf("http://localhost:%d/ping", self.apiPort)
	if self.sslOnly {
		url = fmt.Sprintf("https://localhost:%d/ping", self.sslApiPort)
	}

	client := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	for i := 0; i < 600; i++ {
		resp, err := client.Get(url)
		if err != nil {
			// wait 100 milliseconds before retrying to give the server a
			// chance to startup
			time.Sleep(100 * time.Millisecond)
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			panic(resp)
		}
		// wait for the server to be marked up by other servers
		time.Sleep(500 * time.Millisecond)
		return
	}
	// wait for other servers to send heartbeat and detect that the
	// server is up
	panic("server didn't start")
}

func (self *Server) WaitForServerToSync() {
	for i := 0; i < 600; i++ {
		resp, err := http.Get(fmt.Sprintf("http://localhost:%d/sync?u=root&p=root", self.apiPort))
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return
		}

		if resp.StatusCode != http.StatusInternalServerError {
			panic(resp)
		}
		// wait 100 millisecond to give the servers chance to catch up
		time.Sleep(100 * time.Millisecond)
	}
	panic("servers didn't sync")
}

// optional db
func (self *Server) GetClient(db string, c *C) *influxdb.Client {
	return self.GetClientWithUser(db, "", "", c)
}

func (self *Server) GetClientWithUser(db, username, password string, c *C) *influxdb.Client {
	client, err := influxdb.NewClient(&influxdb.ClientConfig{
		Host:     fmt.Sprintf("localhost:%d", self.apiPort),
		Username: username,
		Password: password,
		Database: db,
	})
	c.Assert(err, IsNil)
	return client
}

func (self *Server) WriteData(data interface{}, c *C, precision ...influxdb.TimePrecision) {
	client := self.GetClient("db1", c)
	var series []*influxdb.Series
	switch x := data.(type) {
	case string:
		c.Assert(json.Unmarshal([]byte(x), &series), IsNil)
	case []*influxdb.Series:
		series = x
	default:
		c.Fatalf("Unknown type: %T", x)
	}

	var err error
	if len(precision) == 0 {
		err = client.WriteSeries(series)
	} else {
		err = client.WriteSeriesWithTimePrecision(series, precision[0])
	}
	c.Assert(err, IsNil)
	self.WaitForServerToSync()
}

func (self *Server) RunQuery(query string, precision influxdb.TimePrecision, c *C) []*influxdb.Series {
	return self.RunQueryAsUser(query, precision, "user", "pass", true, c)
}

func (self *Server) RunQueryAsRoot(query string, precision influxdb.TimePrecision, c *C) []*influxdb.Series {
	return self.RunQueryAsUser(query, precision, "root", "root", true, c)
}

func (self *Server) RunQueryAsUser(query string, precision influxdb.TimePrecision, username, password string, isValid bool, c *C) []*influxdb.Series {
	client := self.GetClientWithUser("db1", username, password, c)
	series, err := client.Query(query, precision)
	if isValid {
		c.Assert(err, IsNil)
	} else {
		c.Assert(err, NotNil)
	}
	return series
}

func (self *Server) Start() error {
	if self.p != nil {
		return fmt.Errorf("Server is already running with pid %d", self.p.Pid)
	}

	fmt.Printf("Starting server")
	dir, err := os.Getwd()
	if err != nil {
		return err
	}

	root := filepath.Join(dir, "..")
	filename := filepath.Join(root, "influxdb")
	if self.configFile == "" {
		self.configFile = "integration/test_config_single.toml"
	}
	args := []string{filename, "-config", self.configFile}
	args = append(args, self.args...)
	p, err := os.StartProcess(filename, args, &os.ProcAttr{
		Dir:   root,
		Env:   os.Environ(),
		Files: []*os.File{os.Stdin, os.Stdout, os.Stderr},
	})
	if err != nil {
		return err
	}
	self.p = p
	return nil
}

func (self *Server) ApiPort() int {
	return self.apiPort
}

func (self *Server) Stop() {
	if self.p == nil {
		return
	}

	self.p.Signal(syscall.SIGTERM)
	self.p.Wait()
	self.p = nil
}

func (self *Server) SetSslOnly(sslOnly bool) {
	self.sslOnly = sslOnly
}

func (self *Server) DoesWalExist() error {
	config := configuration.LoadConfiguration("../" + self.configFile)
	_, err := os.Stat(filepath.Join(config.WalDir, "log.1"))
	return err
}

func ResultsToSeriesCollection(results []*common.SerializedSeries) *SeriesCollection {
	return &SeriesCollection{Members: results}
}

type SeriesCollection struct {
	Members []*common.SerializedSeries
}

func (self *SeriesCollection) GetSeries(name string, c *C) *Series {
	for _, s := range self.Members {
		if s.Name == name {
			return &Series{s}
		}
	}
	c.Fatalf("Couldn't find series '%s' in: %v\n", name, self)
	return nil
}

type Series struct {
	*common.SerializedSeries
}

func (self *Series) GetValueForPointAndColumn(pointIndex int, columnName string, c *C) interface{} {
	columnIndex := -1
	for index, name := range self.Columns {
		if name == columnName {
			columnIndex = index
		}
	}
	if columnIndex == -1 {
		c.Errorf("Couldn't find column '%s' in series: %v\n", columnName, self)
		return nil
	}
	if pointIndex > len(self.Points)-1 {
		c.Errorf("Fewer than %d points in series '%s': %v\n", pointIndex+1, self.Name, self)
	}
	return self.Points[pointIndex][columnIndex]
}

type Point struct {
	Values []interface{}
}

func (self *Server) Query(database, query string, onlyLocal bool, c *C) *SeriesCollection {
	return self.QueryWithUsername(database, query, onlyLocal, c, "paul", "pass")
}

func (self *Server) QueryAsRoot(database, query string, onlyLocal bool, c *C) *SeriesCollection {
	return self.QueryWithUsername(database, query, onlyLocal, c, "root", "root")
}

func (self *Server) GetResponse(database, query, username, password string, onlyLocal bool, c *C) *http.Response {
	encodedQuery := url.QueryEscape(query)
	fullUrl := fmt.Sprintf("http://localhost:%d/db/%s/series?u=%s&p=%s&q=%s", self.apiPort, database, username, password, encodedQuery)
	if onlyLocal {
		fullUrl = fullUrl + "&force_local=true"
	}
	resp, err := http.Get(fullUrl)
	c.Assert(err, IsNil)
	return resp
}

func (self *Server) GetErrorBody(database, query, username, password string, onlyLocal bool, c *C) (string, int) {
	resp := self.GetResponse(database, query, username, password, onlyLocal, c)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Not(Equals), http.StatusOK)
	bytes, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	return string(bytes), resp.StatusCode
}

func (self *Server) QueryWithUsername(database, query string, onlyLocal bool, c *C, username, password string) *SeriesCollection {
	resp := self.GetResponse(database, query, username, password, onlyLocal, c)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	body, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	var js []*common.SerializedSeries
	err = json.Unmarshal(body, &js)
	if err != nil {
		fmt.Println("NOT JSON: ", string(body))
	}
	c.Assert(err, IsNil)
	return ResultsToSeriesCollection(js)
}

func (self *Server) VerifyForbiddenQuery(database, query string, onlyLocal bool, c *C, username, password string) string {
	encodedQuery := url.QueryEscape(query)
	fullUrl := fmt.Sprintf("http://localhost:%d/db/%s/series?u=%s&p=%s&q=%s", self.apiPort, database, username, password, encodedQuery)
	if onlyLocal {
		fullUrl = fullUrl + "&force_local=true"
	}
	resp, err := http.Get(fullUrl)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusForbidden)
	body, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)

	return string(body)
}

func (self *Server) Post(url, data string, c *C) *http.Response {
	err := self.Request("POST", url, data, c)
	return err
}

func (self *Server) Delete(url, body string, c *C) *http.Response {
	err := self.Request("DELETE", url, body, c)
	return err
}

func (self *Server) PostGetBody(url, data string, c *C) []byte {
	resp := self.Request("POST", url, data, c)
	body, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	return body
}

func (self *Server) Get(url string, c *C) []byte {
	resp := self.Request("GET", url, "", c)
	body, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	return body
}

func (self *Server) Request(method, url, data string, c *C) *http.Response {
	fullUrl := fmt.Sprintf("http://localhost:%d%s", self.apiPort, url)
	req, err := http.NewRequest(method, fullUrl, bytes.NewBufferString(data))
	c.Assert(err, IsNil)
	resp, err := http.DefaultClient.Do(req)
	c.Assert(err, IsNil)
	return resp
}

func (self *Server) RemoveAllContinuousQueries(db string, c *C) {
	client := self.GetClient(db, c)
	queries, err := client.GetContinuousQueries()
	c.Assert(err, IsNil)
	for _, q := range queries {
		c.Assert(client.DeleteContinuousQueries(int(q["id"].(float64))), IsNil)
	}
}

func (self *Server) AssertContinuousQueryCount(db string, count int, c *C) {
	client := self.GetClient(db, c)
	queries, err := client.GetContinuousQueries()
	c.Assert(err, IsNil)
	c.Assert(queries, HasLen, count)
}
