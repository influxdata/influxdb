// This package is a set of convenience helpers and structs to make integration testing easier
package run_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/influxdb/influxdb/cmd/influxd/run"
	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/services/httpd"
	"github.com/influxdb/influxdb/toml"
)

// Server represents a test wrapper for run.Server.
type Server struct {
	*run.Server
	Config *run.Config
}

// NewServer returns a new instance of Server.
func NewServer(c *run.Config) *Server {
	buildInfo := &run.BuildInfo{
		Version: "testServer",
		Commit:  "testCommit",
		Branch:  "testBranch",
	}
	srv, _ := run.NewServer(c, buildInfo)
	s := Server{
		Server: srv,
		Config: c,
	}
	s.TSDBStore.EngineOptions.Config = c.Data
	configureLogging(&s)
	return &s
}

// OpenServer opens a test server.
func OpenServer(c *run.Config, joinURLs string) *Server {
	s := NewServer(c)
	configureLogging(s)
	if err := s.Open(); err != nil {
		panic(err.Error())
	}
	return s
}

// OpenServerWithVersion opens a test server with a specific version.
func OpenServerWithVersion(c *run.Config, version string) *Server {
	buildInfo := &run.BuildInfo{
		Version: version,
		Commit:  "",
		Branch:  "",
	}
	srv, _ := run.NewServer(c, buildInfo)
	s := Server{
		Server: srv,
		Config: c,
	}
	configureLogging(&s)
	if err := s.Open(); err != nil {
		panic(err.Error())
	}

	return &s
}

// OpenDefaultServer opens a test server with a default database & retention policy.
func OpenDefaultServer(c *run.Config, joinURLs string) *Server {
	s := OpenServer(c, joinURLs)
	if err := s.CreateDatabaseAndRetentionPolicy("db0", newRetentionPolicyInfo("rp0", 1, 0)); err != nil {
		panic(err)
	}
	if err := s.MetaStore.SetDefaultRetentionPolicy("db0", "rp0"); err != nil {
		panic(err)
	}
	return s
}

// Close shuts down the server and removes all temporary paths.
func (s *Server) Close() {
	s.Server.Close()
	os.RemoveAll(s.Config.Meta.Dir)
	os.RemoveAll(s.Config.Data.Dir)
	os.RemoveAll(s.Config.HintedHandoff.Dir)
}

// URL returns the base URL for the httpd endpoint.
func (s *Server) URL() string {
	for _, service := range s.Services {
		if service, ok := service.(*httpd.Service); ok {
			return "http://" + service.Addr().String()
		}
	}
	panic("httpd server not found in services")
}

// CreateDatabaseAndRetentionPolicy will create the database and retention policy.
func (s *Server) CreateDatabaseAndRetentionPolicy(db string, rp *meta.RetentionPolicyInfo) error {
	if _, err := s.MetaStore.CreateDatabase(db); err != nil {
		return err
	} else if _, err := s.MetaStore.CreateRetentionPolicy(db, rp); err != nil {
		return err
	}
	return nil
}

// Query executes a query against the server and returns the results.
func (s *Server) Query(query string) (results string, err error) {
	return s.QueryWithParams(query, nil)
}

// Query executes a query against the server and returns the results.
func (s *Server) QueryWithParams(query string, values url.Values) (results string, err error) {
	if values == nil {
		values = url.Values{}
	}
	values.Set("q", query)
	return s.HTTPGet(s.URL() + "/query?" + values.Encode())
}

// HTTPGet makes an HTTP GET request to the server and returns the response.
func (s *Server) HTTPGet(url string) (results string, err error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	body := string(MustReadAll(resp.Body))
	switch resp.StatusCode {
	case http.StatusBadRequest:
		if !expectPattern(".*error parsing query*.", body) {
			return "", fmt.Errorf("unexpected status code: code=%d, body=%s", resp.StatusCode, body)
		}
		return body, nil
	case http.StatusOK:
		return body, nil
	default:
		return "", fmt.Errorf("unexpected status code: code=%d, body=%s", resp.StatusCode, body)
	}
}

// HTTPPost makes an HTTP POST request to the server and returns the response.
func (s *Server) HTTPPost(url string, content []byte) (results string, err error) {
	buf := bytes.NewBuffer(content)
	resp, err := http.Post(url, "application/json", buf)
	if err != nil {
		return "", err
	}
	body := string(MustReadAll(resp.Body))
	switch resp.StatusCode {
	case http.StatusBadRequest:
		if !expectPattern(".*error parsing query*.", body) {
			return "", fmt.Errorf("unexpected status code: code=%d, body=%s", resp.StatusCode, body)
		}
		return body, nil
	case http.StatusOK, http.StatusNoContent:
		return body, nil
	default:
		return "", fmt.Errorf("unexpected status code: code=%d, body=%s", resp.StatusCode, body)
	}
}

// Write executes a write against the server and returns the results.
func (s *Server) Write(db, rp, body string, params url.Values) (results string, err error) {
	if params == nil {
		params = url.Values{}
	}
	if params.Get("db") == "" {
		params.Set("db", db)
	}
	if params.Get("rp") == "" {
		params.Set("rp", rp)
	}
	resp, err := http.Post(s.URL()+"/write?"+params.Encode(), "", strings.NewReader(body))
	if err != nil {
		return "", err
	} else if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return "", fmt.Errorf("invalid status code: code=%d, body=%s", resp.StatusCode, MustReadAll(resp.Body))
	}
	return string(MustReadAll(resp.Body)), nil
}

// MustWrite executes a write to the server. Panic on error.
func (s *Server) MustWrite(db, rp, body string, params url.Values) string {
	results, err := s.Write(db, rp, body, params)
	if err != nil {
		panic(err)
	}
	return results
}

// NewConfig returns the default config with temporary paths.
func NewConfig() *run.Config {
	c := run.NewConfig()
	c.ReportingDisabled = true
	c.Cluster.ShardWriterTimeout = toml.Duration(30 * time.Second)
	c.Cluster.WriteTimeout = toml.Duration(30 * time.Second)
	c.Meta.Dir = MustTempFile()
	c.Meta.BindAddress = "127.0.0.1:0"
	c.Meta.HeartbeatTimeout = toml.Duration(50 * time.Millisecond)
	c.Meta.ElectionTimeout = toml.Duration(50 * time.Millisecond)
	c.Meta.LeaderLeaseTimeout = toml.Duration(50 * time.Millisecond)
	c.Meta.CommitTimeout = toml.Duration(5 * time.Millisecond)

	c.Data.Dir = MustTempFile()
	c.Data.WALDir = MustTempFile()
	c.Data.WALLoggingEnabled = false

	c.HintedHandoff.Dir = MustTempFile()

	c.HTTPD.Enabled = true
	c.HTTPD.BindAddress = "127.0.0.1:0"
	c.HTTPD.LogEnabled = testing.Verbose()

	c.Monitor.StoreEnabled = false

	return c
}

func newRetentionPolicyInfo(name string, rf int, duration time.Duration) *meta.RetentionPolicyInfo {
	return &meta.RetentionPolicyInfo{Name: name, ReplicaN: rf, Duration: duration}
}

func maxFloat64() string {
	maxFloat64, _ := json.Marshal(math.MaxFloat64)
	return string(maxFloat64)
}

func maxInt64() string {
	maxInt64, _ := json.Marshal(^int64(0))
	return string(maxInt64)
}

func now() time.Time {
	return time.Now().UTC()
}

func yesterday() time.Time {
	return now().Add(-1 * time.Hour * 24)
}

func mustParseTime(layout, value string) time.Time {
	tm, err := time.Parse(layout, value)
	if err != nil {
		panic(err)
	}
	return tm
}

// MustReadAll reads r. Panic on error.
func MustReadAll(r io.Reader) []byte {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		panic(err)
	}
	return b
}

// MustTempFile returns a path to a temporary file.
func MustTempFile() string {
	f, err := ioutil.TempFile("", "influxd-")
	if err != nil {
		panic(err)
	}
	f.Close()
	os.Remove(f.Name())
	return f.Name()
}

func expectPattern(exp, act string) bool {
	re := regexp.MustCompile(exp)
	if !re.MatchString(act) {
		return false
	}
	return true
}

type Query struct {
	name     string
	command  string
	params   url.Values
	exp, act string
	pattern  bool
	skip     bool
	repeat   int
}

// Execute runs the command and returns an err if it fails
func (q *Query) Execute(s *Server) (err error) {
	if q.params == nil {
		q.act, err = s.Query(q.command)
		return
	}
	q.act, err = s.QueryWithParams(q.command, q.params)
	return
}

func (q *Query) success() bool {
	if q.pattern {
		return expectPattern(q.exp, q.act)
	}
	return q.exp == q.act
}

func (q *Query) Error(err error) string {
	return fmt.Sprintf("%s: %v", q.name, err)
}

func (q *Query) failureMessage() string {
	return fmt.Sprintf("%s: unexpected results\nquery:  %s\nexp:    %s\nactual: %s\n", q.name, q.command, q.exp, q.act)
}

type Test struct {
	initialized bool
	write       string
	params      url.Values
	db          string
	rp          string
	exp         string
	queries     []*Query
}

func NewTest(db, rp string) Test {
	return Test{
		db: db,
		rp: rp,
	}
}

func (t *Test) addQueries(q ...*Query) {
	t.queries = append(t.queries, q...)
}

func (t *Test) init(s *Server) error {
	if t.write == "" || t.initialized {
		return nil
	}
	t.initialized = true
	if res, err := s.Write(t.db, t.rp, t.write, t.params); err != nil {
		return err
	} else if t.exp != res {
		return fmt.Errorf("unexpected results\nexp: %s\ngot: %s\n", t.exp, res)
	}
	return nil
}

func configureLogging(s *Server) {
	// Set the logger to discard unless verbose is on
	if !testing.Verbose() {
		type logSetter interface {
			SetLogger(*log.Logger)
		}
		nullLogger := log.New(ioutil.Discard, "", 0)
		s.MetaStore.Logger = nullLogger
		s.TSDBStore.Logger = nullLogger
		s.HintedHandoff.SetLogger(nullLogger)
		s.Monitor.SetLogger(nullLogger)
		s.QueryExecutor.SetLogger(nullLogger)
		s.Subscriber.SetLogger(nullLogger)
		for _, service := range s.Services {
			if service, ok := service.(logSetter); ok {
				service.SetLogger(nullLogger)
			}
		}
	}
}
