// This package is a set of convenience helpers and structs to make integration testing easier
package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/cmd/influxd/run"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/httpd"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/toml"
)

// Server represents a test wrapper for run.Server.
type Server interface {
	URL() string
	Open() error
	SetLogOutput(w io.Writer)
	Close()
	Closed() bool

	CreateDatabase(db string) (*meta.DatabaseInfo, error)
	CreateDatabaseAndRetentionPolicy(db string, rp *meta.RetentionPolicySpec, makeDefault bool) error
	CreateSubscription(database, rp, name, mode string, destinations []string) error
	Reset() error

	Query(query string) (results string, err error)
	QueryWithParams(query string, values url.Values) (results string, err error)

	Write(db, rp, body string, params url.Values) (results string, err error)
	MustWrite(db, rp, body string, params url.Values) string
	WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error
}

// LocalServer is a Server that is running in-process and can be accessed directly
type LocalServer struct {
	*client
	*run.Server
	Config *run.Config
}

// RemoteServer is a Server that is accessed remotely via the HTTP API
type RemoteServer struct {
	*client
	url string
}

func (s *RemoteServer) URL() string {
	return s.url
}

func (s *RemoteServer) Open() error {
	resp, err := http.Get(s.URL() + "/ping")
	if err != nil {
		return err
	}
	body := strings.TrimSpace(string(MustReadAll(resp.Body)))
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("unexpected status code: code=%d, body=%s", resp.StatusCode, body)
	}
	return nil
}

func (s *RemoteServer) Close() {
	// ignore, we can't shutdown a remote server
}

func (s *RemoteServer) SetLogOutput(w io.Writer) {
	// ignore, we can't change the logging of a remote server
}

func (s *RemoteServer) Closed() bool {
	return true
}

func (s *RemoteServer) CreateDatabase(db string) (*meta.DatabaseInfo, error) {
	stmt := fmt.Sprintf("CREATE+DATABASE+%s", db)

	_, err := s.HTTPPost(s.URL()+"/query?q="+stmt, nil)
	if err != nil {
		return nil, err
	}
	return &meta.DatabaseInfo{}, nil
}

func (s *RemoteServer) CreateDatabaseAndRetentionPolicy(db string, rp *meta.RetentionPolicySpec, makeDefault bool) error {
	if _, err := s.CreateDatabase(db); err != nil {
		return err
	}

	stmt := fmt.Sprintf("CREATE+RETENTION+POLICY+%s+ON+\"%s\"+DURATION+%s+REPLICATION+%v+SHARD+DURATION+%s",
		rp.Name, db, rp.Duration, *rp.ReplicaN, rp.ShardGroupDuration)
	if makeDefault {
		stmt += "+DEFAULT"
	}

	_, err := s.HTTPPost(s.URL()+"/query?q="+stmt, nil)
	if err != nil {
		return err
	}
	return nil
}

func (s *RemoteServer) CreateSubscription(database, rp, name, mode string, destinations []string) error {
	dests := make([]string, 0, len(destinations))
	for _, d := range destinations {
		dests = append(dests, "'"+d+"'")
	}

	stmt := fmt.Sprintf("CREATE+SUBSCRIPTION+%s+ON+\"%s\".\"%s\"+DESTINATIONS+%v+%s",
		name, database, rp, mode, strings.Join(dests, ","))

	_, err := s.HTTPPost(s.URL()+"/query?q="+stmt, nil)
	if err != nil {
		return err
	}
	return nil
}

func (s *RemoteServer) DropDatabase(db string) error {
	stmt := fmt.Sprintf("DROP+DATABASE+%s", db)

	_, err := s.HTTPPost(s.URL()+"/query?q="+stmt, nil)
	if err != nil {
		return err
	}
	return nil
}

// Reset attempts to remove all database state by dropping everything
func (s *RemoteServer) Reset() error {
	stmt := fmt.Sprintf("SHOW+DATABASES")
	results, err := s.HTTPPost(s.URL()+"/query?q="+stmt, nil)
	if err != nil {
		return err
	}

	resp := &httpd.Response{}
	if resp.UnmarshalJSON([]byte(results)); err != nil {
		return err
	}

	for _, db := range resp.Results[0].Series[0].Values {
		if err := s.DropDatabase(fmt.Sprintf("%s", db[0])); err != nil {
			return err
		}
	}
	return nil

}

func (s *RemoteServer) WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error {
	panic("WritePoints not implemented")
}

// NewServer returns a new instance of Server.
func NewServer(c *run.Config) Server {
	buildInfo := &run.BuildInfo{
		Version: "testServer",
		Commit:  "testCommit",
		Branch:  "testBranch",
	}

	// If URL exists, create a server that will run against a remote endpoint
	if url := os.Getenv("URL"); url != "" {
		s := &RemoteServer{
			url: url,
			client: &client{
				URLFn: func() string {
					return url
				},
			},
		}
		if err := s.Reset(); err != nil {
			panic(err.Error())
		}
		return s
	}

	// Otherwise create a local server
	srv, _ := run.NewServer(c, buildInfo)
	s := LocalServer{
		client: &client{},
		Server: srv,
		Config: c,
	}
	s.client.URLFn = s.URL
	return &s
}

// OpenServer opens a test server.
func OpenServer(c *run.Config) Server {
	s := NewServer(c)
	configureLogging(s)
	if err := s.Open(); err != nil {
		panic(err.Error())
	}
	return s
}

// OpenServerWithVersion opens a test server with a specific version.
func OpenServerWithVersion(c *run.Config, version string) Server {
	// We can't change the versino of a remote server.  The test needs to
	// be skipped if using this func.
	if RemoteEnabled() {
		panic("OpenServerWithVersion not support with remote server")
	}

	buildInfo := &run.BuildInfo{
		Version: version,
		Commit:  "",
		Branch:  "",
	}
	srv, _ := run.NewServer(c, buildInfo)
	s := LocalServer{
		client: &client{},
		Server: srv,
		Config: c,
	}
	s.client.URLFn = s.URL

	if err := s.Open(); err != nil {
		panic(err.Error())
	}
	configureLogging(&s)

	return &s
}

// OpenDefaultServer opens a test server with a default database & retention policy.
func OpenDefaultServer(c *run.Config) Server {
	s := OpenServer(c)
	if err := s.CreateDatabaseAndRetentionPolicy("db0", newRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		panic(err)
	}
	return s
}

// Close shuts down the server and removes all temporary paths.
func (s *LocalServer) Close() {
	if err := s.Server.Close(); err != nil {
		panic(err.Error())
	}
	if err := os.RemoveAll(s.Config.Meta.Dir); err != nil {
		panic(err.Error())
	}
	if err := os.RemoveAll(s.Config.Data.Dir); err != nil {
		panic(err.Error())
	}
	// Nil the server so our deadlock detector goroutine can determine if we completed writes
	// without timing out
	s.Server = nil
}

func (s *LocalServer) Closed() bool {
	return s.Server == nil
}

// URL returns the base URL for the httpd endpoint.
func (s *LocalServer) URL() string {
	for _, service := range s.Services {
		if service, ok := service.(*httpd.Service); ok {
			return "http://" + service.Addr().String()
		}
	}
	panic("httpd server not found in services")
}

func (s *LocalServer) CreateDatabase(db string) (*meta.DatabaseInfo, error) {
	return s.MetaClient.CreateDatabase(db)
}

// CreateDatabaseAndRetentionPolicy will create the database and retention policy.
func (s *LocalServer) CreateDatabaseAndRetentionPolicy(db string, rp *meta.RetentionPolicySpec, makeDefault bool) error {
	if _, err := s.MetaClient.CreateDatabase(db); err != nil {
		return err
	} else if _, err := s.MetaClient.CreateRetentionPolicy(db, rp, makeDefault); err != nil {
		return err
	}
	return nil
}

func (s *LocalServer) CreateSubscription(database, rp, name, mode string, destinations []string) error {
	return s.MetaClient.CreateSubscription(database, rp, name, mode, destinations)
}

func (s *LocalServer) DropDatabase(db string) error {
	return s.MetaClient.DropDatabase(db)
}

func (s *LocalServer) Reset() error {
	for _, db := range s.MetaClient.Databases() {
		if err := s.DropDatabase(db.Name); err != nil {
			return err
		}
	}
	return nil
}

// client abstract querying and writing to a Server using HTTP
type client struct {
	URLFn func() string
}

func (c *client) URL() string {
	return c.URLFn()
}

// Query executes a query against the server and returns the results.
func (s *client) Query(query string) (results string, err error) {
	return s.QueryWithParams(query, nil)
}

// MustQuery executes a query against the server and returns the results.
func (s *client) MustQuery(query string) string {
	results, err := s.Query(query)
	if err != nil {
		panic(err)
	}
	return results
}

// Query executes a query against the server and returns the results.
func (s *client) QueryWithParams(query string, values url.Values) (results string, err error) {
	var v url.Values
	if values == nil {
		v = url.Values{}
	} else {
		v, _ = url.ParseQuery(values.Encode())
	}
	v.Set("q", query)
	return s.HTTPPost(s.URL()+"/query?"+v.Encode(), nil)
}

// MustQueryWithParams executes a query against the server and returns the results.
func (s *client) MustQueryWithParams(query string, values url.Values) string {
	results, err := s.QueryWithParams(query, values)
	if err != nil {
		panic(err)
	}
	return results
}

// HTTPGet makes an HTTP GET request to the server and returns the response.
func (s *client) HTTPGet(url string) (results string, err error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	body := strings.TrimSpace(string(MustReadAll(resp.Body)))
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
func (s *client) HTTPPost(url string, content []byte) (results string, err error) {
	buf := bytes.NewBuffer(content)
	resp, err := http.Post(url, "application/json", buf)
	if err != nil {
		return "", err
	}
	body := strings.TrimSpace(string(MustReadAll(resp.Body)))
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

type WriteError struct {
	body       string
	statusCode int
}

func (wr WriteError) StatusCode() int {
	return wr.statusCode
}

func (wr WriteError) Body() string {
	return wr.body
}

func (wr WriteError) Error() string {
	return fmt.Sprintf("invalid status code: code=%d, body=%s", wr.statusCode, wr.body)
}

func (s *LocalServer) WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error {
	return s.PointsWriter.WritePoints(database, retentionPolicy, consistencyLevel, points)
}

// Write executes a write against the server and returns the results.
func (s *client) Write(db, rp, body string, params url.Values) (results string, err error) {
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
		return "", WriteError{statusCode: resp.StatusCode, body: string(MustReadAll(resp.Body))}
	}
	return string(MustReadAll(resp.Body)), nil
}

// MustWrite executes a write to the server. Panic on error.
func (s *client) MustWrite(db, rp, body string, params url.Values) string {
	results, err := s.Write(db, rp, body, params)
	if err != nil {
		panic(err)
	}
	return results
}

// NewConfig returns the default config with temporary paths.
func NewConfig() *run.Config {
	c := run.NewConfig()
	c.BindAddress = "127.0.0.1:0"
	c.ReportingDisabled = true
	c.Coordinator.WriteTimeout = toml.Duration(30 * time.Second)
	c.Meta.Dir = MustTempFile()

	if !testing.Verbose() {
		c.Meta.LoggingEnabled = false
	}

	c.Data.Dir = MustTempFile()
	c.Data.WALDir = MustTempFile()

	c.HTTPD.Enabled = true
	c.HTTPD.BindAddress = "127.0.0.1:0"
	c.HTTPD.LogEnabled = testing.Verbose()

	c.Monitor.StoreEnabled = false

	return c
}

func newRetentionPolicySpec(name string, rf int, duration time.Duration) *meta.RetentionPolicySpec {
	return &meta.RetentionPolicySpec{Name: name, ReplicaN: &rf, Duration: &duration}
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

func RemoteEnabled() bool {
	return os.Getenv("URL") != ""
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
	once     bool
}

// Execute runs the command and returns an err if it fails
func (q *Query) Execute(s Server) (err error) {
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
	return fmt.Sprintf("%s: unexpected results\nquery:  %s\nparams:  %v\nexp:    %s\nactual: %s\n", q.name, q.command, q.params, q.exp, q.act)
}

type Write struct {
	db   string
	rp   string
	data string
}

func (w *Write) duplicate() *Write {
	return &Write{
		db:   w.db,
		rp:   w.rp,
		data: w.data,
	}
}

type Writes []*Write

func (a Writes) duplicate() Writes {
	writes := make(Writes, 0, len(a))
	for _, w := range a {
		writes = append(writes, w.duplicate())
	}
	return writes
}

type Tests map[string]Test

type Test struct {
	initialized bool
	writes      Writes
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

func (t Test) duplicate() Test {
	test := Test{
		initialized: t.initialized,
		writes:      t.writes.duplicate(),
		db:          t.db,
		rp:          t.rp,
		exp:         t.exp,
		queries:     make([]*Query, len(t.queries)),
	}

	if t.params != nil {
		t.params = url.Values{}
		for k, a := range t.params {
			vals := make([]string, len(a))
			copy(vals, a)
			test.params[k] = vals
		}
	}
	copy(test.queries, t.queries)
	return test
}

func (t *Test) addQueries(q ...*Query) {
	t.queries = append(t.queries, q...)
}

func (t *Test) database() string {
	if t.db != "" {
		return t.db
	}
	return "db0"
}

func (t *Test) retentionPolicy() string {
	if t.rp != "" {
		return t.rp
	}
	return "default"
}

func (t *Test) init(s Server) error {
	if len(t.writes) == 0 || t.initialized {
		return nil
	}
	if t.db == "" {
		t.db = "db0"
	}
	if t.rp == "" {
		t.rp = "rp0"
	}

	if err := writeTestData(s, t); err != nil {
		return err
	}

	t.initialized = true

	return nil
}

func writeTestData(s Server, t *Test) error {
	for i, w := range t.writes {
		if w.db == "" {
			w.db = t.database()
		}
		if w.rp == "" {
			w.rp = t.retentionPolicy()
		}

		if err := s.CreateDatabaseAndRetentionPolicy(w.db, newRetentionPolicySpec(w.rp, 1, 0), true); err != nil {
			return err
		}
		if res, err := s.Write(w.db, w.rp, w.data, t.params); err != nil {
			return fmt.Errorf("write #%d: %s", i, err)
		} else if t.exp != res {
			return fmt.Errorf("unexpected results\nexp: %s\ngot: %s\n", t.exp, res)
		}
	}

	return nil
}

func configureLogging(s Server) {
	// Set the logger to discard unless verbose is on
	if !testing.Verbose() {
		s.SetLogOutput(ioutil.Discard)
	}
}
