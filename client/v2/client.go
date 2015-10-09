package client

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/influxdb/influxdb/models"
)

type Config struct {
	// URL of the InfluxDB database
	URL *url.URL

	// Username is the influxdb username, optional
	Username string

	// Password is the influxdb password, optional
	Password string

	// UserAgent is the http User Agent, defaults to "InfluxDBClient"
	UserAgent string

	// Timeout for influxdb writes, defaults to no timeout
	Timeout time.Duration

	// InsecureSkipVerify gets passed to the http client, if true, it will
	// skip https certificate verification. Defaults to false
	InsecureSkipVerify bool
}

type BatchPointsConfig struct {
	// Precision is the write precision of the points, defaults to "ns"
	Precision string

	// Database is the database to write points to
	Database string

	// RetentionPolicy is the retention policy of the points
	RetentionPolicy string

	// Write consistency is the number of servers required to confirm write
	WriteConsistency string
}

type Client interface {
	// Write takes a BatchPoints object and writes all Points to InfluxDB.
	Write(bp BatchPoints) error

	// Query makes an InfluxDB Query on the database
	Query(q Query) (*Response, error)
}

// NewClient creates a client interface from the given config.
func NewClient(conf Config) Client {
	if conf.UserAgent == "" {
		conf.UserAgent = "InfluxDBClient"
	}
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: conf.InsecureSkipVerify,
		},
	}
	return &client{
		url:       conf.URL,
		username:  conf.Username,
		password:  conf.Password,
		useragent: conf.UserAgent,
		httpClient: &http.Client{
			Timeout:   conf.Timeout,
			Transport: tr,
		},
	}
}

type client struct {
	url        *url.URL
	username   string
	password   string
	useragent  string
	httpClient *http.Client
}

// BatchPoints is an interface into a batched grouping of points to write into
// InfluxDB together. BatchPoints is NOT thread-safe, you must create a separate
// batch for each goroutine.
type BatchPoints interface {
	// AddPoint adds the given point to the Batch of points
	AddPoint(p *Point)
	// Points lists the points in the Batch
	Points() []*Point

	// Precision returns the currently set precision of this Batch
	Precision() string
	// SetPrecision sets the precision of this batch.
	SetPrecision(s string) error

	// Database returns the currently set database of this Batch
	Database() string
	// SetDatabase sets the database of this Batch
	SetDatabase(s string)

	// WriteConsistency returns the currently set write consistency of this Batch
	WriteConsistency() string
	// SetWriteConsistency sets the write consistency of this Batch
	SetWriteConsistency(s string)

	// RetentionPolicy returns the currently set retention policy of this Batch
	RetentionPolicy() string
	// SetRetentionPolicy sets the retention policy of this Batch
	SetRetentionPolicy(s string)
}

// NewBatchPoints returns a BatchPoints interface based on the given config.
func NewBatchPoints(c BatchPointsConfig) (BatchPoints, error) {
	if c.Precision == "" {
		c.Precision = "ns"
	}
	if _, err := time.ParseDuration("1" + c.Precision); err != nil {
		return nil, err
	}
	bp := &batchpoints{
		database:         c.Database,
		precision:        c.Precision,
		retentionPolicy:  c.RetentionPolicy,
		writeConsistency: c.WriteConsistency,
	}
	return bp, nil
}

type batchpoints struct {
	points           []*Point
	database         string
	precision        string
	retentionPolicy  string
	writeConsistency string
}

func (bp *batchpoints) AddPoint(p *Point) {
	bp.points = append(bp.points, p)
}

func (bp *batchpoints) Points() []*Point {
	return bp.points
}

func (bp *batchpoints) Precision() string {
	return bp.precision
}

func (bp *batchpoints) Database() string {
	return bp.database
}

func (bp *batchpoints) WriteConsistency() string {
	return bp.writeConsistency
}

func (bp *batchpoints) RetentionPolicy() string {
	return bp.retentionPolicy
}

func (bp *batchpoints) SetPrecision(p string) error {
	if _, err := time.ParseDuration("1" + p); err != nil {
		return err
	}
	bp.precision = p
	return nil
}

func (bp *batchpoints) SetDatabase(db string) {
	bp.database = db
}

func (bp *batchpoints) SetWriteConsistency(wc string) {
	bp.writeConsistency = wc
}

func (bp *batchpoints) SetRetentionPolicy(rp string) {
	bp.retentionPolicy = rp
}

type Point struct {
	pt models.Point
}

// NewPoint returns a point with the given timestamp. If a timestamp is not
// given, then data is sent to the database without a timestamp, in which case
// the server will assign local time upon reception. NOTE: it is recommended
// to send data with a timestamp.
func NewPoint(
	name string,
	tags map[string]string,
	fields map[string]interface{},
	t ...time.Time,
) *Point {
	var T time.Time
	if len(t) > 0 {
		T = t[0]
	}
	return &Point{
		pt: models.NewPoint(name, tags, fields, T),
	}
}

// String returns a line-protocol string of the Point
func (p *Point) String() string {
	return p.pt.String()
}

// PrecisionString returns a line-protocol string of the Point, at precision
func (p *Point) PrecisionString(precison string) string {
	return p.pt.PrecisionString(precison)
}

func (c *client) Write(bp BatchPoints) error {
	u := c.url
	u.Path = "write"

	var b bytes.Buffer
	for _, p := range bp.Points() {
		if _, err := b.WriteString(p.pt.PrecisionString(bp.Precision())); err != nil {
			return err
		}

		if err := b.WriteByte('\n'); err != nil {
			return err
		}
	}

	req, err := http.NewRequest("POST", u.String(), &b)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "")
	req.Header.Set("User-Agent", c.useragent)
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	params := req.URL.Query()
	params.Set("db", bp.Database())
	params.Set("rp", bp.RetentionPolicy())
	params.Set("precision", bp.Precision())
	params.Set("consistency", bp.WriteConsistency())
	req.URL.RawQuery = params.Encode()

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		var err = fmt.Errorf(string(body))
		return err
	}

	return nil
}

// Query defines a query to send to the server
type Query struct {
	Command   string
	Database  string
	Precision string
}

// Response represents a list of statement results.
type Response struct {
	Results []Result
	Err     error
}

// Error returns the first error from any statement.
// Returns nil if no errors occurred on any statements.
func (r *Response) Error() error {
	if r.Err != nil {
		return r.Err
	}
	for _, result := range r.Results {
		if result.Err != nil {
			return result.Err
		}
	}
	return nil
}

// Result represents a resultset returned from a single statement.
type Result struct {
	Series []models.Row
	Err    error
}

// Query sends a command to the server and returns the Response
func (c *client) Query(q Query) (*Response, error) {
	u := c.url

	u.Path = "query"
	values := u.Query()
	values.Set("q", q.Command)
	values.Set("db", q.Database)
	if q.Precision != "" {
		values.Set("epoch", q.Precision)
	}
	u.RawQuery = values.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", c.useragent)
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var response Response
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	decErr := dec.Decode(&response)

	// ignore this error if we got an invalid status code
	if decErr != nil && decErr.Error() == "EOF" && resp.StatusCode != http.StatusOK {
		decErr = nil
	}
	// If we got a valid decode error, send that back
	if decErr != nil {
		return nil, decErr
	}
	// If we don't have an error in our json response, and didn't get statusOK
	// then send back an error
	if resp.StatusCode != http.StatusOK && response.Error() == nil {
		return &response, fmt.Errorf("received status code %d from server",
			resp.StatusCode)
	}
	return &response, nil
}
