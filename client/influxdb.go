package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/influxdb/influxdb/influxql"
)

type Config struct {
	URL      url.URL
	Username string
	Password string
}

type Client struct {
	url        url.URL
	username   string
	password   string
	httpClient *http.Client
}

type Query struct {
	Command  string
	Database string
}

type Write struct {
	Database        string
	RetentionPolicy string
	Points          []Point
}

func NewClient(c Config) (*Client, error) {
	client := Client{
		url:        c.URL,
		username:   c.Username,
		password:   c.Password,
		httpClient: &http.Client{},
	}
	return &client, nil
}

func (c *Client) Query(q Query) (*Results, error) {
	u := c.url

	u.Path = "query"
	values := u.Query()
	values.Set("q", q.Command)
	values.Set("db", q.Database)
	u.RawQuery = values.Encode()

	resp, err := c.httpClient.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var results Results
	err = json.NewDecoder(resp.Body).Decode(&results)
	if err != nil {
		return nil, err
	}
	return &results, nil
}

func (c *Client) Write(writes ...Write) (*Results, error) {
	c.url.Path = "write"
	type data struct {
		Points          []Point `json:"points"`
		Database        string  `json:"database"`
		RetentionPolicy string  `json:"retentionPolicy"`
	}

	d := []data{}
	for _, write := range writes {
		d = append(d, data{Points: write.Points, Database: write.Database, RetentionPolicy: write.RetentionPolicy})
	}

	b := []byte{}
	err := json.Unmarshal(b, &d)

	resp, err := c.httpClient.Post(c.url.String(), "application/json", bytes.NewBuffer(b))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var results Results
	err = json.NewDecoder(resp.Body).Decode(&results)
	if err != nil {
		return nil, err
	}
	return &results, nil
}

func (c *Client) Ping() (time.Duration, string, error) {
	now := time.Now()
	u := c.url
	u.Path = "ping"
	resp, err := c.httpClient.Get(u.String())
	if err != nil {
		return 0, "", err
	}
	version := resp.Header.Get("X-Influxdb-Version")
	return time.Since(now), version, nil
}

// Structs

// Result represents a resultset returned from a single statement.
type Result struct {
	Rows []*influxql.Row
	Err  error
}

// MarshalJSON encodes the result into JSON.
func (r *Result) MarshalJSON() ([]byte, error) {
	// Define a struct that outputs "error" as a string.
	var o struct {
		Rows []*influxql.Row `json:"rows,omitempty"`
		Err  string          `json:"error,omitempty"`
	}

	// Copy fields to output struct.
	o.Rows = r.Rows
	if r.Err != nil {
		o.Err = r.Err.Error()
	}

	return json.Marshal(&o)
}

// UnmarshalJSON decodes the data into the Result struct
func (r *Result) UnmarshalJSON(b []byte) error {
	var o struct {
		Rows []*influxql.Row `json:"rows,omitempty"`
		Err  string          `json:"error,omitempty"`
	}

	err := json.Unmarshal(b, &o)
	if err != nil {
		return err
	}
	r.Rows = o.Rows
	if o.Err != "" {
		r.Err = errors.New(o.Err)
	}
	return nil
}

// Results represents a list of statement results.
type Results struct {
	Results []*Result
	Err     error
}

func (r Results) MarshalJSON() ([]byte, error) {
	// Define a struct that outputs "error" as a string.
	var o struct {
		Results []*Result `json:"results,omitempty"`
		Err     string    `json:"error,omitempty"`
	}

	// Copy fields to output struct.
	o.Results = r.Results
	if r.Err != nil {
		o.Err = r.Err.Error()
	}

	return json.Marshal(&o)
}

// UnmarshalJSON decodes the data into the Results struct
func (r *Results) UnmarshalJSON(b []byte) error {
	var o struct {
		Results []*Result `json:"results,omitempty"`
		Err     string    `json:"error,omitempty"`
	}

	err := json.Unmarshal(b, &o)
	if err != nil {
		return err
	}
	r.Results = o.Results
	if o.Err != "" {
		r.Err = errors.New(o.Err)
	}
	return nil
}

// Error returns the first error from any statement.
// Returns nil if no errors occurred on any statements.
func (a Results) Error() error {
	for _, r := range a.Results {
		if r.Err != nil {
			return r.Err
		}
	}
	return nil
}

// Timestamp is a custom type so we marshal JSON properly into UTC nanosecond time
type Timestamp time.Time

// Time returns the time represented by the Timestamp
func (t Timestamp) Time() time.Time {
	return time.Time(t)
}

// MarshalJSON returns time in UTC with nanoseconds
func (t Timestamp) MarshalJSON() ([]byte, error) {
	// Always send back in UTC with nanoseconds
	s := t.Time().UTC().Format(time.RFC3339Nano)
	return []byte(`"` + s + `"`), nil
}

// Point defines the values that will be written to the database
type Point struct {
	Name      string                 `json:"name"`
	Tags      map[string]string      `json:"tags"`
	Timestamp Timestamp              `json:"timestamp"`
	Values    map[string]interface{} `json:"values"`
	Precision string                 `json:"precision"`
}

// UnmarshalJSON decodes the data into the Point struct
func (p *Point) UnmarshalJSON(b []byte) error {
	var normal struct {
		Name      string                 `json:"name"`
		Tags      map[string]string      `json:"tags"`
		Timestamp time.Time              `json:"timestamp"`
		Precision string                 `json:"precision"`
		Values    map[string]interface{} `json:"values"`
	}
	var epoch struct {
		Name      string                 `json:"name"`
		Tags      map[string]string      `json:"tags"`
		Timestamp int64                  `json:"timestamp"`
		Precision string                 `json:"precision"`
		Values    map[string]interface{} `json:"values"`
	}

	if err := func() error {
		var err error
		if err = json.Unmarshal(b, &epoch); err != nil {
			return err
		}
		// Convert from epoch to time.Time
		ts, err := EpochToTime(epoch.Timestamp, epoch.Precision)
		if err != nil {
			return err
		}
		p.Name = epoch.Name
		p.Tags = epoch.Tags
		p.Timestamp = Timestamp(ts)
		p.Precision = epoch.Precision
		p.Values = epoch.Values
		return nil
	}(); err == nil {
		return nil
	}

	if err := json.Unmarshal(b, &normal); err != nil {
		return err
	}
	normal.Timestamp = SetPrecision(normal.Timestamp, normal.Precision)
	p.Name = normal.Name
	p.Tags = normal.Tags
	p.Timestamp = Timestamp(normal.Timestamp)
	p.Precision = normal.Precision
	p.Values = normal.Values

	return nil
}

// utility functions

func (c *Client) Addr() string {
	return c.url.String()
}

// helper functions

// EpochToTime takes a unix epoch time and uses precision to return back a time.Time
func EpochToTime(epoch int64, precision string) (time.Time, error) {
	if precision == "" {
		precision = "s"
	}
	var t time.Time
	switch precision {
	case "h":
		t = time.Unix(0, epoch*int64(time.Hour))
	case "m":
		t = time.Unix(0, epoch*int64(time.Minute))
	case "s":
		t = time.Unix(0, epoch*int64(time.Second))
	case "ms":
		t = time.Unix(0, epoch*int64(time.Millisecond))
	case "u":
		t = time.Unix(0, epoch*int64(time.Microsecond))
	case "n":
		t = time.Unix(0, epoch)
	default:
		return time.Time{}, fmt.Errorf("Unknowm precision %q", precision)
	}
	return t, nil
}

// SetPrecision will round a time to the specified precision
func SetPrecision(t time.Time, precision string) time.Time {
	switch precision {
	case "n":
	case "u":
		return t.Round(time.Microsecond)
	case "ms":
		return t.Round(time.Millisecond)
	case "s":
		return t.Round(time.Second)
	case "m":
		return t.Round(time.Minute)
	case "h":
		return t.Round(time.Hour)
	}
	return t
}

func detect(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
