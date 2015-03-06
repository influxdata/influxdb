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

type Query struct {
	Command  string
	Database string
}

type Config struct {
	URL       url.URL
	Username  string
	Password  string
	UserAgent string
}

type Client struct {
	url        url.URL
	username   string
	password   string
	httpClient *http.Client
	userAgent  string
}

func NewClient(c Config) (*Client, error) {
	client := Client{
		url:        c.URL,
		username:   c.Username,
		password:   c.Password,
		httpClient: &http.Client{},
		userAgent:  c.UserAgent,
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

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	if c.userAgent != "" {
		req.Header.Set("User-Agent", c.userAgent)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var results Results
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	err = dec.Decode(&results)
	if err != nil {
		return nil, err
	}
	return &results, nil
}

func (c *Client) Write(bp BatchPoints) (*Results, error) {
	c.url.Path = "write"
	type data struct {
		Points          []Point `json:"points"`
		Database        string  `json:"database"`
		RetentionPolicy string  `json:"retentionPolicy"`
	}

	b, err := json.Marshal(&bp)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", c.url.String(), bytes.NewBuffer(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.userAgent != "" {
		req.Header.Set("User-Agent", c.userAgent)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var results Results
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	err = dec.Decode(&results)

	if err != nil {
		return nil, err
	}
	return &results, nil
}

func (c *Client) Ping() (time.Duration, string, error) {
	now := time.Now()
	u := c.url
	u.Path = "ping"

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return 0, "", err
	}
	if c.userAgent != "" {
		req.Header.Set("User-Agent", c.userAgent)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	version := resp.Header.Get("X-Influxdb-Version")
	return time.Since(now), version, nil
}

// Structs

// Result represents a resultset returned from a single statement.
type Result struct {
	Series []influxql.Row
	Err    error
}

// MarshalJSON encodes the result into JSON.
func (r *Result) MarshalJSON() ([]byte, error) {
	// Define a struct that outputs "error" as a string.
	var o struct {
		Series []influxql.Row `json:"series,omitempty"`
		Err    string         `json:"error,omitempty"`
	}

	// Copy fields to output struct.
	o.Series = r.Series
	if r.Err != nil {
		o.Err = r.Err.Error()
	}

	return json.Marshal(&o)
}

// UnmarshalJSON decodes the data into the Result struct
func (r *Result) UnmarshalJSON(b []byte) error {
	var o struct {
		Series []influxql.Row `json:"series,omitempty"`
		Err    string         `json:"error,omitempty"`
	}

	dec := json.NewDecoder(bytes.NewBuffer(b))
	dec.UseNumber()
	err := dec.Decode(&o)
	if err != nil {
		return err
	}
	r.Series = o.Series
	if o.Err != "" {
		r.Err = errors.New(o.Err)
	}
	return nil
}

// Results represents a list of statement results.
type Results struct {
	Results []Result
	Err     error
}

func (r Results) MarshalJSON() ([]byte, error) {
	// Define a struct that outputs "error" as a string.
	var o struct {
		Results []Result `json:"results,omitempty"`
		Err     string   `json:"error,omitempty"`
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
		Results []Result `json:"results,omitempty"`
		Err     string   `json:"error,omitempty"`
	}

	dec := json.NewDecoder(bytes.NewBuffer(b))
	dec.UseNumber()
	err := dec.Decode(&o)
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
	if a.Err != nil {
		return a.Err
	}
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

// Point defines the fields that will be written to the database
type Point struct {
	Name      string
	Tags      map[string]string
	Timestamp time.Time
	Fields    map[string]interface{}
	Precision string
}

// MarshalJSON will format the time in RFC3339Nano
// Precision is also ignored as it is only used for writing, not reading
// Or another way to say it is we always send back in nanosecond precision
func (p *Point) MarshalJSON() ([]byte, error) {
	point := struct {
		Name      string                 `json:"name"`
		Tags      map[string]string      `json:"tags,omitempty"`
		Timestamp string                 `json:"timestamp,omitempty"`
		Fields    map[string]interface{} `json:"fields"`
	}{
		Name:   p.Name,
		Tags:   p.Tags,
		Fields: p.Fields,
	}
	// Let it omit empty if it's really zero
	if !p.Timestamp.IsZero() {
		point.Timestamp = p.Timestamp.UTC().Format(time.RFC3339Nano)
	}
	return json.Marshal(&point)
}

// UnmarshalJSON decodes the data into the Point struct
func (p *Point) UnmarshalJSON(b []byte) error {
	var normal struct {
		Name      string                 `json:"name"`
		Tags      map[string]string      `json:"tags"`
		Timestamp time.Time              `json:"timestamp"`
		Precision string                 `json:"precision"`
		Fields    map[string]interface{} `json:"fields"`
	}
	var epoch struct {
		Name      string                 `json:"name"`
		Tags      map[string]string      `json:"tags"`
		Timestamp *int64                 `json:"timestamp"`
		Precision string                 `json:"precision"`
		Fields    map[string]interface{} `json:"fields"`
	}

	if err := func() error {
		var err error
		dec := json.NewDecoder(bytes.NewBuffer(b))
		dec.UseNumber()
		if err = dec.Decode(&epoch); err != nil {
			return err
		}
		// Convert from epoch to time.Time, but only if Timestamp
		// was actually set.
		var ts time.Time
		if epoch.Timestamp != nil {
			ts, err = EpochToTime(*epoch.Timestamp, epoch.Precision)
			if err != nil {
				return err
			}
		}
		p.Name = epoch.Name
		p.Tags = epoch.Tags
		p.Timestamp = ts
		p.Precision = epoch.Precision
		p.Fields = normalizeFields(epoch.Fields)
		return nil
	}(); err == nil {
		return nil
	}

	dec := json.NewDecoder(bytes.NewBuffer(b))
	dec.UseNumber()
	if err := dec.Decode(&normal); err != nil {
		return err
	}
	normal.Timestamp = SetPrecision(normal.Timestamp, normal.Precision)
	p.Name = normal.Name
	p.Tags = normal.Tags
	p.Timestamp = normal.Timestamp
	p.Precision = normal.Precision
	p.Fields = normalizeFields(normal.Fields)

	return nil
}

// Remove any notion of json.Number
func normalizeFields(fields map[string]interface{}) map[string]interface{} {
	newFields := map[string]interface{}{}

	for k, v := range fields {
		switch v := v.(type) {
		case json.Number:
			jv, e := v.Float64()
			if e != nil {
				panic(fmt.Sprintf("unable to convert json.Number to float64: %s", e))
			}
			newFields[k] = jv
		default:
			newFields[k] = v
		}
	}
	return newFields
}

// BatchPoints is used to send batched data in a single write.
type BatchPoints struct {
	Points          []Point           `json:"points"`
	Database        string            `json:"database"`
	RetentionPolicy string            `json:"retentionPolicy"`
	Tags            map[string]string `json:"tags"`
	Timestamp       time.Time         `json:"timestamp"`
	Precision       string            `json:"precision"`
}

// UnmarshalJSON decodes the data into the BatchPoints struct
func (bp *BatchPoints) UnmarshalJSON(b []byte) error {
	var normal struct {
		Points          []Point           `json:"points"`
		Database        string            `json:"database"`
		RetentionPolicy string            `json:"retentionPolicy"`
		Tags            map[string]string `json:"tags"`
		Timestamp       time.Time         `json:"timestamp"`
		Precision       string            `json:"precision"`
	}
	var epoch struct {
		Points          []Point           `json:"points"`
		Database        string            `json:"database"`
		RetentionPolicy string            `json:"retentionPolicy"`
		Tags            map[string]string `json:"tags"`
		Timestamp       *int64            `json:"timestamp"`
		Precision       string            `json:"precision"`
	}

	if err := func() error {
		var err error
		if err = json.Unmarshal(b, &epoch); err != nil {
			return err
		}
		// Convert from epoch to time.Time
		var ts time.Time
		if epoch.Timestamp != nil {
			ts, err = EpochToTime(*epoch.Timestamp, epoch.Precision)
			if err != nil {
				return err
			}
		}
		bp.Points = epoch.Points
		bp.Database = epoch.Database
		bp.RetentionPolicy = epoch.RetentionPolicy
		bp.Tags = epoch.Tags
		bp.Timestamp = ts
		bp.Precision = epoch.Precision
		return nil
	}(); err == nil {
		return nil
	}

	if err := json.Unmarshal(b, &normal); err != nil {
		return err
	}
	normal.Timestamp = SetPrecision(normal.Timestamp, normal.Precision)
	bp.Points = normal.Points
	bp.Database = normal.Database
	bp.RetentionPolicy = normal.RetentionPolicy
	bp.Tags = normal.Tags
	bp.Timestamp = normal.Timestamp
	bp.Precision = normal.Precision

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
