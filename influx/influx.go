package influx

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/influxdata/chronograf"
)

// Client is a device for retrieving time series data from an InfluxDB instance
type Client struct {
	URL *url.URL

	Logger chronograf.Logger
}

// NewClient initializes an HTTP Client for InfluxDB. UDP, although supported
// for querying InfluxDB, is not supported here to remove the need to
// explicitly Close the client.
func NewClient(host string, lg chronograf.Logger) (*Client, error) {
	l := lg.WithField("host", host)
	u, err := url.Parse(host)
	if err != nil {
		l.Error("Error initialize influx client: err:", err)
		return nil, err
	}
	return &Client{
		URL:    u,
		Logger: l,
	}, nil
}

type Response struct {
	Results json.RawMessage
	Err     string `json:"error,omitempty"`
}

func (r Response) MarshalJSON() ([]byte, error) {
	return r.Results, nil
}

func (c *Client) query(u *url.URL, q chronograf.Query) (chronograf.Response, error) {
	u.Path = "query"
	req, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	c.Logger.
		WithField("component", "proxy").
		WithField("host", req.Host).
		WithField("command", q.Command).
		WithField("db", q.DB).
		WithField("rp", q.RP).
		Debug("query")

	params := req.URL.Query()
	params.Set("q", q.Command)
	params.Set("db", q.DB)
	params.Set("rp", q.RP)
	params.Set("epoch", "ms")
	req.URL.RawQuery = params.Encode()
	httpClient := &http.Client{}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("received status code %d from server", resp.StatusCode)
	}

	var response Response
	dec := json.NewDecoder(resp.Body)
	decErr := dec.Decode(&response)

	// ignore this error if we got an invalid status code
	if decErr != nil && decErr.Error() == "EOF" && resp.StatusCode != http.StatusOK {
		decErr = nil
	}

	// If we got a valid decode error, send that back
	if decErr != nil {
		c.Logger.
			WithField("component", "proxy").
			WithField("host", req.Host).
			WithField("command", q.Command).
			WithField("db", q.DB).
			WithField("rp", q.RP).
			WithField("influx_status", resp.StatusCode).
			Error("Error parsing results from influxdb: err:", decErr)
		return nil, decErr
	}

	// If we don't have an error in our json response, and didn't get statusOK
	// then send back an error
	if resp.StatusCode != http.StatusOK && response.Err != "" {
		c.Logger.
			WithField("component", "proxy").
			WithField("host", req.Host).
			WithField("command", q.Command).
			WithField("db", q.DB).
			WithField("rp", q.RP).
			WithField("influx_status", resp.StatusCode).
			Error("Received non-200 response from influxdb")

		return &response, fmt.Errorf("received status code %d from server",
			resp.StatusCode)
	}
	return &response, nil
}

type result struct {
	Response chronograf.Response
	Err      error
}

// Query issues a request to a configured InfluxDB instance for time series
// information specified by query. Queries must be "fully-qualified," and
// include both the database and retention policy. In-flight requests can be
// cancelled using the provided context.
func (c *Client) Query(ctx context.Context, q chronograf.Query) (chronograf.Response, error) {
	resps := make(chan (result))
	go func() {
		resp, err := c.query(c.URL, q)
		resps <- result{resp, err}
	}()

	select {
	case resp := <-resps:
		return resp.Response, resp.Err
	case <-ctx.Done():
		return nil, chronograf.ErrUpstreamTimeout
	}
}

func (c *Client) Connect(ctx context.Context, src *chronograf.Source) error {
	u, err := url.Parse(src.URL)
	if err != nil {
		return err
	}
	u.User = url.UserPassword(src.Username, src.Password)
	c.URL = u
	return nil
}
