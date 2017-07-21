package influx

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/influxdata/chronograf"
)

var _ chronograf.TimeSeries = &Client{}
var _ chronograf.TSDBStatus = &Client{}
var _ chronograf.Databases = &Client{}

// Shared transports for all clients to prevent leaking connections
var (
	skipVerifyTransport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	defaultTransport = &http.Transport{}
)

// Client is a device for retrieving time series data from an InfluxDB instance
type Client struct {
	URL                *url.URL
	Bearer             Bearer
	InsecureSkipVerify bool
	Logger             chronograf.Logger
}

// Response is a partial JSON decoded InfluxQL response used
// to check for some errors
type Response struct {
	Results json.RawMessage
	Err     string `json:"error,omitempty"`
}

// MarshalJSON returns the raw results bytes from the response
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
	command := q.Command
	// TODO(timraymond): move this upper Query() function
	if len(q.TemplateVars) > 0 {
		command = TemplateReplace(q.Command, q.TemplateVars)
	}
	logs := c.Logger.
		WithField("component", "proxy").
		WithField("host", req.Host).
		WithField("command", command).
		WithField("db", q.DB).
		WithField("rp", q.RP)
	logs.Debug("query")

	params := req.URL.Query()
	params.Set("q", command)
	params.Set("db", q.DB)
	params.Set("rp", q.RP)
	params.Set("epoch", "ms") // TODO(timraymond): set this based on analysis
	req.URL.RawQuery = params.Encode()

	if c.Bearer != nil && u.User != nil {
		token, err := c.Bearer.Token(u.User.Username())
		if err != nil {
			logs.Error("Error creating token", err)
			return nil, fmt.Errorf("Unable to create token")
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}

	hc := &http.Client{}
	if c.InsecureSkipVerify {
		hc.Transport = skipVerifyTransport
	} else {
		hc.Transport = defaultTransport
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var response Response
	dec := json.NewDecoder(resp.Body)
	decErr := dec.Decode(&response)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("received status code %d from server: err: %s", resp.StatusCode, response.Err)
	}

	// ignore this error if we got an invalid status code
	if decErr != nil && decErr.Error() == "EOF" && resp.StatusCode != http.StatusOK {
		decErr = nil
	}

	// If we got a valid decode error, send that back
	if decErr != nil {
		logs.WithField("influx_status", resp.StatusCode).
			Error("Error parsing results from influxdb: err:", decErr)
		return nil, decErr
	}

	// If we don't have an error in our json response, and didn't get statusOK
	// then send back an error
	if resp.StatusCode != http.StatusOK && response.Err != "" {
		logs.
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

// Connect caches the URL and optional Bearer Authorization for the data source
func (c *Client) Connect(ctx context.Context, src *chronograf.Source) error {
	u, err := url.Parse(src.URL)
	if err != nil {
		return err
	}
	u.User = url.UserPassword(src.Username, src.Password)
	// Only allow acceptance of all certs if the scheme is https AND the user opted into to the setting.
	if u.Scheme == "https" && src.InsecureSkipVerify {
		c.InsecureSkipVerify = src.InsecureSkipVerify
	}
	c.URL = u

	// Optionally, add the shared secret JWT token creation
	if src.Username != "" && src.SharedSecret != "" {
		c.Bearer = &BearerJWT{
			src.SharedSecret,
		}
	} else {
		// Clear out the bearer if not needed
		c.Bearer = nil
	}
	return nil
}

// Users transforms InfluxDB into a user store
func (c *Client) Users(ctx context.Context) chronograf.UsersStore {
	return c
}

// Roles aren't support in OSS
func (c *Client) Roles(ctx context.Context) (chronograf.RolesStore, error) {
	return nil, fmt.Errorf("Roles not support in open-source InfluxDB.  Roles are support in Influx Enterprise")
}

// Ping hits the influxdb ping endpoint and returns the type of influx
func (c *Client) Ping(ctx context.Context) error {
	_, _, err := c.pingTimeout(ctx)
	return err
}

// Version hits the influxdb ping endpoint and returns the version of influx
func (c *Client) Version(ctx context.Context) (string, error) {
	version, _, err := c.pingTimeout(ctx)
	return version, err
}

// Type hits the influxdb ping endpoint and returns the type of influx running
func (c *Client) Type(ctx context.Context) (string, error) {
	_, tsdbType, err := c.pingTimeout(ctx)
	return tsdbType, err
}

func (c *Client) pingTimeout(ctx context.Context) (string, string, error) {
	resps := make(chan (pingResult))
	go func() {
		version, tsdbType, err := c.ping(c.URL)
		resps <- pingResult{version, tsdbType, err}
	}()

	select {
	case resp := <-resps:
		return resp.Version, resp.Type, resp.Err
	case <-ctx.Done():
		return "", "", chronograf.ErrUpstreamTimeout
	}
}

type pingResult struct {
	Version string
	Type    string
	Err     error
}

func (c *Client) ping(u *url.URL) (string, string, error) {
	u.Path = "ping"

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return "", "", err
	}

	hc := &http.Client{}
	if c.InsecureSkipVerify {
		hc.Transport = skipVerifyTransport
	} else {
		hc.Transport = defaultTransport
	}

	resp, err := hc.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", "", err
	}

	if resp.StatusCode != http.StatusNoContent {
		var err = fmt.Errorf(string(body))
		return "", "", err
	}

	version := resp.Header.Get("X-Influxdb-Version")
	if strings.Contains(version, "-c") {
		return version, chronograf.InfluxEnterprise, nil
	} else if strings.Contains(version, "relay") {
		return version, chronograf.InfluxRelay, nil
	}
	return version, chronograf.InfluxDB, nil
}
