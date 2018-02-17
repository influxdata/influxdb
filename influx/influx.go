package influx

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

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
	Authorizer         Authorizer
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
		command, err = TemplateReplace(q.Command, q.TemplateVars, time.Now())
		if err != nil {
			return nil, err
		}
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
	params.Set("epoch", "ms")
	if q.Epoch != "" {
		params.Set("epoch", q.Epoch)
	}
	req.URL.RawQuery = params.Encode()

	if c.Authorizer != nil {
		if err := c.Authorizer.Set(req); err != nil {
			logs.Error("Error setting authorization header ", err)
			return nil, err
		}
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
	c.Authorizer = DefaultAuthorization(src)
	// Only allow acceptance of all certs if the scheme is https AND the user opted into to the setting.
	if u.Scheme == "https" && src.InsecureSkipVerify {
		c.InsecureSkipVerify = src.InsecureSkipVerify
	}

	c.URL = u
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

	version := resp.Header.Get("X-Influxdb-Build")
	if version == "ENT" {
		return version, chronograf.InfluxEnterprise, nil
	}
	version = resp.Header.Get("X-Influxdb-Version")
	if strings.Contains(version, "-c") {
		return version, chronograf.InfluxEnterprise, nil
	} else if strings.Contains(version, "relay") {
		return version, chronograf.InfluxRelay, nil
	}

	return version, chronograf.InfluxDB, nil
}

// Write POSTs line protocol to a database and retention policy
func (c *Client) Write(ctx context.Context, point *chronograf.Point) error {
	lp, err := toLineProtocol(point)
	if err != nil {
		return err
	}

	err = c.write(ctx, c.URL, point.Database, point.RetentionPolicy, lp)
	if err == nil {
		return nil
	}

	// Some influxdb errors should not be treated as errors
	if strings.Contains(err.Error(), "hinted handoff queue not empty") {
		// This is an informational message
		return nil
	}

	// If the database was not found, try to recreate it:
	if strings.Contains(err.Error(), "database not found") {
		_, err = c.CreateDB(ctx, &chronograf.Database{
			Name: point.Database,
		})
		if err != nil {
			return err
		}
		// retry the write
		return c.write(ctx, c.URL, point.Database, point.RetentionPolicy, lp)
	}

	return err
}

func (c *Client) write(ctx context.Context, u *url.URL, db, rp, lp string) error {
	u.Path = "write"
	req, err := http.NewRequest("POST", u.String(), strings.NewReader(lp))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	if c.Authorizer != nil {
		if err := c.Authorizer.Set(req); err != nil {
			return err
		}
	}

	params := req.URL.Query()
	params.Set("db", db)
	params.Set("rp", rp)
	req.URL.RawQuery = params.Encode()

	hc := &http.Client{}
	if c.InsecureSkipVerify {
		hc.Transport = skipVerifyTransport
	} else {
		hc.Transport = defaultTransport
	}

	errChan := make(chan (error))
	go func() {
		resp, err := hc.Do(req)
		if err != nil {
			errChan <- err
			return
		}

		if resp.StatusCode == http.StatusNoContent {
			errChan <- nil
			return
		}
		defer resp.Body.Close()

		var response Response
		dec := json.NewDecoder(resp.Body)
		err = dec.Decode(&response)
		if err != nil && err.Error() != "EOF" {
			errChan <- err
			return
		}

		errChan <- errors.New(response.Err)
		return
	}()

	select {
	case err := <-errChan:
		return err
	case <-ctx.Done():
		return chronograf.ErrUpstreamTimeout
	}
}
