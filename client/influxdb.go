package client

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/influxdb/influxdb"
)

const (
	defaultAddr = "localhost:8086"
)

type Config struct {
	Addr     string
	Username string
	Password string
}

type Client struct {
	addr       string
	username   string
	password   string
	httpClient *http.Client
}

type Query struct {
	Command  string
	Database string
}

type Write struct {
}

func NewClient(c Config) (*Client, error) {
	client := Client{
		addr:       detect(c.Addr, defaultAddr),
		username:   c.Username,
		password:   c.Password,
		httpClient: &http.Client{},
	}
	return &client, nil
}

func (c *Client) Query(q Query) (influxdb.Results, error) {
	u, err := c.urlFor("/query")
	if err != nil {
		return nil, err
	}
	values := u.Query()
	values.Set("q", q.Command)
	values.Set("db", q.Database)
	u.RawQuery = values.Encode()

	resp, err := c.httpClient.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var results influxdb.Results
	err = json.NewDecoder(resp.Body).Decode(&results)
	if err != nil {
		return nil, err
	}
	return results, nil
}

func (c *Client) Write(writes ...Write) (influxdb.Results, error) {
	return nil, nil
}

func (c *Client) Ping() (time.Duration, error) {
	now := time.Now()
	u, err := c.urlFor("/ping")
	if err != nil {
		return 0, err
	}
	_, err = c.httpClient.Get(u.String())
	if err != nil {
		return 0, err
	}
	return time.Since(now), nil
}

// utility functions

func (c *Client) Addr() string {
	return c.addr
}

func (c *Client) urlFor(path string) (*url.URL, error) {
	var u *url.URL
	u, err := url.Parse(fmt.Sprintf("%s%s", c.addr, path))
	if err != nil {
		return nil, err
	}
	if c.username != "" {
		u.User = url.UserPassword(c.username, c.password)
	}
	return u, nil
}

// helper functions

func detect(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
