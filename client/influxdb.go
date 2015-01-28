package client

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/url"
	"time"

	"github.com/influxdb/influxdb"
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
	Points          []influxdb.Point
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

func (c *Client) Query(q Query) (*influxdb.Results, error) {
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

	var results influxdb.Results
	err = json.NewDecoder(resp.Body).Decode(&results)
	if err != nil {
		return nil, err
	}
	return &results, nil
}

func (c *Client) Write(writes ...Write) (*influxdb.Results, error) {
	c.url.Path = "write"
	type data struct {
		Points          []influxdb.Point `json:"points"`
		Database        string           `json:"database"`
		RetentionPolicy string           `json:"retentionPolicy"`
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

	var results influxdb.Results
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

// utility functions

func (c *Client) Addr() string {
	return c.url.String()
}

//func (c *Client) urlFor(path string) (*url.URL, error) {
//var u *url.URL
//u, err := url.Parse(fmt.Sprintf("%s%s", c.addr, path))
//if err != nil {
//return nil, err
//}
//if c.username != "" {
//u.User = url.UserPassword(c.username, c.password)
//}
//u.Scheme = "http"
//return u, nil
//}

// helper functions

func detect(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
