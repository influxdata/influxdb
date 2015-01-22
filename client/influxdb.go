package client

import "github.com/influxdb/influxdb"

type Config struct {
}

type Client struct {
}

type Query struct {
}

type Write struct {
}

func NewClient(c Config) (*Client, error) {
	return nil, nil
}

func (c *Client) Query(queries ...Query) (influxdb.Results, error) {
	return nil, nil
}

func (c *Client) Write(writes ...Write) (influxdb.Results, error) {
	return nil, nil
}
