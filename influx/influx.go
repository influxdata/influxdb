package influx

import (
	ixClient "github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/mrfusion"

	"golang.org/x/net/context"
)

// Client is a device for retrieving time series data from an InfluxDB instance
type Client struct {
	ix ixClient.Client
}

// NewClient initializes a Client
func NewClient(host string) (*Client, error) {
	cl, err := ixClient.NewHTTPClient(ixClient.HTTPConfig{
		Addr: host,
	})

	if err != nil {
		return nil, err
	}

	return &Client{
		ix: cl,
	}, nil
}

// Query issues a request to a configured InfluxDB instance for time series
// information specified by query. Queries must be "fully-qualified," and
// include both the database and retention policy. In-flight requests can be
// cancelled using the provided context.
func (c *Client) Query(ctx context.Context, query mrfusion.Query) (mrfusion.Response, error) {
	q := ixClient.NewQuery(string(query), "", "")
	resps := make(chan (response))
	go func() {
		resp, err := c.ix.Query(q)
		resps <- response{resp, err}
	}()

	select {
	case resp := <-resps:
		return resp, resp.err
	case <-ctx.Done():
		return nil, nil
	}
}
