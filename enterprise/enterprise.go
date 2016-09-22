package enterprise

import (
	"container/ring"
	"net/url"
	"strings"

	"github.com/influxdata/mrfusion"
	"github.com/influxdata/mrfusion/influx"
	"github.com/influxdata/plutonium/meta/control"
	"golang.org/x/net/context"
)

var _ mrfusion.TimeSeries = &Client{}

// Client is a device for retrieving time series data from an Influx Enterprise
// cluster. It is configured using the addresses of one or more meta node URLs.
// Data node URLs are retrieved automatically from the meta nodes and queries
// are appropriately load balanced across the cluster.
type Client struct {
	Ctrl interface {
		ShowCluster() (*control.Cluster, error)
	}

	dataNodes *ring.Ring
}

// NewClientWithTimeSeries initializes a Client with a known set of TimeSeries.
// It is not necessary to call Open when creating a Client using this function
func NewClientWithTimeSeries(series ...mrfusion.TimeSeries) *Client {
	c := &Client{}

	c.dataNodes = ring.New(len(series))

	for _, s := range series {
		c.dataNodes.Value = s
		c.dataNodes = c.dataNodes.Next()
	}

	return c
}

// NewClientWithURL initializes an Enterprise client with a URL to a Meta Node.
// Acceptable URLs include host:port combinations as well as scheme://host:port
// varieties. TLS is used when the URL contains "https" or when the TLS
// parameter is set. The latter option is provided for host:port combinations
func NewClientWithURL(mu string, tls bool) (*Client, error) {
	metaURL, err := parseMetaURL(mu, tls)
	if err != nil {
		return nil, err
	}

	return &Client{
		Ctrl: control.NewClient(metaURL.Host),
	}, nil
}

// Open prepares a Client to process queries. It must be called prior to calling Query
func (c *Client) Open() error {
	cluster, err := c.Ctrl.ShowCluster()
	if err != nil {
		return err
	}

	c.dataNodes = ring.New(len(cluster.DataNodes))
	for _, dn := range cluster.DataNodes {
		cl, err := influx.NewClient(dn.HTTPAddr)
		if err != nil {
			continue
		} else {
			c.dataNodes.Value = cl
			c.dataNodes = c.dataNodes.Next()
		}
	}
	return nil
}

// Query retrieves timeseries information pertaining to a specified query. It
// can be cancelled by using a provided context.
func (c *Client) Query(ctx context.Context, q mrfusion.Query) (mrfusion.Response, error) {
	return c.nextDataNode().Query(ctx, q)
}

// MonitoredServices returns the services monitored by this Enterprise cluster.
func (c *Client) MonitoredServices(ctx context.Context) ([]mrfusion.MonitoredService, error) {
	return []mrfusion.MonitoredService{}, nil
}

// nextDataNode retrieves the next available data node
func (c *Client) nextDataNode() mrfusion.TimeSeries {
	c.dataNodes = c.dataNodes.Next()
	return c.dataNodes.Value.(mrfusion.TimeSeries)
}

// parseMetaURL constructs a url from either a host:port combination or a
// scheme://host:port combo. The optional TLS parameter takes precedence over
// any TLS preference found in the provided URL
func parseMetaURL(mu string, tls bool) (metaURL *url.URL, err error) {
	if strings.Contains(mu, "http") {
		metaURL, err = url.Parse(mu)
	} else {
		metaURL = &url.URL{
			Scheme: "http",
			Host:   mu,
		}
	}

	if tls {
		metaURL.Scheme = "https"
	}

	return
}
