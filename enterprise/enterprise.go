package enterprise

import (
	"container/ring"

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

// NewClient initializes and returns a Client.
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

func (c *Client) nextDataNode() mrfusion.TimeSeries {
	c.dataNodes = c.dataNodes.Next()
	return c.dataNodes.Value.(mrfusion.TimeSeries)
}
