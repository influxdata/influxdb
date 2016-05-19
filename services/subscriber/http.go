package subscriber

import (
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/coordinator"
)

// HTTP supports writing points over HTTP using the line protocol.
type HTTP struct {
	c client.Client
}

// NewHTTP returns a new HTTP points writer with default options.
func NewHTTP(addr string, timeout time.Duration) (*HTTP, error) {
	conf := client.HTTPConfig{
		Addr:    addr,
		Timeout: timeout,
	}
	c, err := client.NewHTTPClient(conf)
	if err != nil {
		return nil, err
	}
	return &HTTP{c: c}, nil
}

// WritePoints writes points over HTTP transport.
func (h *HTTP) WritePoints(p *coordinator.WritePointsRequest) (err error) {
	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database:        p.Database,
		RetentionPolicy: p.RetentionPolicy,
	})
	for _, pt := range p.Points {
		bp.AddPoint(client.NewPointFrom(pt))
	}
	err = h.c.Write(bp)
	return
}
