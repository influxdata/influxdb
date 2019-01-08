// Package control provides a query controller.
package control

import (
	"context"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/control"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/query"
	"github.com/prometheus/client_golang/prometheus"
)

// orgLabel is the metric label to use in the controller
const orgLabel = "org"

// Controller implements AsyncQueryService by consuming a control.Controller.
type Controller struct {
	c *control.Controller
}

// NewController creates a new Controller specific to platform.
func New(config control.Config) *Controller {
	config.MetricLabelKeys = append(config.MetricLabelKeys, orgLabel)
	c := control.New(config)
	return &Controller{c: c}
}

// Query satisfies the AsyncQueryService while ensuring the request is propagated on the context.
func (c *Controller) Query(ctx context.Context, req *query.Request) (flux.Query, error) {
	// Set the request on the context so platform specific Flux operations can retrieve it later.
	ctx = query.ContextWithRequest(ctx, req)
	// Set the org label value for controller metrics
	ctx = context.WithValue(ctx, orgLabel, req.OrganizationID.String())
	q, err := c.c.Query(ctx, req.Compiler)
	if err != nil {
		// If the controller reports an error, it's usually because of a syntax error
		// or other problem that the client must fix.
		return q, &platform.Error{
			Code: platform.EInvalid,
			Msg:  err.Error(),
		}
	}

	return q, nil
}

// PrometheusCollectors satisifies the prom.PrometheusCollector interface.
func (c *Controller) PrometheusCollectors() []prometheus.Collector {
	return c.c.PrometheusCollectors()
}

// Shutdown shuts down the underlying Controller.
func (c *Controller) Shutdown(ctx context.Context) error {
	return c.c.Shutdown(ctx)
}
