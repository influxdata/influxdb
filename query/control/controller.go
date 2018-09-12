package control

import (
	"context"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/control"
	"github.com/influxdata/platform/query"
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

// Query satisifies the AsyncQueryService while ensuring the request is propogated on the context.
func (c *Controller) Query(ctx context.Context, req *query.Request) (flux.Query, error) {
	// Set the request on the context so platform specific Flux operations can retrieve it later.
	ctx = query.ContextWithRequest(ctx, req)
	// Set the org label value for controller metrics
	ctx = context.WithValue(ctx, orgLabel, req.OrganizationID.String())
	return c.c.Query(ctx, req.Compiler)
}

// PrometheusCollectors satisifies the prom.PrometheusCollector interface.
func (c *Controller) PrometheusCollectors() []prometheus.Collector {
	return c.c.PrometheusCollectors()
}
