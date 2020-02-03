package control

import (
	"context"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/influxdb/coordinator"
	"github.com/influxdata/influxdb/flux/builtin"
	"github.com/influxdata/influxdb/flux/stdlib/influxdata/influxdb"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type MetaClient = coordinator.MetaClient
type Authorizer = influxdb.Authorizer

func NewController(mc MetaClient, reader influxdb.Reader, auth Authorizer, authEnabled bool, logger *zap.Logger) *Controller {
	builtin.Initialize()

	storageDeps, err := influxdb.NewDependencies(mc, reader, auth, authEnabled)
	if err != nil {
		panic(err)
	}

	return &Controller{
		deps:   []flux.Dependency{storageDeps},
		logger: logger,
	}
}

type Controller struct {
	deps   []flux.Dependency
	logger *zap.Logger
}

func (c *Controller) Query(ctx context.Context, compiler flux.Compiler) (flux.Query, error) {
	for _, dep := range c.deps {
		ctx = dep.Inject(ctx)
	}

	p, err := compiler.Compile(ctx)
	if err != nil {
		return nil, err
	}

	if p, ok := p.(lang.LoggingProgram); ok {
		p.SetLogger(c.logger)
	}

	alloc := &memory.Allocator{}
	return p.Start(ctx, alloc)
}
func (c *Controller) PrometheusCollectors() []prometheus.Collector {
	return nil
}
