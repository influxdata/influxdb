package control

import (
	"context"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/influxdb/coordinator"
	_ "github.com/influxdata/influxdb/flux/builtin"
	"github.com/influxdata/influxdb/flux/stdlib/influxdata/influxdb"
	v1 "github.com/influxdata/influxdb/flux/stdlib/influxdata/influxdb/v1"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type MetaClient = coordinator.MetaClient
type Authorizer = influxdb.Authorizer

func NewController(mc MetaClient, reader influxdb.Reader, auth Authorizer, authEnabled bool, logger *zap.Logger) *Controller {

	executorDependencies := make(execute.Dependencies)

	if err := influxdb.InjectFromDependencies(executorDependencies, influxdb.Dependencies{
		Reader:      reader,
		MetaClient:  mc,
		Authorizer:  auth,
		AuthEnabled: authEnabled,
	}); err != nil {
		panic(err)
	}

	if err := v1.InjectDatabaseDependencies(executorDependencies, v1.DatabaseDependencies{
		MetaClient:  mc,
		Authorizer:  auth,
		AuthEnabled: authEnabled,
	}); err != nil {
		panic(err)
	}

	if err := influxdb.InjectBucketDependencies(executorDependencies, influxdb.BucketDependencies{
		MetaClient:  mc,
		Authorizer:  auth,
		AuthEnabled: authEnabled,
	}); err != nil {
		panic(err)
	}

	return &Controller{
		deps: executorDependencies,
	}
}

type Controller struct {
	deps execute.Dependencies
}

func (c *Controller) Query(ctx context.Context, compiler flux.Compiler) (flux.Query, error) {
	return nil, nil
}
func (c *Controller) PrometheusCollectors() []prometheus.Collector {
	return nil
}
