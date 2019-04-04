package control

import (
	"github.com/influxdata/flux/control"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/influxdb/coordinator"
	_ "github.com/influxdata/influxdb/flux/builtin"
	"github.com/influxdata/influxdb/flux/stdlib/influxdata/influxdb"
	v1 "github.com/influxdata/influxdb/flux/stdlib/influxdata/influxdb/v1"
	"go.uber.org/zap"
)

type MetaClient = coordinator.MetaClient
type Authorizer = influxdb.Authorizer

func NewController(mc MetaClient, reader influxdb.Reader, auth Authorizer, authEnabled bool, logger *zap.Logger) *control.Controller {
	// flux
	var (
		concurrencyQuota = 10
		memoryBytesQuota = 1e6
	)

	cc := control.Config{
		ExecutorDependencies: make(execute.Dependencies),
		ConcurrencyQuota:     concurrencyQuota,
		MemoryBytesQuota:     int64(memoryBytesQuota),
		Logger:               logger,
	}

	if err := influxdb.InjectFromDependencies(cc.ExecutorDependencies, influxdb.Dependencies{
		Reader:      reader,
		MetaClient:  mc,
		Authorizer:  auth,
		AuthEnabled: authEnabled,
	}); err != nil {
		panic(err)
	}

	if err := v1.InjectDatabaseDependencies(cc.ExecutorDependencies, v1.DatabaseDependencies{
		MetaClient:  mc,
		Authorizer:  auth,
		AuthEnabled: authEnabled,
	}); err != nil {
		panic(err)
	}

	if err := influxdb.InjectBucketDependencies(cc.ExecutorDependencies, influxdb.BucketDependencies{
		MetaClient:  mc,
		Authorizer:  auth,
		AuthEnabled: authEnabled,
	}); err != nil {
		panic(err)
	}

	return control.New(cc)
}
