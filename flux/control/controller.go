package control

import (
	"github.com/influxdata/flux/control"
	"github.com/influxdata/flux/execute"
	_ "github.com/influxdata/influxdb/flux/builtin"
	"github.com/influxdata/influxdb/flux/functions/inputs"
	fstorage "github.com/influxdata/platform/query/functions/inputs/storage"
	"go.uber.org/zap"
)

type MetaClient = inputs.MetaClient

func NewController(mc MetaClient, reader fstorage.Reader, logger *zap.Logger) *control.Controller {
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

	err := inputs.InjectFromDependencies(cc.ExecutorDependencies, inputs.Dependencies{Reader: reader, MetaClient: mc})
	if err != nil {
		panic(err)
	}

	err = inputs.InjectBucketDependencies(cc.ExecutorDependencies, mc)
	if err != nil {
		panic(err)
	}

	return control.New(cc)
}
