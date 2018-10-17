package control

import (
	"github.com/influxdata/flux/control"
	"github.com/influxdata/flux/execute"
	_ "github.com/influxdata/influxdb/flux/builtin"
	"github.com/influxdata/influxdb/flux/functions/inputs"
	"github.com/influxdata/influxdb/services/storage"
	"github.com/influxdata/platform/storage/reads"
	"go.uber.org/zap"
)

func NewController(s *storage.Store, logger *zap.Logger) *control.Controller {
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
		Verbose:              false,
	}

	err := inputs.InjectFromDependencies(cc.ExecutorDependencies, inputs.Dependencies{Reader: reads.NewReader(s)})
	if err != nil {
		panic(err)
	}

	err = inputs.InjectBucketDependencies(cc.ExecutorDependencies, s)
	if err != nil {
		panic(err)
	}

	return control.New(cc)
}
