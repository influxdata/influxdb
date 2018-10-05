package control

import (
	"context"

	_ "github.com/influxdata/flux/builtin"
	"github.com/influxdata/flux/control"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/query/functions/inputs"
	fstorage "github.com/influxdata/platform/query/functions/inputs/storage"
	"github.com/influxdata/platform/storage/reads"
	"go.uber.org/zap"
)

func NewController(s reads.Store, logger *zap.Logger) *control.Controller {
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

	err := inputs.InjectFromDependencies(cc.ExecutorDependencies, fstorage.Dependencies{
		Reader:             reads.NewReader(s),
		BucketLookup:       bucketLookup{},
		OrganizationLookup: orgLookup{},
	})
	if err != nil {
		panic(err)
	}
	return control.New(cc)
}

type orgLookup struct{}

func (l orgLookup) Lookup(ctx context.Context, name string) (platform.ID, bool) {
	return platform.ID(name), true
}

type bucketLookup struct{}

func (l bucketLookup) Lookup(orgID platform.ID, name string) (platform.ID, bool) {
	return platform.ID(name), true
}
