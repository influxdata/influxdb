package control

import (
	"context"

	_ "github.com/influxdata/flux/builtin"
	"github.com/influxdata/flux/control"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/functions"
	fstorage "github.com/influxdata/flux/functions/storage"
	"github.com/influxdata/influxdb/flux/functions/store"
	"github.com/influxdata/influxdb/services/storage"
	"github.com/influxdata/platform"
	"go.uber.org/zap"
)

func NewController(s storage.Store, logger *zap.Logger) *control.Controller {
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

	err := functions.InjectFromDependencies(cc.ExecutorDependencies, fstorage.Dependencies{
		Reader:             store.NewReader(s),
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
