package readservice

import (
	"context"

	"github.com/influxdata/platform/query/functions/outputs"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/control"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/functions/inputs"
	fstorage "github.com/influxdata/platform/query/functions/inputs/storage"
	"github.com/influxdata/platform/storage"
	"github.com/influxdata/platform/storage/reads"
	"go.uber.org/zap"
)

func NewProxyQueryService(engine *storage.Engine, bucketSvc platform.BucketService, orgSvc platform.OrganizationService, logger *zap.Logger) (query.ProxyQueryService, error) {
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

	bucketLookupSvc := query.FromBucketService(bucketSvc)
	orgLookupSvc := query.FromOrganizationService(orgSvc)
	err := inputs.InjectFromDependencies(cc.ExecutorDependencies, fstorage.Dependencies{
		Reader:             reads.NewReader(newStore(engine)),
		BucketLookup:       bucketLookupSvc,
		OrganizationLookup: orgLookupSvc,
	})
	if err != nil {
		return nil, err
	}

	if err := inputs.InjectBucketDependencies(cc.ExecutorDependencies, bucketLookupSvc); err != nil {
		return nil, err
	}

	if err := outputs.InjectToDependencies(cc.ExecutorDependencies, outputs.ToDependencies{
		BucketLookup:       bucketLookupSvc,
		OrganizationLookup: orgLookupSvc,
		PointsWriter:       engine,
	}); err != nil {
		return nil, err
	}

	return query.ProxyQueryServiceBridge{
		QueryService: query.QueryServiceBridge{
			AsyncQueryService: &queryAdapter{
				Controller: control.New(cc),
			},
		},
	}, nil
}

type queryAdapter struct {
	Controller *control.Controller
}

func (q *queryAdapter) Query(ctx context.Context, req *query.Request) (flux.Query, error) {
	ctx = query.ContextWithRequest(ctx, req)
	ctx = context.WithValue(ctx, "org", req.OrganizationID.String())
	return q.Controller.Query(ctx, req.Compiler)
}
