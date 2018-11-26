package readservice

import (
	"github.com/influxdata/flux/control"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/query"
	pcontrol "github.com/influxdata/platform/query/control"
	"github.com/influxdata/platform/query/functions/inputs"
	fstorage "github.com/influxdata/platform/query/functions/inputs/storage"
	"github.com/influxdata/platform/query/functions/outputs"
	"github.com/influxdata/platform/storage"
	"github.com/influxdata/platform/storage/reads"
)

// NewProxyQueryService returns a proxy query service based on the given queryController
// suitable for the storage read service.
func NewProxyQueryService(queryController *pcontrol.Controller) query.ProxyQueryService {
	return query.ProxyQueryServiceBridge{
		QueryService: query.QueryServiceBridge{
			AsyncQueryService: queryController,
		},
	}
}

// AddControllerConfigDependencies sets up the dependencies on cc
// such that "from" and "to" flux functions will work correctly.
func AddControllerConfigDependencies(
	cc *control.Config,
	engine *storage.Engine,
	bucketSvc platform.BucketService,
	orgSvc platform.OrganizationService,
) error {
	bucketLookupSvc := query.FromBucketService(bucketSvc)
	orgLookupSvc := query.FromOrganizationService(orgSvc)
	err := inputs.InjectFromDependencies(cc.ExecutorDependencies, fstorage.Dependencies{
		Reader:             reads.NewReader(newStore(engine)),
		BucketLookup:       bucketLookupSvc,
		OrganizationLookup: orgLookupSvc,
	})
	if err != nil {
		return err
	}

	if err := inputs.InjectBucketDependencies(cc.ExecutorDependencies, bucketLookupSvc); err != nil {
		return err
	}

	return outputs.InjectToDependencies(cc.ExecutorDependencies, outputs.ToDependencies{
		BucketLookup:       bucketLookupSvc,
		OrganizationLookup: orgLookupSvc,
		PointsWriter:       engine,
	})
}
