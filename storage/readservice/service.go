package readservice

import (
	"github.com/influxdata/flux/control"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/query"
	pcontrol "github.com/influxdata/influxdb/query/control"
	"github.com/influxdata/influxdb/query/stdlib/influxdata/influxdb"
	"github.com/influxdata/influxdb/storage"
	"github.com/influxdata/influxdb/storage/reads"
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
	err := influxdb.InjectFromDependencies(cc.ExecutorDependencies, influxdb.Dependencies{
		Reader:             reads.NewReader(newStore(engine)),
		BucketLookup:       bucketLookupSvc,
		OrganizationLookup: orgLookupSvc,
	})
	if err != nil {
		return err
	}

	if err := influxdb.InjectBucketDependencies(cc.ExecutorDependencies, bucketLookupSvc); err != nil {
		return err
	}

	return influxdb.InjectToDependencies(cc.ExecutorDependencies, influxdb.ToDependencies{
		BucketLookup:       bucketLookupSvc,
		OrganizationLookup: orgLookupSvc,
		PointsWriter:       engine,
	})
}
