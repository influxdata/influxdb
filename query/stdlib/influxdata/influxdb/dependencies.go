package influxdb

import (
	"context"

	"github.com/influxdata/flux"
	influxdeps "github.com/influxdata/flux/dependencies/influxdb"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/prom"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/storage"
	"github.com/prometheus/client_golang/prometheus"
)

type key int

const dependenciesKey key = iota

type StorageDependencies struct {
	FromDeps   FromDependencies
	BucketDeps BucketDependencies
	ToDeps     ToDependencies
}

func (d StorageDependencies) Inject(ctx context.Context) context.Context {
	ctx = influxdeps.Dependency{
		Provider: Provider{
			Reader:       d.FromDeps.Reader,
			BucketLookup: d.FromDeps.BucketLookup,
		},
	}.Inject(ctx)
	return context.WithValue(ctx, dependenciesKey, d)
}

func GetStorageDependencies(ctx context.Context) StorageDependencies {
	if ctx.Value(dependenciesKey) == nil {
		return StorageDependencies{}
	}
	return ctx.Value(dependenciesKey).(StorageDependencies)
}

// PrometheusCollectors satisfies the prom.PrometheusCollector interface.
func (d StorageDependencies) PrometheusCollectors() []prometheus.Collector {
	depS := []interface{}{
		d.FromDeps,
		d.BucketDeps,
		d.ToDeps,
	}
	collectors := make([]prometheus.Collector, 0, len(depS))
	for _, v := range depS {
		if pc, ok := v.(prom.PrometheusCollector); ok {
			collectors = append(collectors, pc.PrometheusCollectors()...)
		}
	}
	return collectors
}

type Dependencies struct {
	StorageDeps StorageDependencies
	FluxDeps    flux.Dependencies
}

func (d Dependencies) Inject(ctx context.Context) context.Context {
	ctx = d.FluxDeps.Inject(ctx)
	return d.StorageDeps.Inject(ctx)
}

// PrometheusCollectors satisfies the prom.PrometheusCollector interface.
func (d Dependencies) PrometheusCollectors() []prometheus.Collector {
	collectors := d.StorageDeps.PrometheusCollectors()
	if pc, ok := d.FluxDeps.(prom.PrometheusCollector); ok {
		collectors = append(collectors, pc.PrometheusCollectors()...)
	}
	return collectors
}

func NewDependencies(
	reader query.StorageReader,
	writer storage.PointsWriter,
	bucketSvc influxdb.BucketService,
	orgSvc influxdb.OrganizationService,
	ss influxdb.SecretService,
	metricLabelKeys []string,
) (Dependencies, error) {
	fdeps := flux.NewDefaultDependencies()
	fdeps.Deps.SecretService = query.FromSecretService(ss)
	deps := Dependencies{FluxDeps: fdeps}
	bucketLookupSvc := query.FromBucketService(bucketSvc)
	orgLookupSvc := query.FromOrganizationService(orgSvc)
	metrics := NewMetrics(metricLabelKeys)
	deps.StorageDeps.FromDeps = FromDependencies{
		Reader:             reader,
		BucketLookup:       bucketLookupSvc,
		OrganizationLookup: orgLookupSvc,
		Metrics:            metrics,
	}
	if err := deps.StorageDeps.FromDeps.Validate(); err != nil {
		return Dependencies{}, err
	}
	deps.StorageDeps.BucketDeps = bucketLookupSvc
	deps.StorageDeps.ToDeps = ToDependencies{
		BucketLookup:       bucketLookupSvc,
		OrganizationLookup: orgLookupSvc,
		PointsWriter:       writer,
	}
	if err := deps.StorageDeps.ToDeps.Validate(); err != nil {
		return Dependencies{}, err
	}
	return deps, nil
}
