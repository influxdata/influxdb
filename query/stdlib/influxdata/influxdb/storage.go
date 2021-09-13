package influxdb

import (
	"context"

	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/prom"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

type HostLookup interface {
	Hosts() []string
	Watch() <-chan struct{}
}

type BucketLookup interface {
	Lookup(ctx context.Context, orgID platform2.ID, name string) (platform2.ID, bool)
}

type OrganizationLookup interface {
	Lookup(ctx context.Context, name string) (platform2.ID, bool)
}

type FromDependencies struct {
	Reader             query.StorageReader
	BucketLookup       BucketLookup
	OrganizationLookup OrganizationLookup
	Metrics            *metrics
}

func (d FromDependencies) Validate() error {
	if d.Reader == nil {
		return errors.New("missing reader dependency")
	}
	if d.BucketLookup == nil {
		return errors.New("missing bucket lookup dependency")
	}
	if d.OrganizationLookup == nil {
		return errors.New("missing organization lookup dependency")
	}
	return nil
}

// PrometheusCollectors satisfies the PrometheusCollector interface.
func (d FromDependencies) PrometheusCollectors() []prometheus.Collector {
	collectors := make([]prometheus.Collector, 0)
	if pc, ok := d.Reader.(prom.PrometheusCollector); ok {
		collectors = append(collectors, pc.PrometheusCollectors()...)
	}
	if d.Metrics != nil {
		collectors = append(collectors, d.Metrics.PrometheusCollectors()...)
	}
	return collectors
}

type StaticLookup struct {
	hosts []string
}

func NewStaticLookup(hosts []string) StaticLookup {
	return StaticLookup{
		hosts: hosts,
	}
}

func (l StaticLookup) Hosts() []string {
	return l.hosts
}
func (l StaticLookup) Watch() <-chan struct{} {
	// A nil channel always blocks, since hosts never change this is appropriate.
	return nil
}
