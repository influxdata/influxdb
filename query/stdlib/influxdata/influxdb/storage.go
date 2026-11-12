package influxdb

import (
	"context"
	"fmt"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/codes"
	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/prom"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/storage"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

type HostLookup interface {
	Hosts() []string
	Watch() <-chan struct{}
}

// BucketLookup resolves a bucket referenced by name or ID within an org.
type BucketLookup interface {
	// Lookup returns the ID of the bucket with the given name in orgID.
	Lookup(ctx context.Context, orgID platform2.ID, name string) (platform2.ID, bool)
	// LookupByID returns the ID of the bucket with the given ID, provided that
	// it exists in orgID.
	LookupByID(ctx context.Context, orgID platform2.ID, id platform2.ID) (platform2.ID, bool)
}

// lookupBucketID resolves a bucket referenced either by name or by its
// string-encoded ID. Both forms are resolved through the BucketLookup.
func lookupBucketID(ctx context.Context, orgID platform2.ID, buckets BucketLookup, name, id string) (platform2.ID, error) {
	switch {
	case name != "":
		b, ok := buckets.Lookup(ctx, orgID, name)
		if !ok {
			return 0, &flux.Error{
				Code: codes.NotFound,
				Msg:  fmt.Sprintf("could not find bucket %q", name),
			}
		}
		return b, nil
	case len(id) != 0:
		var b platform2.ID
		if err := b.DecodeFromString(id); err != nil {
			return 0, &flux.Error{
				Code: codes.Invalid,
				Msg:  "invalid bucket id",
				Err:  err,
			}
		}
		found, ok := buckets.LookupByID(ctx, orgID, b)
		if !ok {
			return 0, &flux.Error{
				Code: codes.NotFound,
				Msg:  fmt.Sprintf("could not find bucket %q", id),
			}
		}
		return found, nil
	default:
		return 0, &flux.Error{
			Code: codes.Invalid,
			Msg:  "no bucket name or id have been specified",
		}
	}
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

// ToDependencies contains the dependencies for executing the `to` function.
type ToDependencies struct {
	BucketLookup       BucketLookup
	OrganizationLookup OrganizationLookup
	PointsWriter       storage.PointsWriter
}

// Validate returns an error if any required field is unset.
func (d ToDependencies) Validate() error {
	if d.BucketLookup == nil {
		return errors.New("missing bucket lookup dependency")
	}
	if d.OrganizationLookup == nil {
		return errors.New("missing organization lookup dependency")
	}
	if d.PointsWriter == nil {
		return errors.New("missing points writer dependency")
	}
	return nil
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
