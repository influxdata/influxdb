package influxdb

import (
	"context"
	"fmt"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/semantic"
	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/prom"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

type HostLookup interface {
	Hosts() []string
	Watch() <-chan struct{}
}

type BucketLookup interface {
	Lookup(ctx context.Context, orgID platform.ID, name string) (platform.ID, bool)
	LookupName(ctx context.Context, orgID platform.ID, id platform.ID) string
}

type OrganizationLookup interface {
	Lookup(ctx context.Context, name string) (platform.ID, bool)
	LookupName(ctx context.Context, id platform.ID) string
}

type FromDependencies struct {
	Reader             Reader
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

type GroupMode int

const (
	// GroupModeNone merges all series into a single group.
	GroupModeNone GroupMode = iota
	// GroupModeBy produces a table for each unique value of the specified GroupKeys.
	GroupModeBy
)

// ToGroupMode accepts the group mode from Flux and produces the appropriate storage group mode.
func ToGroupMode(fluxMode flux.GroupMode) GroupMode {
	switch fluxMode {
	case flux.GroupModeNone:
		return GroupModeNone
	case flux.GroupModeBy:
		return GroupModeBy
	default:
		panic(fmt.Sprint("unknown group mode: ", fluxMode))
	}
}

type ReadFilterSpec struct {
	OrganizationID platform.ID
	BucketID       platform.ID

	Bounds execute.Bounds

	Predicate *semantic.FunctionExpression
}

type ReadGroupSpec struct {
	ReadFilterSpec

	GroupMode GroupMode
	GroupKeys []string

	AggregateMethod string
}

type ReadTagKeysSpec struct {
	ReadFilterSpec
}

type ReadTagValuesSpec struct {
	ReadFilterSpec
	TagKey string
}

type Reader interface {
	ReadFilter(ctx context.Context, spec ReadFilterSpec, alloc *memory.Allocator) (TableIterator, error)
	ReadGroup(ctx context.Context, spec ReadGroupSpec, alloc *memory.Allocator) (TableIterator, error)

	ReadTagKeys(ctx context.Context, spec ReadTagKeysSpec, alloc *memory.Allocator) (TableIterator, error)
	ReadTagValues(ctx context.Context, spec ReadTagValuesSpec, alloc *memory.Allocator) (TableIterator, error)

	Close()
}

// TableIterator is a table iterator that also keeps track of cursor statistics from the storage engine.
type TableIterator interface {
	flux.TableIterator
	Statistics() cursors.CursorStats
}
