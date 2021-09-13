package query

import (
	"context"
	"fmt"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
)

// StorageReader is an interface for reading tables from the storage subsystem.
type StorageReader interface {
	ReadFilter(ctx context.Context, spec ReadFilterSpec, alloc *memory.Allocator) (TableIterator, error)
	ReadGroup(ctx context.Context, spec ReadGroupSpec, alloc *memory.Allocator) (TableIterator, error)
	ReadWindowAggregate(ctx context.Context, spec ReadWindowAggregateSpec, alloc *memory.Allocator) (TableIterator, error)

	ReadTagKeys(ctx context.Context, spec ReadTagKeysSpec, alloc *memory.Allocator) (TableIterator, error)
	ReadTagValues(ctx context.Context, spec ReadTagValuesSpec, alloc *memory.Allocator) (TableIterator, error)

	ReadSeriesCardinality(ctx context.Context, spec ReadSeriesCardinalitySpec, alloc *memory.Allocator) (TableIterator, error)
	SupportReadSeriesCardinality(ctx context.Context) bool

	Close()
}

type ReadFilterSpec struct {
	OrganizationID platform.ID
	BucketID       platform.ID

	Bounds    execute.Bounds
	Predicate *datatypes.Predicate
}

type ReadGroupSpec struct {
	ReadFilterSpec

	GroupMode GroupMode
	GroupKeys []string

	AggregateMethod string
}

func (spec *ReadGroupSpec) Name() string {
	return fmt.Sprintf("readGroup(%s)", spec.AggregateMethod)
}

type ReadTagKeysSpec struct {
	ReadFilterSpec
}

type ReadTagValuesSpec struct {
	ReadFilterSpec
	TagKey string
}

type ReadSeriesCardinalitySpec struct {
	ReadFilterSpec
}

// ReadWindowAggregateSpec defines the options for WindowAggregate.
//
// Window and the WindowEvery/Offset should be mutually exclusive. If you set either the WindowEvery or Offset with
// nanosecond values, then the Window will be ignored.
type ReadWindowAggregateSpec struct {
	ReadFilterSpec
	WindowEvery int64
	Offset      int64
	Aggregates  []plan.ProcedureKind
	CreateEmpty bool
	TimeColumn  string
	Window      execute.Window

	// ForceAggregate forces all aggregates to be treated as aggregates.
	// This forces selectors, which normally don't return values for empty
	// windows, to return a null value.
	ForceAggregate bool
}

func (spec *ReadWindowAggregateSpec) Name() string {
	var agg string
	if len(spec.Aggregates) > 0 {
		agg = string(spec.Aggregates[0])
	}
	return fmt.Sprintf("readWindow(%s)", agg)
}

// TableIterator is a table iterator that also keeps track of cursor statistics from the storage engine.
type TableIterator interface {
	flux.TableIterator
	Statistics() cursors.CursorStats
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
