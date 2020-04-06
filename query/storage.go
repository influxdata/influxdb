package query

import (
	"context"
	"fmt"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
)

// StorageReader is an interface for reading tables from the storage subsystem.
type StorageReader interface {
	ReadFilter(ctx context.Context, spec ReadFilterSpec, alloc *memory.Allocator) (TableIterator, error)
	ReadGroup(ctx context.Context, spec ReadGroupSpec, alloc *memory.Allocator) (TableIterator, error)

	ReadTagKeys(ctx context.Context, spec ReadTagKeysSpec, alloc *memory.Allocator) (TableIterator, error)
	ReadTagValues(ctx context.Context, spec ReadTagValuesSpec, alloc *memory.Allocator) (TableIterator, error)

	Close()
}

type ReadFilterSpec struct {
	OrganizationID influxdb.ID
	BucketID       influxdb.ID

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
