package influxdb

import (
	"context"
	"fmt"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb/cursors"
	"github.com/influxdata/influxql"
	"github.com/pkg/errors"
)

type Authorizer interface {
	AuthorizeDatabase(u meta.User, priv influxql.Privilege, database string) error
}

type FromDependencies struct {
	Reader      Reader
	MetaClient  MetaClient
	Authorizer  Authorizer
	AuthEnabled bool
}

func (d FromDependencies) Validate() error {
	if d.Reader == nil {
		return errors.New("missing reader dependency")
	}
	if d.MetaClient == nil {
		return errors.New("missing meta client dependency")
	}
	if d.AuthEnabled && d.Authorizer == nil {
		return errors.New("missing authorizer dependency")
	}
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
	Database        string
	RetentionPolicy string

	Bounds execute.Bounds

	Predicate *datatypes.Predicate
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

// Window and the WindowEvery/Offset should be mutually exclusive. If you set either the WindowEvery or Offset with
// nanosecond values, then the Window will be ignored
type ReadWindowAggregateSpec struct {
	ReadFilterSpec
	WindowEvery int64
	Offset      int64
	Aggregates  []plan.ProcedureKind
	CreateEmpty bool
	TimeColumn  string
	Window      execute.Window
}

func (spec *ReadWindowAggregateSpec) Name() string {
	var agg string
	if len(spec.Aggregates) > 0 {
		agg = string(spec.Aggregates[0])
	}
	return fmt.Sprintf("readWindow(%s)", agg)
}

type Reader interface {
	ReadFilter(ctx context.Context, spec ReadFilterSpec, alloc memory.Allocator) (TableIterator, error)
	ReadGroup(ctx context.Context, spec ReadGroupSpec, alloc memory.Allocator) (TableIterator, error)
	ReadWindowAggregate(ctx context.Context, spec ReadWindowAggregateSpec, alloc memory.Allocator) (TableIterator, error)

	ReadTagKeys(ctx context.Context, spec ReadTagKeysSpec, alloc memory.Allocator) (TableIterator, error)
	ReadTagValues(ctx context.Context, spec ReadTagValuesSpec, alloc memory.Allocator) (TableIterator, error)

	Close()
}

// TableIterator is a table iterator that also keeps track of cursor statistics from the storage engine.
type TableIterator interface {
	flux.TableIterator
	Statistics() cursors.CursorStats
}
