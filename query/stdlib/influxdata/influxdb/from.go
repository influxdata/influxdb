package influxdb

import (
	"fmt"

	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/stdlib/influxdata/influxdb"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/query"
	"github.com/pkg/errors"
)

func init() {
	execute.RegisterSource(influxdb.FromKind, createFromSource)
}

// TODO(adam): implement a BucketsAccessed that doesn't depend on flux.
// https://github.com/influxdata/flux/issues/114

func createFromSource(prSpec plan.ProcedureSpec, dsid execute.DatasetID, a execute.Administration) (execute.Source, error) {
	spec := prSpec.(*influxdb.FromProcedureSpec)
	var w execute.Window
	bounds := a.StreamContext().Bounds()
	if bounds == nil {
		return nil, errors.New("nil bounds passed to from")
	}

	if spec.WindowSet {
		w = execute.Window{
			Every:  execute.Duration(spec.Window.Every),
			Period: execute.Duration(spec.Window.Period),
			Round:  execute.Duration(spec.Window.Round),
			Start:  bounds.Start,
		}
	} else {
		duration := execute.Duration(bounds.Stop) - execute.Duration(bounds.Start)
		w = execute.Window{
			Every:  duration,
			Period: duration,
			Start:  bounds.Start,
		}
	}
	currentTime := w.Start + execute.Time(w.Period)

	deps := a.Dependencies()[influxdb.FromKind].(Dependencies)
	req := query.RequestFromContext(a.Context())
	if req == nil {
		return nil, errors.New("missing request on context")
	}
	orgID := req.OrganizationID

	var bucketID platform.ID
	// Determine bucketID
	switch {
	case spec.Bucket != "":
		b, ok := deps.BucketLookup.Lookup(orgID, spec.Bucket)
		if !ok {
			return nil, fmt.Errorf("could not find bucket %q", spec.Bucket)
		}
		bucketID = b
	case len(spec.BucketID) != 0:
		err := bucketID.DecodeFromString(spec.BucketID)
		if err != nil {
			return nil, err
		}
	}

	return NewSource(
		dsid,
		deps.Reader,
		ReadSpec{
			OrganizationID:  orgID,
			BucketID:        bucketID,
			Predicate:       spec.Filter,
			PointsLimit:     spec.PointsLimit,
			SeriesLimit:     spec.SeriesLimit,
			SeriesOffset:    spec.SeriesOffset,
			Descending:      spec.Descending,
			OrderByTime:     spec.OrderByTime,
			GroupMode:       ToGroupMode(spec.GroupMode),
			GroupKeys:       spec.GroupKeys,
			AggregateMethod: spec.AggregateMethod,
		},
		*bounds,
		w,
		currentTime,
	), nil
}

func InjectFromDependencies(depsMap execute.Dependencies, deps Dependencies) error {
	if err := deps.Validate(); err != nil {
		return err
	}
	depsMap[influxdb.FromKind] = deps
	return nil
}
