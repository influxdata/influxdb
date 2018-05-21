package functions

import (
	"fmt"

	"github.com/influxdata/ifql/functions/storage"
	"github.com/influxdata/ifql/id"
	"github.com/influxdata/ifql/interpreter"
	"github.com/influxdata/ifql/query"
	"github.com/influxdata/ifql/query/execute"
	"github.com/influxdata/ifql/query/plan"
	"github.com/influxdata/ifql/semantic"
	"github.com/pkg/errors"
)

const FromKind = "from"

type FromOpSpec struct {
	Database string   `json:"db"`
	Bucket   string   `json:"bucket"`
	Hosts    []string `json:"hosts"`
}

var fromSignature = semantic.FunctionSignature{
	Params: map[string]semantic.Type{
		"db": semantic.String,
	},
	ReturnType: query.TableObjectType,
}

func init() {
	query.RegisterFunction(FromKind, createFromOpSpec, fromSignature)
	query.RegisterOpSpec(FromKind, newFromOp)
	plan.RegisterProcedureSpec(FromKind, newFromProcedure, FromKind)
	execute.RegisterSource(FromKind, createFromSource)
}

func createFromOpSpec(args query.Arguments, a *query.Administration) (query.OperationSpec, error) {
	spec := new(FromOpSpec)

	if db, ok, err := args.GetString("db"); err != nil {
		return nil, err
	} else if ok {
		spec.Database = db
	}

	if bucket, ok, err := args.GetString("bucket"); err != nil {
		return nil, err
	} else if ok {
		spec.Bucket = bucket
	}

	if spec.Database == "" && spec.Bucket == "" {
		return nil, errors.New("must specify one of db or bucket")
	}
	if spec.Database != "" && spec.Bucket != "" {
		return nil, errors.New("must specify only one of db or bucket")
	}

	if array, ok, err := args.GetArray("hosts", semantic.String); err != nil {
		return nil, err
	} else if ok {
		spec.Hosts, err = interpreter.ToStringArray(array)
		if err != nil {
			return nil, err
		}
	}

	return spec, nil
}

func newFromOp() query.OperationSpec {
	return new(FromOpSpec)
}

func (s *FromOpSpec) Kind() query.OperationKind {
	return FromKind
}

type FromProcedureSpec struct {
	Database string
	Bucket   string
	Hosts    []string

	BoundsSet bool
	Bounds    plan.BoundsSpec

	FilterSet bool
	Filter    *semantic.FunctionExpression

	DescendingSet bool
	Descending    bool

	LimitSet     bool
	PointsLimit  int64
	SeriesLimit  int64
	SeriesOffset int64

	WindowSet bool
	Window    plan.WindowSpec

	GroupingSet bool
	OrderByTime bool
	MergeAll    bool
	GroupKeys   []string
	GroupExcept []string

	AggregateSet    bool
	AggregateMethod string
}

func newFromProcedure(qs query.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*FromOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}

	return &FromProcedureSpec{
		Database: spec.Database,
		Bucket:   spec.Bucket,
		Hosts:    spec.Hosts,
	}, nil
}

func (s *FromProcedureSpec) Kind() plan.ProcedureKind {
	return FromKind
}
func (s *FromProcedureSpec) TimeBounds() plan.BoundsSpec {
	return s.Bounds
}
func (s *FromProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(FromProcedureSpec)

	ns.Database = s.Database
	ns.Bucket = s.Bucket

	if len(s.Hosts) > 0 {
		ns.Hosts = make([]string, len(s.Hosts))
		copy(ns.Hosts, s.Hosts)
	}

	ns.BoundsSet = s.BoundsSet
	ns.Bounds = s.Bounds

	ns.FilterSet = s.FilterSet
	// TODO copy predicate
	ns.Filter = s.Filter

	ns.DescendingSet = s.DescendingSet
	ns.Descending = s.Descending

	ns.LimitSet = s.LimitSet
	ns.PointsLimit = s.PointsLimit
	ns.SeriesLimit = s.SeriesLimit
	ns.SeriesOffset = s.SeriesOffset

	ns.WindowSet = s.WindowSet
	ns.Window = s.Window

	ns.AggregateSet = s.AggregateSet
	ns.AggregateMethod = s.AggregateMethod

	return ns
}

func createFromSource(prSpec plan.ProcedureSpec, dsid execute.DatasetID, a execute.Administration) (execute.Source, error) {
	spec := prSpec.(*FromProcedureSpec)
	var w execute.Window
	if spec.WindowSet {
		w = execute.Window{
			Every:  execute.Duration(spec.Window.Every),
			Period: execute.Duration(spec.Window.Period),
			Round:  execute.Duration(spec.Window.Round),
			Start:  a.ResolveTime(spec.Window.Start),
		}
	} else {
		duration := execute.Duration(a.ResolveTime(spec.Bounds.Stop)) - execute.Duration(a.ResolveTime(spec.Bounds.Start))
		w = execute.Window{
			Every:  duration,
			Period: duration,
			Start:  a.ResolveTime(spec.Bounds.Start),
		}
	}
	currentTime := w.Start + execute.Time(w.Period)
	bounds := execute.Bounds{
		Start: a.ResolveTime(spec.Bounds.Start),
		Stop:  a.ResolveTime(spec.Bounds.Stop),
	}

	deps := a.Dependencies()[FromKind].(storage.Dependencies)
	orgID := a.OrganizationID()

	var bucketID id.ID
	if spec.Database == "" {
		b, ok := deps.BucketLookup.Lookup(orgID, spec.Bucket)
		if !ok {
			return nil, fmt.Errorf("could not find bucket %q", spec.Bucket)
		}
		bucketID = b
	} else {
		bucketID = id.ID(spec.Database)
	}

	return storage.NewSource(
		dsid,
		deps.Reader,
		storage.ReadSpec{
			OrganizationID:  orgID,
			BucketID:        bucketID,
			Hosts:           spec.Hosts,
			Predicate:       spec.Filter,
			PointsLimit:     spec.PointsLimit,
			SeriesLimit:     spec.SeriesLimit,
			SeriesOffset:    spec.SeriesOffset,
			Descending:      spec.Descending,
			OrderByTime:     spec.OrderByTime,
			MergeAll:        spec.MergeAll,
			GroupKeys:       spec.GroupKeys,
			GroupExcept:     spec.GroupExcept,
			AggregateMethod: spec.AggregateMethod,
		},
		bounds,
		w,
		currentTime,
	), nil
}

func InjectFromDependencies(depsMap execute.Dependencies, deps storage.Dependencies) error {
	if err := deps.Validate(); err != nil {
		return err
	}
	depsMap[FromKind] = deps
	return nil
}
