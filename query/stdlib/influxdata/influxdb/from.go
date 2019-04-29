package influxdb

import (
	"context"
	"fmt"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/stdlib/influxdata/influxdb"
	"github.com/influxdata/flux/stdlib/universe"
	"github.com/influxdata/flux/values"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/tracing"
	"github.com/influxdata/influxdb/query"
	"github.com/pkg/errors"
)

const FromKind = "influxDBFrom"

type FromOpSpec struct {
	Bucket   string `json:"bucket,omitempty"`
	BucketID string `json:"bucketID,omitempty"`
}

func init() {
	fromSignature := semantic.FunctionPolySignature{
		Parameters: map[string]semantic.PolyType{
			"bucket":   semantic.String,
			"bucketID": semantic.String,
		},
		Required: nil,
		Return:   flux.TableObjectType,
	}

	flux.ReplacePackageValue("influxdata/influxdb", influxdb.FromKind, flux.FunctionValue(FromKind, createFromOpSpec, fromSignature))
	flux.RegisterOpSpec(FromKind, newFromOp)
	plan.RegisterProcedureSpec(FromKind, newFromProcedure, FromKind)
	execute.RegisterSource(PhysicalFromKind, createFromSource)
}

func createFromOpSpec(args flux.Arguments, a *flux.Administration) (flux.OperationSpec, error) {
	spec := new(FromOpSpec)

	if bucket, ok, err := args.GetString("bucket"); err != nil {
		return nil, err
	} else if ok {
		spec.Bucket = bucket
	}

	if bucketID, ok, err := args.GetString("bucketID"); err != nil {
		return nil, err
	} else if ok {
		spec.BucketID = bucketID
	}

	if spec.Bucket == "" && spec.BucketID == "" {
		return nil, errors.New("must specify one of bucket or bucketID")
	}
	if spec.Bucket != "" && spec.BucketID != "" {
		return nil, errors.New("must specify only one of bucket or bucketID")
	}
	return spec, nil
}

func newFromOp() flux.OperationSpec {
	return new(FromOpSpec)
}

func (s *FromOpSpec) Kind() flux.OperationKind {
	return FromKind
}

// BucketsAccessed makes FromOpSpec a query.BucketAwareOperationSpec
func (s *FromOpSpec) BucketsAccessed(orgID *platform.ID) (readBuckets, writeBuckets []platform.BucketFilter) {
	bf := platform.BucketFilter{}
	if s.Bucket != "" {
		bf.Name = &s.Bucket
	}
	if orgID != nil {
		bf.OrganizationID = orgID
	}

	if len(s.BucketID) > 0 {
		if id, err := platform.IDFromString(s.BucketID); err != nil {
			invalidID := platform.InvalidID()
			bf.ID = &invalidID
		} else {
			bf.ID = id
		}
	}

	if bf.ID != nil || bf.Name != nil {
		readBuckets = append(readBuckets, bf)
	}
	return readBuckets, writeBuckets
}

type FromProcedureSpec struct {
	Bucket   string
	BucketID string
}

func newFromProcedure(qs flux.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*FromOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}

	return &FromProcedureSpec{
		Bucket:   spec.Bucket,
		BucketID: spec.BucketID,
	}, nil
}

func (s *FromProcedureSpec) Kind() plan.ProcedureKind {
	return FromKind
}

func (s *FromProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(FromProcedureSpec)

	ns.Bucket = s.Bucket
	ns.BucketID = s.BucketID

	return ns
}

func (s FromProcedureSpec) PostPhysicalValidate(id plan.NodeID) error {
	// FromProcedureSpec has no bounds, so must be invalid.
	var bucket string
	if len(s.Bucket) > 0 {
		bucket = s.Bucket
	} else {
		bucket = s.BucketID
	}
	return fmt.Errorf(`%s: results from "%s" must be bounded`, id, bucket)
}

const PhysicalFromKind = "physFrom"

type PhysicalFromProcedureSpec struct {
	FromProcedureSpec

	plan.DefaultCost
	BoundsSet bool
	Bounds    flux.Bounds

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
	GroupMode   flux.GroupMode
	GroupKeys   []string

	AggregateSet    bool
	AggregateMethod string
}

func (PhysicalFromProcedureSpec) Kind() plan.ProcedureKind {
	return PhysicalFromKind
}

func (s *PhysicalFromProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(PhysicalFromProcedureSpec)

	ns.Bucket = s.Bucket
	ns.BucketID = s.BucketID

	ns.BoundsSet = s.BoundsSet
	ns.Bounds = s.Bounds

	ns.FilterSet = s.FilterSet
	ns.Filter = s.Filter.Copy().(*semantic.FunctionExpression)

	ns.DescendingSet = s.DescendingSet
	ns.Descending = s.Descending

	ns.LimitSet = s.LimitSet
	ns.PointsLimit = s.PointsLimit
	ns.SeriesLimit = s.SeriesLimit
	ns.SeriesOffset = s.SeriesOffset

	ns.WindowSet = s.WindowSet
	ns.Window = s.Window

	ns.GroupingSet = s.GroupingSet
	ns.OrderByTime = s.OrderByTime
	ns.GroupMode = s.GroupMode
	ns.GroupKeys = s.GroupKeys

	ns.AggregateSet = s.AggregateSet
	ns.AggregateMethod = s.AggregateMethod

	return ns
}

// TimeBounds implements plan.BoundsAwareProcedureSpec.
func (s *PhysicalFromProcedureSpec) TimeBounds(predecessorBounds *plan.Bounds) *plan.Bounds {
	if s.BoundsSet {
		bounds := &plan.Bounds{
			Start: values.ConvertTime(s.Bounds.Start.Time(s.Bounds.Now)),
			Stop:  values.ConvertTime(s.Bounds.Stop.Time(s.Bounds.Now)),
		}
		return bounds
	}
	return nil
}

func (s PhysicalFromProcedureSpec) PostPhysicalValidate(id plan.NodeID) error {
	if !s.BoundsSet || (s.Bounds.Start.IsZero() && s.Bounds.Stop.IsZero()) {
		var bucket string
		if len(s.Bucket) > 0 {
			bucket = s.Bucket
		} else {
			bucket = s.BucketID
		}
		return fmt.Errorf(`%s: results from "%s" must be bounded`, id, bucket)
	}

	return nil
}

// FromConversionRule converts a logical `from` node into a physical `from` node.
// TODO(cwolff): this rule can go away when we require a `range`
//  to be pushed into a logical `from` to create a physical `from.`
type FromConversionRule struct {
}

func (FromConversionRule) Name() string {
	return "FromConversionRule"
}

func (FromConversionRule) Pattern() plan.Pattern {
	return plan.Pat(FromKind)
}

func (FromConversionRule) Rewrite(pn plan.Node) (plan.Node, bool, error) {
	logicalFromSpec := pn.ProcedureSpec().(*FromProcedureSpec)
	newNode := plan.CreatePhysicalNode(pn.ID(), &PhysicalFromProcedureSpec{
		FromProcedureSpec: *logicalFromSpec,
	})

	plan.ReplaceNode(pn, newNode)
	return newNode, true, nil
}

// MergeFromRangeRule pushes a `range` into a `from`.
type MergeFromRangeRule struct{}

// Name returns the name of the rule.
func (rule MergeFromRangeRule) Name() string {
	return "MergeFromRangeRule"
}

// Pattern returns the pattern that matches `from -> range`.
func (rule MergeFromRangeRule) Pattern() plan.Pattern {
	return plan.Pat(universe.RangeKind, plan.Pat(PhysicalFromKind))
}

// Rewrite attempts to rewrite a `from -> range` into a `FromRange`.
func (rule MergeFromRangeRule) Rewrite(node plan.Node) (plan.Node, bool, error) {
	from := node.Predecessors()[0]
	fromSpec := from.ProcedureSpec().(*PhysicalFromProcedureSpec)
	rangeSpec := node.ProcedureSpec().(*universe.RangeProcedureSpec)
	fromRange := fromSpec.Copy().(*PhysicalFromProcedureSpec)

	// Set new bounds to `range` bounds initially
	fromRange.Bounds = rangeSpec.Bounds

	var (
		now   = rangeSpec.Bounds.Now
		start = rangeSpec.Bounds.Start
		stop  = rangeSpec.Bounds.Stop
	)

	bounds := &plan.Bounds{
		Start: values.ConvertTime(start.Time(now)),
		Stop:  values.ConvertTime(stop.Time(now)),
	}

	// Intersect bounds if `from` already bounded
	if fromSpec.BoundsSet {
		now = fromSpec.Bounds.Now
		start = fromSpec.Bounds.Start
		stop = fromSpec.Bounds.Stop

		fromBounds := &plan.Bounds{
			Start: values.ConvertTime(start.Time(now)),
			Stop:  values.ConvertTime(stop.Time(now)),
		}

		bounds = bounds.Intersect(fromBounds)
		fromRange.Bounds = flux.Bounds{
			Start: flux.Time{Absolute: bounds.Start.Time()},
			Stop:  flux.Time{Absolute: bounds.Stop.Time()},
		}
	}

	fromRange.BoundsSet = true

	// Finally merge nodes into single operation
	merged, err := plan.MergeToPhysicalNode(node, from, fromRange)
	if err != nil {
		return nil, false, err
	}

	return merged, true, nil
}

// MergeFromFilterRule is a rule that pushes filters into from procedures to be evaluated in the storage layer.
// This rule is likely to be replaced by a more generic rule when we have a better
// framework for pushing filters, etc into sources.
type MergeFromFilterRule struct{}

func (MergeFromFilterRule) Name() string {
	return "MergeFromFilterRule"
}

func (MergeFromFilterRule) Pattern() plan.Pattern {
	return plan.Pat(universe.FilterKind, plan.Pat(PhysicalFromKind))
}

func (MergeFromFilterRule) Rewrite(filterNode plan.Node) (plan.Node, bool, error) {
	filterSpec := filterNode.ProcedureSpec().(*universe.FilterProcedureSpec)
	fromNode := filterNode.Predecessors()[0]
	fromSpec := fromNode.ProcedureSpec().(*PhysicalFromProcedureSpec)

	if fromSpec.AggregateSet || fromSpec.GroupingSet {
		return filterNode, false, nil
	}

	bodyExpr, ok := filterSpec.Fn.Block.Body.(semantic.Expression)
	if !ok {
		return filterNode, false, nil
	}

	if len(filterSpec.Fn.Block.Parameters.List) != 1 {
		// I would expect that type checking would catch this, but just to be safe...
		return filterNode, false, nil
	}

	paramName := filterSpec.Fn.Block.Parameters.List[0].Key.Name

	pushable, notPushable, err := semantic.PartitionPredicates(bodyExpr, func(e semantic.Expression) (bool, error) {
		return isPushableExpr(paramName, e)
	})
	if err != nil {
		return nil, false, err
	}

	if pushable == nil {
		// Nothing could be pushed down, no rewrite can happen
		return filterNode, false, nil
	}

	newFromSpec := fromSpec.Copy().(*PhysicalFromProcedureSpec)
	if newFromSpec.FilterSet {
		newBody := semantic.ExprsToConjunction(newFromSpec.Filter.Block.Body.(semantic.Expression), pushable)
		newFromSpec.Filter.Block.Body = newBody
	} else {
		newFromSpec.FilterSet = true
		newFromSpec.Filter = filterSpec.Fn.Copy().(*semantic.FunctionExpression)
		newFromSpec.Filter.Block.Body = pushable
	}

	if notPushable == nil {
		// All predicates could be pushed down, so eliminate the filter
		mergedNode, err := plan.MergeToPhysicalNode(filterNode, fromNode, newFromSpec)
		if err != nil {
			return nil, false, err
		}
		return mergedNode, true, nil
	}

	err = fromNode.ReplaceSpec(newFromSpec)
	if err != nil {
		return nil, false, err
	}

	newFilterSpec := filterSpec.Copy().(*universe.FilterProcedureSpec)
	newFilterSpec.Fn.Block.Body = notPushable
	err = filterNode.ReplaceSpec(newFilterSpec)
	if err != nil {
		return nil, false, err
	}

	return filterNode, true, nil
}

func createFromSource(prSpec plan.ProcedureSpec, dsid execute.DatasetID, a execute.Administration) (execute.Source, error) {
	span, ctx := tracing.StartSpanFromContext(context.TODO())
	defer span.Finish()

	spec := prSpec.(*PhysicalFromProcedureSpec)
	var w execute.Window
	bounds := a.StreamContext().Bounds()
	if bounds == nil {
		return nil, errors.New("nil bounds passed to from")
	}

	// Note: currently no planner rules will push a window() into from()
	// so the following is dead code.
	if spec.WindowSet {
		w = execute.Window{
			Every:  execute.Duration(spec.Window.Every),
			Period: execute.Duration(spec.Window.Period),
			Offset: execute.Duration(spec.Window.Offset),
		}
	} else {
		duration := execute.Duration(bounds.Stop) - execute.Duration(bounds.Start)
		w = execute.Window{
			Every:  duration,
			Period: duration,
			Offset: bounds.Start.Remainder(duration),
		}
	}
	currentTime := bounds.Start + execute.Time(w.Period)

	deps := a.Dependencies()[FromKind].(Dependencies)
	req := query.RequestFromContext(a.Context())
	if req == nil {
		return nil, errors.New("missing request on context")
	}
	orgID := req.OrganizationID

	var bucketID platform.ID
	// Determine bucketID
	switch {
	case spec.Bucket != "":
		b, ok := deps.BucketLookup.Lookup(ctx, orgID, spec.Bucket)
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
		a.Allocator(),
	), nil
}

func InjectFromDependencies(depsMap execute.Dependencies, deps Dependencies) error {
	if err := deps.Validate(); err != nil {
		return err
	}
	depsMap[FromKind] = deps
	return nil
}
