package plan

import (
	"fmt"
	"time"

	"github.com/influxdata/platform/query"
	uuid "github.com/satori/go.uuid"
)

type ProcedureID uuid.UUID

func (id ProcedureID) String() string {
	return uuid.UUID(id).String()
}

var ZeroProcedureID ProcedureID

type Procedure struct {
	plan     *PlanSpec
	ID       ProcedureID
	Parents  []ProcedureID
	Children []ProcedureID
	Spec     ProcedureSpec
	Bounds   BoundsSpec
}

func (p *Procedure) Copy() *Procedure {
	np := new(Procedure)
	np.ID = p.ID

	np.plan = p.plan

	np.Parents = make([]ProcedureID, len(p.Parents))
	copy(np.Parents, p.Parents)

	np.Children = make([]ProcedureID, len(p.Children))
	copy(np.Children, p.Children)

	np.Spec = p.Spec.Copy()

	return np
}

func (p *Procedure) DoChildren(f func(pr *Procedure)) {
	for _, id := range p.Children {
		f(p.plan.Procedures[id])
	}
}
func (p *Procedure) DoParents(f func(pr *Procedure)) {
	for _, id := range p.Parents {
		f(p.plan.Procedures[id])
	}
}
func (p *Procedure) Child(i int) *Procedure {
	return p.plan.Procedures[p.Children[i]]
}

type Administration interface {
	ConvertID(query.OperationID) ProcedureID
}

type CreateProcedureSpec func(query.OperationSpec, Administration) (ProcedureSpec, error)

// ProcedureSpec specifies an operation as part of a query.
type ProcedureSpec interface {
	// Kind returns the kind of the procedure.
	Kind() ProcedureKind
	Copy() ProcedureSpec
}

type PushDownProcedureSpec interface {
	PushDownRules() []PushDownRule
	PushDown(root *Procedure, dup func() *Procedure)
}

type BoundedProcedureSpec interface {
	TimeBounds() BoundsSpec
}

type YieldProcedureSpec interface {
	YieldName() string
}
type AggregateProcedureSpec interface {
	// AggregateMethod specifies which aggregate method to push down to the storage layer.
	AggregateMethod() string
	// ReAggregateSpec specifies an aggregate procedure to use when aggregating the individual pushed down results.
	ReAggregateSpec() ProcedureSpec
}

type ParentAwareProcedureSpec interface {
	ParentChanged(old, new ProcedureID)
}

// TODO(nathanielc): make this more formal using commute/associative properties
type PushDownRule struct {
	Root    ProcedureKind
	Through []ProcedureKind
	Match   func(ProcedureSpec) bool
}

// ProcedureKind denotes the kind of operations.
type ProcedureKind string

// TODO(adamperlin): make plan.BoundsSpec always use resolved absolute times, and remove
// the `now` parameter from all associated methods.
type BoundsSpec struct {
	Start query.Time
	Stop  query.Time
}

var EmptyBoundsSpec = BoundsSpec{
	Start: query.Now,
	Stop:  query.Now,
}

// IsEmpty reports whether the given bounds
// are empty, i.e., if start >= stop.
func (b BoundsSpec) IsEmpty(now time.Time) bool {
	return b.Start.Time(now).Equal(b.Stop.Time(now)) || b.Start.Time(now).After(b.Stop.Time(now))
}

// HasZero returns true if the given bounds contain a Go zero time value as either Start or Stop.
func (b BoundsSpec) HasZero() bool {
	return b.Start.IsZero() || b.Stop.IsZero()
}

// Contains reports whether a given time is contained within the range
// a given BoundsSpec represents
func (b BoundsSpec) Contains(t time.Time, now time.Time) bool {
	return (t.After(b.Start.Time(now)) || t.Equal(b.Start.Time(now))) && t.Before(b.Stop.Time(now))
}

// Overlaps reports whether two given bounds have overlapping time ranges.
func (b BoundsSpec) Overlaps(o BoundsSpec, now time.Time) bool {
	return b.Contains(o.Start.Time(now), now) ||
		(b.Contains(o.Stop.Time(now), now) && o.Stop.Time(now).After(b.Start.Time(now))) ||
		o.Contains(b.Start.Time(now), now)
}

// Union returns the union of two time bounds (the smallest bounds which contain both input bounds)
// If either of the bounds have zeroes, Union will return a zero-valued BoundsSpec.
// Union with EmptyBoundsSpec always returns EmptyBoundsSpec.
func (b BoundsSpec) Union(o BoundsSpec, now time.Time) (u BoundsSpec) {
	if b.HasZero() || o.HasZero() {
		return
	}

	if b.IsEmpty(now) || o.IsEmpty(now) {
		return EmptyBoundsSpec
	}

	u.Start = b.Start
	if o.Start.Time(now).Before(b.Start.Time(now)) {
		u.Start = o.Start
	}

	u.Stop = b.Stop
	if o.Stop.Time(now).After(b.Stop.Time(now)) {
		u.Stop = o.Stop
	}

	return
}

// Intersect returns the intersection of two bounds (the range over which they overlap).
// If either of the bounds have zeroes, it will return a zero-valued BoundsSpec.
// If there is no intersection, EmptyBoundsSpec is returned.
// Intersect with EmptyBoundsSpec will always return EmptyBoundsSpec.
func (b BoundsSpec) Intersect(o BoundsSpec, now time.Time) (i BoundsSpec) {
	if b.HasZero() || o.HasZero() {
		return
	}

	if b.IsEmpty(now) || o.IsEmpty(now) || !b.Overlaps(o, now) {
		return EmptyBoundsSpec
	}

	i.Start = b.Start
	if o.Start.Time(now).After(b.Start.Time(now)) {
		i.Start = o.Start
	}

	i.Stop = b.Stop
	if o.Stop.Time(now).Before(b.Stop.Time(now)) {
		i.Stop = o.Stop
	}

	return
}

type WindowSpec struct {
	Every  query.Duration
	Period query.Duration
	Round  query.Duration
	Start  query.Time
}

var kindToProcedure = make(map[ProcedureKind]CreateProcedureSpec)
var queryOpToProcedure = make(map[query.OperationKind][]CreateProcedureSpec)

// RegisterProcedureSpec registers a new procedure with the specified kind.
// The call panics if the kind is not unique.
func RegisterProcedureSpec(k ProcedureKind, c CreateProcedureSpec, qks ...query.OperationKind) {
	if kindToProcedure[k] != nil {
		panic(fmt.Errorf("duplicate registration for procedure kind %v", k))
	}
	kindToProcedure[k] = c
	for _, qk := range qks {
		queryOpToProcedure[qk] = append(queryOpToProcedure[qk], c)
	}
}
