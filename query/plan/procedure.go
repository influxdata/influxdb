package plan

import (
	"fmt"

	"github.com/influxdata/platform/query/values"

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
	Bounds   *BoundsSpec
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

	if p.Bounds != nil {
		bounds := *p.Bounds
		np.Bounds = &bounds
	}

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
	TimeBounds() query.Bounds
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

type BoundsSpec struct {
	Start values.Time
	Stop  values.Time
}

var EmptyBoundsSpec = &BoundsSpec{
	Start: values.Time(0),
	Stop:  values.Time(0),
}

// IsEmpty reports whether the given bounds
// are empty, i.e., if start >= stop.
func (b *BoundsSpec) IsEmpty() bool {
	return b.Start >= b.Stop
}

// Contains reports whether a given time is contained within the range
// a given BoundsSpec represents
func (b *BoundsSpec) Contains(t values.Time) bool {
	return t >= b.Start && t < b.Stop
}

// Overlaps reports whether two given bounds have overlapping time ranges.
func (b *BoundsSpec) Overlaps(o *BoundsSpec) bool {
	return b.Contains(o.Start) ||
		(b.Contains(o.Stop) && o.Stop > b.Start) ||
		o.Contains(b.Start)
}

// Union returns the union of two time bounds (the smallest bounds which contain both input bounds)
// If either of the bounds have zeroes, Union will return a zero-valued BoundsSpec.
// Union with EmptyBoundsSpec always returns EmptyBoundsSpec.
func (b *BoundsSpec) Union(o *BoundsSpec) *BoundsSpec {
	if b.IsEmpty() || o.IsEmpty() {
		return EmptyBoundsSpec
	}
	u := new(BoundsSpec)

	u.Start = b.Start
	if o.Start < b.Start {
		u.Start = o.Start
	}

	u.Stop = b.Stop
	if o.Stop > b.Stop {
		u.Stop = o.Stop
	}

	return u
}

// Intersect returns the intersection of two bounds (the range over which they overlap).
// If either of the bounds have zeroes, it will return a zero-valued BoundsSpec.
// If there is no intersection, EmptyBoundsSpec is returned.
// Intersect with EmptyBoundsSpec will always return EmptyBoundsSpec.
func (b *BoundsSpec) Intersect(o *BoundsSpec) *BoundsSpec {
	if b.IsEmpty() || o.IsEmpty() || !b.Overlaps(o) {
		return EmptyBoundsSpec
	}
	i := new(BoundsSpec)

	i.Start = b.Start
	if o.Start > b.Start {
		i.Start = o.Start
	}

	i.Stop = b.Stop
	if o.Stop < b.Stop {
		i.Stop = o.Stop
	}

	return i
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
