package plan

import (
	"fmt"
	"math"
	"time"

	"github.com/influxdata/platform/query"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

// DefaultYieldName is the yield name to use in cases where no explicit yield name was specified.
const DefaultYieldName = "_result"

type PlanSpec struct {
	// Now represents the relative current time of the plan.
	Now time.Time
	// Procedures is a set of all operations
	Procedures map[ProcedureID]*Procedure
	Order      []ProcedureID

	// Results is a list of datasets that are the result of the plan
	Results   map[string]YieldSpec
	Resources query.ResourceManagement
}

// YieldSpec defines how data should be yielded.
type YieldSpec struct {
	ID ProcedureID
}

func (p *PlanSpec) Do(f func(pr *Procedure)) {
	for _, id := range p.Order {
		f(p.Procedures[id])
	}
}
func (p *PlanSpec) lookup(id ProcedureID) *Procedure {
	return p.Procedures[id]
}

type Planner interface {
	// Plan create a plan from the logical plan and available storage.
	Plan(p *LogicalPlanSpec, s Storage) (*PlanSpec, error)
}

type PlanRewriter interface {
	IsolatePath(parent, child *Procedure) (*Procedure, error)
	RemoveBranch(pr *Procedure) error
	AddChild(parent *Procedure, childSpec ProcedureSpec)
}

type planner struct {
	plan *PlanSpec

	modified bool
}

func NewPlanner() Planner {
	return new(planner)
}

func (p *planner) Plan(lp *LogicalPlanSpec, s Storage) (*PlanSpec, error) {
	now := lp.Now

	p.plan = &PlanSpec{
		Now:        now,
		Procedures: make(map[ProcedureID]*Procedure, len(lp.Procedures)),
		Order:      make([]ProcedureID, 0, len(lp.Order)),
		Resources:  lp.Resources,
		Results:    make(map[string]YieldSpec),
	}

	lp.Do(func(pr *Procedure) {
		pr.plan = p.plan
		p.plan.Procedures[pr.ID] = pr
		p.plan.Order = append(p.plan.Order, pr.ID)
	})

	// Find Limit+Where+Range+Select to push down time bounds and predicate
	var order []ProcedureID
	p.modified = true
	for p.modified {
		p.modified = false
		if cap(order) < len(p.plan.Order) {
			order = make([]ProcedureID, len(p.plan.Order))
		} else {
			order = order[:len(p.plan.Order)]
		}
		copy(order, p.plan.Order)
		for _, id := range order {
			pr := p.plan.Procedures[id]
			if pr == nil {
				// Procedure was removed
				continue
			}
			if pd, ok := pr.Spec.(PushDownProcedureSpec); ok {
				rules := pd.PushDownRules()
				for _, rule := range rules {
					if remove, err := p.pushDownAndSearch(pr, rule, pd.PushDown); err != nil {
						return nil, err
					} else if remove {
						if err := p.removeProcedure(pr); err != nil {
							return nil, errors.Wrap(err, "failed to remove procedure")
						}
					}
				}
			}
		}
	}

	// Apply all rewrite rules
	p.modified = true
	for p.modified {
		p.modified = false
		for _, rule := range rewriteRules {
			kind := rule.Root()
			p.plan.Do(func(pr *Procedure) {
				if pr == nil {
					// Procedure was removed
					return
				}
				if pr.Spec.Kind() == kind {
					rule.Rewrite(pr, p)
				}
			})
		}
	}

	// Now that plan is complete find results and time bounds
	var leaves []ProcedureID
	var yields []*Procedure
	for _, id := range p.plan.Order {
		pr := p.plan.Procedures[id]

		// The bounds of the current procedure are always the union
		// of the bounds of any parent procedure
		pr.DoParents(func(parent *Procedure) {
			if pr.Bounds.HasZero() {
				pr.Bounds = parent.Bounds
			} else {
				pr.Bounds = pr.Bounds.Union(parent.Bounds, now)
			}
		})

		// If the procedure is bounded and provides its own additional bounds,
		// the procedure's new bounds are the intersection of any bounds it inherited
		// from its parents, and its own bounds.
		if bounded, ok := pr.Spec.(BoundedProcedureSpec); ok {
			if pr.Bounds.HasZero() {
				pr.Bounds = bounded.TimeBounds()
			} else {
				newBounds := pr.Bounds.Intersect(bounded.TimeBounds(), now)
				if newBounds != EmptyBoundsSpec {
					pr.Bounds = newBounds
				} else {
					pr.Bounds = bounded.TimeBounds()
				}
			}
		}

		if yield, ok := pr.Spec.(YieldProcedureSpec); ok {
			if len(pr.Parents) != 1 {
				return nil, errors.New("yield procedures must have exactly one parent")
			}

			parent := pr.Parents[0]
			name := yield.YieldName()
			_, ok := p.plan.Results[name]
			if ok {
				return nil, fmt.Errorf("found duplicate yield name %q", name)
			}
			p.plan.Results[name] = YieldSpec{ID: parent}
			yields = append(yields, pr)
		} else if len(pr.Children) == 0 {
			// Capture non yield leaves
			leaves = append(leaves, pr.ID)
		}
	}

	for _, pr := range yields {
		// remove yield procedure
		p.removeProcedure(pr)
	}

	if len(p.plan.Results) == 0 {
		if len(leaves) == 1 {
			p.plan.Results[DefaultYieldName] = YieldSpec{ID: leaves[0]}
		} else {
			return nil, errors.New("query must specify explicit yields when there is more than one result.")
		}
	}

	for name, yield := range p.plan.Results {
		if pr, ok := p.plan.Procedures[yield.ID]; ok {
			if pr.Bounds.HasZero() {
				return nil, fmt.Errorf(`result '%s' is unbounded. Add a 'range' call to bound the query.`, name)
			}
		}
	}

	// Update concurrency quota
	if p.plan.Resources.ConcurrencyQuota == 0 {
		p.plan.Resources.ConcurrencyQuota = len(p.plan.Procedures)
	}
	// Update memory quota
	if p.plan.Resources.MemoryBytesQuota == 0 {
		p.plan.Resources.MemoryBytesQuota = math.MaxInt64
	}

	return p.plan, nil
}

func hasKind(kind ProcedureKind, kinds []ProcedureKind) bool {
	for _, k := range kinds {
		if k == kind {
			return true
		}
	}
	return false
}

func (p *planner) pushDownAndSearch(pr *Procedure, rule PushDownRule, do func(parent *Procedure, dup func() *Procedure)) (bool, error) {
	matched := false
	for _, parent := range pr.Parents {
		pp := p.plan.Procedures[parent]
		pk := pp.Spec.Kind()
		if pk == rule.Root {
			if rule.Match == nil || rule.Match(pp.Spec) {
				isolatedParent, err := p.IsolatePath(pp, pr)
				if err != nil {
					return false, err
				}
				if pp != isolatedParent {
					// Wait to call push down function when the duplicate is found
					return false, nil
				}
				do(pp, func() *Procedure { return p.duplicate(pp, false) })
				matched = true
			}
		} else if hasKind(pk, rule.Through) {
			if _, err := p.pushDownAndSearch(pp, rule, do); err != nil {
				return false, err
			}
		}
	}
	return matched, nil
}

// IsolatePath ensures that the child is an only child of the parent.
// The return value is the parent procedure who has an only child.
func (p *planner) IsolatePath(parent, child *Procedure) (*Procedure, error) {
	if len(parent.Children) == 1 {
		return parent, nil
	}
	// Duplicate just this child branch
	dup := p.duplicateChildBranch(parent, child.ID)
	// Remove this entire branch since it has been duplicated.
	if err := p.RemoveBranch(child); err != nil {
		return nil, err
	}
	return dup, nil
}

func (p *planner) AddChild(parent *Procedure, childSpec ProcedureSpec) {
	child := &Procedure{
		plan: p.plan,
		ID:   ProcedureIDFromParentID(parent.ID),
		Spec: childSpec,
	}
	parent.Children = append(parent.Children, child.ID)
	child.Parents = []ProcedureID{parent.ID}

	p.plan.Procedures[child.ID] = child
	p.plan.Order = insertAfter(p.plan.Order, parent.ID, child.ID)
}

func (p *planner) removeProcedure(pr *Procedure) error {
	// It only makes sense to remove a procedure that has a single parent.
	if len(pr.Parents) > 1 {
		return errors.New("cannot remove a procedure that has more than one parent")
	}

	p.modified = true
	delete(p.plan.Procedures, pr.ID)
	p.plan.Order = removeID(p.plan.Order, pr.ID)

	for _, id := range pr.Parents {
		parent := p.plan.Procedures[id]
		parent.Children = removeID(parent.Children, pr.ID)
		parent.Children = append(parent.Children, pr.Children...)
	}
	for _, id := range pr.Children {
		child := p.plan.Procedures[id]
		child.Parents = removeID(child.Parents, pr.ID)
		child.Parents = append(child.Parents, pr.Parents...)

		if len(pr.Parents) == 1 {
			if pa, ok := child.Spec.(ParentAwareProcedureSpec); ok {
				pa.ParentChanged(pr.ID, pr.Parents[0])
			}
		}
	}
	return nil
}

func (p *planner) RemoveBranch(pr *Procedure) error {
	// It only makes sense to remove a procedure that has a single parent.
	if len(pr.Parents) > 1 {
		return errors.New("cannot remove a branch that has more than one parent")
	}
	p.modified = true
	delete(p.plan.Procedures, pr.ID)
	p.plan.Order = removeID(p.plan.Order, pr.ID)

	for _, id := range pr.Parents {
		parent := p.plan.Procedures[id]
		// Check that parent hasn't already been removed
		if parent != nil {
			parent.Children = removeID(parent.Children, pr.ID)
		}
	}

	for _, id := range pr.Children {
		child := p.plan.Procedures[id]
		if err := p.RemoveBranch(child); err != nil {
			return err
		}
	}
	return nil
}

func ProcedureIDForDuplicate(id ProcedureID) ProcedureID {
	return ProcedureID(uuid.NewV5(RootUUID, id.String()))
}

func (p *planner) duplicateChildBranch(pr *Procedure, child ProcedureID) *Procedure {
	return p.duplicate(pr, true, child)
}
func (p *planner) duplicate(pr *Procedure, skipParents bool, onlyChildren ...ProcedureID) *Procedure {
	p.modified = true
	np := pr.Copy()
	np.ID = ProcedureIDForDuplicate(pr.ID)
	p.plan.Procedures[np.ID] = np
	p.plan.Order = insertAfter(p.plan.Order, pr.ID, np.ID)

	if !skipParents {
		for _, id := range np.Parents {
			parent := p.plan.Procedures[id]
			parent.Children = append(parent.Children, np.ID)
		}
	}

	newChildren := make([]ProcedureID, 0, len(np.Children))
	for _, id := range np.Children {
		if len(onlyChildren) > 0 && !hasID(onlyChildren, id) {
			continue
		}
		child := p.plan.Procedures[id]
		newChild := p.duplicate(child, true)
		newChild.Parents = removeID(newChild.Parents, pr.ID)
		newChild.Parents = append(newChild.Parents, np.ID)

		newChildren = append(newChildren, newChild.ID)

		if pa, ok := newChild.Spec.(ParentAwareProcedureSpec); ok {
			pa.ParentChanged(pr.ID, np.ID)
		}
	}
	np.Children = newChildren
	return np
}

func hasID(ids []ProcedureID, id ProcedureID) bool {
	for _, i := range ids {
		if i == id {
			return true
		}
	}
	return false
}
func removeID(ids []ProcedureID, remove ProcedureID) []ProcedureID {
	filtered := ids[0:0]
	for i, id := range ids {
		if id == remove {
			filtered = append(filtered, ids[0:i]...)
			filtered = append(filtered, ids[i+1:]...)
			break
		}
	}
	return filtered
}
func insertAfter(ids []ProcedureID, after, new ProcedureID) []ProcedureID {
	var newIds []ProcedureID
	for i, id := range ids {
		if id == after {
			newIds = append(newIds, ids[:i+1]...)
			newIds = append(newIds, new)
			if i+1 < len(ids) {
				newIds = append(newIds, ids[i+1:]...)
			}
			break
		}
	}
	return newIds
}
