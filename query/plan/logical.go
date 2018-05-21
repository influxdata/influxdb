package plan

import (
	"fmt"

	"github.com/influxdata/platform/query"
	uuid "github.com/satori/go.uuid"
)

var NilUUID uuid.UUID
var RootUUID = NilUUID

type LogicalPlanSpec struct {
	Procedures map[ProcedureID]*Procedure
	Order      []ProcedureID
	Resources  query.ResourceManagement
}

func (lp *LogicalPlanSpec) Do(f func(pr *Procedure)) {
	for _, id := range lp.Order {
		f(lp.Procedures[id])
	}
}

func (lp *LogicalPlanSpec) lookup(id ProcedureID) *Procedure {
	return lp.Procedures[id]
}

type LogicalPlanner interface {
	Plan(*query.Spec) (*LogicalPlanSpec, error)
}

type logicalPlanner struct {
	plan *LogicalPlanSpec
	q    *query.Spec
}

func NewLogicalPlanner() LogicalPlanner {
	return new(logicalPlanner)
}

func (p *logicalPlanner) Plan(q *query.Spec) (*LogicalPlanSpec, error) {
	p.q = q
	p.plan = &LogicalPlanSpec{
		Procedures: make(map[ProcedureID]*Procedure),
		Resources:  q.Resources,
	}
	err := q.Walk(p.walkQuery)
	if err != nil {
		return nil, err
	}
	return p.plan, nil
}

func ProcedureIDFromOperationID(id query.OperationID) ProcedureID {
	return ProcedureID(uuid.NewV5(RootUUID, string(id)))
}
func ProcedureIDFromParentID(id ProcedureID) ProcedureID {
	return ProcedureID(uuid.NewV5(RootUUID, id.String()))
}

func (p *logicalPlanner) walkQuery(o *query.Operation) error {
	spec, err := p.createSpec(o.Spec.Kind(), o.Spec)
	if err != nil {
		return err
	}

	pr := &Procedure{
		ID:   ProcedureIDFromOperationID(o.ID),
		Spec: spec,
	}
	p.plan.Order = append(p.plan.Order, pr.ID)
	p.plan.Procedures[pr.ID] = pr

	// Link parent/child relations
	parentOps := p.q.Parents(o.ID)
	for _, parentOp := range parentOps {
		parentID := ProcedureIDFromOperationID(parentOp.ID)
		parentPr := p.plan.Procedures[parentID]
		parentPr.Children = append(parentPr.Children, pr.ID)
		pr.Parents = append(pr.Parents, parentID)
	}

	return nil
}

func (p *logicalPlanner) createSpec(qk query.OperationKind, spec query.OperationSpec) (ProcedureSpec, error) {
	createPs, ok := queryOpToProcedure[qk]
	if !ok {
		return nil, fmt.Errorf("unknown query operation %v", qk)
	}
	//TODO(nathanielc): Support adding all procedures to logical plan instead of only the first
	return createPs[0](spec, p)
}

func (p *logicalPlanner) ConvertID(qid query.OperationID) ProcedureID {
	return ProcedureIDFromOperationID(qid)
}
