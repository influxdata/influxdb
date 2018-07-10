package execute

import (
	"fmt"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/plan"
)

type Transformation interface {
	RetractBlock(id DatasetID, key query.GroupKey) error
	Process(id DatasetID, b query.Block) error
	UpdateWatermark(id DatasetID, t Time) error
	UpdateProcessingTime(id DatasetID, t Time) error
	Finish(id DatasetID, err error)
}

type Administration interface {
	OrganizationID() platform.ID

	ResolveTime(qt query.Time) Time
	Bounds() Bounds
	Allocator() *Allocator
	Parents() []DatasetID
	ConvertID(plan.ProcedureID) DatasetID

	Dependencies() Dependencies
}

// Dependencies represents the provided dependencies to the execution environment.
// The dependencies is opaque.
type Dependencies map[string]interface{}

type CreateTransformation func(id DatasetID, mode AccumulationMode, spec plan.ProcedureSpec, a Administration) (Transformation, Dataset, error)

var procedureToTransformation = make(map[plan.ProcedureKind]CreateTransformation)

func RegisterTransformation(k plan.ProcedureKind, c CreateTransformation) {
	if procedureToTransformation[k] != nil {
		panic(fmt.Errorf("duplicate registration for transformation with procedure kind %v", k))
	}
	procedureToTransformation[k] = c
}
