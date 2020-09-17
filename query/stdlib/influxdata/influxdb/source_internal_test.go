package influxdb

import (
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/plan"
)

func CreateReadWindowAggregateSource(s plan.ProcedureSpec, id execute.DatasetID, a execute.Administration) (execute.Source, error) {
	return createReadWindowAggregateSource(s, id, a)
}
