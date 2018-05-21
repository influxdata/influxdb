package plantest

import (
	"github.com/influxdata/ifql/query/plan"
	uuid "github.com/satori/go.uuid"
)

func RandomProcedureID() plan.ProcedureID {
	return plan.ProcedureID(uuid.NewV4())
}
