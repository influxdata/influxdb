package influxdb

import (
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/plan"
)

func CreateReadFilterSource(s plan.ProcedureSpec, id execute.DatasetID, a execute.Administration) (execute.Source, error) {
	return createReadFilterSource(s, id, a)
}

func CreateReadGroupSource(s plan.ProcedureSpec, id execute.DatasetID, a execute.Administration) (execute.Source, error) {
	return createReadGroupSource(s, id, a)
}

func CreateReadWindowAggregateSource(s plan.ProcedureSpec, id execute.DatasetID, a execute.Administration) (execute.Source, error) {
	return createReadWindowAggregateSource(s, id, a)
}

func CreateReadTagKeysSource(s plan.ProcedureSpec, id execute.DatasetID, a execute.Administration) (execute.Source, error) {
	return createReadTagKeysSource(s, id, a)
}

func CreateReadTagValuesSource(s plan.ProcedureSpec, id execute.DatasetID, a execute.Administration) (execute.Source, error) {
	return createReadTagValuesSource(s, id, a)
}
