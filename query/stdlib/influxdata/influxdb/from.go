package influxdb

import (
	"fmt"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/stdlib/influxdata/influxdb"
)

const FromKind = "influxDBFrom"

type (
	NameOrID   = influxdb.NameOrID
	FromOpSpec = influxdb.FromOpSpec
)

type FromStorageProcedureSpec struct {
	Bucket influxdb.NameOrID
}

func (s *FromStorageProcedureSpec) Kind() plan.ProcedureKind {
	return FromKind
}

func (s *FromStorageProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(FromStorageProcedureSpec)
	ns.Bucket = s.Bucket
	return ns
}

func (s *FromStorageProcedureSpec) PostPhysicalValidate(id plan.NodeID) error {
	// FromStorageProcedureSpec is a logical operation representing any read
	// from storage. However as a logical operation, it doesn't specify
	// how data is to be read from storage. It is the query planner's
	// job to determine the optimal read strategy and to convert this
	// logical operation into the appropriate physical operation.
	//
	// Logical operations cannot be executed by the query engine. So if
	// this operation is still around post physical planning, it means
	// that a 'range' could not be pushed down to storage. Storage does
	// not support unbounded reads, and so this query must not be
	// validated.
	var bucket string
	if s.Bucket.Name != "" {
		bucket = s.Bucket.Name
	} else {
		bucket = s.Bucket.ID
	}
	return &flux.Error{
		Code: codes.Invalid,
		Msg:  fmt.Sprintf("cannot submit unbounded read to %q; try bounding 'from' with a call to 'range'", bucket),
	}
}
