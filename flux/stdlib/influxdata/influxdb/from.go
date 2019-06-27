package influxdb

import (
	"fmt"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/stdlib/influxdata/influxdb"
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

func (s *FromProcedureSpec) PostPhysicalValidate(id plan.NodeID) error {
	// FromProcedureSpec is a logical operation representing any read
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
	if len(s.Bucket) > 0 {
		bucket = s.Bucket
	} else {
		bucket = s.BucketID
	}
	return fmt.Errorf("cannot submit unbounded read to %q; try bounding 'from' with a call to 'range'", bucket)
}

func InjectFromDependencies(depsMap execute.Dependencies, deps Dependencies) error {
	if err := deps.Validate(); err != nil {
		return err
	}
	depsMap[FromKind] = deps
	return nil
}
