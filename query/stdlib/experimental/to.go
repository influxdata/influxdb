package experimental

import (
	"fmt"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/stdlib/experimental"
	platform "github.com/influxdata/influxdb"
)

// ToKind is the kind for the `to` flux function
const ExperimentalToKind = experimental.ExperimentalToKind

// ToOpSpec is the flux.OperationSpec for the `to` flux function.
type ToOpSpec struct {
	Bucket   string `json:"bucket"`
	BucketID string `json:"bucketID"`
	Org      string `json:"org"`
	OrgID    string `json:"orgID"`
	Host     string `json:"host"`
	Token    string `json:"token"`
}

func init() {
	toSignature := flux.FunctionSignature(
		map[string]semantic.PolyType{
			"bucket":   semantic.String,
			"bucketID": semantic.String,
			"org":      semantic.String,
			"orgID":    semantic.String,
			"host":     semantic.String,
			"token":    semantic.String,
		},
		[]string{},
	)

	flux.ReplacePackageValue("experimental", "to", flux.FunctionValueWithSideEffect("to", createToOpSpec, toSignature))
	flux.RegisterOpSpec(ExperimentalToKind, func() flux.OperationSpec { return &ToOpSpec{} })
	plan.RegisterProcedureSpecWithSideEffect(ExperimentalToKind, newToProcedure, ExperimentalToKind)
}

// ReadArgs reads the args from flux.Arguments into the op spec
func (o *ToOpSpec) ReadArgs(args flux.Arguments) error {
	var err error
	var ok bool

	if o.Bucket, ok, _ = args.GetString("bucket"); !ok {
		if o.BucketID, err = args.GetRequiredString("bucketID"); err != nil {
			return err
		}
	} else if o.BucketID, ok, _ = args.GetString("bucketID"); ok {
		return &flux.Error{
			Code: codes.Invalid,
			Msg:  "cannot provide both `bucket` and `bucketID` parameters to the `to` function",
		}
	}

	if o.Org, ok, _ = args.GetString("org"); !ok {
		if o.OrgID, err = args.GetRequiredString("orgID"); err != nil {
			return err
		}
	} else if o.OrgID, ok, _ = args.GetString("orgID"); ok {
		return &flux.Error{
			Code: codes.Invalid,
			Msg:  "cannot provide both `org` and `orgID` parameters to the `to` function",
		}
	}

	if o.Host, ok, _ = args.GetString("host"); ok {
		if o.Token, err = args.GetRequiredString("token"); err != nil {
			return err
		}
	}

	return err
}

func createToOpSpec(args flux.Arguments, a *flux.Administration) (flux.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}

	s := &ToOpSpec{}
	if err := s.ReadArgs(args); err != nil {
		return nil, err
	}
	return s, nil
}

// Kind returns the kind for the ToOpSpec function.
func (ToOpSpec) Kind() flux.OperationKind {
	return ExperimentalToKind
}

// BucketsAccessed returns the buckets accessed by the spec.
func (o *ToOpSpec) BucketsAccessed(orgID *platform.ID) (readBuckets, writeBuckets []platform.BucketFilter) {
	bf := platform.BucketFilter{}
	if o.Bucket != "" {
		bf.Name = &o.Bucket
	}
	if o.BucketID != "" {
		id, err := platform.IDFromString(o.BucketID)
		if err == nil {
			bf.ID = id
		}
	}
	if o.Org != "" {
		bf.Org = &o.Org
	}
	if o.OrgID != "" {
		id, err := platform.IDFromString(o.OrgID)
		if err == nil {
			bf.OrganizationID = id
		}
	}
	writeBuckets = append(writeBuckets, bf)
	return readBuckets, writeBuckets
}

// ToProcedureSpec is the procedure spec for the `to` flux function.
type ToProcedureSpec struct {
	plan.DefaultCost
	Spec *ToOpSpec
}

// Kind returns the kind for the procedure spec for the `to` flux function.
func (o *ToProcedureSpec) Kind() plan.ProcedureKind {
	return ExperimentalToKind
}

// Copy clones the procedure spec for `to` flux function.
func (o *ToProcedureSpec) Copy() plan.ProcedureSpec {
	s := o.Spec
	res := &ToProcedureSpec{
		Spec: &ToOpSpec{
			Bucket:   s.Bucket,
			BucketID: s.BucketID,
			Org:      s.Org,
			OrgID:    s.OrgID,
			Host:     s.Host,
			Token:    s.Token,
		},
	}
	return res
}

func newToProcedure(qs flux.OperationSpec, a plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*ToOpSpec)
	if !ok && spec != nil {
		return nil, &flux.Error{
			Code: codes.Internal,
			Msg:  fmt.Sprintf("invalid spec type %T", qs),
		}
	}
	return &ToProcedureSpec{Spec: spec}, nil
}
