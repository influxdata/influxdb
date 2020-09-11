package influxdb

import (
	"context"
	"fmt"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/stdlib/influxdata/influxdb"
	"github.com/influxdata/flux/values"
	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/query"
)

func init() {
	execute.RegisterSource(influxdb.BucketsKind, createBucketsSource)
}

type BucketsDecoder struct {
	orgID   platform.ID
	deps    BucketDependencies
	buckets []*platform.Bucket
	alloc   *memory.Allocator
}

func (bd *BucketsDecoder) Connect(ctx context.Context) error {
	return nil
}

func (bd *BucketsDecoder) Fetch(ctx context.Context) (bool, error) {
	b, count := bd.deps.FindAllBuckets(ctx, bd.orgID)
	if count <= 0 {
		return false, &flux.Error{
			Code: codes.NotFound,
			Msg:  fmt.Sprintf("no buckets found in organization %v", bd.orgID),
		}
	}
	bd.buckets = b
	return false, nil
}

func (bd *BucketsDecoder) Decode(ctx context.Context) (flux.Table, error) {
	kb := execute.NewGroupKeyBuilder(nil)
	kb.AddKeyValue("organizationID", values.NewString(bd.buckets[0].OrgID.String()))
	gk, err := kb.Build()
	if err != nil {
		return nil, err
	}

	b := execute.NewColListTableBuilder(gk, bd.alloc)

	if _, err := b.AddCol(flux.ColMeta{
		Label: "name",
		Type:  flux.TString,
	}); err != nil {
		return nil, err
	}
	if _, err := b.AddCol(flux.ColMeta{
		Label: "id",
		Type:  flux.TString,
	}); err != nil {
		return nil, err
	}
	if _, err := b.AddCol(flux.ColMeta{
		Label: "organizationID",
		Type:  flux.TString,
	}); err != nil {
		return nil, err
	}
	if _, err := b.AddCol(flux.ColMeta{
		Label: "retentionPolicy",
		Type:  flux.TString,
	}); err != nil {
		return nil, err
	}
	if _, err := b.AddCol(flux.ColMeta{
		Label: "retentionPeriod",
		Type:  flux.TInt,
	}); err != nil {
		return nil, err
	}

	for _, bucket := range bd.buckets {
		_ = b.AppendString(0, bucket.Name)
		_ = b.AppendString(1, bucket.ID.String())
		_ = b.AppendString(2, bucket.OrgID.String())
		_ = b.AppendString(3, bucket.RetentionPolicyName)
		_ = b.AppendInt(4, bucket.RetentionPeriod.Nanoseconds())
	}

	return b.Table()
}

func (bd *BucketsDecoder) Close() error {
	return nil
}

func createBucketsSource(prSpec plan.ProcedureSpec, dsid execute.DatasetID, a execute.Administration) (execute.Source, error) {
	_, ok := prSpec.(*influxdb.BucketsProcedureSpec)
	if !ok {
		return nil, &flux.Error{
			Code: codes.Internal,
			Msg:  fmt.Sprintf("invalid spec type %T", prSpec),
		}
	}

	// the dependencies used for FromKind are adequate for what we need here
	// so there's no need to inject custom dependencies for buckets()
	deps := GetStorageDependencies(a.Context()).BucketDeps
	req := query.RequestFromContext(a.Context())
	if req == nil {
		return nil, &flux.Error{
			Code: codes.Internal,
			Msg:  "missing request on context",
		}
	}
	orgID := req.OrganizationID

	bd := &BucketsDecoder{orgID: orgID, deps: deps, alloc: a.Allocator()}

	return execute.CreateSourceFromDecoder(bd, dsid, a)
}

type AllBucketLookup interface {
	FindAllBuckets(ctx context.Context, orgID platform.ID) ([]*platform.Bucket, int)
}
type BucketDependencies AllBucketLookup
