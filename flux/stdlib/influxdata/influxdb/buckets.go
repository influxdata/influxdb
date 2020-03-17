package influxdb

import (
	"context"
	"errors"
	"fmt"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/stdlib/influxdata/influxdb"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxql"
)

func init() {
	execute.RegisterSource(influxdb.BucketsKind, createBucketsSource)
}

type BucketsDecoder struct {
	deps  StorageDependencies
	alloc *memory.Allocator
	user  meta.User
}

func (bd *BucketsDecoder) Connect(ctx context.Context) error {
	return nil
}

func (bd *BucketsDecoder) Fetch(ctx context.Context) (bool, error) {
	return false, nil
}

func (bd *BucketsDecoder) Decode(ctx context.Context) (flux.Table, error) {
	kb := execute.NewGroupKeyBuilder(nil)
	kb.AddKeyValue("organizationID", values.NewString(""))
	gk, err := kb.Build()
	if err != nil {
		return nil, err
	}

	b := execute.NewColListTableBuilder(gk, bd.alloc)

	_, _ = b.AddCol(flux.ColMeta{
		Label: "name",
		Type:  flux.TString,
	})
	_, _ = b.AddCol(flux.ColMeta{
		Label: "id",
		Type:  flux.TString,
	})
	_, _ = b.AddCol(flux.ColMeta{
		Label: "organizationID",
		Type:  flux.TString,
	})
	_, _ = b.AddCol(flux.ColMeta{
		Label: "retentionPolicy",
		Type:  flux.TString,
	})
	_, _ = b.AddCol(flux.ColMeta{
		Label: "retentionPeriod",
		Type:  flux.TInt,
	})

	var hasAccess func(db string) bool
	if bd.user == nil {
		hasAccess = func(db string) bool {
			return true
		}
	} else {
		hasAccess = func(db string) bool {
			return bd.deps.Authorizer.AuthorizeDatabase(bd.user, influxql.ReadPrivilege, db) == nil ||
				bd.deps.Authorizer.AuthorizeDatabase(bd.user, influxql.WritePrivilege, db) == nil
		}
	}

	for _, db := range bd.deps.MetaClient.Databases() {
		if hasAccess(db.Name) {
			for _, rp := range db.RetentionPolicies {
				_ = b.AppendString(0, db.Name+"/"+rp.Name)
				_ = b.AppendString(1, "")
				_ = b.AppendString(2, "")
				_ = b.AppendString(3, rp.Name)
				_ = b.AppendInt(4, rp.Duration.Nanoseconds())
			}
		}
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
	deps := GetStorageDependencies(a.Context())

	var user meta.User
	if deps.AuthEnabled {
		user = meta.UserFromContext(a.Context())
		if user == nil {
			return nil, errors.New("createBucketsSource: no user")
		}
	}

	bd := &BucketsDecoder{deps: deps, alloc: a.Allocator(), user: user}

	return execute.CreateSourceFromDecoder(bd, dsid, a)
}

type MetaClient interface {
	Databases() []meta.DatabaseInfo
	Database(name string) *meta.DatabaseInfo
}
