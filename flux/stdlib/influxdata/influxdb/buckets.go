package influxdb

import (
	"errors"
	"fmt"

	"github.com/influxdata/flux"
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
	deps  BucketDependencies
	alloc *memory.Allocator
	user  meta.User
}

func (bd *BucketsDecoder) Connect() error {
	return nil
}

func (bd *BucketsDecoder) Fetch() (bool, error) {
	return false, nil
}

func (bd *BucketsDecoder) Decode() (flux.Table, error) {
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
		Label: "organization",
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
				_ = b.AppendString(2, "influxdb")
				_ = b.AppendString(3, "")
				_ = b.AppendString(4, rp.Name)
				_ = b.AppendInt(5, rp.Duration.Nanoseconds())
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
		return nil, fmt.Errorf("invalid spec type %T", prSpec)
	}

	// the dependencies used for FromKind are adequate for what we need here
	// so there's no need to inject custom dependencies for buckets()
	deps := a.Dependencies()[influxdb.BucketsKind].(BucketDependencies)

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

type BucketDependencies struct {
	MetaClient  MetaClient
	Authorizer  Authorizer
	AuthEnabled bool
}

func (d BucketDependencies) Validate() error {
	if d.MetaClient == nil {
		return errors.New("validate BucketDependencies: missing MetaClient")
	}
	if d.AuthEnabled && d.Authorizer == nil {
		return errors.New("validate BucketDependencies: missing Authorizer")
	}
	return nil
}

func InjectBucketDependencies(depsMap execute.Dependencies, deps BucketDependencies) error {
	if err := deps.Validate(); err != nil {
		return err
	}
	depsMap[influxdb.BucketsKind] = deps
	return nil
}
