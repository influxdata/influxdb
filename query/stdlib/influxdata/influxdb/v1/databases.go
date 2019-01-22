package v1

import (
	"context"
	"fmt"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/stdlib/influxdata/influxdb/v1"
	"github.com/influxdata/flux/values"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/query"
	"github.com/pkg/errors"
)

const DatabasesKind = v1.DatabasesKind

type DatabasesOpSpec struct {
}

func init() {
	flux.ReplacePackageValue("influxdata/influxdb/v1", DatabasesKind, flux.FunctionValue(DatabasesKind, createDatabasesOpSpec, v1.DatabasesSignature))
	flux.RegisterOpSpec(DatabasesKind, newDatabasesOp)
	plan.RegisterProcedureSpec(DatabasesKind, newDatabasesProcedure, DatabasesKind)
}

func createDatabasesOpSpec(args flux.Arguments, a *flux.Administration) (flux.OperationSpec, error) {
	spec := new(DatabasesOpSpec)
	return spec, nil
}

func newDatabasesOp() flux.OperationSpec {
	return new(DatabasesOpSpec)
}

func (s *DatabasesOpSpec) Kind() flux.OperationKind {
	return DatabasesKind
}

type DatabasesProcedureSpec struct {
	plan.DefaultCost
}

func newDatabasesProcedure(qs flux.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	_, ok := qs.(*DatabasesOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}

	return &DatabasesProcedureSpec{}, nil
}

func (s *DatabasesProcedureSpec) Kind() plan.ProcedureKind {
	return DatabasesKind
}

func (s *DatabasesProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(DatabasesProcedureSpec)
	return ns
}

func init() {
	execute.RegisterSource(DatabasesKind, createDatabasesSource)
}

type DatabasesDecoder struct {
	orgID     platform.ID
	deps      *DatabasesDependencies
	databases []*platform.DBRPMapping
	alloc     *memory.Allocator
	ctx       context.Context
}

func (bd *DatabasesDecoder) Connect() error {
	return nil
}

func (bd *DatabasesDecoder) Fetch() (bool, error) {

	b, _, err := bd.deps.DBRP.FindMany(bd.ctx, platform.DBRPMappingFilter{})
	if err != nil {
		return false, err
	}
	bd.databases = b
	return false, nil
}

func (bd *DatabasesDecoder) Decode() (flux.Table, error) {
	kb := execute.NewGroupKeyBuilder(nil)
	if len(bd.databases) == 0 {
		return nil, errors.New("no 1.x databases found")
	}
	kb.AddKeyValue("organizationID", values.NewString(bd.databases[0].OrganizationID.String()))
	gk, err := kb.Build()
	if err != nil {
		return nil, err
	}

	b := execute.NewColListTableBuilder(gk, bd.alloc)

	if _, err := b.AddCol(flux.ColMeta{
		Label: "organizationID",
		Type:  flux.TString,
	}); err != nil {
		return nil, err
	}
	if _, err := b.AddCol(flux.ColMeta{
		Label: "databaseName",
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
	if _, err := b.AddCol(flux.ColMeta{
		Label: "default",
		Type:  flux.TBool,
	}); err != nil {
		return nil, err
	}
	if _, err := b.AddCol(flux.ColMeta{
		Label: "bucketId",
		Type:  flux.TString,
	}); err != nil {
		return nil, err
	}

	for _, db := range bd.databases {
		if bucket, err := bd.deps.BucketLookup.FindBucketByID(bd.ctx, db.BucketID); err != nil {
			return nil, err
		} else {
			_ = b.AppendString(0, db.OrganizationID.String())
			_ = b.AppendString(1, db.Database)
			_ = b.AppendString(2, db.RetentionPolicy)
			_ = b.AppendInt(3, bucket.RetentionPeriod.Nanoseconds())
			_ = b.AppendBool(4, db.Default)
			_ = b.AppendString(5, db.BucketID.String())
		}
	}

	return b.Table()
}

func createDatabasesSource(prSpec plan.ProcedureSpec, dsid execute.DatasetID, a execute.Administration) (execute.Source, error) {
	_, ok := prSpec.(*DatabasesProcedureSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", prSpec)
	}

	// the dependencies used for FromKind are adequate for what we need here
	// so there's no need to inject custom dependencies for databases()
	deps := a.Dependencies()[DatabasesKind].(DatabasesDependencies)
	req := query.RequestFromContext(a.Context())
	if req == nil {
		return nil, errors.New("missing request on context")
	}
	orgID := req.OrganizationID

	bd := &DatabasesDecoder{orgID: orgID, deps: &deps, alloc: a.Allocator(), ctx: a.Context()}

	return execute.CreateSourceFromDecoder(bd, dsid, a)
}

type DatabasesDependencies struct {
	DBRP         platform.DBRPMappingService
	BucketLookup platform.BucketService
}

func InjectDatabasesDependencies(depsMap execute.Dependencies, deps DatabasesDependencies) error {
	if deps.DBRP == nil {
		return errors.New("missing all databases lookup dependency")
	}

	if deps.BucketLookup == nil {
		return errors.New("missing buckets lookup dependency")
	}

	depsMap[DatabasesKind] = deps
	return nil
}
