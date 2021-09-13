package v1

import (
	"context"
	"fmt"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/plan"
	v1 "github.com/influxdata/flux/stdlib/influxdata/influxdb/v1"
	"github.com/influxdata/flux/values"
	platform "github.com/influxdata/influxdb/v2"
	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
	errors2 "github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/pkg/errors"
)

const DatabasesKind = "influxdata/influxdb/v1.localDatabases"

func init() {
	execute.RegisterSource(DatabasesKind, createDatabasesSource)
	plan.RegisterPhysicalRules(LocalDatabasesRule{})
}

type LocalDatabasesProcedureSpec struct {
	plan.DefaultCost
}

func (s *LocalDatabasesProcedureSpec) Kind() plan.ProcedureKind {
	return DatabasesKind
}

func (s *LocalDatabasesProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(LocalDatabasesProcedureSpec)
	return ns
}

type DatabasesDecoder struct {
	orgID     platform2.ID
	deps      *DatabasesDependencies
	databases []*platform.DBRPMapping
	alloc     *memory.Allocator
}

func (bd *DatabasesDecoder) Connect(ctx context.Context) error {
	return nil
}

func (bd *DatabasesDecoder) Fetch(ctx context.Context) (bool, error) {
	b, _, err := bd.deps.DBRP.FindMany(ctx, platform.DBRPMappingFilter{})
	if err != nil {
		return false, err
	}
	bd.databases = b
	return false, nil
}

func (bd *DatabasesDecoder) Decode(ctx context.Context) (flux.Table, error) {
	type databaseInfo struct {
		*platform.DBRPMapping
		RetentionPeriod time.Duration
	}

	databases := make([]databaseInfo, 0, len(bd.databases))
	for _, db := range bd.databases {
		bucket, err := bd.deps.BucketLookup.FindBucketByID(ctx, db.BucketID)
		if err != nil {
			code := errors2.ErrorCode(err)
			if code == errors2.EUnauthorized || code == errors2.EForbidden {
				continue
			}
			return nil, err
		}
		databases = append(databases, databaseInfo{
			DBRPMapping:     db,
			RetentionPeriod: bucket.RetentionPeriod,
		})
	}

	if len(databases) == 0 {
		return nil, &errors2.Error{
			Code: errors2.ENotFound,
			Msg:  "no 1.x databases found",
		}
	}

	kb := execute.NewGroupKeyBuilder(nil)
	kb.AddKeyValue("organizationID", values.NewString(databases[0].OrganizationID.String()))
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

	for _, db := range databases {
		_ = b.AppendString(0, db.OrganizationID.String())
		_ = b.AppendString(1, db.Database)
		_ = b.AppendString(2, db.RetentionPolicy)
		_ = b.AppendInt(3, db.RetentionPeriod.Nanoseconds())
		_ = b.AppendBool(4, db.Default)
		_ = b.AppendString(5, db.BucketID.String())
	}

	return b.Table()
}

func (bd *DatabasesDecoder) Close() error {
	return nil
}

func createDatabasesSource(prSpec plan.ProcedureSpec, dsid execute.DatasetID, a execute.Administration) (execute.Source, error) {
	_, ok := prSpec.(*LocalDatabasesProcedureSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", prSpec)
	}
	deps := GetDatabasesDependencies(a.Context())
	req := query.RequestFromContext(a.Context())
	if req == nil {
		return nil, errors.New("missing request on context")
	}
	orgID := req.OrganizationID

	bd := &DatabasesDecoder{orgID: orgID, deps: &deps, alloc: a.Allocator()}

	return execute.CreateSourceFromDecoder(bd, dsid, a)
}

type key int

const dependenciesKey key = iota

type DatabasesDependencies struct {
	DBRP         platform.DBRPMappingService
	BucketLookup platform.BucketService
}

func (d DatabasesDependencies) Inject(ctx context.Context) context.Context {
	return context.WithValue(ctx, dependenciesKey, d)
}

func GetDatabasesDependencies(ctx context.Context) DatabasesDependencies {
	return ctx.Value(dependenciesKey).(DatabasesDependencies)
}

func (d DatabasesDependencies) Validate() error {
	if d.DBRP == nil {
		return errors.New("missing all databases lookup dependency")
	}
	if d.BucketLookup == nil {
		return errors.New("missing buckets lookup dependency")
	}
	return nil
}

type LocalDatabasesRule struct{}

func (rule LocalDatabasesRule) Name() string {
	return "influxdata/influxdb.LocalDatabasesRule"
}

func (rule LocalDatabasesRule) Pattern() plan.Pattern {
	return plan.Pat(v1.DatabasesKind)
}

func (rule LocalDatabasesRule) Rewrite(ctx context.Context, node plan.Node) (plan.Node, bool, error) {
	fromSpec := node.ProcedureSpec().(*v1.DatabasesProcedureSpec)
	if fromSpec.Host != nil {
		return node, false, nil
	} else if fromSpec.Org != nil {
		return node, false, &flux.Error{
			Code: codes.Unimplemented,
			Msg:  "buckets cannot list from a separate organization; please specify a host or remove the organization",
		}
	}

	return plan.CreateLogicalNode("localDatabases", &LocalDatabasesProcedureSpec{}), true, nil
}
