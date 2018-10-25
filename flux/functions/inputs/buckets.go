package inputs

import (
	"fmt"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/functions/inputs"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb/services/storage"
	"github.com/pkg/errors"
)

func init() {
	execute.RegisterSource(inputs.BucketsKind, createBucketsSource)
}

type BucketsDecoder struct {
	deps  BucketDependencies
	alloc *execute.Allocator
}

func (bd *BucketsDecoder) Connect() error {
	return nil
}

func (bd *BucketsDecoder) Fetch() (bool, error) {
	return false, nil
}

func (bd *BucketsDecoder) Decode() (flux.Table, error) {
	kb := execute.NewGroupKeyBuilder(nil)
	kb.AddKeyValue("organizationID", values.NewString("influxdb"))
	gk, err := kb.Build()
	if err != nil {
		return nil, err
	}

	b := execute.NewColListTableBuilder(gk, bd.alloc)

	b.AddCol(flux.ColMeta{
		Label: "name",
		Type:  flux.TString,
	})
	b.AddCol(flux.ColMeta{
		Label: "id",
		Type:  flux.TString,
	})
	b.AddCol(flux.ColMeta{
		Label: "organization",
		Type:  flux.TString,
	})
	b.AddCol(flux.ColMeta{
		Label: "organizationID",
		Type:  flux.TString,
	})
	b.AddCol(flux.ColMeta{
		Label: "retentionPolicy",
		Type:  flux.TString,
	})
	b.AddCol(flux.ColMeta{
		Label: "retentionPeriod",
		Type:  flux.TInt,
	})

	for _, database := range bd.deps.TSDBStore.Databases() {
		bucket := bd.deps.MetaClient.Database(database)
		rp := bucket.RetentionPolicy(bucket.DefaultRetentionPolicy)
		b.AppendString(0, bucket.Name)
		b.AppendString(1, "")
		b.AppendString(2, "influxdb")
		b.AppendString(3, "")
		b.AppendString(4, rp.Name)
		b.AppendInt(5, rp.Duration.Nanoseconds())
	}

	return b.Table()
}

func createBucketsSource(prSpec plan.ProcedureSpec, dsid execute.DatasetID, a execute.Administration) (execute.Source, error) {
	_, ok := prSpec.(*inputs.BucketsProcedureSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", prSpec)
	}

	// the dependencies used for FromKind are adequate for what we need here
	// so there's no need to inject custom dependencies for buckets()
	deps := a.Dependencies()[inputs.BucketsKind].(BucketDependencies)

	bd := &BucketsDecoder{deps: deps, alloc: a.Allocator()}

	return inputs.CreateSourceFromDecoder(bd, dsid, a)

}

type BucketDependencies *storage.Store

func InjectBucketDependencies(depsMap execute.Dependencies, deps BucketDependencies) error {
	if deps == nil {
		return errors.New("bucket store dependency")
	}
	depsMap[inputs.BucketsKind] = deps
	return nil
}
