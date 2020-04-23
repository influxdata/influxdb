package query

import (
	"context"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/lang"
	platform "github.com/influxdata/influxdb/v2"
)

// BucketAwareOperationSpec specifies an operation that reads or writes buckets
type BucketAwareOperationSpec interface {
	BucketsAccessed(orgID *platform.ID) (readBuckets, writeBuckets []platform.BucketFilter)
}

type constantSecretService struct{}

func (s constantSecretService) LoadSecret(ctx context.Context, k string) (string, error) {
	return "", nil
}

func newDeps() flux.Dependencies {
	deps := flux.NewDefaultDependencies()
	deps.Deps.HTTPClient = nil
	deps.Deps.URLValidator = nil
	deps.Deps.SecretService = constantSecretService{}
	return deps
}

// BucketsAccessed returns the set of buckets read and written by a query spec
func BucketsAccessed(ast *ast.Package, orgID *platform.ID) (readBuckets, writeBuckets []platform.BucketFilter, err error) {
	ctx := newDeps().Inject(context.Background())
	err = lang.WalkIR(ctx, ast, func(o *flux.Operation) error {
		bucketAwareOpSpec, ok := o.Spec.(BucketAwareOperationSpec)
		if ok {
			opBucketsRead, opBucketsWritten := bucketAwareOpSpec.BucketsAccessed(orgID)
			readBuckets = append(readBuckets, opBucketsRead...)
			writeBuckets = append(writeBuckets, opBucketsWritten...)
		}
		return nil
	})

	if err != nil {
		return nil, nil, err
	}

	return readBuckets, writeBuckets, nil
}
