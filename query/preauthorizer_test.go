package query_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/flux"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/errors"
	"github.com/influxdata/influxdb/mock"
	"github.com/influxdata/influxdb/query"
	_ "github.com/influxdata/influxdb/query/builtin"
)

func newBucketServiceWithOneBucket(bucket platform.Bucket) platform.BucketService {
	bs := mock.NewBucketService()
	bs.FindBucketFn = func(ctx context.Context, bucketFilter platform.BucketFilter) (*platform.Bucket, error) {
		if *bucketFilter.Name == bucket.Name {
			return &bucket, nil
		}

		return nil, errors.New("unknown bucket")
	}

	return bs
}

func TestPreAuthorizer_PreAuthorize(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	// fresh pre-authorizer
	auth := &platform.Authorization{Status: platform.Active}
	emptyBucketService := mock.NewBucketService()
	preAuthorizer := query.NewPreAuthorizer(emptyBucketService)

	// Try to pre-authorize invalid bucketID
	q := `from(bucketID:"invalid") |> range(start:-2h) |> yield()`
	spec, err := flux.Compile(ctx, q, now)
	if err != nil {
		t.Fatalf("Error compiling query: %v", err)
	}
	err = preAuthorizer.PreAuthorize(ctx, spec, auth)
	if diagnostic := cmp.Diff("bucket service returned nil bucket", err.Error()); diagnostic != "" {
		t.Errorf("Authorize message mismatch: -want/+got:\n%v", diagnostic)
	}

	// Try to pre-authorize a valid from with bucket service with no buckets
	// and no authorization
	q = `from(bucket:"my_bucket") |> range(start:-2h) |> yield()`
	spec, err = flux.Compile(ctx, q, now)
	if err != nil {
		t.Fatalf("Error compiling query: %v", err)
	}
	err = preAuthorizer.PreAuthorize(ctx, spec, auth)
	if diagnostic := cmp.Diff("bucket service returned nil bucket", err.Error()); diagnostic != "" {
		t.Errorf("Authorize message mismatch: -want/+got:\n%v", diagnostic)
	}

	// Try to authorize with a bucket service that knows about one bucket
	// (still no authorization)
	bucketID, err := platform.IDFromString("deadbeefdeadbeef")
	if err != nil {
		t.Fatal(err)
	}
	orgID := platform.ID(1)
	bucketService := newBucketServiceWithOneBucket(platform.Bucket{
		Name:           "my_bucket",
		ID:             *bucketID,
		OrganizationID: orgID,
	})

	preAuthorizer = query.NewPreAuthorizer(bucketService)
	err = preAuthorizer.PreAuthorize(ctx, spec, auth)
	if diagnostic := cmp.Diff(`no read permission for bucket: "my_bucket"`, err.Error()); diagnostic != "" {
		t.Errorf("Authorize message mismatch: -want/+got:\n%v", diagnostic)
	}

	p, err := platform.NewPermissionAtID(*bucketID, platform.ReadAction, platform.BucketsResourceType, orgID)
	if err != nil {
		t.Fatalf("Error creating read bucket permission query: %v", err)
	}
	// Try to authorize with read permission on bucket
	auth = &platform.Authorization{
		Status:      platform.Active,
		Permissions: []platform.Permission{*p},
	}

	err = preAuthorizer.PreAuthorize(ctx, spec, auth)
	if err != nil {
		t.Errorf("Expected successful authorization, but got error: \"%v\"", err.Error())
	}
}
