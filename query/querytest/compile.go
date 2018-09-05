package querytest

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/semantic/semantictest"
	"github.com/influxdata/platform"
	"fmt"
)

type NewQueryTestCase struct {
	Name             string
	Raw              string
	Want             *query.Spec
	WantErr          bool
	WantReadBuckets  *[]platform.BucketFilter
	WantWriteBuckets *[]platform.BucketFilter
}

var opts = append(
	semantictest.CmpOptions,
	cmp.AllowUnexported(query.Spec{}),
	cmp.AllowUnexported(functions.JoinOpSpec{}),
	cmpopts.IgnoreUnexported(query.Spec{}),
	cmpopts.IgnoreUnexported(functions.JoinOpSpec{}),
)

func NewQueryTestHelper(t *testing.T, tc NewQueryTestCase) {
	t.Helper()

	now := time.Now().UTC()
	got, err := query.Compile(context.Background(), tc.Raw, now)
	if (err != nil) != tc.WantErr {
		t.Errorf("query.NewQuery() error = %v, wantErr %v", err, tc.WantErr)
		return
	}
	if tc.WantErr {
		return
	}
	if tc.Want != nil {
		tc.Want.Now = now
		if !cmp.Equal(tc.Want, got, opts...) {
			t.Errorf("query.NewQuery() = -want/+got %s", cmp.Diff(tc.Want, got, opts...))
		}
	}

	var gotReadBuckets, gotWriteBuckets []platform.BucketFilter
	if tc.WantReadBuckets != nil || tc.WantWriteBuckets != nil {
		gotReadBuckets, gotWriteBuckets, err = got.BucketsAccessed()
	}

	if tc.WantReadBuckets != nil {
		if diagnostic := verifyBuckets(*tc.WantReadBuckets, gotReadBuckets); diagnostic != "" {
			t.Errorf("Could not verify read buckets: %v", diagnostic)
		}
	}

	if tc.WantWriteBuckets != nil {
		if diagnostic := verifyBuckets(*tc.WantWriteBuckets, gotWriteBuckets); diagnostic != "" {
			t.Errorf("Could not verify write buckets: %v", diagnostic)
		}
	}
}

func verifyBuckets(wantBuckets, gotBuckets []platform.BucketFilter) string {
	if len(wantBuckets) != len(gotBuckets) {
		return fmt.Sprintf("Expected %v buckets but got %v", len(wantBuckets), len(gotBuckets))
	}

	for i, wantBucket := range wantBuckets {
		if diagnostic := cmp.Diff(wantBucket, gotBuckets[i]); diagnostic != "" {
			return fmt.Sprintf("Bucket mismatch: -want/+got:\n%v", diagnostic)
		}
	}

	return ""
}
