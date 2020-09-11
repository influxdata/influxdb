package querytest

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/flux"
	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/query"
)

type BucketsAccessedTestCase struct {
	Name             string
	Raw              string
	WantErr          bool
	WantReadBuckets  *[]platform.BucketFilter
	WantWriteBuckets *[]platform.BucketFilter
}

func BucketsAccessedTestHelper(t *testing.T, tc BucketsAccessedTestCase) {
	t.Helper()

	ast, err := flux.Parse(tc.Raw)
	if err != nil {
		t.Fatalf("could not parse flux: %v", err)
	}

	var gotReadBuckets, gotWriteBuckets []platform.BucketFilter
	if tc.WantReadBuckets != nil || tc.WantWriteBuckets != nil {
		gotReadBuckets, gotWriteBuckets, err = query.BucketsAccessed(ast, nil)
		if err != nil {
			t.Fatal(err)
		}
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
