package influxdb_test

import (
	"context"
	"testing"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/query/stdlib/influxdata/influxdb"
	"github.com/stretchr/testify/require"
)

// recordingBucketLookup returns a BucketLookup that accepts the bucket named
// "my-bucket" and the bucket with ID accepted (if valid) and records the
// arguments of each call.
func recordingBucketLookup(accepted platform.ID, calls *[]string, orgs *[]platform.ID) mock.BucketLookupFn {
	return mock.BucketLookupFn{
		LookupFn: func(_ context.Context, orgID platform.ID, name string) (platform.ID, bool) {
			*calls = append(*calls, "Lookup")
			*orgs = append(*orgs, orgID)
			if name == "my-bucket" {
				return bucketID, true
			}
			return platform.InvalidID(), false
		},
		LookupByIDFn: func(_ context.Context, orgID platform.ID, id platform.ID) (platform.ID, bool) {
			*calls = append(*calls, "LookupByID")
			*orgs = append(*orgs, orgID)
			if accepted.Valid() && id == accepted {
				return id, true
			}
			return platform.InvalidID(), false
		},
	}
}

func TestReadRangePhysSpec_LookupBucketID(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		spec      influxdb.ReadRangePhysSpec
		accepted  platform.ID
		wantID    platform.ID
		wantCode  codes.Code
		wantCalls []string
	}{
		{
			name:      "name found",
			spec:      influxdb.ReadRangePhysSpec{Bucket: "my-bucket"},
			wantID:    bucketID,
			wantCalls: []string{"Lookup"},
		},
		{
			name:      "name not found",
			spec:      influxdb.ReadRangePhysSpec{Bucket: "other-bucket"},
			wantCode:  codes.NotFound,
			wantCalls: []string{"Lookup"},
		},
		{
			name:      "id accepted",
			spec:      influxdb.ReadRangePhysSpec{BucketID: platform.ID(42).String()},
			accepted:  42,
			wantID:    42,
			wantCalls: []string{"LookupByID"},
		},
		{
			name:      "id rejected",
			spec:      influxdb.ReadRangePhysSpec{BucketID: platform.ID(42).String()},
			accepted:  bucketID,
			wantCode:  codes.NotFound,
			wantCalls: []string{"LookupByID"},
		},
		{
			name:     "id malformed",
			spec:     influxdb.ReadRangePhysSpec{BucketID: "not-an-id"},
			accepted: bucketID,
			wantCode: codes.Invalid,
		},
		{
			name:     "neither name nor id",
			spec:     influxdb.ReadRangePhysSpec{},
			wantCode: codes.Invalid,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var calls []string
			var orgs []platform.ID
			lookup := recordingBucketLookup(tc.accepted, &calls, &orgs)

			id, err := tc.spec.LookupBucketID(context.Background(), orgID, lookup)
			require.Equal(t, tc.wantCalls, calls)
			for _, o := range orgs {
				require.Equal(t, orgID, o, "lookup must be scoped to the request org")
			}

			if tc.wantCode != codes.Inherit {
				require.Equal(t, tc.wantCode, flux.ErrorCode(err))
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantID, id)
		})
	}
}
