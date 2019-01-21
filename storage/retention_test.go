package storage

import (
	"context"
	"math"
	"math/rand"
	"reflect"
	"testing"
	"time"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/tsdb"
)

func TestRetentionService(t *testing.T) {
	engine := NewTestEngine()
	service := newRetentionEnforcer(engine, NewTestBucketFinder())
	now := time.Date(2018, 4, 10, 23, 12, 33, 0, time.UTC)

	t.Run("no buckets", func(t *testing.T) {
		if err := service.expireData(nil, now); err != nil {
			t.Error(err)
		}
		if err := service.expireData([]*platform.Bucket{}, now); err != nil {
			t.Error(err)
		}
	})

	// Generate some buckets to expire
	buckets := []*platform.Bucket{}
	expMatched := map[string]struct{}{}  // To be used for verifying test results.
	expRejected := map[string]struct{}{} // To be used for verifying test results.
	for i := 0; i < 15; i++ {
		name := genMeasurementName()

		var n [16]byte
		copy(n[:], name)
		orgID, bucketID := tsdb.DecodeName(n)

		// Put 1/3rd in the rpByBucketID into the set to delete and 1/3rd into the set
		// to not delete because no rp, and 1/3rd into the set to not delete because 0 rp.
		if i%3 == 0 {
			buckets = append(buckets, &platform.Bucket{
				OrganizationID:  orgID,
				ID:              bucketID,
				RetentionPeriod: 3 * time.Hour,
			})
			expMatched[string(name)] = struct{}{}
		} else if i%3 == 1 {
			expRejected[string(name)] = struct{}{}
		} else if i%3 == 2 {
			buckets = append(buckets, &platform.Bucket{
				OrganizationID:  orgID,
				ID:              bucketID,
				RetentionPeriod: 0,
			})
			expRejected[string(name)] = struct{}{}
		}
	}

	gotMatched := map[string]struct{}{}
	engine.DeleteBucketRangeFn = func(orgID, bucketID platform.ID, from, to int64) error {
		if from != math.MinInt64 {
			t.Fatalf("got from %d, expected %d", from, math.MinInt64)
		}
		wantTo := now.Add(-3 * time.Hour).UnixNano()
		if to != wantTo {
			t.Fatalf("got to %d, expected %d", to, wantTo)
		}

		name := tsdb.EncodeName(orgID, bucketID)
		if _, ok := expRejected[string(name[:])]; ok {
			t.Fatalf("got a delete for %x", name)
		}
		gotMatched[string(name[:])] = struct{}{}
		return nil
	}

	t.Run("multiple buckets", func(t *testing.T) {
		if err := service.expireData(buckets, now); err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(gotMatched, expMatched) {
			t.Fatalf("got\n%#v\nexpected\n%#v", gotMatched, expMatched)
		}
	})
}

// genMeasurementName generates a random measurement name or panics.
func genMeasurementName() []byte {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return b
}

type TestEngine struct {
	DeleteBucketRangeFn func(platform.ID, platform.ID, int64, int64) error
}

func NewTestEngine() *TestEngine {
	return &TestEngine{
		DeleteBucketRangeFn: func(platform.ID, platform.ID, int64, int64) error { return nil },
	}
}

func (e *TestEngine) DeleteBucketRange(orgID, bucketID platform.ID, min, max int64) error {
	return e.DeleteBucketRangeFn(orgID, bucketID, min, max)
}

type TestBucketFinder struct {
	FindBucketsFn func(context.Context, platform.BucketFilter, ...platform.FindOptions) ([]*platform.Bucket, int, error)
}

func NewTestBucketFinder() *TestBucketFinder {
	return &TestBucketFinder{
		FindBucketsFn: func(context.Context, platform.BucketFilter, ...platform.FindOptions) ([]*platform.Bucket, int, error) {
			return nil, 0, nil
		},
	}
}

func (f *TestBucketFinder) FindBuckets(ctx context.Context, filter platform.BucketFilter, opts ...platform.FindOptions) ([]*platform.Bucket, int, error) {
	return f.FindBucketsFn(ctx, filter, opts...)
}
