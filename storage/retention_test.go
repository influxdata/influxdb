package storage

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"testing"
	"time"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
)

func TestService_expireData(t *testing.T) {
	engine := NewTestEngine()
	service := newRetentionEnforcer(engine, NewTestBucketFinder())
	now := time.Date(2018, 4, 10, 23, 12, 33, 0, time.UTC)

	t.Run("no rpByBucketID", func(t *testing.T) {
		if err := service.expireData(nil, now); err != nil {
			t.Error(err)
		}

		if err := service.expireData(map[platform.ID]time.Duration{}, now); err != nil {
			t.Error(err)
		}
	})

	// Generate some measurement names
	var names [][]byte
	rpByBucketID := map[platform.ID]time.Duration{}
	expMatchedFrequencies := map[string]int{}  // To be used for verifying test results.
	expRejectedFrequencies := map[string]int{} // To be used for verifying test results.
	for i := 0; i < 15; i++ {
		repeat := rand.Intn(10) + 1 // [1, 10]
		name := genMeasurementName()
		for j := 0; j < repeat; j++ {
			names = append(names, name)
		}

		var n [16]byte
		copy(n[:], name)
		_, bucketID := tsdb.DecodeName(n)

		// Put 1/3rd in the rpByBucketID into the set to delete and 1/3rd into the set
		// to not delete because no rp, and 1/3rd into the set to not delete because 0 rp.
		if i%3 == 0 {
			rpByBucketID[bucketID] = 3 * time.Hour
			expMatchedFrequencies[string(name)] = repeat
		} else if i%3 == 1 {
			expRejectedFrequencies[string(name)] = repeat
		} else if i%3 == 2 {
			rpByBucketID[bucketID] = 0
			expRejectedFrequencies[string(name)] = repeat
		}
	}

	// Add a badly formatted measurement.
	for i := 0; i < 5; i++ {
		names = append(names, []byte("zyzwrong"))
	}
	expRejectedFrequencies["zyzwrong"] = 5

	gotMatchedFrequencies := map[string]int{}
	gotRejectedFrequencies := map[string]int{}
	engine.DeleteSeriesRangeWithPredicateFn = func(_ tsdb.SeriesIterator, fn func([]byte, models.Tags) (int64, int64, bool)) error {

		// Iterate over the generated names updating the frequencies by which
		// the predicate function in expireData matches or rejects them.
		for _, name := range names {
			from, to, shouldDelete := fn(name, nil)
			if shouldDelete {
				gotMatchedFrequencies[string(name)]++
				if from != math.MinInt64 {
					return fmt.Errorf("got from %d, expected %d", from, math.MinInt64)
				}
				wantTo := now.Add(-3 * time.Hour).UnixNano()
				if to != wantTo {
					return fmt.Errorf("got to %d, expected %d", to, wantTo)
				}
			} else {
				gotRejectedFrequencies[string(name)]++
			}
		}
		return nil
	}

	t.Run("multiple bucket", func(t *testing.T) {
		if err := service.expireData(rpByBucketID, now); err != nil {
			t.Error(err)
		}

		// Verify that the correct series were marked to be deleted.
		t.Run("matched", func(t *testing.T) {
			if !reflect.DeepEqual(gotMatchedFrequencies, expMatchedFrequencies) {
				t.Fatalf("got\n%#v\nexpected\n%#v", gotMatchedFrequencies, expMatchedFrequencies)
			}
		})

		t.Run("rejected", func(t *testing.T) {
			// Verify that badly formatted measurements were rejected.
			if !reflect.DeepEqual(gotRejectedFrequencies, expRejectedFrequencies) {
				t.Fatalf("got\n%#v\nexpected\n%#v", gotRejectedFrequencies, expRejectedFrequencies)
			}
		})
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

type TestSeriesCursor struct {
	CloseFn func() error
	NextFn  func() (*SeriesCursorRow, error)
}

func (f *TestSeriesCursor) Close() error                    { return f.CloseFn() }
func (f *TestSeriesCursor) Next() (*SeriesCursorRow, error) { return f.NextFn() }

type TestEngine struct {
	CreateSeriesCursorFn             func(context.Context, SeriesCursorRequest, influxql.Expr) (SeriesCursor, error)
	DeleteSeriesRangeWithPredicateFn func(tsdb.SeriesIterator, func([]byte, models.Tags) (int64, int64, bool)) error

	SeriesCursor *TestSeriesCursor
}

func NewTestEngine() *TestEngine {
	cursor := &TestSeriesCursor{
		CloseFn: func() error { return nil },
		NextFn:  func() (*SeriesCursorRow, error) { return nil, nil },
	}

	return &TestEngine{
		SeriesCursor:                     cursor,
		CreateSeriesCursorFn:             func(context.Context, SeriesCursorRequest, influxql.Expr) (SeriesCursor, error) { return cursor, nil },
		DeleteSeriesRangeWithPredicateFn: func(tsdb.SeriesIterator, func([]byte, models.Tags) (int64, int64, bool)) error { return nil },
	}
}

func (e *TestEngine) CreateSeriesCursor(ctx context.Context, req SeriesCursorRequest, cond influxql.Expr) (SeriesCursor, error) {
	return e.CreateSeriesCursorFn(ctx, req, cond)
}

func (e *TestEngine) DeleteSeriesRangeWithPredicate(itr tsdb.SeriesIterator, fn func([]byte, models.Tags) (int64, int64, bool)) error {
	return e.DeleteSeriesRangeWithPredicateFn(itr, fn)
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
