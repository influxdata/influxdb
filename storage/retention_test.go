package storage

import (
	"context"
	"math"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/prom/promtest"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/prometheus/client_golang/prometheus"
)

func TestRetentionService(t *testing.T) {
	engine := NewTestEngine()
	service := newRetentionEnforcer(engine, NewTestBucketFinder())
	now := time.Date(2018, 4, 10, 23, 12, 33, 0, time.UTC)

	t.Run("no buckets", func(t *testing.T) {
		service.expireData(nil, now)
		service.expireData([]*influxdb.Bucket{}, now)
	})

	// Generate some buckets to expire
	buckets := []*influxdb.Bucket{}
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
			buckets = append(buckets, &influxdb.Bucket{
				OrgID:           orgID,
				ID:              bucketID,
				RetentionPeriod: 3 * time.Hour,
			})
			expMatched[string(name)] = struct{}{}
		} else if i%3 == 1 {
			expRejected[string(name)] = struct{}{}
		} else if i%3 == 2 {
			buckets = append(buckets, &influxdb.Bucket{
				OrgID:           orgID,
				ID:              bucketID,
				RetentionPeriod: 0,
			})
			expRejected[string(name)] = struct{}{}
		}
	}

	gotMatched := map[string]struct{}{}
	engine.DeleteBucketRangeFn = func(orgID, bucketID influxdb.ID, from, to int64) error {
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
		service.expireData(buckets, now)
		if !reflect.DeepEqual(gotMatched, expMatched) {
			t.Fatalf("got\n%#v\nexpected\n%#v", gotMatched, expMatched)
		}
	})
}

func TestMetrics_Retention(t *testing.T) {
	// metrics to be shared by multiple file stores.
	metrics := newRetentionMetrics(prometheus.Labels{"engine_id": "", "node_id": ""})

	t1 := newRetentionTracker(metrics, prometheus.Labels{"engine_id": "0", "node_id": "0"})
	t2 := newRetentionTracker(metrics, prometheus.Labels{"engine_id": "1", "node_id": "0"})

	reg := prometheus.NewRegistry()
	reg.MustRegister(metrics.PrometheusCollectors()...)

	base := namespace + "_" + retentionSubsystem + "_"

	// Generate some measurements.
	for i, tracker := range []*retentionTracker{t1, t2} {
		tracker.IncChecks(influxdb.ID(i+1), influxdb.ID(i+1), true)
		tracker.IncChecks(influxdb.ID(i+1), influxdb.ID(i+1), false)
		tracker.CheckDuration(time.Second, true)
		tracker.CheckDuration(time.Second, false)
	}

	// Test that all the correct metrics are present.
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}

	// The label variants for the two caches.
	labelVariants := []prometheus.Labels{
		prometheus.Labels{"engine_id": "0", "node_id": "0"},
		prometheus.Labels{"engine_id": "1", "node_id": "0"},
	}

	for i, labels := range labelVariants {
		for _, status := range []string{"ok", "error"} {
			labels["status"] = status

			l := make(prometheus.Labels, len(labels))
			for k, v := range labels {
				l[k] = v
			}
			l["org_id"] = influxdb.ID(i + 1).String()
			l["bucket_id"] = influxdb.ID(i + 1).String()

			name := base + "checks_total"
			metric := promtest.MustFindMetric(t, mfs, name, l)
			if got, exp := metric.GetCounter().GetValue(), float64(1); got != exp {
				t.Errorf("[%s %d %v] got %v, expected %v", name, i, l, got, exp)
			}

			name = base + "check_duration_seconds"
			metric = promtest.MustFindMetric(t, mfs, name, labels)
			if got, exp := metric.GetHistogram().GetSampleSum(), float64(1); got != exp {
				t.Errorf("[%s %d %v] got %v, expected %v", name, i, labels, got, exp)
			}
		}
	}
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
	DeleteBucketRangeFn func(influxdb.ID, influxdb.ID, int64, int64) error
}

func NewTestEngine() *TestEngine {
	return &TestEngine{
		DeleteBucketRangeFn: func(influxdb.ID, influxdb.ID, int64, int64) error { return nil },
	}
}

func (e *TestEngine) DeleteBucketRange(orgID, bucketID influxdb.ID, min, max int64) error {
	return e.DeleteBucketRangeFn(orgID, bucketID, min, max)
}

type TestBucketFinder struct {
	FindBucketsFn func(context.Context, influxdb.BucketFilter, ...influxdb.FindOptions) ([]*influxdb.Bucket, int, error)
}

func NewTestBucketFinder() *TestBucketFinder {
	return &TestBucketFinder{
		FindBucketsFn: func(context.Context, influxdb.BucketFilter, ...influxdb.FindOptions) ([]*influxdb.Bucket, int, error) {
			return nil, 0, nil
		},
	}
}

func (f *TestBucketFinder) FindBuckets(ctx context.Context, filter influxdb.BucketFilter, opts ...influxdb.FindOptions) ([]*influxdb.Bucket, int, error) {
	return f.FindBucketsFn(ctx, filter, opts...)
}
