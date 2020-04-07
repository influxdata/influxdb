package storage

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/prom/promtest"
	"github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/toml"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/tsm1"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

func TestEngine_runRetentionEnforcer(t *testing.T) {
	t.Parallel()
	c := NewConfig()
	c.RetentionInterval = toml.Duration(time.Second)
	log := zap.NewNop()
	if testing.Verbose() {
		log = logger.New(os.Stdout)
	}

	t.Run("no limiter", func(t *testing.T) {
		t.Parallel()

		path := MustTempDir()
		defer os.RemoveAll(path)

		var runner MockRunner
		engine := NewEngine(path, c, WithNodeID(100), WithEngineID(30))
		engine.retentionEnforcer = &runner

		done := make(chan struct{})
		runner.runf = func() {
			close(done)
		}

		if err := engine.Open(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer engine.Close()

		timer := time.NewTimer(5 * time.Second)
		select {
		case <-timer.C:
			t.Fatal("Test timed out")
		case <-done:
			return
		}
	})

	t.Run("close during limit", func(t *testing.T) {
		t.Parallel()

		path := MustTempDir()
		defer os.RemoveAll(path)

		// close(running)
		// time.Sleep(time.Minute)
		blocked := make(chan struct{})
		limiter := func() func() {
			close(blocked)
			time.Sleep(time.Hour) // block forever
			return func() {}
		}

		engine := NewEngine(path, c, WithNodeID(101), WithEngineID(32), WithRetentionEnforcerLimiter(limiter))

		var runner MockRunner
		engine.retentionEnforcer = &runner

		done := make(chan struct{})
		runner.runf = func() {
			close(done)
		}

		if err := engine.Open(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer engine.Close()

		select {
		case <-blocked: // Now we are stuck waiting on the limiter
			// Close the service early. We should return
			if err := engine.Close(); err != nil {
				t.Fatal(err)
			}
		case <-done:
			return
		}
	})

	t.Run("limiter", func(t *testing.T) {
		t.Parallel()

		path := MustTempDir()
		defer os.RemoveAll(path)

		var mu sync.Mutex
		limiter := func() func() {
			mu.Lock()
			return func() { mu.Unlock() }
		}

		engine1 := NewEngine(path, c, WithNodeID(2), WithEngineID(1), WithRetentionEnforcerLimiter(limiter))
		engine1.WithLogger(log)
		engine2 := NewEngine(path, c, WithNodeID(3), WithEngineID(2), WithRetentionEnforcerLimiter(limiter))
		engine2.WithLogger(log)

		var runner1, runner2 MockRunner
		engine1.retentionEnforcer = &runner1
		engine2.retentionEnforcer = &runner2

		var running int64
		errCh := make(chan error, 2)

		runner1.runf = func() {
			x := atomic.AddInt64(&running, 1)
			if x > 1 {
				errCh <- errors.New("runner 1 ran concurrently with runner 2")
				return
			}

			time.Sleep(time.Second) //Running retention

			atomic.AddInt64(&running, -1)
			runner1.runf = func() {} // Don't run again.
			errCh <- nil
		}

		runner2.runf = func() {
			x := atomic.AddInt64(&running, 1)
			if x > 1 {
				errCh <- errors.New("runner 2 ran concurrently with runner 1")
				return
			}

			time.Sleep(time.Second) //Running retention

			atomic.AddInt64(&running, -1)
			runner2.runf = func() {} // Don't run again.
			errCh <- nil
		}

		if err := engine1.Open(context.Background()); err != nil {
			t.Fatal(err)
		} else if err := engine2.Open(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer engine1.Close()
		defer engine2.Close()

		for i := 0; i < 2; i++ {
			if err := <-errCh; err != nil {
				t.Fatal(err)
			}
		}
	})
}

func TestRetentionService(t *testing.T) {
	t.Parallel()
	engine := NewTestEngine()
	service := newRetentionEnforcer(engine, &TestSnapshotter{}, NewTestBucketFinder())
	now := time.Date(2018, 4, 10, 23, 12, 33, 0, time.UTC)

	t.Run("no buckets", func(t *testing.T) {
		service.expireData(context.Background(), nil, now)
		service.expireData(context.Background(), []*influxdb.Bucket{}, now)
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
	engine.DeleteBucketRangeFn = func(ctx context.Context, orgID, bucketID influxdb.ID, from, to int64) error {
		if from != math.MinInt64 {
			t.Fatalf("got from %d, expected %d", from, int64(math.MinInt64))
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
		service.expireData(context.Background(), buckets, now)
		if !reflect.DeepEqual(gotMatched, expMatched) {
			t.Fatalf("got\n%#v\nexpected\n%#v", gotMatched, expMatched)
		}
	})
}

func TestMetrics_Retention(t *testing.T) {
	t.Parallel()
	// metrics to be shared by multiple file stores.
	metrics := newRetentionMetrics(prometheus.Labels{"engine_id": "", "node_id": ""})

	t1 := newRetentionTracker(metrics, prometheus.Labels{"engine_id": "0", "node_id": "0"})
	t2 := newRetentionTracker(metrics, prometheus.Labels{"engine_id": "1", "node_id": "0"})

	reg := prometheus.NewRegistry()
	reg.MustRegister(metrics.PrometheusCollectors()...)

	base := namespace + "_" + retentionSubsystem + "_"

	// Generate some measurements.
	for _, tracker := range []*retentionTracker{t1, t2} {
		tracker.IncChecks(true)
		tracker.IncChecks(false)
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
		{"engine_id": "0", "node_id": "0"},
		{"engine_id": "1", "node_id": "0"},
	}

	for i, labels := range labelVariants {
		for _, status := range []string{"ok", "error"} {
			labels["status"] = status

			name := base + "checks_total"
			metric := promtest.MustFindMetric(t, mfs, name, labels)
			if got, exp := metric.GetCounter().GetValue(), float64(1); got != exp {
				t.Errorf("[%s %d %v] got %v, expected %v", name, i, labels, got, exp)
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

type MockRunner struct {
	runf func()
}

func (r *MockRunner) run() {
	if r.runf == nil {
		return
	}
	r.runf()
}

type TestEngine struct {
	DeleteBucketRangeFn func(context.Context, influxdb.ID, influxdb.ID, int64, int64) error
}

func NewTestEngine() *TestEngine {
	return &TestEngine{
		DeleteBucketRangeFn: func(context.Context, influxdb.ID, influxdb.ID, int64, int64) error { return nil },
	}
}

func (e *TestEngine) DeleteBucketRange(ctx context.Context, orgID, bucketID influxdb.ID, min, max int64) error {
	return e.DeleteBucketRangeFn(ctx, orgID, bucketID, min, max)
}

type TestSnapshotter struct{}

func (s *TestSnapshotter) WriteSnapshot(ctx context.Context, status tsm1.CacheStatus) error {
	return nil
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

func MustTempDir() string {
	dir, err := ioutil.TempDir("", "storage-engine-test")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp dir: %v", err))
	}
	return dir
}
