package storageflux_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/mock"
	datagen "github.com/influxdata/influxdb/v2/pkg/data/gen"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/storage"
	storageflux "github.com/influxdata/influxdb/v2/storage/flux"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	storagev1 "github.com/influxdata/influxdb/v2/v1/services/storage"
	"github.com/stretchr/testify/require"
)

// MultiShardSetupFunc returns a generator spec plus the overall time range to
// generate. Unlike SetupFunc it hands back the spec rather than a built
// generator, because NewMultiShardStorageReader needs one generator per shard
// group.
type MultiShardSetupFunc func(org, bucket platform.ID) (*datagen.Spec, datagen.TimeRange)

// NewMultiShardStorageReader is NewStorageReader with a shard group duration
// short enough that the data range spans several shard groups. Every series is
// written into every group, so a single series spans multiple shards and
// row.Query carries one cursor iterator per shard.
//
// That is what makes floatMultiShardArrayCursor.nextArrayCursor reachable: it
// returns early while len(c.itrs) == 0, so with the single-shard harness the
// consumer goroutine never closes a KeyCursor and the refcount is never
// double-released.
//
// This duplicates NewStorageReader's setup rather than refactoring it, to keep
// the ~3900 lines of existing tests in table_test.go untouched.
func NewMultiShardStorageReader(tb testing.TB, shardGroupDuration time.Duration, setupFn MultiShardSetupFunc) *StorageReader {
	tb.Helper()

	rootDir := tb.TempDir()

	var closers []closerFunc
	closeAll := func() {
		for _, c := range closers {
			c()
		}
	}

	kvStore := inmem.NewKVStore()
	require.NoError(tb, kvStore.CreateBucket(context.Background(), meta.BucketName))

	metaClient := meta.NewClient(meta.NewConfig(), kvStore)
	require.NoError(tb, metaClient.Open())
	closers = append(closers, func() {
		if err := metaClient.Close(); err != nil {
			tb.Errorf("close meta client: %s", err)
		}
	})

	idgen := mock.NewMockIDGenerator()
	org, bucket := idgen.ID(), idgen.ID()

	spec, tr := setupFn(org, bucket)

	rp := &meta.RetentionPolicySpec{
		Name:               meta.DefaultRetentionPolicyName,
		ShardGroupDuration: shardGroupDuration,
	}
	if _, err := metaClient.CreateDatabaseWithRetentionPolicy(bucket.String(), rp); err != nil {
		closeAll()
		tb.Fatalf("failed to create database: %s", err)
	}

	enginePath := filepath.Join(rootDir, "engine")
	dbPath := filepath.Join(enginePath, "data", bucket.String())
	if err := os.MkdirAll(dbPath, 0700); err != nil {
		closeAll()
		tb.Fatalf("failed to create data directory: %s", err)
	}

	sfile := tsdb.NewSeriesFile(filepath.Join(dbPath, tsdb.SeriesFileDirectory))
	if err := sfile.Open(); err != nil {
		closeAll()
		tb.Fatalf("failed to open series file: %s", err)
	}
	defer sfile.Close()
	sfile.DisableCompactions()

	shardPath := filepath.Join(dbPath, rp.Name)

	// Walk the range one shard group at a time, writing the slice of the
	// series that falls inside each group.
	shards := 0
	for cur := tr.Start; cur.Before(tr.End); {
		sgi, err := metaClient.CreateShardGroup(bucket.String(), rp.Name, cur)
		if err != nil {
			closeAll()
			tb.Fatalf("failed to create shard group at %s: %s", cur, err)
		}

		groupRange := datagen.TimeRange{
			Start: maxTime(sgi.StartTime, tr.Start),
			End:   minTime(sgi.EndTime, tr.End),
		}

		id := sgi.Shards[0].ID
		if err := os.MkdirAll(filepath.Join(shardPath, strconv.FormatUint(id, 10)), 0700); err != nil {
			closeAll()
			tb.Fatalf("failed to create shard directory: %s", err)
		}
		sg := datagen.NewSeriesGeneratorFromSpec(spec, groupRange)
		if err := writeShard(sfile, sg, id, shardPath); err != nil {
			closeAll()
			tb.Fatalf("failed to write shard %d: %s", id, err)
		}

		shards++
		cur = sgi.EndTime
	}
	require.Greater(tb, shards, 1, "harness must produce more than one shard")
	tb.Logf("wrote %d shards", shards)

	for i, p := range sfile.Partitions() {
		c := tsdb.NewSeriesPartitionCompactor()
		if err := c.Compact(p); err != nil {
			closeAll()
			tb.Fatalf("failed to compact series file %d: %s", i, err)
		}
	}
	if err := sfile.Close(); err != nil {
		closeAll()
		tb.Fatalf("failed to close series file: %s", err)
	}

	engine := storage.NewEngine(enginePath, storage.NewConfig(), storage.WithMetaClient(metaClient))
	if err := engine.Open(context.Background()); err != nil {
		closeAll()
		tb.Fatalf("failed to open storage engine: %s", err)
	}
	closers = append(closers, func() {
		if err := engine.Close(); err != nil {
			tb.Errorf("close engine: %s", err)
		}
	})

	store := storagev1.NewStore(engine.TSDBStore(), engine.MetaClient())
	return &StorageReader{
		Org:    org,
		Bucket: bucket,
		Bounds: execute.Bounds{
			Start: values.ConvertTime(tr.Start),
			Stop:  values.ConvertTime(tr.End),
		},
		Close:         closeAll,
		StorageReader: storageflux.NewReader(store),
	}
}

type closerFunc func()

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// TestStorageReader_ReadFilter_CancelDuringDeferredConsume shows that
// storage/flux lets two goroutines close the same cursor chain concurrently,
// which is the reachability half of the "sync: negative WaitGroup counter"
// panics in EAR-6049 and EAR-7019. The other half - that a concurrent
// KeyCursor.Close double-releases its locations and drives TSMReader.refsWG
// negative - is proved directly by TestKeyCursor_ConcurrentClose in
// tsdb/engine/tsm1.
//
// storage/flux hands each table to the caller and then waits for the table to
// signal completion, but on context cancellation it abandons that wait
// (reader.go, filterIterator.handleRead and groupIterator.handleRead):
//
//	select {
//	case <-done:
//	case <-fi.ctx.Done():
//	    table.Cancel()
//	    break READ
//	}
//
// table.Cancel only sets an atomic flag, which table.do checks between
// iterations, so it does not stop an in-flight advance(). handleRead's deferred
// cleanup then calls table.Close() while the consumer is still inside advance()
// on another goroutine. floatTable guards t.cur with t.mu in Close() but not in
// advance(), so the mutex protects only one side.
//
// That interleaving is real because flux does not consume tables inline:
// consecutiveTransport queues the table and a poolDispatcher goroutine calls
// Do() later. This test models that handoff - the Do callback starts a
// goroutine and returns immediately - and cancels the context asynchronously,
// which is what a query timeout, a client disconnect, a response size limit, or
// an aborted join branch does in production. EAR-6049's stacks are exactly this
// shape: poolDispatcher.doWork -> ... -> consecutiveTransportTable.Do ->
// floatTable.do -> floatTable.advance.
//
// The series here span several shards so that advance() reaches
// floatMultiShardArrayCursor.nextArrayCursor, which closes the cursor chain
// itself; with a single shard it returns early on len(c.itrs) == 0 and the
// consumer never closes anything.
//
// Observed under -race: writes in floatArrayAscendingCursor.Close against reads
// in its Next, and a write of t.cur against advance()'s read of it. It also
// faults outright, because Close clears c.tsm.keyCursor while nextTSM() is
// dereferencing it. Finally, recovering those panics - as flux's dispatcher does
// in production - leaves the release loop half finished, so references leak and
// shutdown parks in TSMReader.refsWG.Wait(); that is the reported hang.
//
// This test does not by itself produce the negative counter. That needs
// table.Close() to land inside one of the few brief nextArrayCursor() calls per
// table rather than anywhere in Next(), a much narrower window. See
// TestKeyCursor_ConcurrentClose for the deterministic version.
func TestStorageReader_ReadFilter_CancelDuringDeferredConsume(t *testing.T) {
	// One shard group per hour across six hours, so every series is spread
	// over six shards.
	reader := NewMultiShardStorageReader(t, time.Hour, func(org, bucket platform.ID) (*datagen.Spec, datagen.TimeRange) {
		spec := Spec(org, bucket,
			MeasurementSpec("m0",
				FloatArrayValuesSequence("f0", time.Second, []float64{1.0, 2.0, 3.0, 4.0}),
				TagValuesSequence("t0", "a-%s", 0, 50),
			),
		)
		tr := TimeRange("2019-11-25T00:00:00Z", "2019-11-25T06:00:00Z")
		return spec, tr
	})
	// Not deferred directly: aborting KeyCursor.Close's release loop leaks
	// references, so FileStore.Close blocks forever in TSMReader.refsWG.Wait().
	// That hang is itself one of the reported symptoms, so bound it and report
	// it rather than letting it fail the run as a timeout.
	defer func() {
		closed := make(chan struct{})
		go func() {
			defer close(closed)
			reader.Close()
		}()
		select {
		case <-closed:
		case <-time.After(15 * time.Second):
			t.Log("SHUTDOWN HANG: reader.Close did not return; TSMReader.Close is " +
				"parked in refsWG.Wait() on leaked references")
		}
	}()

	var tables, buffers int64

	// Flux recovers panics raised on dispatcher goroutines
	// (execute.poolDispatcher.recover), which is why these incidents present as
	// persistent query failure rather than a crashed process. Recovering here
	// is therefore faithful to production, and it also lets the run continue
	// past the first faulting interleaving so the rarer refcount corruption can
	// surface instead of the process dying on a nil dereference.
	var mu sync.Mutex
	panics := make(map[string]int)
	recordPanic := func(r any) {
		mu.Lock()
		defer mu.Unlock()
		panics[fmt.Sprint(r)]++
	}

	// The double-release window is narrower than the nil-dereference one: both
	// goroutines must read c.seeks as non-nil before either clears it. So run
	// many attempts, in parallel, sweeping the cancellation offset so it lands
	// at many different points inside the read.
	const (
		workers = 8
		rounds  = 250
	)
	attempt := func(worker, round int) {
		alloc := &memory.ResourceAllocator{Allocator: arrowmem.DefaultAllocator}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ti, err := reader.ReadFilter(ctx, query.ReadFilterSpec{
			OrganizationID: reader.Org,
			BucketID:       reader.Bucket,
			Bounds:         reader.Bounds,
		}, alloc)
		if err != nil {
			recordPanic(fmt.Sprintf("ReadFilter: %v", err))
			return
		}

		var wg sync.WaitGroup
		defer wg.Wait()

		// Stagger per worker as well as per round so the offsets interleave.
		go func() {
			time.Sleep(time.Duration(round*20+worker*7) * time.Microsecond)
			cancel()
		}()

		// Deliberately ignored: cancelling mid-read is expected to surface an
		// error here. The failure this test looks for is a race report or a
		// panic, not a returned error.
		_ = ti.Do(func(tbl flux.Table) error {
			atomic.AddInt64(&tables, 1)
			wg.Go(func() {
				defer func() {
					if r := recover(); r != nil {
						recordPanic(r)
					}
				}()
				_ = tbl.Do(func(flux.ColReader) error {
					atomic.AddInt64(&buffers, 1)
					return nil
				})
			})
			return nil
		})
	}

	var workerWG sync.WaitGroup
	for w := range workers {
		workerWG.Go(func() {
			for r := range rounds {
				attempt(w, r)
			}
		})
	}
	workerWG.Wait()

	t.Logf("consumed tables=%d buffers=%d", atomic.LoadInt64(&tables), atomic.LoadInt64(&buffers))
	mu.Lock()
	defer mu.Unlock()
	for msg, n := range panics {
		t.Logf("recovered %4d x %s", n, msg)
	}
}
