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
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/mock"
	datagen "github.com/influxdata/influxdb/v2/pkg/data/gen"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/storage"
	storageflux "github.com/influxdata/influxdb/v2/storage/flux"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	storagev1 "github.com/influxdata/influxdb/v2/v1/services/storage"
	"github.com/stretchr/testify/require"
)

// MultiShardSetupFunc returns a generator spec plus the overall time range to
// generate. Unlike SetupFunc it hands back the spec rather than a built
// generator, because NewMultiShardStorageReader needs one generator per shard
// group.
type MultiShardSetupFunc func(org, bucket platform.ID) (*datagen.Spec, datagen.TimeRange)

// multiShardReader augments StorageReader with the handles needed to inspect
// TSM reader reference counts after a query.
type multiShardReader struct {
	*StorageReader
	tsdbStore storage.TSDBStore
	shardIDs  []uint64
}

// NewMultiShardStorageReader is NewStorageReader with a shard group duration
// short enough that the data range spans several shard groups. Every series is
// written into every group, so a single series spans multiple shards and
// row.Query carries one cursor iterator per shard.
//
// That is what makes floatMultiShardArrayCursor.nextArrayCursor reachable: it
// returns early while len(c.itrs) == 0, so with the single-shard harness the
// consumer goroutine never closes a KeyCursor.
//
// This duplicates NewStorageReader's setup rather than refactoring it, to keep
// the existing tests in table_test.go untouched.
func NewMultiShardStorageReader(tb testing.TB, shardGroupDuration time.Duration, setupFn MultiShardSetupFunc) *multiShardReader {
	tb.Helper()

	rootDir := tb.TempDir()

	var closers []func()
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
	var shardIDs []uint64
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
		if err := writeShard(sfile, datagen.NewSeriesGeneratorFromSpec(spec, groupRange), id, shardPath); err != nil {
			closeAll()
			tb.Fatalf("failed to write shard %d: %s", id, err)
		}

		shardIDs = append(shardIDs, id)
		cur = sgi.EndTime
	}
	require.Greater(tb, len(shardIDs), 1, "harness must produce more than one shard")

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
	return &multiShardReader{
		StorageReader: &StorageReader{
			Org:    org,
			Bucket: bucket,
			Bounds: execute.Bounds{
				Start: values.ConvertTime(tr.Start),
				Stop:  values.ConvertTime(tr.End),
			},
			Close:         closeAll,
			StorageReader: storageflux.NewReader(store),
		},
		tsdbStore: engine.TSDBStore(),
		shardIDs:  shardIDs,
	}
}

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

// filterSpec is the standard full-range read used by these tests.
func (r *multiShardReader) filterSpec() query.ReadFilterSpec {
	return query.ReadFilterSpec{
		OrganizationID: r.Org,
		BucketID:       r.Bucket,
		Bounds:         r.Bounds,
	}
}

func newAlloc() *memory.ResourceAllocator {
	return &memory.ResourceAllocator{Allocator: arrowmem.DefaultAllocator}
}

// inUseFiles returns the paths of every TSM file still holding a reference.
func (r *multiShardReader) inUseFiles(tb testing.TB) []string {
	tb.Helper()
	var inUse []string
	for _, sh := range r.tsdbStore.Shards(r.shardIDs) {
		eng, err := sh.Engine()
		require.NoError(tb, err)
		tsmEng, ok := eng.(*tsm1.Engine)
		require.True(tb, ok, "expected a tsm1 engine, got %T", eng)
		for _, f := range tsmEng.FileStore.Files() {
			if f.InUse() {
				inUse = append(inUse, f.Path())
			}
		}
	}
	return inUse
}

// requireReferencesReleased asserts that every TSM reader reference taken by a
// query has been released.
//
// This is the strongest single invariant available for this class of bug: it
// catches both a double release (which drives the count negative and panics
// before this runs) and a leak (which leaves the count above zero and would
// later park FileStore.Close in refsWG.Wait()).
//
// Polled rather than asserted once, because a background compaction may
// legitimately hold a reference for a short time. A genuine leak never drains.
func (r *multiShardReader) requireReferencesReleased(tb testing.TB) {
	tb.Helper()
	var last []string
	require.Eventually(tb, func() bool {
		last = r.inUseFiles(tb)
		return len(last) == 0
	}, 15*time.Second, 25*time.Millisecond,
		"TSM references were never released; still in use: %v", last)
}

// closeBounded shuts the reader down with a deadline. Aborting KeyCursor.Close's
// release loop leaks references, which parks FileStore.Close in
// refsWG.Wait() forever; bound it so that shows up as a reported failure rather
// than a test-binary timeout.
func (r *multiShardReader) closeBounded(tb testing.TB) {
	tb.Helper()
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		r.Close()
	}()
	select {
	case <-closed:
	case <-time.After(20 * time.Second):
		tb.Error("SHUTDOWN HANG: reader.Close did not return; TSMReader.Close is " +
			"parked in refsWG.Wait() on leaked references")
	}
}

// smallSpec is a modest data set: 6 one-hour shard groups, 50 series, one point
// per second. Enough that a table spans several buffers and every series spans
// every shard.
func smallSpec() (time.Duration, MultiShardSetupFunc) {
	return time.Hour, func(org, bucket platform.ID) (*datagen.Spec, datagen.TimeRange) {
		spec := Spec(org, bucket,
			MeasurementSpec("m0",
				FloatArrayValuesSequence("f0", time.Second, []float64{1.0, 2.0, 3.0, 4.0}),
				TagValuesSequence("t0", "a-%s", 0, 50),
			),
		)
		return spec, TimeRange("2019-11-25T00:00:00Z", "2019-11-25T06:00:00Z")
	}
}

// largeGroupSpec is a wide data set: 6 one-hour shard groups, 500 series in a
// single measurement, one point per 10s. Used to measure how long a table can
// occupy a consumer goroutine, since that is what a fix which waits for the
// consumer would have to wait for.
func largeGroupSpec() (time.Duration, MultiShardSetupFunc) {
	return time.Hour, func(org, bucket platform.ID) (*datagen.Spec, datagen.TimeRange) {
		spec := Spec(org, bucket,
			MeasurementSpec("m0",
				FloatArrayValuesSequence("f0", 10*time.Second, []float64{1.0, 2.0, 3.0, 4.0}),
				TagValuesSequence("t0", "a-%s", 0, 500),
			),
		)
		return spec, TimeRange("2019-11-25T00:00:00Z", "2019-11-25T06:00:00Z")
	}
}

// TestStorageReader_CancelTeardownLatency measures the wait that the
// ownership-transfer fix (table.awaitAbandoned) adds to query teardown, and
// bounds it.
//
// Before the fix, cancellation returned immediately: handleRead did `break READ`
// and abandoned the consumer, which kept draining anyway - unsafely, which was
// the bug. Waiting for the consumer instead makes teardown latency equal to
// however long the consumer still needs.
//
// The concern was xGroupTable.advance(): for an aggregate it drains every series
// in the group across every shard in a single call (table.gen.go, the
// AccumulateMore/advanceCursor loop), and table.do only checks isCancelled()
// *between* advance() calls, so Cancel() cannot interrupt it. If that drain
// happened on the consumer goroutine, waiting for it would delay a cancelled
// query's response by the cost of a whole group, and the fix would have needed
// isCancelled() checks pushed down into those inner loops - a change across the
// generated templates rather than three call sites.
//
// This test measures where that cost actually falls: time spent constructing the
// table (on handleRead's own goroutine, before the handoff) versus time the
// consumer still needs after the handoff.
func TestStorageReader_CancelTeardownLatency(t *testing.T) {
	dur, setup := largeGroupSpec()
	reader := NewMultiShardStorageReader(t, dur, setup)
	defer reader.closeBounded(t)

	// maxConsumerDrain bounds the wait an ownership-transfer fix would add.
	// Generous: the point is to catch a whole-group drain landing here, not to
	// pin down a precise duration.
	const maxConsumerDrain = 2 * time.Second

	for _, tc := range []struct {
		name string
		read func(ctx context.Context) (query.TableIterator, error)
	}{
		{"ReadFilter", func(ctx context.Context) (query.TableIterator, error) {
			return reader.ReadFilter(ctx, reader.filterSpec(), newAlloc())
		}},
		{"ReadGroup/aggregate", func(ctx context.Context) (query.TableIterator, error) {
			return reader.ReadGroup(ctx, query.ReadGroupSpec{
				ReadFilterSpec:  reader.filterSpec(),
				GroupMode:       query.GroupModeBy,
				GroupKeys:       []string{"_measurement"},
				AggregateMethod: storageflux.CountKind,
			}, newAlloc())
		}},
		{"ReadGroup/no-aggregate", func(ctx context.Context) (query.TableIterator, error) {
			return reader.ReadGroup(ctx, query.ReadGroupSpec{
				ReadFilterSpec: reader.filterSpec(),
				GroupMode:      query.GroupModeBy,
				GroupKeys:      []string{"_measurement"},
			}, newAlloc())
		}},
		{"ReadWindowAggregate", func(ctx context.Context) (query.TableIterator, error) {
			return reader.ReadWindowAggregate(ctx, query.ReadWindowAggregateSpec{
				ReadFilterSpec: reader.filterSpec(),
				Window: execute.Window{
					Every:  flux.ConvertDuration(30 * time.Second),
					Period: flux.ConvertDuration(30 * time.Second),
				},
				Aggregates: []plan.ProcedureKind{storageflux.CountKind},
			}, newAlloc())
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			ti, err := tc.read(ctx)
			require.NoError(t, err)

			var (
				mu       sync.Mutex
				drain    time.Duration
				buffers  int
				wg       sync.WaitGroup
				handedAt time.Time
			)

			start := time.Now()
			_ = ti.Do(func(tbl flux.Table) error {
				// Everything before this point - including the table
				// constructor, which is where a group aggregate does its
				// draining - ran on this goroutine.
				handedAt = time.Now()
				wg.Go(func() {
					t0 := time.Now()
					defer func() {
						d := time.Since(t0)
						mu.Lock()
						if d > drain {
							drain = d
						}
						mu.Unlock()
						// A recovered panic here is the underlying bug, not a
						// latency signal; the other tests cover it.
						_ = recover()
					}()
					_ = tbl.Do(func(flux.ColReader) error {
						mu.Lock()
						buffers++
						mu.Unlock()
						return nil
					})
				})
				// Cancel as soon as the first table is handed off, so the
				// measured drain is what teardown would have to wait for.
				cancel()
				return nil
			})
			returned := time.Since(start)
			wg.Wait()

			mu.Lock()
			d, b := drain, buffers
			mu.Unlock()

			t.Logf("table construction (pre-handoff, on handleRead's goroutine): %s", handedAt.Sub(start))
			t.Logf("handleRead returned after: %s", returned)
			t.Logf("post-handoff consumer drain: %s over %d buffer(s)  <-- the wait a fix would add", d, b)

			require.Less(t, d, maxConsumerDrain,
				"a cancelled query's consumer needs %s after handoff; waiting for it "+
					"would delay teardown by that much, so Cancel() must be able to "+
					"interrupt the drain (isCancelled() checks inside advance()'s inner loops)", d)
		})
	}
}

// panicRecorder collects panics recovered on consumer goroutines, the way
// flux's poolDispatcher recovers them in production.
type panicRecorder struct {
	mu     sync.Mutex
	counts map[string]int
}

func newPanicRecorder() *panicRecorder {
	return &panicRecorder{counts: make(map[string]int)}
}

func (p *panicRecorder) record(r any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.counts[fmt.Sprint(r)]++
}

func (p *panicRecorder) report(tb testing.TB) {
	tb.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	for msg, n := range p.counts {
		tb.Logf("recovered %4d x %s", n, msg)
	}
}

// ---------------------------------------------------------------------------
// Test 1 - liveness. Must hold before and after the fix.
// ---------------------------------------------------------------------------

// TestStorageReader_CancelWithUnconsumedTable guards against the obvious but
// wrong fix for the cursor close race: making handleRead wait for `done`
// unconditionally instead of bailing out on ctx.Done().
//
// That would deadlock. consecutiveTransport.processMessages (flux
// execute/transport.go:248-270) abandons its queue on error or finish, and
// `done` is closed only by table.do's defer or by table.Done() via
// processMsg.Ack(). A table dropped from the queue gets neither, so `done`
// never closes.
//
// This test models exactly that: the Do callback accepts the table and never
// consumes it, never calls Done, and the context is then cancelled. handleRead
// must still return. Any fix that waits for a consumer which will never run
// will hang here.
func TestStorageReader_CancelWithUnconsumedTable(t *testing.T) {
	dur, setup := smallSpec()
	reader := NewMultiShardStorageReader(t, dur, setup)
	defer reader.closeBounded(t)

	for _, tc := range []struct {
		name string
		read func(ctx context.Context) (query.TableIterator, error)
	}{
		{"ReadFilter", func(ctx context.Context) (query.TableIterator, error) {
			return reader.ReadFilter(ctx, reader.filterSpec(), newAlloc())
		}},
		{"ReadGroup", func(ctx context.Context) (query.TableIterator, error) {
			return reader.ReadGroup(ctx, query.ReadGroupSpec{
				ReadFilterSpec: reader.filterSpec(),
				GroupMode:      query.GroupModeBy,
				GroupKeys:      []string{"_measurement"},
			}, newAlloc())
		}},
		{"ReadWindowAggregate", func(ctx context.Context) (query.TableIterator, error) {
			return reader.ReadWindowAggregate(ctx, query.ReadWindowAggregateSpec{
				ReadFilterSpec: reader.filterSpec(),
				Window: execute.Window{
					Every:  flux.ConvertDuration(30 * time.Second),
					Period: flux.ConvertDuration(30 * time.Second),
				},
				Aggregates: []plan.ProcedureKind{storageflux.CountKind},
			}, newAlloc())
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			ti, err := tc.read(ctx)
			require.NoError(t, err)

			returned := make(chan error, 1)
			go func() {
				returned <- ti.Do(func(tbl flux.Table) error {
					// A transport that queues the table and is then abandoned:
					// neither Do nor Done is ever called on it.
					return nil
				})
			}()

			cancel()

			select {
			case <-returned:
			case <-time.After(30 * time.Second):
				t.Fatal("handleRead did not return after cancellation with an " +
					"unconsumed table: the wait for `done` is unbounded")
			}

			reader.requireReferencesReleased(t)
		})
	}
}

// ---------------------------------------------------------------------------
// Test 2 - reference count invariant across every exit path.
// ---------------------------------------------------------------------------

// TestStorageReader_ReferencesReleased asserts that a query releases every TSM
// reader reference it takes, on all of the exits handleRead has: normal
// completion, a consumer error, an f(table) error, and cancellation.
//
// The deterministic scenarios held even before the fix. The two concurrent
// scenarios are the ones the bug could violate; they passed only by luck before
// awaitAbandoned and must now pass deterministically.
func TestStorageReader_ReferencesReleased(t *testing.T) {
	dur, setup := smallSpec()
	reader := NewMultiShardStorageReader(t, dur, setup)
	defer reader.closeBounded(t)

	for _, tc := range []struct {
		name       string
		concurrent bool
		// run drives one full read to completion, including any consumer
		// goroutines it starts.
		run func(t *testing.T, rec *panicRecorder)
	}{
		{
			name: "consumed inline",
			run: func(t *testing.T, rec *panicRecorder) {
				ti, err := reader.ReadFilter(context.Background(), reader.filterSpec(), newAlloc())
				require.NoError(t, err)
				require.NoError(t, ti.Do(func(tbl flux.Table) error {
					return tbl.Do(func(flux.ColReader) error { return nil })
				}))
			},
		},
		{
			name: "consumer returns error",
			run: func(t *testing.T, rec *panicRecorder) {
				ti, err := reader.ReadFilter(context.Background(), reader.filterSpec(), newAlloc())
				require.NoError(t, err)
				// Error from inside the ColReader callback: table.do aborts
				// mid-advance and returns the error up through handleRead.
				err = ti.Do(func(tbl flux.Table) error {
					return tbl.Do(func(flux.ColReader) error {
						return fmt.Errorf("consumer failed")
					})
				})
				require.Error(t, err)
			},
		},
		{
			name: "f(table) returns error inline",
			run: func(t *testing.T, rec *panicRecorder) {
				ti, err := reader.ReadFilter(context.Background(), reader.filterSpec(), newAlloc())
				require.NoError(t, err)
				// Exercises handleRead's `if err := f(table); err != nil`
				// branch with no consumer in flight.
				err = ti.Do(func(tbl flux.Table) error {
					return fmt.Errorf("downstream rejected the table")
				})
				require.Error(t, err)
			},
		},
		{
			name:       "cancelled during deferred consume",
			concurrent: true,
			run: func(t *testing.T, rec *panicRecorder) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				ti, err := reader.ReadFilter(ctx, reader.filterSpec(), newAlloc())
				require.NoError(t, err)

				var wg sync.WaitGroup
				go func() {
					time.Sleep(200 * time.Microsecond)
					cancel()
				}()
				_ = ti.Do(func(tbl flux.Table) error {
					wg.Go(func() {
						defer func() {
							if r := recover(); r != nil {
								rec.record(r)
							}
						}()
						_ = tbl.Do(func(flux.ColReader) error { return nil })
					})
					return nil
				})
				wg.Wait()
			},
		},
		{
			// NOT a reachable production path - flux's contract makes it
			// unreachable: consecutiveTransport.Process returns t.err() from
			// its `select { case <-t.finished: }` *before* pushMsg, and
			// returns nil unconditionally afterwards, so a callback that
			// errors has not queued the table. The multi-transformation
			// branch of Source.processTable is safe for a different reason -
			// execute.CopyTable consumes the storage table inline before any
			// copy is queued.
			//
			// handleRead used to lean on that contract: its f(table) error
			// branch closed the table without waiting for `done`, so this
			// case - which deliberately violates the contract by consuming
			// the table on another goroutine and then erroring - faulted
			// deterministically. Those branches now call awaitAbandoned
			// before Close, so it must pass even under the violation. Kept
			// so a regression of that stronger guarantee is noticed here.
			name:       "f(table) errors after deferred consume starts (contract violation)",
			concurrent: true,
			run: func(t *testing.T, rec *panicRecorder) {
				ti, err := reader.ReadFilter(context.Background(), reader.filterSpec(), newAlloc())
				require.NoError(t, err)

				var wg sync.WaitGroup
				err = ti.Do(func(tbl flux.Table) error {
					wg.Go(func() {
						defer func() {
							if r := recover(); r != nil {
								rec.record(r)
							}
						}()
						_ = tbl.Do(func(flux.ColReader) error { return nil })
					})
					return fmt.Errorf("downstream rejected the table")
				})
				wg.Wait()
				require.Error(t, err)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := newPanicRecorder()
			// Repeat the concurrent cases: the damaging interleaving is narrow.
			iterations := 1
			if tc.concurrent {
				iterations = 100
			}
			for range iterations {
				tc.run(t, rec)
			}
			rec.report(t)
			reader.requireReferencesReleased(t)
		})
	}
}

// ---------------------------------------------------------------------------
// Tests 3 and 4 - race coverage for the remaining close paths and variants.
// ---------------------------------------------------------------------------

// TestStorageReader_CancelDuringDeferredConsume shows that storage/flux used to
// let two goroutines close the same cursor chain concurrently, across all three
// handleRead variants and both of their previously unsynchronised close paths.
//
// storage/flux hands each table to the caller and then waits for it to signal
// completion, but used to abandon that wait on cancellation:
//
//	select {
//	case <-done:
//	case <-fi.ctx.Done():
//	    table.Cancel()
//	    break READ
//	}
//
// and on an f(table) error it did not wait at all. table.Cancel only sets an
// atomic flag, checked between advance() iterations, so it does not stop an
// advance already in flight. The deferred table.Close() then ran concurrently
// with it. Both paths now settle ownership through table.awaitAbandoned before
// closing; this test is the regression guard for that.
//
// The interleaving is real because flux does not consume tables inline:
// consecutiveTransport queues the table and a poolDispatcher goroutine calls
// Do() later. This test models that handoff - the Do callback starts a goroutine
// and returns immediately.
//
// Series span several shards so advance() reaches
// floatMultiShardArrayCursor.nextArrayCursor, which closes the cursor chain
// itself; with one shard it returns early on len(c.itrs) == 0 and the consumer
// never closes anything.
//
// Before the fix, -race reported writes in floatArrayAscendingCursor.Close
// against reads in its Next, and a write of t.cur against advance()'s read. It
// also faulted outright, because Close cleared c.tsm.keyCursor while nextTSM()
// was dereferencing it.
//
// It did not by itself produce the negative WaitGroup counter: that needs
// table.Close() to land inside one of the few brief nextArrayCursor() calls per
// table rather than anywhere in Next(). See TestKeyCursor_ConcurrentClose in
// tsdb/engine/tsm1 for the deterministic version.
func TestStorageReader_CancelDuringDeferredConsume(t *testing.T) {
	dur, setup := smallSpec()
	reader := NewMultiShardStorageReader(t, dur, setup)
	defer reader.closeBounded(t)

	reads := map[string]func(ctx context.Context) (query.TableIterator, error){
		"ReadFilter": func(ctx context.Context) (query.TableIterator, error) {
			return reader.ReadFilter(ctx, reader.filterSpec(), newAlloc())
		},
		"ReadGroup": func(ctx context.Context) (query.TableIterator, error) {
			return reader.ReadGroup(ctx, query.ReadGroupSpec{
				ReadFilterSpec: reader.filterSpec(),
				GroupMode:      query.GroupModeBy,
				GroupKeys:      []string{"_measurement"},
			}, newAlloc())
		},
		"ReadWindowAggregate": func(ctx context.Context) (query.TableIterator, error) {
			return reader.ReadWindowAggregate(ctx, query.ReadWindowAggregateSpec{
				ReadFilterSpec: reader.filterSpec(),
				Window: execute.Window{
					Every:  flux.ConvertDuration(30 * time.Second),
					Period: flux.ConvertDuration(30 * time.Second),
				},
				Aggregates: []plan.ProcedureKind{storageflux.CountKind},
			}, newAlloc())
		},
	}

	// abandonCancel abandons the table by cancelling the context;
	// abandonError abandons it by rejecting it from the Do callback.
	for _, abandon := range []string{"cancel", "f-error"} {
		for name, read := range reads {
			t.Run(abandon+"/"+name, func(t *testing.T) {
				rec := newPanicRecorder()
				var tables, buffers int64

				const (
					workers = 4
					rounds  = 60
				)
				attempt := func(worker, round int) {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()

					ti, err := read(ctx)
					if err != nil {
						rec.record(fmt.Sprintf("read: %v", err))
						return
					}

					var wg sync.WaitGroup
					defer wg.Wait()

					if abandon == "cancel" {
						// Stagger per worker and round so the cancellation
						// lands at many different points inside the read.
						go func() {
							time.Sleep(time.Duration(round*20+worker*7) * time.Microsecond)
							cancel()
						}()
					}

					// Deliberately ignored: abandoning a table mid-read is
					// expected to surface an error. This test looks for a race
					// report or a panic, not a returned error.
					_ = ti.Do(func(tbl flux.Table) error {
						atomic.AddInt64(&tables, 1)
						wg.Go(func() {
							defer func() {
								if r := recover(); r != nil {
									rec.record(r)
								}
							}()
							_ = tbl.Do(func(flux.ColReader) error {
								atomic.AddInt64(&buffers, 1)
								return nil
							})
						})
						if abandon == "f-error" {
							return fmt.Errorf("downstream rejected the table")
						}
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

				t.Logf("tables=%d buffers=%d", atomic.LoadInt64(&tables), atomic.LoadInt64(&buffers))
				rec.report(t)
			})
		}
	}
}
