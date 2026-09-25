package storageflux

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/arrow"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/memory"
	"github.com/stretchr/testify/require"
)

// These tests pin the buffer-ownership invariant on table: whoever wins the
// used CAS - do(), Done(), or awaitAbandoned - owns colBufs and done, and is
// the exactly-once releaser of both. The checked allocator makes a missed
// release (a leak) observable, and a double release fails inside arrow.
//
// This mirrors what windowTableRow already guaranteed for its buffer: before
// awaitAbandoned released the buffer, a table abandoned while unclaimed - a
// cancelled query whose table was dropped from the transport queue without Do
// or Done ever being called - kept its current buffer's arrays accounted
// against the query's allocator forever.

// newUnconsumedTable builds a bare *table holding one allocated buffer, in the
// state handleRead hands tables to f: non-empty, unclaimed, buffer retained.
func newUnconsumedTable(tb testing.TB, alloc memory.Allocator) (*table, chan struct{}) {
	tb.Helper()
	done := make(chan struct{})
	cols := []flux.ColMeta{{Label: execute.DefaultValueColLabel, Type: flux.TFloat}}
	tbl := newTable(done, execute.Bounds{}, nil, cols, make([][]byte, len(cols)), nil, alloc)
	cr := tbl.allocateBuffer(3)
	cr.cols[0] = arrow.NewFloat([]float64{1, 2, 3}, alloc)
	tbl.empty = false
	return &tbl, done
}

func requireDoneClosed(tb testing.TB, done <-chan struct{}) {
	tb.Helper()
	select {
	case <-done:
	default:
		require.FailNow(tb, "done was not closed")
	}
}

// TestTable_AbandonedUnclaimedReleasesBuffer is the leak this file exists for:
// the producer abandons a table no consumer ever claimed, so awaitAbandoned
// wins the CAS and must release the buffer itself - nothing else will, since
// Done only releases when it wins the CAS.
func TestTable_AbandonedUnclaimedReleasesBuffer(t *testing.T) {
	mem := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	alloc := &memory.ResourceAllocator{Allocator: mem}

	tbl, done := newUnconsumedTable(t, alloc)
	tbl.awaitAbandoned(done)
	requireDoneClosed(t, done)
}

// TestTable_DoneUnclaimedReleasesBuffer covers flux discarding an unconsumed
// table through Done (processMsg.Ack): Done wins the CAS and releases.
func TestTable_DoneUnclaimedReleasesBuffer(t *testing.T) {
	mem := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	alloc := &memory.ResourceAllocator{Allocator: mem}

	tbl, done := newUnconsumedTable(t, alloc)
	tbl.Done()
	requireDoneClosed(t, done)
}

// TestTable_ConsumedThenAbandonedReleasesOnce is the control against
// over-releasing: once do() has consumed the table, a late awaitAbandoned and
// a late Done both lose the CAS and must touch nothing.
func TestTable_ConsumedThenAbandonedReleasesOnce(t *testing.T) {
	mem := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	alloc := &memory.ResourceAllocator{Allocator: mem}

	tbl, done := newUnconsumedTable(t, alloc)
	require.NoError(t, tbl.do(
		func(flux.ColReader) error { return nil },
		func() bool { return false },
	))
	requireDoneClosed(t, done)

	tbl.awaitAbandoned(done)
	tbl.Done()
}

// TestTable_DoWithInitErrorReleasesBuffer covers do()'s early error return, the
// one exit of a claimed table that leaves the buffer set. do()'s deferred
// release must cover it, because Done - the previous fallback for this case -
// now loses the CAS and does nothing.
func TestTable_DoWithInitErrorReleasesBuffer(t *testing.T) {
	mem := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	alloc := &memory.ResourceAllocator{Allocator: mem}

	tbl, done := newUnconsumedTable(t, alloc)
	tbl.err = fmt.Errorf("init failed")
	require.Error(t, tbl.do(
		func(flux.ColReader) error { return nil },
		func() bool { return false },
	))
	requireDoneClosed(t, done)

	tbl.Done()
}

// TestTable_PanicInFReleasesBuffer covers the panic half of do()'s deferred
// release: f panics before the loop settles the buffer, so the defer is the
// only releaser left for the claimed buffer. Deferred functions run LIFO, so
// it must also fire before closeDone - done never closes with the buffer
// still retained. PanicsWithValue pins that the original panic, not one
// raised inside the defer, is what unwinds.
func TestTable_PanicInFReleasesBuffer(t *testing.T) {
	mem := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	alloc := &memory.ResourceAllocator{Allocator: mem}

	tbl, done := newUnconsumedTable(t, alloc)
	require.PanicsWithValue(t, "consumer panicked", func() {
		_ = tbl.do(
			func(flux.ColReader) error { panic("consumer panicked") },
			func() bool { return false },
		)
	})
	requireDoneClosed(t, done)
}

// TestTable_PanicInAdvanceReleasesOnce is the panic exit before advance
// installs a fresh buffer, the state an allocator-limit panic in a
// windowTable's pre-allocateBuffer work leaves behind. The loop settles each
// buffer through releaseColBufs and nils colBufs, so the deferred release
// must find nothing to touch: a stale colBufs here would decrement a
// colReader the loop already settled, releasing arrays a retaining consumer
// still holds. The consumer's Retain across f is the anticipated state
// allocateBuffer's own comment describes, and its reference surviving the
// unwind is what makes that regression observable.
func TestTable_PanicInAdvanceReleasesOnce(t *testing.T) {
	mem := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	alloc := &memory.ResourceAllocator{Allocator: mem}

	tbl, done := newUnconsumedTable(t, alloc)
	var retained flux.ColReader
	require.PanicsWithValue(t, "advance panicked", func() {
		_ = tbl.do(
			func(cr flux.ColReader) error {
				cr.Retain()
				retained = cr
				return nil
			},
			func() bool { panic("advance panicked") },
		)
	})
	requireDoneClosed(t, done)

	// The retained reference must be the only one left standing after the
	// unwind; releasing it is what brings the allocator back to zero. NotNil
	// keeps a failure where f never ran (retained never assigned) a clean
	// assertion instead of a panic on the type assertion below.
	require.NotNil(t, retained)
	require.Equal(t, int64(1), atomic.LoadInt64(&retained.(*colReader).refCount))
	retained.Release()
}

// TestTable_PanicMidFillReleasesFreshBuffer is the panic exit after advance
// installs a fresh buffer but before every column is filled: filled and nil
// cols entries coexist. The deferred release owns that buffer and must skip
// the nil entries while still releasing the filled ones. The nil entry
// deliberately precedes the filled column, so a guard that stops at the first
// nil rather than skipping it leaks the filled column and fails the checked
// allocator. PanicsWithValue pins that the original allocator panic, not a
// nil dereference inside the defer, is what unwinds.
func TestTable_PanicMidFillReleasesFreshBuffer(t *testing.T) {
	mem := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	alloc := &memory.ResourceAllocator{Allocator: mem}

	done := make(chan struct{})
	cols := []flux.ColMeta{
		{Label: execute.DefaultTimeColLabel, Type: flux.TTime},
		{Label: execute.DefaultValueColLabel, Type: flux.TFloat},
	}
	tbl := newTable(done, execute.Bounds{}, nil, cols, make([][]byte, len(cols)), nil, alloc)
	cr := tbl.allocateBuffer(3)
	cr.cols[0] = arrow.NewInt([]int64{1, 2, 3}, alloc)
	cr.cols[1] = arrow.NewFloat([]float64{1, 2, 3}, alloc)
	tbl.empty = false

	require.PanicsWithValue(t, "allocator limit exceeded", func() {
		_ = tbl.do(
			func(flux.ColReader) error { return nil },
			func() bool {
				// Fill the value column but not the time column, modeling a
				// limit panic partway through an advance() fill.
				next := tbl.allocateBuffer(2)
				next.cols[1] = arrow.NewFloat([]float64{10, 20}, alloc)
				panic("allocator limit exceeded")
			},
		)
	})
	requireDoneClosed(t, done)
}

// TestTable_ConcurrentDoneAndAbandon races the two claimants that can both
// find the table unclaimed. Exactly one may win the used CAS; the winner
// releases the buffer and closes done, and the loser must touch nothing it
// doesn't own. Run under -race this pins the arbitration itself, and the
// checked allocator reports a missed release. (A double release is silent in
// default builds - arrow's underflow check is a compiled-out debug assert -
// so the race detector is the oracle for the loser touching shared state.)
func TestTable_ConcurrentDoneAndAbandon(t *testing.T) {
	mem := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	alloc := &memory.ResourceAllocator{Allocator: mem}

	for range 500 {
		tbl, done := newUnconsumedTable(t, alloc)

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			tbl.Done()
		}()
		go func() {
			defer wg.Done()
			<-start
			tbl.awaitAbandoned(done)
		}()
		close(start)
		wg.Wait()

		requireDoneClosed(t, done)
	}
}

// TestTable_ConcurrentDoAndAbandon races a consumer claiming the table against
// the producer abandoning it, the arbitration awaitAbandoned exists for. On
// top of the exactly-once release, this pins do()'s losing outcome: abandon
// Cancels before taking the CAS, so a do that loses must report cancellation,
// not "table already used".
func TestTable_ConcurrentDoAndAbandon(t *testing.T) {
	mem := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	alloc := &memory.ResourceAllocator{Allocator: mem}

	for range 500 {
		tbl, done := newUnconsumedTable(t, alloc)

		start := make(chan struct{})
		errCh := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			errCh <- tbl.do(
				func(flux.ColReader) error { return nil },
				func() bool { return false },
			)
		}()
		go func() {
			defer wg.Done()
			<-start
			tbl.awaitAbandoned(done)
		}()
		close(start)
		wg.Wait()

		requireDoneClosed(t, done)

		// do either won the CAS and consumed the table, or lost it to the
		// abandonment and reports cancellation.
		if err := <-errCh; err != nil {
			var ferr *flux.Error
			require.ErrorAs(t, err, &ferr)
			require.Equal(t, codes.Canceled, ferr.Code)
		}
	}
}
