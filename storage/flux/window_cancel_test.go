package storageflux

import (
	"context"
	"fmt"
	"testing"

	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/array"
	"github.com/influxdata/flux/arrow"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/values"
	"github.com/stretchr/testify/require"
)

// splitWindowsInput is a minimal flux.Table over a single buffer, standing in for
// the window table that windowAggregateIterator hands to splitWindows at
// reader.go:720.
type splitWindowsInput struct {
	buf arrow.TableBuffer
}

func (t *splitWindowsInput) Key() flux.GroupKey   { return t.buf.GroupKey }
func (t *splitWindowsInput) Cols() []flux.ColMeta { return t.buf.Columns }
func (t *splitWindowsInput) Empty() bool          { return false }
func (t *splitWindowsInput) Done()                { t.buf.Release() }
func (t *splitWindowsInput) Do(f func(flux.ColReader) error) error {
	return f(&t.buf)
}

// newSplitWindowsInput builds an input table with the column layout
// splitWindows requires: _start, _stop, _time, _value. Row 0's value is null,
// so selector mode emits the first window as an empty table (window.go's
// IsNull branch) where non-selector mode emits a buffer row - the divergence
// that makes the selector subtest variants meaningful.
func newSplitWindowsInput(alloc memory.Allocator, rows int) *splitWindowsInput {
	starts := make([]int64, rows)
	stops := make([]int64, rows)
	times := make([]int64, rows)
	vb := arrow.NewFloatBuilder(alloc)
	vb.Resize(rows)
	for i := range rows {
		starts[i] = int64(i * 10)
		stops[i] = int64(i*10 + 10)
		times[i] = int64(i * 10)
		if i == 0 {
			vb.AppendNull()
		} else {
			vb.Append(float64(i))
		}
	}

	cols := []flux.ColMeta{
		{Label: execute.DefaultStartColLabel, Type: flux.TTime},
		{Label: execute.DefaultStopColLabel, Type: flux.TTime},
		{Label: execute.DefaultTimeColLabel, Type: flux.TTime},
		{Label: execute.DefaultValueColLabel, Type: flux.TFloat},
	}
	key := execute.NewGroupKey(
		[]flux.ColMeta{cols[startColIdx], cols[stopColIdx]},
		[]values.Value{values.NewTime(0), values.NewTime(values.Time(rows * 10))},
	)

	return &splitWindowsInput{
		buf: arrow.TableBuffer{
			GroupKey: key,
			Columns:  cols,
			Values: []array.Array{
				arrow.NewInt(starts, alloc),
				arrow.NewInt(stops, alloc),
				arrow.NewInt(times, alloc),
				vb.NewFloatArray(),
			},
		},
	}
}

// TestSplitWindows_CancelReleasesBuffer covers the same defect in splitWindows
// that abandon fixes in handleRead: the producer stops waiting for the
// consumer on context cancellation and walks away from a table nobody owns.
//
// windowTableRow's buffer is released by whichever of Do or Done claims the
// table. Abandoning it without claiming means neither runs, so the sliced arrays
// keep the input buffer's arrow allocation referenced for as long as the query
// lives.
//
// This is milder than the handleRead case - windowTableRow holds an in-memory
// buffer, not a cursor, so there is no double release and no TSM reference count
// to corrupt - but it is the same failure to settle ownership, and
// windowTableRow already carries the same used/Do/Done CAS structure to settle it
// with.
//
// The checked allocator is what makes the leak observable: it reports any arrow
// buffer still outstanding when the test ends.
//
// Every test here runs against both splitter modes. selector mode changes how
// null values are emitted (an empty table instead of a buffer row), and the
// input's null row 0 makes that branch real: selector variants walk the
// empty-table emission - which settles nothing on rejection, because empty
// tables hold no buffers - while non-selector variants walk a buffer row
// through the same hand-off.
func TestSplitWindows_CancelReleasesBuffer(t *testing.T) {
	for _, selector := range []bool{false, true} {
		t.Run(fmt.Sprintf("selector=%v", selector), func(t *testing.T) {
			mem := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			alloc := &memory.ResourceAllocator{Allocator: mem}
			in := newSplitWindowsInput(alloc, 8)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Model flux's handoff: the callback accepts the table and returns without
			// consuming it, as consecutiveTransport does when it queues a table for a
			// dispatcher goroutine. Cancelling then drives splitWindows down its
			// abandonment path while that table is still unclaimed.
			err := splitWindows(ctx, alloc, in, selector, func(tbl flux.Table) error {
				cancel()
				return nil
			})
			require.ErrorIs(t, err, context.Canceled)
		})
	}
}

// TestSplitWindows_FErrorReleasesBuffer covers the other abandonment path: the
// Do callback rejects the table with an error. An erroring callback has not
// queued the table for a consumer (the same flux contract handleRead relies on),
// so unless splitWindows settles ownership itself, neither Do nor Done ever runs
// and the row's buffer leaks exactly as in the cancellation case. In selector
// mode the rejected table is the null row's empty table, covering the error
// return of the empty-table branch: it settles nothing today because empty
// tables hold no buffers, and a future change that hands that branch a
// buffer-backed table without settling it fails the allocator here.
func TestSplitWindows_FErrorReleasesBuffer(t *testing.T) {
	for _, selector := range []bool{false, true} {
		t.Run(fmt.Sprintf("selector=%v", selector), func(t *testing.T) {
			mem := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			alloc := &memory.ResourceAllocator{Allocator: mem}
			in := newSplitWindowsInput(alloc, 8)

			rejected := fmt.Errorf("downstream rejected the table")
			err := splitWindows(context.Background(), alloc, in, selector, func(tbl flux.Table) error {
				return rejected
			})
			require.ErrorIs(t, err, rejected)
		})
	}
}

// TestSplitWindows_FErrorOnLaterTableReleasesBuffer consumes the first table
// normally and rejects the second. The consumed row released its own buffer
// through Do, so the splitter must abandon only the rejected row: releasing
// the consumed one again would double-release, and skipping the rejected one
// would leak it. The windows after the rejection are never materialized, so
// they have nothing to release.
func TestSplitWindows_FErrorOnLaterTableReleasesBuffer(t *testing.T) {
	for _, selector := range []bool{false, true} {
		t.Run(fmt.Sprintf("selector=%v", selector), func(t *testing.T) {
			mem := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			alloc := &memory.ResourceAllocator{Allocator: mem}
			in := newSplitWindowsInput(alloc, 8)

			rejected := fmt.Errorf("downstream rejected the table")
			tables := 0
			err := splitWindows(context.Background(), alloc, in, selector, func(tbl flux.Table) error {
				tables++
				if tables == 1 {
					return tbl.Do(func(flux.ColReader) error { return nil })
				}
				return rejected
			})
			require.ErrorIs(t, err, rejected)
			require.Equal(t, 2, tables)
		})
	}
}

// TestSplitWindows_PanicInConsumerReleasesBuffer covers the panic exit of
// windowTableRow.Do: flux's dispatcher recovers consumer panics, so the
// process outlives them and a buffer released only by a plain statement after
// f would leak - and a leaked row pins the entire input table's arrays, not
// just its own slice. The release must be deferred, mirroring (*table).do.
// PanicsWithValue pins that the original panic, not one raised inside the
// defer, is what unwinds.
func TestSplitWindows_PanicInConsumerReleasesBuffer(t *testing.T) {
	for _, selector := range []bool{false, true} {
		t.Run(fmt.Sprintf("selector=%v", selector), func(t *testing.T) {
			mem := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			alloc := &memory.ResourceAllocator{Allocator: mem}
			in := newSplitWindowsInput(alloc, 8)

			require.PanicsWithValue(t, "consumer panicked", func() {
				_ = splitWindows(context.Background(), alloc, in, selector, func(tbl flux.Table) error {
					return tbl.Do(func(flux.ColReader) error { panic("consumer panicked") })
				})
			})
		})
	}
}

// TestSplitWindows_ConsumedReleasesBuffer is the control: when every table is
// consumed normally, nothing is outstanding. This must hold before and after the
// fix, and guards against a fix that over-releases.
func TestSplitWindows_ConsumedReleasesBuffer(t *testing.T) {
	for _, selector := range []bool{false, true} {
		t.Run(fmt.Sprintf("selector=%v", selector), func(t *testing.T) {
			mem := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			alloc := &memory.ResourceAllocator{Allocator: mem}
			in := newSplitWindowsInput(alloc, 8)

			tables := 0
			err := splitWindows(context.Background(), alloc, in, selector, func(tbl flux.Table) error {
				tables++
				return tbl.Do(func(flux.ColReader) error { return nil })
			})
			require.NoError(t, err)
			require.Equal(t, 8, tables)
		})
	}
}
