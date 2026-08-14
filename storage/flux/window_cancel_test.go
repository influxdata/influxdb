package storageflux

import (
	"context"
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
// splitWindows requires: _start, _stop, _time, _value.
func newSplitWindowsInput(alloc memory.Allocator, rows int) *splitWindowsInput {
	starts := make([]int64, rows)
	stops := make([]int64, rows)
	times := make([]int64, rows)
	vals := make([]float64, rows)
	for i := range rows {
		starts[i] = int64(i * 10)
		stops[i] = int64(i*10 + 10)
		times[i] = int64(i * 10)
		vals[i] = float64(i)
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
				arrow.NewFloat(vals, alloc),
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
func TestSplitWindows_CancelReleasesBuffer(t *testing.T) {
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
	err := splitWindows(ctx, alloc, in, false, func(tbl flux.Table) error {
		cancel()
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
}

// TestSplitWindows_ConsumedReleasesBuffer is the control: when every table is
// consumed normally, nothing is outstanding. This must hold before and after the
// fix, and guards against a fix that over-releases.
func TestSplitWindows_ConsumedReleasesBuffer(t *testing.T) {
	mem := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	alloc := &memory.ResourceAllocator{Allocator: mem}
	in := newSplitWindowsInput(alloc, 8)

	tables := 0
	err := splitWindows(context.Background(), alloc, in, false, func(tbl flux.Table) error {
		tables++
		return tbl.Do(func(flux.ColReader) error { return nil })
	})
	require.NoError(t, err)
	require.Equal(t, 8, tables)
}
