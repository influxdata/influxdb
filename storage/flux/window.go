package storageflux

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/apache/arrow/go/v7/arrow/memory"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/array"
	"github.com/influxdata/flux/arrow"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

// splitWindows will split a windowTable by creating a new table from each
// row and modifying the group key to use the start and stop values from
// that row.
func splitWindows(ctx context.Context, alloc memory.Allocator, in flux.Table, selector bool, f func(t flux.Table) error) error {
	wts := &windowTableSplitter{
		ctx:      ctx,
		in:       in,
		alloc:    alloc,
		selector: selector,
	}
	return wts.Do(f)
}

type windowTableSplitter struct {
	ctx      context.Context
	in       flux.Table
	alloc    memory.Allocator
	selector bool
}

func (w *windowTableSplitter) Do(f func(flux.Table) error) error {
	defer w.in.Done()

	startIdx, err := w.getTimeColumnIndex(execute.DefaultStartColLabel)
	if err != nil {
		return err
	}

	stopIdx, err := w.getTimeColumnIndex(execute.DefaultStopColLabel)
	if err != nil {
		return err
	}

	return w.in.Do(func(cr flux.ColReader) error {
		// Retrieve the start and stop columns for splitting
		// the windows.
		start := cr.Times(startIdx)
		stop := cr.Times(stopIdx)

		// Iterate through each time to produce a table
		// using the start and stop values.
		arrs := make([]array.Array, len(cr.Cols()))
		for j := range cr.Cols() {
			arrs[j] = getColumnValues(cr, j)
		}

		values := arrs[valueColIdx]

		for i, n := 0, cr.Len(); i < n; i++ {
			startT, stopT := start.Value(i), stop.Value(i)

			// Rewrite the group key using the new time.
			key := groupKeyForWindow(cr.Key(), startT, stopT)
			if w.selector && values.IsNull(i) {
				// Produce an empty table if the value is null
				// and this is a selector.
				table := execute.NewEmptyTable(key, cr.Cols())
				if err := f(table); err != nil {
					return err
				}
				continue
			}

			// Produce a slice for each column into a new
			// table buffer.
			buffer := arrow.TableBuffer{
				GroupKey: key,
				Columns:  cr.Cols(),
				Values:   make([]array.Array, len(cr.Cols())),
			}
			for j, arr := range arrs {
				buffer.Values[j] = arrow.Slice(arr, int64(i), int64(i+1))
			}

			// Wrap these into a single table and execute.
			done := make(chan struct{})
			table := &windowTableRow{
				buffer: buffer,
				done:   done,
			}
			if err := f(table); err != nil {
				return err
			}

			select {
			case <-done:
			case <-w.ctx.Done():
				return w.ctx.Err()
			}
		}
		return nil
	})
}

func (w *windowTableSplitter) getTimeColumnIndex(label string) (int, error) {
	j := execute.ColIdx(label, w.in.Cols())
	if j < 0 {
		return -1, &errors.Error{
			Code: errors.EInvalid,
			Msg:  fmt.Sprintf("missing %q column from window splitter", label),
		}
	} else if c := w.in.Cols()[j]; c.Type != flux.TTime {
		return -1, &errors.Error{
			Code: errors.EInvalid,
			Msg:  fmt.Sprintf("%q column must be of type time", label),
		}
	}
	return j, nil
}

type windowTableRow struct {
	used   int32
	buffer arrow.TableBuffer
	done   chan struct{}
}

func (w *windowTableRow) Key() flux.GroupKey {
	return w.buffer.GroupKey
}

func (w *windowTableRow) Cols() []flux.ColMeta {
	return w.buffer.Columns
}

func (w *windowTableRow) Do(f func(flux.ColReader) error) error {
	if !atomic.CompareAndSwapInt32(&w.used, 0, 1) {
		return &errors.Error{
			Code: errors.EInternal,
			Msg:  "table already read",
		}
	}
	defer close(w.done)

	err := f(&w.buffer)
	w.buffer.Release()
	return err
}

func (w *windowTableRow) Done() {
	if atomic.CompareAndSwapInt32(&w.used, 0, 1) {
		w.buffer.Release()
		close(w.done)
	}
}

func (w *windowTableRow) Empty() bool {
	return false
}

func groupKeyForWindow(key flux.GroupKey, start, stop int64) flux.GroupKey {
	cols := key.Cols()
	vs := make([]values.Value, len(cols))
	for j, c := range cols {
		if c.Label == execute.DefaultStartColLabel {
			vs[j] = values.NewTime(values.Time(start))
		} else if c.Label == execute.DefaultStopColLabel {
			vs[j] = values.NewTime(values.Time(stop))
		} else {
			vs[j] = key.Value(j)
		}
	}
	return execute.NewGroupKey(cols, vs)
}

// getColumnValues returns the array from the column reader as an array.Array.
func getColumnValues(cr flux.ColReader, j int) array.Array {
	switch typ := cr.Cols()[j].Type; typ {
	case flux.TInt:
		return cr.Ints(j)
	case flux.TUInt:
		return cr.UInts(j)
	case flux.TFloat:
		return cr.Floats(j)
	case flux.TString:
		return cr.Strings(j)
	case flux.TBool:
		return cr.Bools(j)
	case flux.TTime:
		return cr.Times(j)
	default:
		panic(fmt.Errorf("unimplemented column type: %s", typ))
	}
}
