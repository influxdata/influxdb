package storageflux

import (
	"fmt"
	"testing"

	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
	"github.com/stretchr/testify/require"
)

// These tests cover the last f(table) hand-off class in this package: the
// metadata iterators (tag keys, tag values, series cardinality) build a
// ColListTable - whose columns are allocator-backed copies made by
// ColListTableBuilder.Table() - and hand it to f. An erroring callback has not
// queued the table for a consumer, so unless handleRead releases the table
// itself, neither Do nor Done ever runs on it and the copies leak, exactly as
// with the storage tables and splitWindows rows.

type stringSliceIterator struct {
	values []string
	i      int
}

func (s *stringSliceIterator) Next() bool {
	if s.i < len(s.values) {
		s.i++
		return true
	}
	return false
}

func (s *stringSliceIterator) Value() string              { return s.values[s.i-1] }
func (s *stringSliceIterator) Stats() cursors.CursorStats { return cursors.CursorStats{} }

type int64SliceIterator struct {
	values []int64
	i      int
}

func (s *int64SliceIterator) Next() bool {
	if s.i < len(s.values) {
		s.i++
		return true
	}
	return false
}

func (s *int64SliceIterator) Value() int64               { return s.values[s.i-1] }
func (s *int64SliceIterator) Stats() cursors.CursorStats { return cursors.CursorStats{} }

// metadataReads drives each iterator's handleRead directly: it only touches
// its allocator and the result-set iterator, so no store is needed.
func metadataReads() map[string]func(alloc memory.Allocator, f func(flux.Table) error) error {
	return map[string]func(alloc memory.Allocator, f func(flux.Table) error) error{
		"tagKeys": func(alloc memory.Allocator, f func(flux.Table) error) error {
			ti := &tagKeysIterator{alloc: alloc}
			return ti.handleRead(f, &stringSliceIterator{values: []string{"t0", "t1"}})
		},
		"tagValues": func(alloc memory.Allocator, f func(flux.Table) error) error {
			ti := &tagValuesIterator{alloc: alloc}
			return ti.handleRead(f, &stringSliceIterator{values: []string{"a", "b"}})
		},
		"seriesCardinality": func(alloc memory.Allocator, f func(flux.Table) error) error {
			si := &seriesCardinalityIterator{alloc: alloc}
			return si.handleRead(f, &int64SliceIterator{values: []int64{42}})
		},
	}
}

// TestMetadataIterators_FErrorReleasesTable rejects the table from the Do
// callback and requires that handleRead release it: nobody else will.
func TestMetadataIterators_FErrorReleasesTable(t *testing.T) {
	for name, read := range metadataReads() {
		t.Run(name, func(t *testing.T) {
			mem := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			alloc := &memory.ResourceAllocator{Allocator: mem}

			rejected := fmt.Errorf("downstream rejected the table")
			err := read(alloc, func(tbl flux.Table) error {
				return rejected
			})
			require.ErrorIs(t, err, rejected)
		})
	}
}

// TestMetadataIterators_ConsumedReleasesTable is the control: a consumed table
// releases itself through Do, so handleRead must not release it again.
func TestMetadataIterators_ConsumedReleasesTable(t *testing.T) {
	for name, read := range metadataReads() {
		t.Run(name, func(t *testing.T) {
			mem := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			alloc := &memory.ResourceAllocator{Allocator: mem}

			err := read(alloc, func(tbl flux.Table) error {
				return tbl.Do(func(flux.ColReader) error { return nil })
			})
			require.NoError(t, err)
		})
	}
}
