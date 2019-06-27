package reads

import (
	"sync/atomic"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/arrow"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/influxdb/models"
)

type table struct {
	bounds execute.Bounds
	key    flux.GroupKey
	cols   []flux.ColMeta

	// cache of the tags on the current series.
	// len(tags) == len(colMeta)
	tags [][]byte
	defs [][]byte

	done chan struct{}

	colBufs *colReader

	err error

	cancelled int32
	alloc     *memory.Allocator
}

func newTable(
	done chan struct{},
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	defs [][]byte,
	alloc *memory.Allocator,
) table {
	return table{
		done:   done,
		bounds: bounds,
		key:    key,
		tags:   make([][]byte, len(cols)),
		defs:   defs,
		cols:   cols,
		alloc:  alloc,
	}
}

func (t *table) Key() flux.GroupKey   { return t.key }
func (t *table) Cols() []flux.ColMeta { return t.cols }
func (t *table) Err() error           { return t.err }
func (t *table) Empty() bool          { return false }

func (t *table) Cancel() {
	atomic.StoreInt32(&t.cancelled, 1)
}

func (t *table) isCancelled() bool {
	return atomic.LoadInt32(&t.cancelled) != 0
}

// getBuffer will retrieve a suitable buffer for the
// table implementations to use. If the existing buffer
// is not used anymore, then it may be reused.
func (t *table) getBuffer(l int) *colReader {
	if t.colBufs != nil && atomic.LoadInt64(&t.colBufs.refCount) == 0 {
		t.colBufs.refCount = 1
		t.colBufs.l = l
		return t.colBufs
	}

	// The current buffer is still being used so we should
	// generate a new one.
	t.colBufs = &colReader{
		refCount: 1,
		key:      t.key,
		colMeta:  t.cols,
		cols:     make([]array.Interface, len(t.cols)),
		l:        l,
	}
	return t.colBufs
}

type colReader struct {
	refCount int64

	key     flux.GroupKey
	colMeta []flux.ColMeta
	cols    []array.Interface
	l       int
}

func (cr *colReader) Retain() {
	atomic.AddInt64(&cr.refCount, 1)
}
func (cr *colReader) Release() {
	if atomic.AddInt64(&cr.refCount, -1) == 0 {
		for _, col := range cr.cols {
			col.Release()
		}
	}
}

func (cr *colReader) Key() flux.GroupKey   { return cr.key }
func (cr *colReader) Cols() []flux.ColMeta { return cr.colMeta }
func (cr *colReader) Len() int             { return cr.l }

func (cr *colReader) Bools(j int) *array.Boolean {
	execute.CheckColType(cr.colMeta[j], flux.TBool)
	return cr.cols[j].(*array.Boolean)
}

func (cr *colReader) Ints(j int) *array.Int64 {
	execute.CheckColType(cr.colMeta[j], flux.TInt)
	return cr.cols[j].(*array.Int64)
}

func (cr *colReader) UInts(j int) *array.Uint64 {
	execute.CheckColType(cr.colMeta[j], flux.TUInt)
	return cr.cols[j].(*array.Uint64)
}

func (cr *colReader) Floats(j int) *array.Float64 {
	execute.CheckColType(cr.colMeta[j], flux.TFloat)
	return cr.cols[j].(*array.Float64)
}

func (cr *colReader) Strings(j int) *array.Binary {
	execute.CheckColType(cr.colMeta[j], flux.TString)
	return cr.cols[j].(*array.Binary)
}

func (cr *colReader) Times(j int) *array.Int64 {
	execute.CheckColType(cr.colMeta[j], flux.TTime)
	return cr.cols[j].(*array.Int64)
}

// readTags populates b.tags with the provided tags
func (t *table) readTags(tags models.Tags) {
	for j := range t.tags {
		t.tags[j] = t.defs[j]
	}

	if len(tags) == 0 {
		return
	}

	for _, tag := range tags {
		j := execute.ColIdx(string(tag.Key), t.cols)
		t.tags[j] = tag.Value
	}
}

// appendTags fills the colBufs for the tag columns with the tag value.
func (t *table) appendTags(cr *colReader) {
	for j := range t.cols {
		v := t.tags[j]
		if v != nil {
			b := arrow.NewStringBuilder(t.alloc)
			b.Reserve(cr.l)
			b.ReserveData(cr.l * len(v))
			for i := 0; i < cr.l; i++ {
				b.Append(v)
			}
			cr.cols[j] = b.NewArray()
			b.Release()
		}
	}
}

// appendBounds fills the colBufs for the time bounds
func (t *table) appendBounds(cr *colReader) {
	bounds := []execute.Time{t.bounds.Start, t.bounds.Stop}
	for j := range []int{startColIdx, stopColIdx} {
		b := arrow.NewIntBuilder(t.alloc)
		b.Reserve(cr.l)
		for i := 0; i < cr.l; i++ {
			b.UnsafeAppend(int64(bounds[j]))
		}
		cr.cols[j] = b.NewArray()
		b.Release()
	}
}

func (t *table) closeDone() {
	if t.done != nil {
		close(t.done)
		t.done = nil
	}
}

func (t *floatTable) toArrowBuffer(vs []float64) *array.Float64 {
	return arrow.NewFloat(vs, t.alloc)
}
func (t *floatGroupTable) toArrowBuffer(vs []float64) *array.Float64 {
	return arrow.NewFloat(vs, t.alloc)
}
func (t *integerTable) toArrowBuffer(vs []int64) *array.Int64 {
	return arrow.NewInt(vs, t.alloc)
}
func (t *integerGroupTable) toArrowBuffer(vs []int64) *array.Int64 {
	return arrow.NewInt(vs, t.alloc)
}
func (t *unsignedTable) toArrowBuffer(vs []uint64) *array.Uint64 {
	return arrow.NewUint(vs, t.alloc)
}
func (t *unsignedGroupTable) toArrowBuffer(vs []uint64) *array.Uint64 {
	return arrow.NewUint(vs, t.alloc)
}
func (t *stringTable) toArrowBuffer(vs []string) *array.Binary {
	return arrow.NewString(vs, t.alloc)
}
func (t *stringGroupTable) toArrowBuffer(vs []string) *array.Binary {
	return arrow.NewString(vs, t.alloc)
}
func (t *booleanTable) toArrowBuffer(vs []bool) *array.Boolean {
	return arrow.NewBool(vs, t.alloc)
}
func (t *booleanGroupTable) toArrowBuffer(vs []bool) *array.Boolean {
	return arrow.NewBool(vs, t.alloc)
}
