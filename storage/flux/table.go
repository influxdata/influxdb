package storageflux

//go:generate env GO111MODULE=on go run github.com/benbjohnson/tmpl -data=@types.tmpldata table.gen.go.tmpl

import (
	"errors"
	"sync/atomic"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/arrow"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/influxdb/v2/models"
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
	empty   bool

	err error

	cancelled, used int32
	cache           *tagsCache
	alloc           *memory.Allocator
}

func newTable(
	done chan struct{},
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	defs [][]byte,
	cache *tagsCache,
	alloc *memory.Allocator,
) table {
	return table{
		done:   done,
		bounds: bounds,
		key:    key,
		tags:   make([][]byte, len(cols)),
		defs:   defs,
		cols:   cols,
		cache:  cache,
		alloc:  alloc,
	}
}

func (t *table) Key() flux.GroupKey   { return t.key }
func (t *table) Cols() []flux.ColMeta { return t.cols }
func (t *table) Err() error           { return t.err }
func (t *table) Empty() bool          { return t.empty }

func (t *table) Cancel() {
	atomic.StoreInt32(&t.cancelled, 1)
}

func (t *table) isCancelled() bool {
	return atomic.LoadInt32(&t.cancelled) != 0
}

func (t *table) init(advance func() bool) {
	t.empty = !advance() && t.err == nil
}

func (t *table) do(f func(flux.ColReader) error, advance func() bool) error {
	// Mark this table as having been used. If this doesn't
	// succeed, then this has already been invoked somewhere else.
	if !atomic.CompareAndSwapInt32(&t.used, 0, 1) {
		return errors.New("table already used")
	}
	defer t.closeDone()

	// If an error occurred during initialization, that is
	// returned here.
	if t.err != nil {
		return t.err
	}

	if !t.Empty() {
		t.err = f(t.colBufs)
		t.colBufs.Release()

		for !t.isCancelled() && t.err == nil && advance() {
			t.err = f(t.colBufs)
			t.colBufs.Release()
		}
		t.colBufs = nil
	}

	return t.err
}

func (t *table) Done() {
	// Mark the table as having been used. If this has already
	// been done, then nothing needs to be done.
	if atomic.CompareAndSwapInt32(&t.used, 0, 1) {
		defer t.closeDone()
	}

	if t.colBufs != nil {
		t.colBufs.Release()
		t.colBufs = nil
	}
}

// allocateBuffer will allocate a suitable buffer for the
// table implementations to use. If the existing buffer
// is not used anymore, then it may be reused.
//
// The allocated buffer can be accessed at colBufs or
// through the returned colReader.
func (t *table) allocateBuffer(l int) *colReader {
	if t.colBufs == nil || atomic.LoadInt64(&t.colBufs.refCount) > 0 {
		// The current buffer is still being used so we should
		// generate a new one.
		t.colBufs = &colReader{
			key:     t.key,
			colMeta: t.cols,
			cols:    make([]array.Interface, len(t.cols)),
		}
	}
	t.colBufs.refCount = 1
	t.colBufs.l = l
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
		// In the case of group aggregate, tags that are not referenced in group() are not included in the result, but
		// readTags () still get a complete tag list. Here is just to skip the tags that should not be present in the result.
		if j < 0 {
			continue
		}
		t.tags[j] = tag.Value
	}
}

// appendTheseTags fills the colBufs for the tag columns with the given tag values.
func (t *table) appendTheseTags(cr *colReader, tags [][]byte) {
	for j := range t.cols {
		v := tags[j]
		if v != nil {
			cr.cols[j] = t.cache.GetTag(string(v), cr.l, t.alloc)
		}
	}
}

// appendTags fills the colBufs for the tag columns with the tag values from the table structure.
func (t *table) appendTags(cr *colReader) {
	t.appendTheseTags(cr, t.tags)
}

// appendBounds fills the colBufs for the time bounds
func (t *table) appendBounds(cr *colReader) {
	start, stop := t.cache.GetBounds(t.bounds, cr.l, t.alloc)
	cr.cols[startColIdx], cr.cols[stopColIdx] = start, stop
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
func (t *floatWindowSelectorTable) toArrowBuffer(vs []float64) *array.Float64 {
	return arrow.NewFloat(vs, t.alloc)
}
func (t *floatWindowTable) mergeValues(intervals []int64) *array.Float64 {
	b := arrow.NewFloatBuilder(t.alloc)
	b.Resize(len(intervals))
	t.appendValues(intervals, b.Append, b.AppendNull)
	return b.NewFloat64Array()
}
func (t *floatEmptyWindowSelectorTable) arrowBuilder() *array.Float64Builder {
	return arrow.NewFloatBuilder(t.alloc)
}
func (t *floatEmptyWindowSelectorTable) append(builder *array.Float64Builder, v float64) {
	builder.Append(v)
}
func (t *integerTable) toArrowBuffer(vs []int64) *array.Int64 {
	return arrow.NewInt(vs, t.alloc)
}
func (t *integerWindowSelectorTable) toArrowBuffer(vs []int64) *array.Int64 {
	return arrow.NewInt(vs, t.alloc)
}
func (t *integerGroupTable) toArrowBuffer(vs []int64) *array.Int64 {
	return arrow.NewInt(vs, t.alloc)
}
func (t *integerWindowTable) mergeValues(intervals []int64) *array.Int64 {
	b := arrow.NewIntBuilder(t.alloc)
	b.Resize(len(intervals))
	appendNull := b.AppendNull
	if t.fillValue != nil {
		appendNull = func() { b.Append(*t.fillValue) }
	}
	t.appendValues(intervals, b.Append, appendNull)
	return b.NewInt64Array()
}
func (t *integerEmptyWindowSelectorTable) arrowBuilder() *array.Int64Builder {
	return arrow.NewIntBuilder(t.alloc)
}
func (t *integerEmptyWindowSelectorTable) append(builder *array.Int64Builder, v int64) {
	builder.Append(v)
}
func (t *unsignedTable) toArrowBuffer(vs []uint64) *array.Uint64 {
	return arrow.NewUint(vs, t.alloc)
}
func (t *unsignedGroupTable) toArrowBuffer(vs []uint64) *array.Uint64 {
	return arrow.NewUint(vs, t.alloc)
}
func (t *unsignedWindowSelectorTable) toArrowBuffer(vs []uint64) *array.Uint64 {
	return arrow.NewUint(vs, t.alloc)
}
func (t *unsignedWindowTable) mergeValues(intervals []int64) *array.Uint64 {
	b := arrow.NewUintBuilder(t.alloc)
	b.Resize(len(intervals))
	t.appendValues(intervals, b.Append, b.AppendNull)
	return b.NewUint64Array()
}
func (t *unsignedEmptyWindowSelectorTable) arrowBuilder() *array.Uint64Builder {
	return arrow.NewUintBuilder(t.alloc)
}
func (t *unsignedEmptyWindowSelectorTable) append(builder *array.Uint64Builder, v uint64) {
	builder.Append(v)
}
func (t *stringTable) toArrowBuffer(vs []string) *array.Binary {
	return arrow.NewString(vs, t.alloc)
}
func (t *stringGroupTable) toArrowBuffer(vs []string) *array.Binary {
	return arrow.NewString(vs, t.alloc)
}
func (t *stringWindowSelectorTable) toArrowBuffer(vs []string) *array.Binary {
	return arrow.NewString(vs, t.alloc)
}
func (t *stringWindowTable) mergeValues(intervals []int64) *array.Binary {
	b := arrow.NewStringBuilder(t.alloc)
	b.Resize(len(intervals))
	t.appendValues(intervals, b.AppendString, b.AppendNull)
	return b.NewBinaryArray()
}
func (t *stringEmptyWindowSelectorTable) arrowBuilder() *array.BinaryBuilder {
	return arrow.NewStringBuilder(t.alloc)
}
func (t *stringEmptyWindowSelectorTable) append(builder *array.BinaryBuilder, v string) {
	builder.AppendString(v)
}
func (t *booleanTable) toArrowBuffer(vs []bool) *array.Boolean {
	return arrow.NewBool(vs, t.alloc)
}
func (t *booleanGroupTable) toArrowBuffer(vs []bool) *array.Boolean {
	return arrow.NewBool(vs, t.alloc)
}
func (t *booleanWindowSelectorTable) toArrowBuffer(vs []bool) *array.Boolean {
	return arrow.NewBool(vs, t.alloc)
}
func (t *booleanWindowTable) mergeValues(intervals []int64) *array.Boolean {
	b := arrow.NewBoolBuilder(t.alloc)
	b.Resize(len(intervals))
	t.appendValues(intervals, b.Append, b.AppendNull)
	return b.NewBooleanArray()
}
func (t *booleanEmptyWindowSelectorTable) arrowBuilder() *array.BooleanBuilder {
	return arrow.NewBoolBuilder(t.alloc)
}
func (t *booleanEmptyWindowSelectorTable) append(builder *array.BooleanBuilder, v bool) {
	builder.Append(v)
}
