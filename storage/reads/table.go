package reads

//go:generate env GO111MODULE=on go run github.com/benbjohnson/tmpl -data=@types.tmpldata table.gen.go.tmpl

import (
	"fmt"
	"sync/atomic"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/arrow"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/cursors"
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

	// The current number of records in memory
	l int

	colBufs []array.Interface
	timeBuf []int64

	err error

	cancelled int32
}

func newTable(
	done chan struct{},
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	defs [][]byte,
) table {
	return table{
		done:    done,
		bounds:  bounds,
		key:     key,
		tags:    make([][]byte, len(cols)),
		defs:    defs,
		colBufs: make([]array.Interface, len(cols)),
		cols:    cols,
	}
}

func (t *table) Statistics() flux.Statistics {
	return flux.Statistics{}
}

func (t *table) Key() flux.GroupKey   { return t.key }
func (t *table) Cols() []flux.ColMeta { return t.cols }
func (t *table) RefCount(n int)       {}
func (t *table) Err() error           { return t.err }
func (t *table) Empty() bool          { return t.l == 0 }
func (t *table) Len() int             { return t.l }

func (t *table) Cancel() {
	atomic.StoreInt32(&t.cancelled, 1)
}

func (t *table) isCancelled() bool {
	return atomic.LoadInt32(&t.cancelled) != 0
}

func (t *table) Bools(j int) *array.Boolean {
	execute.CheckColType(t.cols[j], flux.TBool)
	return t.colBufs[j].(*array.Boolean)
}

func (t *table) Ints(j int) *array.Int64 {
	execute.CheckColType(t.cols[j], flux.TInt)
	return t.colBufs[j].(*array.Int64)
}

func (t *table) UInts(j int) *array.Uint64 {
	execute.CheckColType(t.cols[j], flux.TUInt)
	return t.colBufs[j].(*array.Uint64)
}

func (t *table) Floats(j int) *array.Float64 {
	execute.CheckColType(t.cols[j], flux.TFloat)
	return t.colBufs[j].(*array.Float64)
}

func (t *table) Strings(j int) *array.Binary {
	execute.CheckColType(t.cols[j], flux.TString)
	return t.colBufs[j].(*array.Binary)
}

func (t *table) Times(j int) *array.Int64 {
	execute.CheckColType(t.cols[j], flux.TTime)
	return t.colBufs[j].(*array.Int64)
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
func (t *table) appendTags() {
	for j := range t.cols {
		v := t.tags[j]
		if v != nil {
			b := arrow.NewStringBuilder(&memory.Allocator{})
			b.Reserve(t.l)
			for i := 0; i < t.l; i++ {
				b.Append(v)
			}
			t.colBufs[j] = b.NewArray()
			b.Release()
		}
	}
}

// appendBounds fills the colBufs for the time bounds
func (t *table) appendBounds() {
	bounds := []execute.Time{t.bounds.Start, t.bounds.Stop}
	for j := range []int{startColIdx, stopColIdx} {
		b := arrow.NewIntBuilder(&memory.Allocator{})
		b.Reserve(t.l)
		for i := 0; i < t.l; i++ {
			b.UnsafeAppend(int64(bounds[j]))
		}
		t.colBufs[j] = b.NewArray()
		b.Release()
	}
}

func (t *table) closeDone() {
	if t.done != nil {
		close(t.done)
		t.done = nil
	}
}

// hasPoints returns true if the next block from cur has data. If cur is not
// nil, it will be closed.
func hasPoints(cur cursors.Cursor) bool {
	if cur == nil {
		return false
	}

	res := false
	switch cur := cur.(type) {
	case cursors.IntegerArrayCursor:
		a := cur.Next()
		res = a.Len() > 0
	case cursors.FloatArrayCursor:
		a := cur.Next()
		res = a.Len() > 0
	case cursors.UnsignedArrayCursor:
		a := cur.Next()
		res = a.Len() > 0
	case cursors.BooleanArrayCursor:
		a := cur.Next()
		res = a.Len() > 0
	case cursors.StringArrayCursor:
		a := cur.Next()
		res = a.Len() > 0
	default:
		panic(fmt.Sprintf("unreachable: %T", cur))
	}
	cur.Close()
	return res
}

type tableNoPoints struct {
	table
}

func newTableNoPoints(
	done chan struct{},
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
) *tableNoPoints {
	t := &tableNoPoints{
		table: newTable(done, bounds, key, cols, defs),
	}
	t.readTags(tags)

	return t
}

func (t *tableNoPoints) Close() {}

func (t *tableNoPoints) Statistics() flux.Statistics { return flux.Statistics{} }

func (t *tableNoPoints) Do(f func(flux.ColReader) error) error {
	if t.isCancelled() {
		return nil
	}
	t.err = f(t)
	t.closeDone()
	return t.err
}

type groupTableNoPoints struct {
	table
}

func newGroupTableNoPoints(
	done chan struct{},
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	defs [][]byte,
) *groupTableNoPoints {
	t := &groupTableNoPoints{
		table: newTable(done, bounds, key, cols, defs),
	}

	return t
}

func (t *groupTableNoPoints) Close() {}

func (t *groupTableNoPoints) Do(f func(flux.ColReader) error) error {
	if t.isCancelled() {
		return nil
	}
	t.err = f(t)
	t.closeDone()
	return t.err
}

func (t *groupTableNoPoints) Statistics() flux.Statistics { return flux.Statistics{} }

func (t *floatTable) toArrowBuffer(vs []float64) *array.Float64 {
	return arrow.NewFloat(vs, &memory.Allocator{})
}
func (t *floatGroupTable) toArrowBuffer(vs []float64) *array.Float64 {
	return arrow.NewFloat(vs, &memory.Allocator{})
}
func (t *integerTable) toArrowBuffer(vs []int64) *array.Int64 {
	return arrow.NewInt(vs, &memory.Allocator{})
}
func (t *integerGroupTable) toArrowBuffer(vs []int64) *array.Int64 {
	return arrow.NewInt(vs, &memory.Allocator{})
}
func (t *unsignedTable) toArrowBuffer(vs []uint64) *array.Uint64 {
	return arrow.NewUint(vs, &memory.Allocator{})
}
func (t *unsignedGroupTable) toArrowBuffer(vs []uint64) *array.Uint64 {
	return arrow.NewUint(vs, &memory.Allocator{})
}
func (t *stringTable) toArrowBuffer(vs []string) *array.Binary {
	return arrow.NewString(vs, &memory.Allocator{})
}
func (t *stringGroupTable) toArrowBuffer(vs []string) *array.Binary {
	return arrow.NewString(vs, &memory.Allocator{})
}
func (t *booleanTable) toArrowBuffer(vs []bool) *array.Boolean {
	return arrow.NewBool(vs, &memory.Allocator{})
}
func (t *booleanGroupTable) toArrowBuffer(vs []bool) *array.Boolean {
	return arrow.NewBool(vs, &memory.Allocator{})
}
