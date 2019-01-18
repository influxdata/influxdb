package querytest

import (
	"context"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb/query/stdlib/influxdata/influxdb"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb/cursors"
)

type mockStore struct {
	cur    reads.SeriesCursor
	bounds execute.Bounds
}

func NewStore(data []*Table, queryBounds execute.Bounds) (*mockStore, error) {
	ms := &mockStore{
		cur: &tableCursor{
			data: data,
		},
		bounds: queryBounds,
	}
	return ms, nil
}

func (ms *mockStore) Read(ctx context.Context, req *datatypes.ReadRequest) (reads.ResultSet, error) {
	req.TimestampRange = datatypes.TimestampRange{
		Start: int64(ms.bounds.Start),
		End:   int64(ms.bounds.Stop),
	}
	return reads.NewResultSet(ctx, req, ms.cur), nil
}

// No grouping gets pushed down to storage, so, this method should never be called.
// Group push down is specified directly into the request, but we hard code it without it in querytest/reader/ReaderTestHelper.
func (*mockStore) GroupRead(ctx context.Context, req *datatypes.ReadRequest) (reads.GroupResultSet, error) {
	panic("mockStore is not designed for GroupRead")
}

func (*mockStore) GetSource(rs influxdb.ReadSpec) (proto.Message, error) {
	return &mockMsg{}, nil
}

type mockMsg struct{}

func (m *mockMsg) Reset() {
	*m = mockMsg{}
}

func (*mockMsg) String() string {
	return "mockMsg()"
}

func (*mockMsg) ProtoMessage() {
}

// ----- Cursors

type tableCursor struct {
	data []*Table
	i    int
}

func (tc *tableCursor) Close()     {}
func (tc *tableCursor) Err() error { return nil }

func (tc *tableCursor) Next() *reads.SeriesRow {
	if tc.i >= len(tc.data) {
		return nil
	}

	tbl := tc.data[tc.i]

	sr := &reads.SeriesRow{}
	sr.Name = []byte(tbl.Measurement)
	sr.Field = tbl.Field

	// add _measurement and _field to tags
	key := []byte("_measurement")
	value := sr.Name
	sr.Tags.Set(key, value)
	key = []byte("_field")
	value = []byte(sr.Field)
	sr.Tags.Set(key, value)

	// add other tags
	for j, l := range tbl.TagLabels {
		key := []byte(l)
		value := []byte(tbl.TagValues[j])
		sr.Tags.Set(key, value)
	}

	sr.SeriesTags = sr.Tags.Clone()

	itr := &cursorIterator{}
	bc := &baseCursor{data: tbl}
	switch tbl.ValueType {
	case flux.TBool:
		itr.c = &boolCursor{bc}
	case flux.TInt:
		itr.c = &intCursor{bc}
	case flux.TUInt:
		itr.c = &uintCursor{bc}
	case flux.TFloat:
		itr.c = &floatCursor{bc}
	case flux.TString:
		itr.c = &stringCursor{bc}
	case flux.TTime:
		itr.c = &intCursor{bc}
	}

	sr.Query = append(sr.Query, itr)

	tc.i++
	return sr
}

type baseCursor struct {
	data *Table
	done bool
}

func (*baseCursor) Stats() cursors.CursorStats {
	return cursors.CursorStats{}
}

func (*baseCursor) Close() {}

func (*baseCursor) Err() error {
	return nil
}

func (bc *baseCursor) more() bool {
	if bc.done {
		return false
	}
	bc.done = true
	return true
}

type cursorIterator struct {
	c    cursors.Cursor
	stop bool
}

func (*cursorIterator) Stats() cursors.CursorStats {
	return cursors.CursorStats{}
}

func (ci *cursorIterator) Next(ctx context.Context, r *cursors.CursorRequest) (cursors.Cursor, error) {
	if ci.stop {
		return nil, nil
	}
	ci.stop = true
	return ci.c, nil
}

type intCursor struct {
	*baseCursor
}

func (c *intCursor) Next() *cursors.IntegerArray {
	if c.more() {
		return &cursors.IntegerArray{
			Timestamps: extractTimes(c.data),
			Values:     extractIntValues(c.data),
		}
	}
	return &cursors.IntegerArray{}
}

type uintCursor struct {
	*baseCursor
}

func (c *uintCursor) Next() *cursors.UnsignedArray {
	if c.more() {
		return &cursors.UnsignedArray{
			Timestamps: extractTimes(c.data),
			Values:     extractUIntValues(c.data),
		}
	}
	return &cursors.UnsignedArray{}
}

type floatCursor struct {
	*baseCursor
}

func (c *floatCursor) Next() *cursors.FloatArray {
	if c.more() {
		return &cursors.FloatArray{
			Timestamps: extractTimes(c.data),
			Values:     extractFloatValues(c.data),
		}
	}
	return &cursors.FloatArray{}
}

type boolCursor struct {
	*baseCursor
}

func (c *boolCursor) Next() *cursors.BooleanArray {
	if c.more() {
		return &cursors.BooleanArray{
			Timestamps: extractTimes(c.data),
			Values:     extractBoolValues(c.data),
		}
	}
	return &cursors.BooleanArray{}
}

type stringCursor struct {
	*baseCursor
}

func (c *stringCursor) Next() *cursors.StringArray {
	if c.more() {
		return &cursors.StringArray{
			Timestamps: extractTimes(c.data),
			Values:     extractStringValues(c.data),
		}
	}

	return &cursors.StringArray{}
}

func extractTimes(data *Table) []int64 {
	s := make([]int64, 0, len(data.Rows))
	for _, row := range data.Rows {
		s = append(s, int64(row.time))
	}
	return s
}

func extractIntValues(data *Table) []int64 {
	s := make([]int64, 0, len(data.Rows))
	for _, row := range data.Rows {
		var v int64
		if ts, ok := row.value.(values.Time); ok {
			v = int64(ts)
		} else {
			v = row.value.(int64)
		}
		s = append(s, v)
	}
	return s
}

func extractUIntValues(data *Table) []uint64 {
	s := make([]uint64, 0, len(data.Rows))
	for _, row := range data.Rows {
		s = append(s, row.value.(uint64))
	}
	return s
}

func extractFloatValues(data *Table) []float64 {
	s := make([]float64, 0, len(data.Rows))
	for _, row := range data.Rows {
		s = append(s, row.value.(float64))
	}
	return s
}

func extractBoolValues(data *Table) []bool {
	s := make([]bool, 0, len(data.Rows))
	for _, row := range data.Rows {
		s = append(s, row.value.(bool))
	}
	return s
}

func extractStringValues(data *Table) []string {
	s := make([]string, 0, len(data.Rows))
	for _, row := range data.Rows {
		s = append(s, row.value.(string))
	}
	return s
}
