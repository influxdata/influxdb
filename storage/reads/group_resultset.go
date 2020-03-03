package reads

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/influxdata/influxdb/kit/tracing"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb/cursors"
)

type groupResultSet struct {
	ctx          context.Context
	req          *datatypes.ReadGroupRequest
	arrayCursors *arrayCursors

	i             int
	seriesRows    []*SeriesRow
	keys          [][]byte
	nilSort       []byte
	groupByCursor groupByCursor
	km            keyMerger

	newSeriesCursorFn func() (SeriesCursor, error)
	nextGroupFn       func(c *groupResultSet) GroupCursor

	eof bool
}

type GroupOption func(g *groupResultSet)

// GroupOptionNilSortLo configures nil values to be sorted lower than any
// other value
func GroupOptionNilSortLo() GroupOption {
	return func(g *groupResultSet) {
		g.nilSort = nilSortLo
	}
}

func NewGroupResultSet(ctx context.Context, req *datatypes.ReadGroupRequest, newCursorFn func() (SeriesCursor, error), opts ...GroupOption) GroupResultSet {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	g := &groupResultSet{
		ctx:               ctx,
		req:               req,
		arrayCursors:      newArrayCursors(ctx, req.Range.Start, req.Range.End, true),
		keys:              make([][]byte, len(req.GroupKeys)),
		nilSort:           nilSortHi,
		newSeriesCursorFn: newCursorFn,
	}

	for _, o := range opts {
		o(g)
	}

	for i, k := range req.GroupKeys {
		g.keys[i] = []byte(k)
	}

	var n int
	var err error

	span.LogKV("group_type", req.Group.String())
	switch req.Group {
	case datatypes.GroupBy:
		g.nextGroupFn = groupByNextGroup
		g.groupByCursor = groupByCursor{
			ctx:          ctx,
			arrayCursors: g.arrayCursors,
			agg:          req.Aggregate,
			vals:         make([][]byte, len(req.GroupKeys)),
		}
		n, err = g.groupBySort()

	case datatypes.GroupNone:
		g.nextGroupFn = groupNoneNextGroup
		n, err = g.groupNoneSort()

	default:
		panic(fmt.Sprintf("%s not implemented", req.Group.String()))
	}

	if n == 0 || err != nil {
		return nil
	}
	span.LogKV("rows", n)

	return g
}

// nilSort values determine the lexicographical order of nil values in the
// partition key
var (
	// nil sorts lowest
	nilSortLo = []byte{0x00}
	// nil sorts highest
	nilSortHi = []byte{0xff} // sort nil values
)

func (g *groupResultSet) Err() error { return nil }

func (g *groupResultSet) Close() {}

func (g *groupResultSet) Next() GroupCursor {
	if g.eof {
		return nil
	}

	return g.nextGroupFn(g)
}

// seriesHasPoints reads the first block of TSM data to verify the series has points for
// the time range of the query.
func (g *groupResultSet) seriesHasPoints(row *SeriesRow) bool {
	// TODO(sgc): this is expensive. Storage engine must provide efficient time range queries of series keys.
	cur := g.arrayCursors.createCursor(*row)
	var ts []int64
	switch c := cur.(type) {
	case cursors.IntegerArrayCursor:
		a := c.Next()
		ts = a.Timestamps
	case cursors.FloatArrayCursor:
		a := c.Next()
		ts = a.Timestamps
	case cursors.UnsignedArrayCursor:
		a := c.Next()
		ts = a.Timestamps
	case cursors.BooleanArrayCursor:
		a := c.Next()
		ts = a.Timestamps
	case cursors.StringArrayCursor:
		a := c.Next()
		ts = a.Timestamps
	case nil:
		return false
	default:
		panic(fmt.Sprintf("unreachable: %T", c))
	}
	cur.Close()
	return len(ts) > 0
}

func groupNoneNextGroup(g *groupResultSet) GroupCursor {
	seriesCursor, err := g.newSeriesCursorFn()
	if err != nil {
		// TODO(sgc): store error
		return nil
	} else if seriesCursor == nil {
		return nil
	}

	g.eof = true
	return &groupNoneCursor{
		ctx:          g.ctx,
		arrayCursors: g.arrayCursors,
		agg:          g.req.Aggregate,
		seriesCursor: seriesCursor,
		keys:         g.km.get(),
	}
}

func (g *groupResultSet) groupNoneSort() (int, error) {
	seriesCursor, err := g.newSeriesCursorFn()
	if err != nil {
		return 0, err
	} else if seriesCursor == nil {
		return 0, nil
	}
	defer seriesCursor.Close()

	allTime := g.req.Hints.HintSchemaAllTime()
	g.km.clear()
	n := 0
	for seriesRow := seriesCursor.Next(); seriesRow != nil; seriesRow = seriesCursor.Next() {
		if allTime || g.seriesHasPoints(seriesRow) {
			n++
			g.km.mergeTagKeys(seriesRow.Tags)
		}
	}

	return n, nil
}

func groupByNextGroup(g *groupResultSet) GroupCursor {
	row := g.seriesRows[g.i]
	for i := range g.keys {
		g.groupByCursor.vals[i] = row.Tags.Get(g.keys[i])
	}

	g.km.clear()
	rowKey := row.SortKey
	j := g.i
	for j < len(g.seriesRows) && bytes.Equal(rowKey, g.seriesRows[j].SortKey) {
		g.km.mergeTagKeys(g.seriesRows[j].Tags)
		j++
	}

	g.groupByCursor.reset(g.seriesRows[g.i:j])
	g.groupByCursor.keys = g.km.get()

	g.i = j
	if j == len(g.seriesRows) {
		g.eof = true
	}

	return &g.groupByCursor
}

// groupBySort retrieves all SeriesRows from the series cursor,
// makes a defensive copy of their Tags and SeriesTags,
// sorts them, and stores them in g.seriesRows.
func (g *groupResultSet) groupBySort() (int, error) {
	seriesCursor, err := g.newSeriesCursorFn()
	if err != nil {
		return 0, err
	} else if seriesCursor == nil {
		return 0, nil
	}
	defer seriesCursor.Close()

	var seriesRows []*SeriesRow
	vals := make([][]byte, len(g.keys))
	tagsBuf := &tagsBuffer{sz: 4096}
	allTime := g.req.Hints.HintSchemaAllTime()

	for seriesRow := seriesCursor.Next(); seriesRow != nil; seriesRow = seriesCursor.Next() {
		if !allTime && !g.seriesHasPoints(seriesRow) {
			continue
		}
		nr := *seriesRow
		nr.SeriesTags = tagsBuf.copyTags(nr.SeriesTags) // TODO(jacobmarble): Why?
		nr.Tags = tagsBuf.copyTags(nr.Tags)

		l := len(g.keys) // for sort key separators
		for i, k := range g.keys {
			vals[i] = nr.Tags.Get(k)
			if len(vals[i]) == 0 {
				vals[i] = g.nilSort
			}
			l += len(vals[i])
		}

		nr.SortKey = make([]byte, 0, l)
		for _, v := range vals {
			nr.SortKey = append(nr.SortKey, v...)
			nr.SortKey = append(nr.SortKey, ',')
		}

		seriesRows = append(seriesRows, &nr)
	}

	sort.Slice(seriesRows, func(i, j int) bool {
		return bytes.Compare(seriesRows[i].SortKey, seriesRows[j].SortKey) == -1
	})

	g.seriesRows = seriesRows

	return len(seriesRows), nil
}

type groupNoneCursor struct {
	ctx          context.Context
	arrayCursors *arrayCursors
	agg          *datatypes.Aggregate
	seriesCursor SeriesCursor
	seriesRow    SeriesRow
	keys         [][]byte
}

func (c *groupNoneCursor) Err() error                 { return nil }
func (c *groupNoneCursor) Tags() models.Tags          { return c.seriesRow.Tags }
func (c *groupNoneCursor) Keys() [][]byte             { return c.keys }
func (c *groupNoneCursor) PartitionKeyVals() [][]byte { return nil }
func (c *groupNoneCursor) Close()                     { c.seriesCursor.Close() }
func (c *groupNoneCursor) Stats() cursors.CursorStats { return c.seriesRow.Query.Stats() }

func (c *groupNoneCursor) Next() bool {
	seriesRow := c.seriesCursor.Next()
	if seriesRow == nil {
		return false
	}

	c.seriesRow = *seriesRow

	return true
}

func (c *groupNoneCursor) Cursor() cursors.Cursor {
	cur := c.arrayCursors.createCursor(c.seriesRow)
	if c.agg != nil {
		cur = newAggregateArrayCursor(c.ctx, c.agg, cur)
	}
	return cur
}

type groupByCursor struct {
	ctx          context.Context
	arrayCursors *arrayCursors
	agg          *datatypes.Aggregate
	i            int
	rows         []*SeriesRow
	keys         [][]byte
	vals         [][]byte
}

func (c *groupByCursor) reset(rows []*SeriesRow) {
	c.i = 0
	c.rows = rows
}

func (c *groupByCursor) Err() error                 { return nil }
func (c *groupByCursor) Keys() [][]byte             { return c.keys }
func (c *groupByCursor) PartitionKeyVals() [][]byte { return c.vals }
func (c *groupByCursor) Tags() models.Tags          { return c.rows[c.i-1].Tags }
func (c *groupByCursor) Close()                     {}

func (c *groupByCursor) Next() bool {
	if c.i < len(c.rows) {
		c.i++
		return true
	}
	return false
}

func (c *groupByCursor) Cursor() cursors.Cursor {
	cur := c.arrayCursors.createCursor(*c.rows[c.i-1])
	if c.agg != nil {
		cur = newAggregateArrayCursor(c.ctx, c.agg, cur)
	}
	return cur
}

func (c *groupByCursor) Stats() cursors.CursorStats {
	var stats cursors.CursorStats
	for _, row := range c.rows {
		stats.Add(row.Query.Stats())
	}
	return stats
}
