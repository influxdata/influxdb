package reads

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb/cursors"
)

type groupResultSet struct {
	ctx context.Context
	req *datatypes.ReadGroupRequest
	agg *datatypes.Aggregate
	mb  multiShardCursors

	i       int
	rows    []*SeriesRow
	keys    [][]byte
	nilSort []byte
	rgc     groupByCursor
	km      keyMerger

	newCursorFn func() (SeriesCursor, error)
	nextGroupFn func(c *groupResultSet) GroupCursor
	sortFn      func(c *groupResultSet) (int, error)

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
	g := &groupResultSet{
		ctx:         ctx,
		req:         req,
		agg:         req.Aggregate,
		keys:        make([][]byte, len(req.GroupKeys)),
		nilSort:     nilSortHi,
		newCursorFn: newCursorFn,
	}

	for _, o := range opts {
		o(g)
	}

	g.mb = newMultiShardArrayCursors(ctx, req.Range.Start, req.Range.End, true, math.MaxInt64)

	for i, k := range req.GroupKeys {
		g.keys[i] = []byte(k)
	}

	switch req.Group {
	case datatypes.GroupBy:
		g.sortFn = groupBySort
		g.nextGroupFn = groupByNextGroup
		g.rgc = groupByCursor{
			ctx:  ctx,
			mb:   g.mb,
			agg:  req.Aggregate,
			vals: make([][]byte, len(req.GroupKeys)),
		}

	case datatypes.GroupNone:
		g.sortFn = groupNoneSort
		g.nextGroupFn = groupNoneNextGroup

	default:
		panic("not implemented")
	}

	n, err := g.sort()
	if n == 0 || err != nil {
		return nil
	}

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

func (g *groupResultSet) sort() (int, error) {
	n, err := g.sortFn(g)
	return n, err
}

// seriesHasPoints reads the first block of TSM data to verify the series has points for
// the time range of the query.
func (g *groupResultSet) seriesHasPoints(row *SeriesRow) bool {
	// TODO(sgc): this is expensive. Storage engine must provide efficient time range queries of series keys.
	cur := g.mb.createCursor(*row)
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
	cur, err := g.newCursorFn()
	if err != nil {
		// TODO(sgc): store error
		return nil
	} else if cur == nil {
		return nil
	}

	g.eof = true
	return &groupNoneCursor{
		ctx:  g.ctx,
		mb:   g.mb,
		agg:  g.agg,
		cur:  cur,
		keys: g.km.get(),
	}
}

func groupNoneSort(g *groupResultSet) (int, error) {
	cur, err := g.newCursorFn()
	if err != nil {
		return 0, err
	} else if cur == nil {
		return 0, nil
	}

	allTime := g.req.Hints.HintSchemaAllTime()
	g.km.clear()
	n := 0
	row := cur.Next()
	for row != nil {
		if allTime || g.seriesHasPoints(row) {
			n++
			g.km.mergeTagKeys(row.Tags)
		}
		row = cur.Next()
	}

	cur.Close()
	return n, nil
}

func groupByNextGroup(g *groupResultSet) GroupCursor {
	row := g.rows[g.i]
	for i := range g.keys {
		g.rgc.vals[i] = row.Tags.Get(g.keys[i])
	}

	g.km.clear()
	rowKey := row.SortKey
	j := g.i
	for j < len(g.rows) && bytes.Equal(rowKey, g.rows[j].SortKey) {
		g.km.mergeTagKeys(g.rows[j].Tags)
		j++
	}

	g.rgc.reset(g.rows[g.i:j])
	g.rgc.keys = g.km.get()

	g.i = j
	if j == len(g.rows) {
		g.eof = true
	}

	return &g.rgc
}

func groupBySort(g *groupResultSet) (int, error) {
	cur, err := g.newCursorFn()
	if err != nil {
		return 0, err
	} else if cur == nil {
		return 0, nil
	}

	var rows []*SeriesRow
	vals := make([][]byte, len(g.keys))
	tagsBuf := &tagsBuffer{sz: 4096}
	allTime := g.req.Hints.HintSchemaAllTime()

	row := cur.Next()
	for row != nil {
		if allTime || g.seriesHasPoints(row) {
			nr := *row
			nr.SeriesTags = tagsBuf.copyTags(nr.SeriesTags)
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

			rows = append(rows, &nr)
		}
		row = cur.Next()
	}

	sort.Slice(rows, func(i, j int) bool {
		return bytes.Compare(rows[i].SortKey, rows[j].SortKey) == -1
	})

	g.rows = rows

	cur.Close()
	return len(rows), nil
}

type groupNoneCursor struct {
	ctx  context.Context
	mb   multiShardCursors
	agg  *datatypes.Aggregate
	cur  SeriesCursor
	row  SeriesRow
	keys [][]byte
}

func (c *groupNoneCursor) Err() error                 { return nil }
func (c *groupNoneCursor) Tags() models.Tags          { return c.row.Tags }
func (c *groupNoneCursor) Keys() [][]byte             { return c.keys }
func (c *groupNoneCursor) PartitionKeyVals() [][]byte { return nil }
func (c *groupNoneCursor) Close()                     { c.cur.Close() }
func (c *groupNoneCursor) Stats() cursors.CursorStats { return c.row.Query.Stats() }

func (c *groupNoneCursor) Next() bool {
	row := c.cur.Next()
	if row == nil {
		return false
	}

	c.row = *row

	return true
}

func (c *groupNoneCursor) Cursor() cursors.Cursor {
	cur := c.mb.createCursor(c.row)
	if c.agg != nil {
		cur = c.mb.newAggregateCursor(c.ctx, c.agg, cur)
	}
	return cur
}

type groupByCursor struct {
	ctx  context.Context
	mb   multiShardCursors
	agg  *datatypes.Aggregate
	i    int
	rows []*SeriesRow
	keys [][]byte
	vals [][]byte
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
	cur := c.mb.createCursor(*c.rows[c.i-1])
	if c.agg != nil {
		cur = c.mb.newAggregateCursor(c.ctx, c.agg, cur)
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
