package reads

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
	"github.com/influxdata/influxdb/v2/v1/tsdb/cursors"
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
	km            KeyMerger

	newSeriesCursorFn func() (SeriesCursor, error)
	nextGroupFn       func(c *groupResultSet) GroupCursor

	eof bool
}

type GroupOption func(g *groupResultSet)

// GroupOptionNilSortLo configures nil values to be sorted lower than any
// other value
func GroupOptionNilSortLo() GroupOption {
	return func(g *groupResultSet) {
		g.nilSort = NilSortLo
	}
}

func NewGroupResultSet(ctx context.Context, req *datatypes.ReadGroupRequest, newSeriesCursorFn func() (SeriesCursor, error), opts ...GroupOption) GroupResultSet {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()
	span.LogKV("group_type", req.Group.String())

	g := &groupResultSet{
		ctx:               ctx,
		req:               req,
		keys:              make([][]byte, len(req.GroupKeys)),
		nilSort:           NilSortHi,
		newSeriesCursorFn: newSeriesCursorFn,
	}

	for _, o := range opts {
		o(g)
	}

	g.arrayCursors = newArrayCursors(
		ctx,
		req.Range.Start,
		req.Range.End,
		// The following is an optimization where the selector `last`
		// is implemented as a descending array cursor followed by a
		// limit array cursor that selects only the first point, i.e
		// the point with the largest timestamp, from the descending
		// array cursor.
		req.Aggregate == nil || req.Aggregate.Type != datatypes.AggregateTypeLast,
	)

	for i, k := range req.GroupKeys {
		g.keys[i] = []byte(k)
	}

	switch req.Group {
	case datatypes.GroupBy:
		g.nextGroupFn = groupByNextGroup
		g.groupByCursor = groupByCursor{
			ctx:          ctx,
			arrayCursors: g.arrayCursors,
			agg:          req.Aggregate,
			vals:         make([][]byte, len(req.GroupKeys)),
		}

		if n, err := g.groupBySort(); n == 0 || err != nil {
			return nil
		} else {
			span.LogKV("rows", n)
		}

	case datatypes.GroupNone:
		g.nextGroupFn = groupNoneNextGroup

		if n, err := g.groupNoneSort(); n == 0 || err != nil {
			return nil
		} else {
			span.LogKV("rows", n)
		}

	default:
		panic("not implemented")
	}

	return g
}

// NilSort values determine the lexicographical order of nil values in the
// partition key
var (
	// nil sorts lowest
	NilSortLo = []byte{0x00}
	// nil sorts highest
	NilSortHi = []byte{0xff}
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
		cur:          seriesCursor,
		keys:         g.km.Get(),
	}
}

func (g *groupResultSet) groupNoneSort() (int, error) {
	seriesCursor, err := g.newSeriesCursorFn()
	if err != nil {
		return 0, err
	} else if seriesCursor == nil {
		return 0, nil
	}

	allTime := g.req.Hints.HintSchemaAllTime()
	g.km.Clear()
	n := 0
	seriesRow := seriesCursor.Next()
	for seriesRow != nil {
		if allTime || g.seriesHasPoints(seriesRow) {
			n++
			g.km.MergeTagKeys(seriesRow.Tags)
		}
		seriesRow = seriesCursor.Next()
	}

	seriesCursor.Close()
	return n, nil
}

func groupByNextGroup(g *groupResultSet) GroupCursor {
	row := g.seriesRows[g.i]
	for i := range g.keys {
		g.groupByCursor.vals[i] = row.Tags.Get(g.keys[i])
	}

	g.km.Clear()
	rowKey := row.SortKey
	j := g.i
	for j < len(g.seriesRows) && bytes.Equal(rowKey, g.seriesRows[j].SortKey) {
		g.km.MergeTagKeys(g.seriesRows[j].Tags)
		j++
	}

	g.groupByCursor.reset(g.seriesRows[g.i:j])
	g.groupByCursor.keys = g.km.Get()

	g.i = j
	if j == len(g.seriesRows) {
		g.eof = true
	}

	return &g.groupByCursor
}

func (g *groupResultSet) groupBySort() (int, error) {
	seriesCursor, err := g.newSeriesCursorFn()
	if err != nil {
		return 0, err
	} else if seriesCursor == nil {
		return 0, nil
	}

	var seriesRows []*SeriesRow
	vals := make([][]byte, len(g.keys))
	tagsBuf := &tagsBuffer{sz: 4096}
	allTime := g.req.Hints.HintSchemaAllTime()

	seriesRow := seriesCursor.Next()
	for seriesRow != nil {
		if allTime || g.seriesHasPoints(seriesRow) {
			nr := *seriesRow
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
				// separate sort key values with ascii null character
				nr.SortKey = append(nr.SortKey, '\000')
			}

			seriesRows = append(seriesRows, &nr)
		}
		seriesRow = seriesCursor.Next()
	}

	sort.Slice(seriesRows, func(i, j int) bool {
		return bytes.Compare(seriesRows[i].SortKey, seriesRows[j].SortKey) == -1
	})

	g.seriesRows = seriesRows

	seriesCursor.Close()
	return len(seriesRows), nil
}

type groupNoneCursor struct {
	ctx          context.Context
	arrayCursors *arrayCursors
	agg          *datatypes.Aggregate
	cur          SeriesCursor
	row          SeriesRow
	keys         [][]byte
}

func (c *groupNoneCursor) Err() error                 { return nil }
func (c *groupNoneCursor) Tags() models.Tags          { return c.row.Tags }
func (c *groupNoneCursor) Keys() [][]byte             { return c.keys }
func (c *groupNoneCursor) PartitionKeyVals() [][]byte { return nil }
func (c *groupNoneCursor) Close()                     { c.cur.Close() }
func (c *groupNoneCursor) Stats() cursors.CursorStats { return c.row.Query.Stats() }

func (c *groupNoneCursor) Aggregate() *datatypes.Aggregate {
	return c.agg
}

func (c *groupNoneCursor) Next() bool {
	row := c.cur.Next()
	if row == nil {
		return false
	}

	c.row = *row

	return true
}

func (c *groupNoneCursor) Cursor() cursors.Cursor {
	cur := c.arrayCursors.createCursor(c.row)
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
	seriesRows   []*SeriesRow
	keys         [][]byte
	vals         [][]byte
}

func (c *groupByCursor) reset(seriesRows []*SeriesRow) {
	c.i = 0
	c.seriesRows = seriesRows
}

func (c *groupByCursor) Err() error                 { return nil }
func (c *groupByCursor) Keys() [][]byte             { return c.keys }
func (c *groupByCursor) PartitionKeyVals() [][]byte { return c.vals }
func (c *groupByCursor) Tags() models.Tags          { return c.seriesRows[c.i-1].Tags }
func (c *groupByCursor) Close()                     {}

func (c *groupByCursor) Aggregate() *datatypes.Aggregate {
	return c.agg
}

func (c *groupByCursor) Next() bool {
	if c.i < len(c.seriesRows) {
		c.i++
		return true
	}
	return false
}

func (c *groupByCursor) Cursor() cursors.Cursor {
	cur := c.arrayCursors.createCursor(*c.seriesRows[c.i-1])
	if c.agg != nil {
		cur = newAggregateArrayCursor(c.ctx, c.agg, cur)
	}
	return cur
}

func (c *groupByCursor) Stats() cursors.CursorStats {
	var stats cursors.CursorStats
	for _, seriesRow := range c.seriesRows {
		stats.Add(seriesRow.Query.Stats())
	}
	return stats
}
