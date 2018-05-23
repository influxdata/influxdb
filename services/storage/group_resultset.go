package storage

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"
)

type GroupCursor interface {
	Tags() models.Tags
	Keys() [][]byte
	PartitionKeyVals() [][]byte
	Next() bool
	Cursor() tsdb.Cursor
	Close()
}

type groupResultSet struct {
	ctx context.Context
	req *ReadRequest
	rr  readRequest
	mb  *multiShardBatchCursors

	i    int
	rows []*seriesRow
	keys [][]byte
	rgc  groupByCursor
	km   keyMerger

	newCursorFn func() (seriesCursor, error)
	nextGroupFn func(c *groupResultSet) GroupCursor
	sortFn      func(c *groupResultSet) (int, error)

	eof bool
}

func newGroupResultSet(ctx context.Context, req *ReadRequest, newCursorFn func() (seriesCursor, error)) *groupResultSet {
	g := &groupResultSet{
		ctx: ctx,
		req: req,
		rr: readRequest{
			ctx:       ctx,
			start:     req.TimestampRange.Start,
			end:       req.TimestampRange.End,
			asc:       !req.Descending,
			limit:     req.PointsLimit,
			aggregate: req.Aggregate,
		},
		keys:        make([][]byte, len(req.GroupKeys)),
		newCursorFn: newCursorFn,
	}

	g.mb = newMultiShardBatchCursors(ctx, &g.rr)

	for i, k := range req.GroupKeys {
		g.keys[i] = []byte(k)
	}

	switch req.Group {
	case GroupBy:
		g.sortFn = groupBySort
		g.nextGroupFn = groupByNextGroup
		g.rgc = groupByCursor{
			mb:   g.mb,
			req:  &g.rr,
			vals: make([][]byte, len(req.GroupKeys)),
		}

	case GroupNone:
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

var nilKey = [...]byte{0xff}

func (g *groupResultSet) Close() {}

func (g *groupResultSet) Next() GroupCursor {
	if g.eof {
		return nil
	}

	return g.nextGroupFn(g)
}

func (g *groupResultSet) sort() (int, error) {
	log := logger.LoggerFromContext(g.ctx)
	if log != nil {
		var f func()
		log, f = logger.NewOperation(log, "Sort", "group.sort", zap.String("group_type", g.req.Group.String()))
		defer f()
	}

	span := opentracing.SpanFromContext(g.ctx)
	if span != nil {
		span = opentracing.StartSpan(
			"group.sort",
			opentracing.ChildOf(span.Context()),
			opentracing.Tag{Key: "group_type", Value: g.req.Group.String()})
		defer span.Finish()
	}

	n, err := g.sortFn(g)

	if span != nil {
		span.SetTag("rows", n)
	}

	if log != nil {
		log.Info("Sort completed", zap.Int("rows", n))
	}

	return n, err
}

// seriesHasPoints reads the first block of TSM data to verify the series has points for
// the time range of the query.
func (g *groupResultSet) seriesHasPoints(row *seriesRow) bool {
	// TODO(sgc): this is expensive. Storage engine must provide efficient time range queries of series keys.
	cur := g.mb.createCursor(*row)
	var ts []int64
	switch c := cur.(type) {
	case tsdb.IntegerBatchCursor:
		ts, _ = c.Next()
	case tsdb.FloatBatchCursor:
		ts, _ = c.Next()
	case tsdb.UnsignedBatchCursor:
		ts, _ = c.Next()
	case tsdb.BooleanBatchCursor:
		ts, _ = c.Next()
	case tsdb.StringBatchCursor:
		ts, _ = c.Next()
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
		mb:   g.mb,
		req:  &g.rr,
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

	n := 0
	row := cur.Next()
	g.km.setTags(row.tags)

	allTime := g.req.Hints.HintSchemaAllTime()

	for {
		n++
		row = cur.Next()
		if row == nil {
			break
		}
		if allTime || g.seriesHasPoints(row) {
			g.km.mergeTagKeys(row.tags)
		}
	}

	cur.Close()
	return n, nil
}

func groupByNextGroup(g *groupResultSet) GroupCursor {
	row := g.rows[g.i]
	for i := range g.keys {
		g.rgc.vals[i] = row.tags.Get(g.keys[i])
	}

	allTime := g.req.Hints.HintSchemaAllTime()

	rowKey := row.sortKey
	g.km.setTags(row.tags)
	j := g.i + 1
	for j < len(g.rows) && bytes.Equal(rowKey, g.rows[j].sortKey) {
		if allTime || g.seriesHasPoints(g.rows[j]) {
			g.km.mergeTagKeys(g.rows[j].tags)
		}
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

	var rows []*seriesRow
	vals := make([][]byte, len(g.keys))
	tagsBuf := &tagsBuffer{sz: 4096}

	row := cur.Next()
	for row != nil {
		nr := *row
		nr.stags = tagsBuf.copyTags(nr.stags)
		nr.tags = tagsBuf.copyTags(nr.tags)

		l := 0
		for i, k := range g.keys {
			vals[i] = nr.tags.Get(k)
			if len(vals[i]) == 0 {
				vals[i] = nilKey[:] // if there was no value, ensure it sorts last
			}
			l += len(vals[i])
		}

		nr.sortKey = make([]byte, 0, l)
		for _, v := range vals {
			nr.sortKey = append(nr.sortKey, v...)
		}

		rows = append(rows, &nr)
		row = cur.Next()
	}

	sort.Slice(rows, func(i, j int) bool {
		return bytes.Compare(rows[i].sortKey, rows[j].sortKey) == -1
	})

	g.rows = rows

	cur.Close()
	return len(rows), nil
}

type groupNoneCursor struct {
	mb   *multiShardBatchCursors
	req  *readRequest
	cur  seriesCursor
	row  seriesRow
	keys [][]byte
}

func (c *groupNoneCursor) Tags() models.Tags          { return c.row.tags }
func (c *groupNoneCursor) Keys() [][]byte             { return c.keys }
func (c *groupNoneCursor) PartitionKeyVals() [][]byte { return nil }
func (c *groupNoneCursor) Close()                     { c.cur.Close() }

func (c *groupNoneCursor) Next() bool {
	row := c.cur.Next()
	if row == nil {
		return false
	}

	c.row = *row

	return true
}

func (c *groupNoneCursor) Cursor() tsdb.Cursor {
	cur := c.mb.createCursor(c.row)
	if c.req.aggregate != nil {
		cur = newAggregateBatchCursor(c.req.ctx, c.req.aggregate, cur)
	}
	return cur
}

type groupByCursor struct {
	mb   *multiShardBatchCursors
	req  *readRequest
	i    int
	rows []*seriesRow
	keys [][]byte
	vals [][]byte
}

func (c *groupByCursor) reset(rows []*seriesRow) {
	c.i = 0
	c.rows = rows
}

func (c *groupByCursor) Keys() [][]byte             { return c.keys }
func (c *groupByCursor) PartitionKeyVals() [][]byte { return c.vals }
func (c *groupByCursor) Tags() models.Tags          { return c.rows[c.i-1].tags }
func (c *groupByCursor) Close()                     {}

func (c *groupByCursor) Next() bool {
	if c.i < len(c.rows) {
		c.i++
		return true
	}
	return false
}

func (c *groupByCursor) Cursor() tsdb.Cursor {
	cur := c.mb.createCursor(*c.rows[c.i-1])
	if c.req.aggregate != nil {
		cur = newAggregateBatchCursor(c.req.ctx, c.req.aggregate, cur)
	}
	return cur
}

// keyMerger is responsible for determining a merged set of tag keys
type keyMerger struct {
	i    int
	keys [2][][]byte
}

func (km *keyMerger) setTags(tags models.Tags) {
	km.i = 0
	if cap(km.keys[0]) < len(tags) {
		km.keys[0] = make([][]byte, len(tags))
	} else {
		km.keys[0] = km.keys[0][:len(tags)]
	}
	for i := range tags {
		km.keys[0][i] = tags[i].Key
	}
}

func (km *keyMerger) get() [][]byte { return km.keys[km.i&1] }

func (km *keyMerger) String() string {
	var s []string
	for _, k := range km.get() {
		s = append(s, string(k))
	}
	return strings.Join(s, ",")
}

func (km *keyMerger) mergeTagKeys(tags models.Tags) {
	keys := km.keys[km.i&1]
	i, j := 0, 0
	for i < len(keys) && j < len(tags) && bytes.Equal(keys[i], tags[j].Key) {
		i++
		j++
	}

	if j == len(tags) {
		// no new tags
		return
	}

	km.i = (km.i + 1) & 1
	l := len(keys) + len(tags)
	if cap(km.keys[km.i]) < l {
		km.keys[km.i] = make([][]byte, l)
	} else {
		km.keys[km.i] = km.keys[km.i][:l]
	}

	keya := km.keys[km.i]

	// back up the pointers
	if i > 0 {
		i--
		j--
	}

	k := i
	copy(keya[:k], keys[:k])

	for i < len(keys) && j < len(tags) {
		cmp := bytes.Compare(keys[i], tags[j].Key)
		if cmp < 0 {
			keya[k] = keys[i]
			i++
		} else if cmp > 0 {
			keya[k] = tags[j].Key
			j++
		} else {
			keya[k] = keys[i]
			i++
			j++
		}
		k++
	}

	if i < len(keys) {
		k += copy(keya[k:], keys[i:])
	}

	for j < len(tags) {
		keya[k] = tags[j].Key
		j++
		k++
	}

	km.keys[km.i] = keya[:k]
}
