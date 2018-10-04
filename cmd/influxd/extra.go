package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	google_protobuf1 "github.com/gogo/protobuf/types"
	google_protobuf2 "github.com/gogo/protobuf/types"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/control"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/functions"
	fstorage "github.com/influxdata/flux/functions/storage"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb/logger"
	influxquery "github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/bolt"
	"github.com/influxdata/platform/models"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/tsdb"
	opentracing "github.com/opentracing/opentracing-go"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type queryAdapter struct {
	Controller *control.Controller
}

func (q *queryAdapter) Query(ctx context.Context, req *query.Request) (flux.Query, error) {
	ctx = query.ContextWithRequest(ctx, req)
	ctx = context.WithValue(ctx, "org", req.OrganizationID.String())
	return q.Controller.Query(ctx, req.Compiler)
}

type Store interface {
	Read(ctx context.Context, req *ReadRequest) (ResultSet, error)
	GroupRead(ctx context.Context, req *ReadRequest) (GroupResultSet, error)
	WithLogger(log *zap.Logger)
}

func NewController(
	s Store,
	bucketLookup fstorage.BucketLookup,
	orgLookup fstorage.OrganizationLookup,
	logger *zap.Logger,
) *control.Controller {

	// flux
	var (
		concurrencyQuota = 10
		memoryBytesQuota = 1e6
	)

	cc := control.Config{
		ExecutorDependencies: make(execute.Dependencies),
		ConcurrencyQuota:     concurrencyQuota,
		MemoryBytesQuota:     int64(memoryBytesQuota),
		Logger:               logger,
		Verbose:              false,
	}

	err := functions.InjectFromDependencies(cc.ExecutorDependencies, fstorage.Dependencies{
		Reader:             NewReader(s),
		BucketLookup:       bucketLookup,
		OrganizationLookup: orgLookup,
	})
	if err != nil {
		panic(err)
	}
	return control.New(cc)
}

type bucketLookup struct {
	bolt *bolt.Client
}

func (b *bucketLookup) Lookup(orgID platform.ID, name string) (platform.ID, bool) {
	bucket, err := b.bolt.FindBucketByName(context.TODO(), orgID, name)
	if err != nil {
		return nil, false
	}
	return bucket.ID, true
}

type orgLookup struct {
	bolt *bolt.Client
}

func (o *orgLookup) Lookup(ctx context.Context, name string) (platform.ID, bool) {
	org, err := o.bolt.FindOrganizationByName(ctx, name)
	if err != nil {
		return nil, false
	}
	return org.ID, true
}

type store struct {
	shard *tsdb.Shard
}

func (s *store) WithLogger(*zap.Logger) {}

// Read begins a read operation using the parameters defined by ReadRequest and
// returns a ResultSet to enumerate the data.
func (s *store) Read(ctx context.Context, req *ReadRequest) (ResultSet, error) {
	if len(req.GroupKeys) > 0 {
		panic("Read: len(Grouping) > 0")
	}

	if req.Hints.NoPoints() {
		req.PointsLimit = -1
	}

	if req.PointsLimit == 0 {
		req.PointsLimit = math.MaxInt64
	}

	source, err := getReadSource(req)
	if err != nil {
		return nil, err
	}

	if req.TimestampRange.Start == 0 {
		req.TimestampRange.Start = models.MinNanoTime
	}

	if req.TimestampRange.End == 0 {
		req.TimestampRange.End = models.MaxNanoTime
	}

	var cur SeriesCursor
	if ic, err := newIndexSeriesCursor(ctx, source, req, s.shard); err != nil {
		return nil, err
	} else if ic == nil {
		return nil, nil
	} else {
		cur = ic
	}

	if req.SeriesLimit > 0 || req.SeriesOffset > 0 {
		cur = NewLimitSeriesCursor(ctx, cur, req.SeriesLimit, req.SeriesOffset)
	}

	return NewResultSet(ctx, req, cur), nil
}

func (s *store) GroupRead(ctx context.Context, req *ReadRequest) (GroupResultSet, error) {
	if req.SeriesLimit > 0 || req.SeriesOffset > 0 {
		return nil, errors.New("GroupRead: SeriesLimit and SeriesOffset not supported when Grouping")
	}

	if req.Hints.NoPoints() {
		req.PointsLimit = -1
	}

	if req.PointsLimit == 0 {
		req.PointsLimit = math.MaxInt64
	}

	source, err := getReadSource(req)
	if err != nil {
		return nil, err
	}

	if req.TimestampRange.Start <= 0 {
		req.TimestampRange.Start = models.MinNanoTime
	}

	if req.TimestampRange.End <= 0 {
		req.TimestampRange.End = models.MaxNanoTime
	}

	newCursor := func() (SeriesCursor, error) {
		cur, err := newIndexSeriesCursor(ctx, source, req, s.shard)
		if cur == nil || err != nil {
			return nil, err
		}
		return cur, nil
	}

	return NewGroupResultSet(ctx, req, newCursor), nil
}

const (
	fieldTagKey       = "_f"
	measurementTagKey = "_m"

	fieldKey       = "_field"
	measurementKey = "_measurement"
	valueKey       = "_value"
)

var (
	fieldTagKeyBytes       = []byte(fieldTagKey)
	measurementTagKeyBytes = []byte(measurementTagKey)

	fieldKeyBytes       = []byte(fieldKey)
	measurementKeyBytes = []byte(measurementKey)
)

type indexSeriesCursor struct {
	sqry         tsdb.SeriesCursor
	err          error
	tags         models.Tags
	cond         influxql.Expr
	row          SeriesRow
	eof          bool
	hasValueExpr bool
}

func newIndexSeriesCursor(ctx context.Context, src *ReadSource, req *ReadRequest, shard *tsdb.Shard) (*indexSeriesCursor, error) {
	queries, err := shard.CreateCursorIterator(ctx)
	if err != nil {
		return nil, err
	}

	if queries == nil {
		return nil, nil
	}

	span := opentracing.SpanFromContext(ctx)
	if span != nil {
		span = opentracing.StartSpan("index_cursor.create", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}

	opt := influxquery.IteratorOptions{
		Aux:        []influxql.VarRef{{Val: "key"}},
		Authorizer: influxquery.OpenAuthorizer,
		Ascending:  true,
		Ordered:    true,
	}
	p := &indexSeriesCursor{row: SeriesRow{Query: tsdb.CursorIterators{queries}}}

	m := append(append([]byte(nil), src.RetentionPolicy...), src.Database...)
	mi := tsdb.NewMeasurementSliceIterator([][]byte{m})

	if root := req.Predicate.GetRoot(); root != nil {
		if p.cond, err = NodeToExpr(root, nil); err != nil {
			return nil, err
		}

		p.hasValueExpr = hasFieldValueKey(p.cond)
		if !p.hasValueExpr {
			opt.Condition = p.cond
		} else {
			opt.Condition = influxql.Reduce(RewriteExprRemoveFieldValue(influxql.CloneExpr(p.cond)), nil)
			if isTrueBooleanLiteral(opt.Condition) {
				opt.Condition = nil
			}
		}
	}

	p.sqry, err = shard.CreateSeriesCursor(ctx, tsdb.SeriesCursorRequest{Measurements: mi}, opt.Condition)
	if err != nil {
		p.Close()
		return nil, err
	}
	return p, nil
}

func (c *indexSeriesCursor) Close() {
	if !c.eof {
		c.eof = true
		if c.sqry != nil {
			c.sqry.Close()
			c.sqry = nil
		}
	}
}

func copyTags(dst, src models.Tags) models.Tags {
	if cap(dst) < src.Len() {
		dst = make(models.Tags, src.Len())
	} else {
		dst = dst[:src.Len()]
	}
	copy(dst, src)
	return dst
}

func (c *indexSeriesCursor) Next() *SeriesRow {
	if c.eof {
		return nil
	}

	// next series key
	sr, err := c.sqry.Next()
	if err != nil {
		c.err = err
		c.Close()
		return nil
	} else if sr == nil {
		c.Close()
		return nil
	}

	c.row.Name = sr.Name
	//TODO(edd): check this.
	c.row.SeriesTags = copyTags(c.row.SeriesTags, sr.Tags)
	c.row.Tags = copyTags(c.row.Tags, sr.Tags)
	c.row.Field = string(c.row.Tags.Get(fieldTagKeyBytes))

	normalizeTags(c.row.Tags)

	if c.cond != nil && c.hasValueExpr {
		// TODO(sgc): lazily evaluate valueCond
		c.row.ValueCond = influxql.Reduce(c.cond, c)
		if isTrueBooleanLiteral(c.row.ValueCond) {
			// we've reduced the expression to "true"
			c.row.ValueCond = nil
		}
	}

	return &c.row
}

func (c *indexSeriesCursor) Value(key string) (interface{}, bool) {
	res := c.row.Tags.Get([]byte(key))
	// Return res as a string so it compares correctly with the string literals
	return string(res), res != nil
}

func (c *indexSeriesCursor) Err() error {
	return c.err
}

func isTrueBooleanLiteral(expr influxql.Expr) bool {
	b, ok := expr.(*influxql.BooleanLiteral)
	if ok {
		return b.Val
	}
	return false
}

type hasRefs struct {
	refs  []string
	found []bool
}

func (v *hasRefs) allFound() bool {
	for _, val := range v.found {
		if !val {
			return false
		}
	}
	return true
}

func (v *hasRefs) Visit(node influxql.Node) influxql.Visitor {
	if v.allFound() {
		return nil
	}

	if n, ok := node.(*influxql.VarRef); ok {
		for i, r := range v.refs {
			if !v.found[i] && r == n.Val {
				v.found[i] = true
				if v.allFound() {
					return nil
				}
			}
		}
	}
	return v
}

func hasFieldValueKey(expr influxql.Expr) bool {
	refs := hasRefs{refs: []string{valueKey}, found: make([]bool, 1)}
	influxql.Walk(&refs, expr)
	return refs.found[0]
}

func getReadSource(req *ReadRequest) (*ReadSource, error) {
	if req.ReadSource == nil {
		return nil, errors.New("missing read source")
	}

	var source ReadSource
	if err := types.UnmarshalAny(req.ReadSource, &source); err != nil {
		return nil, err
	}
	return &source, nil
}

func encodeVarintStorage(dAtA []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return offset + 1
}

func sovStorage(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
func sozStorage(x uint64) (n int) {
	return sovStorage(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}

func skipStorage(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowStorage
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowStorage
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
			return iNdEx, nil
		case 1:
			iNdEx += 8
			return iNdEx, nil
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowStorage
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			iNdEx += length
			if length < 0 {
				return 0, ErrInvalidLengthStorage
			}
			return iNdEx, nil
		case 3:
			for {
				var innerWire uint64
				var start int = iNdEx
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return 0, ErrIntOverflowStorage
					}
					if iNdEx >= l {
						return 0, io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					innerWire |= (uint64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				innerWireType := int(innerWire & 0x7)
				if innerWireType == 4 {
					break
				}
				next, err := skipStorage(dAtA[start:])
				if err != nil {
					return 0, err
				}
				iNdEx = start + next
			}
			return iNdEx, nil
		case 4:
			return iNdEx, nil
		case 5:
			iNdEx += 4
			return iNdEx, nil
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
	}
	panic("unreachable")
}

var (
	ErrInvalidLengthStorage = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowStorage   = fmt.Errorf("proto: integer overflow")
)

type SeriesRow struct {
	SortKey    []byte
	Name       []byte      // measurement name
	SeriesTags models.Tags // unmodified series tags
	Tags       models.Tags
	Field      string
	Query      tsdb.CursorIterators
	ValueCond  influxql.Expr
}

type ResultSet interface {
	Close()
	Next() bool
	Cursor() tsdb.Cursor
	Tags() models.Tags
}

type resultSet struct {
	ctx context.Context
	agg *Aggregate
	cur SeriesCursor
	row SeriesRow
	mb  multiShardCursors
}

func NewResultSet(ctx context.Context, req *ReadRequest, cur SeriesCursor) ResultSet {
	return &resultSet{
		ctx: ctx,
		agg: req.Aggregate,
		cur: cur,
		mb:  newMultiShardArrayCursors(ctx, req.TimestampRange.Start, req.TimestampRange.End, !req.Descending, req.PointsLimit),
	}
}

// Close closes the result set. Close is idempotent.
func (r *resultSet) Close() {
	if r == nil {
		return // Nothing to do.
	}
	r.row.Query = nil
	r.cur.Close()
}

// Next returns true if there are more results available.
func (r *resultSet) Next() bool {
	if r == nil {
		return false
	}

	row := r.cur.Next()
	if row == nil {
		return false
	}

	r.row = *row

	return true
}

func (r *resultSet) Cursor() tsdb.Cursor {
	cur := r.mb.createCursor(r.row)
	if r.agg != nil {
		cur = r.mb.newAggregateCursor(r.ctx, r.agg, cur)
	}
	return cur
}

func (r *resultSet) Tags() models.Tags {
	return r.row.Tags
}

type multiShardCursors interface {
	createCursor(row SeriesRow) tsdb.Cursor
	newAggregateCursor(ctx context.Context, agg *Aggregate, cursor tsdb.Cursor) tsdb.Cursor
}

type SeriesCursor interface {
	Close()
	Next() *SeriesRow
	Err() error
}

type limitSeriesCursor struct {
	SeriesCursor
	n, o, c int64
}

func NewLimitSeriesCursor(ctx context.Context, cur SeriesCursor, n, o int64) SeriesCursor {
	return &limitSeriesCursor{SeriesCursor: cur, o: o, n: n}
}

func (c *limitSeriesCursor) Next() *SeriesRow {
	if c.o > 0 {
		for i := int64(0); i < c.o; i++ {
			if c.SeriesCursor.Next() == nil {
				break
			}
		}
		c.o = 0
	}

	if c.c >= c.n {
		return nil
	}
	c.c++
	return c.SeriesCursor.Next()
}

type groupResultSet struct {
	ctx context.Context
	req *ReadRequest
	agg *Aggregate
	mb  multiShardCursors

	i    int
	rows []*SeriesRow
	keys [][]byte
	rgc  groupByCursor
	km   keyMerger

	newCursorFn func() (SeriesCursor, error)
	nextGroupFn func(c *groupResultSet) GroupCursor
	sortFn      func(c *groupResultSet) (int, error)

	eof bool
}

type GroupResultSet interface {
	Next() GroupCursor
	Close()
}

func NewGroupResultSet(ctx context.Context, req *ReadRequest, newCursorFn func() (SeriesCursor, error)) GroupResultSet {
	g := &groupResultSet{
		ctx:         ctx,
		req:         req,
		agg:         req.Aggregate,
		keys:        make([][]byte, len(req.GroupKeys)),
		newCursorFn: newCursorFn,
	}

	g.mb = newMultiShardArrayCursors(ctx, req.TimestampRange.Start, req.TimestampRange.End, !req.Descending, req.PointsLimit)

	for i, k := range req.GroupKeys {
		g.keys[i] = []byte(k)
	}

	switch req.Group {
	case GroupBy:
		g.sortFn = groupBySort
		g.nextGroupFn = groupByNextGroup
		g.rgc = groupByCursor{
			ctx:  ctx,
			mb:   g.mb,
			agg:  req.Aggregate,
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
func (g *groupResultSet) seriesHasPoints(row *SeriesRow) bool {
	// TODO(sgc): this is expensive. Storage engine must provide efficient time range queries of series keys.
	cur := g.mb.createCursor(*row)
	var ts []int64
	switch c := cur.(type) {
	case tsdb.IntegerArrayCursor:
		a := c.Next()
		ts = a.Timestamps
	case tsdb.FloatArrayCursor:
		a := c.Next()
		ts = a.Timestamps
	case tsdb.UnsignedArrayCursor:
		a := c.Next()
		ts = a.Timestamps
	case tsdb.BooleanArrayCursor:
		a := c.Next()
		ts = a.Timestamps
	case tsdb.StringArrayCursor:
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

type GroupCursor interface {
	Tags() models.Tags
	Keys() [][]byte
	PartitionKeyVals() [][]byte
	Next() bool
	Cursor() tsdb.Cursor
	Close()
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
		n++
		if allTime || g.seriesHasPoints(row) {
			g.km.mergeTagKeys(row.Tags)
		}
		row = cur.Next()
	}

	cur.Close()
	return n, nil
}

func groupByNextGroup(g *groupResultSet) GroupCursor {
next:
	row := g.rows[g.i]
	for i := range g.keys {
		g.rgc.vals[i] = row.Tags.Get(g.keys[i])
	}

	g.km.clear()
	allTime := g.req.Hints.HintSchemaAllTime()
	c := 0
	rowKey := row.SortKey
	j := g.i
	for j < len(g.rows) && bytes.Equal(rowKey, g.rows[j].SortKey) {
		if allTime || g.seriesHasPoints(g.rows[j]) {
			g.km.mergeTagKeys(g.rows[j].Tags)
			c++
		}
		j++
	}

	g.rgc.reset(g.rows[g.i:j])
	g.rgc.keys = g.km.get()

	g.i = j
	if j == len(g.rows) {
		g.eof = true
	} else if c == 0 {
		// no rows with points
		goto next
	}

	return &g.rgc
}

type tagsBuffer struct {
	sz  int
	i   int
	buf models.Tags
}

func (tb *tagsBuffer) copyTags(src models.Tags) models.Tags {
	var buf models.Tags
	if len(src) > tb.sz {
		buf = make(models.Tags, len(src))
	} else {
		if tb.i+len(src) > len(tb.buf) {
			tb.buf = make(models.Tags, tb.sz)
			tb.i = 0
		}

		buf = tb.buf[tb.i : tb.i+len(src)]
		tb.i += len(src)
	}

	copy(buf, src)

	return buf
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

	row := cur.Next()
	for row != nil {
		nr := *row
		nr.SeriesTags = tagsBuf.copyTags(nr.SeriesTags)
		nr.Tags = tagsBuf.copyTags(nr.Tags)

		l := 0
		for i, k := range g.keys {
			vals[i] = nr.Tags.Get(k)
			if len(vals[i]) == 0 {
				vals[i] = nilKey[:] // if there was no value, ensure it sorts last
			}
			l += len(vals[i])
		}

		nr.SortKey = make([]byte, 0, l)
		for _, v := range vals {
			nr.SortKey = append(nr.SortKey, v...)
		}

		rows = append(rows, &nr)
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
	agg  *Aggregate
	cur  SeriesCursor
	row  SeriesRow
	keys [][]byte
}

func (c *groupNoneCursor) Tags() models.Tags          { return c.row.Tags }
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
	if c.agg != nil {
		cur = c.mb.newAggregateCursor(c.ctx, c.agg, cur)
	}
	return cur
}

type groupByCursor struct {
	ctx  context.Context
	mb   multiShardCursors
	agg  *Aggregate
	i    int
	rows []*SeriesRow
	keys [][]byte
	vals [][]byte
}

func (c *groupByCursor) reset(rows []*SeriesRow) {
	c.i = 0
	c.rows = rows
}

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

func (c *groupByCursor) Cursor() tsdb.Cursor {
	cur := c.mb.createCursor(*c.rows[c.i-1])
	if c.agg != nil {
		cur = c.mb.newAggregateCursor(c.ctx, c.agg, cur)
	}
	return cur
}

// keyMerger is responsible for determining a merged set of tag keys
type keyMerger struct {
	i    int
	keys [2][][]byte
}

func (km *keyMerger) clear() {
	km.i = 0
	km.keys[0] = km.keys[0][:0]
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

type multiShardArrayCursors struct {
	ctx   context.Context
	limit int64
	req   tsdb.CursorRequest

	cursors struct {
		i integerMultiShardArrayCursor
		f floatMultiShardArrayCursor
		u unsignedMultiShardArrayCursor
		b booleanMultiShardArrayCursor
		s stringMultiShardArrayCursor
	}
}

func newMultiShardArrayCursors(ctx context.Context, start, end int64, asc bool, limit int64) *multiShardArrayCursors {
	if limit < 0 {
		limit = 1
	}

	m := &multiShardArrayCursors{
		ctx:   ctx,
		limit: limit,
		req: tsdb.CursorRequest{
			Ascending: asc,
			StartTime: start,
			EndTime:   end,
		},
	}

	cc := cursorContext{
		ctx:   ctx,
		limit: limit,
		req:   &m.req,
	}

	m.cursors.i.cursorContext = cc
	m.cursors.f.cursorContext = cc
	m.cursors.u.cursorContext = cc
	m.cursors.b.cursorContext = cc
	m.cursors.s.cursorContext = cc

	return m
}

type astExpr struct {
	expr influxql.Expr
}

func (e *astExpr) EvalBool(v valuer) bool {
	return evalExprBool(e.expr, v)
}

func evalExprBool(expr influxql.Expr, m valuer) bool {
	v, _ := evalExpr(expr, m).(bool)
	return v
}

// evalExpr evaluates expr against a map.
func evalExpr(expr influxql.Expr, m valuer) interface{} {
	if expr == nil {
		return nil
	}

	switch expr := expr.(type) {
	case *influxql.BinaryExpr:
		return evalBinaryExpr(expr, m)
	case *influxql.BooleanLiteral:
		return expr.Val
	case *influxql.IntegerLiteral:
		return expr.Val
	case *influxql.UnsignedLiteral:
		return expr.Val
	case *influxql.NumberLiteral:
		return expr.Val
	case *influxql.ParenExpr:
		return evalExpr(expr.Expr, m)
	case *influxql.RegexLiteral:
		return expr.Val
	case *influxql.StringLiteral:
		return expr.Val
	case *influxql.VarRef:
		v, _ := m.Value(expr.Val)
		return v
	default:
		return nil
	}
}

func evalBinaryExpr(expr *influxql.BinaryExpr, m valuer) interface{} {
	lhs := evalExpr(expr.LHS, m)
	rhs := evalExpr(expr.RHS, m)
	if lhs == nil && rhs != nil {
		// When the LHS is nil and the RHS is a boolean, implicitly cast the
		// nil to false.
		if _, ok := rhs.(bool); ok {
			lhs = false
		}
	} else if lhs != nil && rhs == nil {
		// Implicit cast of the RHS nil to false when the LHS is a boolean.
		if _, ok := lhs.(bool); ok {
			rhs = false
		}
	}

	// Evaluate if both sides are simple types.
	switch lhs := lhs.(type) {
	case bool:
		rhs, ok := rhs.(bool)
		switch expr.Op {
		case influxql.AND:
			return ok && (lhs && rhs)
		case influxql.OR:
			return ok && (lhs || rhs)
		case influxql.BITWISE_AND:
			return ok && (lhs && rhs)
		case influxql.BITWISE_OR:
			return ok && (lhs || rhs)
		case influxql.BITWISE_XOR:
			return ok && (lhs != rhs)
		case influxql.EQ:
			return ok && (lhs == rhs)
		case influxql.NEQ:
			return ok && (lhs != rhs)
		}
	case float64:
		// Try the rhs as a float64 or int64
		rhsf, ok := rhs.(float64)
		if !ok {
			var rhsi int64
			if rhsi, ok = rhs.(int64); ok {
				rhsf = float64(rhsi)
			}
		}

		rhs := rhsf
		switch expr.Op {
		case influxql.EQ:
			return ok && (lhs == rhs)
		case influxql.NEQ:
			return ok && (lhs != rhs)
		case influxql.LT:
			return ok && (lhs < rhs)
		case influxql.LTE:
			return ok && (lhs <= rhs)
		case influxql.GT:
			return ok && (lhs > rhs)
		case influxql.GTE:
			return ok && (lhs >= rhs)
		case influxql.ADD:
			if !ok {
				return nil
			}
			return lhs + rhs
		case influxql.SUB:
			if !ok {
				return nil
			}
			return lhs - rhs
		case influxql.MUL:
			if !ok {
				return nil
			}
			return lhs * rhs
		case influxql.DIV:
			if !ok {
				return nil
			} else if rhs == 0 {
				return float64(0)
			}
			return lhs / rhs
		case influxql.MOD:
			if !ok {
				return nil
			}
			return math.Mod(lhs, rhs)
		}
	case int64:
		// Try as a float64 to see if a float cast is required.
		rhsf, ok := rhs.(float64)
		if ok {
			lhs := float64(lhs)
			rhs := rhsf
			switch expr.Op {
			case influxql.EQ:
				return lhs == rhs
			case influxql.NEQ:
				return lhs != rhs
			case influxql.LT:
				return lhs < rhs
			case influxql.LTE:
				return lhs <= rhs
			case influxql.GT:
				return lhs > rhs
			case influxql.GTE:
				return lhs >= rhs
			case influxql.ADD:
				return lhs + rhs
			case influxql.SUB:
				return lhs - rhs
			case influxql.MUL:
				return lhs * rhs
			case influxql.DIV:
				if rhs == 0 {
					return float64(0)
				}
				return lhs / rhs
			case influxql.MOD:
				return math.Mod(lhs, rhs)
			}
		} else {
			rhs, ok := rhs.(int64)
			switch expr.Op {
			case influxql.EQ:
				return ok && (lhs == rhs)
			case influxql.NEQ:
				return ok && (lhs != rhs)
			case influxql.LT:
				return ok && (lhs < rhs)
			case influxql.LTE:
				return ok && (lhs <= rhs)
			case influxql.GT:
				return ok && (lhs > rhs)
			case influxql.GTE:
				return ok && (lhs >= rhs)
			case influxql.ADD:
				if !ok {
					return nil
				}
				return lhs + rhs
			case influxql.SUB:
				if !ok {
					return nil
				}
				return lhs - rhs
			case influxql.MUL:
				if !ok {
					return nil
				}
				return lhs * rhs
			case influxql.DIV:
				if !ok {
					return nil
				} else if rhs == 0 {
					return float64(0)
				}
				return lhs / rhs
			case influxql.MOD:
				if !ok {
					return nil
				} else if rhs == 0 {
					return int64(0)
				}
				return lhs % rhs
			case influxql.BITWISE_AND:
				if !ok {
					return nil
				}
				return lhs & rhs
			case influxql.BITWISE_OR:
				if !ok {
					return nil
				}
				return lhs | rhs
			case influxql.BITWISE_XOR:
				if !ok {
					return nil
				}
				return lhs ^ rhs
			}
		}
	case string:
		switch expr.Op {
		case influxql.EQ:
			rhs, ok := rhs.(string)
			if !ok {
				return nil
			}
			return lhs == rhs
		case influxql.NEQ:
			rhs, ok := rhs.(string)
			if !ok {
				return nil
			}
			return lhs != rhs
		case influxql.EQREGEX:
			rhs, ok := rhs.(*regexp.Regexp)
			if !ok {
				return nil
			}
			return rhs.MatchString(lhs)
		case influxql.NEQREGEX:
			rhs, ok := rhs.(*regexp.Regexp)
			if !ok {
				return nil
			}
			return !rhs.MatchString(lhs)
		}
	case []byte:
		switch expr.Op {
		case influxql.EQ:
			rhs, ok := rhs.(string)
			if !ok {
				return nil
			}
			return string(lhs) == rhs
		case influxql.NEQ:
			rhs, ok := rhs.(string)
			if !ok {
				return nil
			}
			return string(lhs) != rhs
		case influxql.EQREGEX:
			rhs, ok := rhs.(*regexp.Regexp)
			if !ok {
				return nil
			}
			return rhs.Match(lhs)
		case influxql.NEQREGEX:
			rhs, ok := rhs.(*regexp.Regexp)
			if !ok {
				return nil
			}
			return !rhs.Match(lhs)
		}
	}
	return nil
}

func (m *multiShardArrayCursors) createCursor(row SeriesRow) tsdb.Cursor {
	m.req.Name = row.Name
	m.req.Tags = row.SeriesTags
	m.req.Field = row.Field

	var cond expression
	if row.ValueCond != nil {
		cond = &astExpr{row.ValueCond}
	}

	var shard tsdb.CursorIterator
	var cur tsdb.Cursor
	for cur == nil && len(row.Query) > 0 {
		shard, row.Query = row.Query[0], row.Query[1:]
		cur, _ = shard.Next(m.ctx, &m.req)
	}

	if cur == nil {
		return nil
	}

	switch c := cur.(type) {
	case tsdb.IntegerArrayCursor:
		m.cursors.i.reset(c, row.Query, cond)
		return &m.cursors.i
	case tsdb.FloatArrayCursor:
		m.cursors.f.reset(c, row.Query, cond)
		return &m.cursors.f
	case tsdb.UnsignedArrayCursor:
		m.cursors.u.reset(c, row.Query, cond)
		return &m.cursors.u
	case tsdb.StringArrayCursor:
		m.cursors.s.reset(c, row.Query, cond)
		return &m.cursors.s
	case tsdb.BooleanArrayCursor:
		m.cursors.b.reset(c, row.Query, cond)
		return &m.cursors.b
	default:
		panic(fmt.Sprintf("unreachable: %T", cur))
	}
}

func newAggregateArrayCursor(ctx context.Context, agg *Aggregate, cursor tsdb.Cursor) tsdb.Cursor {
	if cursor == nil {
		return nil
	}

	switch agg.Type {
	case AggregateTypeSum:
		return newSumArrayCursor(cursor)
	case AggregateTypeCount:
		return newCountArrayCursor(cursor)
	default:
		// TODO(sgc): should be validated higher up
		panic("invalid aggregate")
	}
}

func newSumArrayCursor(cur tsdb.Cursor) tsdb.Cursor {
	switch cur := cur.(type) {
	case tsdb.FloatArrayCursor:
		return newFloatArraySumCursor(cur)
	case tsdb.IntegerArrayCursor:
		return newIntegerArraySumCursor(cur)
	case tsdb.UnsignedArrayCursor:
		return newUnsignedArraySumCursor(cur)
	default:
		// TODO(sgc): propagate an error instead?
		return nil
	}
}

func newCountArrayCursor(cur tsdb.Cursor) tsdb.Cursor {
	switch cur := cur.(type) {
	case tsdb.FloatArrayCursor:
		return &integerFloatCountArrayCursor{FloatArrayCursor: cur}
	case tsdb.IntegerArrayCursor:
		return &integerIntegerCountArrayCursor{IntegerArrayCursor: cur}
	case tsdb.UnsignedArrayCursor:
		return &integerUnsignedCountArrayCursor{UnsignedArrayCursor: cur}
	case tsdb.StringArrayCursor:
		return &integerStringCountArrayCursor{StringArrayCursor: cur}
	case tsdb.BooleanArrayCursor:
		return &integerBooleanCountArrayCursor{BooleanArrayCursor: cur}
	default:
		panic(fmt.Sprintf("unreachable: %T", cur))
	}
}

func (m *multiShardArrayCursors) newAggregateCursor(ctx context.Context, agg *Aggregate, cursor tsdb.Cursor) tsdb.Cursor {
	return newAggregateArrayCursor(ctx, agg, cursor)
}

// ********************
// Float Array Cursor

type floatArrayFilterCursor struct {
	tsdb.FloatArrayCursor
	cond expression
	m    *singleValue
	res  *tsdb.FloatArray
	tmp  *tsdb.FloatArray
}

func newFloatFilterArrayCursor(cond expression) *floatArrayFilterCursor {
	return &floatArrayFilterCursor{
		cond: cond,
		m:    &singleValue{},
		res:  tsdb.NewFloatArrayLen(tsdb.DefaultMaxPointsPerBlock),
		tmp:  &tsdb.FloatArray{},
	}
}

func (c *floatArrayFilterCursor) reset(cur tsdb.FloatArrayCursor) {
	c.FloatArrayCursor = cur
	c.tmp.Timestamps, c.tmp.Values = nil, nil
}

func (c *floatArrayFilterCursor) Next() *tsdb.FloatArray {
	pos := 0
	var a *tsdb.FloatArray

	if a.Len() > 0 {
		a = c.tmp
		c.tmp.Timestamps = nil
		c.tmp.Values = nil
	} else {
		a = c.FloatArrayCursor.Next()
	}

LOOP:
	for len(a.Timestamps) > 0 {
		for i, v := range a.Values {
			c.m.v = v
			if c.cond.EvalBool(c.m) {
				c.res.Timestamps[pos] = a.Timestamps[i]
				c.res.Values[pos] = v
				pos++
				if pos >= tsdb.DefaultMaxPointsPerBlock {
					c.tmp.Timestamps = a.Timestamps[i+1:]
					c.tmp.Values = a.Values[i+1:]
					break LOOP
				}
			}
		}
		a = c.FloatArrayCursor.Next()
	}

	c.res.Timestamps = c.res.Timestamps[:pos]
	c.res.Values = c.res.Values[:pos]

	return c.res
}

type floatMultiShardArrayCursor struct {
	tsdb.FloatArrayCursor
	cursorContext
	filter *floatArrayFilterCursor
}

func (c *floatMultiShardArrayCursor) reset(cur tsdb.FloatArrayCursor, itrs tsdb.CursorIterators, cond expression) {
	if cond != nil {
		if c.filter == nil {
			c.filter = newFloatFilterArrayCursor(cond)
		}
		c.filter.reset(cur)
		cur = c.filter
	}

	c.FloatArrayCursor = cur
	c.itrs = itrs
	c.err = nil
	c.count = 0
}

func (c *floatMultiShardArrayCursor) Err() error { return c.err }

func (c *floatMultiShardArrayCursor) Next() *tsdb.FloatArray {
	for {
		a := c.FloatArrayCursor.Next()
		if a.Len() == 0 {
			if c.nextArrayCursor() {
				continue
			}
		}
		c.count += int64(a.Len())
		if c.count > c.limit {
			diff := c.count - c.limit
			c.count -= diff
			rem := int64(a.Len()) - diff
			a.Timestamps = a.Timestamps[:rem]
			a.Values = a.Values[:rem]
		}
		return a
	}
}

func (c *floatMultiShardArrayCursor) nextArrayCursor() bool {
	if len(c.itrs) == 0 {
		return false
	}

	c.FloatArrayCursor.Close()

	var itr tsdb.CursorIterator
	var cur tsdb.Cursor
	for cur == nil && len(c.itrs) > 0 {
		itr, c.itrs = c.itrs[0], c.itrs[1:]
		cur, _ = itr.Next(c.ctx, c.req)
	}

	var ok bool
	if cur != nil {
		var next tsdb.FloatArrayCursor
		next, ok = cur.(tsdb.FloatArrayCursor)
		if !ok {
			cur.Close()
			next = FloatEmptyArrayCursor
			c.itrs = nil
			c.err = errors.New("expected float cursor")
		} else {
			if c.filter != nil {
				c.filter.reset(next)
				next = c.filter
			}
		}
		c.FloatArrayCursor = next
	} else {
		c.FloatArrayCursor = FloatEmptyArrayCursor
	}

	return ok
}

type floatArraySumCursor struct {
	tsdb.FloatArrayCursor
	ts  [1]int64
	vs  [1]float64
	res *tsdb.FloatArray
}

func newFloatArraySumCursor(cur tsdb.FloatArrayCursor) *floatArraySumCursor {
	return &floatArraySumCursor{
		FloatArrayCursor: cur,
		res:              &tsdb.FloatArray{},
	}
}

func (c floatArraySumCursor) Next() *tsdb.FloatArray {
	a := c.FloatArrayCursor.Next()
	if len(a.Timestamps) == 0 {
		return a
	}

	ts := a.Timestamps[0]
	var acc float64

	for {
		for _, v := range a.Values {
			acc += v
		}
		a = c.FloatArrayCursor.Next()
		if len(a.Timestamps) == 0 {
			c.ts[0] = ts
			c.vs[0] = acc
			c.res.Timestamps = c.ts[:]
			c.res.Values = c.vs[:]
			return c.res
		}
	}
}

type integerFloatCountArrayCursor struct {
	tsdb.FloatArrayCursor
}

func (c *integerFloatCountArrayCursor) Next() *tsdb.IntegerArray {
	a := c.FloatArrayCursor.Next()
	if len(a.Timestamps) == 0 {
		return &tsdb.IntegerArray{}
	}

	ts := a.Timestamps[0]
	var acc int64
	for {
		acc += int64(len(a.Timestamps))
		a = c.FloatArrayCursor.Next()
		if len(a.Timestamps) == 0 {
			res := tsdb.NewIntegerArrayLen(1)
			res.Timestamps[0] = ts
			res.Values[0] = acc
			return res
		}
	}
}

type floatEmptyArrayCursor struct {
	res tsdb.FloatArray
}

var FloatEmptyArrayCursor tsdb.FloatArrayCursor = &floatEmptyArrayCursor{}

func (c *floatEmptyArrayCursor) Err() error             { return nil }
func (c *floatEmptyArrayCursor) Close()                 {}
func (c *floatEmptyArrayCursor) Next() *tsdb.FloatArray { return &c.res }

// ********************
// Integer Array Cursor

type integerArrayFilterCursor struct {
	tsdb.IntegerArrayCursor
	cond expression
	m    *singleValue
	res  *tsdb.IntegerArray
	tmp  *tsdb.IntegerArray
}

func newIntegerFilterArrayCursor(cond expression) *integerArrayFilterCursor {
	return &integerArrayFilterCursor{
		cond: cond,
		m:    &singleValue{},
		res:  tsdb.NewIntegerArrayLen(tsdb.DefaultMaxPointsPerBlock),
		tmp:  &tsdb.IntegerArray{},
	}
}

func (c *integerArrayFilterCursor) reset(cur tsdb.IntegerArrayCursor) {
	c.IntegerArrayCursor = cur
	c.tmp.Timestamps, c.tmp.Values = nil, nil
}

func (c *integerArrayFilterCursor) Next() *tsdb.IntegerArray {
	pos := 0
	var a *tsdb.IntegerArray

	if a.Len() > 0 {
		a = c.tmp
		c.tmp.Timestamps = nil
		c.tmp.Values = nil
	} else {
		a = c.IntegerArrayCursor.Next()
	}

LOOP:
	for len(a.Timestamps) > 0 {
		for i, v := range a.Values {
			c.m.v = v
			if c.cond.EvalBool(c.m) {
				c.res.Timestamps[pos] = a.Timestamps[i]
				c.res.Values[pos] = v
				pos++
				if pos >= tsdb.DefaultMaxPointsPerBlock {
					c.tmp.Timestamps = a.Timestamps[i+1:]
					c.tmp.Values = a.Values[i+1:]
					break LOOP
				}
			}
		}
		a = c.IntegerArrayCursor.Next()
	}

	c.res.Timestamps = c.res.Timestamps[:pos]
	c.res.Values = c.res.Values[:pos]

	return c.res
}

type integerMultiShardArrayCursor struct {
	tsdb.IntegerArrayCursor
	cursorContext
	filter *integerArrayFilterCursor
}

func (c *integerMultiShardArrayCursor) reset(cur tsdb.IntegerArrayCursor, itrs tsdb.CursorIterators, cond expression) {
	if cond != nil {
		if c.filter == nil {
			c.filter = newIntegerFilterArrayCursor(cond)
		}
		c.filter.reset(cur)
		cur = c.filter
	}

	c.IntegerArrayCursor = cur
	c.itrs = itrs
	c.err = nil
	c.count = 0
}

func (c *integerMultiShardArrayCursor) Err() error { return c.err }

func (c *integerMultiShardArrayCursor) Next() *tsdb.IntegerArray {
	for {
		a := c.IntegerArrayCursor.Next()
		if a.Len() == 0 {
			if c.nextArrayCursor() {
				continue
			}
		}
		c.count += int64(a.Len())
		if c.count > c.limit {
			diff := c.count - c.limit
			c.count -= diff
			rem := int64(a.Len()) - diff
			a.Timestamps = a.Timestamps[:rem]
			a.Values = a.Values[:rem]
		}
		return a
	}
}

func (c *integerMultiShardArrayCursor) nextArrayCursor() bool {
	if len(c.itrs) == 0 {
		return false
	}

	c.IntegerArrayCursor.Close()

	var itr tsdb.CursorIterator
	var cur tsdb.Cursor
	for cur == nil && len(c.itrs) > 0 {
		itr, c.itrs = c.itrs[0], c.itrs[1:]
		cur, _ = itr.Next(c.ctx, c.req)
	}

	var ok bool
	if cur != nil {
		var next tsdb.IntegerArrayCursor
		next, ok = cur.(tsdb.IntegerArrayCursor)
		if !ok {
			cur.Close()
			next = IntegerEmptyArrayCursor
			c.itrs = nil
			c.err = errors.New("expected integer cursor")
		} else {
			if c.filter != nil {
				c.filter.reset(next)
				next = c.filter
			}
		}
		c.IntegerArrayCursor = next
	} else {
		c.IntegerArrayCursor = IntegerEmptyArrayCursor
	}

	return ok
}

type integerArraySumCursor struct {
	tsdb.IntegerArrayCursor
	ts  [1]int64
	vs  [1]int64
	res *tsdb.IntegerArray
}

func newIntegerArraySumCursor(cur tsdb.IntegerArrayCursor) *integerArraySumCursor {
	return &integerArraySumCursor{
		IntegerArrayCursor: cur,
		res:                &tsdb.IntegerArray{},
	}
}

func (c integerArraySumCursor) Next() *tsdb.IntegerArray {
	a := c.IntegerArrayCursor.Next()
	if len(a.Timestamps) == 0 {
		return a
	}

	ts := a.Timestamps[0]
	var acc int64

	for {
		for _, v := range a.Values {
			acc += v
		}
		a = c.IntegerArrayCursor.Next()
		if len(a.Timestamps) == 0 {
			c.ts[0] = ts
			c.vs[0] = acc
			c.res.Timestamps = c.ts[:]
			c.res.Values = c.vs[:]
			return c.res
		}
	}
}

type integerIntegerCountArrayCursor struct {
	tsdb.IntegerArrayCursor
}

func (c *integerIntegerCountArrayCursor) Next() *tsdb.IntegerArray {
	a := c.IntegerArrayCursor.Next()
	if len(a.Timestamps) == 0 {
		return &tsdb.IntegerArray{}
	}

	ts := a.Timestamps[0]
	var acc int64
	for {
		acc += int64(len(a.Timestamps))
		a = c.IntegerArrayCursor.Next()
		if len(a.Timestamps) == 0 {
			res := tsdb.NewIntegerArrayLen(1)
			res.Timestamps[0] = ts
			res.Values[0] = acc
			return res
		}
	}
}

type integerEmptyArrayCursor struct {
	res tsdb.IntegerArray
}

var IntegerEmptyArrayCursor tsdb.IntegerArrayCursor = &integerEmptyArrayCursor{}

func (c *integerEmptyArrayCursor) Err() error               { return nil }
func (c *integerEmptyArrayCursor) Close()                   {}
func (c *integerEmptyArrayCursor) Next() *tsdb.IntegerArray { return &c.res }

// ********************
// Unsigned Array Cursor

type unsignedArrayFilterCursor struct {
	tsdb.UnsignedArrayCursor
	cond expression
	m    *singleValue
	res  *tsdb.UnsignedArray
	tmp  *tsdb.UnsignedArray
}

func newUnsignedFilterArrayCursor(cond expression) *unsignedArrayFilterCursor {
	return &unsignedArrayFilterCursor{
		cond: cond,
		m:    &singleValue{},
		res:  tsdb.NewUnsignedArrayLen(tsdb.DefaultMaxPointsPerBlock),
		tmp:  &tsdb.UnsignedArray{},
	}
}

func (c *unsignedArrayFilterCursor) reset(cur tsdb.UnsignedArrayCursor) {
	c.UnsignedArrayCursor = cur
	c.tmp.Timestamps, c.tmp.Values = nil, nil
}

func (c *unsignedArrayFilterCursor) Next() *tsdb.UnsignedArray {
	pos := 0
	var a *tsdb.UnsignedArray

	if a.Len() > 0 {
		a = c.tmp
		c.tmp.Timestamps = nil
		c.tmp.Values = nil
	} else {
		a = c.UnsignedArrayCursor.Next()
	}

LOOP:
	for len(a.Timestamps) > 0 {
		for i, v := range a.Values {
			c.m.v = v
			if c.cond.EvalBool(c.m) {
				c.res.Timestamps[pos] = a.Timestamps[i]
				c.res.Values[pos] = v
				pos++
				if pos >= tsdb.DefaultMaxPointsPerBlock {
					c.tmp.Timestamps = a.Timestamps[i+1:]
					c.tmp.Values = a.Values[i+1:]
					break LOOP
				}
			}
		}
		a = c.UnsignedArrayCursor.Next()
	}

	c.res.Timestamps = c.res.Timestamps[:pos]
	c.res.Values = c.res.Values[:pos]

	return c.res
}

type unsignedMultiShardArrayCursor struct {
	tsdb.UnsignedArrayCursor
	cursorContext
	filter *unsignedArrayFilterCursor
}

func (c *unsignedMultiShardArrayCursor) reset(cur tsdb.UnsignedArrayCursor, itrs tsdb.CursorIterators, cond expression) {
	if cond != nil {
		if c.filter == nil {
			c.filter = newUnsignedFilterArrayCursor(cond)
		}
		c.filter.reset(cur)
		cur = c.filter
	}

	c.UnsignedArrayCursor = cur
	c.itrs = itrs
	c.err = nil
	c.count = 0
}

func (c *unsignedMultiShardArrayCursor) Err() error { return c.err }

func (c *unsignedMultiShardArrayCursor) Next() *tsdb.UnsignedArray {
	for {
		a := c.UnsignedArrayCursor.Next()
		if a.Len() == 0 {
			if c.nextArrayCursor() {
				continue
			}
		}
		c.count += int64(a.Len())
		if c.count > c.limit {
			diff := c.count - c.limit
			c.count -= diff
			rem := int64(a.Len()) - diff
			a.Timestamps = a.Timestamps[:rem]
			a.Values = a.Values[:rem]
		}
		return a
	}
}

func (c *unsignedMultiShardArrayCursor) nextArrayCursor() bool {
	if len(c.itrs) == 0 {
		return false
	}

	c.UnsignedArrayCursor.Close()

	var itr tsdb.CursorIterator
	var cur tsdb.Cursor
	for cur == nil && len(c.itrs) > 0 {
		itr, c.itrs = c.itrs[0], c.itrs[1:]
		cur, _ = itr.Next(c.ctx, c.req)
	}

	var ok bool
	if cur != nil {
		var next tsdb.UnsignedArrayCursor
		next, ok = cur.(tsdb.UnsignedArrayCursor)
		if !ok {
			cur.Close()
			next = UnsignedEmptyArrayCursor
			c.itrs = nil
			c.err = errors.New("expected unsigned cursor")
		} else {
			if c.filter != nil {
				c.filter.reset(next)
				next = c.filter
			}
		}
		c.UnsignedArrayCursor = next
	} else {
		c.UnsignedArrayCursor = UnsignedEmptyArrayCursor
	}

	return ok
}

type unsignedArraySumCursor struct {
	tsdb.UnsignedArrayCursor
	ts  [1]int64
	vs  [1]uint64
	res *tsdb.UnsignedArray
}

func newUnsignedArraySumCursor(cur tsdb.UnsignedArrayCursor) *unsignedArraySumCursor {
	return &unsignedArraySumCursor{
		UnsignedArrayCursor: cur,
		res:                 &tsdb.UnsignedArray{},
	}
}

func (c unsignedArraySumCursor) Next() *tsdb.UnsignedArray {
	a := c.UnsignedArrayCursor.Next()
	if len(a.Timestamps) == 0 {
		return a
	}

	ts := a.Timestamps[0]
	var acc uint64

	for {
		for _, v := range a.Values {
			acc += v
		}
		a = c.UnsignedArrayCursor.Next()
		if len(a.Timestamps) == 0 {
			c.ts[0] = ts
			c.vs[0] = acc
			c.res.Timestamps = c.ts[:]
			c.res.Values = c.vs[:]
			return c.res
		}
	}
}

type integerUnsignedCountArrayCursor struct {
	tsdb.UnsignedArrayCursor
}

func (c *integerUnsignedCountArrayCursor) Next() *tsdb.IntegerArray {
	a := c.UnsignedArrayCursor.Next()
	if len(a.Timestamps) == 0 {
		return &tsdb.IntegerArray{}
	}

	ts := a.Timestamps[0]
	var acc int64
	for {
		acc += int64(len(a.Timestamps))
		a = c.UnsignedArrayCursor.Next()
		if len(a.Timestamps) == 0 {
			res := tsdb.NewIntegerArrayLen(1)
			res.Timestamps[0] = ts
			res.Values[0] = acc
			return res
		}
	}
}

type unsignedEmptyArrayCursor struct {
	res tsdb.UnsignedArray
}

var UnsignedEmptyArrayCursor tsdb.UnsignedArrayCursor = &unsignedEmptyArrayCursor{}

func (c *unsignedEmptyArrayCursor) Err() error                { return nil }
func (c *unsignedEmptyArrayCursor) Close()                    {}
func (c *unsignedEmptyArrayCursor) Next() *tsdb.UnsignedArray { return &c.res }

// ********************
// String Array Cursor

type stringArrayFilterCursor struct {
	tsdb.StringArrayCursor
	cond expression
	m    *singleValue
	res  *tsdb.StringArray
	tmp  *tsdb.StringArray
}

func newStringFilterArrayCursor(cond expression) *stringArrayFilterCursor {
	return &stringArrayFilterCursor{
		cond: cond,
		m:    &singleValue{},
		res:  tsdb.NewStringArrayLen(tsdb.DefaultMaxPointsPerBlock),
		tmp:  &tsdb.StringArray{},
	}
}

func (c *stringArrayFilterCursor) reset(cur tsdb.StringArrayCursor) {
	c.StringArrayCursor = cur
	c.tmp.Timestamps, c.tmp.Values = nil, nil
}

func (c *stringArrayFilterCursor) Next() *tsdb.StringArray {
	pos := 0
	var a *tsdb.StringArray

	if a.Len() > 0 {
		a = c.tmp
		c.tmp.Timestamps = nil
		c.tmp.Values = nil
	} else {
		a = c.StringArrayCursor.Next()
	}

LOOP:
	for len(a.Timestamps) > 0 {
		for i, v := range a.Values {
			c.m.v = v
			if c.cond.EvalBool(c.m) {
				c.res.Timestamps[pos] = a.Timestamps[i]
				c.res.Values[pos] = v
				pos++
				if pos >= tsdb.DefaultMaxPointsPerBlock {
					c.tmp.Timestamps = a.Timestamps[i+1:]
					c.tmp.Values = a.Values[i+1:]
					break LOOP
				}
			}
		}
		a = c.StringArrayCursor.Next()
	}

	c.res.Timestamps = c.res.Timestamps[:pos]
	c.res.Values = c.res.Values[:pos]

	return c.res
}

type stringMultiShardArrayCursor struct {
	tsdb.StringArrayCursor
	cursorContext
	filter *stringArrayFilterCursor
}

func (c *stringMultiShardArrayCursor) reset(cur tsdb.StringArrayCursor, itrs tsdb.CursorIterators, cond expression) {
	if cond != nil {
		if c.filter == nil {
			c.filter = newStringFilterArrayCursor(cond)
		}
		c.filter.reset(cur)
		cur = c.filter
	}

	c.StringArrayCursor = cur
	c.itrs = itrs
	c.err = nil
	c.count = 0
}

func (c *stringMultiShardArrayCursor) Err() error { return c.err }

func (c *stringMultiShardArrayCursor) Next() *tsdb.StringArray {
	for {
		a := c.StringArrayCursor.Next()
		if a.Len() == 0 {
			if c.nextArrayCursor() {
				continue
			}
		}
		c.count += int64(a.Len())
		if c.count > c.limit {
			diff := c.count - c.limit
			c.count -= diff
			rem := int64(a.Len()) - diff
			a.Timestamps = a.Timestamps[:rem]
			a.Values = a.Values[:rem]
		}
		return a
	}
}

func (c *stringMultiShardArrayCursor) nextArrayCursor() bool {
	if len(c.itrs) == 0 {
		return false
	}

	c.StringArrayCursor.Close()

	var itr tsdb.CursorIterator
	var cur tsdb.Cursor
	for cur == nil && len(c.itrs) > 0 {
		itr, c.itrs = c.itrs[0], c.itrs[1:]
		cur, _ = itr.Next(c.ctx, c.req)
	}

	var ok bool
	if cur != nil {
		var next tsdb.StringArrayCursor
		next, ok = cur.(tsdb.StringArrayCursor)
		if !ok {
			cur.Close()
			next = StringEmptyArrayCursor
			c.itrs = nil
			c.err = errors.New("expected string cursor")
		} else {
			if c.filter != nil {
				c.filter.reset(next)
				next = c.filter
			}
		}
		c.StringArrayCursor = next
	} else {
		c.StringArrayCursor = StringEmptyArrayCursor
	}

	return ok
}

type integerStringCountArrayCursor struct {
	tsdb.StringArrayCursor
}

func (c *integerStringCountArrayCursor) Next() *tsdb.IntegerArray {
	a := c.StringArrayCursor.Next()
	if len(a.Timestamps) == 0 {
		return &tsdb.IntegerArray{}
	}

	ts := a.Timestamps[0]
	var acc int64
	for {
		acc += int64(len(a.Timestamps))
		a = c.StringArrayCursor.Next()
		if len(a.Timestamps) == 0 {
			res := tsdb.NewIntegerArrayLen(1)
			res.Timestamps[0] = ts
			res.Values[0] = acc
			return res
		}
	}
}

type stringEmptyArrayCursor struct {
	res tsdb.StringArray
}

var StringEmptyArrayCursor tsdb.StringArrayCursor = &stringEmptyArrayCursor{}

func (c *stringEmptyArrayCursor) Err() error              { return nil }
func (c *stringEmptyArrayCursor) Close()                  {}
func (c *stringEmptyArrayCursor) Next() *tsdb.StringArray { return &c.res }

// ********************
// Boolean Array Cursor

type booleanArrayFilterCursor struct {
	tsdb.BooleanArrayCursor
	cond expression
	m    *singleValue
	res  *tsdb.BooleanArray
	tmp  *tsdb.BooleanArray
}

func newBooleanFilterArrayCursor(cond expression) *booleanArrayFilterCursor {
	return &booleanArrayFilterCursor{
		cond: cond,
		m:    &singleValue{},
		res:  tsdb.NewBooleanArrayLen(tsdb.DefaultMaxPointsPerBlock),
		tmp:  &tsdb.BooleanArray{},
	}
}

func (c *booleanArrayFilterCursor) reset(cur tsdb.BooleanArrayCursor) {
	c.BooleanArrayCursor = cur
	c.tmp.Timestamps, c.tmp.Values = nil, nil
}

func (c *booleanArrayFilterCursor) Next() *tsdb.BooleanArray {
	pos := 0
	var a *tsdb.BooleanArray

	if a.Len() > 0 {
		a = c.tmp
		c.tmp.Timestamps = nil
		c.tmp.Values = nil
	} else {
		a = c.BooleanArrayCursor.Next()
	}

LOOP:
	for len(a.Timestamps) > 0 {
		for i, v := range a.Values {
			c.m.v = v
			if c.cond.EvalBool(c.m) {
				c.res.Timestamps[pos] = a.Timestamps[i]
				c.res.Values[pos] = v
				pos++
				if pos >= tsdb.DefaultMaxPointsPerBlock {
					c.tmp.Timestamps = a.Timestamps[i+1:]
					c.tmp.Values = a.Values[i+1:]
					break LOOP
				}
			}
		}
		a = c.BooleanArrayCursor.Next()
	}

	c.res.Timestamps = c.res.Timestamps[:pos]
	c.res.Values = c.res.Values[:pos]

	return c.res
}

type booleanMultiShardArrayCursor struct {
	tsdb.BooleanArrayCursor
	cursorContext
	filter *booleanArrayFilterCursor
}

func (c *booleanMultiShardArrayCursor) reset(cur tsdb.BooleanArrayCursor, itrs tsdb.CursorIterators, cond expression) {
	if cond != nil {
		if c.filter == nil {
			c.filter = newBooleanFilterArrayCursor(cond)
		}
		c.filter.reset(cur)
		cur = c.filter
	}

	c.BooleanArrayCursor = cur
	c.itrs = itrs
	c.err = nil
	c.count = 0
}

func (c *booleanMultiShardArrayCursor) Err() error { return c.err }

func (c *booleanMultiShardArrayCursor) Next() *tsdb.BooleanArray {
	for {
		a := c.BooleanArrayCursor.Next()
		if a.Len() == 0 {
			if c.nextArrayCursor() {
				continue
			}
		}
		c.count += int64(a.Len())
		if c.count > c.limit {
			diff := c.count - c.limit
			c.count -= diff
			rem := int64(a.Len()) - diff
			a.Timestamps = a.Timestamps[:rem]
			a.Values = a.Values[:rem]
		}
		return a
	}
}

func (c *booleanMultiShardArrayCursor) nextArrayCursor() bool {
	if len(c.itrs) == 0 {
		return false
	}

	c.BooleanArrayCursor.Close()

	var itr tsdb.CursorIterator
	var cur tsdb.Cursor
	for cur == nil && len(c.itrs) > 0 {
		itr, c.itrs = c.itrs[0], c.itrs[1:]
		cur, _ = itr.Next(c.ctx, c.req)
	}

	var ok bool
	if cur != nil {
		var next tsdb.BooleanArrayCursor
		next, ok = cur.(tsdb.BooleanArrayCursor)
		if !ok {
			cur.Close()
			next = BooleanEmptyArrayCursor
			c.itrs = nil
			c.err = errors.New("expected boolean cursor")
		} else {
			if c.filter != nil {
				c.filter.reset(next)
				next = c.filter
			}
		}
		c.BooleanArrayCursor = next
	} else {
		c.BooleanArrayCursor = BooleanEmptyArrayCursor
	}

	return ok
}

type integerBooleanCountArrayCursor struct {
	tsdb.BooleanArrayCursor
}

func (c *integerBooleanCountArrayCursor) Next() *tsdb.IntegerArray {
	a := c.BooleanArrayCursor.Next()
	if len(a.Timestamps) == 0 {
		return &tsdb.IntegerArray{}
	}

	ts := a.Timestamps[0]
	var acc int64
	for {
		acc += int64(len(a.Timestamps))
		a = c.BooleanArrayCursor.Next()
		if len(a.Timestamps) == 0 {
			res := tsdb.NewIntegerArrayLen(1)
			res.Timestamps[0] = ts
			res.Values[0] = acc
			return res
		}
	}
}

type booleanEmptyArrayCursor struct {
	res tsdb.BooleanArray
}

var BooleanEmptyArrayCursor tsdb.BooleanArrayCursor = &booleanEmptyArrayCursor{}

func (c *booleanEmptyArrayCursor) Err() error               { return nil }
func (c *booleanEmptyArrayCursor) Close()                   {}
func (c *booleanEmptyArrayCursor) Next() *tsdb.BooleanArray { return &c.res }

type singleValue struct {
	v interface{}
}

func (v *singleValue) Value(key string) (interface{}, bool) {
	return v.v, true
}

type expression interface {
	EvalBool(v valuer) bool
}

// valuer is the interface that wraps the Value() method.
type valuer interface {
	// Value returns the value and existence flag for a given key.
	Value(key string) (interface{}, bool)
}

type cursorContext struct {
	ctx   context.Context
	req   *tsdb.CursorRequest
	itrs  tsdb.CursorIterators
	limit int64
	count int64
	err   error
}

type storageTable interface {
	flux.Table
	Close()
	Done() chan struct{}
}

type storeReader struct {
	s Store
}

func NewReader(s Store) fstorage.Reader {
	return &storeReader{s: s}
}

func (r *storeReader) Read(ctx context.Context, rs fstorage.ReadSpec, start, stop execute.Time) (flux.TableIterator, error) {
	var predicate *Predicate
	if rs.Predicate != nil {
		p, err := ToStoragePredicate(rs.Predicate)
		if err != nil {
			return nil, err
		}
		predicate = (*Predicate)(p)
	}

	return &tableIterator{
		ctx:       ctx,
		bounds:    execute.Bounds{Start: start, Stop: stop},
		s:         r.s,
		readSpec:  rs,
		predicate: predicate,
	}, nil
}

func (r *storeReader) Close() {}

type tableIterator struct {
	ctx       context.Context
	bounds    execute.Bounds
	s         Store
	readSpec  fstorage.ReadSpec
	predicate *Predicate
}

func (bi *tableIterator) Do(f func(flux.Table) error) error {
	// TODO(jeff): THIS IS WAY WRONG! in the sense that i pulled in the wrong
	// ReadSource, but I expect this to all go away before we need to fix it.
	src := ReadSource{
		RetentionPolicy: string(bi.readSpec.OrganizationID),
		Database:        string(bi.readSpec.BucketID),
	}
	if i := strings.IndexByte(src.Database, '/'); i > -1 {
		src.RetentionPolicy = src.Database[i+1:]
		src.Database = src.Database[:i]
	}

	// Setup read request
	var req ReadRequest
	if any, err := types.MarshalAny(&src); err != nil {
		return err
	} else {
		req.ReadSource = any
	}
	req.Predicate = bi.predicate
	req.Descending = bi.readSpec.Descending
	req.TimestampRange.Start = int64(bi.bounds.Start)
	req.TimestampRange.End = int64(bi.bounds.Stop)
	req.Group = convertGroupMode(bi.readSpec.GroupMode)
	req.GroupKeys = bi.readSpec.GroupKeys
	req.SeriesLimit = bi.readSpec.SeriesLimit
	req.PointsLimit = bi.readSpec.PointsLimit
	req.SeriesOffset = bi.readSpec.SeriesOffset

	if req.PointsLimit == -1 {
		req.Hints.SetNoPoints()
	}

	if agg, err := determineAggregateMethod(bi.readSpec.AggregateMethod); err != nil {
		return err
	} else if agg != AggregateTypeNone {
		req.Aggregate = &Aggregate{Type: agg}
	}

	switch {
	case req.Group != GroupAll:
		rs, err := bi.s.GroupRead(bi.ctx, &req)
		if err != nil {
			return err
		}

		if req.Hints.NoPoints() {
			return bi.handleGroupReadNoPoints(f, rs)
		}
		return bi.handleGroupRead(f, rs)

	default:
		rs, err := bi.s.Read(bi.ctx, &req)
		if err != nil {
			return err
		}

		if req.Hints.NoPoints() {
			return bi.handleReadNoPoints(f, rs)
		}
		return bi.handleRead(f, rs)
	}
}

func normalizeTags(tags models.Tags) {
	for i, tag := range tags {
		if len(tag.Key) == 2 && tag.Key[0] == '_' {
			switch tag.Key[1] {
			case 'f':
				tags[i].Key = fieldKeyBytes
			case 'm':
				tags[i].Key = measurementKeyBytes
			}
		}
	}
}

func (bi *tableIterator) handleRead(f func(flux.Table) error, rs ResultSet) error {
	defer func() {
		rs.Close()
	}()

READ:
	for rs.Next() {
		cur := rs.Cursor()
		if cur == nil {
			// no data for series key + field combination
			continue
		}

		key := groupKeyForSeries(rs.Tags(), &bi.readSpec, bi.bounds)
		var table storageTable

		switch cur := cur.(type) {
		case tsdb.IntegerArrayCursor:
			cols, defs := determineTableColsForSeries(rs.Tags(), flux.TInt)
			table = newIntegerTable(cur, bi.bounds, key, cols, rs.Tags(), defs)
		case tsdb.FloatArrayCursor:
			cols, defs := determineTableColsForSeries(rs.Tags(), flux.TFloat)
			table = newFloatTable(cur, bi.bounds, key, cols, rs.Tags(), defs)
		case tsdb.UnsignedArrayCursor:
			cols, defs := determineTableColsForSeries(rs.Tags(), flux.TUInt)
			table = newUnsignedTable(cur, bi.bounds, key, cols, rs.Tags(), defs)
		case tsdb.BooleanArrayCursor:
			cols, defs := determineTableColsForSeries(rs.Tags(), flux.TBool)
			table = newBooleanTable(cur, bi.bounds, key, cols, rs.Tags(), defs)
		case tsdb.StringArrayCursor:
			cols, defs := determineTableColsForSeries(rs.Tags(), flux.TString)
			table = newStringTable(cur, bi.bounds, key, cols, rs.Tags(), defs)
		default:
			panic(fmt.Sprintf("unreachable: %T", cur))
		}

		if table.Empty() {
			table.Close()
			continue
		}

		if err := f(table); err != nil {
			table.Close()
			return err
		}
		select {
		case <-table.Done():
		case <-bi.ctx.Done():
			break READ
		}
	}
	return nil
}

func (bi *tableIterator) handleReadNoPoints(f func(flux.Table) error, rs ResultSet) error {
	defer func() {
		rs.Close()
	}()

READ:
	for rs.Next() {
		cur := rs.Cursor()
		if !hasPoints(cur) {
			// no data for series key + field combination
			continue
		}

		key := groupKeyForSeries(rs.Tags(), &bi.readSpec, bi.bounds)
		cols, defs := determineTableColsForSeries(rs.Tags(), flux.TString)
		table := newTableNoPoints(bi.bounds, key, cols, rs.Tags(), defs)

		if err := f(table); err != nil {
			table.Close()
			return err
		}
		select {
		case <-table.Done():
		case <-bi.ctx.Done():
			break READ
		}
	}
	return nil
}

func (bi *tableIterator) handleGroupRead(f func(flux.Table) error, rs GroupResultSet) error {
	defer func() {
		rs.Close()
	}()
	gc := rs.Next()
READ:
	for gc != nil {
		var cur tsdb.Cursor
		for gc.Next() {
			cur = gc.Cursor()
			if cur != nil {
				break
			}
		}

		if cur == nil {
			gc = rs.Next()
			continue
		}

		key := groupKeyForGroup(gc.PartitionKeyVals(), &bi.readSpec, bi.bounds)
		var table storageTable

		switch cur := cur.(type) {
		case tsdb.IntegerArrayCursor:
			cols, defs := determineTableColsForGroup(gc.Keys(), flux.TInt)
			table = newIntegerGroupTable(gc, cur, bi.bounds, key, cols, gc.Tags(), defs)
		case tsdb.FloatArrayCursor:
			cols, defs := determineTableColsForGroup(gc.Keys(), flux.TFloat)
			table = newFloatGroupTable(gc, cur, bi.bounds, key, cols, gc.Tags(), defs)
		case tsdb.UnsignedArrayCursor:
			cols, defs := determineTableColsForGroup(gc.Keys(), flux.TUInt)
			table = newUnsignedGroupTable(gc, cur, bi.bounds, key, cols, gc.Tags(), defs)
		case tsdb.BooleanArrayCursor:
			cols, defs := determineTableColsForGroup(gc.Keys(), flux.TBool)
			table = newBooleanGroupTable(gc, cur, bi.bounds, key, cols, gc.Tags(), defs)
		case tsdb.StringArrayCursor:
			cols, defs := determineTableColsForGroup(gc.Keys(), flux.TString)
			table = newStringGroupTable(gc, cur, bi.bounds, key, cols, gc.Tags(), defs)
		default:
			panic(fmt.Sprintf("unreachable: %T", cur))
		}

		if err := f(table); err != nil {
			table.Close()
			return err
		}
		// Wait until the table has been read.
		select {
		case <-table.Done():
		case <-bi.ctx.Done():
			break READ
		}

		gc = rs.Next()
	}
	return nil
}

func (bi *tableIterator) handleGroupReadNoPoints(f func(flux.Table) error, rs GroupResultSet) error {
	defer func() {
		rs.Close()
	}()
	gc := rs.Next()
READ:
	for gc != nil {
		key := groupKeyForGroup(gc.PartitionKeyVals(), &bi.readSpec, bi.bounds)
		cols, defs := determineTableColsForGroup(gc.Keys(), flux.TString)
		table := newGroupTableNoPoints(gc, bi.bounds, key, cols, gc.Tags(), defs)

		if err := f(table); err != nil {
			table.Close()
			return err
		}
		// Wait until the table has been read.
		select {
		case <-table.Done():
		case <-bi.ctx.Done():
			break READ
		}

		gc = rs.Next()
	}
	return nil
}

func determineAggregateMethod(agg string) (Aggregate_AggregateType, error) {
	if agg == "" {
		return AggregateTypeNone, nil
	}

	if t, ok := Aggregate_AggregateType_value[strings.ToUpper(agg)]; ok {
		return Aggregate_AggregateType(t), nil
	}
	return 0, fmt.Errorf("unknown aggregate type %q", agg)
}

func convertGroupMode(m fstorage.GroupMode) ReadRequest_Group {
	switch m {
	case fstorage.GroupModeNone:
		return GroupNone
	case fstorage.GroupModeBy:
		return GroupBy
	case fstorage.GroupModeExcept:
		return GroupExcept

	case fstorage.GroupModeDefault, fstorage.GroupModeAll:
		fallthrough
	default:
		return GroupAll
	}
}

const (
	startColIdx = 0
	stopColIdx  = 1
	timeColIdx  = 2
	valueColIdx = 3
)

func determineTableColsForSeries(tags models.Tags, typ flux.DataType) ([]flux.ColMeta, [][]byte) {
	cols := make([]flux.ColMeta, 4+len(tags))
	defs := make([][]byte, 4+len(tags))
	cols[startColIdx] = flux.ColMeta{
		Label: execute.DefaultStartColLabel,
		Type:  flux.TTime,
	}
	cols[stopColIdx] = flux.ColMeta{
		Label: execute.DefaultStopColLabel,
		Type:  flux.TTime,
	}
	cols[timeColIdx] = flux.ColMeta{
		Label: execute.DefaultTimeColLabel,
		Type:  flux.TTime,
	}
	cols[valueColIdx] = flux.ColMeta{
		Label: execute.DefaultValueColLabel,
		Type:  typ,
	}
	for j, tag := range tags {
		cols[4+j] = flux.ColMeta{
			Label: string(tag.Key),
			Type:  flux.TString,
		}
		defs[4+j] = []byte("")
	}
	return cols, defs
}

func groupKeyForSeries(tags models.Tags, readSpec *fstorage.ReadSpec, bnds execute.Bounds) flux.GroupKey {
	cols := make([]flux.ColMeta, 2, len(tags))
	vs := make([]values.Value, 2, len(tags))
	cols[0] = flux.ColMeta{
		Label: execute.DefaultStartColLabel,
		Type:  flux.TTime,
	}
	vs[0] = values.NewTimeValue(bnds.Start)
	cols[1] = flux.ColMeta{
		Label: execute.DefaultStopColLabel,
		Type:  flux.TTime,
	}
	vs[1] = values.NewTimeValue(bnds.Stop)
	switch readSpec.GroupMode {
	case fstorage.GroupModeBy:
		// group key in GroupKeys order, including tags in the GroupKeys slice
		for _, k := range readSpec.GroupKeys {
			if v := tags.Get([]byte(k)); len(v) > 0 {
				cols = append(cols, flux.ColMeta{
					Label: k,
					Type:  flux.TString,
				})
				vs = append(vs, values.NewStringValue(string(v)))
			}
		}
	case fstorage.GroupModeExcept:
		// group key in GroupKeys order, skipping tags in the GroupKeys slice
		panic("not implemented")
	case fstorage.GroupModeDefault, fstorage.GroupModeAll:
		for i := range tags {
			cols = append(cols, flux.ColMeta{
				Label: string(tags[i].Key),
				Type:  flux.TString,
			})
			vs = append(vs, values.NewStringValue(string(tags[i].Value)))
		}
	}
	return execute.NewGroupKey(cols, vs)
}

func determineTableColsForGroup(tagKeys [][]byte, typ flux.DataType) ([]flux.ColMeta, [][]byte) {
	cols := make([]flux.ColMeta, 4+len(tagKeys))
	defs := make([][]byte, 4+len(tagKeys))
	cols[startColIdx] = flux.ColMeta{
		Label: execute.DefaultStartColLabel,
		Type:  flux.TTime,
	}
	cols[stopColIdx] = flux.ColMeta{
		Label: execute.DefaultStopColLabel,
		Type:  flux.TTime,
	}
	cols[timeColIdx] = flux.ColMeta{
		Label: execute.DefaultTimeColLabel,
		Type:  flux.TTime,
	}
	cols[valueColIdx] = flux.ColMeta{
		Label: execute.DefaultValueColLabel,
		Type:  typ,
	}
	for j, tag := range tagKeys {
		cols[4+j] = flux.ColMeta{
			Label: string(tag),
			Type:  flux.TString,
		}
		defs[4+j] = []byte("")

	}
	return cols, defs
}

func groupKeyForGroup(kv [][]byte, readSpec *fstorage.ReadSpec, bnds execute.Bounds) flux.GroupKey {
	cols := make([]flux.ColMeta, 2, len(readSpec.GroupKeys)+2)
	vs := make([]values.Value, 2, len(readSpec.GroupKeys)+2)
	cols[0] = flux.ColMeta{
		Label: execute.DefaultStartColLabel,
		Type:  flux.TTime,
	}
	vs[0] = values.NewTimeValue(bnds.Start)
	cols[1] = flux.ColMeta{
		Label: execute.DefaultStopColLabel,
		Type:  flux.TTime,
	}
	vs[1] = values.NewTimeValue(bnds.Stop)
	for i := range readSpec.GroupKeys {
		cols = append(cols, flux.ColMeta{
			Label: readSpec.GroupKeys[i],
			Type:  flux.TString,
		})
		vs = append(vs, values.NewStringValue(string(kv[i])))
	}
	return execute.NewGroupKey(cols, vs)
}

//
// *********** Float ***********
//

type floatTable struct {
	table
	cur    tsdb.FloatArrayCursor
	valBuf []float64
}

func newFloatTable(
	cur tsdb.FloatArrayCursor,
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
) *floatTable {
	t := &floatTable{
		table: newTable(bounds, key, cols, defs),
		cur:   cur,
	}
	t.readTags(tags)
	t.more = t.advance()

	return t
}

func (t *floatTable) Close() {
	if t.cur != nil {
		t.cur.Close()
		t.cur = nil
	}
	if t.done != nil {
		close(t.done)
		t.done = nil
	}
}

func (t *floatTable) Do(f func(flux.ColReader) error) error {
	defer t.Close()

	if !t.more {
		return t.err
	}

	f(t)
	for t.advance() {
		if err := f(t); err != nil {
			return err
		}
	}

	return t.err
}

func (t *floatTable) advance() bool {
	a := t.cur.Next()
	t.l = a.Len()
	if t.l == 0 {
		return false
	}

	if cap(t.timeBuf) < t.l {
		t.timeBuf = make([]execute.Time, t.l)
	} else {
		t.timeBuf = t.timeBuf[:t.l]
	}

	for i := range a.Timestamps {
		t.timeBuf[i] = execute.Time(a.Timestamps[i])
	}

	if cap(t.valBuf) < t.l {
		t.valBuf = make([]float64, t.l)
	} else {
		t.valBuf = t.valBuf[:t.l]
	}

	copy(t.valBuf, a.Values)
	t.colBufs[timeColIdx] = t.timeBuf
	t.colBufs[valueColIdx] = t.valBuf
	t.appendTags()
	t.appendBounds()
	t.empty = false
	return true
}

// group table

type floatGroupTable struct {
	table
	gc     GroupCursor
	cur    tsdb.FloatArrayCursor
	valBuf []float64
}

func newFloatGroupTable(
	gc GroupCursor,
	cur tsdb.FloatArrayCursor,
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
) *floatGroupTable {
	t := &floatGroupTable{
		table: newTable(bounds, key, cols, defs),
		gc:    gc,
		cur:   cur,
	}
	t.readTags(tags)
	t.more = t.advance()

	return t
}

func (t *floatGroupTable) Close() {
	if t.cur != nil {
		t.cur.Close()
		t.cur = nil
	}
	if t.gc != nil {
		t.gc.Close()
		t.gc = nil
	}
	if t.done != nil {
		close(t.done)
		t.done = nil
	}
}

func (t *floatGroupTable) Do(f func(flux.ColReader) error) error {
	defer t.Close()

	if !t.more {
		return t.err
	}

	f(t)
	for t.advance() {
		if err := f(t); err != nil {
			return err
		}
	}

	return t.err
}

func (t *floatGroupTable) advance() bool {
RETRY:
	a := t.cur.Next()
	t.l = a.Len()
	if t.l == 0 {
		if t.advanceCursor() {
			goto RETRY
		}

		return false
	}

	if cap(t.timeBuf) < t.l {
		t.timeBuf = make([]execute.Time, t.l)
	} else {
		t.timeBuf = t.timeBuf[:t.l]
	}

	for i := range a.Timestamps {
		t.timeBuf[i] = execute.Time(a.Timestamps[i])
	}

	if cap(t.valBuf) < t.l {
		t.valBuf = make([]float64, t.l)
	} else {
		t.valBuf = t.valBuf[:t.l]
	}

	copy(t.valBuf, a.Values)
	t.colBufs[timeColIdx] = t.timeBuf
	t.colBufs[valueColIdx] = t.valBuf
	t.appendTags()
	t.appendBounds()
	t.empty = false
	return true
}

func (t *floatGroupTable) advanceCursor() bool {
	t.cur.Close()
	t.cur = nil
	for t.gc.Next() {
		cur := t.gc.Cursor()
		if cur == nil {
			continue
		}

		if cur, ok := cur.(tsdb.FloatArrayCursor); !ok {
			// TODO(sgc): error or skip?
			cur.Close()
			t.err = fmt.Errorf("expected float cursor type, got %T", cur)
			return false
		} else {
			t.readTags(t.gc.Tags())
			t.cur = cur
			return true
		}
	}
	return false
}

//
// *********** Integer ***********
//

type integerTable struct {
	table
	cur    tsdb.IntegerArrayCursor
	valBuf []int64
}

func newIntegerTable(
	cur tsdb.IntegerArrayCursor,
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
) *integerTable {
	t := &integerTable{
		table: newTable(bounds, key, cols, defs),
		cur:   cur,
	}
	t.readTags(tags)
	t.more = t.advance()

	return t
}

func (t *integerTable) Close() {
	if t.cur != nil {
		t.cur.Close()
		t.cur = nil
	}
	if t.done != nil {
		close(t.done)
		t.done = nil
	}
}

func (t *integerTable) Do(f func(flux.ColReader) error) error {
	defer t.Close()

	if !t.more {
		return t.err
	}

	f(t)
	for t.advance() {
		if err := f(t); err != nil {
			return err
		}
	}

	return t.err
}

func (t *integerTable) advance() bool {
	a := t.cur.Next()
	t.l = a.Len()
	if t.l == 0 {
		return false
	}

	if cap(t.timeBuf) < t.l {
		t.timeBuf = make([]execute.Time, t.l)
	} else {
		t.timeBuf = t.timeBuf[:t.l]
	}

	for i := range a.Timestamps {
		t.timeBuf[i] = execute.Time(a.Timestamps[i])
	}

	if cap(t.valBuf) < t.l {
		t.valBuf = make([]int64, t.l)
	} else {
		t.valBuf = t.valBuf[:t.l]
	}

	copy(t.valBuf, a.Values)
	t.colBufs[timeColIdx] = t.timeBuf
	t.colBufs[valueColIdx] = t.valBuf
	t.appendTags()
	t.appendBounds()
	t.empty = false
	return true
}

// group table

type integerGroupTable struct {
	table
	gc     GroupCursor
	cur    tsdb.IntegerArrayCursor
	valBuf []int64
}

func newIntegerGroupTable(
	gc GroupCursor,
	cur tsdb.IntegerArrayCursor,
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
) *integerGroupTable {
	t := &integerGroupTable{
		table: newTable(bounds, key, cols, defs),
		gc:    gc,
		cur:   cur,
	}
	t.readTags(tags)
	t.more = t.advance()

	return t
}

func (t *integerGroupTable) Close() {
	if t.cur != nil {
		t.cur.Close()
		t.cur = nil
	}
	if t.gc != nil {
		t.gc.Close()
		t.gc = nil
	}
	if t.done != nil {
		close(t.done)
		t.done = nil
	}
}

func (t *integerGroupTable) Do(f func(flux.ColReader) error) error {
	defer t.Close()

	if !t.more {
		return t.err
	}

	f(t)
	for t.advance() {
		if err := f(t); err != nil {
			return err
		}
	}

	return t.err
}

func (t *integerGroupTable) advance() bool {
RETRY:
	a := t.cur.Next()
	t.l = a.Len()
	if t.l == 0 {
		if t.advanceCursor() {
			goto RETRY
		}

		return false
	}

	if cap(t.timeBuf) < t.l {
		t.timeBuf = make([]execute.Time, t.l)
	} else {
		t.timeBuf = t.timeBuf[:t.l]
	}

	for i := range a.Timestamps {
		t.timeBuf[i] = execute.Time(a.Timestamps[i])
	}

	if cap(t.valBuf) < t.l {
		t.valBuf = make([]int64, t.l)
	} else {
		t.valBuf = t.valBuf[:t.l]
	}

	copy(t.valBuf, a.Values)
	t.colBufs[timeColIdx] = t.timeBuf
	t.colBufs[valueColIdx] = t.valBuf
	t.appendTags()
	t.appendBounds()
	t.empty = false
	return true
}

func (t *integerGroupTable) advanceCursor() bool {
	t.cur.Close()
	t.cur = nil
	for t.gc.Next() {
		cur := t.gc.Cursor()
		if cur == nil {
			continue
		}

		if cur, ok := cur.(tsdb.IntegerArrayCursor); !ok {
			// TODO(sgc): error or skip?
			cur.Close()
			t.err = fmt.Errorf("expected integer cursor type, got %T", cur)
			return false
		} else {
			t.readTags(t.gc.Tags())
			t.cur = cur
			return true
		}
	}
	return false
}

//
// *********** Unsigned ***********
//

type unsignedTable struct {
	table
	cur    tsdb.UnsignedArrayCursor
	valBuf []uint64
}

func newUnsignedTable(
	cur tsdb.UnsignedArrayCursor,
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
) *unsignedTable {
	t := &unsignedTable{
		table: newTable(bounds, key, cols, defs),
		cur:   cur,
	}
	t.readTags(tags)
	t.more = t.advance()

	return t
}

func (t *unsignedTable) Close() {
	if t.cur != nil {
		t.cur.Close()
		t.cur = nil
	}
	if t.done != nil {
		close(t.done)
		t.done = nil
	}
}

func (t *unsignedTable) Do(f func(flux.ColReader) error) error {
	defer t.Close()

	if !t.more {
		return t.err
	}

	f(t)
	for t.advance() {
		if err := f(t); err != nil {
			return err
		}
	}

	return t.err
}

func (t *unsignedTable) advance() bool {
	a := t.cur.Next()
	t.l = a.Len()
	if t.l == 0 {
		return false
	}

	if cap(t.timeBuf) < t.l {
		t.timeBuf = make([]execute.Time, t.l)
	} else {
		t.timeBuf = t.timeBuf[:t.l]
	}

	for i := range a.Timestamps {
		t.timeBuf[i] = execute.Time(a.Timestamps[i])
	}

	if cap(t.valBuf) < t.l {
		t.valBuf = make([]uint64, t.l)
	} else {
		t.valBuf = t.valBuf[:t.l]
	}

	copy(t.valBuf, a.Values)
	t.colBufs[timeColIdx] = t.timeBuf
	t.colBufs[valueColIdx] = t.valBuf
	t.appendTags()
	t.appendBounds()
	t.empty = false
	return true
}

// group table

type unsignedGroupTable struct {
	table
	gc     GroupCursor
	cur    tsdb.UnsignedArrayCursor
	valBuf []uint64
}

func newUnsignedGroupTable(
	gc GroupCursor,
	cur tsdb.UnsignedArrayCursor,
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
) *unsignedGroupTable {
	t := &unsignedGroupTable{
		table: newTable(bounds, key, cols, defs),
		gc:    gc,
		cur:   cur,
	}
	t.readTags(tags)
	t.more = t.advance()

	return t
}

func (t *unsignedGroupTable) Close() {
	if t.cur != nil {
		t.cur.Close()
		t.cur = nil
	}
	if t.gc != nil {
		t.gc.Close()
		t.gc = nil
	}
	if t.done != nil {
		close(t.done)
		t.done = nil
	}
}

func (t *unsignedGroupTable) Do(f func(flux.ColReader) error) error {
	defer t.Close()

	if !t.more {
		return t.err
	}

	f(t)
	for t.advance() {
		if err := f(t); err != nil {
			return err
		}
	}

	return t.err
}

func (t *unsignedGroupTable) advance() bool {
RETRY:
	a := t.cur.Next()
	t.l = a.Len()
	if t.l == 0 {
		if t.advanceCursor() {
			goto RETRY
		}

		return false
	}

	if cap(t.timeBuf) < t.l {
		t.timeBuf = make([]execute.Time, t.l)
	} else {
		t.timeBuf = t.timeBuf[:t.l]
	}

	for i := range a.Timestamps {
		t.timeBuf[i] = execute.Time(a.Timestamps[i])
	}

	if cap(t.valBuf) < t.l {
		t.valBuf = make([]uint64, t.l)
	} else {
		t.valBuf = t.valBuf[:t.l]
	}

	copy(t.valBuf, a.Values)
	t.colBufs[timeColIdx] = t.timeBuf
	t.colBufs[valueColIdx] = t.valBuf
	t.appendTags()
	t.appendBounds()
	t.empty = false
	return true
}

func (t *unsignedGroupTable) advanceCursor() bool {
	t.cur.Close()
	t.cur = nil
	for t.gc.Next() {
		cur := t.gc.Cursor()
		if cur == nil {
			continue
		}

		if cur, ok := cur.(tsdb.UnsignedArrayCursor); !ok {
			// TODO(sgc): error or skip?
			cur.Close()
			t.err = fmt.Errorf("expected unsigned cursor type, got %T", cur)
			return false
		} else {
			t.readTags(t.gc.Tags())
			t.cur = cur
			return true
		}
	}
	return false
}

//
// *********** String ***********
//

type stringTable struct {
	table
	cur    tsdb.StringArrayCursor
	valBuf []string
}

func newStringTable(
	cur tsdb.StringArrayCursor,
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
) *stringTable {
	t := &stringTable{
		table: newTable(bounds, key, cols, defs),
		cur:   cur,
	}
	t.readTags(tags)
	t.more = t.advance()

	return t
}

func (t *stringTable) Close() {
	if t.cur != nil {
		t.cur.Close()
		t.cur = nil
	}
	if t.done != nil {
		close(t.done)
		t.done = nil
	}
}

func (t *stringTable) Do(f func(flux.ColReader) error) error {
	defer t.Close()

	if !t.more {
		return t.err
	}

	f(t)
	for t.advance() {
		if err := f(t); err != nil {
			return err
		}
	}

	return t.err
}

func (t *stringTable) advance() bool {
	a := t.cur.Next()
	t.l = a.Len()
	if t.l == 0 {
		return false
	}

	if cap(t.timeBuf) < t.l {
		t.timeBuf = make([]execute.Time, t.l)
	} else {
		t.timeBuf = t.timeBuf[:t.l]
	}

	for i := range a.Timestamps {
		t.timeBuf[i] = execute.Time(a.Timestamps[i])
	}

	if cap(t.valBuf) < t.l {
		t.valBuf = make([]string, t.l)
	} else {
		t.valBuf = t.valBuf[:t.l]
	}

	copy(t.valBuf, a.Values)
	t.colBufs[timeColIdx] = t.timeBuf
	t.colBufs[valueColIdx] = t.valBuf
	t.appendTags()
	t.appendBounds()
	t.empty = false
	return true
}

// group table

type stringGroupTable struct {
	table
	gc     GroupCursor
	cur    tsdb.StringArrayCursor
	valBuf []string
}

func newStringGroupTable(
	gc GroupCursor,
	cur tsdb.StringArrayCursor,
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
) *stringGroupTable {
	t := &stringGroupTable{
		table: newTable(bounds, key, cols, defs),
		gc:    gc,
		cur:   cur,
	}
	t.readTags(tags)
	t.more = t.advance()

	return t
}

func (t *stringGroupTable) Close() {
	if t.cur != nil {
		t.cur.Close()
		t.cur = nil
	}
	if t.gc != nil {
		t.gc.Close()
		t.gc = nil
	}
	if t.done != nil {
		close(t.done)
		t.done = nil
	}
}

func (t *stringGroupTable) Do(f func(flux.ColReader) error) error {
	defer t.Close()

	if !t.more {
		return t.err
	}

	f(t)
	for t.advance() {
		if err := f(t); err != nil {
			return err
		}
	}

	return t.err
}

func (t *stringGroupTable) advance() bool {
RETRY:
	a := t.cur.Next()
	t.l = a.Len()
	if t.l == 0 {
		if t.advanceCursor() {
			goto RETRY
		}

		return false
	}

	if cap(t.timeBuf) < t.l {
		t.timeBuf = make([]execute.Time, t.l)
	} else {
		t.timeBuf = t.timeBuf[:t.l]
	}

	for i := range a.Timestamps {
		t.timeBuf[i] = execute.Time(a.Timestamps[i])
	}

	if cap(t.valBuf) < t.l {
		t.valBuf = make([]string, t.l)
	} else {
		t.valBuf = t.valBuf[:t.l]
	}

	copy(t.valBuf, a.Values)
	t.colBufs[timeColIdx] = t.timeBuf
	t.colBufs[valueColIdx] = t.valBuf
	t.appendTags()
	t.appendBounds()
	t.empty = false
	return true
}

func (t *stringGroupTable) advanceCursor() bool {
	t.cur.Close()
	t.cur = nil
	for t.gc.Next() {
		cur := t.gc.Cursor()
		if cur == nil {
			continue
		}

		if cur, ok := cur.(tsdb.StringArrayCursor); !ok {
			// TODO(sgc): error or skip?
			cur.Close()
			t.err = fmt.Errorf("expected string cursor type, got %T", cur)
			return false
		} else {
			t.readTags(t.gc.Tags())
			t.cur = cur
			return true
		}
	}
	return false
}

//
// *********** Boolean ***********
//

type booleanTable struct {
	table
	cur    tsdb.BooleanArrayCursor
	valBuf []bool
}

func newBooleanTable(
	cur tsdb.BooleanArrayCursor,
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
) *booleanTable {
	t := &booleanTable{
		table: newTable(bounds, key, cols, defs),
		cur:   cur,
	}
	t.readTags(tags)
	t.more = t.advance()

	return t
}

func (t *booleanTable) Close() {
	if t.cur != nil {
		t.cur.Close()
		t.cur = nil
	}
	if t.done != nil {
		close(t.done)
		t.done = nil
	}
}

func (t *booleanTable) Do(f func(flux.ColReader) error) error {
	defer t.Close()

	if !t.more {
		return t.err
	}

	f(t)
	for t.advance() {
		if err := f(t); err != nil {
			return err
		}
	}

	return t.err
}

func (t *booleanTable) advance() bool {
	a := t.cur.Next()
	t.l = a.Len()
	if t.l == 0 {
		return false
	}

	if cap(t.timeBuf) < t.l {
		t.timeBuf = make([]execute.Time, t.l)
	} else {
		t.timeBuf = t.timeBuf[:t.l]
	}

	for i := range a.Timestamps {
		t.timeBuf[i] = execute.Time(a.Timestamps[i])
	}

	if cap(t.valBuf) < t.l {
		t.valBuf = make([]bool, t.l)
	} else {
		t.valBuf = t.valBuf[:t.l]
	}

	copy(t.valBuf, a.Values)
	t.colBufs[timeColIdx] = t.timeBuf
	t.colBufs[valueColIdx] = t.valBuf
	t.appendTags()
	t.appendBounds()
	t.empty = false
	return true
}

// group table

type booleanGroupTable struct {
	table
	gc     GroupCursor
	cur    tsdb.BooleanArrayCursor
	valBuf []bool
}

func newBooleanGroupTable(
	gc GroupCursor,
	cur tsdb.BooleanArrayCursor,
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
) *booleanGroupTable {
	t := &booleanGroupTable{
		table: newTable(bounds, key, cols, defs),
		gc:    gc,
		cur:   cur,
	}
	t.readTags(tags)
	t.more = t.advance()

	return t
}

func (t *booleanGroupTable) Close() {
	if t.cur != nil {
		t.cur.Close()
		t.cur = nil
	}
	if t.gc != nil {
		t.gc.Close()
		t.gc = nil
	}
	if t.done != nil {
		close(t.done)
		t.done = nil
	}
}

func (t *booleanGroupTable) Do(f func(flux.ColReader) error) error {
	defer t.Close()

	if !t.more {
		return t.err
	}

	f(t)
	for t.advance() {
		if err := f(t); err != nil {
			return err
		}
	}

	return t.err
}

func (t *booleanGroupTable) advance() bool {
RETRY:
	a := t.cur.Next()
	t.l = a.Len()
	if t.l == 0 {
		if t.advanceCursor() {
			goto RETRY
		}

		return false
	}

	if cap(t.timeBuf) < t.l {
		t.timeBuf = make([]execute.Time, t.l)
	} else {
		t.timeBuf = t.timeBuf[:t.l]
	}

	for i := range a.Timestamps {
		t.timeBuf[i] = execute.Time(a.Timestamps[i])
	}

	if cap(t.valBuf) < t.l {
		t.valBuf = make([]bool, t.l)
	} else {
		t.valBuf = t.valBuf[:t.l]
	}

	copy(t.valBuf, a.Values)
	t.colBufs[timeColIdx] = t.timeBuf
	t.colBufs[valueColIdx] = t.valBuf
	t.appendTags()
	t.appendBounds()
	t.empty = false
	return true
}

func (t *booleanGroupTable) advanceCursor() bool {
	t.cur.Close()
	t.cur = nil
	for t.gc.Next() {
		cur := t.gc.Cursor()
		if cur == nil {
			continue
		}

		if cur, ok := cur.(tsdb.BooleanArrayCursor); !ok {
			// TODO(sgc): error or skip?
			cur.Close()
			t.err = fmt.Errorf("expected boolean cursor type, got %T", cur)
			return false
		} else {
			t.readTags(t.gc.Tags())
			t.cur = cur
			return true
		}
	}
	return false
}

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

	colBufs []interface{}
	timeBuf []execute.Time

	err error

	empty bool
	more  bool
}

func newTable(
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	defs [][]byte,
) table {
	return table{
		bounds:  bounds,
		key:     key,
		tags:    make([][]byte, len(cols)),
		defs:    defs,
		colBufs: make([]interface{}, len(cols)),
		cols:    cols,
		done:    make(chan struct{}),
		empty:   true,
	}
}

func (t *table) Done() chan struct{}  { return t.done }
func (t *table) Key() flux.GroupKey   { return t.key }
func (t *table) Cols() []flux.ColMeta { return t.cols }
func (t *table) RefCount(n int)       {}
func (t *table) Err() error           { return t.err }
func (t *table) Empty() bool          { return t.empty }
func (t *table) Len() int             { return t.l }

func (t *table) Bools(j int) []bool {
	execute.CheckColType(t.cols[j], flux.TBool)
	return t.colBufs[j].([]bool)
}

func (t *table) Ints(j int) []int64 {
	execute.CheckColType(t.cols[j], flux.TInt)
	return t.colBufs[j].([]int64)
}

func (t *table) UInts(j int) []uint64 {
	execute.CheckColType(t.cols[j], flux.TUInt)
	return t.colBufs[j].([]uint64)
}

func (t *table) Floats(j int) []float64 {
	execute.CheckColType(t.cols[j], flux.TFloat)
	return t.colBufs[j].([]float64)
}

func (t *table) Strings(j int) []string {
	execute.CheckColType(t.cols[j], flux.TString)
	return t.colBufs[j].([]string)
}

func (t *table) Times(j int) []execute.Time {
	execute.CheckColType(t.cols[j], flux.TTime)
	return t.colBufs[j].([]execute.Time)
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
			if t.colBufs[j] == nil {
				t.colBufs[j] = make([]string, len(t.cols))
			}
			colBuf := t.colBufs[j].([]string)
			if cap(colBuf) < t.l {
				colBuf = make([]string, t.l)
			} else {
				colBuf = colBuf[:t.l]
			}
			vStr := string(v)
			for i := range colBuf {
				colBuf[i] = vStr
			}
			t.colBufs[j] = colBuf
		}
	}
}

// appendBounds fills the colBufs for the time bounds
func (t *table) appendBounds() {
	bounds := []execute.Time{t.bounds.Start, t.bounds.Stop}
	for j := range []int{startColIdx, stopColIdx} {
		if t.colBufs[j] == nil {
			t.colBufs[j] = make([]execute.Time, len(t.cols))
		}
		colBuf := t.colBufs[j].([]execute.Time)
		if cap(colBuf) < t.l {
			colBuf = make([]execute.Time, t.l)
		} else {
			colBuf = colBuf[:t.l]
		}
		for i := range colBuf {
			colBuf[i] = bounds[j]
		}
		t.colBufs[j] = colBuf
	}
}

func hasPoints(cur tsdb.Cursor) bool {
	if cur == nil {
		return false
	}

	res := false
	switch cur := cur.(type) {
	case tsdb.IntegerArrayCursor:
		a := cur.Next()
		res = a.Len() > 0
	case tsdb.FloatArrayCursor:
		a := cur.Next()
		res = a.Len() > 0
	case tsdb.UnsignedArrayCursor:
		a := cur.Next()
		res = a.Len() > 0
	case tsdb.BooleanArrayCursor:
		a := cur.Next()
		res = a.Len() > 0
	case tsdb.StringArrayCursor:
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
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
) *tableNoPoints {
	t := &tableNoPoints{
		table: newTable(bounds, key, cols, defs),
	}
	t.readTags(tags)

	return t
}

func (t *tableNoPoints) Close() {
	if t.done != nil {
		close(t.done)
		t.done = nil
	}
}

func (t *tableNoPoints) Do(f func(flux.ColReader) error) error {
	defer t.Close()

	f(t)

	return t.err
}

type groupTableNoPoints struct {
	table
	gc GroupCursor
}

func newGroupTableNoPoints(
	gc GroupCursor,
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
) *groupTableNoPoints {
	t := &groupTableNoPoints{
		table: newTable(bounds, key, cols, defs),
		gc:    gc,
	}
	t.readTags(tags)

	return t
}

func (t *groupTableNoPoints) Close() {
	if t.gc != nil {
		t.gc.Close()
		t.gc = nil
	}
	if t.done != nil {
		close(t.done)
		t.done = nil
	}
}

func (t *groupTableNoPoints) Do(f func(flux.ColReader) error) error {
	defer t.Close()

	for t.advanceCursor() {
		if err := f(t); err != nil {
			return err
		}
	}

	return t.err
}

func (t *groupTableNoPoints) advanceCursor() bool {
	for t.gc.Next() {
		if hasPoints(t.gc.Cursor()) {
			t.readTags(t.gc.Tags())
			return true
		}
	}
	return false
}

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion2 // please upgrade the proto package

type ReadRequest_Group int32

const (
	// GroupNone returns all series as a single group.
	// The single GroupFrame.TagKeys will be the union of all tag keys.
	GroupNone ReadRequest_Group = 0
	// GroupAll returns a unique group for each series.
	// As an optimization, no GroupFrames will be generated.
	GroupAll ReadRequest_Group = 1
	// GroupBy returns a group for each unique value of the specified GroupKeys.
	GroupBy ReadRequest_Group = 2
	// GroupExcept in not implemented.
	GroupExcept ReadRequest_Group = 3
)

var ReadRequest_Group_name = map[int32]string{
	0: "GROUP_NONE",
	1: "GROUP_ALL",
	2: "GROUP_BY",
	3: "GROUP_EXCEPT",
}
var ReadRequest_Group_value = map[string]int32{
	"GROUP_NONE":   0,
	"GROUP_ALL":    1,
	"GROUP_BY":     2,
	"GROUP_EXCEPT": 3,
}

func (x ReadRequest_Group) String() string {
	return proto.EnumName(ReadRequest_Group_name, int32(x))
}
func (ReadRequest_Group) EnumDescriptor() ([]byte, []int) {
	return fileDescriptorStorageCommon, []int{0, 0}
}

type ReadRequest_HintFlags int32

const (
	HintNone     ReadRequest_HintFlags = 0
	HintNoPoints ReadRequest_HintFlags = 1
	HintNoSeries ReadRequest_HintFlags = 2
	// HintSchemaAllTime performs schema queries without using time ranges
	HintSchemaAllTime ReadRequest_HintFlags = 4
)

var ReadRequest_HintFlags_name = map[int32]string{
	0: "HINT_NONE",
	1: "HINT_NO_POINTS",
	2: "HINT_NO_SERIES",
	4: "HINT_SCHEMA_ALL_TIME",
}
var ReadRequest_HintFlags_value = map[string]int32{
	"HINT_NONE":            0,
	"HINT_NO_POINTS":       1,
	"HINT_NO_SERIES":       2,
	"HINT_SCHEMA_ALL_TIME": 4,
}

func (x ReadRequest_HintFlags) String() string {
	return proto.EnumName(ReadRequest_HintFlags_name, int32(x))
}
func (ReadRequest_HintFlags) EnumDescriptor() ([]byte, []int) {
	return fileDescriptorStorageCommon, []int{0, 1}
}

type Aggregate_AggregateType int32

const (
	AggregateTypeNone  Aggregate_AggregateType = 0
	AggregateTypeSum   Aggregate_AggregateType = 1
	AggregateTypeCount Aggregate_AggregateType = 2
)

var Aggregate_AggregateType_name = map[int32]string{
	0: "NONE",
	1: "SUM",
	2: "COUNT",
}
var Aggregate_AggregateType_value = map[string]int32{
	"NONE":  0,
	"SUM":   1,
	"COUNT": 2,
}

func (x Aggregate_AggregateType) String() string {
	return proto.EnumName(Aggregate_AggregateType_name, int32(x))
}
func (Aggregate_AggregateType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptorStorageCommon, []int{1, 0}
}

type ReadResponse_FrameType int32

const (
	FrameTypeSeries ReadResponse_FrameType = 0
	FrameTypePoints ReadResponse_FrameType = 1
)

var ReadResponse_FrameType_name = map[int32]string{
	0: "SERIES",
	1: "POINTS",
}
var ReadResponse_FrameType_value = map[string]int32{
	"SERIES": 0,
	"POINTS": 1,
}

func (x ReadResponse_FrameType) String() string {
	return proto.EnumName(ReadResponse_FrameType_name, int32(x))
}
func (ReadResponse_FrameType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptorStorageCommon, []int{3, 0}
}

type ReadResponse_DataType int32

const (
	DataTypeFloat    ReadResponse_DataType = 0
	DataTypeInteger  ReadResponse_DataType = 1
	DataTypeUnsigned ReadResponse_DataType = 2
	DataTypeBoolean  ReadResponse_DataType = 3
	DataTypeString   ReadResponse_DataType = 4
)

var ReadResponse_DataType_name = map[int32]string{
	0: "FLOAT",
	1: "INTEGER",
	2: "UNSIGNED",
	3: "BOOLEAN",
	4: "STRING",
}
var ReadResponse_DataType_value = map[string]int32{
	"FLOAT":    0,
	"INTEGER":  1,
	"UNSIGNED": 2,
	"BOOLEAN":  3,
	"STRING":   4,
}

func (x ReadResponse_DataType) String() string {
	return proto.EnumName(ReadResponse_DataType_name, int32(x))
}
func (ReadResponse_DataType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptorStorageCommon, []int{3, 1}
}

// Request message for Storage.Read.
type ReadRequest struct {
	ReadSource     *google_protobuf2.Any `protobuf:"bytes,13,opt,name=read_source,json=readSource" json:"read_source,omitempty"`
	TimestampRange TimestampRange        `protobuf:"bytes,2,opt,name=timestamp_range,json=timestampRange" json:"timestamp_range"`
	// Descending indicates whether points should be returned in descending order.
	Descending bool `protobuf:"varint,3,opt,name=descending,proto3" json:"descending,omitempty"`
	// GroupKeys specifies a list of tag keys used to order the data. It is dependent on the Group property to determine
	// its behavior.
	GroupKeys []string `protobuf:"bytes,4,rep,name=group_keys,json=groupKeys" json:"group_keys,omitempty"`
	//
	Group ReadRequest_Group `protobuf:"varint,11,opt,name=group,proto3,enum=com.github.influxdata.influxdb.services.storage.ReadRequest_Group" json:"group,omitempty"`
	// Aggregate specifies an optional aggregate to apply to the data.
	// TODO(sgc): switch to slice for multiple aggregates in a single request
	Aggregate *Aggregate `protobuf:"bytes,9,opt,name=aggregate" json:"aggregate,omitempty"`
	Predicate *Predicate `protobuf:"bytes,5,opt,name=predicate" json:"predicate,omitempty"`
	// SeriesLimit determines the maximum number of series to be returned for the request. Specify 0 for no limit.
	SeriesLimit int64 `protobuf:"varint,6,opt,name=series_limit,json=seriesLimit,proto3" json:"series_limit,omitempty"`
	// SeriesOffset determines how many series to skip before processing the request.
	SeriesOffset int64 `protobuf:"varint,7,opt,name=series_offset,json=seriesOffset,proto3" json:"series_offset,omitempty"`
	// PointsLimit determines the maximum number of values per series to be returned for the request.
	// Specify 0 for no limit. -1 to return series frames only.
	PointsLimit int64 `protobuf:"varint,8,opt,name=points_limit,json=pointsLimit,proto3" json:"points_limit,omitempty"`
	// Trace contains opaque data if a trace is active.
	Trace map[string]string `protobuf:"bytes,10,rep,name=trace" json:"trace,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	// Hints is a bitwise OR of HintFlags to control the behavior
	// of the read request.
	Hints HintFlags `protobuf:"fixed32,12,opt,name=hints,proto3,casttype=HintFlags" json:"hints,omitempty"`
}

func (m *ReadRequest) Reset()                    { *m = ReadRequest{} }
func (m *ReadRequest) String() string            { return proto.CompactTextString(m) }
func (*ReadRequest) ProtoMessage()               {}
func (*ReadRequest) Descriptor() ([]byte, []int) { return fileDescriptorStorageCommon, []int{0} }

type Aggregate struct {
	Type Aggregate_AggregateType `protobuf:"varint,1,opt,name=type,proto3,enum=com.github.influxdata.influxdb.services.storage.Aggregate_AggregateType" json:"type,omitempty"`
}

func (m *Aggregate) Reset()                    { *m = Aggregate{} }
func (m *Aggregate) String() string            { return proto.CompactTextString(m) }
func (*Aggregate) ProtoMessage()               {}
func (*Aggregate) Descriptor() ([]byte, []int) { return fileDescriptorStorageCommon, []int{1} }

type Tag struct {
	Key   []byte `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Value []byte `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
}

func (m *Tag) Reset()                    { *m = Tag{} }
func (m *Tag) String() string            { return proto.CompactTextString(m) }
func (*Tag) ProtoMessage()               {}
func (*Tag) Descriptor() ([]byte, []int) { return fileDescriptorStorageCommon, []int{2} }

// Response message for Storage.Read.
type ReadResponse struct {
	Frames []ReadResponse_Frame `protobuf:"bytes,1,rep,name=frames" json:"frames"`
}

func (m *ReadResponse) Reset()                    { *m = ReadResponse{} }
func (m *ReadResponse) String() string            { return proto.CompactTextString(m) }
func (*ReadResponse) ProtoMessage()               {}
func (*ReadResponse) Descriptor() ([]byte, []int) { return fileDescriptorStorageCommon, []int{3} }

type ReadResponse_Frame struct {
	// Types that are valid to be assigned to Data:
	//	*ReadResponse_Frame_Group
	//	*ReadResponse_Frame_Series
	//	*ReadResponse_Frame_FloatPoints
	//	*ReadResponse_Frame_IntegerPoints
	//	*ReadResponse_Frame_UnsignedPoints
	//	*ReadResponse_Frame_BooleanPoints
	//	*ReadResponse_Frame_StringPoints
	Data isReadResponse_Frame_Data `protobuf_oneof:"data"`
}

func (m *ReadResponse_Frame) Reset()         { *m = ReadResponse_Frame{} }
func (m *ReadResponse_Frame) String() string { return proto.CompactTextString(m) }
func (*ReadResponse_Frame) ProtoMessage()    {}
func (*ReadResponse_Frame) Descriptor() ([]byte, []int) {
	return fileDescriptorStorageCommon, []int{3, 0}
}

type isReadResponse_Frame_Data interface {
	isReadResponse_Frame_Data()
	MarshalTo([]byte) (int, error)
	Size() int
}

type ReadResponse_Frame_Group struct {
	Group *ReadResponse_GroupFrame `protobuf:"bytes,7,opt,name=group,oneof"`
}
type ReadResponse_Frame_Series struct {
	Series *ReadResponse_SeriesFrame `protobuf:"bytes,1,opt,name=series,oneof"`
}
type ReadResponse_Frame_FloatPoints struct {
	FloatPoints *ReadResponse_FloatPointsFrame `protobuf:"bytes,2,opt,name=float_points,json=floatPoints,oneof"`
}
type ReadResponse_Frame_IntegerPoints struct {
	IntegerPoints *ReadResponse_IntegerPointsFrame `protobuf:"bytes,3,opt,name=integer_points,json=integerPoints,oneof"`
}
type ReadResponse_Frame_UnsignedPoints struct {
	UnsignedPoints *ReadResponse_UnsignedPointsFrame `protobuf:"bytes,4,opt,name=unsigned_points,json=unsignedPoints,oneof"`
}
type ReadResponse_Frame_BooleanPoints struct {
	BooleanPoints *ReadResponse_BooleanPointsFrame `protobuf:"bytes,5,opt,name=boolean_points,json=booleanPoints,oneof"`
}
type ReadResponse_Frame_StringPoints struct {
	StringPoints *ReadResponse_StringPointsFrame `protobuf:"bytes,6,opt,name=string_points,json=stringPoints,oneof"`
}

func (*ReadResponse_Frame_Group) isReadResponse_Frame_Data()          {}
func (*ReadResponse_Frame_Series) isReadResponse_Frame_Data()         {}
func (*ReadResponse_Frame_FloatPoints) isReadResponse_Frame_Data()    {}
func (*ReadResponse_Frame_IntegerPoints) isReadResponse_Frame_Data()  {}
func (*ReadResponse_Frame_UnsignedPoints) isReadResponse_Frame_Data() {}
func (*ReadResponse_Frame_BooleanPoints) isReadResponse_Frame_Data()  {}
func (*ReadResponse_Frame_StringPoints) isReadResponse_Frame_Data()   {}

func (m *ReadResponse_Frame) GetData() isReadResponse_Frame_Data {
	if m != nil {
		return m.Data
	}
	return nil
}

func (m *ReadResponse_Frame) GetGroup() *ReadResponse_GroupFrame {
	if x, ok := m.GetData().(*ReadResponse_Frame_Group); ok {
		return x.Group
	}
	return nil
}

func (m *ReadResponse_Frame) GetSeries() *ReadResponse_SeriesFrame {
	if x, ok := m.GetData().(*ReadResponse_Frame_Series); ok {
		return x.Series
	}
	return nil
}

func (m *ReadResponse_Frame) GetFloatPoints() *ReadResponse_FloatPointsFrame {
	if x, ok := m.GetData().(*ReadResponse_Frame_FloatPoints); ok {
		return x.FloatPoints
	}
	return nil
}

func (m *ReadResponse_Frame) GetIntegerPoints() *ReadResponse_IntegerPointsFrame {
	if x, ok := m.GetData().(*ReadResponse_Frame_IntegerPoints); ok {
		return x.IntegerPoints
	}
	return nil
}

func (m *ReadResponse_Frame) GetUnsignedPoints() *ReadResponse_UnsignedPointsFrame {
	if x, ok := m.GetData().(*ReadResponse_Frame_UnsignedPoints); ok {
		return x.UnsignedPoints
	}
	return nil
}

func (m *ReadResponse_Frame) GetBooleanPoints() *ReadResponse_BooleanPointsFrame {
	if x, ok := m.GetData().(*ReadResponse_Frame_BooleanPoints); ok {
		return x.BooleanPoints
	}
	return nil
}

func (m *ReadResponse_Frame) GetStringPoints() *ReadResponse_StringPointsFrame {
	if x, ok := m.GetData().(*ReadResponse_Frame_StringPoints); ok {
		return x.StringPoints
	}
	return nil
}

// XXX_OneofFuncs is for the internal use of the proto package.
func (*ReadResponse_Frame) XXX_OneofFuncs() (func(msg proto.Message, b *proto.Buffer) error, func(msg proto.Message, tag, wire int, b *proto.Buffer) (bool, error), func(msg proto.Message) (n int), []interface{}) {
	return _ReadResponse_Frame_OneofMarshaler, _ReadResponse_Frame_OneofUnmarshaler, _ReadResponse_Frame_OneofSizer, []interface{}{
		(*ReadResponse_Frame_Group)(nil),
		(*ReadResponse_Frame_Series)(nil),
		(*ReadResponse_Frame_FloatPoints)(nil),
		(*ReadResponse_Frame_IntegerPoints)(nil),
		(*ReadResponse_Frame_UnsignedPoints)(nil),
		(*ReadResponse_Frame_BooleanPoints)(nil),
		(*ReadResponse_Frame_StringPoints)(nil),
	}
}

func _ReadResponse_Frame_OneofMarshaler(msg proto.Message, b *proto.Buffer) error {
	m := msg.(*ReadResponse_Frame)
	// data
	switch x := m.Data.(type) {
	case *ReadResponse_Frame_Group:
		_ = b.EncodeVarint(7<<3 | proto.WireBytes)
		if err := b.EncodeMessage(x.Group); err != nil {
			return err
		}
	case *ReadResponse_Frame_Series:
		_ = b.EncodeVarint(1<<3 | proto.WireBytes)
		if err := b.EncodeMessage(x.Series); err != nil {
			return err
		}
	case *ReadResponse_Frame_FloatPoints:
		_ = b.EncodeVarint(2<<3 | proto.WireBytes)
		if err := b.EncodeMessage(x.FloatPoints); err != nil {
			return err
		}
	case *ReadResponse_Frame_IntegerPoints:
		_ = b.EncodeVarint(3<<3 | proto.WireBytes)
		if err := b.EncodeMessage(x.IntegerPoints); err != nil {
			return err
		}
	case *ReadResponse_Frame_UnsignedPoints:
		_ = b.EncodeVarint(4<<3 | proto.WireBytes)
		if err := b.EncodeMessage(x.UnsignedPoints); err != nil {
			return err
		}
	case *ReadResponse_Frame_BooleanPoints:
		_ = b.EncodeVarint(5<<3 | proto.WireBytes)
		if err := b.EncodeMessage(x.BooleanPoints); err != nil {
			return err
		}
	case *ReadResponse_Frame_StringPoints:
		_ = b.EncodeVarint(6<<3 | proto.WireBytes)
		if err := b.EncodeMessage(x.StringPoints); err != nil {
			return err
		}
	case nil:
	default:
		return fmt.Errorf("ReadResponse_Frame.Data has unexpected type %T", x)
	}
	return nil
}

func _ReadResponse_Frame_OneofUnmarshaler(msg proto.Message, tag, wire int, b *proto.Buffer) (bool, error) {
	m := msg.(*ReadResponse_Frame)
	switch tag {
	case 7: // data.group
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		msg := new(ReadResponse_GroupFrame)
		err := b.DecodeMessage(msg)
		m.Data = &ReadResponse_Frame_Group{msg}
		return true, err
	case 1: // data.series
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		msg := new(ReadResponse_SeriesFrame)
		err := b.DecodeMessage(msg)
		m.Data = &ReadResponse_Frame_Series{msg}
		return true, err
	case 2: // data.float_points
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		msg := new(ReadResponse_FloatPointsFrame)
		err := b.DecodeMessage(msg)
		m.Data = &ReadResponse_Frame_FloatPoints{msg}
		return true, err
	case 3: // data.integer_points
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		msg := new(ReadResponse_IntegerPointsFrame)
		err := b.DecodeMessage(msg)
		m.Data = &ReadResponse_Frame_IntegerPoints{msg}
		return true, err
	case 4: // data.unsigned_points
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		msg := new(ReadResponse_UnsignedPointsFrame)
		err := b.DecodeMessage(msg)
		m.Data = &ReadResponse_Frame_UnsignedPoints{msg}
		return true, err
	case 5: // data.boolean_points
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		msg := new(ReadResponse_BooleanPointsFrame)
		err := b.DecodeMessage(msg)
		m.Data = &ReadResponse_Frame_BooleanPoints{msg}
		return true, err
	case 6: // data.string_points
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		msg := new(ReadResponse_StringPointsFrame)
		err := b.DecodeMessage(msg)
		m.Data = &ReadResponse_Frame_StringPoints{msg}
		return true, err
	default:
		return false, nil
	}
}

func _ReadResponse_Frame_OneofSizer(msg proto.Message) (n int) {
	m := msg.(*ReadResponse_Frame)
	// data
	switch x := m.Data.(type) {
	case *ReadResponse_Frame_Group:
		s := proto.Size(x.Group)
		n += proto.SizeVarint(7<<3 | proto.WireBytes)
		n += proto.SizeVarint(uint64(s))
		n += s
	case *ReadResponse_Frame_Series:
		s := proto.Size(x.Series)
		n += proto.SizeVarint(1<<3 | proto.WireBytes)
		n += proto.SizeVarint(uint64(s))
		n += s
	case *ReadResponse_Frame_FloatPoints:
		s := proto.Size(x.FloatPoints)
		n += proto.SizeVarint(2<<3 | proto.WireBytes)
		n += proto.SizeVarint(uint64(s))
		n += s
	case *ReadResponse_Frame_IntegerPoints:
		s := proto.Size(x.IntegerPoints)
		n += proto.SizeVarint(3<<3 | proto.WireBytes)
		n += proto.SizeVarint(uint64(s))
		n += s
	case *ReadResponse_Frame_UnsignedPoints:
		s := proto.Size(x.UnsignedPoints)
		n += proto.SizeVarint(4<<3 | proto.WireBytes)
		n += proto.SizeVarint(uint64(s))
		n += s
	case *ReadResponse_Frame_BooleanPoints:
		s := proto.Size(x.BooleanPoints)
		n += proto.SizeVarint(5<<3 | proto.WireBytes)
		n += proto.SizeVarint(uint64(s))
		n += s
	case *ReadResponse_Frame_StringPoints:
		s := proto.Size(x.StringPoints)
		n += proto.SizeVarint(6<<3 | proto.WireBytes)
		n += proto.SizeVarint(uint64(s))
		n += s
	case nil:
	default:
		panic(fmt.Sprintf("proto: unexpected type %T in oneof", x))
	}
	return n
}

type ReadResponse_GroupFrame struct {
	// TagKeys
	TagKeys [][]byte `protobuf:"bytes,1,rep,name=tag_keys,json=tagKeys" json:"tag_keys,omitempty"`
	// PartitionKeyVals is the values of the partition key for this group, order matching ReadRequest.GroupKeys
	PartitionKeyVals [][]byte `protobuf:"bytes,2,rep,name=partition_key_vals,json=partitionKeyVals" json:"partition_key_vals,omitempty"`
}

func (m *ReadResponse_GroupFrame) Reset()         { *m = ReadResponse_GroupFrame{} }
func (m *ReadResponse_GroupFrame) String() string { return proto.CompactTextString(m) }
func (*ReadResponse_GroupFrame) ProtoMessage()    {}
func (*ReadResponse_GroupFrame) Descriptor() ([]byte, []int) {
	return fileDescriptorStorageCommon, []int{3, 1}
}

type ReadResponse_SeriesFrame struct {
	Tags     []Tag                 `protobuf:"bytes,1,rep,name=tags" json:"tags"`
	DataType ReadResponse_DataType `protobuf:"varint,2,opt,name=data_type,json=dataType,proto3,enum=com.github.influxdata.influxdb.services.storage.ReadResponse_DataType" json:"data_type,omitempty"`
}

func (m *ReadResponse_SeriesFrame) Reset()         { *m = ReadResponse_SeriesFrame{} }
func (m *ReadResponse_SeriesFrame) String() string { return proto.CompactTextString(m) }
func (*ReadResponse_SeriesFrame) ProtoMessage()    {}
func (*ReadResponse_SeriesFrame) Descriptor() ([]byte, []int) {
	return fileDescriptorStorageCommon, []int{3, 2}
}

type ReadResponse_FloatPointsFrame struct {
	Timestamps []int64   `protobuf:"fixed64,1,rep,packed,name=timestamps" json:"timestamps,omitempty"`
	Values     []float64 `protobuf:"fixed64,2,rep,packed,name=values" json:"values,omitempty"`
}

func (m *ReadResponse_FloatPointsFrame) Reset()         { *m = ReadResponse_FloatPointsFrame{} }
func (m *ReadResponse_FloatPointsFrame) String() string { return proto.CompactTextString(m) }
func (*ReadResponse_FloatPointsFrame) ProtoMessage()    {}
func (*ReadResponse_FloatPointsFrame) Descriptor() ([]byte, []int) {
	return fileDescriptorStorageCommon, []int{3, 3}
}

type ReadResponse_IntegerPointsFrame struct {
	Timestamps []int64 `protobuf:"fixed64,1,rep,packed,name=timestamps" json:"timestamps,omitempty"`
	Values     []int64 `protobuf:"varint,2,rep,packed,name=values" json:"values,omitempty"`
}

func (m *ReadResponse_IntegerPointsFrame) Reset()         { *m = ReadResponse_IntegerPointsFrame{} }
func (m *ReadResponse_IntegerPointsFrame) String() string { return proto.CompactTextString(m) }
func (*ReadResponse_IntegerPointsFrame) ProtoMessage()    {}
func (*ReadResponse_IntegerPointsFrame) Descriptor() ([]byte, []int) {
	return fileDescriptorStorageCommon, []int{3, 4}
}

type ReadResponse_UnsignedPointsFrame struct {
	Timestamps []int64  `protobuf:"fixed64,1,rep,packed,name=timestamps" json:"timestamps,omitempty"`
	Values     []uint64 `protobuf:"varint,2,rep,packed,name=values" json:"values,omitempty"`
}

func (m *ReadResponse_UnsignedPointsFrame) Reset()         { *m = ReadResponse_UnsignedPointsFrame{} }
func (m *ReadResponse_UnsignedPointsFrame) String() string { return proto.CompactTextString(m) }
func (*ReadResponse_UnsignedPointsFrame) ProtoMessage()    {}
func (*ReadResponse_UnsignedPointsFrame) Descriptor() ([]byte, []int) {
	return fileDescriptorStorageCommon, []int{3, 5}
}

type ReadResponse_BooleanPointsFrame struct {
	Timestamps []int64 `protobuf:"fixed64,1,rep,packed,name=timestamps" json:"timestamps,omitempty"`
	Values     []bool  `protobuf:"varint,2,rep,packed,name=values" json:"values,omitempty"`
}

func (m *ReadResponse_BooleanPointsFrame) Reset()         { *m = ReadResponse_BooleanPointsFrame{} }
func (m *ReadResponse_BooleanPointsFrame) String() string { return proto.CompactTextString(m) }
func (*ReadResponse_BooleanPointsFrame) ProtoMessage()    {}
func (*ReadResponse_BooleanPointsFrame) Descriptor() ([]byte, []int) {
	return fileDescriptorStorageCommon, []int{3, 6}
}

type ReadResponse_StringPointsFrame struct {
	Timestamps []int64  `protobuf:"fixed64,1,rep,packed,name=timestamps" json:"timestamps,omitempty"`
	Values     []string `protobuf:"bytes,2,rep,name=values" json:"values,omitempty"`
}

func (m *ReadResponse_StringPointsFrame) Reset()         { *m = ReadResponse_StringPointsFrame{} }
func (m *ReadResponse_StringPointsFrame) String() string { return proto.CompactTextString(m) }
func (*ReadResponse_StringPointsFrame) ProtoMessage()    {}
func (*ReadResponse_StringPointsFrame) Descriptor() ([]byte, []int) {
	return fileDescriptorStorageCommon, []int{3, 7}
}

type CapabilitiesResponse struct {
	Caps map[string]string `protobuf:"bytes,1,rep,name=caps" json:"caps,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (m *CapabilitiesResponse) Reset()         { *m = CapabilitiesResponse{} }
func (m *CapabilitiesResponse) String() string { return proto.CompactTextString(m) }
func (*CapabilitiesResponse) ProtoMessage()    {}
func (*CapabilitiesResponse) Descriptor() ([]byte, []int) {
	return fileDescriptorStorageCommon, []int{4}
}

type HintsResponse struct {
}

func (m *HintsResponse) Reset()                    { *m = HintsResponse{} }
func (m *HintsResponse) String() string            { return proto.CompactTextString(m) }
func (*HintsResponse) ProtoMessage()               {}
func (*HintsResponse) Descriptor() ([]byte, []int) { return fileDescriptorStorageCommon, []int{5} }

// Specifies a continuous range of nanosecond timestamps.
type TimestampRange struct {
	// Start defines the inclusive lower bound.
	Start int64 `protobuf:"varint,1,opt,name=start,proto3" json:"start,omitempty"`
	// End defines the inclusive upper bound.
	End int64 `protobuf:"varint,2,opt,name=end,proto3" json:"end,omitempty"`
}

func (m *TimestampRange) Reset()                    { *m = TimestampRange{} }
func (m *TimestampRange) String() string            { return proto.CompactTextString(m) }
func (*TimestampRange) ProtoMessage()               {}
func (*TimestampRange) Descriptor() ([]byte, []int) { return fileDescriptorStorageCommon, []int{6} }

func init() {
	proto.RegisterType((*ReadRequest)(nil), "com.github.influxdata.influxdb.services.storage.ReadRequest")
	proto.RegisterType((*Aggregate)(nil), "com.github.influxdata.influxdb.services.storage.Aggregate")
	proto.RegisterType((*Tag)(nil), "com.github.influxdata.influxdb.services.storage.Tag")
	proto.RegisterType((*ReadResponse)(nil), "com.github.influxdata.influxdb.services.storage.ReadResponse")
	proto.RegisterType((*ReadResponse_Frame)(nil), "com.github.influxdata.influxdb.services.storage.ReadResponse.Frame")
	proto.RegisterType((*ReadResponse_GroupFrame)(nil), "com.github.influxdata.influxdb.services.storage.ReadResponse.GroupFrame")
	proto.RegisterType((*ReadResponse_SeriesFrame)(nil), "com.github.influxdata.influxdb.services.storage.ReadResponse.SeriesFrame")
	proto.RegisterType((*ReadResponse_FloatPointsFrame)(nil), "com.github.influxdata.influxdb.services.storage.ReadResponse.FloatPointsFrame")
	proto.RegisterType((*ReadResponse_IntegerPointsFrame)(nil), "com.github.influxdata.influxdb.services.storage.ReadResponse.IntegerPointsFrame")
	proto.RegisterType((*ReadResponse_UnsignedPointsFrame)(nil), "com.github.influxdata.influxdb.services.storage.ReadResponse.UnsignedPointsFrame")
	proto.RegisterType((*ReadResponse_BooleanPointsFrame)(nil), "com.github.influxdata.influxdb.services.storage.ReadResponse.BooleanPointsFrame")
	proto.RegisterType((*ReadResponse_StringPointsFrame)(nil), "com.github.influxdata.influxdb.services.storage.ReadResponse.StringPointsFrame")
	proto.RegisterType((*CapabilitiesResponse)(nil), "com.github.influxdata.influxdb.services.storage.CapabilitiesResponse")
	proto.RegisterType((*HintsResponse)(nil), "com.github.influxdata.influxdb.services.storage.HintsResponse")
	proto.RegisterType((*TimestampRange)(nil), "com.github.influxdata.influxdb.services.storage.TimestampRange")
	proto.RegisterEnum("com.github.influxdata.influxdb.services.storage.ReadRequest_Group", ReadRequest_Group_name, ReadRequest_Group_value)
	proto.RegisterEnum("com.github.influxdata.influxdb.services.storage.ReadRequest_HintFlags", ReadRequest_HintFlags_name, ReadRequest_HintFlags_value)
	proto.RegisterEnum("com.github.influxdata.influxdb.services.storage.Aggregate_AggregateType", Aggregate_AggregateType_name, Aggregate_AggregateType_value)
	proto.RegisterEnum("com.github.influxdata.influxdb.services.storage.ReadResponse_FrameType", ReadResponse_FrameType_name, ReadResponse_FrameType_value)
	proto.RegisterEnum("com.github.influxdata.influxdb.services.storage.ReadResponse_DataType", ReadResponse_DataType_name, ReadResponse_DataType_value)
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// Client API for Storage service

type StorageClient interface {
	// Read performs a read operation using the given ReadRequest
	Read(ctx context.Context, in *ReadRequest, opts ...grpc.CallOption) (Storage_ReadClient, error)
	// Capabilities returns a map of keys and values identifying the capabilities supported by the storage engine
	Capabilities(ctx context.Context, in *google_protobuf1.Empty, opts ...grpc.CallOption) (*CapabilitiesResponse, error)
	Hints(ctx context.Context, in *google_protobuf1.Empty, opts ...grpc.CallOption) (*HintsResponse, error)
}

type storageClient struct {
	cc *grpc.ClientConn
}

func NewStorageClient(cc *grpc.ClientConn) StorageClient {
	return &storageClient{cc}
}

func (c *storageClient) Read(ctx context.Context, in *ReadRequest, opts ...grpc.CallOption) (Storage_ReadClient, error) {
	stream, err := grpc.NewClientStream(ctx, &_Storage_serviceDesc.Streams[0], c.cc, "/com.github.influxdata.influxdb.services.storage.Storage/Read", opts...)
	if err != nil {
		return nil, err
	}
	x := &storageReadClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type Storage_ReadClient interface {
	Recv() (*ReadResponse, error)
	grpc.ClientStream
}

type storageReadClient struct {
	grpc.ClientStream
}

func (x *storageReadClient) Recv() (*ReadResponse, error) {
	m := new(ReadResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *storageClient) Capabilities(ctx context.Context, in *google_protobuf1.Empty, opts ...grpc.CallOption) (*CapabilitiesResponse, error) {
	out := new(CapabilitiesResponse)
	err := grpc.Invoke(ctx, "/com.github.influxdata.influxdb.services.storage.Storage/Capabilities", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *storageClient) Hints(ctx context.Context, in *google_protobuf1.Empty, opts ...grpc.CallOption) (*HintsResponse, error) {
	out := new(HintsResponse)
	err := grpc.Invoke(ctx, "/com.github.influxdata.influxdb.services.storage.Storage/Hints", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Server API for Storage service

type StorageServer interface {
	// Read performs a read operation using the given ReadRequest
	Read(*ReadRequest, Storage_ReadServer) error
	// Capabilities returns a map of keys and values identifying the capabilities supported by the storage engine
	Capabilities(context.Context, *google_protobuf1.Empty) (*CapabilitiesResponse, error)
	Hints(context.Context, *google_protobuf1.Empty) (*HintsResponse, error)
}

func RegisterStorageServer(s *grpc.Server, srv StorageServer) {
	s.RegisterService(&_Storage_serviceDesc, srv)
}

func _Storage_Read_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(ReadRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(StorageServer).Read(m, &storageReadServer{stream})
}

type Storage_ReadServer interface {
	Send(*ReadResponse) error
	grpc.ServerStream
}

type storageReadServer struct {
	grpc.ServerStream
}

func (x *storageReadServer) Send(m *ReadResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _Storage_Capabilities_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(google_protobuf1.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StorageServer).Capabilities(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/com.github.influxdata.influxdb.services.storage.Storage/Capabilities",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StorageServer).Capabilities(ctx, req.(*google_protobuf1.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _Storage_Hints_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(google_protobuf1.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StorageServer).Hints(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/com.github.influxdata.influxdb.services.storage.Storage/Hints",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StorageServer).Hints(ctx, req.(*google_protobuf1.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

var _Storage_serviceDesc = grpc.ServiceDesc{
	ServiceName: "com.github.influxdata.influxdb.services.storage.Storage",
	HandlerType: (*StorageServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Capabilities",
			Handler:    _Storage_Capabilities_Handler,
		},
		{
			MethodName: "Hints",
			Handler:    _Storage_Hints_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Read",
			Handler:       _Storage_Read_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "storage_common.proto",
}

func (m *ReadRequest) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *ReadRequest) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	dAtA[i] = 0x12
	i++
	i = encodeVarintStorageCommon(dAtA, i, uint64(m.TimestampRange.Size()))
	n1, err := m.TimestampRange.MarshalTo(dAtA[i:])
	if err != nil {
		return 0, err
	}
	i += n1
	if m.Descending {
		dAtA[i] = 0x18
		i++
		if m.Descending {
			dAtA[i] = 1
		} else {
			dAtA[i] = 0
		}
		i++
	}
	if len(m.GroupKeys) > 0 {
		for _, s := range m.GroupKeys {
			dAtA[i] = 0x22
			i++
			l = len(s)
			for l >= 1<<7 {
				dAtA[i] = uint8(uint64(l)&0x7f | 0x80)
				l >>= 7
				i++
			}
			dAtA[i] = uint8(l)
			i++
			i += copy(dAtA[i:], s)
		}
	}
	if m.Predicate != nil {
		dAtA[i] = 0x2a
		i++
		i = encodeVarintStorageCommon(dAtA, i, uint64(m.Predicate.Size()))
		n2, err := m.Predicate.MarshalTo(dAtA[i:])
		if err != nil {
			return 0, err
		}
		i += n2
	}
	if m.SeriesLimit != 0 {
		dAtA[i] = 0x30
		i++
		i = encodeVarintStorageCommon(dAtA, i, uint64(m.SeriesLimit))
	}
	if m.SeriesOffset != 0 {
		dAtA[i] = 0x38
		i++
		i = encodeVarintStorageCommon(dAtA, i, uint64(m.SeriesOffset))
	}
	if m.PointsLimit != 0 {
		dAtA[i] = 0x40
		i++
		i = encodeVarintStorageCommon(dAtA, i, uint64(m.PointsLimit))
	}
	if m.Aggregate != nil {
		dAtA[i] = 0x4a
		i++
		i = encodeVarintStorageCommon(dAtA, i, uint64(m.Aggregate.Size()))
		n3, err := m.Aggregate.MarshalTo(dAtA[i:])
		if err != nil {
			return 0, err
		}
		i += n3
	}
	if len(m.Trace) > 0 {
		for k, _ := range m.Trace {
			dAtA[i] = 0x52
			i++
			v := m.Trace[k]
			mapSize := 1 + len(k) + sovStorageCommon(uint64(len(k))) + 1 + len(v) + sovStorageCommon(uint64(len(v)))
			i = encodeVarintStorageCommon(dAtA, i, uint64(mapSize))
			dAtA[i] = 0xa
			i++
			i = encodeVarintStorageCommon(dAtA, i, uint64(len(k)))
			i += copy(dAtA[i:], k)
			dAtA[i] = 0x12
			i++
			i = encodeVarintStorageCommon(dAtA, i, uint64(len(v)))
			i += copy(dAtA[i:], v)
		}
	}
	if m.Group != 0 {
		dAtA[i] = 0x58
		i++
		i = encodeVarintStorageCommon(dAtA, i, uint64(m.Group))
	}
	if m.Hints != 0 {
		dAtA[i] = 0x65
		i++
		i = encodeFixed32StorageCommon(dAtA, i, uint32(m.Hints))
	}
	if m.ReadSource != nil {
		dAtA[i] = 0x6a
		i++
		i = encodeVarintStorageCommon(dAtA, i, uint64(m.ReadSource.Size()))
		n4, err := m.ReadSource.MarshalTo(dAtA[i:])
		if err != nil {
			return 0, err
		}
		i += n4
	}
	return i, nil
}

func (m *Aggregate) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Aggregate) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.Type != 0 {
		dAtA[i] = 0x8
		i++
		i = encodeVarintStorageCommon(dAtA, i, uint64(m.Type))
	}
	return i, nil
}

func (m *Tag) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Tag) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.Key) > 0 {
		dAtA[i] = 0xa
		i++
		i = encodeVarintStorageCommon(dAtA, i, uint64(len(m.Key)))
		i += copy(dAtA[i:], m.Key)
	}
	if len(m.Value) > 0 {
		dAtA[i] = 0x12
		i++
		i = encodeVarintStorageCommon(dAtA, i, uint64(len(m.Value)))
		i += copy(dAtA[i:], m.Value)
	}
	return i, nil
}

func (m *ReadResponse) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *ReadResponse) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.Frames) > 0 {
		for _, msg := range m.Frames {
			dAtA[i] = 0xa
			i++
			i = encodeVarintStorageCommon(dAtA, i, uint64(msg.Size()))
			n, err := msg.MarshalTo(dAtA[i:])
			if err != nil {
				return 0, err
			}
			i += n
		}
	}
	return i, nil
}

func (m *ReadResponse_Frame) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *ReadResponse_Frame) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.Data != nil {
		nn5, err := m.Data.MarshalTo(dAtA[i:])
		if err != nil {
			return 0, err
		}
		i += nn5
	}
	return i, nil
}

func (m *ReadResponse_Frame_Series) MarshalTo(dAtA []byte) (int, error) {
	i := 0
	if m.Series != nil {
		dAtA[i] = 0xa
		i++
		i = encodeVarintStorageCommon(dAtA, i, uint64(m.Series.Size()))
		n6, err := m.Series.MarshalTo(dAtA[i:])
		if err != nil {
			return 0, err
		}
		i += n6
	}
	return i, nil
}
func (m *ReadResponse_Frame_FloatPoints) MarshalTo(dAtA []byte) (int, error) {
	i := 0
	if m.FloatPoints != nil {
		dAtA[i] = 0x12
		i++
		i = encodeVarintStorageCommon(dAtA, i, uint64(m.FloatPoints.Size()))
		n7, err := m.FloatPoints.MarshalTo(dAtA[i:])
		if err != nil {
			return 0, err
		}
		i += n7
	}
	return i, nil
}
func (m *ReadResponse_Frame_IntegerPoints) MarshalTo(dAtA []byte) (int, error) {
	i := 0
	if m.IntegerPoints != nil {
		dAtA[i] = 0x1a
		i++
		i = encodeVarintStorageCommon(dAtA, i, uint64(m.IntegerPoints.Size()))
		n8, err := m.IntegerPoints.MarshalTo(dAtA[i:])
		if err != nil {
			return 0, err
		}
		i += n8
	}
	return i, nil
}
func (m *ReadResponse_Frame_UnsignedPoints) MarshalTo(dAtA []byte) (int, error) {
	i := 0
	if m.UnsignedPoints != nil {
		dAtA[i] = 0x22
		i++
		i = encodeVarintStorageCommon(dAtA, i, uint64(m.UnsignedPoints.Size()))
		n9, err := m.UnsignedPoints.MarshalTo(dAtA[i:])
		if err != nil {
			return 0, err
		}
		i += n9
	}
	return i, nil
}
func (m *ReadResponse_Frame_BooleanPoints) MarshalTo(dAtA []byte) (int, error) {
	i := 0
	if m.BooleanPoints != nil {
		dAtA[i] = 0x2a
		i++
		i = encodeVarintStorageCommon(dAtA, i, uint64(m.BooleanPoints.Size()))
		n10, err := m.BooleanPoints.MarshalTo(dAtA[i:])
		if err != nil {
			return 0, err
		}
		i += n10
	}
	return i, nil
}
func (m *ReadResponse_Frame_StringPoints) MarshalTo(dAtA []byte) (int, error) {
	i := 0
	if m.StringPoints != nil {
		dAtA[i] = 0x32
		i++
		i = encodeVarintStorageCommon(dAtA, i, uint64(m.StringPoints.Size()))
		n11, err := m.StringPoints.MarshalTo(dAtA[i:])
		if err != nil {
			return 0, err
		}
		i += n11
	}
	return i, nil
}
func (m *ReadResponse_Frame_Group) MarshalTo(dAtA []byte) (int, error) {
	i := 0
	if m.Group != nil {
		dAtA[i] = 0x3a
		i++
		i = encodeVarintStorageCommon(dAtA, i, uint64(m.Group.Size()))
		n12, err := m.Group.MarshalTo(dAtA[i:])
		if err != nil {
			return 0, err
		}
		i += n12
	}
	return i, nil
}
func (m *ReadResponse_GroupFrame) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *ReadResponse_GroupFrame) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.TagKeys) > 0 {
		for _, b := range m.TagKeys {
			dAtA[i] = 0xa
			i++
			i = encodeVarintStorageCommon(dAtA, i, uint64(len(b)))
			i += copy(dAtA[i:], b)
		}
	}
	if len(m.PartitionKeyVals) > 0 {
		for _, b := range m.PartitionKeyVals {
			dAtA[i] = 0x12
			i++
			i = encodeVarintStorageCommon(dAtA, i, uint64(len(b)))
			i += copy(dAtA[i:], b)
		}
	}
	return i, nil
}

func (m *ReadResponse_SeriesFrame) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *ReadResponse_SeriesFrame) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.Tags) > 0 {
		for _, msg := range m.Tags {
			dAtA[i] = 0xa
			i++
			i = encodeVarintStorageCommon(dAtA, i, uint64(msg.Size()))
			n, err := msg.MarshalTo(dAtA[i:])
			if err != nil {
				return 0, err
			}
			i += n
		}
	}
	if m.DataType != 0 {
		dAtA[i] = 0x10
		i++
		i = encodeVarintStorageCommon(dAtA, i, uint64(m.DataType))
	}
	return i, nil
}

func (m *ReadResponse_FloatPointsFrame) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *ReadResponse_FloatPointsFrame) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.Timestamps) > 0 {
		dAtA[i] = 0xa
		i++
		i = encodeVarintStorageCommon(dAtA, i, uint64(len(m.Timestamps)*8))
		for _, num := range m.Timestamps {
			dAtA[i] = uint8(num)
			i++
			dAtA[i] = uint8(num >> 8)
			i++
			dAtA[i] = uint8(num >> 16)
			i++
			dAtA[i] = uint8(num >> 24)
			i++
			dAtA[i] = uint8(num >> 32)
			i++
			dAtA[i] = uint8(num >> 40)
			i++
			dAtA[i] = uint8(num >> 48)
			i++
			dAtA[i] = uint8(num >> 56)
			i++
		}
	}
	if len(m.Values) > 0 {
		dAtA[i] = 0x12
		i++
		i = encodeVarintStorageCommon(dAtA, i, uint64(len(m.Values)*8))
		for _, num := range m.Values {
			f13 := math.Float64bits(float64(num))
			dAtA[i] = uint8(f13)
			i++
			dAtA[i] = uint8(f13 >> 8)
			i++
			dAtA[i] = uint8(f13 >> 16)
			i++
			dAtA[i] = uint8(f13 >> 24)
			i++
			dAtA[i] = uint8(f13 >> 32)
			i++
			dAtA[i] = uint8(f13 >> 40)
			i++
			dAtA[i] = uint8(f13 >> 48)
			i++
			dAtA[i] = uint8(f13 >> 56)
			i++
		}
	}
	return i, nil
}

func (m *ReadResponse_IntegerPointsFrame) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *ReadResponse_IntegerPointsFrame) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.Timestamps) > 0 {
		dAtA[i] = 0xa
		i++
		i = encodeVarintStorageCommon(dAtA, i, uint64(len(m.Timestamps)*8))
		for _, num := range m.Timestamps {
			dAtA[i] = uint8(num)
			i++
			dAtA[i] = uint8(num >> 8)
			i++
			dAtA[i] = uint8(num >> 16)
			i++
			dAtA[i] = uint8(num >> 24)
			i++
			dAtA[i] = uint8(num >> 32)
			i++
			dAtA[i] = uint8(num >> 40)
			i++
			dAtA[i] = uint8(num >> 48)
			i++
			dAtA[i] = uint8(num >> 56)
			i++
		}
	}
	if len(m.Values) > 0 {
		dAtA15 := make([]byte, len(m.Values)*10)
		var j14 int
		for _, num1 := range m.Values {
			num := uint64(num1)
			for num >= 1<<7 {
				dAtA15[j14] = uint8(uint64(num)&0x7f | 0x80)
				num >>= 7
				j14++
			}
			dAtA15[j14] = uint8(num)
			j14++
		}
		dAtA[i] = 0x12
		i++
		i = encodeVarintStorageCommon(dAtA, i, uint64(j14))
		i += copy(dAtA[i:], dAtA15[:j14])
	}
	return i, nil
}

func (m *ReadResponse_UnsignedPointsFrame) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *ReadResponse_UnsignedPointsFrame) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.Timestamps) > 0 {
		dAtA[i] = 0xa
		i++
		i = encodeVarintStorageCommon(dAtA, i, uint64(len(m.Timestamps)*8))
		for _, num := range m.Timestamps {
			dAtA[i] = uint8(num)
			i++
			dAtA[i] = uint8(num >> 8)
			i++
			dAtA[i] = uint8(num >> 16)
			i++
			dAtA[i] = uint8(num >> 24)
			i++
			dAtA[i] = uint8(num >> 32)
			i++
			dAtA[i] = uint8(num >> 40)
			i++
			dAtA[i] = uint8(num >> 48)
			i++
			dAtA[i] = uint8(num >> 56)
			i++
		}
	}
	if len(m.Values) > 0 {
		dAtA17 := make([]byte, len(m.Values)*10)
		var j16 int
		for _, num := range m.Values {
			for num >= 1<<7 {
				dAtA17[j16] = uint8(uint64(num)&0x7f | 0x80)
				num >>= 7
				j16++
			}
			dAtA17[j16] = uint8(num)
			j16++
		}
		dAtA[i] = 0x12
		i++
		i = encodeVarintStorageCommon(dAtA, i, uint64(j16))
		i += copy(dAtA[i:], dAtA17[:j16])
	}
	return i, nil
}

func (m *ReadResponse_BooleanPointsFrame) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *ReadResponse_BooleanPointsFrame) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.Timestamps) > 0 {
		dAtA[i] = 0xa
		i++
		i = encodeVarintStorageCommon(dAtA, i, uint64(len(m.Timestamps)*8))
		for _, num := range m.Timestamps {
			dAtA[i] = uint8(num)
			i++
			dAtA[i] = uint8(num >> 8)
			i++
			dAtA[i] = uint8(num >> 16)
			i++
			dAtA[i] = uint8(num >> 24)
			i++
			dAtA[i] = uint8(num >> 32)
			i++
			dAtA[i] = uint8(num >> 40)
			i++
			dAtA[i] = uint8(num >> 48)
			i++
			dAtA[i] = uint8(num >> 56)
			i++
		}
	}
	if len(m.Values) > 0 {
		dAtA[i] = 0x12
		i++
		i = encodeVarintStorageCommon(dAtA, i, uint64(len(m.Values)))
		for _, b := range m.Values {
			if b {
				dAtA[i] = 1
			} else {
				dAtA[i] = 0
			}
			i++
		}
	}
	return i, nil
}

func (m *ReadResponse_StringPointsFrame) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *ReadResponse_StringPointsFrame) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.Timestamps) > 0 {
		dAtA[i] = 0xa
		i++
		i = encodeVarintStorageCommon(dAtA, i, uint64(len(m.Timestamps)*8))
		for _, num := range m.Timestamps {
			dAtA[i] = uint8(num)
			i++
			dAtA[i] = uint8(num >> 8)
			i++
			dAtA[i] = uint8(num >> 16)
			i++
			dAtA[i] = uint8(num >> 24)
			i++
			dAtA[i] = uint8(num >> 32)
			i++
			dAtA[i] = uint8(num >> 40)
			i++
			dAtA[i] = uint8(num >> 48)
			i++
			dAtA[i] = uint8(num >> 56)
			i++
		}
	}
	if len(m.Values) > 0 {
		for _, s := range m.Values {
			dAtA[i] = 0x12
			i++
			l = len(s)
			for l >= 1<<7 {
				dAtA[i] = uint8(uint64(l)&0x7f | 0x80)
				l >>= 7
				i++
			}
			dAtA[i] = uint8(l)
			i++
			i += copy(dAtA[i:], s)
		}
	}
	return i, nil
}

func (m *CapabilitiesResponse) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *CapabilitiesResponse) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.Caps) > 0 {
		for k, _ := range m.Caps {
			dAtA[i] = 0xa
			i++
			v := m.Caps[k]
			mapSize := 1 + len(k) + sovStorageCommon(uint64(len(k))) + 1 + len(v) + sovStorageCommon(uint64(len(v)))
			i = encodeVarintStorageCommon(dAtA, i, uint64(mapSize))
			dAtA[i] = 0xa
			i++
			i = encodeVarintStorageCommon(dAtA, i, uint64(len(k)))
			i += copy(dAtA[i:], k)
			dAtA[i] = 0x12
			i++
			i = encodeVarintStorageCommon(dAtA, i, uint64(len(v)))
			i += copy(dAtA[i:], v)
		}
	}
	return i, nil
}

func (m *HintsResponse) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *HintsResponse) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	return i, nil
}

func (m *TimestampRange) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *TimestampRange) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.Start != 0 {
		dAtA[i] = 0x8
		i++
		i = encodeVarintStorageCommon(dAtA, i, uint64(m.Start))
	}
	if m.End != 0 {
		dAtA[i] = 0x10
		i++
		i = encodeVarintStorageCommon(dAtA, i, uint64(m.End))
	}
	return i, nil
}

func encodeFixed64StorageCommon(dAtA []byte, offset int, v uint64) int {
	dAtA[offset] = uint8(v)
	dAtA[offset+1] = uint8(v >> 8)
	dAtA[offset+2] = uint8(v >> 16)
	dAtA[offset+3] = uint8(v >> 24)
	dAtA[offset+4] = uint8(v >> 32)
	dAtA[offset+5] = uint8(v >> 40)
	dAtA[offset+6] = uint8(v >> 48)
	dAtA[offset+7] = uint8(v >> 56)
	return offset + 8
}
func encodeFixed32StorageCommon(dAtA []byte, offset int, v uint32) int {
	dAtA[offset] = uint8(v)
	dAtA[offset+1] = uint8(v >> 8)
	dAtA[offset+2] = uint8(v >> 16)
	dAtA[offset+3] = uint8(v >> 24)
	return offset + 4
}
func encodeVarintStorageCommon(dAtA []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return offset + 1
}
func (m *ReadRequest) Size() (n int) {
	var l int
	_ = l
	l = m.TimestampRange.Size()
	n += 1 + l + sovStorageCommon(uint64(l))
	if m.Descending {
		n += 2
	}
	if len(m.GroupKeys) > 0 {
		for _, s := range m.GroupKeys {
			l = len(s)
			n += 1 + l + sovStorageCommon(uint64(l))
		}
	}
	if m.Predicate != nil {
		l = m.Predicate.Size()
		n += 1 + l + sovStorageCommon(uint64(l))
	}
	if m.SeriesLimit != 0 {
		n += 1 + sovStorageCommon(uint64(m.SeriesLimit))
	}
	if m.SeriesOffset != 0 {
		n += 1 + sovStorageCommon(uint64(m.SeriesOffset))
	}
	if m.PointsLimit != 0 {
		n += 1 + sovStorageCommon(uint64(m.PointsLimit))
	}
	if m.Aggregate != nil {
		l = m.Aggregate.Size()
		n += 1 + l + sovStorageCommon(uint64(l))
	}
	if len(m.Trace) > 0 {
		for k, v := range m.Trace {
			_ = k
			_ = v
			mapEntrySize := 1 + len(k) + sovStorageCommon(uint64(len(k))) + 1 + len(v) + sovStorageCommon(uint64(len(v)))
			n += mapEntrySize + 1 + sovStorageCommon(uint64(mapEntrySize))
		}
	}
	if m.Group != 0 {
		n += 1 + sovStorageCommon(uint64(m.Group))
	}
	if m.Hints != 0 {
		n += 5
	}
	if m.ReadSource != nil {
		l = m.ReadSource.Size()
		n += 1 + l + sovStorageCommon(uint64(l))
	}
	return n
}

func (m *Aggregate) Size() (n int) {
	var l int
	_ = l
	if m.Type != 0 {
		n += 1 + sovStorageCommon(uint64(m.Type))
	}
	return n
}

func (m *Tag) Size() (n int) {
	var l int
	_ = l
	l = len(m.Key)
	if l > 0 {
		n += 1 + l + sovStorageCommon(uint64(l))
	}
	l = len(m.Value)
	if l > 0 {
		n += 1 + l + sovStorageCommon(uint64(l))
	}
	return n
}

func (m *ReadResponse) Size() (n int) {
	var l int
	_ = l
	if len(m.Frames) > 0 {
		for _, e := range m.Frames {
			l = e.Size()
			n += 1 + l + sovStorageCommon(uint64(l))
		}
	}
	return n
}

func (m *ReadResponse_Frame) Size() (n int) {
	var l int
	_ = l
	if m.Data != nil {
		n += m.Data.Size()
	}
	return n
}

func (m *ReadResponse_Frame_Series) Size() (n int) {
	var l int
	_ = l
	if m.Series != nil {
		l = m.Series.Size()
		n += 1 + l + sovStorageCommon(uint64(l))
	}
	return n
}
func (m *ReadResponse_Frame_FloatPoints) Size() (n int) {
	var l int
	_ = l
	if m.FloatPoints != nil {
		l = m.FloatPoints.Size()
		n += 1 + l + sovStorageCommon(uint64(l))
	}
	return n
}
func (m *ReadResponse_Frame_IntegerPoints) Size() (n int) {
	var l int
	_ = l
	if m.IntegerPoints != nil {
		l = m.IntegerPoints.Size()
		n += 1 + l + sovStorageCommon(uint64(l))
	}
	return n
}
func (m *ReadResponse_Frame_UnsignedPoints) Size() (n int) {
	var l int
	_ = l
	if m.UnsignedPoints != nil {
		l = m.UnsignedPoints.Size()
		n += 1 + l + sovStorageCommon(uint64(l))
	}
	return n
}
func (m *ReadResponse_Frame_BooleanPoints) Size() (n int) {
	var l int
	_ = l
	if m.BooleanPoints != nil {
		l = m.BooleanPoints.Size()
		n += 1 + l + sovStorageCommon(uint64(l))
	}
	return n
}
func (m *ReadResponse_Frame_StringPoints) Size() (n int) {
	var l int
	_ = l
	if m.StringPoints != nil {
		l = m.StringPoints.Size()
		n += 1 + l + sovStorageCommon(uint64(l))
	}
	return n
}
func (m *ReadResponse_Frame_Group) Size() (n int) {
	var l int
	_ = l
	if m.Group != nil {
		l = m.Group.Size()
		n += 1 + l + sovStorageCommon(uint64(l))
	}
	return n
}
func (m *ReadResponse_GroupFrame) Size() (n int) {
	var l int
	_ = l
	if len(m.TagKeys) > 0 {
		for _, b := range m.TagKeys {
			l = len(b)
			n += 1 + l + sovStorageCommon(uint64(l))
		}
	}
	if len(m.PartitionKeyVals) > 0 {
		for _, b := range m.PartitionKeyVals {
			l = len(b)
			n += 1 + l + sovStorageCommon(uint64(l))
		}
	}
	return n
}

func (m *ReadResponse_SeriesFrame) Size() (n int) {
	var l int
	_ = l
	if len(m.Tags) > 0 {
		for _, e := range m.Tags {
			l = e.Size()
			n += 1 + l + sovStorageCommon(uint64(l))
		}
	}
	if m.DataType != 0 {
		n += 1 + sovStorageCommon(uint64(m.DataType))
	}
	return n
}

func (m *ReadResponse_FloatPointsFrame) Size() (n int) {
	var l int
	_ = l
	if len(m.Timestamps) > 0 {
		n += 1 + sovStorageCommon(uint64(len(m.Timestamps)*8)) + len(m.Timestamps)*8
	}
	if len(m.Values) > 0 {
		n += 1 + sovStorageCommon(uint64(len(m.Values)*8)) + len(m.Values)*8
	}
	return n
}

func (m *ReadResponse_IntegerPointsFrame) Size() (n int) {
	var l int
	_ = l
	if len(m.Timestamps) > 0 {
		n += 1 + sovStorageCommon(uint64(len(m.Timestamps)*8)) + len(m.Timestamps)*8
	}
	if len(m.Values) > 0 {
		l = 0
		for _, e := range m.Values {
			l += sovStorageCommon(uint64(e))
		}
		n += 1 + sovStorageCommon(uint64(l)) + l
	}
	return n
}

func (m *ReadResponse_UnsignedPointsFrame) Size() (n int) {
	var l int
	_ = l
	if len(m.Timestamps) > 0 {
		n += 1 + sovStorageCommon(uint64(len(m.Timestamps)*8)) + len(m.Timestamps)*8
	}
	if len(m.Values) > 0 {
		l = 0
		for _, e := range m.Values {
			l += sovStorageCommon(uint64(e))
		}
		n += 1 + sovStorageCommon(uint64(l)) + l
	}
	return n
}

func (m *ReadResponse_BooleanPointsFrame) Size() (n int) {
	var l int
	_ = l
	if len(m.Timestamps) > 0 {
		n += 1 + sovStorageCommon(uint64(len(m.Timestamps)*8)) + len(m.Timestamps)*8
	}
	if len(m.Values) > 0 {
		n += 1 + sovStorageCommon(uint64(len(m.Values))) + len(m.Values)*1
	}
	return n
}

func (m *ReadResponse_StringPointsFrame) Size() (n int) {
	var l int
	_ = l
	if len(m.Timestamps) > 0 {
		n += 1 + sovStorageCommon(uint64(len(m.Timestamps)*8)) + len(m.Timestamps)*8
	}
	if len(m.Values) > 0 {
		for _, s := range m.Values {
			l = len(s)
			n += 1 + l + sovStorageCommon(uint64(l))
		}
	}
	return n
}

func (m *CapabilitiesResponse) Size() (n int) {
	var l int
	_ = l
	if len(m.Caps) > 0 {
		for k, v := range m.Caps {
			_ = k
			_ = v
			mapEntrySize := 1 + len(k) + sovStorageCommon(uint64(len(k))) + 1 + len(v) + sovStorageCommon(uint64(len(v)))
			n += mapEntrySize + 1 + sovStorageCommon(uint64(mapEntrySize))
		}
	}
	return n
}

func (m *HintsResponse) Size() (n int) {
	var l int
	_ = l
	return n
}

func (m *TimestampRange) Size() (n int) {
	var l int
	_ = l
	if m.Start != 0 {
		n += 1 + sovStorageCommon(uint64(m.Start))
	}
	if m.End != 0 {
		n += 1 + sovStorageCommon(uint64(m.End))
	}
	return n
}

func sovStorageCommon(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
func sozStorageCommon(x uint64) (n int) {
	return sovStorageCommon(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *ReadRequest) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowStorageCommon
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: ReadRequest: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: ReadRequest: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field TimestampRange", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowStorageCommon
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthStorageCommon
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if err := m.TimestampRange.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Descending", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowStorageCommon
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				v |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Descending = bool(v != 0)
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field GroupKeys", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowStorageCommon
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthStorageCommon
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.GroupKeys = append(m.GroupKeys, string(dAtA[iNdEx:postIndex]))
			iNdEx = postIndex
		case 5:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Predicate", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowStorageCommon
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthStorageCommon
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Predicate == nil {
				m.Predicate = &Predicate{}
			}
			if err := m.Predicate.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 6:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field SeriesLimit", wireType)
			}
			m.SeriesLimit = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowStorageCommon
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.SeriesLimit |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 7:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field SeriesOffset", wireType)
			}
			m.SeriesOffset = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowStorageCommon
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.SeriesOffset |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 8:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field PointsLimit", wireType)
			}
			m.PointsLimit = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowStorageCommon
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.PointsLimit |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 9:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Aggregate", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowStorageCommon
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthStorageCommon
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Aggregate == nil {
				m.Aggregate = &Aggregate{}
			}
			if err := m.Aggregate.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 10:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Trace", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowStorageCommon
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthStorageCommon
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Trace == nil {
				m.Trace = make(map[string]string)
			}
			var mapkey string
			var mapvalue string
			for iNdEx < postIndex {
				entryPreIndex := iNdEx
				var wire uint64
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowStorageCommon
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					wire |= (uint64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				fieldNum := int32(wire >> 3)
				if fieldNum == 1 {
					var stringLenmapkey uint64
					for shift := uint(0); ; shift += 7 {
						if shift >= 64 {
							return ErrIntOverflowStorageCommon
						}
						if iNdEx >= l {
							return io.ErrUnexpectedEOF
						}
						b := dAtA[iNdEx]
						iNdEx++
						stringLenmapkey |= (uint64(b) & 0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					intStringLenmapkey := int(stringLenmapkey)
					if intStringLenmapkey < 0 {
						return ErrInvalidLengthStorageCommon
					}
					postStringIndexmapkey := iNdEx + intStringLenmapkey
					if postStringIndexmapkey > l {
						return io.ErrUnexpectedEOF
					}
					mapkey = string(dAtA[iNdEx:postStringIndexmapkey])
					iNdEx = postStringIndexmapkey
				} else if fieldNum == 2 {
					var stringLenmapvalue uint64
					for shift := uint(0); ; shift += 7 {
						if shift >= 64 {
							return ErrIntOverflowStorageCommon
						}
						if iNdEx >= l {
							return io.ErrUnexpectedEOF
						}
						b := dAtA[iNdEx]
						iNdEx++
						stringLenmapvalue |= (uint64(b) & 0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					intStringLenmapvalue := int(stringLenmapvalue)
					if intStringLenmapvalue < 0 {
						return ErrInvalidLengthStorageCommon
					}
					postStringIndexmapvalue := iNdEx + intStringLenmapvalue
					if postStringIndexmapvalue > l {
						return io.ErrUnexpectedEOF
					}
					mapvalue = string(dAtA[iNdEx:postStringIndexmapvalue])
					iNdEx = postStringIndexmapvalue
				} else {
					iNdEx = entryPreIndex
					skippy, err := skipStorageCommon(dAtA[iNdEx:])
					if err != nil {
						return err
					}
					if skippy < 0 {
						return ErrInvalidLengthStorageCommon
					}
					if (iNdEx + skippy) > postIndex {
						return io.ErrUnexpectedEOF
					}
					iNdEx += skippy
				}
			}
			m.Trace[mapkey] = mapvalue
			iNdEx = postIndex
		case 11:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Group", wireType)
			}
			m.Group = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowStorageCommon
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Group |= (ReadRequest_Group(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 12:
			if wireType != 5 {
				return fmt.Errorf("proto: wrong wireType = %d for field Hints", wireType)
			}
			m.Hints = 0
			if (iNdEx + 4) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += 4
			m.Hints = HintFlags(dAtA[iNdEx-4])
			m.Hints |= HintFlags(dAtA[iNdEx-3]) << 8
			m.Hints |= HintFlags(dAtA[iNdEx-2]) << 16
			m.Hints |= HintFlags(dAtA[iNdEx-1]) << 24
		case 13:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field ReadSource", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowStorageCommon
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthStorageCommon
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.ReadSource == nil {
				m.ReadSource = &google_protobuf2.Any{}
			}
			if err := m.ReadSource.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipStorageCommon(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthStorageCommon
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *Aggregate) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowStorageCommon
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Aggregate: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Aggregate: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Type", wireType)
			}
			m.Type = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowStorageCommon
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Type |= (Aggregate_AggregateType(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipStorageCommon(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthStorageCommon
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *Tag) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowStorageCommon
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Tag: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Tag: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Key", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowStorageCommon
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthStorageCommon
			}
			postIndex := iNdEx + byteLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Key = append(m.Key[:0], dAtA[iNdEx:postIndex]...)
			if m.Key == nil {
				m.Key = []byte{}
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Value", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowStorageCommon
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthStorageCommon
			}
			postIndex := iNdEx + byteLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Value = append(m.Value[:0], dAtA[iNdEx:postIndex]...)
			if m.Value == nil {
				m.Value = []byte{}
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipStorageCommon(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthStorageCommon
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *ReadResponse) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowStorageCommon
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: ReadResponse: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: ReadResponse: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Frames", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowStorageCommon
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthStorageCommon
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Frames = append(m.Frames, ReadResponse_Frame{})
			if err := m.Frames[len(m.Frames)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipStorageCommon(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthStorageCommon
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *ReadResponse_Frame) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowStorageCommon
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Frame: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Frame: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Series", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowStorageCommon
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthStorageCommon
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			v := &ReadResponse_SeriesFrame{}
			if err := v.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			m.Data = &ReadResponse_Frame_Series{v}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field FloatPoints", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowStorageCommon
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthStorageCommon
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			v := &ReadResponse_FloatPointsFrame{}
			if err := v.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			m.Data = &ReadResponse_Frame_FloatPoints{v}
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field IntegerPoints", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowStorageCommon
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthStorageCommon
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			v := &ReadResponse_IntegerPointsFrame{}
			if err := v.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			m.Data = &ReadResponse_Frame_IntegerPoints{v}
			iNdEx = postIndex
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field UnsignedPoints", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowStorageCommon
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthStorageCommon
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			v := &ReadResponse_UnsignedPointsFrame{}
			if err := v.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			m.Data = &ReadResponse_Frame_UnsignedPoints{v}
			iNdEx = postIndex
		case 5:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field BooleanPoints", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowStorageCommon
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthStorageCommon
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			v := &ReadResponse_BooleanPointsFrame{}
			if err := v.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			m.Data = &ReadResponse_Frame_BooleanPoints{v}
			iNdEx = postIndex
		case 6:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field StringPoints", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowStorageCommon
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthStorageCommon
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			v := &ReadResponse_StringPointsFrame{}
			if err := v.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			m.Data = &ReadResponse_Frame_StringPoints{v}
			iNdEx = postIndex
		case 7:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Group", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowStorageCommon
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthStorageCommon
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			v := &ReadResponse_GroupFrame{}
			if err := v.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			m.Data = &ReadResponse_Frame_Group{v}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipStorageCommon(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthStorageCommon
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *ReadResponse_GroupFrame) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowStorageCommon
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: GroupFrame: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: GroupFrame: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field TagKeys", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowStorageCommon
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthStorageCommon
			}
			postIndex := iNdEx + byteLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.TagKeys = append(m.TagKeys, make([]byte, postIndex-iNdEx))
			copy(m.TagKeys[len(m.TagKeys)-1], dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field PartitionKeyVals", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowStorageCommon
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthStorageCommon
			}
			postIndex := iNdEx + byteLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.PartitionKeyVals = append(m.PartitionKeyVals, make([]byte, postIndex-iNdEx))
			copy(m.PartitionKeyVals[len(m.PartitionKeyVals)-1], dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipStorageCommon(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthStorageCommon
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *ReadResponse_SeriesFrame) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowStorageCommon
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: SeriesFrame: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: SeriesFrame: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Tags", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowStorageCommon
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthStorageCommon
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Tags = append(m.Tags, Tag{})
			if err := m.Tags[len(m.Tags)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field DataType", wireType)
			}
			m.DataType = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowStorageCommon
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.DataType |= (ReadResponse_DataType(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipStorageCommon(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthStorageCommon
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *ReadResponse_FloatPointsFrame) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowStorageCommon
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: FloatPointsFrame: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: FloatPointsFrame: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType == 1 {
				var v int64
				if (iNdEx + 8) > l {
					return io.ErrUnexpectedEOF
				}
				iNdEx += 8
				v = int64(dAtA[iNdEx-8])
				v |= int64(dAtA[iNdEx-7]) << 8
				v |= int64(dAtA[iNdEx-6]) << 16
				v |= int64(dAtA[iNdEx-5]) << 24
				v |= int64(dAtA[iNdEx-4]) << 32
				v |= int64(dAtA[iNdEx-3]) << 40
				v |= int64(dAtA[iNdEx-2]) << 48
				v |= int64(dAtA[iNdEx-1]) << 56
				m.Timestamps = append(m.Timestamps, v)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowStorageCommon
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= (int(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				if packedLen < 0 {
					return ErrInvalidLengthStorageCommon
				}
				postIndex := iNdEx + packedLen
				if postIndex > l {
					return io.ErrUnexpectedEOF
				}
				for iNdEx < postIndex {
					var v int64
					if (iNdEx + 8) > l {
						return io.ErrUnexpectedEOF
					}
					iNdEx += 8
					v = int64(dAtA[iNdEx-8])
					v |= int64(dAtA[iNdEx-7]) << 8
					v |= int64(dAtA[iNdEx-6]) << 16
					v |= int64(dAtA[iNdEx-5]) << 24
					v |= int64(dAtA[iNdEx-4]) << 32
					v |= int64(dAtA[iNdEx-3]) << 40
					v |= int64(dAtA[iNdEx-2]) << 48
					v |= int64(dAtA[iNdEx-1]) << 56
					m.Timestamps = append(m.Timestamps, v)
				}
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Timestamps", wireType)
			}
		case 2:
			if wireType == 1 {
				var v uint64
				if (iNdEx + 8) > l {
					return io.ErrUnexpectedEOF
				}
				iNdEx += 8
				v = uint64(dAtA[iNdEx-8])
				v |= uint64(dAtA[iNdEx-7]) << 8
				v |= uint64(dAtA[iNdEx-6]) << 16
				v |= uint64(dAtA[iNdEx-5]) << 24
				v |= uint64(dAtA[iNdEx-4]) << 32
				v |= uint64(dAtA[iNdEx-3]) << 40
				v |= uint64(dAtA[iNdEx-2]) << 48
				v |= uint64(dAtA[iNdEx-1]) << 56
				v2 := float64(math.Float64frombits(v))
				m.Values = append(m.Values, v2)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowStorageCommon
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= (int(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				if packedLen < 0 {
					return ErrInvalidLengthStorageCommon
				}
				postIndex := iNdEx + packedLen
				if postIndex > l {
					return io.ErrUnexpectedEOF
				}
				for iNdEx < postIndex {
					var v uint64
					if (iNdEx + 8) > l {
						return io.ErrUnexpectedEOF
					}
					iNdEx += 8
					v = uint64(dAtA[iNdEx-8])
					v |= uint64(dAtA[iNdEx-7]) << 8
					v |= uint64(dAtA[iNdEx-6]) << 16
					v |= uint64(dAtA[iNdEx-5]) << 24
					v |= uint64(dAtA[iNdEx-4]) << 32
					v |= uint64(dAtA[iNdEx-3]) << 40
					v |= uint64(dAtA[iNdEx-2]) << 48
					v |= uint64(dAtA[iNdEx-1]) << 56
					v2 := float64(math.Float64frombits(v))
					m.Values = append(m.Values, v2)
				}
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Values", wireType)
			}
		default:
			iNdEx = preIndex
			skippy, err := skipStorageCommon(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthStorageCommon
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *ReadResponse_IntegerPointsFrame) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowStorageCommon
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: IntegerPointsFrame: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: IntegerPointsFrame: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType == 1 {
				var v int64
				if (iNdEx + 8) > l {
					return io.ErrUnexpectedEOF
				}
				iNdEx += 8
				v = int64(dAtA[iNdEx-8])
				v |= int64(dAtA[iNdEx-7]) << 8
				v |= int64(dAtA[iNdEx-6]) << 16
				v |= int64(dAtA[iNdEx-5]) << 24
				v |= int64(dAtA[iNdEx-4]) << 32
				v |= int64(dAtA[iNdEx-3]) << 40
				v |= int64(dAtA[iNdEx-2]) << 48
				v |= int64(dAtA[iNdEx-1]) << 56
				m.Timestamps = append(m.Timestamps, v)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowStorageCommon
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= (int(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				if packedLen < 0 {
					return ErrInvalidLengthStorageCommon
				}
				postIndex := iNdEx + packedLen
				if postIndex > l {
					return io.ErrUnexpectedEOF
				}
				for iNdEx < postIndex {
					var v int64
					if (iNdEx + 8) > l {
						return io.ErrUnexpectedEOF
					}
					iNdEx += 8
					v = int64(dAtA[iNdEx-8])
					v |= int64(dAtA[iNdEx-7]) << 8
					v |= int64(dAtA[iNdEx-6]) << 16
					v |= int64(dAtA[iNdEx-5]) << 24
					v |= int64(dAtA[iNdEx-4]) << 32
					v |= int64(dAtA[iNdEx-3]) << 40
					v |= int64(dAtA[iNdEx-2]) << 48
					v |= int64(dAtA[iNdEx-1]) << 56
					m.Timestamps = append(m.Timestamps, v)
				}
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Timestamps", wireType)
			}
		case 2:
			if wireType == 0 {
				var v int64
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowStorageCommon
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					v |= (int64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				m.Values = append(m.Values, v)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowStorageCommon
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= (int(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				if packedLen < 0 {
					return ErrInvalidLengthStorageCommon
				}
				postIndex := iNdEx + packedLen
				if postIndex > l {
					return io.ErrUnexpectedEOF
				}
				for iNdEx < postIndex {
					var v int64
					for shift := uint(0); ; shift += 7 {
						if shift >= 64 {
							return ErrIntOverflowStorageCommon
						}
						if iNdEx >= l {
							return io.ErrUnexpectedEOF
						}
						b := dAtA[iNdEx]
						iNdEx++
						v |= (int64(b) & 0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					m.Values = append(m.Values, v)
				}
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Values", wireType)
			}
		default:
			iNdEx = preIndex
			skippy, err := skipStorageCommon(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthStorageCommon
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *ReadResponse_UnsignedPointsFrame) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowStorageCommon
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: UnsignedPointsFrame: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: UnsignedPointsFrame: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType == 1 {
				var v int64
				if (iNdEx + 8) > l {
					return io.ErrUnexpectedEOF
				}
				iNdEx += 8
				v = int64(dAtA[iNdEx-8])
				v |= int64(dAtA[iNdEx-7]) << 8
				v |= int64(dAtA[iNdEx-6]) << 16
				v |= int64(dAtA[iNdEx-5]) << 24
				v |= int64(dAtA[iNdEx-4]) << 32
				v |= int64(dAtA[iNdEx-3]) << 40
				v |= int64(dAtA[iNdEx-2]) << 48
				v |= int64(dAtA[iNdEx-1]) << 56
				m.Timestamps = append(m.Timestamps, v)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowStorageCommon
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= (int(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				if packedLen < 0 {
					return ErrInvalidLengthStorageCommon
				}
				postIndex := iNdEx + packedLen
				if postIndex > l {
					return io.ErrUnexpectedEOF
				}
				for iNdEx < postIndex {
					var v int64
					if (iNdEx + 8) > l {
						return io.ErrUnexpectedEOF
					}
					iNdEx += 8
					v = int64(dAtA[iNdEx-8])
					v |= int64(dAtA[iNdEx-7]) << 8
					v |= int64(dAtA[iNdEx-6]) << 16
					v |= int64(dAtA[iNdEx-5]) << 24
					v |= int64(dAtA[iNdEx-4]) << 32
					v |= int64(dAtA[iNdEx-3]) << 40
					v |= int64(dAtA[iNdEx-2]) << 48
					v |= int64(dAtA[iNdEx-1]) << 56
					m.Timestamps = append(m.Timestamps, v)
				}
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Timestamps", wireType)
			}
		case 2:
			if wireType == 0 {
				var v uint64
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowStorageCommon
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					v |= (uint64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				m.Values = append(m.Values, v)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowStorageCommon
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= (int(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				if packedLen < 0 {
					return ErrInvalidLengthStorageCommon
				}
				postIndex := iNdEx + packedLen
				if postIndex > l {
					return io.ErrUnexpectedEOF
				}
				for iNdEx < postIndex {
					var v uint64
					for shift := uint(0); ; shift += 7 {
						if shift >= 64 {
							return ErrIntOverflowStorageCommon
						}
						if iNdEx >= l {
							return io.ErrUnexpectedEOF
						}
						b := dAtA[iNdEx]
						iNdEx++
						v |= (uint64(b) & 0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					m.Values = append(m.Values, v)
				}
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Values", wireType)
			}
		default:
			iNdEx = preIndex
			skippy, err := skipStorageCommon(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthStorageCommon
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *ReadResponse_BooleanPointsFrame) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowStorageCommon
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: BooleanPointsFrame: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: BooleanPointsFrame: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType == 1 {
				var v int64
				if (iNdEx + 8) > l {
					return io.ErrUnexpectedEOF
				}
				iNdEx += 8
				v = int64(dAtA[iNdEx-8])
				v |= int64(dAtA[iNdEx-7]) << 8
				v |= int64(dAtA[iNdEx-6]) << 16
				v |= int64(dAtA[iNdEx-5]) << 24
				v |= int64(dAtA[iNdEx-4]) << 32
				v |= int64(dAtA[iNdEx-3]) << 40
				v |= int64(dAtA[iNdEx-2]) << 48
				v |= int64(dAtA[iNdEx-1]) << 56
				m.Timestamps = append(m.Timestamps, v)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowStorageCommon
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= (int(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				if packedLen < 0 {
					return ErrInvalidLengthStorageCommon
				}
				postIndex := iNdEx + packedLen
				if postIndex > l {
					return io.ErrUnexpectedEOF
				}
				for iNdEx < postIndex {
					var v int64
					if (iNdEx + 8) > l {
						return io.ErrUnexpectedEOF
					}
					iNdEx += 8
					v = int64(dAtA[iNdEx-8])
					v |= int64(dAtA[iNdEx-7]) << 8
					v |= int64(dAtA[iNdEx-6]) << 16
					v |= int64(dAtA[iNdEx-5]) << 24
					v |= int64(dAtA[iNdEx-4]) << 32
					v |= int64(dAtA[iNdEx-3]) << 40
					v |= int64(dAtA[iNdEx-2]) << 48
					v |= int64(dAtA[iNdEx-1]) << 56
					m.Timestamps = append(m.Timestamps, v)
				}
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Timestamps", wireType)
			}
		case 2:
			if wireType == 0 {
				var v int
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowStorageCommon
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					v |= (int(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				m.Values = append(m.Values, bool(v != 0))
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowStorageCommon
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= (int(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				if packedLen < 0 {
					return ErrInvalidLengthStorageCommon
				}
				postIndex := iNdEx + packedLen
				if postIndex > l {
					return io.ErrUnexpectedEOF
				}
				for iNdEx < postIndex {
					var v int
					for shift := uint(0); ; shift += 7 {
						if shift >= 64 {
							return ErrIntOverflowStorageCommon
						}
						if iNdEx >= l {
							return io.ErrUnexpectedEOF
						}
						b := dAtA[iNdEx]
						iNdEx++
						v |= (int(b) & 0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					m.Values = append(m.Values, bool(v != 0))
				}
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Values", wireType)
			}
		default:
			iNdEx = preIndex
			skippy, err := skipStorageCommon(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthStorageCommon
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *ReadResponse_StringPointsFrame) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowStorageCommon
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: StringPointsFrame: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: StringPointsFrame: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType == 1 {
				var v int64
				if (iNdEx + 8) > l {
					return io.ErrUnexpectedEOF
				}
				iNdEx += 8
				v = int64(dAtA[iNdEx-8])
				v |= int64(dAtA[iNdEx-7]) << 8
				v |= int64(dAtA[iNdEx-6]) << 16
				v |= int64(dAtA[iNdEx-5]) << 24
				v |= int64(dAtA[iNdEx-4]) << 32
				v |= int64(dAtA[iNdEx-3]) << 40
				v |= int64(dAtA[iNdEx-2]) << 48
				v |= int64(dAtA[iNdEx-1]) << 56
				m.Timestamps = append(m.Timestamps, v)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowStorageCommon
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= (int(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				if packedLen < 0 {
					return ErrInvalidLengthStorageCommon
				}
				postIndex := iNdEx + packedLen
				if postIndex > l {
					return io.ErrUnexpectedEOF
				}
				for iNdEx < postIndex {
					var v int64
					if (iNdEx + 8) > l {
						return io.ErrUnexpectedEOF
					}
					iNdEx += 8
					v = int64(dAtA[iNdEx-8])
					v |= int64(dAtA[iNdEx-7]) << 8
					v |= int64(dAtA[iNdEx-6]) << 16
					v |= int64(dAtA[iNdEx-5]) << 24
					v |= int64(dAtA[iNdEx-4]) << 32
					v |= int64(dAtA[iNdEx-3]) << 40
					v |= int64(dAtA[iNdEx-2]) << 48
					v |= int64(dAtA[iNdEx-1]) << 56
					m.Timestamps = append(m.Timestamps, v)
				}
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field Timestamps", wireType)
			}
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Values", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowStorageCommon
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthStorageCommon
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Values = append(m.Values, string(dAtA[iNdEx:postIndex]))
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipStorageCommon(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthStorageCommon
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *CapabilitiesResponse) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowStorageCommon
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: CapabilitiesResponse: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: CapabilitiesResponse: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Caps", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowStorageCommon
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthStorageCommon
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Caps == nil {
				m.Caps = make(map[string]string)
			}
			var mapkey string
			var mapvalue string
			for iNdEx < postIndex {
				entryPreIndex := iNdEx
				var wire uint64
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowStorageCommon
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					wire |= (uint64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				fieldNum := int32(wire >> 3)
				if fieldNum == 1 {
					var stringLenmapkey uint64
					for shift := uint(0); ; shift += 7 {
						if shift >= 64 {
							return ErrIntOverflowStorageCommon
						}
						if iNdEx >= l {
							return io.ErrUnexpectedEOF
						}
						b := dAtA[iNdEx]
						iNdEx++
						stringLenmapkey |= (uint64(b) & 0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					intStringLenmapkey := int(stringLenmapkey)
					if intStringLenmapkey < 0 {
						return ErrInvalidLengthStorageCommon
					}
					postStringIndexmapkey := iNdEx + intStringLenmapkey
					if postStringIndexmapkey > l {
						return io.ErrUnexpectedEOF
					}
					mapkey = string(dAtA[iNdEx:postStringIndexmapkey])
					iNdEx = postStringIndexmapkey
				} else if fieldNum == 2 {
					var stringLenmapvalue uint64
					for shift := uint(0); ; shift += 7 {
						if shift >= 64 {
							return ErrIntOverflowStorageCommon
						}
						if iNdEx >= l {
							return io.ErrUnexpectedEOF
						}
						b := dAtA[iNdEx]
						iNdEx++
						stringLenmapvalue |= (uint64(b) & 0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					intStringLenmapvalue := int(stringLenmapvalue)
					if intStringLenmapvalue < 0 {
						return ErrInvalidLengthStorageCommon
					}
					postStringIndexmapvalue := iNdEx + intStringLenmapvalue
					if postStringIndexmapvalue > l {
						return io.ErrUnexpectedEOF
					}
					mapvalue = string(dAtA[iNdEx:postStringIndexmapvalue])
					iNdEx = postStringIndexmapvalue
				} else {
					iNdEx = entryPreIndex
					skippy, err := skipStorageCommon(dAtA[iNdEx:])
					if err != nil {
						return err
					}
					if skippy < 0 {
						return ErrInvalidLengthStorageCommon
					}
					if (iNdEx + skippy) > postIndex {
						return io.ErrUnexpectedEOF
					}
					iNdEx += skippy
				}
			}
			m.Caps[mapkey] = mapvalue
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipStorageCommon(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthStorageCommon
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *HintsResponse) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowStorageCommon
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: HintsResponse: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: HintsResponse: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		default:
			iNdEx = preIndex
			skippy, err := skipStorageCommon(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthStorageCommon
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *TimestampRange) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowStorageCommon
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: TimestampRange: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: TimestampRange: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Start", wireType)
			}
			m.Start = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowStorageCommon
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Start |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field End", wireType)
			}
			m.End = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowStorageCommon
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.End |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipStorageCommon(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthStorageCommon
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipStorageCommon(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowStorageCommon
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowStorageCommon
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
			return iNdEx, nil
		case 1:
			iNdEx += 8
			return iNdEx, nil
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowStorageCommon
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			iNdEx += length
			if length < 0 {
				return 0, ErrInvalidLengthStorageCommon
			}
			return iNdEx, nil
		case 3:
			for {
				var innerWire uint64
				var start int = iNdEx
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return 0, ErrIntOverflowStorageCommon
					}
					if iNdEx >= l {
						return 0, io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					innerWire |= (uint64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				innerWireType := int(innerWire & 0x7)
				if innerWireType == 4 {
					break
				}
				next, err := skipStorageCommon(dAtA[start:])
				if err != nil {
					return 0, err
				}
				iNdEx = start + next
			}
			return iNdEx, nil
		case 4:
			return iNdEx, nil
		case 5:
			iNdEx += 4
			return iNdEx, nil
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
	}
	panic("unreachable")
}

var (
	ErrInvalidLengthStorageCommon = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowStorageCommon   = fmt.Errorf("proto: integer overflow")
)

func init() { proto.RegisterFile("storage_common.proto", fileDescriptorStorageCommon) }

var fileDescriptorStorageCommon = []byte{
	// 1568 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xac, 0x57, 0xcd, 0x8f, 0xe3, 0x48,
	0x15, 0x8f, 0xf3, 0xed, 0x97, 0x2f, 0x4f, 0x6d, 0xd3, 0xca, 0x7a, 0x99, 0xc4, 0x1b, 0x21, 0x94,
	0xc3, 0x6e, 0x1a, 0x35, 0x20, 0x46, 0x2b, 0x3e, 0x14, 0xf7, 0xba, 0x3b, 0x61, 0xba, 0x93, 0xa6,
	0x92, 0x46, 0x03, 0x42, 0x0a, 0xd5, 0x49, 0xb5, 0xdb, 0xda, 0xc4, 0x0e, 0xb6, 0x33, 0xea, 0xdc,
	0x38, 0x80, 0x58, 0x45, 0x1c, 0xf6, 0xca, 0x21, 0x27, 0x8e, 0x5c, 0x11, 0x7f, 0x00, 0xa7, 0x39,
	0xf2, 0x07, 0xa0, 0x88, 0xcd, 0xfe, 0x17, 0x9c, 0x50, 0x55, 0xd9, 0x89, 0xd3, 0x3d, 0x1c, 0x32,
	0x99, 0x5b, 0xbd, 0x0f, 0xff, 0xde, 0xef, 0xbd, 0xaa, 0xf7, 0xaa, 0x0c, 0x47, 0x9e, 0xef, 0xb8,
	0xc4, 0xa4, 0x83, 0xa1, 0x33, 0x99, 0x38, 0x76, 0x63, 0xea, 0x3a, 0xbe, 0x83, 0x4e, 0x86, 0xce,
	0xa4, 0x61, 0x5a, 0xfe, 0xfd, 0xec, 0xb6, 0x61, 0xd9, 0x77, 0xe3, 0xd9, 0xc3, 0x88, 0xf8, 0x24,
	0x5c, 0xde, 0x36, 0x3c, 0xea, 0xbe, 0xb6, 0x86, 0xd4, 0x6b, 0x04, 0x5f, 0xab, 0x9f, 0x06, 0xce,
	0x43, 0x67, 0x72, 0x62, 0x3a, 0xa6, 0x73, 0xc2, 0x71, 0x6e, 0x67, 0x77, 0x5c, 0xe2, 0x02, 0x5f,
	0x09, 0x7c, 0xf5, 0x23, 0xd3, 0x71, 0xcc, 0x31, 0xdd, 0x7a, 0xd1, 0xc9, 0xd4, 0x9f, 0x07, 0xc6,
	0x0f, 0x1f, 0x1b, 0x89, 0x1d, 0x9a, 0x4a, 0x53, 0x97, 0x8e, 0xac, 0x21, 0xf1, 0xa9, 0x50, 0xd4,
	0xfe, 0x2d, 0x43, 0x0e, 0x53, 0x32, 0xc2, 0xf4, 0x77, 0x33, 0xea, 0xf9, 0xe8, 0xf7, 0x12, 0x94,
	0x7c, 0x6b, 0x42, 0x3d, 0x9f, 0x4c, 0xa6, 0x03, 0x97, 0xd8, 0x26, 0x2d, 0xc7, 0x35, 0xa9, 0x9e,
	0x3b, 0xfd, 0x59, 0x63, 0xcf, 0x9c, 0x1a, 0xfd, 0x10, 0x07, 0x33, 0x18, 0xfd, 0xf8, 0xcd, 0xaa,
	0x1a, 0x5b, 0xaf, 0xaa, 0xc5, 0x5d, 0x3d, 0x2e, 0xfa, 0x3b, 0x32, 0xaa, 0x00, 0x8c, 0xa8, 0x37,
	0xa4, 0xf6, 0xc8, 0xb2, 0xcd, 0x72, 0x42, 0x93, 0xea, 0x59, 0x1c, 0xd1, 0xa0, 0x4f, 0x00, 0x4c,
	0xd7, 0x99, 0x4d, 0x07, 0x5f, 0xd0, 0xb9, 0x57, 0x4e, 0x6a, 0x89, 0xba, 0xac, 0x17, 0xd6, 0xab,
	0xaa, 0x7c, 0xc1, 0xb4, 0x2f, 0xe9, 0xdc, 0xc3, 0xb2, 0x19, 0x2e, 0xd1, 0x2b, 0x90, 0x37, 0x39,
	0x97, 0x53, 0x3c, 0x93, 0xcf, 0xf6, 0xce, 0xe4, 0x3a, 0x44, 0xc0, 0x5b, 0x30, 0x74, 0x0a, 0x79,
	0x8f, 0xba, 0x16, 0xf5, 0x06, 0x63, 0x6b, 0x62, 0xf9, 0xe5, 0xb4, 0x26, 0xd5, 0x13, 0x7a, 0x69,
	0xbd, 0xaa, 0xe6, 0x7a, 0x5c, 0x7f, 0xc9, 0xd4, 0x38, 0xe7, 0x6d, 0x05, 0xf4, 0x43, 0x28, 0x04,
	0xdf, 0x38, 0x77, 0x77, 0x1e, 0xf5, 0xcb, 0x19, 0xfe, 0x91, 0xb2, 0x5e, 0x55, 0xf3, 0xe2, 0xa3,
	0x2e, 0xd7, 0xe3, 0x00, 0x5a, 0x48, 0x2c, 0xd4, 0xd4, 0xb1, 0x6c, 0x3f, 0x0c, 0x95, 0xdd, 0x86,
	0xba, 0xe6, 0xfa, 0x20, 0xd4, 0x74, 0x2b, 0xb0, 0xc4, 0x89, 0x69, 0xba, 0xd4, 0x64, 0x89, 0xcb,
	0xef, 0x98, 0x78, 0x33, 0x44, 0xc0, 0x5b, 0x30, 0x74, 0x0f, 0x29, 0xdf, 0x25, 0x43, 0x5a, 0x06,
	0x2d, 0x51, 0xcf, 0x9d, 0x5e, 0xec, 0x8d, 0x1a, 0x39, 0x70, 0x8d, 0x3e, 0x43, 0x32, 0x6c, 0xdf,
	0x9d, 0xeb, 0xf2, 0x7a, 0x55, 0x4d, 0x71, 0x19, 0x8b, 0x00, 0xe8, 0x15, 0xa4, 0xf8, 0x4e, 0x96,
	0x73, 0x9a, 0x54, 0x2f, 0x9e, 0xea, 0x07, 0x45, 0xe2, 0xc7, 0x03, 0x0b, 0x40, 0xf4, 0x09, 0xa4,
	0xee, 0x59, 0xad, 0xca, 0x79, 0x4d, 0xaa, 0x67, 0xf4, 0x63, 0x16, 0xba, 0xc5, 0x14, 0xff, 0x5d,
	0x55, 0x65, 0xb6, 0x38, 0x1f, 0x13, 0xd3, 0xc3, 0xc2, 0x09, 0x19, 0x90, 0x73, 0x29, 0x19, 0x0d,
	0x3c, 0x67, 0xe6, 0x0e, 0x69, 0xb9, 0xc0, 0xab, 0x79, 0xd4, 0x10, 0x7d, 0xd6, 0x08, 0xfb, 0xac,
	0xd1, 0xb4, 0xe7, 0x7a, 0x71, 0xbd, 0xaa, 0x02, 0x0b, 0xdb, 0xe3, 0xbe, 0x18, 0xdc, 0xcd, 0x5a,
	0x7d, 0x01, 0xb0, 0x4d, 0x17, 0x29, 0x90, 0xf8, 0x82, 0xce, 0xcb, 0x92, 0x26, 0xd5, 0x65, 0xcc,
	0x96, 0xe8, 0x08, 0x52, 0xaf, 0xc9, 0x78, 0x26, 0x3a, 0x4e, 0xc6, 0x42, 0xf8, 0x2c, 0xfe, 0x42,
	0xaa, 0xfd, 0x49, 0x82, 0x14, 0xe7, 0x8f, 0x9e, 0x03, 0x5c, 0xe0, 0xee, 0xcd, 0xf5, 0xa0, 0xd3,
	0xed, 0x18, 0x4a, 0x4c, 0x2d, 0x2c, 0x96, 0x9a, 0x38, 0xf9, 0x1d, 0xc7, 0xa6, 0xe8, 0x23, 0x90,
	0x85, 0xb9, 0x79, 0x79, 0xa9, 0x48, 0x6a, 0x7e, 0xb1, 0xd4, 0xb2, 0xdc, 0xda, 0x1c, 0x8f, 0xd1,
	0x87, 0x90, 0x15, 0x46, 0xfd, 0x57, 0x4a, 0x5c, 0xcd, 0x2d, 0x96, 0x5a, 0x86, 0xdb, 0xf4, 0x39,
	0xfa, 0x18, 0xf2, 0xc2, 0x64, 0xbc, 0x3a, 0x33, 0xae, 0xfb, 0x4a, 0x42, 0x2d, 0x2d, 0x96, 0x5a,
	0x8e, 0x9b, 0x8d, 0x87, 0x21, 0x9d, 0xfa, 0x6a, 0xf2, 0xcb, 0xbf, 0x56, 0x62, 0xb5, 0xbf, 0x49,
	0xb0, 0xad, 0x0f, 0x0b, 0xd7, 0x6a, 0x77, 0xfa, 0x21, 0x19, 0x1e, 0x8e, 0x59, 0x39, 0x97, 0xef,
	0x40, 0x31, 0x30, 0x0e, 0xae, 0xbb, 0xed, 0x4e, 0xbf, 0xa7, 0x48, 0xaa, 0xb2, 0x58, 0x6a, 0x79,
	0xe1, 0x21, 0x4e, 0x6e, 0xd4, 0xab, 0x67, 0xe0, 0xb6, 0xd1, 0x53, 0xe2, 0x51, 0x2f, 0xd1, 0x15,
	0xe8, 0x04, 0x8e, 0xb8, 0x57, 0xef, 0xac, 0x65, 0x5c, 0x35, 0x59, 0x76, 0x83, 0x7e, 0xfb, 0xca,
	0x50, 0x92, 0xea, 0xb7, 0x16, 0x4b, 0xed, 0x19, 0xf3, 0xed, 0x0d, 0xef, 0xe9, 0x84, 0x34, 0xc7,
	0x63, 0x36, 0x5f, 0x02, 0xb6, 0xdf, 0x48, 0x20, 0x6f, 0xce, 0x30, 0xfa, 0x0d, 0x24, 0xfd, 0xf9,
	0x94, 0xf2, 0x92, 0x17, 0x4f, 0x5b, 0xef, 0xde, 0x0d, 0xdb, 0x55, 0x7f, 0x3e, 0xa5, 0x98, 0xa3,
	0xd6, 0x1e, 0xa0, 0xb0, 0xa3, 0x46, 0x55, 0x48, 0x06, 0x75, 0xe1, 0x1c, 0x77, 0x8c, 0xbc, 0x40,
	0xcf, 0x21, 0xd1, 0xbb, 0xb9, 0x52, 0x24, 0xf5, 0x68, 0xb1, 0xd4, 0x94, 0x1d, 0x7b, 0x6f, 0x36,
	0x41, 0x1f, 0x43, 0xea, 0xac, 0x7b, 0xd3, 0xe9, 0x2b, 0x71, 0xf5, 0x78, 0xb1, 0xd4, 0xd0, 0x8e,
	0xc3, 0x99, 0x33, 0xb3, 0xc3, 0x3d, 0xf9, 0x14, 0x12, 0x7d, 0x62, 0x46, 0x0f, 0x54, 0xfe, 0x2d,
	0x07, 0x2a, 0x1f, 0x1c, 0xa8, 0xda, 0xaa, 0x04, 0x79, 0xd1, 0x18, 0xde, 0xd4, 0xb1, 0x3d, 0x8a,
	0x08, 0xa4, 0xef, 0x5c, 0x32, 0xa1, 0x5e, 0x59, 0xe2, 0x1d, 0x7d, 0xf6, 0x8e, 0x7d, 0x26, 0xe0,
	0x1a, 0xe7, 0x0c, 0x4b, 0x4f, 0xb2, 0x71, 0x8f, 0x03, 0x60, 0xf5, 0x2f, 0x19, 0x48, 0x71, 0x3d,
	0x1a, 0x42, 0x5a, 0xcc, 0x36, 0x4e, 0x34, 0x77, 0xda, 0x3e, 0x2c, 0x98, 0x38, 0x1f, 0x1c, 0xba,
	0x15, 0xc3, 0x01, 0x34, 0xfa, 0x83, 0x04, 0xf9, 0xbb, 0xb1, 0x43, 0xfc, 0x81, 0x18, 0x89, 0xc1,
	0x1d, 0xd6, 0x39, 0x30, 0x31, 0x86, 0x28, 0x8e, 0xad, 0xc8, 0x91, 0x4f, 0xe0, 0x88, 0xb6, 0x15,
	0xc3, 0xb9, 0xbb, 0xad, 0x88, 0xfe, 0x2c, 0x41, 0xd1, 0xb2, 0x7d, 0x6a, 0x52, 0x37, 0x24, 0x92,
	0xe0, 0x44, 0xae, 0x0f, 0x23, 0xd2, 0x16, 0x98, 0x51, 0x2a, 0xcf, 0xd6, 0xab, 0x6a, 0x61, 0x47,
	0xdf, 0x8a, 0xe1, 0x82, 0x15, 0x55, 0xa0, 0xaf, 0x24, 0x28, 0xcd, 0x6c, 0xcf, 0x32, 0x6d, 0x3a,
	0x0a, 0xf9, 0x24, 0x39, 0x9f, 0x5f, 0x1c, 0xc6, 0xe7, 0x26, 0x00, 0x8d, 0x12, 0x42, 0xec, 0xaa,
	0xdf, 0x35, 0xb4, 0x62, 0xb8, 0x38, 0xdb, 0xd1, 0xf0, 0x0a, 0xdd, 0x3a, 0xce, 0x98, 0x12, 0x3b,
	0x64, 0x94, 0x7a, 0x1f, 0x15, 0xd2, 0x05, 0xe6, 0x93, 0x0a, 0xed, 0xe8, 0x59, 0x85, 0x6e, 0xa3,
	0x0a, 0xf4, 0xa5, 0x04, 0x05, 0xcf, 0x77, 0x2d, 0xdb, 0x0c, 0xd9, 0xa4, 0x39, 0x9b, 0xee, 0x81,
	0x87, 0x94, 0x43, 0x46, 0xc9, 0x88, 0x1b, 0x3f, 0xa2, 0x6e, 0xc5, 0x70, 0xde, 0x8b, 0xc8, 0xe8,
	0xb7, 0xe1, 0xdd, 0x97, 0xe1, 0x0c, 0x5a, 0x87, 0x31, 0xe0, 0x83, 0x3c, 0xec, 0x12, 0x01, 0xac,
	0xa7, 0x21, 0xc9, 0x20, 0xd4, 0x07, 0x80, 0xad, 0x19, 0x7d, 0x17, 0xb2, 0x3e, 0x31, 0xc5, 0xe3,
	0x8a, 0x8d, 0x83, 0xbc, 0x9e, 0x5b, 0xaf, 0xaa, 0x99, 0x3e, 0x31, 0xf9, 0xd3, 0x2a, 0xe3, 0x8b,
	0x05, 0xd2, 0x01, 0x4d, 0x89, 0xeb, 0x5b, 0xbe, 0xe5, 0xd8, 0xcc, 0x7b, 0xf0, 0x9a, 0x8c, 0x59,
	0x9f, 0xb1, 0x2f, 0x8e, 0xd6, 0xab, 0xaa, 0x72, 0x1d, 0x5a, 0x5f, 0xd2, 0xf9, 0x2f, 0xc9, 0xd8,
	0xc3, 0xca, 0xf4, 0x91, 0x46, 0xfd, 0xa7, 0x04, 0xb9, 0x48, 0x03, 0xa3, 0x0e, 0x24, 0x7d, 0x62,
	0x86, 0x63, 0xe8, 0x07, 0xfb, 0xbf, 0x38, 0x89, 0x19, 0xcc, 0x1d, 0x8e, 0x83, 0x86, 0x20, 0xb3,
	0x2f, 0x06, 0x7c, 0xea, 0xc7, 0xf9, 0xd4, 0x3f, 0x3f, 0xac, 0x8e, 0x9f, 0x13, 0x9f, 0xf0, 0x99,
	0x9f, 0x1d, 0x05, 0x2b, 0xf5, 0xe7, 0xa0, 0x3c, 0x1e, 0x0c, 0xec, 0x0d, 0xbb, 0x79, 0xd5, 0x8a,
	0x74, 0x14, 0x1c, 0xd1, 0xa0, 0x63, 0x48, 0xf3, 0x59, 0x2c, 0x0a, 0x26, 0xe1, 0x40, 0x52, 0x2f,
	0x01, 0x3d, 0xed, 0xed, 0x3d, 0xd1, 0x12, 0x1b, 0xb4, 0x2b, 0xf8, 0xe0, 0x2d, 0x9d, 0xb9, 0x27,
	0x5c, 0x32, 0x4a, 0xee, 0x69, 0x5b, 0xed, 0x89, 0x96, 0xdd, 0xa0, 0xbd, 0x84, 0x67, 0x4f, 0xda,
	0x62, 0x4f, 0x30, 0x39, 0x04, 0xab, 0xf5, 0x40, 0xe6, 0x00, 0xc1, 0xbd, 0x9b, 0x0e, 0x5e, 0x12,
	0x31, 0xf5, 0x83, 0xc5, 0x52, 0x2b, 0x6d, 0x4c, 0xc1, 0x63, 0xa2, 0x0a, 0xe9, 0xcd, 0x83, 0x64,
	0xd7, 0x41, 0x70, 0x09, 0xae, 0xd5, 0x7f, 0x48, 0x90, 0x0d, 0xf7, 0x1b, 0x7d, 0x1b, 0x52, 0xe7,
	0x97, 0xdd, 0x66, 0x5f, 0x89, 0xa9, 0xcf, 0x16, 0x4b, 0xad, 0x10, 0x1a, 0xf8, 0xd6, 0x23, 0x0d,
	0x32, 0xed, 0x4e, 0xdf, 0xb8, 0x30, 0x70, 0x08, 0x19, 0xda, 0x83, 0xed, 0x44, 0x35, 0xc8, 0xde,
	0x74, 0x7a, 0xed, 0x8b, 0x8e, 0xf1, 0xb9, 0x12, 0x17, 0x17, 0x7e, 0xe8, 0x12, 0xee, 0x11, 0x43,
	0xd1, 0xbb, 0xdd, 0x4b, 0xa3, 0xd9, 0x51, 0x12, 0xbb, 0x28, 0x41, 0xdd, 0x51, 0x05, 0xd2, 0xbd,
	0x3e, 0x6e, 0x77, 0x2e, 0x94, 0xa4, 0x8a, 0x16, 0x4b, 0xad, 0x18, 0x3a, 0x88, 0x52, 0x06, 0xc4,
	0xff, 0x2e, 0xc1, 0xd1, 0x19, 0x99, 0x92, 0x5b, 0x6b, 0x6c, 0xf9, 0x16, 0xf5, 0x36, 0x17, 0xfd,
	0x10, 0x92, 0x43, 0x32, 0x0d, 0xfb, 0x6b, 0xff, 0xa1, 0xf6, 0x36, 0x50, 0xa6, 0xf4, 0xf8, 0x8b,
	0x16, 0x73, 0x70, 0xf5, 0x47, 0x20, 0x6f, 0x54, 0x7b, 0x3d, 0x72, 0x4b, 0x50, 0xe0, 0x4f, 0xf0,
	0x10, 0xb9, 0xf6, 0x02, 0x1e, 0xfd, 0x2b, 0xb2, 0x8f, 0x3d, 0x9f, 0xb8, 0x3e, 0x07, 0x4c, 0x60,
	0x21, 0xb0, 0x20, 0xd4, 0x1e, 0x71, 0xc0, 0x04, 0x66, 0xcb, 0xd3, 0xaf, 0xe3, 0x90, 0xe9, 0x09,
	0xd2, 0xe8, 0x8f, 0x12, 0x24, 0x59, 0x0f, 0xa3, 0x1f, 0x1f, 0xf2, 0xfb, 0xa0, 0xfe, 0xe4, 0xa0,
	0xc1, 0xf1, 0x3d, 0x09, 0x4d, 0x20, 0x1f, 0xad, 0x1f, 0x3a, 0x7e, 0xf2, 0xff, 0x60, 0xb0, 0x9f,
	0x78, 0xd5, 0x78, 0x2f, 0xdb, 0x82, 0x06, 0x20, 0x7e, 0x68, 0xfe, 0x6f, 0x9c, 0x9f, 0xee, 0x1d,
	0x67, 0x67, 0x77, 0xf4, 0xe7, 0x6f, 0xbe, 0xae, 0xc4, 0xde, 0xac, 0x2b, 0xd2, 0xbf, 0xd6, 0x15,
	0xe9, 0x3f, 0xeb, 0x8a, 0xf4, 0xd5, 0x37, 0x95, 0xd8, 0xaf, 0x33, 0x81, 0xf3, 0x6d, 0x9a, 0x87,
	0xfb, 0xfe, 0xff, 0x02, 0x00, 0x00, 0xff, 0xff, 0x39, 0xed, 0x9b, 0xa4, 0x21, 0x11, 0x00, 0x00,
}

type HintFlags uint32

func (h HintFlags) NoPoints() bool {
	return uint32(h)&uint32(HintNoPoints) != 0
}

func (h *HintFlags) SetNoPoints() {
	*h |= HintFlags(HintNoPoints)
}

func (h HintFlags) NoSeries() bool {
	return uint32(h)&uint32(HintNoSeries) != 0
}

func (h *HintFlags) SetNoSeries() {
	*h |= HintFlags(HintNoSeries)
}

func (h HintFlags) HintSchemaAllTime() bool {
	return uint32(h)&uint32(HintSchemaAllTime) != 0
}

func (h *HintFlags) SetHintSchemaAllTime() {
	*h |= HintFlags(HintSchemaAllTime)
}

func (h HintFlags) String() string {
	f := uint32(h)

	var s []string
	enums := proto.EnumValueMap("com.github.influxdata.influxdb.services.storage.ReadRequest_HintFlags")
	if h == 0 {
		return "HINT_NONE"
	}

	for k, v := range enums {
		if v == 0 {
			continue
		}
		v := uint32(v)
		if f&v == v {
			s = append(s, k)
		}
	}

	return strings.Join(s, ",")
}

type Node_Type int32

const (
	NodeTypeLogicalExpression    Node_Type = 0
	NodeTypeComparisonExpression Node_Type = 1
	NodeTypeParenExpression      Node_Type = 2
	NodeTypeTagRef               Node_Type = 3
	NodeTypeLiteral              Node_Type = 4
	NodeTypeFieldRef             Node_Type = 5
)

var Node_Type_name = map[int32]string{
	0: "LOGICAL_EXPRESSION",
	1: "COMPARISON_EXPRESSION",
	2: "PAREN_EXPRESSION",
	3: "TAG_REF",
	4: "LITERAL",
	5: "FIELD_REF",
}
var Node_Type_value = map[string]int32{
	"LOGICAL_EXPRESSION":    0,
	"COMPARISON_EXPRESSION": 1,
	"PAREN_EXPRESSION":      2,
	"TAG_REF":               3,
	"LITERAL":               4,
	"FIELD_REF":             5,
}

func (x Node_Type) String() string {
	return proto.EnumName(Node_Type_name, int32(x))
}
func (Node_Type) EnumDescriptor() ([]byte, []int) { return fileDescriptorPredicate, []int{0, 0} }

type Node_Comparison int32

const (
	ComparisonEqual        Node_Comparison = 0
	ComparisonNotEqual     Node_Comparison = 1
	ComparisonStartsWith   Node_Comparison = 2
	ComparisonRegex        Node_Comparison = 3
	ComparisonNotRegex     Node_Comparison = 4
	ComparisonLess         Node_Comparison = 5
	ComparisonLessEqual    Node_Comparison = 6
	ComparisonGreater      Node_Comparison = 7
	ComparisonGreaterEqual Node_Comparison = 8
)

var Node_Comparison_name = map[int32]string{
	0: "EQUAL",
	1: "NOT_EQUAL",
	2: "STARTS_WITH",
	3: "REGEX",
	4: "NOT_REGEX",
	5: "LT",
	6: "LTE",
	7: "GT",
	8: "GTE",
}
var Node_Comparison_value = map[string]int32{
	"EQUAL":       0,
	"NOT_EQUAL":   1,
	"STARTS_WITH": 2,
	"REGEX":       3,
	"NOT_REGEX":   4,
	"LT":          5,
	"LTE":         6,
	"GT":          7,
	"GTE":         8,
}

func (x Node_Comparison) String() string {
	return proto.EnumName(Node_Comparison_name, int32(x))
}
func (Node_Comparison) EnumDescriptor() ([]byte, []int) { return fileDescriptorPredicate, []int{0, 1} }

// Logical operators apply to boolean values and combine to produce a single boolean result.
type Node_Logical int32

const (
	LogicalAnd Node_Logical = 0
	LogicalOr  Node_Logical = 1
)

var Node_Logical_name = map[int32]string{
	0: "AND",
	1: "OR",
}
var Node_Logical_value = map[string]int32{
	"AND": 0,
	"OR":  1,
}

func (x Node_Logical) String() string {
	return proto.EnumName(Node_Logical_name, int32(x))
}
func (Node_Logical) EnumDescriptor() ([]byte, []int) { return fileDescriptorPredicate, []int{0, 2} }

type Node struct {
	NodeType Node_Type `protobuf:"varint,1,opt,name=node_type,json=nodeType,proto3,enum=com.github.influxdata.influxdb.services.storage.Node_Type" json:"nodeType"`
	Children []*Node   `protobuf:"bytes,2,rep,name=children" json:"children,omitempty"`
	// Types that are valid to be assigned to Value:
	//	*Node_StringValue
	//	*Node_BooleanValue
	//	*Node_IntegerValue
	//	*Node_UnsignedValue
	//	*Node_FloatValue
	//	*Node_RegexValue
	//	*Node_TagRefValue
	//	*Node_FieldRefValue
	//	*Node_Logical_
	//	*Node_Comparison_
	Value isNode_Value `protobuf_oneof:"value"`
}

func (m *Node) Reset()                    { *m = Node{} }
func (m *Node) String() string            { return proto.CompactTextString(m) }
func (*Node) ProtoMessage()               {}
func (*Node) Descriptor() ([]byte, []int) { return fileDescriptorPredicate, []int{0} }

type isNode_Value interface {
	isNode_Value()
	MarshalTo([]byte) (int, error)
	Size() int
}

type Node_StringValue struct {
	StringValue string `protobuf:"bytes,3,opt,name=string_value,json=stringValue,proto3,oneof"`
}
type Node_BooleanValue struct {
	BooleanValue bool `protobuf:"varint,4,opt,name=bool_value,json=boolValue,proto3,oneof"`
}
type Node_IntegerValue struct {
	IntegerValue int64 `protobuf:"varint,5,opt,name=int_value,json=intValue,proto3,oneof"`
}
type Node_UnsignedValue struct {
	UnsignedValue uint64 `protobuf:"varint,6,opt,name=uint_value,json=uintValue,proto3,oneof"`
}
type Node_FloatValue struct {
	FloatValue float64 `protobuf:"fixed64,7,opt,name=float_value,json=floatValue,proto3,oneof"`
}
type Node_RegexValue struct {
	RegexValue string `protobuf:"bytes,8,opt,name=regex_value,json=regexValue,proto3,oneof"`
}
type Node_TagRefValue struct {
	TagRefValue string `protobuf:"bytes,9,opt,name=tag_ref_value,json=tagRefValue,proto3,oneof"`
}
type Node_FieldRefValue struct {
	FieldRefValue string `protobuf:"bytes,10,opt,name=field_ref_value,json=fieldRefValue,proto3,oneof"`
}
type Node_Logical_ struct {
	Logical Node_Logical `protobuf:"varint,11,opt,name=logical,proto3,enum=com.github.influxdata.influxdb.services.storage.Node_Logical,oneof"`
}
type Node_Comparison_ struct {
	Comparison Node_Comparison `protobuf:"varint,12,opt,name=comparison,proto3,enum=com.github.influxdata.influxdb.services.storage.Node_Comparison,oneof"`
}

func (*Node_StringValue) isNode_Value()   {}
func (*Node_BooleanValue) isNode_Value()  {}
func (*Node_IntegerValue) isNode_Value()  {}
func (*Node_UnsignedValue) isNode_Value() {}
func (*Node_FloatValue) isNode_Value()    {}
func (*Node_RegexValue) isNode_Value()    {}
func (*Node_TagRefValue) isNode_Value()   {}
func (*Node_FieldRefValue) isNode_Value() {}
func (*Node_Logical_) isNode_Value()      {}
func (*Node_Comparison_) isNode_Value()   {}

func (m *Node) GetValue() isNode_Value {
	if m != nil {
		return m.Value
	}
	return nil
}

func (m *Node) GetNodeType() Node_Type {
	if m != nil {
		return m.NodeType
	}
	return NodeTypeLogicalExpression
}

func (m *Node) GetChildren() []*Node {
	if m != nil {
		return m.Children
	}
	return nil
}

func (m *Node) GetStringValue() string {
	if x, ok := m.GetValue().(*Node_StringValue); ok {
		return x.StringValue
	}
	return ""
}

func (m *Node) GetBooleanValue() bool {
	if x, ok := m.GetValue().(*Node_BooleanValue); ok {
		return x.BooleanValue
	}
	return false
}

func (m *Node) GetIntegerValue() int64 {
	if x, ok := m.GetValue().(*Node_IntegerValue); ok {
		return x.IntegerValue
	}
	return 0
}

func (m *Node) GetUnsignedValue() uint64 {
	if x, ok := m.GetValue().(*Node_UnsignedValue); ok {
		return x.UnsignedValue
	}
	return 0
}

func (m *Node) GetFloatValue() float64 {
	if x, ok := m.GetValue().(*Node_FloatValue); ok {
		return x.FloatValue
	}
	return 0
}

func (m *Node) GetRegexValue() string {
	if x, ok := m.GetValue().(*Node_RegexValue); ok {
		return x.RegexValue
	}
	return ""
}

func (m *Node) GetTagRefValue() string {
	if x, ok := m.GetValue().(*Node_TagRefValue); ok {
		return x.TagRefValue
	}
	return ""
}

func (m *Node) GetFieldRefValue() string {
	if x, ok := m.GetValue().(*Node_FieldRefValue); ok {
		return x.FieldRefValue
	}
	return ""
}

func (m *Node) GetLogical() Node_Logical {
	if x, ok := m.GetValue().(*Node_Logical_); ok {
		return x.Logical
	}
	return LogicalAnd
}

func (m *Node) GetComparison() Node_Comparison {
	if x, ok := m.GetValue().(*Node_Comparison_); ok {
		return x.Comparison
	}
	return ComparisonEqual
}

// XXX_OneofFuncs is for the internal use of the proto package.
func (*Node) XXX_OneofFuncs() (func(msg proto.Message, b *proto.Buffer) error, func(msg proto.Message, tag, wire int, b *proto.Buffer) (bool, error), func(msg proto.Message) (n int), []interface{}) {
	return _Node_OneofMarshaler, _Node_OneofUnmarshaler, _Node_OneofSizer, []interface{}{
		(*Node_StringValue)(nil),
		(*Node_BooleanValue)(nil),
		(*Node_IntegerValue)(nil),
		(*Node_UnsignedValue)(nil),
		(*Node_FloatValue)(nil),
		(*Node_RegexValue)(nil),
		(*Node_TagRefValue)(nil),
		(*Node_FieldRefValue)(nil),
		(*Node_Logical_)(nil),
		(*Node_Comparison_)(nil),
	}
}

func _Node_OneofMarshaler(msg proto.Message, b *proto.Buffer) error {
	m := msg.(*Node)
	// value
	switch x := m.Value.(type) {
	case *Node_StringValue:
		_ = b.EncodeVarint(3<<3 | proto.WireBytes)
		_ = b.EncodeStringBytes(x.StringValue)
	case *Node_BooleanValue:
		t := uint64(0)
		if x.BooleanValue {
			t = 1
		}
		_ = b.EncodeVarint(4<<3 | proto.WireVarint)
		_ = b.EncodeVarint(t)
	case *Node_IntegerValue:
		_ = b.EncodeVarint(5<<3 | proto.WireVarint)
		_ = b.EncodeVarint(uint64(x.IntegerValue))
	case *Node_UnsignedValue:
		_ = b.EncodeVarint(6<<3 | proto.WireVarint)
		_ = b.EncodeVarint(uint64(x.UnsignedValue))
	case *Node_FloatValue:
		_ = b.EncodeVarint(7<<3 | proto.WireFixed64)
		_ = b.EncodeFixed64(math.Float64bits(x.FloatValue))
	case *Node_RegexValue:
		_ = b.EncodeVarint(8<<3 | proto.WireBytes)
		_ = b.EncodeStringBytes(x.RegexValue)
	case *Node_TagRefValue:
		_ = b.EncodeVarint(9<<3 | proto.WireBytes)
		_ = b.EncodeStringBytes(x.TagRefValue)
	case *Node_FieldRefValue:
		_ = b.EncodeVarint(10<<3 | proto.WireBytes)
		_ = b.EncodeStringBytes(x.FieldRefValue)
	case *Node_Logical_:
		_ = b.EncodeVarint(11<<3 | proto.WireVarint)
		_ = b.EncodeVarint(uint64(x.Logical))
	case *Node_Comparison_:
		_ = b.EncodeVarint(12<<3 | proto.WireVarint)
		_ = b.EncodeVarint(uint64(x.Comparison))
	case nil:
	default:
		return fmt.Errorf("Node.Value has unexpected type %T", x)
	}
	return nil
}

func _Node_OneofUnmarshaler(msg proto.Message, tag, wire int, b *proto.Buffer) (bool, error) {
	m := msg.(*Node)
	switch tag {
	case 3: // value.string_value
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		x, err := b.DecodeStringBytes()
		m.Value = &Node_StringValue{x}
		return true, err
	case 4: // value.bool_value
		if wire != proto.WireVarint {
			return true, proto.ErrInternalBadWireType
		}
		x, err := b.DecodeVarint()
		m.Value = &Node_BooleanValue{x != 0}
		return true, err
	case 5: // value.int_value
		if wire != proto.WireVarint {
			return true, proto.ErrInternalBadWireType
		}
		x, err := b.DecodeVarint()
		m.Value = &Node_IntegerValue{int64(x)}
		return true, err
	case 6: // value.uint_value
		if wire != proto.WireVarint {
			return true, proto.ErrInternalBadWireType
		}
		x, err := b.DecodeVarint()
		m.Value = &Node_UnsignedValue{x}
		return true, err
	case 7: // value.float_value
		if wire != proto.WireFixed64 {
			return true, proto.ErrInternalBadWireType
		}
		x, err := b.DecodeFixed64()
		m.Value = &Node_FloatValue{math.Float64frombits(x)}
		return true, err
	case 8: // value.regex_value
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		x, err := b.DecodeStringBytes()
		m.Value = &Node_RegexValue{x}
		return true, err
	case 9: // value.tag_ref_value
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		x, err := b.DecodeStringBytes()
		m.Value = &Node_TagRefValue{x}
		return true, err
	case 10: // value.field_ref_value
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		x, err := b.DecodeStringBytes()
		m.Value = &Node_FieldRefValue{x}
		return true, err
	case 11: // value.logical
		if wire != proto.WireVarint {
			return true, proto.ErrInternalBadWireType
		}
		x, err := b.DecodeVarint()
		m.Value = &Node_Logical_{Node_Logical(x)}
		return true, err
	case 12: // value.comparison
		if wire != proto.WireVarint {
			return true, proto.ErrInternalBadWireType
		}
		x, err := b.DecodeVarint()
		m.Value = &Node_Comparison_{Node_Comparison(x)}
		return true, err
	default:
		return false, nil
	}
}

func _Node_OneofSizer(msg proto.Message) (n int) {
	m := msg.(*Node)
	// value
	switch x := m.Value.(type) {
	case *Node_StringValue:
		n += proto.SizeVarint(3<<3 | proto.WireBytes)
		n += proto.SizeVarint(uint64(len(x.StringValue)))
		n += len(x.StringValue)
	case *Node_BooleanValue:
		n += proto.SizeVarint(4<<3 | proto.WireVarint)
		n += 1
	case *Node_IntegerValue:
		n += proto.SizeVarint(5<<3 | proto.WireVarint)
		n += proto.SizeVarint(uint64(x.IntegerValue))
	case *Node_UnsignedValue:
		n += proto.SizeVarint(6<<3 | proto.WireVarint)
		n += proto.SizeVarint(uint64(x.UnsignedValue))
	case *Node_FloatValue:
		n += proto.SizeVarint(7<<3 | proto.WireFixed64)
		n += 8
	case *Node_RegexValue:
		n += proto.SizeVarint(8<<3 | proto.WireBytes)
		n += proto.SizeVarint(uint64(len(x.RegexValue)))
		n += len(x.RegexValue)
	case *Node_TagRefValue:
		n += proto.SizeVarint(9<<3 | proto.WireBytes)
		n += proto.SizeVarint(uint64(len(x.TagRefValue)))
		n += len(x.TagRefValue)
	case *Node_FieldRefValue:
		n += proto.SizeVarint(10<<3 | proto.WireBytes)
		n += proto.SizeVarint(uint64(len(x.FieldRefValue)))
		n += len(x.FieldRefValue)
	case *Node_Logical_:
		n += proto.SizeVarint(11<<3 | proto.WireVarint)
		n += proto.SizeVarint(uint64(x.Logical))
	case *Node_Comparison_:
		n += proto.SizeVarint(12<<3 | proto.WireVarint)
		n += proto.SizeVarint(uint64(x.Comparison))
	case nil:
	default:
		panic(fmt.Sprintf("proto: unexpected type %T in oneof", x))
	}
	return n
}

type Predicate struct {
	Root *Node `protobuf:"bytes,1,opt,name=root" json:"root,omitempty"`
}

func (m *Predicate) Reset()                    { *m = Predicate{} }
func (m *Predicate) String() string            { return proto.CompactTextString(m) }
func (*Predicate) ProtoMessage()               {}
func (*Predicate) Descriptor() ([]byte, []int) { return fileDescriptorPredicate, []int{1} }

func (m *Predicate) GetRoot() *Node {
	if m != nil {
		return m.Root
	}
	return nil
}

func init() {
	proto.RegisterType((*Node)(nil), "com.github.influxdata.influxdb.services.storage.Node")
	proto.RegisterType((*Predicate)(nil), "com.github.influxdata.influxdb.services.storage.Predicate")
	proto.RegisterEnum("com.github.influxdata.influxdb.services.storage.Node_Type", Node_Type_name, Node_Type_value)
	proto.RegisterEnum("com.github.influxdata.influxdb.services.storage.Node_Comparison", Node_Comparison_name, Node_Comparison_value)
	proto.RegisterEnum("com.github.influxdata.influxdb.services.storage.Node_Logical", Node_Logical_name, Node_Logical_value)
}
func (m *Node) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Node) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.NodeType != 0 {
		dAtA[i] = 0x8
		i++
		i = encodeVarintPredicate(dAtA, i, uint64(m.NodeType))
	}
	if len(m.Children) > 0 {
		for _, msg := range m.Children {
			dAtA[i] = 0x12
			i++
			i = encodeVarintPredicate(dAtA, i, uint64(msg.Size()))
			n, err := msg.MarshalTo(dAtA[i:])
			if err != nil {
				return 0, err
			}
			i += n
		}
	}
	if m.Value != nil {
		nn1, err := m.Value.MarshalTo(dAtA[i:])
		if err != nil {
			return 0, err
		}
		i += nn1
	}
	return i, nil
}

func (m *Node_StringValue) MarshalTo(dAtA []byte) (int, error) {
	i := 0
	dAtA[i] = 0x1a
	i++
	i = encodeVarintPredicate(dAtA, i, uint64(len(m.StringValue)))
	i += copy(dAtA[i:], m.StringValue)
	return i, nil
}
func (m *Node_BooleanValue) MarshalTo(dAtA []byte) (int, error) {
	i := 0
	dAtA[i] = 0x20
	i++
	if m.BooleanValue {
		dAtA[i] = 1
	} else {
		dAtA[i] = 0
	}
	i++
	return i, nil
}
func (m *Node_IntegerValue) MarshalTo(dAtA []byte) (int, error) {
	i := 0
	dAtA[i] = 0x28
	i++
	i = encodeVarintPredicate(dAtA, i, uint64(m.IntegerValue))
	return i, nil
}
func (m *Node_UnsignedValue) MarshalTo(dAtA []byte) (int, error) {
	i := 0
	dAtA[i] = 0x30
	i++
	i = encodeVarintPredicate(dAtA, i, uint64(m.UnsignedValue))
	return i, nil
}
func (m *Node_FloatValue) MarshalTo(dAtA []byte) (int, error) {
	i := 0
	dAtA[i] = 0x39
	i++
	i = encodeFixed64Predicate(dAtA, i, uint64(math.Float64bits(float64(m.FloatValue))))
	return i, nil
}
func (m *Node_RegexValue) MarshalTo(dAtA []byte) (int, error) {
	i := 0
	dAtA[i] = 0x42
	i++
	i = encodeVarintPredicate(dAtA, i, uint64(len(m.RegexValue)))
	i += copy(dAtA[i:], m.RegexValue)
	return i, nil
}
func (m *Node_TagRefValue) MarshalTo(dAtA []byte) (int, error) {
	i := 0
	dAtA[i] = 0x4a
	i++
	i = encodeVarintPredicate(dAtA, i, uint64(len(m.TagRefValue)))
	i += copy(dAtA[i:], m.TagRefValue)
	return i, nil
}
func (m *Node_FieldRefValue) MarshalTo(dAtA []byte) (int, error) {
	i := 0
	dAtA[i] = 0x52
	i++
	i = encodeVarintPredicate(dAtA, i, uint64(len(m.FieldRefValue)))
	i += copy(dAtA[i:], m.FieldRefValue)
	return i, nil
}
func (m *Node_Logical_) MarshalTo(dAtA []byte) (int, error) {
	i := 0
	dAtA[i] = 0x58
	i++
	i = encodeVarintPredicate(dAtA, i, uint64(m.Logical))
	return i, nil
}
func (m *Node_Comparison_) MarshalTo(dAtA []byte) (int, error) {
	i := 0
	dAtA[i] = 0x60
	i++
	i = encodeVarintPredicate(dAtA, i, uint64(m.Comparison))
	return i, nil
}
func (m *Predicate) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Predicate) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.Root != nil {
		dAtA[i] = 0xa
		i++
		i = encodeVarintPredicate(dAtA, i, uint64(m.Root.Size()))
		n2, err := m.Root.MarshalTo(dAtA[i:])
		if err != nil {
			return 0, err
		}
		i += n2
	}
	return i, nil
}

func encodeFixed64Predicate(dAtA []byte, offset int, v uint64) int {
	dAtA[offset] = uint8(v)
	dAtA[offset+1] = uint8(v >> 8)
	dAtA[offset+2] = uint8(v >> 16)
	dAtA[offset+3] = uint8(v >> 24)
	dAtA[offset+4] = uint8(v >> 32)
	dAtA[offset+5] = uint8(v >> 40)
	dAtA[offset+6] = uint8(v >> 48)
	dAtA[offset+7] = uint8(v >> 56)
	return offset + 8
}
func encodeFixed32Predicate(dAtA []byte, offset int, v uint32) int {
	dAtA[offset] = uint8(v)
	dAtA[offset+1] = uint8(v >> 8)
	dAtA[offset+2] = uint8(v >> 16)
	dAtA[offset+3] = uint8(v >> 24)
	return offset + 4
}
func encodeVarintPredicate(dAtA []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return offset + 1
}
func (m *Node) Size() (n int) {
	var l int
	_ = l
	if m.NodeType != 0 {
		n += 1 + sovPredicate(uint64(m.NodeType))
	}
	if len(m.Children) > 0 {
		for _, e := range m.Children {
			l = e.Size()
			n += 1 + l + sovPredicate(uint64(l))
		}
	}
	if m.Value != nil {
		n += m.Value.Size()
	}
	return n
}

func (m *Node_StringValue) Size() (n int) {
	var l int
	_ = l
	l = len(m.StringValue)
	n += 1 + l + sovPredicate(uint64(l))
	return n
}
func (m *Node_BooleanValue) Size() (n int) {
	var l int
	_ = l
	n += 2
	return n
}
func (m *Node_IntegerValue) Size() (n int) {
	var l int
	_ = l
	n += 1 + sovPredicate(uint64(m.IntegerValue))
	return n
}
func (m *Node_UnsignedValue) Size() (n int) {
	var l int
	_ = l
	n += 1 + sovPredicate(uint64(m.UnsignedValue))
	return n
}
func (m *Node_FloatValue) Size() (n int) {
	var l int
	_ = l
	n += 9
	return n
}
func (m *Node_RegexValue) Size() (n int) {
	var l int
	_ = l
	l = len(m.RegexValue)
	n += 1 + l + sovPredicate(uint64(l))
	return n
}
func (m *Node_TagRefValue) Size() (n int) {
	var l int
	_ = l
	l = len(m.TagRefValue)
	n += 1 + l + sovPredicate(uint64(l))
	return n
}
func (m *Node_FieldRefValue) Size() (n int) {
	var l int
	_ = l
	l = len(m.FieldRefValue)
	n += 1 + l + sovPredicate(uint64(l))
	return n
}
func (m *Node_Logical_) Size() (n int) {
	var l int
	_ = l
	n += 1 + sovPredicate(uint64(m.Logical))
	return n
}
func (m *Node_Comparison_) Size() (n int) {
	var l int
	_ = l
	n += 1 + sovPredicate(uint64(m.Comparison))
	return n
}
func (m *Predicate) Size() (n int) {
	var l int
	_ = l
	if m.Root != nil {
		l = m.Root.Size()
		n += 1 + l + sovPredicate(uint64(l))
	}
	return n
}

func sovPredicate(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
func sozPredicate(x uint64) (n int) {
	return sovPredicate(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *Node) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowPredicate
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Node: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Node: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field NodeType", wireType)
			}
			m.NodeType = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPredicate
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.NodeType |= (Node_Type(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Children", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPredicate
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthPredicate
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Children = append(m.Children, &Node{})
			if err := m.Children[len(m.Children)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field StringValue", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPredicate
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthPredicate
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Value = &Node_StringValue{string(dAtA[iNdEx:postIndex])}
			iNdEx = postIndex
		case 4:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field BooleanValue", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPredicate
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				v |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			b := bool(v != 0)
			m.Value = &Node_BooleanValue{b}
		case 5:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field IntegerValue", wireType)
			}
			var v int64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPredicate
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				v |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Value = &Node_IntegerValue{v}
		case 6:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field UnsignedValue", wireType)
			}
			var v uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPredicate
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				v |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Value = &Node_UnsignedValue{v}
		case 7:
			if wireType != 1 {
				return fmt.Errorf("proto: wrong wireType = %d for field FloatValue", wireType)
			}
			var v uint64
			if (iNdEx + 8) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += 8
			v = uint64(dAtA[iNdEx-8])
			v |= uint64(dAtA[iNdEx-7]) << 8
			v |= uint64(dAtA[iNdEx-6]) << 16
			v |= uint64(dAtA[iNdEx-5]) << 24
			v |= uint64(dAtA[iNdEx-4]) << 32
			v |= uint64(dAtA[iNdEx-3]) << 40
			v |= uint64(dAtA[iNdEx-2]) << 48
			v |= uint64(dAtA[iNdEx-1]) << 56
			m.Value = &Node_FloatValue{float64(math.Float64frombits(v))}
		case 8:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field RegexValue", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPredicate
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthPredicate
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Value = &Node_RegexValue{string(dAtA[iNdEx:postIndex])}
			iNdEx = postIndex
		case 9:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field TagRefValue", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPredicate
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthPredicate
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Value = &Node_TagRefValue{string(dAtA[iNdEx:postIndex])}
			iNdEx = postIndex
		case 10:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field FieldRefValue", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPredicate
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthPredicate
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Value = &Node_FieldRefValue{string(dAtA[iNdEx:postIndex])}
			iNdEx = postIndex
		case 11:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Logical", wireType)
			}
			var v Node_Logical
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPredicate
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				v |= (Node_Logical(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Value = &Node_Logical_{v}
		case 12:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Comparison", wireType)
			}
			var v Node_Comparison
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPredicate
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				v |= (Node_Comparison(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Value = &Node_Comparison_{v}
		default:
			iNdEx = preIndex
			skippy, err := skipPredicate(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthPredicate
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *Predicate) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowPredicate
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Predicate: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Predicate: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Root", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPredicate
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthPredicate
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Root == nil {
				m.Root = &Node{}
			}
			if err := m.Root.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipPredicate(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthPredicate
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipPredicate(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowPredicate
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowPredicate
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
			return iNdEx, nil
		case 1:
			iNdEx += 8
			return iNdEx, nil
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowPredicate
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			iNdEx += length
			if length < 0 {
				return 0, ErrInvalidLengthPredicate
			}
			return iNdEx, nil
		case 3:
			for {
				var innerWire uint64
				var start int = iNdEx
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return 0, ErrIntOverflowPredicate
					}
					if iNdEx >= l {
						return 0, io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					innerWire |= (uint64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				innerWireType := int(innerWire & 0x7)
				if innerWireType == 4 {
					break
				}
				next, err := skipPredicate(dAtA[start:])
				if err != nil {
					return 0, err
				}
				iNdEx = start + next
			}
			return iNdEx, nil
		case 4:
			return iNdEx, nil
		case 5:
			iNdEx += 4
			return iNdEx, nil
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
	}
	panic("unreachable")
}

var (
	ErrInvalidLengthPredicate = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowPredicate   = fmt.Errorf("proto: integer overflow")
)

func init() { proto.RegisterFile("predicate.proto", fileDescriptorPredicate) }

var fileDescriptorPredicate = []byte{
	// 883 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x9c, 0x95, 0xcb, 0x6e, 0xdb, 0x46,
	0x14, 0x86, 0x45, 0x5d, 0x2c, 0xe9, 0xc8, 0x17, 0x66, 0x12, 0xc7, 0x0a, 0xdb, 0x48, 0x03, 0x07,
	0x05, 0xd4, 0x45, 0x65, 0xd8, 0xad, 0x37, 0x0d, 0x8a, 0x56, 0x72, 0x68, 0x59, 0x00, 0x2b, 0x29,
	0x14, 0x73, 0x69, 0x37, 0x02, 0x25, 0x8d, 0x68, 0x02, 0x34, 0x47, 0x19, 0x8e, 0x02, 0xe7, 0x0d,
	0x0a, 0xae, 0xba, 0x2f, 0xb8, 0xea, 0xcb, 0x74, 0x53, 0xa0, 0x4f, 0x20, 0x14, 0xea, 0xae, 0x7d,
	0x89, 0x82, 0xc3, 0x9b, 0xdc, 0x76, 0x53, 0xef, 0xe6, 0x9c, 0xf9, 0xbf, 0xff, 0xcc, 0x1c, 0x1e,
	0x92, 0x70, 0xb0, 0x64, 0x64, 0x6e, 0xcf, 0x4c, 0x4e, 0xda, 0x4b, 0x46, 0x39, 0x45, 0x27, 0x33,
	0x7a, 0xd3, 0xb6, 0x6c, 0x7e, 0xbd, 0x9a, 0xb6, 0x6d, 0x77, 0xe1, 0xac, 0x6e, 0xe7, 0x26, 0x37,
	0x93, 0xe5, 0xb4, 0xed, 0x11, 0xf6, 0xde, 0x9e, 0x11, 0xaf, 0xed, 0x71, 0xca, 0x4c, 0x8b, 0x28,
	0x9f, 0xc5, 0xe2, 0x19, 0xbd, 0x39, 0xb1, 0xa8, 0x45, 0x4f, 0x84, 0xcf, 0x74, 0xb5, 0x10, 0x91,
	0x08, 0xc4, 0x2a, 0xf2, 0x3f, 0xfe, 0xab, 0x06, 0xc5, 0x01, 0x9d, 0x13, 0xf4, 0x0e, 0xaa, 0x2e,
	0x9d, 0x93, 0x09, 0xff, 0xb0, 0x24, 0x75, 0x09, 0x4b, 0xad, 0xfd, 0xb3, 0x2f, 0xdb, 0xff, 0xb3,
	0x78, 0x3b, 0x74, 0x6a, 0x1b, 0x1f, 0x96, 0xa4, 0x5b, 0xdf, 0xac, 0x9b, 0x95, 0x30, 0x0c, 0xa3,
	0x3f, 0xd7, 0xcd, 0x8a, 0x1b, 0xaf, 0xf5, 0x74, 0x85, 0x5e, 0x42, 0x65, 0x76, 0x6d, 0x3b, 0x73,
	0x46, 0xdc, 0x7a, 0x1e, 0x17, 0x5a, 0xb5, 0xb3, 0xf3, 0x7b, 0x55, 0xd4, 0x53, 0x1b, 0xf4, 0x05,
	0xec, 0x7a, 0x9c, 0xd9, 0xae, 0x35, 0x79, 0x6f, 0x3a, 0x2b, 0x52, 0x2f, 0x60, 0xa9, 0x55, 0xed,
	0x1e, 0x6c, 0xd6, 0xcd, 0xda, 0x58, 0xe4, 0x5f, 0x87, 0xe9, 0xab, 0x9c, 0x5e, 0xf3, 0xb2, 0x10,
	0x9d, 0x02, 0x4c, 0x29, 0x75, 0x62, 0xa6, 0x88, 0xa5, 0x56, 0xa5, 0x2b, 0x6f, 0xd6, 0xcd, 0xdd,
	0x2e, 0xa5, 0x0e, 0x31, 0xdd, 0x04, 0xaa, 0x86, 0xaa, 0x08, 0x39, 0x81, 0xaa, 0xed, 0xf2, 0x98,
	0x28, 0x61, 0xa9, 0x55, 0x88, 0x88, 0xbe, 0xcb, 0x89, 0x45, 0x58, 0x42, 0x54, 0x6c, 0x97, 0x47,
	0xc0, 0x19, 0xc0, 0x2a, 0x23, 0x76, 0xb0, 0xd4, 0x2a, 0x76, 0x1f, 0x6c, 0xd6, 0xcd, 0xbd, 0x57,
	0xae, 0x67, 0x5b, 0x2e, 0x99, 0xa7, 0x45, 0x56, 0x29, 0x73, 0x0a, 0xb5, 0x85, 0x43, 0xcd, 0x04,
	0x2a, 0x63, 0xa9, 0x25, 0x75, 0xf7, 0x37, 0xeb, 0x26, 0x5c, 0x86, 0xe9, 0x84, 0x80, 0x45, 0x1a,
	0x85, 0x08, 0x23, 0x16, 0xb9, 0x8d, 0x91, 0x8a, 0xb8, 0xbf, 0x40, 0xf4, 0x30, 0x9d, 0x22, 0x2c,
	0x8d, 0xd0, 0x39, 0xec, 0x71, 0xd3, 0x9a, 0x30, 0xb2, 0x88, 0xa1, 0x6a, 0xd6, 0x34, 0xc3, 0xb4,
	0x74, 0xb2, 0x48, 0x9b, 0xc6, 0xb3, 0x10, 0x3d, 0x87, 0x83, 0x85, 0x4d, 0x9c, 0xf9, 0x16, 0x08,
	0x02, 0x14, 0xb7, 0xba, 0x0c, 0xb7, 0xb6, 0xd0, 0xbd, 0xc5, 0x76, 0x02, 0x7d, 0x07, 0x65, 0x87,
	0x5a, 0xf6, 0xcc, 0x74, 0xea, 0x35, 0x31, 0x6b, 0x5f, 0xdd, 0x6f, 0xd6, 0xb4, 0xc8, 0xe4, 0x2a,
	0xa7, 0x27, 0x7e, 0x68, 0x0a, 0x30, 0xa3, 0x37, 0x4b, 0x93, 0xd9, 0x1e, 0x75, 0xeb, 0xbb, 0xc2,
	0xfd, 0x9b, 0xfb, 0xb9, 0x5f, 0xa4, 0x3e, 0x61, 0xcb, 0x32, 0xd7, 0xe3, 0x9f, 0xf2, 0x50, 0x14,
	0x23, 0x7c, 0x0e, 0x48, 0x1b, 0xf6, 0xfa, 0x17, 0x1d, 0x6d, 0xa2, 0xbe, 0x1d, 0xe9, 0xea, 0x78,
	0xdc, 0x1f, 0x0e, 0xe4, 0x9c, 0xf2, 0xd4, 0x0f, 0xf0, 0x93, 0x64, 0xfc, 0xe3, 0x43, 0xaa, 0xb7,
	0x4b, 0x46, 0x3c, 0xcf, 0xa6, 0x2e, 0x7a, 0x0e, 0x87, 0x17, 0xc3, 0x6f, 0x47, 0x1d, 0xbd, 0x3f,
	0x1e, 0x0e, 0xb6, 0x49, 0x49, 0xc1, 0x7e, 0x80, 0x3f, 0x4e, 0xc8, 0xec, 0x00, 0x5b, 0xf0, 0x29,
	0xc8, 0xa3, 0x8e, 0xae, 0xde, 0xe1, 0xf2, 0xca, 0x47, 0x7e, 0x80, 0x8f, 0x12, 0x6e, 0x64, 0x32,
	0xb2, 0x8d, 0x34, 0xa1, 0x6c, 0x74, 0x7a, 0x13, 0x5d, 0xbd, 0x94, 0x0b, 0x0a, 0xf2, 0x03, 0xbc,
	0x9f, 0x28, 0xa3, 0x07, 0x8c, 0x30, 0x94, 0xb5, 0xbe, 0xa1, 0xea, 0x1d, 0x4d, 0x2e, 0x2a, 0x0f,
	0xfd, 0x00, 0x1f, 0xa4, 0x87, 0xb7, 0x39, 0x61, 0xa6, 0x83, 0x9e, 0x41, 0xf5, 0xb2, 0xaf, 0x6a,
	0x2f, 0x84, 0x49, 0x49, 0x79, 0xe4, 0x07, 0x58, 0x4e, 0x34, 0xc9, 0xc3, 0x56, 0x8a, 0x3f, 0xfc,
	0xdc, 0xc8, 0x1d, 0xff, 0x9a, 0x07, 0xc8, 0x4e, 0x8e, 0x1a, 0x50, 0x52, 0x5f, 0xbe, 0xea, 0x68,
	0x72, 0x2e, 0x72, 0xde, 0xba, 0xd4, 0xbb, 0x95, 0xe9, 0xa0, 0x4f, 0xa0, 0x3a, 0x18, 0x1a, 0x93,
	0x48, 0x23, 0x29, 0x8f, 0xfd, 0x00, 0xa3, 0x4c, 0x33, 0xa0, 0x3c, 0x92, 0x7d, 0x0a, 0xb5, 0xb1,
	0xd1, 0xd1, 0x8d, 0xf1, 0xe4, 0x4d, 0xdf, 0xb8, 0x92, 0xf3, 0x4a, 0xdd, 0x0f, 0xf0, 0xa3, 0x4c,
	0x38, 0xe6, 0x26, 0xe3, 0xde, 0x1b, 0x9b, 0x5f, 0x87, 0x15, 0x75, 0xb5, 0xa7, 0xbe, 0x95, 0x0b,
	0xff, 0xac, 0x28, 0x5e, 0x82, 0xa4, 0x62, 0xa4, 0x29, 0xfe, 0x47, 0xc5, 0x48, 0xa6, 0x40, 0x5e,
	0x33, 0xe4, 0x52, 0xd4, 0xb0, 0x6c, 0x5f, 0x23, 0x9e, 0x87, 0x30, 0x14, 0x34, 0x43, 0x95, 0x77,
	0x94, 0x23, 0x3f, 0xc0, 0x0f, 0xef, 0x6e, 0x46, 0xe7, 0x7d, 0x0a, 0xf9, 0x9e, 0x21, 0x97, 0x95,
	0x43, 0x3f, 0xc0, 0x0f, 0x32, 0x41, 0x8f, 0x11, 0x93, 0x13, 0x86, 0x9e, 0x41, 0xa1, 0x67, 0xa8,
	0x72, 0x45, 0x51, 0xfc, 0x00, 0x3f, 0xfe, 0xd7, 0xbe, 0xf0, 0x88, 0xfb, 0xf9, 0x35, 0x94, 0xe3,
	0x11, 0x42, 0x47, 0x50, 0xe8, 0x0c, 0x5e, 0xc8, 0x39, 0x65, 0xdf, 0x0f, 0x30, 0xc4, 0xd9, 0x8e,
	0x3b, 0x47, 0x87, 0x90, 0x1f, 0xea, 0xb2, 0xa4, 0xec, 0xf9, 0x01, 0xae, 0xc6, 0xf9, 0x21, 0x8b,
	0x0c, 0xba, 0x65, 0x28, 0x89, 0x17, 0xf4, 0xf8, 0x35, 0x54, 0x47, 0xc9, 0x0f, 0x06, 0xf5, 0xa1,
	0xc8, 0x28, 0xe5, 0xe2, 0x63, 0x7f, 0xef, 0x4f, 0xaf, 0xb0, 0xe8, 0x3e, 0xf9, 0x65, 0xd3, 0x90,
	0x7e, 0xdb, 0x34, 0xa4, 0xdf, 0x37, 0x0d, 0xe9, 0xc7, 0x3f, 0x1a, 0xb9, 0xef, 0xcb, 0xb1, 0x6a,
	0xba, 0x23, 0xfe, 0x33, 0x9f, 0xff, 0x1d, 0x00, 0x00, 0xff, 0xff, 0x37, 0x3b, 0xbc, 0x43, 0xda,
	0x06, 0x00, 0x00,
}

var measurementRemap = map[string]string{measurementKey: "_name"}

// NodeToExpr transforms a predicate node to an influxql.Expr.
func NodeToExpr(node *Node, remap map[string]string) (influxql.Expr, error) {
	v := &nodeToExprVisitor{remap: remap}
	WalkNode(v, node)
	if err := v.Err(); err != nil {
		return nil, err
	}

	if len(v.exprs) > 1 {
		return nil, errors.New("invalid expression")
	}

	if len(v.exprs) == 0 {
		return nil, nil
	}

	// TODO(edd): It would be preferable if RewriteRegexConditions was a
	// package level function in influxql.
	stmt := &influxql.SelectStatement{
		Condition: v.exprs[0],
	}
	stmt.RewriteRegexConditions()
	return stmt.Condition, nil
}

type nodeToExprVisitor struct {
	remap map[string]string
	exprs []influxql.Expr
	err   error
}

func (v *nodeToExprVisitor) Visit(n *Node) NodeVisitor {
	if v.err != nil {
		return nil
	}

	switch n.NodeType {
	case NodeTypeLogicalExpression:
		if len(n.Children) > 1 {
			op := influxql.AND
			if n.GetLogical() == LogicalOr {
				op = influxql.OR
			}

			WalkNode(v, n.Children[0])
			if v.err != nil {
				return nil
			}

			for i := 1; i < len(n.Children); i++ {
				WalkNode(v, n.Children[i])
				if v.err != nil {
					return nil
				}

				if len(v.exprs) >= 2 {
					lhs, rhs := v.pop2()
					v.exprs = append(v.exprs, &influxql.BinaryExpr{LHS: lhs, Op: op, RHS: rhs})
				}
			}

			return nil
		}

	case NodeTypeParenExpression:
		if len(n.Children) != 1 {
			v.err = errors.New("ParenExpression expects one child")
			return nil
		}

		WalkNode(v, n.Children[0])
		if v.err != nil {
			return nil
		}

		if len(v.exprs) > 0 {
			v.exprs = append(v.exprs, &influxql.ParenExpr{Expr: v.pop()})
		}

		return nil

	case NodeTypeComparisonExpression:
		walkChildren(v, n)

		if len(v.exprs) < 2 {
			v.err = errors.New("ComparisonExpression expects two children")
			return nil
		}

		lhs, rhs := v.pop2()

		be := &influxql.BinaryExpr{LHS: lhs, RHS: rhs}
		switch n.GetComparison() {
		case ComparisonEqual:
			be.Op = influxql.EQ
		case ComparisonNotEqual:
			be.Op = influxql.NEQ
		case ComparisonStartsWith:
			// TODO(sgc): rewrite to anchored RE, as index does not support startsWith yet
			v.err = errors.New("startsWith not implemented")
			return nil
		case ComparisonRegex:
			be.Op = influxql.EQREGEX
		case ComparisonNotRegex:
			be.Op = influxql.NEQREGEX
		case ComparisonLess:
			be.Op = influxql.LT
		case ComparisonLessEqual:
			be.Op = influxql.LTE
		case ComparisonGreater:
			be.Op = influxql.GT
		case ComparisonGreaterEqual:
			be.Op = influxql.GTE
		default:
			v.err = errors.New("invalid comparison operator")
			return nil
		}

		v.exprs = append(v.exprs, be)

		return nil

	case NodeTypeTagRef:
		ref := n.GetTagRefValue()
		if v.remap != nil {
			if nk, ok := v.remap[ref]; ok {
				ref = nk
			}
		}

		v.exprs = append(v.exprs, &influxql.VarRef{Val: ref})
		return nil

	case NodeTypeFieldRef:
		v.exprs = append(v.exprs, &influxql.VarRef{Val: "$"})
		return nil

	case NodeTypeLiteral:
		switch val := n.Value.(type) {
		case *Node_StringValue:
			v.exprs = append(v.exprs, &influxql.StringLiteral{Val: val.StringValue})

		case *Node_RegexValue:
			// TODO(sgc): consider hashing the RegexValue and cache compiled version
			re, err := regexp.Compile(val.RegexValue)
			if err != nil {
				v.err = err
			}
			v.exprs = append(v.exprs, &influxql.RegexLiteral{Val: re})
			return nil

		case *Node_IntegerValue:
			v.exprs = append(v.exprs, &influxql.IntegerLiteral{Val: val.IntegerValue})

		case *Node_UnsignedValue:
			v.exprs = append(v.exprs, &influxql.UnsignedLiteral{Val: val.UnsignedValue})

		case *Node_FloatValue:
			v.exprs = append(v.exprs, &influxql.NumberLiteral{Val: val.FloatValue})

		case *Node_BooleanValue:
			v.exprs = append(v.exprs, &influxql.BooleanLiteral{Val: val.BooleanValue})

		default:
			v.err = errors.New("unexpected literal type")
			return nil
		}

		return nil

	default:
		return v
	}
	return nil
}

func (v *nodeToExprVisitor) Err() error {
	return v.err
}

func (v *nodeToExprVisitor) pop() influxql.Expr {
	if len(v.exprs) == 0 {
		panic("stack empty")
	}

	var top influxql.Expr
	top, v.exprs = v.exprs[len(v.exprs)-1], v.exprs[:len(v.exprs)-1]
	return top
}

func (v *nodeToExprVisitor) pop2() (influxql.Expr, influxql.Expr) {
	if len(v.exprs) < 2 {
		panic("stack empty")
	}

	rhs := v.exprs[len(v.exprs)-1]
	lhs := v.exprs[len(v.exprs)-2]
	v.exprs = v.exprs[:len(v.exprs)-2]
	return lhs, rhs
}

// HasSingleMeasurementNoOR determines if an index optimisation is available.
//
// Typically the read service will use the query engine to retrieve all field
// keys for all measurements that match the expression, which can be very
// inefficient if it can be proved that only one measurement matches the expression.
//
// This condition is determined when the following is true:
//
//		* there is only one occurrence of the tag key `_measurement`.
//		* there are no OR operators in the expression tree.
//		* the operator for the `_measurement` binary expression is ==.
//
func HasSingleMeasurementNoOR(expr influxql.Expr) (string, bool) {
	var lastMeasurement string
	foundOnce := true
	var invalidOP bool

	influxql.WalkFunc(expr, func(node influxql.Node) {
		if !foundOnce || invalidOP {
			return
		}

		if be, ok := node.(*influxql.BinaryExpr); ok {
			if be.Op == influxql.OR {
				invalidOP = true
				return
			}

			if ref, ok := be.LHS.(*influxql.VarRef); ok {
				if ref.Val == measurementRemap[measurementKey] {
					if be.Op != influxql.EQ {
						invalidOP = true
						return
					}

					if lastMeasurement != "" {
						foundOnce = false
					}

					// Check that RHS is a literal string
					if ref, ok := be.RHS.(*influxql.StringLiteral); ok {
						lastMeasurement = ref.Val
					}
				}
			}
		}
	})
	return lastMeasurement, len(lastMeasurement) > 0 && foundOnce && !invalidOP
}

func HasFieldKeyOrValue(expr influxql.Expr) (bool, bool) {
	refs := hasRefs{refs: []string{fieldKey, "$"}, found: make([]bool, 2)}
	influxql.Walk(&refs, expr)
	return refs.found[0], refs.found[1]
}

func RewriteExprRemoveFieldKeyAndValue(expr influxql.Expr) influxql.Expr {
	return influxql.RewriteExpr(expr, func(expr influxql.Expr) influxql.Expr {
		if be, ok := expr.(*influxql.BinaryExpr); ok {
			if ref, ok := be.LHS.(*influxql.VarRef); ok {
				if ref.Val == fieldKey || ref.Val == "$" {
					return &influxql.BooleanLiteral{Val: true}
				}
			}
		}

		return expr
	})
}

func RewriteExprRemoveFieldValue(expr influxql.Expr) influxql.Expr {
	return influxql.RewriteExpr(expr, func(expr influxql.Expr) influxql.Expr {
		if be, ok := expr.(*influxql.BinaryExpr); ok {
			if ref, ok := be.LHS.(*influxql.VarRef); ok {
				if ref.Val == "$" {
					return &influxql.BooleanLiteral{Val: true}
				}
			}
		}

		return expr
	})
}

func ToStoragePredicate(f *semantic.FunctionExpression) (*Predicate, error) {
	if len(f.Params) != 1 {
		return nil, errors.New("storage predicate functions must have exactly one parameter")
	}

	root, err := toStoragePredicate(f.Body.(semantic.Expression), f.Params[0].Key.Name)
	if err != nil {
		return nil, err
	}

	return &Predicate{
		Root: root,
	}, nil
}

func toStoragePredicate(n semantic.Expression, objectName string) (*Node, error) {
	switch n := n.(type) {
	case *semantic.LogicalExpression:
		left, err := toStoragePredicate(n.Left, objectName)
		if err != nil {
			return nil, err
		}
		right, err := toStoragePredicate(n.Right, objectName)
		if err != nil {
			return nil, err
		}
		children := []*Node{left, right}
		switch n.Operator {
		case ast.AndOperator:
			return &Node{
				NodeType: NodeTypeLogicalExpression,
				Value:    &Node_Logical_{Logical: LogicalAnd},
				Children: children,
			}, nil
		case ast.OrOperator:
			return &Node{
				NodeType: NodeTypeLogicalExpression,
				Value:    &Node_Logical_{Logical: LogicalOr},
				Children: children,
			}, nil
		default:
			return nil, fmt.Errorf("unknown logical operator %v", n.Operator)
		}
	case *semantic.BinaryExpression:
		left, err := toStoragePredicate(n.Left, objectName)
		if err != nil {
			return nil, err
		}
		right, err := toStoragePredicate(n.Right, objectName)
		if err != nil {
			return nil, err
		}
		children := []*Node{left, right}
		op, err := toComparisonOperator(n.Operator)
		if err != nil {
			return nil, err
		}
		return &Node{
			NodeType: NodeTypeComparisonExpression,
			Value:    &Node_Comparison_{Comparison: op},
			Children: children,
		}, nil
	case *semantic.StringLiteral:
		return &Node{
			NodeType: NodeTypeLiteral,
			Value: &Node_StringValue{
				StringValue: n.Value,
			},
		}, nil
	case *semantic.IntegerLiteral:
		return &Node{
			NodeType: NodeTypeLiteral,
			Value: &Node_IntegerValue{
				IntegerValue: n.Value,
			},
		}, nil
	case *semantic.BooleanLiteral:
		return &Node{
			NodeType: NodeTypeLiteral,
			Value: &Node_BooleanValue{
				BooleanValue: n.Value,
			},
		}, nil
	case *semantic.FloatLiteral:
		return &Node{
			NodeType: NodeTypeLiteral,
			Value: &Node_FloatValue{
				FloatValue: n.Value,
			},
		}, nil
	case *semantic.RegexpLiteral:
		return &Node{
			NodeType: NodeTypeLiteral,
			Value: &Node_RegexValue{
				RegexValue: n.Value.String(),
			},
		}, nil
	case *semantic.MemberExpression:
		// Sanity check that the object is the objectName identifier
		if ident, ok := n.Object.(*semantic.IdentifierExpression); !ok || ident.Name != objectName {
			return nil, fmt.Errorf("unknown object %q", n.Object)
		}
		switch n.Property {
		case fieldKey:
			return &Node{
				NodeType: NodeTypeTagRef,
				Value: &Node_TagRefValue{
					TagRefValue: fieldTagKey,
				},
			}, nil
		case measurementKey:
			return &Node{
				NodeType: NodeTypeTagRef,
				Value: &Node_TagRefValue{
					TagRefValue: measurementTagKey,
				},
			}, nil
		case valueKey:
			return &Node{
				NodeType: NodeTypeFieldRef,
				Value: &Node_FieldRefValue{
					FieldRefValue: valueKey,
				},
			}, nil

		}
		return &Node{
			NodeType: NodeTypeTagRef,
			Value: &Node_TagRefValue{
				TagRefValue: n.Property,
			},
		}, nil
	case *semantic.DurationLiteral:
		return nil, errors.New("duration literals not supported in storage predicates")
	case *semantic.DateTimeLiteral:
		return nil, errors.New("time literals not supported in storage predicates")
	default:
		return nil, fmt.Errorf("unsupported semantic expression type %T", n)
	}
}

func toComparisonOperator(o ast.OperatorKind) (Node_Comparison, error) {
	switch o {
	case ast.EqualOperator:
		return ComparisonEqual, nil
	case ast.NotEqualOperator:
		return ComparisonNotEqual, nil
	case ast.RegexpMatchOperator:
		return ComparisonRegex, nil
	case ast.NotRegexpMatchOperator:
		return ComparisonNotRegex, nil
	case ast.StartsWithOperator:
		return ComparisonStartsWith, nil
	case ast.LessThanOperator:
		return ComparisonLess, nil
	case ast.LessThanEqualOperator:
		return ComparisonLessEqual, nil
	case ast.GreaterThanOperator:
		return ComparisonGreater, nil
	case ast.GreaterThanEqualOperator:
		return ComparisonGreaterEqual, nil
	default:
		return 0, fmt.Errorf("unknown operator %v", o)
	}
}

// NodeVisitor can be called by Walk to traverse the Node hierarchy.
// The Visit() function is called once per node.
type NodeVisitor interface {
	Visit(*Node) NodeVisitor
}

func walkChildren(v NodeVisitor, node *Node) {
	for _, n := range node.Children {
		WalkNode(v, n)
	}
}

func WalkNode(v NodeVisitor, node *Node) {
	if v = v.Visit(node); v == nil {
		return
	}

	walkChildren(v, node)
}

func PredicateToExprString(p *Predicate) string {
	if p == nil {
		return "[none]"
	}

	var v predicateExpressionPrinter
	WalkNode(&v, p.Root)
	return v.Buffer.String()
}

type predicateExpressionPrinter struct {
	bytes.Buffer
}

func (v *predicateExpressionPrinter) Visit(n *Node) NodeVisitor {
	switch n.NodeType {
	case NodeTypeLogicalExpression:
		if len(n.Children) > 0 {
			var op string
			if n.GetLogical() == LogicalAnd {
				op = " AND "
			} else {
				op = " OR "
			}
			WalkNode(v, n.Children[0])
			for _, e := range n.Children[1:] {
				v.Buffer.WriteString(op)
				WalkNode(v, e)
			}
		}

		return nil

	case NodeTypeParenExpression:
		if len(n.Children) == 1 {
			v.Buffer.WriteString("( ")
			WalkNode(v, n.Children[0])
			v.Buffer.WriteString(" )")
		}

		return nil

	case NodeTypeComparisonExpression:
		WalkNode(v, n.Children[0])
		v.Buffer.WriteByte(' ')
		switch n.GetComparison() {
		case ComparisonEqual:
			v.Buffer.WriteByte('=')
		case ComparisonNotEqual:
			v.Buffer.WriteString("!=")
		case ComparisonStartsWith:
			v.Buffer.WriteString("startsWith")
		case ComparisonRegex:
			v.Buffer.WriteString("=~")
		case ComparisonNotRegex:
			v.Buffer.WriteString("!~")
		case ComparisonLess:
			v.Buffer.WriteByte('<')
		case ComparisonLessEqual:
			v.Buffer.WriteString("<=")
		case ComparisonGreater:
			v.Buffer.WriteByte('>')
		case ComparisonGreaterEqual:
			v.Buffer.WriteString(">=")
		}

		v.Buffer.WriteByte(' ')
		WalkNode(v, n.Children[1])
		return nil

	case NodeTypeTagRef:
		v.Buffer.WriteByte('\'')
		v.Buffer.WriteString(n.GetTagRefValue())
		v.Buffer.WriteByte('\'')
		return nil

	case NodeTypeFieldRef:
		v.Buffer.WriteByte('$')
		return nil

	case NodeTypeLiteral:
		switch val := n.Value.(type) {
		case *Node_StringValue:
			v.Buffer.WriteString(strconv.Quote(val.StringValue))

		case *Node_RegexValue:
			v.Buffer.WriteByte('/')
			v.Buffer.WriteString(val.RegexValue)
			v.Buffer.WriteByte('/')

		case *Node_IntegerValue:
			v.Buffer.WriteString(strconv.FormatInt(val.IntegerValue, 10))

		case *Node_UnsignedValue:
			v.Buffer.WriteString(strconv.FormatUint(val.UnsignedValue, 10))

		case *Node_FloatValue:
			v.Buffer.WriteString(strconv.FormatFloat(val.FloatValue, 'f', 10, 64))

		case *Node_BooleanValue:
			if val.BooleanValue {
				v.Buffer.WriteString("true")
			} else {
				v.Buffer.WriteString("false")
			}
		}

		return nil

	default:
		return v
	}
}

type ReadSource struct {
	// Database identifies which database to query.
	Database string `protobuf:"bytes,1,opt,name=database,proto3" json:"database,omitempty"`
	// RetentionPolicy identifies which retention policy to query.
	RetentionPolicy string `protobuf:"bytes,2,opt,name=retention_policy,json=retentionPolicy,proto3" json:"retention_policy,omitempty"`
}

func (m *ReadSource) Reset()                    { *m = ReadSource{} }
func (m *ReadSource) String() string            { return proto.CompactTextString(m) }
func (*ReadSource) ProtoMessage()               {}
func (*ReadSource) Descriptor() ([]byte, []int) { return fileDescriptorStorage, []int{0} }

func init() {
	proto.RegisterType((*ReadSource)(nil), "com.github.influxdata.idpe.read.ReadSource")
}
func (m *ReadSource) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *ReadSource) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.Database) > 0 {
		dAtA[i] = 0xa
		i++
		i = encodeVarintStorage(dAtA, i, uint64(len(m.Database)))
		i += copy(dAtA[i:], m.Database)
	}
	if len(m.RetentionPolicy) > 0 {
		dAtA[i] = 0x12
		i++
		i = encodeVarintStorage(dAtA, i, uint64(len(m.RetentionPolicy)))
		i += copy(dAtA[i:], m.RetentionPolicy)
	}
	return i, nil
}

func encodeFixed64Storage(dAtA []byte, offset int, v uint64) int {
	dAtA[offset] = uint8(v)
	dAtA[offset+1] = uint8(v >> 8)
	dAtA[offset+2] = uint8(v >> 16)
	dAtA[offset+3] = uint8(v >> 24)
	dAtA[offset+4] = uint8(v >> 32)
	dAtA[offset+5] = uint8(v >> 40)
	dAtA[offset+6] = uint8(v >> 48)
	dAtA[offset+7] = uint8(v >> 56)
	return offset + 8
}
func encodeFixed32Storage(dAtA []byte, offset int, v uint32) int {
	dAtA[offset] = uint8(v)
	dAtA[offset+1] = uint8(v >> 8)
	dAtA[offset+2] = uint8(v >> 16)
	dAtA[offset+3] = uint8(v >> 24)
	return offset + 4
}
func (m *ReadSource) Size() (n int) {
	var l int
	_ = l
	l = len(m.Database)
	if l > 0 {
		n += 1 + l + sovStorage(uint64(l))
	}
	l = len(m.RetentionPolicy)
	if l > 0 {
		n += 1 + l + sovStorage(uint64(l))
	}
	return n
}

func (m *ReadSource) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowStorage
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: ReadSource: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: ReadSource: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Database", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowStorage
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthStorage
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Database = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field RetentionPolicy", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowStorage
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthStorage
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.RetentionPolicy = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipStorage(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthStorage
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func init() { proto.RegisterFile("ostorage.proto", fileDescriptorStorage) }

var fileDescriptorStorage = []byte{
	// 209 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0x2d, 0x2e, 0xc9, 0x2f,
	0x4a, 0x4c, 0x4f, 0xd5, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0xd2, 0x4f, 0xce, 0xcf, 0xd5, 0x4b,
	0xcf, 0x2c, 0xc9, 0x28, 0x4d, 0xd2, 0xcb, 0xcc, 0x4b, 0xcb, 0x29, 0xad, 0x48, 0x49, 0x2c, 0x49,
	0x84, 0x31, 0x93, 0xf4, 0x8a, 0x53, 0x8b, 0xca, 0x32, 0x93, 0x53, 0x8b, 0xf5, 0xa0, 0xda, 0xa4,
	0x74, 0xa1, 0x8a, 0x93, 0xf3, 0x73, 0xf5, 0xd3, 0xf3, 0xd3, 0xf3, 0xf5, 0xc1, 0xe6, 0x24, 0x95,
	0xa6, 0x81, 0x79, 0x60, 0x0e, 0x98, 0x05, 0x31, 0x5f, 0x29, 0x83, 0x8b, 0x2b, 0x28, 0x35, 0x31,
	0x25, 0x38, 0xbf, 0xb4, 0x28, 0x39, 0x55, 0x48, 0x8a, 0x8b, 0x03, 0x64, 0x7c, 0x52, 0x62, 0x71,
	0xaa, 0x04, 0xa3, 0x02, 0xa3, 0x06, 0x67, 0x10, 0x9c, 0x2f, 0x64, 0xc7, 0x25, 0x50, 0x94, 0x5a,
	0x92, 0x9a, 0x57, 0x92, 0x99, 0x9f, 0x17, 0x5f, 0x90, 0x9f, 0x93, 0x99, 0x5c, 0x29, 0xc1, 0x04,
	0x52, 0xe3, 0x24, 0xfc, 0xe8, 0x9e, 0x3c, 0x7f, 0x10, 0x4c, 0x2e, 0x00, 0x2c, 0x15, 0xc4, 0x5f,
	0x84, 0x2a, 0xe0, 0x24, 0x7b, 0xe2, 0xa1, 0x1c, 0xc3, 0x89, 0x47, 0x72, 0x8c, 0x17, 0x1e, 0xc9,
	0x31, 0x3e, 0x78, 0x24, 0xc7, 0x38, 0xe1, 0xb1, 0x1c, 0x43, 0x14, 0x3b, 0xd4, 0xdd, 0x49, 0x6c,
	0x60, 0xf7, 0x18, 0x03, 0x02, 0x00, 0x00, 0xff, 0xff, 0xb9, 0x44, 0xf3, 0x2d, 0x00, 0x01, 0x00,
	0x00,
}
