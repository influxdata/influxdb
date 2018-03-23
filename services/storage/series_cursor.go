package storage

import (
	"bytes"
	"context"
	"errors"
	"sort"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	"github.com/opentracing/opentracing-go"
)

var (
	measurementKey = []byte("_measurement")
	fieldKey       = []byte("_field")
)

type seriesCursor interface {
	Close()
	Next() *seriesRow
	Err() error
}

type seriesRow struct {
	name      []byte      // measurement name
	stags     models.Tags // unmodified series tags
	field     string
	tags      models.Tags
	query     tsdb.CursorIterators
	valueCond influxql.Expr
}

type mapValuer map[string]string

var _ influxql.Valuer = mapValuer(nil)

func (vs mapValuer) Value(key string) (interface{}, bool) {
	v, ok := vs[key]
	return v, ok
}

type indexSeriesCursor struct {
	sqry            tsdb.SeriesCursor
	fields          []string
	nf              []string
	err             error
	tags            models.Tags
	filterset       mapValuer
	cond            influxql.Expr
	measurementCond influxql.Expr
	row             seriesRow
	eof             bool
	hasFieldExpr    bool
	hasValueExpr    bool
	multiTenant     bool
}

func newIndexSeriesCursor(ctx context.Context, req *ReadRequest, shards []*tsdb.Shard) (*indexSeriesCursor, error) {
	queries, err := tsdb.CreateCursorIterators(ctx, shards)
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

	opt := query.IteratorOptions{
		Aux:        []influxql.VarRef{{Val: "key"}},
		Authorizer: query.OpenAuthorizer,
		Ascending:  true,
		Ordered:    true,
	}
	p := &indexSeriesCursor{row: seriesRow{query: queries}}

	var (
		remap map[string]string
		mi    tsdb.MeasurementIterator
	)
	if req.RequestType == ReadRequestTypeMultiTenant {
		p.multiTenant = true
		m := []byte(req.OrgID)
		m = append(m, 0, 0)
		m = append(m, req.Database...)
		mi = tsdb.NewMeasurementSliceIterator([][]byte{m})
	} else {
		remap = measurementRemap
	}

	if root := req.Predicate.GetRoot(); root != nil {
		if p.cond, err = NodeToExpr(root, remap); err != nil {
			return nil, err
		}

		p.hasFieldExpr, p.hasValueExpr = HasFieldKeyOrValue(p.cond)
		if !(p.hasFieldExpr || p.hasValueExpr) {
			p.measurementCond = p.cond
			opt.Condition = p.cond
		} else {
			p.measurementCond = influxql.Reduce(RewriteExprRemoveFieldValue(influxql.CloneExpr(p.cond)), nil)
			if isBooleanLiteral(p.measurementCond) {
				p.measurementCond = nil
			}

			opt.Condition = influxql.Reduce(RewriteExprRemoveFieldKeyAndValue(influxql.CloneExpr(p.cond)), nil)
			if isBooleanLiteral(opt.Condition) {
				opt.Condition = nil
			}
		}
	}

	sg := tsdb.Shards(shards)
	p.sqry, err = sg.CreateSeriesCursor(ctx, tsdb.SeriesCursorRequest{Measurements: mi}, opt.Condition)
	if p.sqry != nil && err == nil {
		var (
			itr query.Iterator
			fi  query.FloatIterator
		)
		if itr, err = sg.CreateIterator(ctx, &influxql.Measurement{SystemIterator: "_fieldKeys"}, opt); itr != nil && err == nil {
			if fi, err = toFloatIterator(itr); err != nil {
				goto CLEANUP
			}

			p.fields = extractFields(fi)
			fi.Close()
			return p, nil
		}
	}

CLEANUP:
	p.Close()
	return nil, err
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

func (c *indexSeriesCursor) Next() *seriesRow {
	if c.eof {
		return nil
	}

RETRY:
	if len(c.nf) == 0 {
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

		c.row.name = sr.Name
		c.row.stags = sr.Tags
		c.tags = copyTags(c.tags, sr.Tags)

		c.filterset = make(mapValuer)
		for _, tag := range c.tags {
			c.filterset[string(tag.Key)] = string(tag.Value)
		}

		if !c.multiTenant {
			c.filterset["_name"] = string(sr.Name)
			c.tags.Set(measurementKey, sr.Name)
		}

		c.nf = c.fields
	}

	c.row.field, c.nf = c.nf[0], c.nf[1:]
	c.filterset["_field"] = c.row.field

	if c.measurementCond != nil && !evalExprBool(c.measurementCond, c.filterset) {
		goto RETRY
	}

	c.tags.Set(fieldKey, []byte(c.row.field))

	if c.cond != nil && c.hasValueExpr {
		// TODO(sgc): lazily evaluate valueCond
		c.row.valueCond = influxql.Reduce(c.cond, c.filterset)
		if isBooleanLiteral(c.row.valueCond) {
			// we've reduced the expression to "true"
			c.row.valueCond = nil
		}
	}

	c.row.tags = copyTags(c.row.tags, c.tags)

	return &c.row
}

func (c *indexSeriesCursor) Err() error {
	return c.err
}

type limitSeriesCursor struct {
	seriesCursor
	n, o, c uint64
}

func newLimitSeriesCursor(ctx context.Context, cur seriesCursor, n, o uint64) *limitSeriesCursor {
	return &limitSeriesCursor{seriesCursor: cur, o: o, n: n}
}

func (c *limitSeriesCursor) Next() *seriesRow {
	if c.o > 0 {
		for i := uint64(0); i < c.o; i++ {
			if c.seriesCursor.Next() == nil {
				break
			}
		}
		c.o = 0
	}

	if c.c >= c.n {
		return nil
	}
	c.c++
	return c.seriesCursor.Next()
}

type groupSeriesCursor struct {
	seriesCursor
	ctx  context.Context
	rows []seriesRow
	keys [][]byte
	f    bool
}

func newGroupSeriesCursor(ctx context.Context, cur seriesCursor, keys []string) *groupSeriesCursor {
	g := &groupSeriesCursor{seriesCursor: cur, ctx: ctx}

	g.keys = make([][]byte, 0, len(keys))
	for _, k := range keys {
		g.keys = append(g.keys, []byte(k))
	}

	return g
}

func (c *groupSeriesCursor) Next() *seriesRow {
	if !c.f {
		c.sort()
	}

	if len(c.rows) > 0 {
		row := &c.rows[0]
		c.rows = c.rows[1:]
		return row
	}

	return nil
}

func (c *groupSeriesCursor) sort() {
	span := opentracing.SpanFromContext(c.ctx)
	if span != nil {
		span = opentracing.StartSpan("group_series_cursor.sort", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}

	var rows []seriesRow
	row := c.seriesCursor.Next()
	for row != nil {
		rows = append(rows, *row)
		row = c.seriesCursor.Next()
	}

	sort.Slice(rows, func(i, j int) bool {
		for _, k := range c.keys {
			ik := rows[i].tags.Get(k)
			jk := rows[j].tags.Get(k)
			cmp := bytes.Compare(ik, jk)
			if cmp == 0 {
				continue
			}
			return cmp == -1
		}

		return false
	})

	if span != nil {
		span.SetTag("rows", len(rows))
	}

	c.rows = rows

	// free early
	c.seriesCursor.Close()
	c.f = true
}

func isBooleanLiteral(expr influxql.Expr) bool {
	_, ok := expr.(*influxql.BooleanLiteral)
	return ok
}

func toFloatIterator(iter query.Iterator) (query.FloatIterator, error) {
	sitr, ok := iter.(query.FloatIterator)
	if !ok {
		return nil, errors.New("expected FloatIterator")
	}

	return sitr, nil
}

func extractFields(itr query.FloatIterator) []string {
	var a []string
	for {
		p, err := itr.Next()
		if err != nil {
			return nil
		} else if p == nil {
			break
		} else if f, ok := p.Aux[0].(string); ok {
			a = append(a, f)
		}
	}

	if len(a) == 0 {
		return a
	}

	sort.Strings(a)
	i := 1
	for j := 1; j < len(a); j++ {
		if a[j] != a[j-1] {
			a[i] = a[j]
			i++
		}
	}

	return a[:i]
}
