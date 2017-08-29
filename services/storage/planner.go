package storage

import (
	"errors"

	"bytes"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/uber-go/zap"
)

var (
	measurementKey = []byte("_measurement")
	fieldKey       = []byte("_field")
)

type ResultSet struct {
	p          planner
	start, end int64
	asc        bool

	row   plannerRow
	shard *tsdb.Shard
}

func (r *ResultSet) Close() {
	r.row.shards = nil
	r.p.Close()
}

func (r *ResultSet) Next() bool {
	if len(r.row.shards) == 0 {
		if !r.p.Next() {
			return false
		}

		r.row = r.p.Read()
	}

	r.shard, r.row.shards = r.row.shards[0], r.row.shards[1:]
	return true
}

func (r *ResultSet) Cursor() tsdb.Cursor {
	req := tsdb.CursorRequest{Measurement: r.row.measurement, Series: r.row.key, Field: r.row.field, Ascending: r.asc, StartTime: r.start, EndTime: r.end}
	c, _ := r.shard.CreateCursor(req)

	if r.row.valueCond != nil {
		c = newFilterCursor(c, r.row.valueCond)
	}

	return c
}

func (r *ResultSet) Tags() models.Tags {
	return r.row.tags
}

func (r *ResultSet) SeriesKey() string {
	// TODO(sgc): this must escape
	var buf bytes.Buffer
	for _, tag := range r.row.tags {
		buf.Write(tag.Key)
		buf.WriteByte(':')
		buf.Write(tag.Value)
		buf.WriteByte(',')
	}
	s := buf.String()
	return s[:len(s)-1]
}

type planner interface {
	Close()
	Next() bool
	Read() plannerRow
	Err() error
}

type plannerRow struct {
	measurement, key, field string
	tags                    models.Tags
	shards                  []*tsdb.Shard
	valueCond               influxql.Expr
}

type allMeasurementsPlanner struct {
	shards          []*tsdb.Shard
	sitr            query.FloatIterator
	fields          []string
	nf              []string
	m               string
	key             string
	f               string
	err             error
	eof             bool
	tags            models.Tags
	filterset       mapValuer
	cond            influxql.Expr
	measurementCond influxql.Expr
}

type mapValuer map[string]string

var _ influxql.Valuer = mapValuer(nil)

func (vs mapValuer) Value(key string) (interface{}, bool) {
	v, ok := vs[key]
	return v, ok
}

func toFloatIterator(iter query.Iterator, err error) (query.FloatIterator, error) {
	if err != nil {
		return nil, err
	}

	sitr, ok := iter.(query.FloatIterator)
	if !ok {
		return nil, errors.New("expected FloatIterator")
	}

	return sitr, nil
}

func extractFields(itr query.FloatIterator) []string {
	var fields []string
	for {
		p, err := itr.Next()
		if err != nil {
			return nil
		} else if p == nil {
			break
		} else if f, ok := p.Aux[0].(string); ok {
			fields = append(fields, f)
		}
	}

	return fields
}

func newAllMeasurementsPlanner(req *ReadRequest, shards []*tsdb.Shard, log zap.Logger) (*allMeasurementsPlanner, error) {
	opt := query.IteratorOptions{
		Aux:        []influxql.VarRef{{Val: "key"}},
		Authorizer: query.OpenAuthorizer{},
		Ordered:    true,
	}
	p := &allMeasurementsPlanner{shards: shards}

	var err error

	if root := req.Predicate.GetRoot(); root != nil {
		p.cond, err = NodeToExpr(root)
		if err != nil {
			return nil, err
		}

		if !HasFieldKeyOrValue(p.cond) {
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
	if itr, err := sg.CreateIterator("_series", opt); err != nil {
		return nil, err
	} else {
		// TODO(sgc): need to rethink how we enumerate series across shards; dedupe is inefficient
		itr = query.NewDedupeIterator(itr)

		if req.SeriesLimit > 0 || req.SeriesOffset > 0 {
			opt := query.IteratorOptions{Limit: int(req.SeriesLimit), Offset: int(req.SeriesOffset)}
			itr = query.NewLimitIterator(itr, opt)
		}

		p.sitr, err = toFloatIterator(itr, nil)
		if err != nil {
			return nil, err
		}
	}

	if itr, err := sg.CreateIterator("_fieldKeys", opt); err != nil {
		return nil, err
	} else {
		itr = query.NewDedupeIterator(itr)
		fitr, err := toFloatIterator(itr, nil)
		if err != nil {
			return nil, err
		}

		p.fields = extractFields(fitr)
	}


	return p, nil
}

func (p *allMeasurementsPlanner) Close() {
	p.eof = true
}

func (p *allMeasurementsPlanner) Next() bool {
	if p.eof {
		return false
	}

RETRY:
	if len(p.nf) == 0 {
		// next series key
		fp, err := p.sitr.Next()
		if err != nil {
			p.err = err
			p.eof = true
			return false
		} else if fp == nil {
			p.eof = true
			return false
		}

		key, ok := fp.Aux[0].(string)
		if !ok {
			p.err = errors.New("expected string for series key")
			p.eof = true
			return false
		}
		keyb := []byte(key)
		mm, _ := models.ParseName(keyb)
		p.m = string(mm)
		p.tags, _ = models.ParseTags(keyb)

		p.filterset = mapValuer{"_name": p.m}
		for _, tag := range p.tags {
			p.filterset[string(tag.Key)] = string(tag.Value)
		}

		p.tags.Set(measurementKey, mm)
		p.key = key
		p.nf = p.fields
	}

	p.f, p.nf = p.nf[0], p.nf[1:]
	p.filterset["_field"] = p.f

	if p.measurementCond != nil && !evalExprBool(p.measurementCond, p.filterset) {
		goto RETRY
	}

	p.tags.Set(fieldKey, []byte(p.f))

	return true
}

func (p *allMeasurementsPlanner) Read() plannerRow {
	var cond influxql.Expr
	if p.cond != nil {
		cond = influxql.Reduce(influxql.CloneExpr(p.cond), p.filterset)
		if isBooleanLiteral(cond) {
			// we've reduced the expression to "true"
			cond = nil
		}
	}

	return plannerRow{p.m, p.key, p.f, p.tags.Clone(), p.shards, cond}
}

func (p *allMeasurementsPlanner) Err() error {
	return p.err
}

func isBooleanLiteral(expr influxql.Expr) bool {
	_, ok := expr.(*influxql.BooleanLiteral)
	return ok
}
