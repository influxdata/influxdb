package storage

import (
	"errors"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/uber-go/zap"
)

type planner interface {
	Next() bool
	Read() (measurement, key, field string, tagset map[string]string, shards []*tsdb.Shard)
	Err() error
}

type allMeasurementsPlanner struct {
	shards    []*tsdb.Shard
	sitr      influxql.FloatIterator
	fields    []string
	nf        []string
	m         string
	key       string
	f         string
	err       error
	eof       bool
	tagset    map[string]string
	filterset map[string]string
	cond      influxql.Expr
}

func toFloatIterator(iter influxql.Iterator, err error) (influxql.FloatIterator, error) {
	if err != nil {
		return nil, err
	}

	sitr, ok := iter.(influxql.FloatIterator)
	if !ok {
		return nil, errors.New("expected FloatIterator")
	}

	return sitr, nil
}

func extractFields(itr influxql.FloatIterator) []string {
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
	opt := influxql.IteratorOptions{Aux: []influxql.VarRef{{Val: "key"}}}
	p := &allMeasurementsPlanner{shards: shards}

	var err error

	if req.Predicate.GetRoot() != nil {
		var hf bool
		opt.Condition, hf, err = NodeToExprNoField(req.Predicate.Root)
		if err != nil {
			return nil, err
		}

		if hf {
			// we've already parsed it, so no error here
			p.cond, _ = NodeToExpr(req.Predicate.Root)
		} else {
			p.cond = opt.Condition
		}
	}

	sg := tsdb.Shards(shards)
	p.sitr, err = toFloatIterator(sg.CreateIterator("_series", opt))
	if err != nil {
		return nil, err
	}
	fitr, err := toFloatIterator(sg.CreateIterator("_fieldKeys", opt))
	if err != nil {
		return nil, err
	}

	p.fields = extractFields(fitr)

	return p, nil
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

		m, tags := models.ParseKey([]byte(key))
		p.filterset = map[string]string{"_name": m}
		p.tagset = map[string]string{"_measurement": m}
		for _, tag := range tags {
			key, val := string(tag.Key), string(tag.Value)
			p.tagset[key] = val
			p.filterset[key] = val
		}

		p.key = key
		p.m = m
		p.nf = p.fields
	}

	p.f, p.nf = p.nf[0], p.nf[1:]
	p.filterset["_field"] = p.f

	if p.cond != nil && !evalExprBool(p.cond, p.filterset) {
		goto RETRY
	}

	p.tagset["_field"] = p.f

	return true
}

func (p *allMeasurementsPlanner) Read() (measurement, key, field string, tagset map[string]string, shards []*tsdb.Shard) {
	return p.m, p.key, p.f, p.tagset, p.shards
}

func (p *allMeasurementsPlanner) Err() error {
	return p.err
}

func (p *allMeasurementsPlanner) walk() {}
