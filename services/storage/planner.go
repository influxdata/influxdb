package storage

import (
	"errors"

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

type planner interface {
	Close()
	Next() bool
	Read() (measurement, key, field string, tags models.Tags, shards []*tsdb.Shard)
	Err() error
}

type allMeasurementsPlanner struct {
	shards    []*tsdb.Shard
	sitr      query.FloatIterator
	fields    []string
	nf        []string
	m         string
	key       string
	f         string
	err       error
	eof       bool
	tags      models.Tags
	filterset map[string]string
	cond      influxql.Expr
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
	}
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

		p.filterset = map[string]string{"_name": p.m}
		for _, tag := range p.tags {
			p.filterset[string(tag.Key)] = string(tag.Value)
		}

		p.tags.Set(measurementKey, mm)
		p.key = key
		p.nf = p.fields
	}

	p.f, p.nf = p.nf[0], p.nf[1:]
	p.filterset["_field"] = p.f

	if p.cond != nil && !evalExprBool(p.cond, p.filterset) {
		goto RETRY
	}

	p.tags.Set(fieldKey, []byte(p.f))

	return true
}

func (p *allMeasurementsPlanner) Read() (measurement, key, field string, tags models.Tags, shards []*tsdb.Shard) {
	return p.m, p.key, p.f, p.tags, p.shards
}

func (p *allMeasurementsPlanner) Err() error {
	return p.err
}
