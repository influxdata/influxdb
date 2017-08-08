package storage

import (
	"errors"

	"strings"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/uber-go/zap"
)

type planner interface {
	Next() bool
	Read() (measurement, key, field string, shards []*tsdb.Shard)
	Err() error
}

type allMeasurementsPlanner struct {
	shards []*tsdb.Shard
	sitr   influxql.FloatIterator
	fields []string
	nf     []string
	m      string
	key    string
	f      string
	err    error
	eof    bool
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

func newAllMeasurementsPlanner(shards []*tsdb.Shard, log zap.Logger) (*allMeasurementsPlanner, error) {
	opt := influxql.IteratorOptions{Aux: []influxql.VarRef{{Val: "key"}}}
	sg := tsdb.Shards(shards)
	sitr, err := toFloatIterator(sg.CreateIterator("_series", opt))
	if err != nil {
		return nil, err
	}
	fitr, err := toFloatIterator(sg.CreateIterator("_fieldKeys", opt))
	if err != nil {
		return nil, err
	}

	p := &allMeasurementsPlanner{
		shards: shards,
		sitr:   sitr,
		fields: extractFields(fitr),
	}

	return p, nil
}

func (p *allMeasurementsPlanner) Next() bool {
	if p.eof {
		return false
	}

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
		p.key = key
		if i := strings.IndexByte(p.key, ','); i > -1 {
			p.m = p.key[:i]
		}

		p.nf = p.fields
	}

	p.f, p.nf = p.nf[0], p.nf[1:]

	return true
}

func (p *allMeasurementsPlanner) Read() (measurement, key, field string, shards []*tsdb.Shard) {
	return p.m, p.key, p.f, p.shards
}

func (p *allMeasurementsPlanner) Err() error {
	return p.err
}
