package coordinator

// This implements the SeriesWriter interface for use with continuous
// queries to write their output back into the db

import (
	"github.com/influxdb/influxdb/engine"
	"github.com/influxdb/influxdb/parser"
	"github.com/influxdb/influxdb/protocol"
)

type ContinuousQueryWriter struct {
	c          *Coordinator
	query      *parser.SelectQuery
	db, target string
}

func NewContinuousQueryWriter(
	c *Coordinator,
	db, target string,
	query *parser.SelectQuery,
) *ContinuousQueryWriter {
	return &ContinuousQueryWriter{
		c:      c,
		query:  query,
		db:     db,
		target: target,
	}
}

func (self *ContinuousQueryWriter) Yield(series *protocol.Series) (bool, error) {
	err := self.c.InterpolateValuesAndCommit(
		self.query.GetQueryString(),
		self.db,
		series,
		self.target,
		true,
	)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (self *ContinuousQueryWriter) Close() error {
	return nil
}

func (self *ContinuousQueryWriter) Name() string {
	return "ContinuousQueryWriter"
}

func (self *ContinuousQueryWriter) Next() engine.Processor {
	return nil
}
