package engine

import (
	"fmt"

	"github.com/influxdb/influxdb/parser"
	p "github.com/influxdb/influxdb/protocol"
)

type FilteringEngine struct {
	query        *parser.SelectQuery
	processor    Processor
	shouldFilter bool
}

func NewFilteringEngine(query *parser.SelectQuery, processor Processor) *FilteringEngine {
	shouldFilter := query.GetWhereCondition() != nil
	return &FilteringEngine{query, processor, shouldFilter}
}

func (self *FilteringEngine) Yield(seriesIncoming *p.Series) (bool, error) {
	if !self.shouldFilter {
		return self.processor.Yield(seriesIncoming)
	}

	series, err := Filter(self.query, seriesIncoming)
	if err != nil {
		return false, fmt.Errorf("Error while filtering points: %s [query = %s]", err, self.query.GetQueryString())
	}
	if len(series.Points) == 0 {
		return true, nil
	}
	return self.processor.Yield(series)
}

func (self *FilteringEngine) Close() error {
	return self.processor.Close()
}

func (self *FilteringEngine) Name() string {
	return self.processor.Name()
}

func (self *FilteringEngine) Next() Processor {
	return self.processor.Next()
}
