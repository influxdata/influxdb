package engine

import (
	log "code.google.com/p/log4go"
	"github.com/influxdb/influxdb/parser"
	p "github.com/influxdb/influxdb/protocol"
)

type FilteringEngine struct {
	query        *parser.SelectQuery
	processor    QueryProcessor
	shouldFilter bool
}

func NewFilteringEngine(query *parser.SelectQuery, processor QueryProcessor) *FilteringEngine {
	shouldFilter := query.GetWhereCondition() != nil
	return &FilteringEngine{query, processor, shouldFilter}
}

// optimize for yield series and use it here
func (self *FilteringEngine) YieldPoint(seriesName *string, columnNames []string, point *p.Point) bool {
	return self.YieldSeries(&p.Series{
		Name:   seriesName,
		Fields: columnNames,
		Points: []*p.Point{point},
	})
}

func (self *FilteringEngine) YieldSeries(seriesIncoming *p.Series) bool {
	if !self.shouldFilter {
		return self.processor.YieldSeries(seriesIncoming)
	}

	series, err := Filter(self.query, seriesIncoming)
	if err != nil {
		log.Error("Error while filtering points: %s [query = %s]", err, self.query.GetQueryString())
		return false
	}
	if len(series.Points) == 0 {
		return true
	}
	return self.processor.YieldSeries(series)
}

func (self *FilteringEngine) Close() {
	self.processor.Close()
}

func (self *FilteringEngine) SetShardInfo(shardId int, shardLocal bool) {
	self.processor.SetShardInfo(shardId, shardLocal)
}
func (self *FilteringEngine) GetName() string {
	return self.processor.GetName()
}
