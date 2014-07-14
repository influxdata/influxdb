package engine

// This engine buffers points and passes them through without modification. Works for queries
// that can't be aggregated locally or queries that don't require it like deletes and drops.
import (
	log "code.google.com/p/log4go"
	"github.com/influxdb/influxdb/common"
	"github.com/influxdb/influxdb/protocol"
)

type PassthroughEngine struct {
	responseChan        chan *protocol.Response
	response            *protocol.Response
	maxPointsInResponse int
	limiter             *Limiter
	responseType        *protocol.Response_Type

	// query statistics
	runStartTime  float64
	runEndTime    float64
	pointsRead    int64
	pointsWritten int64
	shardId       int
	shardLocal    bool
}

func NewPassthroughEngine(responseChan chan *protocol.Response, maxPointsInResponse int) *PassthroughEngine {
	return NewPassthroughEngineWithLimit(responseChan, maxPointsInResponse, 0)
}

func NewPassthroughEngineWithLimit(responseChan chan *protocol.Response, maxPointsInResponse, limit int) *PassthroughEngine {
	passthroughEngine := &PassthroughEngine{
		responseChan:        responseChan,
		maxPointsInResponse: maxPointsInResponse,
		limiter:             NewLimiter(limit),
		responseType:        &queryResponse,
		runStartTime:        0,
		runEndTime:          0,
		pointsRead:          0,
		pointsWritten:       0,
		shardId:             0,
		shardLocal:          false, //that really doesn't matter if it is not EXPLAIN query
	}

	return passthroughEngine
}

func (self *PassthroughEngine) YieldPoint(seriesName *string, columnNames []string, point *protocol.Point) bool {
	series := &protocol.Series{Name: seriesName, Points: []*protocol.Point{point}, Fields: columnNames}
	return self.YieldSeries(series)
}

func (self *PassthroughEngine) YieldSeries(seriesIncoming *protocol.Series) bool {
	log.Debug("PassthroughEngine YieldSeries %d", len(seriesIncoming.Points))
	if *seriesIncoming.Name == "explain query" {
		self.responseType = &explainQueryResponse
		log.Debug("Response Changed!")
	} else {
		self.responseType = &queryResponse
	}

	self.limiter.calculateLimitAndSlicePoints(seriesIncoming)
	if len(seriesIncoming.Points) == 0 {
		log.Debug("Not sent == 0")
		return false
	}

	if self.response == nil {
		self.response = &protocol.Response{
			Type:   self.responseType,
			Series: seriesIncoming,
		}
	} else if self.response.Series.GetName() != seriesIncoming.GetName() {
		self.responseChan <- self.response
		self.response = &protocol.Response{
			Type:   self.responseType,
			Series: seriesIncoming,
		}
	} else if len(self.response.Series.Points) > self.maxPointsInResponse {
		self.responseChan <- self.response
		self.response = &protocol.Response{
			Type:   self.responseType,
			Series: seriesIncoming,
		}
	} else {
		self.response.Series = common.MergeSeries(self.response.Series, seriesIncoming)
	}
	return !self.limiter.hitLimit(seriesIncoming.GetName())
	//return true
}

func (self *PassthroughEngine) Close() {
	if self.response != nil && self.response.Series != nil && self.response.Series.Name != nil {
		self.responseChan <- self.response
	}
	response := &protocol.Response{Type: &endStreamResponse}
	self.responseChan <- response
}

func (self *PassthroughEngine) SetShardInfo(shardId int, shardLocal bool) {
	//EXPLAIN doens't really work with this query (yet ?)
}

func (self *PassthroughEngine) GetName() string {
	return "PassthroughEngine"
}
