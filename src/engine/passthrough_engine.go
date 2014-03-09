package engine

// This engine buffers points and passes them through without modification. Works for queries
// that can't be aggregated locally or queries that don't require it like deletes and drops.
import (
	log "code.google.com/p/log4go"
	"protocol"
	"time"
)

type PassthroughEngine struct {
	responseChan        chan *protocol.Response
	response            *protocol.Response
	maxPointsInResponse int
	limiter             *Limiter
	responseType		*protocol.Response_Type

	// query statistics	
	runStartTime 		float64
	runEndTime 			float64
	pointsRead			int64
	pointsWritten		int64
	shardId				int
	shardLocal			bool	
}

func NewPassthroughEngine(responseChan chan *protocol.Response, maxPointsInResponse int) *PassthroughEngine {
	return NewPassthroughEngineWithLimit(responseChan, maxPointsInResponse, 0)
}

func NewPassthroughEngineWithLimit(responseChan chan *protocol.Response, maxPointsInResponse, limit int) *PassthroughEngine {
	passthroughEngine := &PassthroughEngine{
		responseChan:        responseChan,
		maxPointsInResponse: maxPointsInResponse,
		limiter:             NewLimiter(limit),
		responseType:		 &queryResponse,
		runStartTime:		 0,
		runEndTime:			 0,
		pointsRead:			 0,
		pointsWritten:		 0,
		shardId:			 0,
		shardLocal:			 false, //that really doesn't matter if it is not EXPLAIN query
	}

	return passthroughEngine
}

func (self *PassthroughEngine) YieldPoint(seriesName *string, columnNames []string, point *protocol.Point) bool {
	self.responseType = &queryResponse
	series := &protocol.Series{Name: seriesName, Points: []*protocol.Point{point}, Fields: columnNames}
	self.limiter.calculateLimitAndSlicePoints(series)
	if len(series.Points) == 0 {
		return false
	}

	if self.response == nil {
		self.response = &protocol.Response{
			Type:   self.responseType,
			Series: series,
		}
	} else if *self.response.Series.Name != *seriesName {
		self.responseChan <- self.response
		self.response = &protocol.Response{
			Type:   self.responseType,
			Series: series,
		}
	} else if len(self.response.Series.Points) > self.maxPointsInResponse {
		self.responseChan <- self.response
		self.response = &protocol.Response{
			Type:   self.responseType,
			Series: series,
		}
	} else {
		self.response.Series.Points = append(self.response.Series.Points, point)
	}
	return !self.limiter.hitLimit(*seriesName)
}

func (self *PassthroughEngine) YieldSeries(seriesName *string, fieldNames []string, seriesIncoming *protocol.Series) bool {
	log.Debug("PassthroughEngine YieldSeries %d", len(seriesIncoming.Points))
	if *seriesIncoming.Name == "explain query" {
		self.responseType = &explainQueryResponse
		log.Debug("Response Changed!")
	} else {
		self.responseType = &queryResponse
	}


/*
	seriesCopy := &protocol.Series{Name: protocol.String(*seriesName), Fields: fieldNames, Points: make([]*protocol.Point, 0, POINT_BATCH_SIZE)}
	for _, point := range seriesIncoming.Points {
		seriesCopy.Points = append(seriesCopy.Points, point)
	}
*/
	//log.Debug("PT Copied %d %d", len(seriesIncoming.Points), POINT_BATCH_SIZE)
	self.limiter.calculateLimitAndSlicePoints(seriesIncoming)
	if len(seriesIncoming.Points) == 0 {
		log.Error("Not sent == 0")
		return false
	}	

	//log.Debug("PassthroughEngine", seriesCopy)	
	/*
	self.response = &protocol.Response{
		Type:   &queryResponse,
		Series: seriesIncoming,
	}	
	self.responseChan <- self.response
	*/
	if self.response == nil {
		self.response = &protocol.Response{
			Type:   self.responseType,
			Series: seriesIncoming,
		}
	} else if *self.response.Series.Name != *seriesName {
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
		self.response.Series.Points = append(self.response.Series.Points, seriesIncoming.Points...)
	}
	return !self.limiter.hitLimit(*seriesName)
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

func (self *PassthroughEngine) SendQueryStats() {
	timestamp := time.Now().Unix()	

	runTime := self.runEndTime - self.runStartTime
	points := []*protocol.Point{}
	pointsRead := self.pointsRead
	pointsWritten := self.pointsWritten
	shardId := int64(self.shardId)
	shardLocal := self.shardLocal
	engineName := "PassthroughEngine"

	point := &protocol.Point{
		Values: []*protocol.FieldValue{
			&protocol.FieldValue{StringValue: &engineName},
			&protocol.FieldValue{Int64Value: &shardId},
			&protocol.FieldValue{BoolValue: &shardLocal},
			&protocol.FieldValue{DoubleValue: &runTime},
			&protocol.FieldValue{Int64Value: &pointsRead},
			&protocol.FieldValue{Int64Value: &pointsWritten},			
		},
		Timestamp:      &timestamp,		
	}
	points = append(points, point)

	seriesName := "explain query"
	series := &protocol.Series{
		Name:   &seriesName,
		Fields: []string{"engine_name", "shard_id", "shard_local", "run_time", "points_read", "points_written"},
		Points: points,
	}
	response := &protocol.Response{Type: &explainQueryResponse, Series: series}
	self.responseChan <- response
}