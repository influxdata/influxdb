package engine

// This engine buffers points and passes them through without modification. Works for queries
// that can't be aggregated locally or queries that don't require it like deletes and drops.
import (
	log "code.google.com/p/log4go"
	"protocol"
)

type PassthroughEngine struct {
	responseChan        chan *protocol.Response
	response            *protocol.Response
	maxPointsInResponse int
	limiter             *Limiter
}

func NewPassthroughEngine(responseChan chan *protocol.Response, maxPointsInResponse int) *PassthroughEngine {
	return NewPassthroughEngineWithLimit(responseChan, maxPointsInResponse, 0)
}

func NewPassthroughEngineWithLimit(responseChan chan *protocol.Response, maxPointsInResponse, limit int) *PassthroughEngine {
	return &PassthroughEngine{
		responseChan:        responseChan,
		maxPointsInResponse: maxPointsInResponse,
		limiter:             NewLimiter(limit),
	}
}

func (self *PassthroughEngine) YieldPoint(seriesName *string, columnNames []string, point *protocol.Point) bool {
	series := &protocol.Series{Name: seriesName, Points: []*protocol.Point{point}, Fields: columnNames}
	self.limiter.calculateLimitAndSlicePoints(series)
	if len(series.Points) == 0 {
		return false
	}

	if self.response == nil {
		self.response = &protocol.Response{
			Type:   &queryResponse,
			Series: series,
		}
	} else if self.response.Series.Name != seriesName {
		self.responseChan <- self.response
		self.response = &protocol.Response{
			Type:   &queryResponse,
			Series: series,
		}
	} else if len(self.response.Series.Points) > self.maxPointsInResponse {
		self.responseChan <- self.response
		self.response = &protocol.Response{
			Type:   &queryResponse,
			Series: series,
		}
	} else {
		self.response.Series.Points = append(self.response.Series.Points, point)
	}
	return !self.limiter.hitLimit(*seriesName)
}

func (self *PassthroughEngine) YieldSeries(seriesName *string, fieldNames []string, seriesIncoming *protocol.Series) bool {
	log.Debug("PassthroughEngine YieldSeries %d", len(seriesIncoming.Points))
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
			Type:   &queryResponse,
			Series: seriesIncoming,
		}
	} else if *self.response.Series.Name != *seriesName {
		self.responseChan <- self.response
		self.response = &protocol.Response{
			Type:   &queryResponse,
			Series: seriesIncoming,
		}
	} else if len(self.response.Series.Points) > self.maxPointsInResponse {
		self.responseChan <- self.response
		self.response = &protocol.Response{
			Type:   &queryResponse,
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
