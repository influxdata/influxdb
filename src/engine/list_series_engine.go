package engine

import (
	"protocol"
)

const (
	MAX_SERIES_IN_RESPONSE = 10000
)

var (
	queryResponse     = protocol.Response_QUERY
	endStreamResponse = protocol.Response_END_STREAM
)

type ListSeriesEngine struct {
	responseChan chan *protocol.Response
	response     *protocol.Response
}

func NewListSeriesEngine(responseChan chan *protocol.Response) *ListSeriesEngine {
	response := &protocol.Response{
		Type:        &queryResponse,
		MultiSeries: make([]*protocol.Series, 0),
	}

	return &ListSeriesEngine{
		responseChan: responseChan,
		response:     response,
	}
}

func (self *ListSeriesEngine) YieldPoint(seriesName *string, columnNames []string, point *protocol.Point) bool {
	if len(self.response.MultiSeries) > MAX_SERIES_IN_RESPONSE {
		self.responseChan <- self.response
		self.response = &protocol.Response{
			Type:        &queryResponse,
			MultiSeries: make([]*protocol.Series, 0),
		}
	}
	self.response.MultiSeries = append(self.response.MultiSeries, &protocol.Series{Name: seriesName})
	return true
}

func (self *ListSeriesEngine) YieldSeries(seriesName *string, columnNames []string, seriesIncoming *protocol.Series) bool {
	if len(self.response.MultiSeries) > MAX_SERIES_IN_RESPONSE {
		self.responseChan <- self.response
		self.response = &protocol.Response{
			Type:        &queryResponse,
			MultiSeries: make([]*protocol.Series, 0),
		}
	}
	self.response.MultiSeries = append(self.response.MultiSeries, &protocol.Series{Name: seriesName})
	return true
}

func (self *ListSeriesEngine) Close() {
	if len(self.response.MultiSeries) > 0 {
		self.responseChan <- self.response
	}
	response := &protocol.Response{Type: &endStreamResponse}
	self.responseChan <- response
}
