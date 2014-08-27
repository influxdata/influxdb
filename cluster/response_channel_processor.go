package cluster

import (
	"code.google.com/p/log4go"
	"github.com/influxdb/influxdb/protocol"
)

type ResponseChannelProcessor struct {
	r ResponseChannel
}

var (
	QueryResponse     = protocol.Response_QUERY
	EndStreamResponse = protocol.Response_END_STREAM
)

func NewResponseChannelProcessor(r ResponseChannel) *ResponseChannelProcessor {
	return &ResponseChannelProcessor{r}
}

func (p *ResponseChannelProcessor) Yield(s *protocol.Series) (bool, error) {
	log4go.Debug("Yielding to %s %s", p.r.Name(), s)
	ok := p.r.Yield(&protocol.Response{
		Type:        &QueryResponse,
		MultiSeries: []*protocol.Series{s},
	})
	return ok, nil
}

func (p *ResponseChannelProcessor) Close() error {
	p.r.Yield(&protocol.Response{
		Type: &EndStreamResponse,
	})
	return nil
}

func (p *ResponseChannelProcessor) Name() string {
	return "ResponseChannelProcessor"
}
