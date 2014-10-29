package cluster

import (
	"code.google.com/p/log4go"
	"github.com/influxdb/influxdb/engine"
	"github.com/influxdb/influxdb/protocol"
)

// ResponseChannelProcessor converts Series to Responses. This is used
// to chain `engine.Processor` with a `ResponseChannel'
type ResponseChannelProcessor struct {
	r ResponseChannel
}

func NewResponseChannelProcessor(r ResponseChannel) *ResponseChannelProcessor {
	return &ResponseChannelProcessor{r}
}

func (p *ResponseChannelProcessor) Yield(s *protocol.Series) (bool, error) {
	log4go.Debug("Yielding to %s %s", p.r.Name(), s)
	ok := p.r.Yield(&protocol.Response{
		Type:        protocol.Response_QUERY.Enum(),
		MultiSeries: []*protocol.Series{s},
	})
	return ok, nil
}

func (p *ResponseChannelProcessor) Close() error {
	p.r.Yield(&protocol.Response{
		Type: protocol.Response_END_STREAM.Enum(),
	})
	return nil
}

func (p *ResponseChannelProcessor) Name() string {
	return "ResponseChannelProcessor"
}

func (p *ResponseChannelProcessor) Next() engine.Processor {
	return nil
}
