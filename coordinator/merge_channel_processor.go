package coordinator

import (
	"fmt"

	"code.google.com/p/log4go"

	"github.com/influxdb/influxdb/common"
	"github.com/influxdb/influxdb/engine"
	"github.com/influxdb/influxdb/protocol"
)

type MergeChannelProcessor struct {
	next engine.Processor
	c    chan (<-chan *protocol.Response)
	e    chan error
}

func NewMergeChannelProcessor(next engine.Processor, concurrency int) *MergeChannelProcessor {
	p := &MergeChannelProcessor{
		next: next,
		e:    make(chan error, concurrency),
		c:    make(chan (<-chan *protocol.Response), concurrency),
	}
	for i := 0; i < concurrency; i++ {
		p.e <- nil
	}
	return p
}

func (p *MergeChannelProcessor) Close() (err error) {
	close(p.c)

	for e := range p.e {
		if e != nil {
			err = e
		}
	}

	for c := range p.c {
	nextChannel:
		for r := range c {
			switch r.GetType() {
			case protocol.Response_END_STREAM,
				protocol.Response_ACCESS_DENIED,
				protocol.Response_WRITE_OK,
				protocol.Response_HEARTBEAT:
				break nextChannel
			}
		}
	}
	return err
}

func (p *MergeChannelProcessor) NextChannel(bs int) (chan<- *protocol.Response, error) {
	err := <-p.e
	if err != nil {
		return nil, err
	}
	c := make(chan *protocol.Response, bs)
	p.c <- c
	return c, nil
}

func (p *MergeChannelProcessor) String() string {
	return fmt.Sprintf("MergeChannelProcessor (%d)", cap(p.e))
}

func (p *MergeChannelProcessor) ProcessChannels() {
	defer close(p.e)

	for channel := range p.c {
	nextChannel:
		for response := range channel {
			log4go.Debug("%s received %s", p, response)

			switch response.GetType() {

			// all these four types end the stream
			case protocol.Response_WRITE_OK,
				protocol.Response_HEARTBEAT,
				protocol.Response_ACCESS_DENIED,
				protocol.Response_END_STREAM:

				var err error
				if m := response.ErrorMessage; m != nil {
					err = common.NewQueryError(common.InvalidArgument, *m)
				}
				p.e <- err
				break nextChannel

			case protocol.Response_QUERY:
				for _, s := range response.MultiSeries {
					log4go.Debug("Yielding to %s: %s", p.next.Name(), s)
					_, err := p.next.Yield(s)
					if err != nil {
						p.e <- err
						return
					}
				}
			}
		}
	}
}
