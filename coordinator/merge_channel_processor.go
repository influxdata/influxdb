package coordinator

import (
	"errors"
	"fmt"

	"code.google.com/p/log4go"

	"github.com/influxdb/influxdb/common"
	"github.com/influxdb/influxdb/engine"
	"github.com/influxdb/influxdb/protocol"
)

// This struct is responsible for merging responses from multiple
// response channels and controlling the concurrency of querying by
// giving away `concurrency' channels at any given time. This is used
// in the coordinator to merge the responses received from different
// shards, which could be remote or local.
type MergeChannelProcessor struct {
	next engine.Processor
	c    chan (<-chan *protocol.Response)
	e    chan error
}

// Return a new MergeChannelProcessor that will yield to `next'
func NewMergeChannelProcessor(next engine.Processor, concurrency int) *MergeChannelProcessor {
	p := &MergeChannelProcessor{
		next: next,
		e:    make(chan error, concurrency),
		c:    make(chan (<-chan *protocol.Response), concurrency),
	}
	// Fill `p.e' with `concurrency' nils, see NextChannel() for an
	// explanation of why we do this.
	for i := 0; i < concurrency; i++ {
		p.e <- nil
	}
	return p
}

// Closes MergeChannelProcessor, this method has to make sure that all
// responses are received from the response channels. This is
// important since the protobuf client may block trying to insert a
// new response which will cause the entire node to stop receiving
// remote responses.
func (p *MergeChannelProcessor) Close() (err error) {
	// Close the channels' channel. This will cause NextChannel() to
	// panic if it was called after Close() is called and will cause
	// ProcessChannels() to return since there are no more channels to
	// read.
	close(p.c)

	// Read all errors reported by the ProcessChannels(), this for loop
	// will not end until ProcessChannels() returns and `p.e' is closed
	for e := range p.e {
		if e != nil {
			err = e
		}
	}

	// At this point ProcessChannels() has returned and NextChannel()
	// cannot be called. Go over all channels that were returned and
	// make sure we read all responses on those channels.
	for c := range p.c {
	nextChannel:
		for r := range c {
			switch rt := r.GetType(); rt {
			case protocol.Response_END_STREAM,
				protocol.Response_HEARTBEAT:
				break nextChannel
			case protocol.Response_ERROR:
				// If there were no errors from before, return this error
				if err == nil {
					err = errors.New(r.GetErrorMessage())
				}
				break nextChannel
			case protocol.Response_QUERY:
				continue // ignore the response, we're closing
			default:
				panic(fmt.Errorf("Unexpected response type: %s", rt))
			}
		}
	}
	return err
}

// Returns a new channel with buffer size `bs'. This method will block
// until there are channels available to return. Remember
// MergeChannelProcessor controls the concurrency of the query by
// guaranteeing no more than `concurrency' channels are given away at
// any given time.
func (p *MergeChannelProcessor) NextChannel(bs int) (chan<- *protocol.Response, error) {
	// `p.e' serves two purpose in MergeChannelProcessor. To return
	// errors received in ProcessChannels, and to control the
	// concurrency. Initially `p.e' has `concurrency' nils in it which
	// will allow the first `concurrency' calls to NextChannel() to
	// return without any errors and without blocking. Successive calls
	// to NextChannel() will wait until a nil or an error is inserted in
	// `p.e'.
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

// Start processing the channels that are yielded by NextChannel() and
// yield the Series in the responses to the next Process.
func (p *MergeChannelProcessor) ProcessChannels() {
	defer close(p.e)

	for channel := range p.c {
		if p.processChannel(channel) {
			return
		}
	}
}

// Process responses from the given channel. Returns true if
// processing should stop for other channels. False otherwise.
func (p *MergeChannelProcessor) processChannel(channel <-chan *protocol.Response) bool {
	for response := range channel {
		log4go.Debug("%s received %s", p, response)

		switch rt := response.GetType(); rt {

		// all these types end the stream
		case protocol.Response_HEARTBEAT,
			protocol.Response_END_STREAM:
			p.e <- nil
			return false

		case protocol.Response_ERROR:
			err := common.NewQueryError(common.InvalidArgument, response.GetErrorMessage())
			p.e <- err
			return false

		case protocol.Response_QUERY:
			for _, s := range response.MultiSeries {
				log4go.Debug("Yielding to %s: %s", p.next.Name(), s)
				_, err := p.next.Yield(s)
				if err != nil {
					p.e <- err
					return true
				}
			}

		default:
			panic(fmt.Errorf("Unknown response type: %s", rt))
		}
	}
	panic(errors.New("Reached end of method"))
}
