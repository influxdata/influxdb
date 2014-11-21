package messaging

// LoopbackClient is the client for use when there is no Broker. That is,
// when system is comprised of a single node. Since there is no need
// for distributed consensus, there is no need to connect to a Broker.
type LoopbackClient struct {
	index uint64
	c     chan *Message
}

// NewLoopbackClient returns a new instance of LoopbackClient.
func NewLoopbackClient() *LoopbackClient {
	c := &LoopbackClient{c: make(chan *Message, 1)}
	return c
}

// Publish attaches an autoincrementing index to the message. It then simply
// loops the message back out the channel.
func (l *LoopbackClient) Publish(m *Message) (uint64, error) {
	l.index++
	m.Index = l.index

	l.c <- m
	return m.Index, nil
}

// C returns a channel for streaming message.
func (l *LoopbackClient) C() <-chan *Message { return l.c }
