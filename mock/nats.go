package mock

import (
	"bytes"
	"io"
	"sync"

	"github.com/influxdata/platform/nats"
)

// NatsServer is the mocked nats server based buffered channel.
type NatsServer struct {
	sync.RWMutex
	queue map[string]chan io.Reader
}

// create an empty channel for a subject
func (s *NatsServer) initSubject(subject string) (chan io.Reader, error) {
	s.Lock()
	defer s.Unlock()
	if _, ok := s.queue[subject]; !ok {
		s.queue[subject] = make(chan io.Reader)
	}
	return s.queue[subject], nil
}

// NewNats returns a mocked version of publisher, subscriber
func NewNats() (nats.Publisher, nats.Subscriber) {
	server := &NatsServer{
		queue: make(map[string]chan io.Reader),
	}

	publisher := &NatsPublisher{
		server: server,
	}

	subcriber := &NatsSubscriber{
		server: server,
	}

	return publisher, subcriber
}

// NatsPublisher is a mocked nats publisher.
type NatsPublisher struct {
	server *NatsServer
}

// Publish add subject and msg to server.
func (p *NatsPublisher) Publish(subject string, r io.Reader) error {
	_, err := p.server.initSubject(subject)
	p.server.queue[subject] <- r
	return err
}

// NatsSubscriber is mocked nats subscriber.
type NatsSubscriber struct {
	server *NatsServer
}

// Subscribe implements nats.Subscriber inteferface.
func (s *NatsSubscriber) Subscribe(subject, group string, handler nats.Handler) error {
	ch, err := s.server.initSubject(subject)
	if err != nil {
		return err
	}

	go func(s *NatsSubscriber, subject string, handler nats.Handler) {
		for r := range ch {
			handler.Process(&natsSubscription{subject: subject},
				&natsMessage{
					r: r,
				},
			)
		}
	}(s, subject, handler)
	return nil
}

type natsMessage struct {
	r     io.Reader
	read  bool
	bytes []byte
}

func (m *natsMessage) Data() []byte {
	if m.read {
		return m.bytes
	}
	buf := new(bytes.Buffer)
	buf.ReadFrom(m.r)
	m.bytes = buf.Bytes()
	m.read = true
	return m.bytes
}

func (m *natsMessage) Ack() error {
	return nil
}

type natsSubscription struct {
	subject string
}

func (s *natsSubscription) Pending() (int64, int64, error) {
	return 0, 0, nil
}

func (s *natsSubscription) Delivered() (int64, error) {
	return 0, nil
}

func (s *natsSubscription) Close() error {
	return nil
}
