package nats

import (
	stan "github.com/nats-io/go-nats-streaming"
)

type Message interface {
	Data() []byte
	Ack() error
}

type message struct {
	m *stan.Msg
}

func (m *message) Data() []byte {
	return m.m.Data
}

func (m *message) Ack() error {
	return m.m.Ack()
}
