package nats

import "go.uber.org/zap"

type Handler interface {
	// Process does something with a received subscription message, then acks it.
	Process(s Subscription, m Message)
}

type LogHandler struct {
	log *zap.Logger
}

func (lh *LogHandler) Process(s Subscription, m Message) {
	lh.log.Info(string(m.Data()))
	m.Ack()
}
