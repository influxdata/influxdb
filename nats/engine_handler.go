package nats

import (
	"github.com/influxdata/platform/models"
	"go.uber.org/zap"
)

type Engine interface {
	WritePoints(points []models.Point) error
}

type EngineHandler struct {
	Logger *zap.Logger
	Engine Engine
}

func NewEngineHandler() *EngineHandler {
	return &EngineHandler{}
}

func (lh *EngineHandler) Process(s Subscription, m Message) {
	points, err := models.ParsePoints(m.Data())
	if err != nil {
		lh.Logger.Info("error parsing points", zap.Error(err))
		m.Ack()
		return
	}

	// TODO(jeff): This is super problematic. We need to only ack after the engine has flushed
	// the cache. There's no real good way to either force that or to wait for it. It's also
	// unclear what the semantics of Ack are with respect to previous messages. Oh well, for
	// now just ack and if the process dies, you lose data!

	if err := lh.Engine.WritePoints(points); err != nil {
		// TODO(jeff): we need some idea of if this is a permanent or temporary error.
		// For example, some sorts of PartialWriteErrors should not be retried. For
		// now, just Ack.
		lh.Logger.Info("error writing points", zap.Error(err))
		m.Ack()
		return
	}

	m.Ack()
	return
}
