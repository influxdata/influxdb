package diagnostic

import (
	"fmt"
	"net"
	"time"

	graphite "github.com/influxdata/influxdb/services/graphite/diagnostic"
	"go.uber.org/zap"
)

type GraphiteHandlerBuilder struct {
	l *zap.Logger
}

func (s *Service) GraphiteContext() graphite.ContextBuilder {
	if s == nil {
		return nil
	}
	return &GraphiteHandlerBuilder{l: s.l.With(zap.String("service", "graphite"))}
}

type GraphiteHandler struct {
	l *zap.Logger
}

func (h *GraphiteHandlerBuilder) WithContext(bindAddress string) graphite.Context {
	return &GraphiteHandler{l: h.l.With(zap.String("addr", bindAddress))}
}

func (h *GraphiteHandler) Starting(batchSize int, batchTimeout time.Duration) {
	h.l.Info("starting graphite service", zap.Int("batchSize", batchSize), zap.String("batchTimeout", batchTimeout.String()))
}

func (h *GraphiteHandler) Listening(protocol string, addr net.Addr) {
	h.l.Info("listening", zap.String("protocol", protocol), zap.String("addr", addr.String()))
}

func (h *GraphiteHandler) TCPListenerClosed() {
	h.l.Info("graphite TCP listener closed")
}

func (h *GraphiteHandler) TCPAcceptError(err error) {
	h.l.Info("error accepting TCP connection", zap.Error(err))
}

func (h *GraphiteHandler) LineParseError(line string, err error) {
	h.l.Info(fmt.Sprintf("unable to parse line: %s: %s", line, err))
}

func (h *GraphiteHandler) InternalStorageCreateError(err error) {
	h.l.Info(fmt.Sprintf("Required database or retention policy do not yet exist: %s", err.Error()))
}

func (h *GraphiteHandler) PointWriterError(database string, err error) {
	h.l.Info(fmt.Sprintf("failed to write point batch to database %q: %s", database, err))
}
