package diagnostic

import (
	"fmt"
	"net"

	collectd "github.com/influxdata/influxdb/services/collectd/diagnostic"
	"go.uber.org/zap"
)

type CollectdHandler struct {
	l *zap.Logger
}

func (s *Service) CollectdHandler() collectd.Handler {
	if s == nil {
		return nil
	}
	return &CollectdHandler{l: s.l.With(zap.String("service", "collectd"))}
}

func (h *CollectdHandler) Starting() {
	h.l.Info("Starting collectd service")
}

func (h *CollectdHandler) Listening(addr net.Addr) {
	h.l.Info(fmt.Sprint("Listening on UDP: ", addr.String()))
}

func (h *CollectdHandler) Closed() {
	h.l.Info("collectd UDP closed")
}

func (h *CollectdHandler) UnableToReadDirectory(path string, err error) {
	h.l.Info(fmt.Sprintf("Unable to read directory %s: %s\n", path, err))
}

func (h *CollectdHandler) LoadingPath(path string) {
	h.l.Info(fmt.Sprintf("Loading %s\n", path))
}

func (h *CollectdHandler) TypesParseError(path string, err error) {
	h.l.Info(fmt.Sprintf("Unable to parse collectd types file: %s\n", path))
}

func (h *CollectdHandler) ReadFromUDPError(err error) {
	h.l.Info(fmt.Sprintf("collectd ReadFromUDP error: %s", err))
}

func (h *CollectdHandler) ParseError(err error) {
	h.l.Info(fmt.Sprintf("Collectd parse error: %s", err))
}

func (h *CollectdHandler) DroppingPoint(name string, err error) {
	h.l.Info(fmt.Sprintf("Dropping point %v: %v", name, err))
}

func (h *CollectdHandler) InternalStorageCreateError(err error) {
	h.l.Info(fmt.Sprintf("Required database or retention policy do not yet exist: %s", err.Error()))
}

func (h *CollectdHandler) PointWriterError(database string, err error) {
	h.l.Info(fmt.Sprintf("failed to write point batch to database %q: %s", database, err))
}
