package diagnostic

import (
	"fmt"

	udp "github.com/influxdata/influxdb/services/udp/diagnostic"
	"github.com/uber-go/zap"
)

type UDPHandler struct {
	l zap.Logger
}

func (s *Service) UDPContext() udp.Context {
	if s == nil {
		return nil
	}
	return &UDPHandler{l: s.l.With(zap.String("service", "udp"))}
}

func (h *UDPHandler) Started(bindAddress string) {
	h.l.Info(fmt.Sprintf("Started listening on UDP: %s", bindAddress))
}

func (h *UDPHandler) Closed() {
	h.l.Info("Service closed")
}

func (h *UDPHandler) CreateInternalStorageFailure(db string, err error) {
	h.l.Info(fmt.Sprintf("Required database %s does not yet exist: %s", db, err.Error()))
}

func (h *UDPHandler) PointWriterError(database string, err error) {
	h.l.Info(fmt.Sprintf("failed to write point batch to database %q: %s", database, err))
}

func (h *UDPHandler) ParseError(err error) {
	h.l.Info(fmt.Sprintf("Failed to parse points: %s", err))
}

func (h *UDPHandler) ReadFromError(err error) {
	h.l.Info(fmt.Sprintf("Failed to read UDP message: %s", err))
}
