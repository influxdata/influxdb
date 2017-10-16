package diagnostic

import (
	"fmt"

	snapshotter "github.com/influxdata/influxdb/services/snapshotter/diagnostic"
	"go.uber.org/zap"
)

type SnapshotterHandler struct {
	l *zap.Logger
}

func (s *Service) SnapshotterContext() snapshotter.Context {
	if s == nil {
		return nil
	}
	return &SnapshotterHandler{l: s.l.With(zap.String("service", "snapshot"))}
}

func (h *SnapshotterHandler) Starting() {
	h.l.Info("Starting snapshot service")
}

func (h *SnapshotterHandler) Closed() {
	h.l.Info("snapshot listener closed")
}

func (h *SnapshotterHandler) AcceptError(err error) {
	h.l.Info(fmt.Sprint("error accepting snapshot request: ", err.Error()))
}

func (h *SnapshotterHandler) Error(err error) {
	h.l.Info(err.Error())
}
