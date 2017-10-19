package diagnostic

import (
	"fmt"

	coordinator "github.com/influxdata/influxdb/coordinator/diagnostic"
	"go.uber.org/zap"
)

type PointsWriterHandler struct {
	l *zap.Logger
}

func (s *Service) PointsWriterHandler() coordinator.PointsWriterHandler {
	if s == nil {
		return nil
	}
	return &PointsWriterHandler{l: s.l.With(zap.String("service", "write"))}
}

func (h *PointsWriterHandler) WriteFailed(shardID uint64, err error) {
	h.l.Info(fmt.Sprintf("write failed for shard %d: %v", shardID, err))
}
