package diagnostic

import (
	"fmt"
	"time"

	retention "github.com/influxdata/influxdb/services/retention/diagnostic"
	"go.uber.org/zap"
)

type RetentionHandler struct {
	l *zap.Logger
}

func (s *Service) RetentionContext() retention.Context {
	if s == nil {
		return nil
	}
	return &RetentionHandler{l: s.l.With(zap.String("service", "retention"))}
}

func (h *RetentionHandler) Starting(checkInterval time.Duration) {
	h.l.Info(fmt.Sprint("Starting retention policy enforcement service with check interval of ", checkInterval))
}

func (h *RetentionHandler) Closing() {
	h.l.Info("retention policy enforcement terminating")
}

func (h *RetentionHandler) StartingCheck() {
	h.l.Info("retention policy shard deletion check commencing")
}

func (h *RetentionHandler) DeletedShard(id uint64, db, rp string) {
	h.l.Info(fmt.Sprintf("shard ID %d from database %s, retention policy %s, deleted",
		id, db, rp))
}

func (h *RetentionHandler) DeleteShardError(id uint64, db, rp string, err error) {
	h.l.Error(fmt.Sprintf("failed to delete shard ID %d from database %s, retention policy %s: %s",
		id, db, rp, err.Error()))
}

func (h *RetentionHandler) DeletedShardGroup(id uint64, db, rp string) {
	h.l.Info(fmt.Sprintf("deleted shard group %d from database %s, retention policy %s",
		id, db, rp))
}

func (h *RetentionHandler) DeleteShardGroupError(id uint64, db, rp string, err error) {
	h.l.Info(fmt.Sprintf("failed to delete shard group %d from database %s, retention policy %s: %s",
		id, db, rp, err))
}

func (h *RetentionHandler) PruneShardGroupsError(err error) {
	h.l.Info(fmt.Sprintf("error pruning shard groups: %s", err))
}
