package diagnostic

import (
	"fmt"

	meta "github.com/influxdata/influxdb/services/meta/diagnostic"
	"go.uber.org/zap"
)

type MetaClientHandler struct {
	l *zap.Logger
}

func (s *Service) MetaClientContext() meta.Context {
	if s == nil {
		return nil
	}
	return &MetaClientHandler{l: s.l.With(zap.String("service", "metaclient"))}
}

func (h *MetaClientHandler) ShardGroupExists(group uint64, db, rp string) {
	h.l.Info(fmt.Sprintf("shard group %d exists for database %s, retention policy %s", group, db, rp))
}

func (h *MetaClientHandler) PrecreateShardError(group uint64, err error) {
	h.l.Info(fmt.Sprintf("failed to precreate successive shard group for group %d: %s", group, err))
}

func (h *MetaClientHandler) NewShardGroup(group uint64, db, rp string) {
	h.l.Info(fmt.Sprintf("new shard group %d successfully precreated for database %s, retention policy %s", group, db, rp))
}
