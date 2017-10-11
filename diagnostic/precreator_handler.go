package diagnostic

import (
	"fmt"
	"time"

	precreator "github.com/influxdata/influxdb/services/precreator/diagnostic"
	"github.com/uber-go/zap"
)

type PrecreatorHandler struct {
	l zap.Logger
}

func (s *Service) PrecreatorContext() precreator.Context {
	if s == nil {
		return nil
	}
	return &PrecreatorHandler{l: s.l.With(zap.String("service", "shard-precreation"))}
}

func (h *PrecreatorHandler) Starting(checkInterval, advancePeriod time.Duration) {
	h.l.Info(fmt.Sprintf("Starting precreation service with check interval of %s, advance period of %s",
		checkInterval, advancePeriod))
}

func (h *PrecreatorHandler) Closing() {
	h.l.Info("Precreation service terminating")
}

func (h *PrecreatorHandler) PrecreateError(err error) {
	h.l.Info(fmt.Sprintf("failed to precreate shards: %s", err.Error()))
}
