package diagnostic

import (
	"fmt"
	"time"

	continuous_querier "github.com/influxdata/influxdb/services/continuous_querier/diagnostic"
	"github.com/uber-go/zap"
)

type ContinuousQuerierHandler struct {
	l zap.Logger
}

func (s *Service) ContinuousQuerierContext() continuous_querier.Context {
	if s == nil {
		return nil
	}
	return &ContinuousQuerierHandler{l: s.l.With(zap.String("service", "continuous_querier"))}
}

func (h *ContinuousQuerierHandler) Starting() {
	h.l.Info("Starting continuous query service")
}

func (h *ContinuousQuerierHandler) Closing() {
	h.l.Info("continuous query service terminating")
}

func (h *ContinuousQuerierHandler) RunningByRequest(now time.Time) {
	h.l.Info(fmt.Sprintf("running continuous queries by request for time: %v", now))
}

func (h *ContinuousQuerierHandler) ExecuteContinuousQuery(name string, start, end time.Time) {
	h.l.Info(fmt.Sprintf("executing continuous query %s (%v to %v)", name, start, end))
}

func (h *ContinuousQuerierHandler) ExecuteContinuousQueryError(query string, err error) {
	h.l.Info(fmt.Sprintf("error executing query: %s: err = %s", query, err))
}

func (h *ContinuousQuerierHandler) FinishContinuousQuery(name string, written int64, start, end time.Time, dur time.Duration) {
	h.l.Info(fmt.Sprintf("finished continuous query %s, %d points(s) written (%v to %v) in %s", name, written, start, end, dur))
}
