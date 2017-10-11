package diagnostic

import (
	"fmt"
	"time"

	query "github.com/influxdata/influxdb/query/diagnostic"
	"github.com/uber-go/zap"
)

type QueryExecutorHandler struct {
	l zap.Logger
}

func (s *Service) QueryContext() query.Context {
	if s == nil {
		return nil
	}
	return &QueryExecutorHandler{l: s.l.With(zap.String("service", "query"))}
}

func (h *QueryExecutorHandler) PrintStatement(query string) {
	h.l.Info(query)
}

func (h *QueryExecutorHandler) QueryPanic(query string, err interface{}, stack []byte) {
	h.l.Error(fmt.Sprintf("%s [panic:%s] %s", query, err, stack))
}

func (h *QueryExecutorHandler) DetectedSlowQuery(query string, qid uint64, database string, threshold time.Duration) {
	h.l.Warn("Detected slow query",
		zap.String("query", query),
		zap.Uint64("qid", qid),
		zap.String("database", database),
		zap.String("threshold", threshold.String()),
	)
}
