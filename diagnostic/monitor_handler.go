package diagnostic

import (
	"fmt"
	"time"

	monitor "github.com/influxdata/influxdb/monitor/diagnostic"
	"go.uber.org/zap"
)

type MonitorHandler struct {
	l *zap.Logger
}

func (s *Service) MonitorContext() monitor.Context {
	if s == nil {
		return nil
	}
	return &MonitorHandler{l: s.l.With(zap.String("service", "monitor"))}
}

func (h *MonitorHandler) Starting() {
	h.l.Info("Starting monitor system")
}

func (h *MonitorHandler) AlreadyOpen() {
	h.l.Info("Monitor is already open")
}

func (h *MonitorHandler) Closing() {
	h.l.Info("shutting down monitor system")
}

func (h *MonitorHandler) AlreadyClosed() {
	h.l.Info("Monitor is already closed.")
}

func (h *MonitorHandler) DiagnosticRegistered(name string) {
	h.l.Info("registered monitoring diagnostic", zap.String("name", name))
}

func (h *MonitorHandler) CreateInternalStorageFailure(db string, err error) {
	h.l.Info(fmt.Sprintf("failed to create database '%s', failed to create storage: %s",
		db, err.Error()))
}

func (h *MonitorHandler) StoreStatistics(db, rp string, interval time.Duration) {
	h.l.Info(fmt.Sprintf("Storing statistics in database '%s' retention policy '%s', at interval %s",
		db, rp, interval))
}

func (h *MonitorHandler) StoreStatisticsError(err error) {
	h.l.Info(fmt.Sprintf("failed to store statistics: %s", err))
}

func (h *MonitorHandler) StatisticsRetrievalFailure(err error) {
	h.l.Info(fmt.Sprintf("failed to retrieve registered statistics: %s", err))
}

func (h *MonitorHandler) DroppingPoint(name string, err error) {
	h.l.Info(fmt.Sprintf("Dropping point %v: %v", name, err))
}

func (h *MonitorHandler) StoreStatisticsDone() {
	h.l.Info(fmt.Sprintf("terminating storage of statistics"))
}
