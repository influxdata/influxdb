package diagnostic

import (
	"fmt"
	"net"

	opentsdb "github.com/influxdata/influxdb/services/opentsdb/diagnostic"
	"github.com/uber-go/zap"
)

type OpenTSDBHandler struct {
	l zap.Logger
}

func (s *Service) OpenTSDBContext() opentsdb.Context {
	if s == nil {
		return nil
	}
	return &OpenTSDBHandler{l: s.l.With(zap.String("service", "opentsdb"))}
}

func (h *OpenTSDBHandler) Starting() {
	h.l.Info("Starting OpenTSDB service")
}

func (h *OpenTSDBHandler) Listening(tls bool, addr net.Addr) {
	if tls {
		h.l.Info(fmt.Sprint("Listening on TLS: ", addr.String()))
	} else {
		h.l.Info(fmt.Sprint("Listening on: ", addr.String()))
	}
}

func (h *OpenTSDBHandler) Closed(err error) {
	if err != nil {
		h.l.Info(fmt.Sprint("error accepting openTSDB: ", err.Error()))
	} else {
		h.l.Info("openTSDB TCP listener closed")
	}
}

func (h *OpenTSDBHandler) ConnReadError(err error) {
	h.l.Info(fmt.Sprint("error reading from openTSDB connection ", err.Error()))
}

func (h *OpenTSDBHandler) InternalStorageCreateError(database string, err error) {
	h.l.Info(fmt.Sprintf("Required database %s does not yet exist: %s", database, err.Error()))
}

func (h *OpenTSDBHandler) PointWriterError(database string, err error) {
	h.l.Info(fmt.Sprintf("failed to write point batch to database %q: %s", database, err))
}

func (h *OpenTSDBHandler) MalformedLine(line string, addr net.Addr) {
	h.l.Info(fmt.Sprintf("malformed line '%s' from %s", line, addr.String()))
}

func (h *OpenTSDBHandler) MalformedTime(time string, addr net.Addr, err error) {
	h.l.Info(fmt.Sprintf("malformed time '%s' from %s: %s", time, addr.String(), err))
}

func (h *OpenTSDBHandler) MalformedTag(tag string, addr net.Addr) {
	h.l.Info(fmt.Sprintf("malformed tag data '%v' from %s", tag, addr.String()))
}

func (h *OpenTSDBHandler) MalformedFloat(valueStr string, addr net.Addr) {
	h.l.Info(fmt.Sprintf("bad float '%s' from %s", valueStr, addr.String()))
}

func (h *OpenTSDBHandler) DroppingPoint(metric string, err error) {
	h.l.Info(fmt.Sprintf("Dropping point %v: %v", metric, err))
}

func (h *OpenTSDBHandler) WriteError(err error) {
	h.l.Info(fmt.Sprint("write series error: ", err))
}
