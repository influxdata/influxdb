package diagnostic

import (
	"fmt"
	"net"

	httpd "github.com/influxdata/influxdb/services/httpd/diagnostic"
	"go.uber.org/zap"
)

type HTTPDHandler struct {
	l *zap.Logger
}

func (s *Service) HTTPDContext() httpd.Context {
	if s == nil {
		return nil
	}
	return &HTTPDHandler{l: s.l.With(zap.String("service", "httpd"))}
}

func (h *HTTPDHandler) Starting(authEnabled bool) {
	h.l.Info("Starting HTTP service")
	h.l.Info(fmt.Sprint("Authentication enabled:", authEnabled))
}

func (h *HTTPDHandler) Listening(protocol string, addr net.Addr) {
	h.l.Info(fmt.Sprintf("Listening on %s: %s", protocol, addr.String()))
}

func (h *HTTPDHandler) UnauthorizedRequest(user string, query, database string) {
	h.l.Info(fmt.Sprintf("Unauthorized request | user: %q | query: %q | database %q", user, query, database))
}

func (h *HTTPDHandler) AsyncQueryError(query string, err error) {
	h.l.Info(fmt.Sprintf("error while running async query: %s: %s", query, err))
}

func (h *HTTPDHandler) WriteBodyReceived(bytes []byte) {
	h.l.Info(fmt.Sprintf("Write body received by handler: %s", bytes))
}

func (h *HTTPDHandler) WriteBodyReadError() {
	h.l.Info("Write handler unable to read bytes from request body")
}

func (h *HTTPDHandler) StatusDeprecated() {
	h.l.Info("WARNING: /status has been deprecated.  Use /ping instead.")
}

func (h *HTTPDHandler) PromWriteBodyReceived(body []byte) {
	h.l.Info(fmt.Sprintf("Prom write body received by handler: %s", body))
}

func (h *HTTPDHandler) PromWriteBodyReadError() {
	h.l.Info("Prom write handler unable to read bytes from request body")
}

func (h *HTTPDHandler) PromWriteError(err error) {
	h.l.Info(fmt.Sprintf("Prom write handler: %s", err.Error()))
}

func (h *HTTPDHandler) JWTClaimsAssertError() {
	h.l.Info("Could not assert JWT token claims as jwt.MapClaims")
}

func (h *HTTPDHandler) HttpError(status int, errStr string) {
	h.l.Error(fmt.Sprintf("[%d] - %q", status, errStr))
}
