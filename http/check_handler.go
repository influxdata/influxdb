package http

import (
	"encoding/json"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/check"
	"github.com/influxdata/influxdb/v2/toml"
	"go.uber.org/zap"
)

const startingBody = `{"status":"starting"}` + "\n"

// delegateHandler wraps an http.Handler so the atomic pointer targets a
// concrete struct instead of an interface, avoiding the pointer-to-interface
// awkwardness of atomic.Pointer[http.Handler]. The embedded Handler promotes
// ServeHTTP so callers write d.ServeHTTP(w, r).
type delegateHandler struct{ http.Handler }

// HealthReadyHandler serves /health and /ready backed by a *check.Check and
// forwards any other request to an optional delegate handler. Before the
// delegate is installed, non-check requests get a 503 "starting" response.
// It is safe for concurrent use; checkers may be registered while it is
// serving.
type HealthReadyHandler struct {
	check     *check.Check
	startTime time.Time
	delegate  atomic.Pointer[delegateHandler] // nil == no delegate installed
	headers   *AddHeader
	log       *zap.Logger
}

type healthBody struct {
	Name    string          `json:"name"`
	Status  string          `json:"status"`
	Message string          `json:"message"`
	Checks  check.Responses `json:"checks"`
	Version string          `json:"version"`
	Commit  string          `json:"commit"`
}

type readyBody struct {
	Status string          `json:"status"`
	Start  time.Time       `json:"started"`
	Up     toml.Duration   `json:"up"`
	Checks check.Responses `json:"checks,omitempty"`
}

// NewHealthReadyHandler returns a HealthReadyHandler with no registered
// checkers and no delegate installed. A nil log is replaced with zap.NewNop
// so write-error logging is always safe to call.
func NewHealthReadyHandler(log *zap.Logger) *HealthReadyHandler {
	if log == nil {
		log = zap.NewNop()
	}
	return &HealthReadyHandler{
		check:     check.NewCheck(),
		startTime: time.Now(),
		headers: &AddHeader{
			WriteHeader: func(h http.Header) {
				h.Add("X-Influxdb-Build", "OSS")
				h.Add("X-Influxdb-Version", platform.GetBuildInfo().Version)
			},
		},
		log: log,
	}
}

// AddReadyCheck registers a ready check.
func (h *HealthReadyHandler) AddReadyCheck(c check.Checker) { h.check.AddReadyCheck(c) }

// AddHealthCheck registers a health check.
func (h *HealthReadyHandler) AddHealthCheck(c check.Checker) { h.check.AddHealthCheck(c) }

// SetHandler installs the delegate handler used for any request that is not
// /health or /ready. A nil next is ignored to prevent a nil delegate from
// being published. Note: Go's typed-nil-through-interface gotcha means a
// concrete typed nil (e.g. (*T)(nil)) will still pass this guard; the
// delegate's own ServeHTTP needs to tolerate a nil receiver if that
// pattern is possible. Safe to call concurrently with ServeHTTP.
func (h *HealthReadyHandler) SetHandler(next http.Handler) {
	if next == nil {
		return
	}
	h.delegate.Store(&delegateHandler{next})
}

// ServeHTTP dispatches /health and /ready to the local renderers. Other
// paths are forwarded to the installed delegate; if no delegate has been
// installed, it returns 503 with a small "starting" body. Build-info
// headers are added to responses this handler renders itself; delegated
// responses rely on the delegate's own header middleware to avoid
// double-adding them.
func (h *HealthReadyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case HealthPath:
		h.headers.WriteHeader(w.Header())
		h.writeHealth(w, r)
	case ReadyPath:
		h.headers.WriteHeader(w.Header())
		h.writeReady(w, r)
	default:
		if d := h.delegate.Load(); d != nil {
			d.ServeHTTP(w, r)
			return
		}
		h.headers.WriteHeader(w.Header())
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusServiceUnavailable)
		if _, err := io.WriteString(w, startingBody); err != nil {
			h.log.Debug("failed to write starting body",
				zap.String("path", r.URL.Path),
				zap.Error(err))
		}
	}
}

func (h *HealthReadyHandler) writeHealth(w http.ResponseWriter, r *http.Request) {
	resp := h.check.CheckHealth(r.Context())
	info := platform.GetBuildInfo()
	status := http.StatusOK
	message := "ready for queries and writes"
	if resp.Status == check.StatusFail {
		status = http.StatusServiceUnavailable
		message = firstFailureMessage(resp.Checks)
	}
	body := healthBody{
		Name:    "influxdb",
		Status:  string(resp.Status),
		Message: message,
		Checks:  resp.Checks,
		Version: info.Version,
		Commit:  info.Commit,
	}
	h.writeJSON(w, r, status, body)
}

func (h *HealthReadyHandler) writeReady(w http.ResponseWriter, r *http.Request) {
	resp := h.check.CheckReady(r.Context())
	status := http.StatusOK
	readyStatus := "ready"
	var checks check.Responses
	if resp.Status == check.StatusFail {
		status = http.StatusServiceUnavailable
		readyStatus = "starting"
		checks = failingChecks(resp.Checks)
	}
	body := readyBody{
		Status: readyStatus,
		Start:  h.startTime,
		Up:     toml.Duration(time.Since(h.startTime)),
		Checks: checks,
	}
	h.writeJSON(w, r, status, body)
}

// writeJSON marshals body and writes it with the given status. If marshaling
// fails the response collapses to a 500 with a fixed JSON error body, matching
// kit/check.writeResponse. Marshal errors are not expected for the shape-pinned
// bodies this handler emits today; this guards against future changes that
// introduce a field that can fail to encode. Write errors (client hung up
// mid-response) are logged at debug level — the headers are already on the
// wire at that point, so there is no recovery.
func (h *HealthReadyHandler) writeJSON(w http.ResponseWriter, r *http.Request, status int, body interface{}) {
	buf, err := json.Marshal(body)
	if err != nil {
		h.log.Error("failed to marshal response body",
			zap.String("path", r.URL.Path),
			zap.Error(err))
		buf = []byte(`{"message":"error marshaling response","status":"fail"}`)
		status = http.StatusInternalServerError
	}
	buf = append(buf, '\n')
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if _, err := w.Write(buf); err != nil {
		h.log.Debug("failed to write response body",
			zap.String("path", r.URL.Path),
			zap.Error(err))
	}
}

func firstFailureMessage(checks check.Responses) string {
	for _, c := range checks {
		if c.Status == check.StatusFail {
			if c.Message != "" {
				return c.Message
			}
			return string(check.StatusFail)
		}
	}
	return "starting"
}

func failingChecks(checks check.Responses) check.Responses {
	var out check.Responses
	for _, c := range checks {
		if c.Status == check.StatusFail {
			out = append(out, c)
		}
	}
	return out
}
