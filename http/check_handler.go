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
)

const startingBody = `{"status":"starting"}` + "\n"

// HealthReadyHandler serves /health and /ready backed by a *check.Check and
// forwards any other request to an optional delegate handler. Before the
// delegate is installed, non-check requests get a 503 "starting" response.
// It is safe for concurrent use; checkers may be registered while it is
// serving.
type HealthReadyHandler struct {
	check     *check.Check
	startTime time.Time
	delegate  atomic.Pointer[http.Handler]
	headers   *AddHeader
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
// checkers and no delegate installed.
func NewHealthReadyHandler() *HealthReadyHandler {
	return &HealthReadyHandler{
		check:     check.NewCheck(),
		startTime: time.Now(),
		headers: &AddHeader{
			WriteHeader: func(h http.Header) {
				h.Add("X-Influxdb-Build", "OSS")
				h.Add("X-Influxdb-Version", platform.GetBuildInfo().Version)
			},
		},
	}
}

// AddReadyCheck registers a ready check.
func (h *HealthReadyHandler) AddReadyCheck(c check.Checker) { h.check.AddReadyCheck(c) }

// AddHealthCheck registers a health check.
func (h *HealthReadyHandler) AddHealthCheck(c check.Checker) { h.check.AddHealthCheck(c) }

// SetHandler installs the delegate handler used for any request that is not
// /health or /ready. It is safe to call concurrently with ServeHTTP.
func (h *HealthReadyHandler) SetHandler(next http.Handler) { h.delegate.Store(&next) }

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
		if p := h.delegate.Load(); p != nil {
			(*p).ServeHTTP(w, r)
			return
		}
		h.headers.WriteHeader(w.Header())
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, startingBody)
	}
}

// applyForceHealth mirrors the ?force=true&healthy=... override machinery
// from check.Check.ServeHTTP.
func (h *HealthReadyHandler) applyForceHealth(r *http.Request) {
	q := r.URL.Query()
	switch q.Get("force") {
	case "true":
		switch q.Get("healthy") {
		case "true":
			h.check.ForceHealth(true)
		case "false":
			h.check.ForceHealth(false)
		}
	case "false":
		h.check.ClearHealthOverride()
	}
}

// applyForceReady mirrors the ?force=true&ready=... override machinery from
// check.Check.ServeHTTP.
func (h *HealthReadyHandler) applyForceReady(r *http.Request) {
	q := r.URL.Query()
	switch q.Get("force") {
	case "true":
		switch q.Get("ready") {
		case "true":
			h.check.ForceReady(true)
		case "false":
			h.check.ForceReady(false)
		}
	case "false":
		h.check.ClearReadyOverride()
	}
}

func (h *HealthReadyHandler) writeHealth(w http.ResponseWriter, r *http.Request) {
	h.applyForceHealth(r)
	resp := h.check.CheckHealth(r.Context())
	info := platform.GetBuildInfo()
	status := http.StatusOK
	message := "ready for queries and writes"
	if resp.Status == check.StatusFail {
		status = http.StatusServiceUnavailable
		message = firstFailureMessage(resp.Checks)
	}
	checks := resp.Checks
	if checks == nil {
		checks = check.Responses{}
	}
	body := healthBody{
		Name:    "influxdb",
		Status:  string(resp.Status),
		Message: message,
		Checks:  checks,
		Version: info.Version,
		Commit:  info.Commit,
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func (h *HealthReadyHandler) writeReady(w http.ResponseWriter, r *http.Request) {
	h.applyForceReady(r)
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
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
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
