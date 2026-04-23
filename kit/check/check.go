// Package check standardizes /health and /ready endpoints.
// This allows you to easily know when your server is ready and healthy.
package check

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
)

// Status string to indicate the overall status of the check.
type Status string

const (
	// StatusFail indicates a specific check has failed.
	StatusFail Status = "fail"
	// StatusPass indicates a specific check has passed.
	StatusPass Status = "pass"

	// DefaultCheckName is the name of the default checker.
	DefaultCheckName = "internal"
)

// Check wraps a map of service names to status checkers.
type Check struct {
	mu             sync.RWMutex
	healthChecks   []Checker
	readyChecks    []Checker
	healthOverride override
	readyOverride  override

	passthroughHandler http.Handler
}

// Checker indicates a service whose health can be checked.
type Checker interface {
	Check(ctx context.Context) Response
}

// NewCheck returns a Health with a default checker.
func NewCheck() *Check {
	ch := &Check{}
	ch.healthOverride.disable()
	ch.readyOverride.disable()
	return ch
}

// AddHealthCheck adds the check to the list of ready checks.
// If c is a NamedChecker, the name will be added.
func (c *Check) AddHealthCheck(check Checker) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if nc, ok := check.(NamedChecker); ok {
		c.healthChecks = append(c.healthChecks, Named(nc.CheckName(), nc))
	} else {
		c.healthChecks = append(c.healthChecks, check)
	}
}

// AddReadyCheck adds the check to the list of ready checks.
// If c is a NamedChecker, the name will be added.
func (c *Check) AddReadyCheck(check Checker) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if nc, ok := check.(NamedChecker); ok {
		c.readyChecks = append(c.readyChecks, Named(nc.CheckName(), nc))
	} else {
		c.readyChecks = append(c.readyChecks, check)
	}
}

// CheckHealth evaluates c's set of health checks and returns a populated Response.
func (c *Check) CheckHealth(ctx context.Context) Response {
	// Snapshot the checker slice under the read lock and release the lock
	// before evaluating. Checkers can block (network calls) or even re-enter
	// registration, so we must not hold c.mu across Check invocations.
	c.mu.RLock()
	checks := append([]Checker(nil), c.healthChecks...)
	c.mu.RUnlock()

	response := Response{
		Name:   "Health",
		Status: StatusPass,
		Checks: make(Responses, 0, len(checks)+1),
	}

	status, overriding := c.healthOverride.get()
	if overriding {
		response.Status = status
		response.Checks = append(response.Checks, Response{
			Name:    "manual-override",
			Status:  status,
			Message: "health manually overridden",
		})
	}
	for _, ch := range checks {
		resp := ch.Check(ctx)
		if resp.Status != StatusPass && !overriding {
			response.Status = resp.Status
		}
		response.Checks = append(response.Checks, resp)
	}
	sort.Sort(response.Checks)
	return response
}

// CheckReady evaluates c's set of ready checks and returns a populated Response.
func (c *Check) CheckReady(ctx context.Context) Response {
	// See CheckHealth: snapshot under lock, evaluate outside.
	c.mu.RLock()
	checks := append([]Checker(nil), c.readyChecks...)
	c.mu.RUnlock()

	response := Response{
		Name:   "Ready",
		Status: StatusPass,
		Checks: make(Responses, 0, len(checks)+1),
	}

	status, overriding := c.readyOverride.get()
	if overriding {
		response.Status = status
		response.Checks = append(response.Checks, Response{
			Name:    "manual-override",
			Status:  status,
			Message: "ready manually overridden",
		})
	}
	for _, ch := range checks {
		resp := ch.Check(ctx)
		if resp.Status != StatusPass && !overriding {
			response.Status = resp.Status
		}
		response.Checks = append(response.Checks, resp)
	}
	sort.Sort(response.Checks)
	return response
}

// ForceHealth enables a manual override on the health status. If pass is true
// the health status is forced to StatusPass; otherwise it is forced to
// StatusFail. Clear the override with ClearHealthOverride.
func (c *Check) ForceHealth(pass bool) {
	if pass {
		c.healthOverride.enable(StatusPass)
	} else {
		c.healthOverride.enable(StatusFail)
	}
}

// ClearHealthOverride disables any active health-status override.
func (c *Check) ClearHealthOverride() {
	c.healthOverride.disable()
}

// ForceReady enables a manual override on the ready status. If pass is true
// the ready status is forced to StatusPass; otherwise it is forced to
// StatusFail. Clear the override with ClearReadyOverride.
func (c *Check) ForceReady(pass bool) {
	if pass {
		c.readyOverride.enable(StatusPass)
	} else {
		c.readyOverride.enable(StatusFail)
	}
}

// ClearReadyOverride disables any active ready-status override.
func (c *Check) ClearReadyOverride() {
	c.readyOverride.disable()
}

// SetPassthrough allows you to set a handler to use if the request is not a ready or health check.
// This can be useful if you intend to use this as a middleware.
func (c *Check) SetPassthrough(h http.Handler) {
	c.passthroughHandler = h
}

// ServeHTTP serves /ready and /health requests with the respective checks.
func (c *Check) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	const (
		pathReady  = "/ready"
		pathHealth = "/health"
		queryForce = "force"
	)

	path := r.URL.Path

	// Allow requests not intended for checks to pass through.
	if path != pathReady && path != pathHealth {
		if c.passthroughHandler != nil {
			c.passthroughHandler.ServeHTTP(w, r)
			return
		}

		// We can't handle this request.
		w.WriteHeader(http.StatusNotFound)
		return
	}

	ctx := r.Context()
	query := r.URL.Query()

	switch path {
	case pathReady:
		switch query.Get(queryForce) {
		case "true":
			switch query.Get("ready") {
			case "true":
				c.readyOverride.enable(StatusPass)
			case "false":
				c.readyOverride.enable(StatusFail)
			}
		case "false":
			c.readyOverride.disable()
		}
		writeResponse(w, c.CheckReady(ctx))
	case pathHealth:
		switch query.Get(queryForce) {
		case "true":
			switch query.Get("healthy") {
			case "true":
				c.healthOverride.enable(StatusPass)
			case "false":
				c.healthOverride.enable(StatusFail)
			}
		case "false":
			c.healthOverride.disable()
		}
		writeResponse(w, c.CheckHealth(ctx))
	}
}

// writeResponse writes a Response to the wire as JSON. The HTTP status code
// accompanying the payload is the primary means for signaling the status of the
// checks. The possible status codes are:
//
// - 200 OK: All checks pass.
// - 503 Service Unavailable: Some checks are failing.
// - 500 Internal Server Error: There was a problem serializing the Response.
func writeResponse(w http.ResponseWriter, resp Response) {
	status := http.StatusOK
	if resp.Status == StatusFail {
		status = http.StatusServiceUnavailable
	}

	msg, err := json.MarshalIndent(resp, "", "  ")
	if err != nil {
		msg = []byte(`{"message": "error marshaling response", "status": "fail"}`)
		status = http.StatusInternalServerError
	}
	w.WriteHeader(status)
	fmt.Fprintln(w, string(msg))
}

// override is a manual override for an entire group of checks.
type override struct {
	mtx    sync.Mutex
	status Status
	active bool
}

// get returns the Status of an override as well as whether or not an override
// is currently active.
func (m *override) get() (Status, bool) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	return m.status, m.active
}

// disable disables the override.
func (m *override) disable() {
	m.mtx.Lock()
	m.active = false
	m.status = StatusFail
	m.mtx.Unlock()
}

// enable turns on the override and establishes a specific Status for which to.
func (m *override) enable(s Status) {
	m.mtx.Lock()
	m.active = true
	m.status = s
	m.mtx.Unlock()
}
