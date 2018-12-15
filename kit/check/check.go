// Package check standardizes /health and /ready endpoints.
// This allows you to easily know when your server is ready and healthy.
package check

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
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
	healthChecks []Checker
	readyChecks  []Checker

	passthroughHandler http.Handler
}

// Checker indicates a service whose health can be checked.
type Checker interface {
	Check(ctx context.Context) Response
}

// NewCheck returns a Health with a default checker.
func NewCheck() *Check {
	return &Check{}
}

// AddHealthCheck adds the check to the list of ready checks.
// If c is a NamedChecker, the name will be added.
func (c *Check) AddHealthCheck(check Checker) {
	if nc, ok := check.(NamedChecker); ok {
		c.healthChecks = append(c.healthChecks, Named(nc.CheckName(), nc))
	} else {
		c.healthChecks = append(c.healthChecks, check)
	}
}

// AddReadyCheck adds the check to the list of ready checks.
// If c is a NamedChecker, the name will be added.
func (c *Check) AddReadyCheck(check Checker) {
	if nc, ok := check.(NamedChecker); ok {
		c.readyChecks = append(c.readyChecks, Named(nc.CheckName(), nc))
	} else {
		c.readyChecks = append(c.readyChecks, check)
	}
}

// CheckHealth evaluates c's set of health checks and returns a populated Response.
func (c *Check) CheckHealth(ctx context.Context) Response {
	response := Response{
		Name:   "Health",
		Status: StatusPass,
		Checks: make(Responses, len(c.healthChecks)),
	}
	for i, ch := range c.healthChecks {
		resp := ch.Check(ctx)
		if resp.Status != StatusPass {
			response.Status = resp.Status
		}
		response.Checks[i] = resp
	}
	sort.Sort(response.Checks)
	return response
}

// CheckReady evaluates c's set of ready checks and returns a populated Response.
func (c *Check) CheckReady(ctx context.Context) Response {
	response := Response{
		Name:   "Ready",
		Status: StatusPass,
		Checks: make(Responses, len(c.readyChecks)),
	}
	for i, c := range c.readyChecks {
		resp := c.Check(ctx)
		if resp.Status != StatusPass {
			response.Status = resp.Status
		}
		response.Checks[i] = resp
	}
	sort.Sort(response.Checks)
	return response
}

// SetPassthrough allows you to set a handler to use if the request is not a ready or health check.
// This can be usefull if you intend to use this as a middleware.
func (c *Check) SetPassthrough(h http.Handler) {
	c.passthroughHandler = h
}

// ServeHTTP serves /ready and /health requests with the respective checks.
func (c *Check) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// allow requests not intended for checks to pass through.
	if r.URL.Path != "/ready" && r.URL.Path != "/health" {
		if c.passthroughHandler != nil {
			c.passthroughHandler.ServeHTTP(w, r)
			return
		}

		// We cant handle this request.
		w.WriteHeader(http.StatusNotFound)
		return
	}

	msg := ""
	status := http.StatusOK

	var resp Response
	switch r.URL.Path {
	case "/ready":
		resp = c.CheckReady(r.Context())
	case "/health":
		resp = c.CheckHealth(r.Context())
	}

	// Set the HTTP status if the check failed
	if resp.Status == StatusFail {
		// Normal state, the HTTP response status reflects the status-reported health.
		status = http.StatusServiceUnavailable
	}

	b, err := json.MarshalIndent(resp, "", "  ")
	if err != nil {
		b = []byte(`{"message": "error marshaling response", "status": "fail"}`)
		status = http.StatusInternalServerError
	}
	msg = string(b)
	w.WriteHeader(status)
	fmt.Fprintln(w, msg)
}
