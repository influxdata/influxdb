// Package check standardizes /health and /ready endpoints.
// This allows you to easily know when your server is ready and healthy.
package check

import (
	"context"
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
	mu           sync.RWMutex
	healthChecks []Checker
	readyChecks  []Checker
	readyNames   []string
}

// Checker indicates a service whose health can be checked.
type Checker interface {
	Check(ctx context.Context) Response
}

// NewCheck returns an empty Check with no default checkers registered.
func NewCheck() *Check {
	return &Check{}
}

// AddHealthCheck registers an anonymous health check. If check happens to
// implement NamedChecker, registration is delegated to AddNamedHealthCheck
// so the name is recorded; otherwise the check is stored as-is and its
// recorded name is empty. Prefer AddNamedHealthCheck when the caller
// already knows the name.
func (c *Check) AddHealthCheck(check Checker) {
	if nc, ok := check.(NamedChecker); ok {
		c.AddNamedHealthCheck(nc)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.healthChecks = append(c.healthChecks, check)
}

// AddReadyCheck registers an anonymous ready check. See AddHealthCheck for
// the NamedChecker fallback.
func (c *Check) AddReadyCheck(check Checker) {
	if nc, ok := check.(NamedChecker); ok {
		c.AddNamedReadyCheck(nc)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readyChecks = append(c.readyChecks, check)
	c.readyNames = append(c.readyNames, "")
}

// AddNamedHealthCheck registers nc as a health check. The name is taken
// from nc.CheckName(); nc.Check is responsible for stamping Response.Name
// (see NamedChecker), so no additional wrapping happens here.
func (c *Check) AddNamedHealthCheck(nc NamedChecker) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.healthChecks = append(c.healthChecks, nc)
}

// AddNamedReadyCheck registers nc as a ready check. See AddNamedHealthCheck
// for naming semantics.
func (c *Check) AddNamedReadyCheck(nc NamedChecker) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readyChecks = append(c.readyChecks, nc)
	c.readyNames = append(c.readyNames, nc.CheckName())
}

// ReadyCheckNames returns the names of currently-registered ready checks
// in registration order. Anonymous checks (registered without a name) are
// returned as empty strings.
func (c *Check) ReadyCheckNames() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]string, len(c.readyNames))
	copy(out, c.readyNames)
	return out
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
		Checks: make(Responses, 0, len(checks)),
	}

	for _, ch := range checks {
		resp := ch.Check(ctx)
		if resp.Status != StatusPass {
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
		Checks: make(Responses, 0, len(checks)),
	}

	for _, ch := range checks {
		resp := ch.Check(ctx)
		if resp.Status != StatusPass {
			response.Status = resp.Status
		}
		response.Checks = append(response.Checks, resp)
	}
	sort.Sort(response.Checks)
	return response
}
