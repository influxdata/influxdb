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

	// NameHealth is the Name carried by the aggregate Response returned
	// from CheckHealth.
	NameHealth = "Health"
	// NameReady is the Name carried by the aggregate Response returned
	// from CheckReady.
	NameReady = "Ready"
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
// in registration order. All ready checks are required to be named, so
// no entry is ever empty.
func (c *Check) ReadyCheckNames() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]string, len(c.readyNames))
	copy(out, c.readyNames)
	return out
}

// CheckHealth evaluates c's set of health checks and returns a populated Response.
func (c *Check) CheckHealth(ctx context.Context) Response {
	return c.evaluate(ctx, NameHealth, c.snapshotHealth)
}

// CheckReady evaluates c's set of ready checks and returns a populated Response.
func (c *Check) CheckReady(ctx context.Context) Response {
	return c.evaluate(ctx, NameReady, c.snapshotReady)
}

func (c *Check) snapshotHealth() []Checker {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return append([]Checker(nil), c.healthChecks...)
}

func (c *Check) snapshotReady() []Checker {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return append([]Checker(nil), c.readyChecks...)
}

// evaluate runs every checker returned by snap and aggregates the
// responses into a single BasicResponse. The snap callback is taken
// under the read lock and the lock is released before any Check runs:
// checkers can block (network calls) or re-enter registration, so we
// must not hold c.mu across Check invocations.
func (c *Check) evaluate(ctx context.Context, name string, snap func() []Checker) Response {
	checks := snap()
	results := make(Responses, 0, len(checks))
	overall := StatusPass
	for _, ch := range checks {
		resp := ch.Check(ctx)
		// Cache Status() to one call: a stateful Response (e.g.
		// FreshnessResponse) may observe a different snapshot on a
		// second invocation, which would let overall disagree with
		// the value appended into results.
		if s := resp.Status(); s != StatusPass {
			overall = s
		}
		results = append(results, resp)
	}
	sort.Sort(results)
	return NewBasicResponse(name, overall, "", results)
}
