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

	// frozen reports that Freeze has installed a static snapshot. Once set,
	// the check sets never change again: later registrations are dropped and
	// a second Freeze is a no-op.
	frozen bool
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
// A registration after Freeze is ignored; see there.
func (c *Check) AddHealthCheck(check Checker) {
	if nc, ok := check.(NamedChecker); ok {
		c.AddNamedHealthCheck(nc)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.frozen {
		return
	}
	c.healthChecks = append(c.healthChecks, check)
}

// AddNamedHealthCheck registers nc as a health check. The name is taken
// from nc.CheckName(); nc.Check is responsible for stamping Response.Name
// (see NamedChecker), so no additional wrapping happens here.
//
// A registration after Freeze is ignored; see there.
func (c *Check) AddNamedHealthCheck(nc NamedChecker) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.frozen {
		return
	}
	c.healthChecks = append(c.healthChecks, nc)
}

// AddNamedReadyCheck registers nc as a ready check. See AddNamedHealthCheck
// for naming semantics and for what a registration after Freeze does.
func (c *Check) AddNamedReadyCheck(nc NamedChecker) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.frozen {
		return
	}
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

// frozenChecker answers with a fixed Response. It implements NamedChecker so a
// frozen set can rebuild readyNames and so evaluate needs no special case: to
// everything downstream a frozen check is an ordinary registered check that
// happens never to change its mind.
type frozenChecker struct{ resp BasicResponse }

func (f frozenChecker) CheckName() string              { return f.resp.Name() }
func (f frozenChecker) Check(context.Context) Response { return f.resp }

// probe evaluates ch for the freeze, under a context of its own bounded at
// DefaultProbeTimeout, and flattens what it returns.
//
// The bound is per probe rather than one budget shared across the set, and that
// distinction is the whole of this function. A shared budget spent by an early
// slow checker leaves every checker after it running on a dead context, and a
// dead context does not yield "unknown": sqlite.SqlStore.Check, for one, turns
// it into NamedFail(name, "context deadline exceeded"). Responses sorts
// failures ahead of passes and then by name, and /health's top-level message is
// the first of them, so a subsystem that merely ran out of someone else's time
// could outrank and mask the failure the freeze was taken to preserve -- the
// exact drift Freeze exists to prevent, reintroduced by its own timeout.
//
// Bounding each probe separately costs a worst case of one DefaultProbeTimeout
// per registered check, reached only if every subsystem is wedged at once. A
// checker that ignores its context entirely (a bbolt View cannot be cancelled)
// is unbounded either way, so the shared budget never bought that back.
func probe(ctx context.Context, ch Checker) BasicResponse {
	probeCtx, cancel := BoundDeadline(ctx, DefaultProbeTimeout)
	defer cancel()
	return snapshot(ch.Check(probeCtx))
}

// Freeze replaces every registered health and ready check with a static
// snapshot of what it reports now, so CheckHealth and CheckReady go on
// returning that same answer for the life of the process.
//
// It exists for terminal states. A process on its way out tears its subsystems
// down, and their checks then report that deliberate teardown as a fresh
// failure; because Responses sort failures first and then by name, a closed
// store can outrank -- and so mask -- the failure that made the process
// terminal. Freezing first preserves the report as it stood when that decision
// was made.
//
// Each snapshot is flattened into a BasicResponse so a live Response cannot
// keep moving inside the frozen set: a *FreshnessResponse ages into a
// staleness failure on its own once its prober stops. Both render the same
// JSON object, so a frozen body has the same shape as the one served a moment
// earlier.
//
// The registered set and its order are unchanged, so ReadyCheckNames reports
// what it did before. Only the values are pinned.
//
// Freeze is terminal and first-freeze-wins: a second call is a no-op, there is
// no thaw, and checks registered afterwards are ignored. A registration racing
// the freeze may or may not be captured, which is why the caller must be the
// one thing still running.
//
// Every probe is bounded on its own, at DefaultProbeTimeout, rather than out of
// one budget shared by the whole set; see probe. ctx is their parent, so a
// deadline on it still caps the freeze as a whole -- give it one only as a
// backstop, generous enough that a healthy freeze never reaches it. Once it
// expires the remaining probes run on a dead context, and a cancelled probe
// records the freeze itself rather than the state being frozen. For the same
// reason, pass a context that a signal cannot cancel.
func (c *Check) Freeze(ctx context.Context) {
	c.mu.RLock()
	frozen := c.frozen
	health := append([]Checker(nil), c.healthChecks...)
	ready := append([]Checker(nil), c.readyChecks...)
	c.mu.RUnlock()
	if frozen {
		return
	}

	// Evaluate with no lock held, for the reason evaluate documents: a checker
	// can block on a network call and can re-enter registration.
	frozenHealth := make([]Checker, len(health))
	for i, ch := range health {
		frozenHealth[i] = frozenChecker{resp: probe(ctx, ch)}
	}
	// Ready names are rebuilt from the frozen responses rather than carried
	// over, so a check registered in the gap between the two locks -- and
	// therefore absent from the frozen set -- leaves both lists together.
	frozenReady := make([]Checker, len(ready))
	readyNames := make([]string, len(ready))
	for i, ch := range ready {
		resp := probe(ctx, ch)
		frozenReady[i] = frozenChecker{resp: resp}
		readyNames[i] = resp.Name()
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.frozen {
		return
	}
	c.healthChecks = frozenHealth
	c.readyChecks = frozenReady
	c.readyNames = readyNames
	c.frozen = true
}
