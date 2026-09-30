// Package check standardizes /health and /ready endpoints.
// This allows you to easily know when your server is ready and healthy.
package check

import (
	"context"
	"errors"
	"fmt"
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

// ErrDuplicateCheckName is returned when a check is registered under a name
// already registered in the same set (health or ready).
var ErrDuplicateCheckName = errors.New("duplicate check name")

// ErrEmptyCheckName is returned when a check is registered with an empty name.
var ErrEmptyCheckName = errors.New("empty check name")

const (
	kindHealth = "health"
	kindReady  = "ready"
)

// checkSet holds checks in registration order plus an index of their names,
// so a name can be registered at most once per set. It is not safe for
// concurrent use; Check guards it with mu.
type checkSet struct {
	checks []NamedChecker
	names  map[string]struct{}
}

// add registers nc, rejecting an empty or already-registered name. kind names
// the set in the returned error. A rejected check leaves s unchanged.
func (s *checkSet) add(kind string, nc NamedChecker) error {
	name := nc.CheckName()
	if name == "" {
		return fmt.Errorf("register %s check: %w", kind, ErrEmptyCheckName)
	}
	if _, dup := s.names[name]; dup {
		return fmt.Errorf("register %s check %q: %w", kind, name, ErrDuplicateCheckName)
	}
	if s.names == nil {
		s.names = make(map[string]struct{})
	}
	s.names[name] = struct{}{}
	s.checks = append(s.checks, nc)
	return nil
}

// snapshot returns a copy of the registered checks in registration order.
func (s *checkSet) snapshot() []NamedChecker {
	return append([]NamedChecker(nil), s.checks...)
}

// nameList returns the registered names in registration order. It is never
// nil.
func (s *checkSet) nameList() []string {
	out := make([]string, len(s.checks))
	for i, ch := range s.checks {
		out[i] = ch.CheckName()
	}
	return out
}

// Check holds the named health and ready checks served by /health and
// /ready. Names are unique within each set; the same name may appear once in
// the health set and once in the ready set.
type Check struct {
	mu     sync.RWMutex
	health checkSet
	ready  checkSet

	// frozen reports that Freeze has installed a static snapshot. Once set,
	// the check sets never change again: later registrations are dropped and
	// a second Freeze is a no-op.
	frozen bool
}

// Checker indicates a service whose health can be checked.
//
// Check must return promptly. ctx carries a deadline, but that deadline is
// only cooperative: nothing can interrupt a Check that has already blocked.
// A wedged implementation hangs the /health or /ready request that called
// it, and it hangs Freeze -- neither the per-probe DefaultProbeTimeout that
// Freeze applies nor any backstop its caller wraps around the freeze as a
// whole can preempt the call. A check that never returns can therefore hold
// a process open past the teardown the freeze was taken to precede.
//
// So keep uninterruptible work off the check path. Two patterns cover it:
//
//   - Probe in the background and have Check report the last result. This is
//     the only option when the probe cannot be cancelled at all: a bbolt View
//     runs to completion whatever its caller wants. bolt.KVStore does this --
//     a prober goroutine refreshes a FreshnessResponse on a ticker and Check
//     returns that value, so the uncancellable read never runs inside a probe.
//
//   - Probe inline only through an API that honors ctx, and bound it with
//     BoundDeadline so the latency stays capped even when the caller passes
//     a context with no deadline of its own. sqlite.SqlStore.Check does this
//     around PingContext.
//
// Check may be called concurrently, and it may be called after the subsystem
// it reports on has been closed -- teardown does not deregister checks.
// Report that state as a failure rather than panicking on it.
type Checker interface {
	Check(ctx context.Context) Response
}

// NewCheck returns an empty Check with no default checkers registered.
func NewCheck() *Check {
	return &Check{}
}

// AddNamedHealthCheck registers nc as a health check under nc.CheckName();
// nc.Check is responsible for stamping Response.Name (see NamedChecker), so
// no additional wrapping happens here.
//
// It returns an error wrapping ErrEmptyCheckName for an empty name, or
// ErrDuplicateCheckName for a name already registered as a health check; the
// earlier registration is kept. A ready check of the same name does not
// conflict.
//
// A registration after Freeze is dropped and returns nil; see there.
func (c *Check) AddNamedHealthCheck(nc NamedChecker) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.frozen {
		return nil
	}
	return c.health.add(kindHealth, nc)
}

// AddNamedReadyCheck registers nc as a ready check. See AddNamedHealthCheck
// for naming, uniqueness, and what a registration after Freeze does; ready
// names are unique among ready checks only.
func (c *Check) AddNamedReadyCheck(nc NamedChecker) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.frozen {
		return nil
	}
	return c.ready.add(kindReady, nc)
}

// ReadyCheckNames returns the names of currently-registered ready checks
// in registration order. Registration rejects empty and duplicate names, so
// every entry is non-empty and distinct.
func (c *Check) ReadyCheckNames() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ready.nameList()
}

// CheckHealth evaluates c's set of health checks and returns a populated Response.
func (c *Check) CheckHealth(ctx context.Context) Response {
	return c.evaluate(ctx, NameHealth, c.snapshotHealth)
}

// CheckReady evaluates c's set of ready checks and returns a populated Response.
func (c *Check) CheckReady(ctx context.Context) Response {
	return c.evaluate(ctx, NameReady, c.snapshotReady)
}

func (c *Check) snapshotHealth() []NamedChecker {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.health.snapshot()
}

func (c *Check) snapshotReady() []NamedChecker {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ready.snapshot()
}

// evaluate runs every checker returned by snap and aggregates the
// responses into a single BasicResponse. The snap callback is taken
// under the read lock and the lock is released before any Check runs:
// checkers can block (network calls) or re-enter registration, so we
// must not hold c.mu across Check invocations.
func (c *Check) evaluate(ctx context.Context, name string, snap func() []NamedChecker) Response {
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

// frozenChecker answers with a fixed Response. It implements NamedChecker so
// evaluate needs no special case: to everything downstream a frozen check is
// an ordinary registered check that happens never to change its mind.
//
// name is the registration name of the check it replaced, not resp.Name():
// a checker that breaks the NamedChecker contract by stamping some other name
// must not be able to rename its entry, or collide with another, by being
// frozen.
type frozenChecker struct {
	name string
	resp BasicResponse
}

func (f frozenChecker) CheckName() string              { return f.name }
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
// The registered names and their order are unchanged -- each frozen check
// keeps the CheckName it was registered under, not the name its response
// carried -- so ReadyCheckNames reports what it did before and no name can
// become duplicated. Only the values are pinned.
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
	health := c.health.snapshot()
	ready := c.ready.snapshot()
	c.mu.RUnlock()
	if frozen {
		return
	}

	// Evaluate with no lock held, for the reason evaluate documents: a checker
	// can block on a network call and can re-enter registration. The frozen
	// sets are built fresh from the snapshots rather than patched in place, so
	// a check registered in the gap between the two locks -- and therefore
	// never probed -- is dropped from the checks and the name index together.
	frozenHealth := freezeSet(ctx, health)
	frozenReady := freezeSet(ctx, ready)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.frozen {
		return
	}
	c.health = frozenHealth
	c.ready = frozenReady
	c.frozen = true
}

// freezeSet probes every check in checks and returns a new checkSet of their
// frozen replacements, under the same registration names and in the same
// order. checks is a snapshot of a checkSet, whose names are already unique,
// so the result's names are too.
func freezeSet(ctx context.Context, checks []NamedChecker) checkSet {
	s := checkSet{
		checks: make([]NamedChecker, len(checks)),
		names:  make(map[string]struct{}, len(checks)),
	}
	for i, ch := range checks {
		name := ch.CheckName()
		s.checks[i] = frozenChecker{name: name, resp: probe(ctx, ch)}
		s.names[name] = struct{}{}
	}
	return s
}
