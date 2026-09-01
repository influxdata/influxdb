package check

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// mutableCheck is a NamedChecker whose answer the test can change after
// registration, so a frozen set can be shown not to follow it. It counts its
// invocations, which is what pins Freeze as terminal: a second Freeze that
// re-evaluated the checkers would show up here even when the resulting
// snapshot happened to be identical.
type mutableCheck struct {
	name  string
	resp  atomic.Pointer[BasicResponse]
	calls atomic.Int64
}

func newMutableCheck(name string, status Status, msg string) *mutableCheck {
	m := &mutableCheck{name: name}
	m.set(status, msg)
	return m
}

func (m *mutableCheck) set(status Status, msg string) {
	r := NewBasicResponse(m.name, status, msg, nil)
	m.resp.Store(&r)
}

func (m *mutableCheck) CheckName() string { return m.name }

func (m *mutableCheck) Check(context.Context) Response {
	m.calls.Add(1)
	return *m.resp.Load()
}

// fixedChecker exposes an existing Response as a NamedChecker without copying
// or wrapping it, so a test can hand the check set a live *FreshnessResponse
// and still hold the pointer. This is how bolt.KVStore registers its own
// freshness-backed check.
type fixedChecker struct {
	name string
	resp Response
}

func (f fixedChecker) CheckName() string              { return f.name }
func (f fixedChecker) Check(context.Context) Response { return f.resp }

// TestCheck_Freeze_PinsValues covers the whole point of the freeze: what the
// checks reported at the moment of the call is what they go on reporting,
// however the subsystems behind them behave afterwards.
func TestCheck_Freeze_PinsValues(t *testing.T) {
	ctx := context.Background()
	c := NewCheck()
	h := newMutableCheck("h", StatusPass, "healthy")
	r := newMutableCheck("r", StatusFail, "why it failed")
	c.AddNamedHealthCheck(h)
	c.AddNamedReadyCheck(r)

	c.Freeze(ctx)

	// Teardown, as far as these checkers are concerned: the passing one starts
	// failing and the failing one starts passing. Both must be ignored.
	h.set(StatusFail, "torn down")
	r.set(StatusPass, "")

	health := c.CheckHealth(ctx)
	require.Equal(t, StatusPass, health.Status())
	require.Len(t, health.Checks(), 1)
	require.Equal(t, "h", health.Checks()[0].Name())
	require.Equal(t, StatusPass, health.Checks()[0].Status())
	require.Equal(t, "healthy", health.Checks()[0].Message())

	ready := c.CheckReady(ctx)
	require.Equal(t, StatusFail, ready.Status())
	require.Len(t, ready.Checks(), 1)
	require.Equal(t, "r", ready.Checks()[0].Name())
	require.Equal(t, StatusFail, ready.Checks()[0].Status())
	require.Equal(t, "why it failed", ready.Checks()[0].Message())
}

// TestCheck_Freeze_FlattensFreshnessResponse is the flattening pin. A
// *FreshnessResponse retained by pointer inside the frozen set would age into
// a staleness failure on its own once its prober stopped, which is exactly the
// drift the freeze exists to prevent.
func TestCheck_Freeze_FlattensFreshnessResponse(t *testing.T) {
	ctx := context.Background()
	const staleness = 20 * time.Millisecond

	f := NewFreshnessResponse("probed", staleness)
	f.Update(Pass())

	c := NewCheck()
	c.AddNamedHealthCheck(fixedChecker{name: "probed", resp: f})
	c.Freeze(ctx)

	time.Sleep(3 * staleness)
	require.Equal(t, StatusFail, f.Status(),
		"the live response must age out, or this test proves nothing")

	resp := c.CheckHealth(ctx)
	require.Equal(t, StatusPass, resp.Status())
	require.Len(t, resp.Checks(), 1)
	require.Equal(t, StatusPass, resp.Checks()[0].Status())
	require.NotContains(t, resp.Checks()[0].Message(), "stale:")
}

// TestCheck_Freeze_FlattensNestedChecks covers the recursion: a live Response
// reached only through another response's Checks() ages just as readily as a
// top-level one.
func TestCheck_Freeze_FlattensNestedChecks(t *testing.T) {
	ctx := context.Background()
	const staleness = 20 * time.Millisecond

	inner := NewFreshnessResponse("inner", staleness)
	inner.Update(Pass())
	outer := NewBasicResponse("outer", StatusPass, "", Responses{inner})

	c := NewCheck()
	c.AddNamedHealthCheck(fixedChecker{name: "outer", resp: outer})
	c.Freeze(ctx)

	time.Sleep(3 * staleness)
	require.Equal(t, StatusFail, inner.Status(),
		"the live nested response must age out, or this test proves nothing")

	resp := c.CheckHealth(ctx)
	require.Len(t, resp.Checks(), 1)
	nested := resp.Checks()[0].Checks()
	require.Len(t, nested, 1)
	require.Equal(t, "inner", nested[0].Name())
	require.Equal(t, StatusPass, nested[0].Status())
	require.NotContains(t, nested[0].Message(), "stale:")
}

// TestCheck_Freeze_PreservesReadyCheckNames pins registration order across the
// freeze. ReadyCheckNames means registration order -- CheckReady's aggregate is
// sorted, and building the frozen set from that would silently reorder it.
func TestCheck_Freeze_PreservesReadyCheckNames(t *testing.T) {
	ctx := context.Background()
	c := NewCheck()
	// Deliberately not alphabetical, and deliberately not sorted by status
	// either, so a set rebuilt from the sorted aggregate could not match.
	for _, name := range []string{"zulu", "alpha", "mike"} {
		c.AddNamedReadyCheck(newMutableCheck(name, StatusFail, MsgNotReady))
	}
	c.AddNamedReadyCheck(newMutableCheck("bravo", StatusPass, ""))

	before := c.ReadyCheckNames()
	c.Freeze(ctx)
	require.Equal(t, before, c.ReadyCheckNames())
	require.Equal(t, []string{"zulu", "alpha", "mike", "bravo"}, c.ReadyCheckNames())
}

// TestCheck_Freeze_IsTerminal pins first-freeze-wins and the registration
// guard. The call count is what makes "terminal" an assertion rather than an
// inference: a second Freeze that re-ran the checkers would be visible here
// even though the second snapshot would look the same.
func TestCheck_Freeze_IsTerminal(t *testing.T) {
	ctx := context.Background()
	c := NewCheck()
	h := newMutableCheck("h", StatusPass, "first")
	c.AddNamedHealthCheck(h)

	c.Freeze(ctx)
	require.Equal(t, int64(1), h.calls.Load())

	h.set(StatusFail, "second")
	c.Freeze(ctx)
	require.Equal(t, int64(1), h.calls.Load(), "a second Freeze re-evaluated the checkers")

	// All three registration paths: the named health check, the anonymous one,
	// and the ready check.
	c.AddNamedHealthCheck(newMutableCheck("late-named", StatusFail, "after the freeze"))
	c.AddHealthCheck(CheckerFunc(func(context.Context) Response {
		return NamedFail("late-anonymous", "after the freeze")
	}))
	c.AddNamedReadyCheck(newMutableCheck("late-ready", StatusFail, "after the freeze"))

	health := c.CheckHealth(ctx)
	require.Equal(t, StatusPass, health.Status())
	require.Len(t, health.Checks(), 1)
	require.Equal(t, "first", health.Checks()[0].Message())

	require.Empty(t, c.CheckReady(ctx).Checks())
	require.Empty(t, c.ReadyCheckNames())
}

// statusByName indexes an aggregate's sub-checks so a test can assert about one
// of them without depending on the order Responses sorts them into.
func statusByName(rs Responses) map[string]Response {
	out := make(map[string]Response, len(rs))
	for _, r := range rs {
		out[r.Name()] = r
	}
	return out
}

// TestCheck_Freeze_BoundsEachProbeSeparately pins that every probe gets a
// context of its own rather than a share of one budget spent in registration
// order.
//
// A shared budget leaves every checker after a slow one running on a dead
// context, and a dead context does not read as "unknown" downstream: a real
// checker turns it into a failure with the cancellation as its message
// (sqlite.SqlStore.Check does exactly this). Responses sorts failures ahead of
// passes and then by name, and /health's top-level message is the first of
// them, so a subsystem that merely ran out of someone else's time could outrank
// and mask the failure the freeze was taken to preserve -- the drift Freeze
// exists to prevent, reintroduced by its own timeout.
//
// The test fails three different ways, which is the point: unbounded probes
// hang on the first checker, a shared budget fails the assertions on the two
// after it, and only per-probe bounds pass.
func TestCheck_Freeze_BoundsEachProbeSeparately(t *testing.T) {
	c := NewCheck()

	// Sorts first and spends its entire probe budget. Under one shared budget
	// it would spend everyone else's with it.
	c.AddNamedHealthCheck(NamedFunc("a-slow", func(ctx context.Context) Response {
		<-ctx.Done()
		return NamedFail("a-slow", ctx.Err().Error())
	}))
	// Reports whether it was given any time of its own.
	c.AddNamedHealthCheck(NamedFunc("b-fast", func(ctx context.Context) Response {
		if err := ctx.Err(); err != nil {
			return NamedFail("b-fast", err.Error())
		}
		return NamedPass("b-fast")
	}))
	// The ready set is evaluated after the health set, so a shared budget is
	// already gone by the time it is reached.
	c.AddNamedReadyCheck(NamedFunc("c-ready", func(ctx context.Context) Response {
		if err := ctx.Err(); err != nil {
			return NamedFail("c-ready", err.Error())
		}
		return NamedPass("c-ready")
	}))

	// No deadline of its own: whatever bounds a probe here, Freeze applied.
	c.Freeze(context.Background())

	health := statusByName(c.CheckHealth(context.Background()).Checks())
	require.Len(t, health, 2)
	require.Equal(t, StatusFail, health["a-slow"].Status(),
		"the slow checker must have been bounded at all, or this proves nothing")
	require.Equal(t, context.DeadlineExceeded.Error(), health["a-slow"].Message(),
		"the bound must be a deadline of its own, not an inherited cancel")
	require.Equal(t, StatusPass, health["b-fast"].Status(),
		"a slow probe spent the budget of the check registered after it: %s",
		health["b-fast"].Message())

	ready := statusByName(c.CheckReady(context.Background()).Checks())
	require.Equal(t, StatusPass, ready["c-ready"].Status(),
		"a slow health probe spent the budget of the ready checks: %s",
		ready["c-ready"].Message())
}
