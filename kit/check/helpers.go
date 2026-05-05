package check

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

// NamedChecker is a superset of Checker that also indicates the name of the service.
// Prefer to implement NamedChecker if your service has a fixed name,
// as opposed to calling *Health.AddNamed.
type NamedChecker interface {
	Checker
	CheckName() string
}

// CheckerFunc is an adapter of a plain func() error to the Checker interface.
type CheckerFunc func(ctx context.Context) Response

// Check implements Checker.
func (f CheckerFunc) Check(ctx context.Context) Response {
	return f(ctx)
}

// namedChecker pairs a Checker with a fixed name. It implements NamedChecker
// so AddHealthCheck/AddReadyCheck can record the name without invoking
// Check, and its Check method stamps the name onto the Response.
type namedChecker struct {
	name    string
	checker Checker
}

func (n namedChecker) CheckName() string { return n.name }

func (n namedChecker) Check(ctx context.Context) Response {
	resp := n.checker.Check(ctx)
	resp.Name = n.name
	return resp
}

// Named returns a Checker that also implements NamedChecker, delegates to
// checker, and stamps name onto the response. This way callers can attach
// a human-readable name to a response without encoding that logic into the
// underlying check.
func Named(name string, checker Checker) Checker {
	return namedChecker{name: name, checker: checker}
}

// NamedFunc is the same as Named except it takes a CheckerFunc.
func NamedFunc(name string, fn CheckerFunc) Checker {
	return Named(name, fn)
}

// ErrCheck will create a health checker that executes a function. If the function returns an error,
// it will return an unhealthy response. Otherwise, it will be as if the Ok function was called.
// Note: it is better to use CheckFunc, because with Check, the context is ignored.
func ErrCheck(fn func() error) Checker {
	return CheckerFunc(func(_ context.Context) Response {
		if err := fn(); err != nil {
			return Error(err)
		}
		return Pass()
	})
}

// Pass is a utility function to generate a passing status response with the default parameters.
func Pass() Response {
	return Response{
		Status: StatusPass,
	}
}

// Info is a utility function to generate a healthy status with a printf message.
func Info(msg string, args ...interface{}) Response {
	return Response{
		Status:  StatusPass,
		Message: fmt.Sprintf(msg, args...),
	}
}

// Error is a utility function for creating a response from an error message.
func Error(err error) Response {
	return Response{
		Status:  StatusFail,
		Message: err.Error(),
	}
}

// DefaultProbeTimeout is the recommended upper bound for a single
// Check probe. It keeps aggregate /health latency bounded when a
// subsystem is wedged, while still leaving room for one slow round-trip.
const DefaultProbeTimeout = 500 * time.Millisecond

// BoundDeadline caps ctx's deadline at max. If ctx already has a deadline
// at or inside max, it is returned unchanged with a no-op cancel; otherwise
// a child context with a max-duration deadline is returned. Callers must
// always defer the returned cancel.
//
// Intended for Check implementations that want a bounded probe latency
// even when /health callers pass context.Background().
func BoundDeadline(ctx context.Context, max time.Duration) (context.Context, context.CancelFunc) {
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= max {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, max)
}

// ReadyGate is a NamedChecker that reports StatusFail until Ready is called,
// after which it reports StatusPass. It is intended for gating readiness on
// subsystem init phases that complete once at startup.
type ReadyGate struct {
	name  string
	ready atomic.Bool
}

// NewReadyGate returns a ReadyGate that initially reports StatusFail.
func NewReadyGate(name string) *ReadyGate {
	return &ReadyGate{name: name}
}

// CheckName returns the configured name of the gate.
func (g *ReadyGate) CheckName() string { return g.name }

// Check returns StatusPass once Ready has been called, otherwise StatusFail.
func (g *ReadyGate) Check(context.Context) Response {
	if g.ready.Load() {
		return Pass()
	}
	return Response{Status: StatusFail, Message: "not ready"}
}

// Ready flips the gate to report StatusPass.
func (g *ReadyGate) Ready() { g.ready.Store(true) }

// Unready flips the gate to report StatusFail.
func (g *ReadyGate) Unready() { g.ready.Store(false) }
