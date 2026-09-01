package check

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"
)

// NamedChecker is a superset of Checker that also indicates the name of
// the service. Implementations MUST stamp Response.Name() with
// CheckName() before returning from Check, so that AddNamedHealthCheck /
// AddNamedReadyCheck can register the value without an extra wrapper.
// Use Named to attach a name to a Checker that does not already do so.
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
// so AddNamedHealthCheck/AddNamedReadyCheck can record the name without
// invoking Check, and its Check method stamps the name onto the Response.
type namedChecker struct {
	name    string
	checker Checker
}

func (n namedChecker) CheckName() string { return n.name }

func (n namedChecker) Check(ctx context.Context) Response {
	return Rename(n.checker.Check(ctx), n.name)
}

// Rename returns r with its reported name replaced by name. A
// BasicResponse is rewritten by value via WithName; any other
// Response is wrapped in a renamedResponse so the inner type's state
// (e.g. FreshnessResponse) is not copied. Used by namedChecker and
// by callers that compose a Response themselves and need to stamp a
// name without knowing the concrete implementation.
func Rename(r Response, name string) Response {
	if br, ok := r.(BasicResponse); ok {
		return br.WithName(name)
	}
	return renamedResponse{name: name, inner: r}
}

// renamedResponse overrides Name() on an inner Response without copying
// state. Used by Rename when the inner Response is stateful (e.g.
// FreshnessResponse) so WithName cannot be applied as a value copy.
type renamedResponse struct {
	name  string
	inner Response
}

func (r renamedResponse) Name() string      { return r.name }
func (r renamedResponse) Status() Status    { return r.inner.Status() }
func (r renamedResponse) Message() string   { return r.inner.Message() }
func (r renamedResponse) Checks() Responses { return r.inner.Checks() }

// Snapshot implements Snapshotter by delegating to the inner Response, so a
// rename does not cost the caller the single coherent read: without this, a
// renamed *FreshnessResponse would be flattened through the four accessors and
// could report a status from one observation with a message from the next.
func (r renamedResponse) Snapshot() BasicResponse {
	if s, ok := r.inner.(Snapshotter); ok {
		return s.Snapshot().WithName(r.name)
	}
	return NewBasicResponse(r.name, r.inner.Status(), r.inner.Message(), r.inner.Checks())
}

// MarshalJSON emits the renamed wire shape from a single snapshot, for the
// reason FreshnessResponse.MarshalJSON does.
func (r renamedResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.Snapshot())
}

// Named returns a NamedChecker that delegates to checker and stamps name
// onto the response. Callers attach a human-readable name to a response
// without encoding that logic into the underlying check.
func Named(name string, checker Checker) NamedChecker {
	return namedChecker{name: name, checker: checker}
}

// NamedFunc is the same as Named except it takes a CheckerFunc.
func NamedFunc(name string, fn CheckerFunc) NamedChecker {
	return Named(name, fn)
}

// ErrCheck will create a health checker that executes a function. If the function returns an error,
// it will return an unhealthy response. Otherwise, it will be as if the Ok function was called.
// Note: it is better to use CheckerFunc, because with Check, the context is ignored.
func ErrCheck(fn func() error) Checker {
	return CheckerFunc(func(_ context.Context) Response {
		if err := fn(); err != nil {
			return Error(err)
		}
		return Pass()
	})
}

// basic builds a BasicResponse with no nested checks. Internal helper
// used by the Pass/Info/Error/Fail/NamedPass/NamedFail factories so
// each call site reads as "this response has name/status/message"
// rather than repeating the BasicResponse{wireResponse{...}} literal.
// Use NewBasicResponse when nested checks are needed.
func basic(name string, status Status, msg string) BasicResponse {
	return BasicResponse{wireResponse{Name: name, Status: status, Message: msg}}
}

// Pass is a utility function to generate a passing status response with the default parameters.
func Pass() BasicResponse { return basic("", StatusPass, "") }

// Info is a utility function to generate a healthy status with a printf message.
func Info(msg string, args ...interface{}) BasicResponse {
	return basic("", StatusPass, fmt.Sprintf(msg, args...))
}

// Error is a utility function for creating a failing response from an error.
func Error(err error) BasicResponse { return basic("", StatusFail, err.Error()) }

// Fail is a utility function for creating a failing response with a fixed message.
func Fail(msg string) BasicResponse { return basic("", StatusFail, msg) }

// NamedPass builds a passing response with the given name.
func NamedPass(name string) BasicResponse { return basic(name, StatusPass, "") }

// NamedFail builds a failing response with the given name and message.
func NamedFail(name, msg string) BasicResponse { return basic(name, StatusFail, msg) }

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

// MsgNotReady is the message a ReadyGate reports when it has been neither
// fired by Ready nor failed by Fail: the subsystem is still initializing and
// there is nothing more to say about it.
const MsgNotReady = "not ready"

// ReadyGate is a NamedChecker that reports StatusFail until Ready is called,
// after which it reports StatusPass. It is intended for gating readiness on
// subsystem init phases that complete once at startup.
//
// A gate whose phase failed outright can be latched with Fail, after which it
// reports the failure message in place of MsgNotReady.
type ReadyGate struct {
	name    string
	ready   atomic.Bool
	failMsg atomic.Pointer[string] // terminal startup failure; see Fail
}

// NewReadyGate returns a ReadyGate that initially reports StatusFail.
func NewReadyGate(name string) *ReadyGate {
	return &ReadyGate{name: name}
}

// CheckName returns the configured name of the gate.
func (g *ReadyGate) CheckName() string { return g.name }

// Check returns StatusPass once Ready has been called, otherwise StatusFail.
// A gate latched by Fail reports StatusFail carrying the failure message,
// whatever Ready and Unready have done. The response is stamped with the
// gate's name to satisfy the NamedChecker contract.
func (g *ReadyGate) Check(context.Context) Response {
	if msg := g.failMsg.Load(); msg != nil {
		return NamedFail(g.name, *msg)
	}
	if g.ready.Load() {
		return NamedPass(g.name)
	}
	return NamedFail(g.name, MsgNotReady)
}

// Ready flips the gate to report StatusPass.
func (g *ReadyGate) Ready() { g.ready.Store(true) }

// Unready flips the gate to report StatusFail.
func (g *ReadyGate) Unready() { g.ready.Store(false) }

// Fail latches a terminal startup failure carrying err's message, so the gate
// reports why it will never become ready rather than a bare MsgNotReady. The
// first error wins; a nil err is ignored.
//
// Startup-only: Fail outranks both Ready and Unready and cannot be cleared, so
// a runtime degradation monitor must call Unready, never Fail.
func (g *ReadyGate) Fail(err error) {
	if isNilError(err) {
		return
	}
	// Resolve the message here, while the caller still holds the error, rather
	// than storing the error and calling Error() from an HTTP handler later.
	msg := err.Error()
	g.failMsg.CompareAndSwap(nil, &msg)
}

// isNilError reports whether err is nil, including the typed-nil-in-interface
// case ((*T)(nil) stored in an error) where err == nil is false but calling
// Error() panics. A latched gate is read from an HTTP handler, so a panic here
// would turn a startup failure into a 500 on /health and /ready.
func isNilError(err error) bool {
	if err == nil {
		return true
	}
	// Kinds outside this set cannot be nil, and IsNil panics on them.
	switch v := reflect.ValueOf(err); v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
