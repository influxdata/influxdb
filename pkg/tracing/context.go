package tracing

import "context"

type (
	spanContextKey  struct{}
	traceContextKey struct{}
)

// NewContextWithSpan returns a new context with the given Span added.
func NewContextWithSpan(ctx context.Context, c *Span) context.Context {
	return context.WithValue(ctx, spanContextKey{}, c)
}

// SpanFromContext returns the Span associated with ctx or nil if no Span has been assigned.
func SpanFromContext(ctx context.Context) *Span {
	c, _ := ctx.Value(spanContextKey{}).(*Span)
	return c
}

// NewContextWithTrace returns a new context with the given Trace added.
func NewContextWithTrace(ctx context.Context, t *Trace) context.Context {
	return context.WithValue(ctx, traceContextKey{}, t)
}

// TraceFromContext returns the Trace associated with ctx or nil if no Trace has been assigned.
func TraceFromContext(ctx context.Context) *Trace {
	c, _ := ctx.Value(traceContextKey{}).(*Trace)
	return c
}
