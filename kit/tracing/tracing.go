package tracing

import (
	"context"
	"errors"
	"net/http"
	"runtime"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
)

// LogError adds a span log for an error.
// Returns unchanged error, so useful to wrap as in:
//  return 0, tracing.LogError(err)
func LogError(span opentracing.Span, err error) error {
	span.LogFields(log.Error(err))
	return err
}

// InjectToHTTPRequest adds tracing headers to an HTTP request.
// Easier than adding this boilerplate everywhere.
func InjectToHTTPRequest(span opentracing.Span, req *http.Request) {
	err := opentracing.GlobalTracer().Inject(span.Context(), opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(req.Header))
	if err != nil {
		span.LogFields(log.String("trace-inject-error", err.Error()))
	}
}

// ExtractFromHTTPRequest gets a child span of the parent referenced in HTTP request headers.
// Easier than adding this boilerplate everywhere.
func ExtractFromHTTPRequest(req *http.Request, handlerName string) (opentracing.Span, *http.Request) {
	spanContext, err := opentracing.GlobalTracer().Extract(opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(req.Header))
	if err != nil {
		span, ctx := opentracing.StartSpanFromContext(req.Context(), handlerName+":"+req.URL.Path)
		span.LogFields(log.String("trace-extract-error", err.Error()))
		req = req.WithContext(ctx)
		return span, req
	}

	span := opentracing.StartSpan(handlerName+":"+req.URL.Path, opentracing.ChildOf(spanContext))
	req = req.WithContext(opentracing.ContextWithSpan(req.Context(), span))
	return span, req
}

// StartSpanFromContext is an improved opentracing.StartSpanFromContext.
// Uses the calling function as the operation name, and logs the filename and line number.
//
// Passing nil context triggers new context and root span construction.
// Context without parent span reference triggers root span construction.
// This function never returns nil values.
//
// Performance
//
// This function incurs a small performance penalty, roughly 1000 ns/op, 376 B/op, 6 allocs/op.
// Jaeger timestamp and duration precision is only µs, so this is pretty negligible.
//
// Alternatives
//
// If this performance penalty is too much, try these, which are also demonstrated in benchmark tests:
//  // Create a root span
//  span := opentracing.StartSpan("operation name")
//
//  // Create a child span
//  span := opentracing.StartSpan("operation name", opentracing.ChildOf(sc))
//
//  // Sugar to create a child span
//  span, ctx := opentracing.StartSpanFromContext(ctx, "operation name")
func StartSpanFromContext(ctx context.Context) (opentracing.Span, context.Context) {
	if ctx == nil {
		// Guard against nil context.
		ctx = context.Background()
	}

	var frame runtime.Frame
	{
		// Get caller frame.
		pc, _, _, ok := runtime.Caller(1)
		if !ok {
			span, ctx := opentracing.StartSpanFromContext(ctx, "unknown")
			span.LogFields(log.Error(errors.New("runtime.Caller failed")))
			return span, ctx
		}

		frame, _ = runtime.CallersFrames([]uintptr{pc}).Next()
	}

	var span opentracing.Span
	if parentSpan := opentracing.SpanFromContext(ctx); parentSpan != nil {
		// Create a child span.
		span = opentracing.StartSpan(frame.Function, opentracing.ChildOf(parentSpan.Context()))
	} else {
		// Create a root span.
		span = opentracing.StartSpan(frame.Function)
	}
	// New context references this span, not the parent (if there was one).
	ctx = opentracing.ContextWithSpan(ctx, span)

	span.LogFields(log.String("filename", frame.File), log.Int("line", frame.Line))

	return span, ctx
}
