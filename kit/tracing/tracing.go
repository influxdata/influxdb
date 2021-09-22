package tracing

import (
	"context"
	"errors"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/go-chi/chi"
	"github.com/influxdata/httprouter"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/uber/jaeger-client-go"
)

// LogError adds a span log for an error.
// Returns unchanged error, so useful to wrap as in:
//  return 0, tracing.LogError(err)
func LogError(span opentracing.Span, err error) error {
	if err == nil {
		return nil
	}

	// Get caller frame.
	var pcs [1]uintptr
	n := runtime.Callers(2, pcs[:])
	if n < 1 {
		span.LogFields(log.Error(err))
		span.LogFields(log.Error(errors.New("runtime.Callers failed")))
		return err
	}

	file, line := runtime.FuncForPC(pcs[0]).FileLine(pcs[0])
	span.LogFields(log.String("filename", file), log.Int("line", line), log.Error(err))

	return err
}

// InjectToHTTPRequest adds tracing headers to an HTTP request.
// Easier than adding this boilerplate everywhere.
func InjectToHTTPRequest(span opentracing.Span, req *http.Request) {
	err := opentracing.GlobalTracer().Inject(span.Context(), opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(req.Header))
	if err != nil {
		LogError(span, err)
	}
}

// ExtractFromHTTPRequest gets a child span of the parent referenced in HTTP request headers.
// Returns the request with updated tracing context.
// Easier than adding this boilerplate everywhere.
func ExtractFromHTTPRequest(req *http.Request, handlerName string) (opentracing.Span, *http.Request) {
	spanContext, err := opentracing.GlobalTracer().Extract(opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(req.Header))
	if err != nil {
		span, ctx := opentracing.StartSpanFromContext(req.Context(), "request")
		annotateSpan(span, handlerName, req)

		_ = LogError(span, err)

		return span, req.WithContext(ctx)
	}

	span := opentracing.StartSpan("request", opentracing.ChildOf(spanContext), ext.RPCServerOption(spanContext))
	annotateSpan(span, handlerName, req)

	return span, req.WithContext(opentracing.ContextWithSpan(req.Context(), span))
}

func annotateSpan(span opentracing.Span, handlerName string, req *http.Request) {
	if route := httprouter.MatchedRouteFromContext(req.Context()); route != "" {
		span.SetTag("route", route)
	}
	span.SetTag("method", req.Method)
	if ctx := chi.RouteContext(req.Context()); ctx != nil {
		span.SetTag("route", ctx.RoutePath)
	}
	span.SetTag("handler", handlerName)
	span.LogKV("path", req.URL.Path)
}

// span is a simple wrapper around opentracing.Span in order to
// get access to the duration of the span for metrics reporting.
type Span struct {
	opentracing.Span
	start    time.Time
	Duration time.Duration
	hist     prometheus.Observer
	gauge    prometheus.Gauge
}

func StartSpanFromContextWithPromMetrics(ctx context.Context, operationName string, hist prometheus.Observer, gauge prometheus.Gauge, opts ...opentracing.StartSpanOption) (*Span, context.Context) {
	start := time.Now()
	s, sctx := StartSpanFromContextWithOperationName(ctx, operationName, opentracing.StartTime(start))
	gauge.Inc()
	return &Span{s, start, 0, hist, gauge}, sctx
}

func (s *Span) Finish() {
	finish := time.Now()
	s.Duration = finish.Sub(s.start)
	s.Span.FinishWithOptions(opentracing.FinishOptions{
		FinishTime: finish,
	})
	s.hist.Observe(s.Duration.Seconds())
	s.gauge.Dec()
}

// StartSpanFromContext is an improved opentracing.StartSpanFromContext.
// Uses the calling function as the operation name, and logs the filename and line number.
//
// Passing nil context induces panic.
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
//  ctx := opentracing.ContextWithSpan(context.Background(), span)
//
//  // Create a child span
//  span := opentracing.StartSpan("operation name", opentracing.ChildOf(sc))
//  ctx := opentracing.ContextWithSpan(context.Background(), span)
//
//  // Sugar to create a child span
//  span, ctx := opentracing.StartSpanFromContext(ctx, "operation name")
func StartSpanFromContext(ctx context.Context, opts ...opentracing.StartSpanOption) (opentracing.Span, context.Context) {
	if ctx == nil {
		panic("StartSpanFromContext called with nil context")
	}

	// Get caller frame.
	var pcs [1]uintptr
	n := runtime.Callers(2, pcs[:])
	if n < 1 {
		span, ctx := opentracing.StartSpanFromContext(ctx, "unknown", opts...)
		span.LogFields(log.Error(errors.New("runtime.Callers failed")))
		return span, ctx
	}
	fn := runtime.FuncForPC(pcs[0])
	name := fn.Name()
	if lastSlash := strings.LastIndexByte(name, '/'); lastSlash > 0 {
		name = name[lastSlash+1:]
	}

	var span opentracing.Span
	if parentSpan := opentracing.SpanFromContext(ctx); parentSpan != nil {
		// Create a child span.
		opts = append(opts, opentracing.ChildOf(parentSpan.Context()))
		span = opentracing.StartSpan(name, opts...)
	} else {
		// Create a root span.
		span = opentracing.StartSpan(name)
	}
	// New context references this span, not the parent (if there was one).
	ctx = opentracing.ContextWithSpan(ctx, span)

	file, line := fn.FileLine(pcs[0])
	span.LogFields(log.String("filename", file), log.Int("line", line))

	return span, ctx
}

// StartSpanFromContextWithOperationName is like StartSpanFromContext, but the caller determines the operation name.
func StartSpanFromContextWithOperationName(ctx context.Context, operationName string, opts ...opentracing.StartSpanOption) (opentracing.Span, context.Context) {
	if ctx == nil {
		panic("StartSpanFromContextWithOperationName called with nil context")
	}

	// Get caller frame.
	var pcs [1]uintptr
	n := runtime.Callers(2, pcs[:])
	if n < 1 {
		span, ctx := opentracing.StartSpanFromContext(ctx, operationName, opts...)
		span.LogFields(log.Error(errors.New("runtime.Callers failed")))
		return span, ctx
	}
	file, line := runtime.FuncForPC(pcs[0]).FileLine(pcs[0])

	var span opentracing.Span
	if parentSpan := opentracing.SpanFromContext(ctx); parentSpan != nil {
		opts = append(opts, opentracing.ChildOf(parentSpan.Context()))
		// Create a child span.
		span = opentracing.StartSpan(operationName, opts...)
	} else {
		// Create a root span.
		span = opentracing.StartSpan(operationName, opts...)
	}
	// New context references this span, not the parent (if there was one).
	ctx = opentracing.ContextWithSpan(ctx, span)

	span.LogFields(log.String("filename", file), log.Int("line", line))

	return span, ctx
}

// InfoFromSpan returns the traceID and if it was sampled from the span, given it is a jaeger span.
// It returns whether a span associated to the context has been found.
func InfoFromSpan(span opentracing.Span) (traceID string, sampled bool, found bool) {
	if spanContext, ok := span.Context().(jaeger.SpanContext); ok {
		traceID = spanContext.TraceID().String()
		sampled = spanContext.IsSampled()
		return traceID, sampled, true
	}
	return "", false, false
}

// InfoFromContext returns the traceID and if it was sampled from the Jaeger span
// found in the given context. It returns whether a span associated to the context has been found.
func InfoFromContext(ctx context.Context) (traceID string, sampled bool, found bool) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		return InfoFromSpan(span)
	}
	return "", false, false
}
