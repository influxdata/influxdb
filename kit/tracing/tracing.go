package tracing

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
)

// LogError adds a span log for an error.
// Returns unchanged error, so useful to wrap as in:
//
// return 0, tracing.LogError(err)
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

// StartSpanFromContext is an easier-to-use opentracing.StartSpanFromContext.
// Uses the calling function as the operation name, and logs the file:line.
// TODO unused for now; consider removing this because performance isn't awesome.
func StartSpanFromContext(ctx context.Context) (opentracing.Span, context.Context) {
	pc, _, _, ok := runtime.Caller(1)
	if !ok {
		span, ctx := opentracing.StartSpanFromContext(ctx, "unknown")
		span.LogFields(log.Error(errors.New("failed to get calling frame")))
		return span, ctx
	}

	frame, _ := runtime.CallersFrames([]uintptr{pc}).Next()

	span, ctx := opentracing.StartSpanFromContext(ctx, frame.Function)
	span.LogFields(log.String("location", fmt.Sprintf("%s:%d", frame.File, frame.Line)))

	return span, ctx
}
