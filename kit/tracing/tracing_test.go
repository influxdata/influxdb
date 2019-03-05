package tracing

import (
	"context"
	"fmt"
	"github.com/uber/jaeger-client-go"
	"net/http"
	"runtime"
	"testing"

	"github.com/opentracing/opentracing-go"
)

func TestInjectAndExtractHTTPRequest(t *testing.T) {
	// Use Jaeger tracer simply to avoid using a noop tracer implementation.

	sampler := jaeger.NewConstSampler(true)
	reporter := jaeger.NewInMemoryReporter()
	tracer, closer := jaeger.NewTracer("service name", sampler, reporter)
	defer closer.Close()

	oldTracer := opentracing.GlobalTracer()
	opentracing.SetGlobalTracer(tracer)
	defer opentracing.SetGlobalTracer(oldTracer)

	request, err := http.NewRequest(http.MethodPost, "http://localhost/", nil)
	if err != nil {
		t.Fatal(err)
	}

	span := tracer.StartSpan("operation name")

	InjectToHTTPRequest(span, request)
	gotSpan, _ := ExtractFromHTTPRequest(request, "MyStruct")

	if span.Context().(jaeger.SpanContext).TraceID().String() != gotSpan.Context().(jaeger.SpanContext).TraceID().String() {
		t.Error("injected and extracted traceIDs not equal")
	}

	if span.Context().(jaeger.SpanContext).SpanID() != gotSpan.Context().(jaeger.SpanContext).ParentID() {
		t.Error("injected and extracted spanIDs not equal")
	}
}

func TestStartSpanFromContext(t *testing.T) {
	// Use Jaeger tracer simply to avoid using a noop tracer implementation.

	sampler := jaeger.NewConstSampler(true)
	reporter := jaeger.NewInMemoryReporter()
	tracer, closer := jaeger.NewTracer("service name", sampler, reporter)
	defer closer.Close()

	oldTracer := opentracing.GlobalTracer()
	opentracing.SetGlobalTracer(tracer)
	defer opentracing.SetGlobalTracer(oldTracer)

	type testCase struct {
		ctx          context.Context
		expectParent bool
	}
	var testCases []testCase

	testCases = append(testCases,
		testCase{
			ctx:          nil,
			expectParent: false,
		},
		testCase{
			ctx:          context.Background(),
			expectParent: false,
		})

	parentSpan := opentracing.StartSpan("parent operation name")
	testCases = append(testCases, testCase{
		ctx:          opentracing.ContextWithSpan(context.Background(), parentSpan),
		expectParent: true,
	})

	for i, tc := range testCases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {

			span, ctx := StartSpanFromContext(tc.ctx)
			if ctx == nil {
				t.Error("never expect non-nil ctx")
			}
			if span == nil {
				t.Error("never expect non-nil Span")
			}
			foundParent := span.Context().(jaeger.SpanContext).ParentID() != 0
			if tc.expectParent != foundParent {
				t.Errorf("parent: expect %v got %v", tc.expectParent, foundParent)
			}
			if ctx == tc.ctx {
				t.Errorf("always expect fresh context")
			}
		})
	}
}

/*
BenchmarkLocal_StartSpanFromContext-8                         1000000    1262 ns/op    376 B/op    6 allocs/op
BenchmarkLocal_StartSpanFromContext_runtimeCaller-8           3000000     526 ns/op
BenchmarkLocal_StartSpanFromContext_runtimeCallersFrames-8   10000000     237 ns/op
BenchmarkOpentracing_StartSpanFromContext-8                  10000000     157 ns/op     96 B/op    3 allocs/op
BenchmarkOpentracing_StartSpan_root-8                       200000000    7.72 ns/op      0 B/op    0 allocs/op
BenchmarkOpentracing_StartSpan_child-8                       20000000    70.5 ns/op     48 B/op    2 allocs/op
*/

func BenchmarkLocal_StartSpanFromContext(b *testing.B) {
	b.ReportAllocs()

	parentSpan := opentracing.StartSpan("parent operation name")
	ctx := opentracing.ContextWithSpan(context.Background(), parentSpan)

	for n := 0; n < b.N; n++ {
		StartSpanFromContext(ctx)
	}
}

func BenchmarkLocal_StartSpanFromContext_runtimeCaller(b *testing.B) {
	for n := 0; n < b.N; n++ {
		_, _, _, _ = runtime.Caller(1)
	}
}

func BenchmarkLocal_StartSpanFromContext_runtimeCallersFrames(b *testing.B) {
	pc, _, _, ok := runtime.Caller(1)
	if !ok {
		b.Fatal("runtime.Caller failed")
	}

	for n := 0; n < b.N; n++ {
		_, _ = runtime.CallersFrames([]uintptr{pc}).Next()
	}
}

func BenchmarkOpentracing_StartSpanFromContext(b *testing.B) {
	b.ReportAllocs()

	parentSpan := opentracing.StartSpan("parent operation name")
	ctx := opentracing.ContextWithSpan(context.Background(), parentSpan)

	for n := 0; n < b.N; n++ {
		_, _ = opentracing.StartSpanFromContext(ctx, "operation name")
	}
}

func BenchmarkOpentracing_StartSpan_root(b *testing.B) {
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		_ = opentracing.StartSpan("operation name")
	}
}

func BenchmarkOpentracing_StartSpan_child(b *testing.B) {
	b.ReportAllocs()

	parentSpan := opentracing.StartSpan("parent operation name")

	for n := 0; n < b.N; n++ {
		_ = opentracing.StartSpan("operation name", opentracing.ChildOf(parentSpan.Context()))
	}
}

// TODO test nil and other context problems with StartSpanFromContext.