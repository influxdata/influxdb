package tracing

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
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
		expectPanic  bool
		expectParent bool
	}
	var testCases []testCase

	testCases = append(testCases,
		testCase{
			ctx:          nil,
			expectPanic:  true,
			expectParent: false,
		},
		testCase{
			ctx:          context.Background(),
			expectPanic:  false,
			expectParent: false,
		})

	parentSpan := opentracing.StartSpan("parent operation name")
	testCases = append(testCases, testCase{
		ctx:          opentracing.ContextWithSpan(context.Background(), parentSpan),
		expectPanic:  false,
		expectParent: true,
	})

	for i, tc := range testCases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			var span opentracing.Span
			var ctx context.Context
			var gotPanic bool

			func(inputCtx context.Context) {
				defer func() {
					if recover() != nil {
						gotPanic = true
					}
				}()
				span, ctx = StartSpanFromContext(inputCtx)
			}(tc.ctx)

			if tc.expectPanic != gotPanic {
				t.Errorf("panic: expect %v got %v", tc.expectPanic, gotPanic)
			}
			if tc.expectPanic {
				// No other valid checks if panic.
				return
			}
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
BenchmarkLocal_StartSpanFromContext-8                          2000000     681 ns/op     224 B/op     4 allocs/op
BenchmarkLocal_StartSpanFromContext_runtimeCaller-8            3000000     534 ns/op
BenchmarkLocal_StartSpanFromContext_runtimeCallers-8          10000000     196 ns/op
BenchmarkLocal_StartSpanFromContext_runtimeFuncForPC-8       200000000       7.28 ns/op
BenchmarkLocal_StartSpanFromContext_runtimeCallersFrames-8    10000000     234 ns/op
BenchmarkLocal_StartSpanFromContext_runtimeFuncFileLine-8     20000000     103 ns/op
BenchmarkOpentracing_StartSpanFromContext-8                   10000000     155 ns/op      96 B/op     3 allocs/op
BenchmarkOpentracing_StartSpan_root-8                        200000000       7.68 ns/op    0 B/op     0 allocs/op
BenchmarkOpentracing_StartSpan_child-8                        20000000      71.2 ns/op    48 B/op     2 allocs/op
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

func BenchmarkLocal_StartSpanFromContext_runtimeCallers(b *testing.B) {
	var pcs [1]uintptr

	for n := 0; n < b.N; n++ {
		_ = runtime.Callers(2, pcs[:])
	}
}

func BenchmarkLocal_StartSpanFromContext_runtimeFuncForPC(b *testing.B) {
	var pcs [1]uintptr
	_ = runtime.Callers(2, pcs[:])

	for n := 0; n < b.N; n++ {
		_ = runtime.FuncForPC(pcs[0])
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

func BenchmarkLocal_StartSpanFromContext_runtimeFuncFileLine(b *testing.B) {
	var pcs [1]uintptr
	_ = runtime.Callers(2, pcs[:])
	fn := runtime.FuncForPC(pcs[0])

	for n := 0; n < b.N; n++ {
		_, _ = fn.FileLine(pcs[0])
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
