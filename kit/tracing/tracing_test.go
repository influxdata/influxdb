package tracing

import (
	"context"
	"github.com/opentracing/opentracing-go"
	"testing"
)

/*
BenchmarkStartSpanFromContext-8              	 1000000	      1086 ns/op
BenchmarkOpentracingStartSpanFromContext-8   	20000000	        73.2 ns/op
*/

func BenchmarkStartSpanFromContext(b *testing.B) {
	// Using noop tracer (default).
	withRuntimeOverhead := func(ctx context.Context) {
		span, _ := StartSpanFromContext(ctx)
		span.Finish()
	}

	for n := 0; n < b.N; n++ {
		ctx := context.Background()
		withRuntimeOverhead(ctx)
	}
}

func BenchmarkOpentracingStartSpanFromContext(b *testing.B) {
	// Using noop tracer (default).
	withStaticOperationName := func(ctx context.Context) {
		span, _ := opentracing.StartSpanFromContext(ctx, "operation name")
		span.Finish()
	}

	for n := 0; n < b.N; n++ {
		ctx := context.Background()
		withStaticOperationName(ctx)
	}
}
