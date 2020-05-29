package testing

import (
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
)

// SetupInMemoryTracing sets the global tracer to an in memory Jaeger instance for testing.
// The returned function should be deferred by the caller to tear down this setup after testing is complete.
func SetupInMemoryTracing(name string) func() {
	var (
		old            = opentracing.GlobalTracer()
		tracer, closer = jaeger.NewTracer(name,
			jaeger.NewConstSampler(true),
			jaeger.NewInMemoryReporter(),
		)
	)

	opentracing.SetGlobalTracer(tracer)
	return func() {
		_ = closer.Close()
		opentracing.SetGlobalTracer(old)
	}
}
