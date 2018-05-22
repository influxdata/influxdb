// +build tracing_jaeger

package tracing

import (
	"io"
	"time"

	"github.com/uber/jaeger-client-go/config"
	jlog "github.com/uber/jaeger-client-go/log"
	"github.com/uber/jaeger-lib/metrics"
)

func open(serviceName string) io.Closer {
	cfg := config.Configuration{
		Sampler: &config.SamplerConfig{
			Type:  "const",
			Param: 1,
		},
		Reporter: &config.ReporterConfig{
			LogSpans:            false,
			BufferFlushInterval: 1 * time.Second,
		},
	}

	// Initialize tracer with a logger and a metrics factory
	closer, err := cfg.InitGlobalTracer(
		serviceName,
		config.Logger(jlog.StdLogger),
		config.Metrics(metrics.NullFactory),
		config.Gen128Bit(true),
	)
	if err != nil {
		// log.Printf("Could not initialize jaeger tracer: %s", err.Error())
		return nil
	}
	return closer
}
