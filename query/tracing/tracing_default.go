// +build !tracing_jaeger

package tracing

import "io"

func open(serviceName string) io.Closer {
	return nil
}
