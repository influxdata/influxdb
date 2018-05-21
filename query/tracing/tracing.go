package tracing

import (
	"io"
)

func Open(serviceName string) io.Closer {
	return open(serviceName)
}
