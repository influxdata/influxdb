package diagnostic

import (
	"net"
	"time"
)

type ContextBuilder interface {
	WithContext(bindAddress string) Context
}

type Context interface {
	Starting(batchSize int, batchTimeout time.Duration)
	Listening(protocol string, addr net.Addr)
	TCPListenerClosed()
	TCPAcceptError(err error)
	LineParseError(line string, err error)
	InternalStorageCreateError(err error)
	PointWriterError(database string, err error)
}
