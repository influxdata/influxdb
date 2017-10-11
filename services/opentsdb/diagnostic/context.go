package diagnostic

import "net"

type Context interface {
	// Service diagnostics.
	Starting()
	Listening(tls bool, addr net.Addr)
	Closed(err error)

	ConnReadError(err error)
	InternalStorageCreateError(database string, err error)
	PointWriterError(database string, err error)

	MalformedLine(line string, addr net.Addr)
	MalformedTime(time string, addr net.Addr, err error)
	MalformedTag(tag string, addr net.Addr)
	MalformedFloat(valueStr string, addr net.Addr)

	// Handler diagnostics.
	DroppingPoint(metric string, err error)
	WriteError(err error)
}
