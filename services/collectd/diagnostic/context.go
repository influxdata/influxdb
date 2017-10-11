package diagnostic

import "net"

type Context interface {
	Starting()
	Listening(addr net.Addr)
	Closed()
	UnableToReadDirectory(path string, err error)
	LoadingPath(path string)
	TypesParseError(path string, err error)

	ReadFromUDPError(err error)
	ParseError(err error)
	DroppingPoint(name string, err error)
	InternalStorageCreateError(err error)
	PointWriterError(database string, err error)
}
