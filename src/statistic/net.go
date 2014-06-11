package statistic

import (
	"net"
)

// trace bytes written, connection count.
type TracingListener struct {
	net.Listener
}

type TracingConn struct {
	net.Conn
}

func (self TracingListener) Accept() (c net.Conn, err error) {
	conn, err := self.Listener.Accept()
	if err != nil {
		return nil, err
	}

	Prove(
		NewMetricUint64(TYPE_CURRENT_CONNECTION, OPERATION_INCREMENT),
		NewMetricUint64(TYPE_TOTAL_CONNECTION, OPERATION_INCREMENT),
	)

	return TracingConn{conn}, nil
}

func (self TracingConn) Close() error {
	Prove(
		NewMetricUint64(TYPE_CURRENT_CONNECTION, OPERATION_DECREMENT),
	)

	return self.Conn.Close()
}

func (self TracingConn) Read(b []byte) (int, error) {
	Prove(
		NewMetricUint64(TYPE_HTTPAPI_BYTES_READ, OPERATION_INCREMENT, uint64(len(b))),
	)
	return self.Conn.Read(b)
}

func (self TracingConn) Write(b []byte) (int, error) {
	Prove(
		NewMetricUint64(TYPE_HTTPAPI_BYTES_WRITTEN, OPERATION_INCREMENT, uint64(len(b))),
	)

	return self.Conn.Write(b)
}
