package tests

import (
	"io"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/stretchr/testify/require"
)

// slowProxy implements the basic slow proxy.
type slowProxy struct {
	listener net.Listener
	dest     string // The destination address for proxy

	bytesPerSecond int // Target BPS rate
	burstLimit     int // Maxmimum burst speed
}

// NewSlowProxy creates a slowProxy to a given server with the specified rate limits.
func NewSlowProxy(src, dest string, bytesPerSecond, burstLimit int) (*slowProxy, error) {
	// Create the Listener now so client code doesn't get stuck with a non-functional proxy
	listener, err := net.Listen("tcp", src)
	if err != nil {
		return nil, err
	}

	return &slowProxy{
		listener:       listener,
		dest:           dest,
		bytesPerSecond: bytesPerSecond,
		burstLimit:     burstLimit,
	}, nil
}

// Address returns the listening address of the slowProxy service.
func (s *slowProxy) Addr() net.Addr {
	return s.listener.Addr()
}

// Serve runs the slow proxy server.
func (s *slowProxy) Serve() error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return err
		}
		// Run handleConnection async since server dial might block
		go s.handleConnection(conn)
	}
}

// Close the proxy server and frees network resources. Already running connections are not stopped.
func (s *slowProxy) Close() error {
	return s.listener.Close()
}

// handleConnection sets up and handles a single proxy connection.
func (s *slowProxy) handleConnection(client_conn net.Conn) error {
	// Open up connection to destination server
	server_conn, err := net.Dial("tcp", s.dest)
	if err != nil {
		client_conn.Close()
		return nil
	}

	slow_server := limiter.NewWriter(server_conn, s.bytesPerSecond, s.burstLimit)
	slow_client := limiter.NewWriter(client_conn, s.bytesPerSecond, s.burstLimit)

	go s.shuttleData(slow_server, client_conn)
	go s.shuttleData(slow_client, server_conn)

	return nil
}

// shuttleData shuttles data in one direction from a source to a sink.
// It also closes both sides when an error occurs, which includes EOF.
func (s *slowProxy) shuttleData(dst io.WriteCloser, src io.ReadCloser) {
	_, _ = io.Copy(dst, src)
	dst.Close()
	src.Close()
}

func TestSlowProxy(t *testing.T) {
	const (
		bwLimit  = 200
		dataSize = bwLimit * 2
	)
	sendbuf := make([]byte, dataSize)
	rand.Read(sendbuf)

	// Create the test server
	listener, err := net.Listen("tcp", ":")
	require.NoError(t, err)
	defer listener.Close()

	// Create and run the SlowProxy for testing
	sp, err := NewSlowProxy(":", listener.Addr().String(), bwLimit, bwLimit)
	require.NoError(t, err)
	require.NotNil(t, sp)
	require.NotEqual(t, listener.Addr().String(), sp.Addr().String())
	go sp.Serve()

	// connectionBody runs the body of a test connection. Returns received data.
	connectionBody := func(conn net.Conn, sender bool) []byte {
		recvbuf := make([]byte, dataSize*2)
		var recvbufLen int
		if sender {
			count, err := conn.Write(sendbuf)
			require.NoError(t, err)
			require.Equal(t, len(sendbuf), count)

			// Read any data that might be there for us (mainly for sanity checks)
			conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
			count, _ = conn.Read(recvbuf[recvbufLen:])
			recvbufLen += count
			conn.SetReadDeadline(time.Time{})
		} else {
			conn.SetReadDeadline(time.Time{})
			for {
				count, err := conn.Read(recvbuf[recvbufLen:])
				recvbufLen += count
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
			}
		}
		return recvbuf[:recvbufLen]
	}

	singleServingServer := func(sender bool) []byte {
		conn, err := listener.Accept()
		require.NoError(t, err)
		defer conn.Close()
		return connectionBody(conn, sender)
	}

	singleServingClient := func(sender bool) []byte {
		conn, err := net.Dial("tcp", sp.Addr().String())
		require.NoError(t, err)
		defer conn.Close()
		return connectionBody(conn, sender)
	}

	runTest := func(clientSends bool) {
		// Spin the server up
		serverRecv := make(chan []byte, 1)
		go func(recv chan []byte) {
			recv <- singleServingServer(!clientSends)
		}(serverRecv)

		// Wait for server connection to complete, then check results look correct.
		t1 := time.Now()
		clientRecvbuf := singleServingClient(clientSends)
		serverRecvbuf := <-serverRecv
		elapsed := time.Since(t1)

		// Now see if the data was sent properly
		if clientSends {
			require.Empty(t, clientRecvbuf)
			require.Equal(t, sendbuf, serverRecvbuf)
		} else {
			require.Equal(t, sendbuf, clientRecvbuf)
			require.Empty(t, serverRecvbuf)
		}

		// See if the throttling looks appropriate. We're looking for +/- 5% on duration
		expDurationSecs := float64(dataSize) / bwLimit
		minDuration := time.Duration(expDurationSecs * 0.95 * float64(time.Second))
		maxDuration := time.Duration(expDurationSecs * 1.05 * float64(time.Second))
		require.Greater(t, elapsed, minDuration)
		require.Less(t, elapsed, maxDuration)
	}

	// Test shuttling data in both directions, with close initiaited once by each side
	runTest(true)
	runTest(false)
}
