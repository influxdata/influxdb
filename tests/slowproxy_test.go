package tests

import (
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb/pkg/errors"
	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/stretchr/testify/require"
)

// slowProxy implements the basic slow proxy.
type slowProxy struct {
	listener net.Listener
	dest     string // The destination address for proxy

	bytesPerSecond int // Target BPS rate
	burstLimit     int // Maxmimum burst speed

	muConnErrors sync.Mutex // muConnErrors protects connErrors
	connErrors   []error    // List of connection errors since last check
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

// Addr returns the listening address of the slowProxy service.
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
		// Run handleConnection async since it blocks while its connection is open, collect errors.
		go func() {
			if err := s.handleConnection(conn); err != nil {
				s.muConnErrors.Lock()
				s.connErrors = append(s.connErrors, fmt.Errorf("handleConnection: %w", err))
				s.muConnErrors.Unlock()
			}
		}()
	}
}

// ConnectionErrors returns a slice of errors from closed connections since the last time Errors was called.
func (s *slowProxy) ConnectionErrors() []error {
	var errors []error
	s.muConnErrors.Lock()
	copy(errors, s.connErrors)
	s.connErrors = nil
	s.muConnErrors.Unlock()
	return errors
}

// Close the proxy server and frees network resources. Already running connections are not stopped.
func (s *slowProxy) Close() error {
	return s.listener.Close()
}

// handleConnection sets up and handles a single proxy connection.
func (s *slowProxy) handleConnection(clientConn net.Conn) (err error) {
	defer errors.Capture(&err, clientConn.Close)()

	// Open up connection to destination server
	serverConn, err := net.Dial("tcp", s.dest)
	if err != nil {
		return
	}
	defer errors.Capture(&err, serverConn.Close)()

	slowServer := limiter.NewWriter(serverConn, s.bytesPerSecond, s.burstLimit)
	slowClient := limiter.NewWriter(clientConn, s.bytesPerSecond, s.burstLimit)

	errorChan := make(chan error, 2)
	var wg sync.WaitGroup
	shuttleData := func(dst io.WriteCloser, src io.ReadCloser) {
		defer wg.Done()
		errorChan <- func(dst io.WriteCloser, src io.ReadCloser) (err error) {
			defer errors.Capture(&err, dst.Close)()
			defer errors.Capture(&err, src.Close)()
			_, err = io.Copy(dst, src)
			return
		}(dst, src)
	}

	wg.Add(2)
	go shuttleData(slowServer, clientConn)
	go shuttleData(slowClient, serverConn)
	wg.Wait()
	close(errorChan)

	for shuttleErr := range errorChan {
		// io.EOF is an expected condition, not an error for us
		if shuttleErr != nil && shuttleErr != io.EOF {
			if err == nil {
				err = shuttleErr
			}
		}
	}

	return
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
	defer sp.Close()

	spErrCh := make(chan error, 1)
	go func() {
		spErrCh <- sp.Serve()
	}()

	// connectionBody runs the body of a test connection. Returns received data.
	connectionBody := func(conn net.Conn, sender bool) ([]byte, error) {
		recvbuf := make([]byte, dataSize*2)
		var recvbufLen int
		if sender {
			count, err := conn.Write(sendbuf)
			if err != nil {
				return nil, err
			}
			if len(sendbuf) != count {
				return nil, fmt.Errorf("incomplete write: %d of %d bytes", count, len(sendbuf))
			}

			// Read any data that might be there for us (mainly for sanity checks)
			conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
			// We expect to get an i/o timeout error on the Read. If we don't get an
			// error, we'll get data, which will cause a failure later on. So
			// we'll ignore any that occurs here.
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
				if err != nil {
					return nil, err
				}
			}
		}
		return recvbuf[:recvbufLen], nil
	}

	singleServingServer := func(sender bool) ([]byte, error) {
		conn, err := listener.Accept()
		if err != nil {
			return nil, err
		}
		defer conn.Close()
		return connectionBody(conn, sender)
	}

	singleServingClient := func(sender bool) ([]byte, error) {
		conn, err := net.Dial("tcp", sp.Addr().String())
		if err != nil {
			return nil, err
		}
		defer conn.Close()
		return connectionBody(conn, sender)
	}

	type connResult struct {
		recv []byte // Received data
		err  error  // Any error from the connection
	}
	runTest := func(clientSends bool) {
		// Spin the server up
		serverResCh := make(chan connResult, 1)
		go func(ch chan connResult) {
			recv, err := singleServingServer(!clientSends)
			ch <- connResult{recv: recv, err: err}
		}(serverResCh)

		// Wait for server connection to complete, then check results look correct.
		t1 := time.Now()
		clientRecvbuf, clientErr := singleServingClient(clientSends)
		serverResult := <-serverResCh
		elapsed := time.Since(t1)

		// Make sure neither the client nor the server encountered an error
		require.NoError(t, clientErr)
		require.NoError(t, serverResult.err)

		// Now see if the data was sent properly
		if clientSends {
			require.Empty(t, clientRecvbuf)
			require.Equal(t, sendbuf, serverResult.recv)
		} else {
			require.Equal(t, sendbuf, clientRecvbuf)
			require.Empty(t, serverResult.recv)
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

	require.NoError(t, sp.Close())
	spErr := <-spErrCh
	require.Error(t, spErr)
	require.Contains(t, spErr.Error(), "use of closed network connection")
	require.Empty(t, sp.ConnectionErrors())
}
