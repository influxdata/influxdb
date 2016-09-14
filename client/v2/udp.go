package client

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

var ErrLargePoint = errors.New("point exceeds allowed size")

const (
	// UDPPayloadSize is a reasonable default payload size for UDP packets that
	// could be travelling over the internet.
	UDPPayloadSize = 512

	// MaxPayloadSize is a safe maximum limit for a UDP payload over IPv4
	MaxUDPPayloadSize = 65467
)

// UDPConfig is the config data needed to create a UDP Client
type UDPConfig struct {
	// Addr should be of the form "host:port"
	// or "[ipv6-host%zone]:port".
	Addr string

	// PayloadSize is the maximum size of a UDP client message, optional
	// Tune this based on your network. Defaults to UDPBufferSize.
	PayloadSize int
}

// NewUDPClient returns a client interface for writing to an InfluxDB UDP
// service from the given config.
func NewUDPClient(conf UDPConfig) (Client, error) {
	var udpAddr *net.UDPAddr
	udpAddr, err := net.ResolveUDPAddr("udp", conf.Addr)
	if err != nil {
		return nil, err
	}

	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return nil, err
	}

	payloadSize := conf.PayloadSize
	if payloadSize == 0 {
		payloadSize = UDPPayloadSize
	}

	return &udpclient{
		conn:        conn,
		payloadSize: payloadSize,
	}, nil
}

// Ping will check to see if the server is up with an optional timeout on waiting for leader.
// Ping returns how long the request took, the version of the server it connected to, and an error if one occurred.
func (uc *udpclient) Ping(timeout time.Duration) (time.Duration, string, error) {
	return 0, "", nil
}

// Close releases the udpclient's resources.
func (uc *udpclient) Close() error {
	return uc.conn.Close()
}

type udpclient struct {
	conn        io.WriteCloser
	payloadSize int
}

func (uc *udpclient) Write(bp BatchPoints) error {
	var b bytes.Buffer
	var d time.Duration
	d, _ = time.ParseDuration("1" + bp.Precision())

	var delayedError error

	var checkBuffer = func(s string) {
		if b.Len() > 0 && b.Len()+len(s) > uc.payloadSize {
			if _, err := uc.conn.Write(b.Bytes()); err != nil {
				delayedError = err
			}
			b.Reset()
		}
	}

	for _, p := range bp.Points() {
		point := p.pt.RoundedString(d) + "\n"
		if len(point) > MaxUDPPayloadSize {
			delayedError = ErrLargePoint
			continue
		}

		checkBuffer(point)

		if p.Time().IsZero() || len(point) <= uc.payloadSize {
			b.WriteString(point)
			continue
		}

		points := p.pt.Split(uc.payloadSize - 1) // newline will be added
		for _, sp := range points {
			point = sp.RoundedString(d) + "\n"
			checkBuffer(point)
			b.WriteString(point)
		}
	}

	if b.Len() > 0 {
		if _, err := uc.conn.Write(b.Bytes()); err != nil {
			return err
		}
	}
	return delayedError
}

func (uc *udpclient) Query(q Query) (*Response, error) {
	return nil, fmt.Errorf("Querying via UDP is not supported")
}
