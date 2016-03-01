package cluster_test

import (
	"fmt"
	"io"
	"net"
	"time"

	"github.com/influxdata/influxdb/cluster"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tcp"
)

type metaClient struct {
	host string
}

func (m *metaClient) DataNode(nodeID uint64) (*meta.NodeInfo, error) {
	return &meta.NodeInfo{
		ID:      nodeID,
		TCPHost: m.host,
	}, nil
}

func (m *metaClient) ShardOwner(shardID uint64) (db, rp string, sgi *meta.ShardGroupInfo) {
	return "db", "rp", &meta.ShardGroupInfo{}
}

type testService struct {
	nodeID    uint64
	ln        net.Listener
	muxln     net.Listener
	responses chan *serviceResponse

	TSDBStore TSDBStore
}

func newTestWriteService(f func(shardID uint64, points []models.Point) error) testService {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}

	mux := tcp.NewMux()
	muxln := mux.Listen(cluster.MuxHeader)
	go mux.Serve(ln)

	s := testService{
		ln:    ln,
		muxln: muxln,
	}
	s.TSDBStore.WriteToShardFn = f
	s.responses = make(chan *serviceResponse, 1024)
	return s
}

func (ts *testService) Close() {
	if ts.ln != nil {
		ts.ln.Close()
	}
}

type serviceResponses []serviceResponse
type serviceResponse struct {
	shardID uint64
	ownerID uint64
	points  []models.Point
}

func (ts *testService) writeShardSuccess(shardID uint64, points []models.Point) error {
	ts.responses <- &serviceResponse{
		shardID: shardID,
		points:  points,
	}
	return nil
}

func writeShardFail(shardID uint64, points []models.Point) error {
	return fmt.Errorf("failed to write")
}

func writeShardSlow(shardID uint64, points []models.Point) error {
	time.Sleep(1 * time.Second)
	return nil
}

func (ts *testService) ResponseN(n int) ([]*serviceResponse, error) {
	var a []*serviceResponse
	for {
		select {
		case r := <-ts.responses:
			a = append(a, r)
			if len(a) == n {
				return a, nil
			}
		case <-time.After(time.Second):
			return a, fmt.Errorf("unexpected response count: expected: %d, actual: %d", n, len(a))
		}
	}
}

// Service is a test wrapper for cluster.Service.
type Service struct {
	*cluster.Service

	ln        net.Listener
	TSDBStore TSDBStore
}

// NewService returns a new instance of Service.
func NewService() *Service {
	s := &Service{
		Service: cluster.NewService(cluster.Config{}),
	}
	s.Service.TSDBStore = &s.TSDBStore
	return s
}

// MustOpenService returns a new, open service on a random port. Panic on error.
func MustOpenService() *Service {
	s := NewService()
	s.ln = MustListen("tcp", "127.0.0.1:0")
	s.Listener = &muxListener{s.ln}
	if err := s.Open(); err != nil {
		panic(err)
	}
	return s
}

// Close closes the listener and waits for the service to close.
func (s *Service) Close() error {
	if s.ln != nil {
		s.ln.Close()
	}
	return s.Service.Close()
}

// Addr returns the network address of the service.
func (s *Service) Addr() net.Addr { return s.ln.Addr() }

// muxListener is a net.Listener implementation that strips off the first byte.
// This is used to simulate the listener from pkg/mux.
type muxListener struct {
	net.Listener
}

// Accept accepts the next connection and removes the first byte.
func (ln *muxListener) Accept() (net.Conn, error) {
	conn, err := ln.Listener.Accept()
	if err != nil {
		return nil, err
	}

	var buf [1]byte
	if _, err := io.ReadFull(conn, buf[:]); err != nil {
		conn.Close()
		return nil, err
	} else if buf[0] != cluster.MuxHeader {
		conn.Close()
		panic(fmt.Sprintf("unexpected mux header byte: %d", buf[0]))
	}

	return conn, nil
}

// MustListen opens a listener. Panic on error.
func MustListen(network, laddr string) net.Listener {
	ln, err := net.Listen(network, laddr)
	if err != nil {
		panic(err)
	}
	return ln
}
