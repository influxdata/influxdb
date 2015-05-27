package cluster_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/tsdb"
)

type metaStore struct {
	host string
}

func (m *metaStore) Node(nodeID uint64) (*meta.NodeInfo, error) {
	return &meta.NodeInfo{
		ID:   nodeID,
		Host: m.host,
	}, nil
}

type testServer struct {
	writeShardFunc func(shardID uint64, points []tsdb.Point) error
}

func newTestServer(f func(shardID uint64, points []tsdb.Point) error) testServer {
	return testServer{
		writeShardFunc: f,
	}
}

type serverResponses []serverResponse
type serverResponse struct {
	shardID uint64
	points  []tsdb.Point
}

func (t testServer) WriteShard(shardID uint64, points []tsdb.Point) error {
	return t.writeShardFunc(shardID, points)
}

func writeShardSuccess(shardID uint64, points []tsdb.Point) error {
	responses <- &serverResponse{
		shardID: shardID,
		points:  points,
	}
	return nil
}

func writeShardFail(shardID uint64, points []tsdb.Point) error {
	return fmt.Errorf("failed to write")
}

var responses = make(chan *serverResponse, 1024)

func (testServer) ResponseN(n int) ([]*serverResponse, error) {
	var a []*serverResponse
	for {
		select {
		case r := <-responses:
			a = append(a, r)
			if len(a) == n {
				return a, nil
			}
		case <-time.After(time.Second):
			return a, fmt.Errorf("unexpected response count: expected: %d, actual: %d", n, len(a))
		}
	}
}

func TestServer_Close_ErrServerClosed(t *testing.T) {
	var (
		ts testServer
		s  = cluster.NewServer(ts, "127.0.0.1:0")
	)

	if e := s.Open(); e != nil {
		t.Fatalf("err does not match.  expected %v, got %v", nil, e)
	}

	// Close the server
	s.Close()

	// Try to close it again
	if err := s.Close(); err != cluster.ErrServerClosed {
		t.Fatalf("expected an error, got %v", err)
	}
}

func TestServer_Close_ErrBindAddressRequired(t *testing.T) {
	var (
		ts testServer
		s  = cluster.NewServer(ts, "")
	)
	if e := s.Open(); e == nil {
		t.Fatalf("exprected error %s, got nil.", cluster.ErrBindAddressRequired)
	}
}
