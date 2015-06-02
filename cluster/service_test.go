package cluster_test

import (
	"fmt"
	"time"

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

type testService struct {
	nodeID         uint64
	writeShardFunc func(shardID uint64, points []tsdb.Point) error
}

func newTestService(f func(shardID uint64, points []tsdb.Point) error) testService {
	return testService{
		writeShardFunc: f,
	}
}

type serviceResponses []serviceResponse
type serviceResponse struct {
	shardID uint64
	ownerID uint64
	points  []tsdb.Point
}

func (t testService) WriteToShard(shardID uint64, points []tsdb.Point) error {
	return t.writeShardFunc(shardID, points)
}

func writeShardSuccess(shardID uint64, points []tsdb.Point) error {
	responses <- &serviceResponse{
		shardID: shardID,
		points:  points,
	}
	return nil
}

func writeShardFail(shardID uint64, points []tsdb.Point) error {
	return fmt.Errorf("failed to write")
}

var responses = make(chan *serviceResponse, 1024)

func (testService) ResponseN(n int) ([]*serviceResponse, error) {
	var a []*serviceResponse
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
