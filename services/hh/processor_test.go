package hh

import (
	"io/ioutil"
	"testing"
	"time"

	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/models"
)

type fakeShardWriter struct {
	ShardWriteFn func(shardID, nodeID uint64, points []models.Point) error
}

func (f *fakeShardWriter) WriteShard(shardID, nodeID uint64, points []models.Point) error {
	return f.ShardWriteFn(shardID, nodeID, points)
}

type fakeMetaStore struct {
	NodeFn func(nodeID uint64) (*meta.NodeInfo, error)
}

func (f *fakeMetaStore) Node(nodeID uint64) (*meta.NodeInfo, error) {
	return f.NodeFn(nodeID)
}

func TestProcessorProcess(t *testing.T) {
	dir, err := ioutil.TempDir("", "processor_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	// expected data to be queue and sent to the shardWriter
	var expShardID, activeNodeID, inactiveNodeID, count = uint64(100), uint64(200), uint64(300), 0
	pt := models.NewPoint("cpu", models.Tags{"foo": "bar"}, models.Fields{"value": 1.0}, time.Unix(0, 0))

	sh := &fakeShardWriter{
		ShardWriteFn: func(shardID, nodeID uint64, points []models.Point) error {
			count += 1
			if shardID != expShardID {
				t.Errorf("Process() shardID mismatch: got %v, exp %v", shardID, expShardID)
			}
			if nodeID != activeNodeID {
				t.Errorf("Process() nodeID mismatch: got %v, exp %v", nodeID, activeNodeID)
			}

			if exp := 1; len(points) != exp {
				t.Fatalf("Process() points mismatch: got %v, exp %v", len(points), exp)
			}

			if points[0].String() != pt.String() {
				t.Fatalf("Process() points mismatch:\n got %v\n exp %v", points[0].String(), pt.String())
			}

			return nil
		},
	}
	metastore := &fakeMetaStore{
		NodeFn: func(nodeID uint64) (*meta.NodeInfo, error) {
			if nodeID == activeNodeID {
				return &meta.NodeInfo{}, nil
			}
			return nil, nil
		},
	}

	p, err := NewProcessor(dir, sh, metastore, ProcessorOptions{MaxSize: 1024})
	if err != nil {
		t.Fatalf("Process() failed to create processor: %v", err)
	}

	// This should queue a write for the active node.
	if err := p.WriteShard(expShardID, activeNodeID, []models.Point{pt}); err != nil {
		t.Fatalf("Process() failed to write points: %v", err)
	}

	// This should queue a write for the inactive node.
	if err := p.WriteShard(expShardID, inactiveNodeID, []models.Point{pt}); err != nil {
		t.Fatalf("Process() failed to write points: %v", err)
	}

	// This should send the write to the shard writer
	if err := p.Process(); err != nil {
		t.Fatalf("Process() failed to write points: %v", err)
	}

	if exp := 1; count != exp {
		t.Fatalf("Process() write count mismatch: got %v, exp %v", count, exp)
	}

	// All active nodes should have been handled so no writes should be sent again
	if err := p.Process(); err != nil {
		t.Fatalf("Process() failed to write points: %v", err)
	}

	// Count should stay the same
	if exp := 1; count != exp {
		t.Fatalf("Process() write count mismatch: got %v, exp %v", count, exp)
	}

	// Make the inactive node active.
	sh.ShardWriteFn = func(shardID, nodeID uint64, points []models.Point) error {
		count += 1
		if shardID != expShardID {
			t.Errorf("Process() shardID mismatch: got %v, exp %v", shardID, expShardID)
		}
		if nodeID != inactiveNodeID {
			t.Errorf("Process() nodeID mismatch: got %v, exp %v", nodeID, activeNodeID)
		}

		if exp := 1; len(points) != exp {
			t.Fatalf("Process() points mismatch: got %v, exp %v", len(points), exp)
		}

		if points[0].String() != pt.String() {
			t.Fatalf("Process() points mismatch:\n got %v\n exp %v", points[0].String(), pt.String())
		}

		return nil
	}
	metastore.NodeFn = func(nodeID uint64) (*meta.NodeInfo, error) {
		return &meta.NodeInfo{}, nil
	}

	// This should send the final write to the shard writer
	if err := p.Process(); err != nil {
		t.Fatalf("Process() failed to write points: %v", err)
	}

	if exp := 2; count != exp {
		t.Fatalf("Process() write count mismatch: got %v, exp %v", count, exp)
	}

	// All queues should have been handled, so no more writes should result.
	if err := p.Process(); err != nil {
		t.Fatalf("Process() failed to write points: %v", err)
	}

	if exp := 2; count != exp {
		t.Fatalf("Process() write count mismatch: got %v, exp %v", count, exp)
	}
}
