package hh

import (
	"io/ioutil"
	"testing"
	"time"

	"github.com/influxdb/influxdb/models"
)

type fakeShardWriter struct {
	ShardWriteFn func(shardID, nodeID uint64, points []models.Point) error
}

func (f *fakeShardWriter) WriteShard(shardID, nodeID uint64, points []models.Point) error {
	return f.ShardWriteFn(shardID, nodeID, points)
}

type fakeMetaProvider struct {
	NodesFn func() []uint64
}

func (f fakeMetaProvider) Nodes() []uint64 {
	return f.NodesFn()
}

func TestProcessorProcess(t *testing.T) {
	dir, err := ioutil.TempDir("", "processor_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	// expected data to be queue and sent to the shardWriter
	var expShardID, expNodeID, count = uint64(100), uint64(200), 0
	pt := models.NewPoint("cpu", models.Tags{"foo": "bar"}, models.Fields{"value": 1.0}, time.Unix(0, 0))

	sh := &fakeShardWriter{
		ShardWriteFn: func(shardID, nodeID uint64, points []models.Point) error {
			count += 1
			if shardID != expShardID {
				t.Errorf("Process() shardID mismatch: got %v, exp %v", shardID, expShardID)
			}
			if nodeID != expNodeID {
				t.Errorf("Process() nodeID mismatch: got %v, exp %v", nodeID, expNodeID)
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

	p, err := NewProcessor(dir, sh, ProcessorOptions{MaxSize: 1024})
	if err != nil {
		t.Fatalf("Process() failed to create processor: %v", err)
	}

	// This should queue the writes
	if err := p.WriteShard(expShardID, expNodeID, []models.Point{pt}); err != nil {
		t.Fatalf("Process() failed to write points: %v", err)
	}

	// This should send the write to the shard writer
	if err := p.Process(); err != nil {
		t.Fatalf("Process() failed to write points: %v", err)
	}

	if exp := 1; count != exp {
		t.Fatalf("Process() write count mismatch: got %v, exp %v", count, exp)
	}

	// Queue should be empty so no writes should be send again
	if err := p.Process(); err != nil {
		t.Fatalf("Process() failed to write points: %v", err)
	}

	// Count should stay the same
	if exp := 1; count != exp {
		t.Fatalf("Process() write count mismatch: got %v, exp %v", count, exp)
	}
}

func TestProcessorProcessWithProvider(t *testing.T) {
	dir, err := ioutil.TempDir("", "processor_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	// Active nodes and a point.
	activeNodes := []uint64{1}
	pt := models.NewPoint("cpu", models.Tags{"foo": "bar"}, models.Fields{"value": 1.0}, time.Unix(0, 0))

	// This shard writer should only be called for active nodes.
	count := 0
	sh := &fakeShardWriter{
		ShardWriteFn: func(shardID, nodeID uint64, points []models.Point) error {
			if !contains(activeNodes, nodeID) {
				t.Fatalf("proccess incorrectly processed inactive node ID %d", nodeID)
			}
			count++
			return nil
		},
	}

	// Create the processor for test.
	p, err := NewProcessor(dir, sh, ProcessorOptions{MaxSize: 1024})
	if err != nil {
		t.Fatalf("Process() failed to create processor: %v", err)
	}
	p.MetaProvider = fakeMetaProvider{
		NodesFn: func() []uint64 { return activeNodes },
	}

	// This should queue the writes, only the first should actually be processed.
	if err := p.WriteShard(999, activeNodes[0], []models.Point{pt}); err != nil {
		t.Fatalf("Process() failed to write points: %v", err)
	}
	if err := p.WriteShard(999, 999, []models.Point{pt}); err != nil {
		t.Fatalf("Process() failed to write points: %v", err)
	}

	if err := p.Process(); err != nil {
		t.Fatalf("Process() failed to process points: %v", err)
	}

	// Shard write should only have been called once.
	if count != 1 {
		t.Fatalf("Process() write count mismatch: got %v, exp 1", count)
	}
}
