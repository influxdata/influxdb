package internal

import (
	"github.com/influxdata/influxdb/v2/kit/platform"
	"os"
	"path/filepath"
	"strconv"

	"github.com/influxdata/influxdb/v2/pkg/durablequeue"
	"go.uber.org/zap"
)

type durableQueueManager struct {
	queue  *durablequeue.Queue
	logger *zap.Logger
}

func NewDurableQueueManager(log *zap.Logger) *durableQueueManager {
	return &durableQueueManager{
		logger: log,
	}
}

// InitializeQueue creates a new durable queue which is associated with a replication stream.
func (qm *durableQueueManager) InitializeQueue(replicationID platform.ID, maxQueueSizeBytes int64) *durablequeue.Queue {
	// Set up path for new queue on disk
	dir := filepath.Join(os.Getenv("HOME"), ".influxdbv2", "engine", "replicationq", strconv.FormatUint(uint64(replicationID), 10))
	err := os.MkdirAll(dir, 0777)
	if err != nil {

	}

	// Initialize a new durable queue for the associated replication stream
	newQueue, _ := durablequeue.NewQueue(
		dir,
		maxQueueSizeBytes,
		durablequeue.DefaultSegmentSize,
		&durablequeue.SharedCount{},
		durablequeue.MaxWritesPending,
		func(bytes []byte) error {
			return nil
		},
	)
	qm.queue = newQueue
	err = qm.queue.Open()
	if err != nil {
		return nil
	}

	return nil
}

// DeleteQueue deletes a durable queue and its associated data on disk.
func (qm *durableQueueManager) DeleteQueue(replicationID platform.ID) error {
	// Delete the queue
	err := qm.queue.Close()
	if err != nil {
		return err
	}

	// Delete any enqueued, un-flushed data on disk for this queue
	err = qm.queue.Remove()
	if err != nil {
		return err
	}

	return nil
}

// UpdateMaxQueueSize updates the maximum size of the durable queue.
func (qm *durableQueueManager) UpdateMaxQueueSize(replicationID platform.ID, maxQueueSizeBytes int64) error {
	err := qm.queue.SetMaxSize(maxQueueSizeBytes)
	if err != nil {
		return err
	}

	return nil
}
