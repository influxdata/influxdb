package internal

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/pkg/durablequeue"
	"go.uber.org/zap"
)

type durableQueueManager struct {
	replicationQueues map[platform.ID]*durablequeue.Queue
	logger            *zap.Logger
}

// NewDurableQueueManager creates a new durableQueueManager struct, for managing durable queues associated with
//replication streams.
func NewDurableQueueManager(log *zap.Logger) *durableQueueManager {
	replicationQueues := make(map[platform.ID]*durablequeue.Queue)

	return &durableQueueManager{
		replicationQueues: replicationQueues,
		logger:            log,
	}
}

// InitializeQueue creates a new durable queue which is associated with a replication stream.
func (qm *durableQueueManager) InitializeQueue(replicationID platform.ID, maxQueueSizeBytes int64) error {
	// Set up path for new queue on disk
	dir := filepath.Join(
		os.Getenv("HOME"),
		".influxdbv2",
		"engine",
		"replicationq",
		replicationID.String(),
	)
	err := os.MkdirAll(dir, 0777)
	if err != nil {
		return err
	}

	// Create a new durable queue
	newQueue, err := durablequeue.NewQueue(
		dir,
		maxQueueSizeBytes,
		durablequeue.DefaultSegmentSize,
		&durablequeue.SharedCount{},
		durablequeue.MaxWritesPending,
		func(bytes []byte) error {
			return nil
		},
	)

	if err != nil {
		return err
	}

	// Map new durable queue to its corresponding replication stream via replication ID
	qm.replicationQueues[replicationID] = newQueue

	// Open the new queue
	err = newQueue.Open()
	if err != nil {
		return err
	}

	qm.logger.Debug("Created new durable queue for replication stream",
		zap.String("id", replicationID.String()), zap.String("path", dir))

	return nil
}

// DeleteQueue deletes a durable queue and its associated data on disk.
func (qm *durableQueueManager) DeleteQueue(replicationID platform.ID) error {
	if qm.replicationQueues[replicationID] == nil {
		return fmt.Errorf("durable queue not found for replication ID %q", replicationID)
	}

	// Close the queue
	err := qm.replicationQueues[replicationID].Close()
	if err != nil {
		return err
	}

	// Delete any enqueued, un-flushed data on disk for this queue
	err = qm.replicationQueues[replicationID].Remove()
	if err != nil {
		return err
	}

	// Remove entry from replicationQueues map
	delete(qm.replicationQueues, replicationID)

	qm.logger.Debug("Closed replication stream durable queue and deleted its data on disk",
		zap.String("id", replicationID.String()), zap.String("path", filepath.Join(
			os.Getenv("HOME"),
			".influxdbv2",
			"engine",
			"replicationq",
			replicationID.String(),
		)))

	return nil
}

// UpdateMaxQueueSize updates the maximum size of the durable queue.
func (qm *durableQueueManager) UpdateMaxQueueSize(replicationID platform.ID, maxQueueSizeBytes int64) error {
	if qm.replicationQueues[replicationID] == nil {
		return fmt.Errorf("durable queue not found for replication ID %q", replicationID)
	}

	err := qm.replicationQueues[replicationID].SetMaxSize(maxQueueSizeBytes)
	if err != nil {
		return err
	}

	return nil
}
