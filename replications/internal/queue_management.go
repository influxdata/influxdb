package internal

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/pkg/durablequeue"
	"go.uber.org/zap"
)

type durableQueueManager struct {
	replicationQueues map[platform.ID]*durablequeue.Queue
	logger            *zap.Logger
	queuePath         string
	mutex             sync.RWMutex
}

// NewDurableQueueManager creates a new durableQueueManager struct, for managing durable queues associated with
//replication streams.
func NewDurableQueueManager(log *zap.Logger, enginePath string) *durableQueueManager {
	replicationQueues := make(map[platform.ID]*durablequeue.Queue)

	queuePath := filepath.Join(
		enginePath,
		"replicationq",
	)
	os.MkdirAll(queuePath, 0777)

	return &durableQueueManager{
		replicationQueues: replicationQueues,
		logger:            log,
		queuePath:         queuePath,
	}
}

// InitializeQueue creates and opens a new durable queue which is associated with a replication stream.
func (qm *durableQueueManager) InitializeQueue(replicationID platform.ID, maxQueueSizeBytes int64) error {
	qm.mutex.Lock()
	defer qm.mutex.Unlock()

	// Check for duplicate replication ID
	if _, exists := qm.replicationQueues[replicationID]; exists {
		return fmt.Errorf("durable queue already exists for replication ID %q", replicationID)
	}

	// Set up path for new queue on disk
	dir := filepath.Join(
		qm.queuePath,
		replicationID.String(),
	)
	if err := os.MkdirAll(dir, 0777); err != nil {
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
	if err := newQueue.Open(); err != nil {
		return err
	}

	qm.logger.Debug("Created new durable queue for replication stream",
		zap.String("id", replicationID.String()), zap.String("path", dir))

	return nil
}

// DeleteQueue deletes a durable queue and its associated data on disk.
func (qm *durableQueueManager) DeleteQueue(replicationID platform.ID) error {
	qm.mutex.Lock()
	defer qm.mutex.Unlock()

	if qm.replicationQueues[replicationID] == nil {
		return fmt.Errorf("durable queue not found for replication ID %q", replicationID)
	}

	// Close the queue
	if err := qm.replicationQueues[replicationID].Close(); err != nil {
		return err
	}

	qm.logger.Debug("Closed replication stream durable queue",
		zap.String("id", replicationID.String()), zap.String("path", qm.replicationQueues[replicationID].Dir()))

	// Delete any enqueued, un-flushed data on disk for this queue
	if err := qm.replicationQueues[replicationID].Remove(); err != nil {
		return err
	}

	qm.logger.Debug("Deleted data associated with replication stream durable queue",
		zap.String("id", replicationID.String()), zap.String("path", qm.replicationQueues[replicationID].Dir()))

	// Remove entry from replicationQueues map
	delete(qm.replicationQueues, replicationID)

	return nil
}

// UpdateMaxQueueSize updates the maximum size of a durable queue.
func (qm *durableQueueManager) UpdateMaxQueueSize(replicationID platform.ID, maxQueueSizeBytes int64) error {
	qm.mutex.RLock()
	defer qm.mutex.RUnlock()

	if qm.replicationQueues[replicationID] == nil {
		return fmt.Errorf("durable queue not found for replication ID %q", replicationID)
	}

	if err := qm.replicationQueues[replicationID].SetMaxSize(maxQueueSizeBytes); err != nil {
		return err
	}

	return nil
}

// CurrentQueueSizes returns the current size-on-disk for the requested set of durable queues.
func (qm *durableQueueManager) CurrentQueueSizes(ids []platform.ID) (map[platform.ID]int64, error) {
	qm.mutex.RLock()
	defer qm.mutex.RUnlock()

	sizes := make(map[platform.ID]int64, len(ids))

	for _, id := range ids {
		if qm.replicationQueues[id] == nil {
			return nil, fmt.Errorf("durable queue not found for replication ID %q", id)
		}
		sizes[id] = qm.replicationQueues[id].DiskUsage()
	}

	return sizes, nil
}

// StartReplicationQueues updates the durableQueueManager.replicationQueues map, fully removing any partially deleted
// queues (present on disk, but not tracked in sqlite), opening all current queues, and logging info for each.
func (qm *durableQueueManager) StartReplicationQueues(trackedReplications map[platform.ID]int64) error {
	errOccurred := false

	for id, size := range trackedReplications {
		// Re-initialize a queue struct for each replication stream from sqlite
		queue, err := durablequeue.NewQueue(
			filepath.Join(qm.queuePath, id.String()),
			size,
			durablequeue.DefaultSegmentSize,
			&durablequeue.SharedCount{},
			durablequeue.MaxWritesPending,
			func(bytes []byte) error {
				return nil
			},
		)

		if err != nil {
			qm.logger.Error("failed to initialize replication stream durable queue", zap.Error(err))
			errOccurred = true
		}

		// Open and map the queue struct to its replication ID
		if err := queue.Open(); err != nil {
			qm.logger.Error("failed to open replication stream durable queue", zap.Error(err), zap.String("id", id.String()))
			errOccurred = true
		} else {
			qm.replicationQueues[id] = queue
			qm.logger.Info("Opened replication stream", zap.String("id", id.String()), zap.String("path", queue.Dir()))
		}
	}

	if errOccurred {
		return fmt.Errorf("startup tasks for replications durable queue management failed, see server logs for details")
	}

	// Get contents of replicationq directory
	entries, err := os.ReadDir(qm.queuePath)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		// Skip over non-relevant entries (must be a dir named with a replication ID)
		if !entry.IsDir() {
			continue
		}

		id, err := platform.IDFromString(entry.Name())
		if err != nil {
			continue
		}

		// Partial delete found, needs to be fully removed
		if qm.replicationQueues[*id] == nil {
			if err := os.RemoveAll(filepath.Join(qm.queuePath, id.String())); err != nil {
				qm.logger.Error("failed to remove durable queue during partial delete cleanup", zap.Error(err), zap.String("id", id.String()))
				errOccurred = true
			}
		}
	}

	if errOccurred {
		return fmt.Errorf("startup tasks for replications durable queues failed, see server logs for details")
	} else {
		return nil
	}
}

// CloseAll loops through all current replication stream queues and closes them without deleting on-disk resources
func (qm *durableQueueManager) CloseAll() error {
	errOccurred := false

	for id, queue := range qm.replicationQueues {
		if err := queue.Close(); err != nil {
			qm.logger.Error("failed to close durable queue", zap.Error(err), zap.String("id", id.String()))
			errOccurred = true
		}
	}

	if errOccurred {
		return fmt.Errorf("shutdown tasks for replications durable queues failed, see server logs for details")
	} else {
		return nil
	}
}
