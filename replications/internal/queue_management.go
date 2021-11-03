package internal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	sq "github.com/Masterminds/squirrel"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/sqlite"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/pkg/durablequeue"
	"go.uber.org/zap"
)

type durableQueueManager struct {
	replicationQueues map[platform.ID]*durablequeue.Queue
	logger            *zap.Logger
	enginePath        string
	mutex             sync.RWMutex
}

// NewDurableQueueManager creates a new durableQueueManager struct, for managing durable queues associated with
//replication streams.
func NewDurableQueueManager(log *zap.Logger, enginePath string) *durableQueueManager {
	replicationQueues := make(map[platform.ID]*durablequeue.Queue)

	queueDir := filepath.Join(
		enginePath,
		"replicationq",
	)
	os.MkdirAll(queueDir, 0777)

	return &durableQueueManager{
		replicationQueues: replicationQueues,
		logger:            log,
		enginePath:        enginePath,
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
		qm.enginePath,
		"replicationq",
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

// QueuePath returns the path to the replication queues on disk
func (qm *durableQueueManager) QueuePath() string {
	return filepath.Join(qm.enginePath, "replicationq")
}

// ReplicationQueues returns a map of all current replication stream IDs and their corresponding durable queue structs
func (qm *durableQueueManager) ReplicationQueues() map[platform.ID]*durablequeue.Queue {
	return qm.replicationQueues
}

// StartReplicationQueues updates the durableQueueManager.replicationQueues map, fully removing any partially deleted
// queues (present on disk, but not tracked in sqlite) and opening all current queues
func (qm *durableQueueManager) StartReplicationQueues(ctx context.Context, store *sqlite.SqlStore) error {
	// Get contents of replicationq directory
	entries, err := os.ReadDir(qm.QueuePath())
	if err != nil {
		return err
	}

	errOccurred := false

	for _, entry := range entries {
		// Skip over non-relevant entries (must be a dir named with a replication ID)
		if !entry.IsDir() {
			continue
		}

		id, err := platform.IDFromString(entry.Name())
		if err != nil {
			continue
		}

		// Check sqlite to see if current replication stream is tracked there
		q := sq.Select(
			"id", "max_queue_size_bytes").
			From("replications").
			Where(sq.Eq{"id": id})

		query, args, err := q.ToSql()
		if err != nil {
			qm.logger.Error("failed to build query into a SQL string and bound args", zap.Error(err), zap.String("id", id.String()))
			errOccurred = true
		}

		var r []influxdb.Replication
		var maxQueueSize int64
		isPartialDelete := false

		if err := store.DB.SelectContext(ctx, &r, query, args...); err != nil {
			// Replication stream is present on disk but untracked in sqlite, needs to be fully deleted
			isPartialDelete = true
			maxQueueSize = 67108860 // use default value to allow for delete
		} else {
			// Replication stream is present on disk and in sqlite
			maxQueueSize = r[0].MaxQueueSizeBytes // use current value from sqlite
		}

		// Initialize queue struct for existing queue
		newQueue, err := durablequeue.NewQueue(
			filepath.Join(qm.QueuePath(), id.String()),
			maxQueueSize,
			durablequeue.DefaultSegmentSize,
			&durablequeue.SharedCount{},
			durablequeue.MaxWritesPending,
			func(bytes []byte) error {
				return nil
			},
		)

		if err != nil {
			qm.logger.Error("failed create durable queue struct during partial delete cleanup", zap.Error(err), zap.String("id", id.String()))
			errOccurred = true
		}

		// Close and remove the queue if it was a partial delete
		if isPartialDelete {
			if err := newQueue.Close(); err != nil {
				qm.logger.Error("failed to close durable queue during partial delete cleanup", zap.Error(err), zap.String("id", id.String()))
				errOccurred = true
			}

			if err := newQueue.Remove(); err != nil {
				qm.logger.Error("failed to remove durable queue during partial delete cleanup", zap.Error(err), zap.String("id", id.String()))
				errOccurred = true
			}
		} else {
			// Open and map the queue to its replication ID if it is valid
			if err := newQueue.Open(); err != nil {
				qm.logger.Error("failed to open replication stream durable queue", zap.Error(err), zap.String("id", id.String()), zap.String("path", newQueue.Dir()))
				errOccurred = true
			}
			qm.replicationQueues[r[0].ID] = newQueue
		}
	}

	if errOccurred {
		return fmt.Errorf("startup tasks for replications durable queue management failed, see server logs for details")
	} else {
		return nil
	}
}

// CloseAll loops through all current replication stream queues and closes them without deleting on-disk resources
func (qm *durableQueueManager) CloseAll() error {
	for _, queue := range qm.replicationQueues {
		if err := queue.Close(); err != nil {
			return err
		}
	}
	return nil
}
