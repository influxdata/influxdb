package internal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/pkg/durablequeue"
	"github.com/influxdata/influxdb/v2/replications/metrics"
	"github.com/influxdata/influxdb/v2/replications/remotewrite"
	"go.uber.org/zap"
)

type remoteWriter interface {
	Write(context.Context, []byte) error
}

type replicationQueue struct {
	id           platform.ID
	queue        *durablequeue.Queue
	wg           sync.WaitGroup
	done         chan struct{}
	receive      chan struct{}
	logger       *zap.Logger
	metrics      *metrics.ReplicationsMetrics
	remoteWriter remoteWriter
}

type durableQueueManager struct {
	replicationQueues map[platform.ID]*replicationQueue
	logger            *zap.Logger
	queuePath         string
	mutex             sync.RWMutex
	metrics           *metrics.ReplicationsMetrics
	configStore       remotewrite.HttpConfigStore
}

var errStartup = errors.New("startup tasks for replications durable queue management failed, see server logs for details")
var errShutdown = errors.New("shutdown tasks for replications durable queues failed, see server logs for details")

// NewDurableQueueManager creates a new durableQueueManager struct, for managing durable queues associated with
// replication streams.
func NewDurableQueueManager(log *zap.Logger, queuePath string, metrics *metrics.ReplicationsMetrics, configStore remotewrite.HttpConfigStore) *durableQueueManager {
	replicationQueues := make(map[platform.ID]*replicationQueue)

	os.MkdirAll(queuePath, 0777)

	return &durableQueueManager{
		replicationQueues: replicationQueues,
		logger:            log,
		queuePath:         queuePath,
		metrics:           metrics,
		configStore:       configStore,
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

	// Open the new queue
	if err := newQueue.Open(); err != nil {
		return err
	}

	// Map new durable queue and scanner to its corresponding replication stream via replication ID
	rq := qm.newReplicationQueue(replicationID, newQueue)
	qm.replicationQueues[replicationID] = rq
	rq.Open()

	qm.logger.Debug("Created new durable queue for replication stream",
		zap.String("id", replicationID.String()), zap.String("path", dir))

	return nil
}

func (rq *replicationQueue) Open() {
	rq.wg.Add(1)
	go rq.run()
}

func (rq *replicationQueue) Close() error {
	close(rq.receive)
	close(rq.done)
	rq.wg.Wait() // wait for goroutine to finish processing all messages
	return rq.queue.Close()
}

func (rq *replicationQueue) run() {
	defer rq.wg.Done()

	for {
		select {
		case <-rq.done: // end the goroutine when done is messaged
			return
		case <-rq.receive: // run the scanner on data append
			for rq.SendWrite() {
			}
		}
	}
}

// SendWrite processes data enqueued into the durablequeue.Queue.
// SendWrite is responsible for processing all data in the queue at the time of calling.
// Retryable errors should be handled and retried in the dp function.
// Unprocessable data should be dropped in the dp function.
func (rq *replicationQueue) SendWrite() (error bool) {
	// Any error in creating the scanner should exit the loop in run()
	// Either it is io.EOF indicating no data, or some other failure in making
	// the Scanner object that we don't know how to handle.
	scan, err := rq.queue.NewScanner()
	if err != nil {
		if err != io.EOF {
			rq.logger.Error("Error creating replications queue scanner", zap.Error(err))
		}
		return false
	}

	for scan.Next() {
		// An io.EOF error here indicates that there is no more data
		// left to process, and is an expected error.
		if scan.Err() == io.EOF {
			break
		}

		// Any other here indicates a problem, so we log the error and
		// drop the data with a call to scan.Advance() later.
		if scan.Err() != nil {
			rq.logger.Info("Segment read error.", zap.Error(scan.Err()))
			break
		}

		// An error here indicates an unhandlable error. Data is not corrupt, and
		// the remote write is not retryable. A potential example of an error here
		// is an authentication error with the remote host.
		if err = rq.remoteWriter.Write(context.Background(), scan.Bytes()); err != nil {
			rq.logger.Error("Error in replication stream", zap.Error(err))
			return false
		}
	}

	// Update metrics after the call to scan.Advance()
	defer func() {
		rq.metrics.Dequeue(rq.id, rq.queue.DiskUsage())
	}()

	if _, err = scan.Advance(); err != nil {
		if err != io.EOF {
			rq.logger.Error("Error in replication queue scanner", zap.Error(err))
		}
		return false
	}
	return true
}

// DeleteQueue deletes a durable queue and its associated data on disk.
func (qm *durableQueueManager) DeleteQueue(replicationID platform.ID) error {
	qm.mutex.Lock()
	defer qm.mutex.Unlock()

	if _, exist := qm.replicationQueues[replicationID]; !exist {
		return fmt.Errorf("durable queue not found for replication ID %q", replicationID)
	}

	rq := qm.replicationQueues[replicationID]

	// Close the queue
	if err := rq.Close(); err != nil {
		return err
	}

	qm.logger.Debug("Closed replication stream durable queue",
		zap.String("id", replicationID.String()), zap.String("path", rq.queue.Dir()))

	// Delete any enqueued, un-flushed data on disk for this queue
	if err := rq.queue.Remove(); err != nil {
		return err
	}

	qm.logger.Debug("Deleted data associated with replication stream durable queue",
		zap.String("id", replicationID.String()), zap.String("path", rq.queue.Dir()))

	// Remove entry from replicationQueues map
	delete(qm.replicationQueues, replicationID)

	return nil
}

// UpdateMaxQueueSize updates the maximum size of a durable queue.
func (qm *durableQueueManager) UpdateMaxQueueSize(replicationID platform.ID, maxQueueSizeBytes int64) error {
	qm.mutex.RLock()
	defer qm.mutex.RUnlock()

	if _, exist := qm.replicationQueues[replicationID]; !exist {
		return fmt.Errorf("durable queue not found for replication ID %q", replicationID)
	}

	if err := qm.replicationQueues[replicationID].queue.SetMaxSize(maxQueueSizeBytes); err != nil {
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
		if _, exist := qm.replicationQueues[id]; !exist {
			return nil, fmt.Errorf("durable queue not found for replication ID %q", id)
		}
		sizes[id] = qm.replicationQueues[id].queue.DiskUsage()
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
			continue
		}

		// Open and map the queue struct to its replication ID
		if err := queue.Open(); err != nil {
			qm.logger.Error("failed to open replication stream durable queue", zap.Error(err), zap.String("id", id.String()))
			errOccurred = true
			continue
		} else {
			qm.replicationQueues[id] = qm.newReplicationQueue(id, queue)
			qm.replicationQueues[id].Open()
			qm.logger.Info("Opened replication stream", zap.String("id", id.String()), zap.String("path", queue.Dir()))
		}
	}

	if errOccurred {
		return errStartup
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
		if _, exist := qm.replicationQueues[*id]; !exist {
			if err := os.RemoveAll(filepath.Join(qm.queuePath, id.String())); err != nil {
				qm.logger.Error("failed to remove durable queue during partial delete cleanup", zap.Error(err), zap.String("id", id.String()))
				errOccurred = true
			}
		}
	}

	if errOccurred {
		return errStartup
	} else {
		return nil
	}
}

// CloseAll loops through all current replication stream queues and closes them without deleting on-disk resources
func (qm *durableQueueManager) CloseAll() error {
	errOccurred := false

	for id, replicationQueue := range qm.replicationQueues {
		if err := replicationQueue.Close(); err != nil {
			qm.logger.Error("failed to close durable queue", zap.Error(err), zap.String("id", id.String()))
			errOccurred = true
		}
	}

	if errOccurred {
		return errShutdown
	} else {
		return nil
	}
}

// EnqueueData persists a set of bytes to a replication's durable queue.
func (qm *durableQueueManager) EnqueueData(replicationID platform.ID, data []byte, numPoints int) error {
	qm.mutex.RLock()
	defer qm.mutex.RUnlock()

	rq, ok := qm.replicationQueues[replicationID]
	if !ok {
		return fmt.Errorf("durable queue not found for replication ID %q", replicationID)
	}

	if err := rq.queue.Append(data); err != nil {
		return err
	}
	// Update metrics for this replication queue when adding data to the queue.
	qm.metrics.EnqueueData(replicationID, len(data), numPoints, rq.queue.DiskUsage())

	qm.replicationQueues[replicationID].receive <- struct{}{}

	return nil
}

func (qm *durableQueueManager) newReplicationQueue(id platform.ID, queue *durablequeue.Queue) *replicationQueue {
	logger := qm.logger.With(zap.String("replication_id", id.String()))

	return &replicationQueue{
		id:           id,
		queue:        queue,
		done:         make(chan struct{}),
		receive:      make(chan struct{}),
		logger:       logger,
		metrics:      qm.metrics,
		remoteWriter: remotewrite.NewWriter(id, qm.configStore, qm.metrics, logger),
	}
}
