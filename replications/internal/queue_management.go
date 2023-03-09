package internal

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/pkg/durablequeue"
	"github.com/influxdata/influxdb/v2/replications/metrics"
	"github.com/influxdata/influxdb/v2/replications/remotewrite"
	"go.uber.org/zap"
)

const (
	scannerAdvanceInterval = 10 * time.Second
	purgeInterval          = 60 * time.Second
	defaultMaxAge          = 7 * 24 * time.Hour // 1 week
)

type remoteWriter interface {
	Write(data []byte, attempt int) (time.Duration, error)
}

type replicationQueue struct {
	id            platform.ID
	orgID         platform.ID
	localBucketID platform.ID
	queue         *durablequeue.Queue
	wg            sync.WaitGroup
	done          chan struct{}
	receive       chan struct{}
	logger        *zap.Logger
	metrics       *metrics.ReplicationsMetrics
	remoteWriter  remoteWriter
	failedWrites  int
	maxAge        time.Duration
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
func (qm *durableQueueManager) InitializeQueue(replicationID platform.ID, maxQueueSizeBytes int64, orgID platform.ID, localBucketID platform.ID, maxAgeSeconds int64) error {
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
	rq := qm.newReplicationQueue(replicationID, orgID, localBucketID, newQueue, maxAgeSeconds)
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
	retry := time.NewTimer(math.MaxInt64)
	purgeTicker := time.NewTicker(purgeInterval)

	sendWrite := func() time.Duration {
		for {
			waitForRetry, shouldRetry := rq.SendWrite()

			if !shouldRetry {
				return math.MaxInt64
			}

			// immediately retry if the wait time is zero
			if waitForRetry == 0 {
				continue
			}
			return waitForRetry
		}
	}

	for {
		select {
		case <-rq.done: // end the goroutine when done is messaged
			return
		case <-rq.receive: // run the scanner on data append
			// Receive channel has a buffer to prevent a potential race condition where rq.SendWrite has reached EOF and will
			// return false, but data is queued after evaluating the scanner and before the loop is ready to select on the
			// receive channel again. This would result in data remaining unprocessed in the queue until the next send on the
			// receive channel since the send to the receive channel in qm.EnqueueData is non-blocking. The buffer ensures
			// that rq.SendWrite will be called again in this situation and not leave data in the queue. Outside of this
			// specific scenario, the buffer might result in an extra call to rq.SendWrite that will immediately return on
			// EOF.
			retryTime := sendWrite()
			if !retry.Stop() {
				<-retry.C
			}
			retry.Reset(retryTime)
		case <-retry.C:
			retryTime := sendWrite()
			retry.Reset(retryTime)
		case <-purgeTicker.C:
			if rq.maxAge != 0 {
				rq.queue.PurgeOlderThan(time.Now().Add(-rq.maxAge))
			}
		}
	}
}

// SendWrite processes data enqueued into the durablequeue.Queue.
// SendWrite is responsible for processing all data in the queue at the time of calling.
func (rq *replicationQueue) SendWrite() (waitForRetry time.Duration, shouldRetry bool) {
	// Any error in creating the scanner should exit the loop in run()
	// Either it is io.EOF indicating no data, or some other failure in making
	// the Scanner object that we don't know how to handle.
	scan, err := rq.queue.NewScanner()
	if err != nil {
		if !errors.Is(err, io.EOF) {
			rq.logger.Error("Error creating replications queue scanner", zap.Error(err))
		}
		return 0, false
	}

	advanceScanner := func() error {
		if _, err = scan.Advance(); err != nil {
			if err != io.EOF {
				rq.logger.Error("Error in replication queue scanner", zap.Error(err))
			}
			return err
		}
		rq.metrics.Dequeue(rq.id, rq.queue.TotalBytes())
		return nil
	}

	ticker := time.NewTicker(scannerAdvanceInterval)
	defer ticker.Stop()

	for scan.Next() {
		if err := scan.Err(); err != nil {
			if errors.Is(err, io.EOF) {
				// An io.EOF error here indicates that there is no more data left to process, and is an expected error.
				return 0, false
			}
			// Any other error here indicates a problem reading the data from the queue, so we log the error and drop the data
			// with a call to scan.Advance() later.
			rq.logger.Info("Segment read error.", zap.Error(scan.Err()))
		}

		if waitForRetry, err := rq.remoteWriter.Write(scan.Bytes(), rq.failedWrites); err != nil {
			rq.failedWrites++
			// We failed the remote write. Do not advance the scanner
			rq.logger.Error("Error in replication stream", zap.Error(err), zap.Int("retries", rq.failedWrites))
			return waitForRetry, true
		}

		// a successful write resets the number of failed write attempts to zero
		rq.failedWrites = 0

		// Advance the scanner periodically to prevent extended runs of local writes without updating the underlying queue
		// position.
		select {
		case <-ticker.C:
			if err := advanceScanner(); err != nil {
				return 0, false
			}
		default:
		}
	}

	if err := advanceScanner(); err != nil {
		return 0, false
	}
	return 0, true
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

// Returns the remaining number of bytes in Queue to be read:
func (qm *durableQueueManager) RemainingQueueSizes(ids []platform.ID) (map[platform.ID]int64, error) {
	qm.mutex.RLock()
	defer qm.mutex.RUnlock()

	sizes := make(map[platform.ID]int64, len(ids))

	for _, id := range ids {
		if _, exist := qm.replicationQueues[id]; !exist {
			return nil, fmt.Errorf("durable queue not found for replication ID %q", id)
		}
		sizes[id] = qm.replicationQueues[id].queue.TotalBytes()
	}

	return sizes, nil
}

// StartReplicationQueues updates the durableQueueManager.replicationQueues map, fully removing any partially deleted
// queues (present on disk, but not tracked in sqlite), opening all current queues, and logging info for each.
func (qm *durableQueueManager) StartReplicationQueues(trackedReplications map[platform.ID]*influxdb.TrackedReplication) error {
	errOccurred := false

	for id, repl := range trackedReplications {
		// Re-initialize a queue struct for each replication stream from sqlite
		queue, err := durablequeue.NewQueue(
			filepath.Join(qm.queuePath, id.String()),
			repl.MaxQueueSizeBytes,
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
			// This could have errored after a backup/restore (we do not persist the replicationq).
			// Check if the dir exists, create if it doesn't, then open and carry on
			if pErr, ok := err.(*fs.PathError); ok {
				path := pErr.Path
				if _, err := os.Stat(path); err != nil && os.IsNotExist(err) {
					if err := os.MkdirAll(path, 0777); err != nil {
						qm.logger.Error("error attempting to recreate missing replication queue", zap.Error(err), zap.String("id", id.String()), zap.String("path", path))
						errOccurred = true
						continue
					}

					if err := queue.Open(); err != nil {
						qm.logger.Error("error attempting to open replication queue", zap.Error(err), zap.String("id", id.String()), zap.String("path", path))
						errOccurred = true
						continue
					}

					qm.replicationQueues[id] = qm.newReplicationQueue(id, repl.OrgID, repl.LocalBucketID, queue, repl.MaxAgeSeconds)
					qm.replicationQueues[id].Open()
					qm.logger.Info("Opened replication stream", zap.String("id", id.String()), zap.String("path", queue.Dir()))
				}
			} else {
				qm.logger.Error("failed to open replication stream durable queue", zap.Error(err), zap.String("id", id.String()), zap.String("path", queue.Dir()))
				errOccurred = true
			}
		} else {
			qm.replicationQueues[id] = qm.newReplicationQueue(id, repl.OrgID, repl.LocalBucketID, queue, repl.MaxAgeSeconds)
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
func (qm *durableQueueManager) EnqueueData(replicationID platform.ID, data []byte, numPoints int) (err error) {
	qm.mutex.RLock()
	defer qm.mutex.RUnlock()

	// Update metrics if data fails to be added to queue
	defer func() {
		if err != nil {
			qm.metrics.EnqueueError(replicationID, len(data), numPoints)
		}
	}()

	rq, ok := qm.replicationQueues[replicationID]
	if !ok {
		return fmt.Errorf("durable queue not found for replication ID %q", replicationID)
	}

	if err := rq.queue.Append(data); err != nil {
		return err
	}
	// Update metrics for this replication queue when adding data to the queue.
	qm.metrics.EnqueueData(replicationID, len(data), numPoints, rq.queue.TotalBytes())

	// Send to the replication receive channel if it is not full to activate the queue processing. If the receive channel
	// is full, don't block further writes and return.
	select {
	case qm.replicationQueues[replicationID].receive <- struct{}{}:
	default:
	}

	return nil
}

func (qm *durableQueueManager) newReplicationQueue(id platform.ID, orgID platform.ID, localBucketID platform.ID, queue *durablequeue.Queue, maxAgeSeconds int64) *replicationQueue {
	logger := qm.logger.With(zap.String("replication_id", id.String()))
	done := make(chan struct{})
	// check for max age minimum
	var maxAgeTime time.Duration
	if maxAgeSeconds < 0 {
		maxAgeTime = defaultMaxAge
	} else {
		maxAgeTime = time.Duration(maxAgeSeconds) * time.Second
	}

	return &replicationQueue{
		id:            id,
		orgID:         orgID,
		localBucketID: localBucketID,
		queue:         queue,
		done:          done,
		receive:       make(chan struct{}, 1),
		logger:        logger,
		metrics:       qm.metrics,
		remoteWriter:  remotewrite.NewWriter(id, qm.configStore, qm.metrics, logger, done),
		maxAge:        maxAgeTime,
	}
}

// GetReplications returns the ids of all currently registered replication streams matching the provided orgID
// and localBucketID
func (qm *durableQueueManager) GetReplications(orgID platform.ID, localBucketID platform.ID) []platform.ID {
	replications := make([]platform.ID, 0, len(qm.replicationQueues))

	for _, repl := range qm.replicationQueues {
		if repl.orgID == orgID && repl.localBucketID == localBucketID {
			replications = append(replications, repl.id)
		}
	}
	return replications
}
