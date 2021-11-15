package internal

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"
)

var (
	id1               = platform.ID(1)
	id2               = platform.ID(2)
	maxQueueSizeBytes = 3 * influxdb.DefaultReplicationMaxQueueSizeBytes
)

func TestCreateNewQueueDirExists(t *testing.T) {
	t.Parallel()

	queuePath, qm := initQueueManager(t)
	defer os.RemoveAll(filepath.Dir(queuePath))

	err := qm.InitializeQueue(id1, maxQueueSizeBytes)

	require.NoError(t, err)
	require.DirExists(t, filepath.Join(queuePath, id1.String()))
}

func TestEnqueueScanLog(t *testing.T) {
	t.Parallel()

	// Initialize queue manager with zap observer (to allow assertions on log messages)
	enginePath, err := os.MkdirTemp("", "engine")
	require.NoError(t, err)
	queuePath := filepath.Join(enginePath, "replicationq")

	observedZapCore, observedLogs := observer.New(zap.InfoLevel)
	observedLogger := zap.New(observedZapCore)

	qm := NewDurableQueueManager(observedLogger, queuePath)
	defer os.RemoveAll(filepath.Dir(queuePath))

	// Create new queue
	err = qm.InitializeQueue(id1, maxQueueSizeBytes)
	require.NoError(t, err)

	// Enqueue some data
	testData := "weather,location=us-midwest temperature=82 1465839830100400200"
	err = qm.EnqueueData(id1, []byte(testData))
	require.NoError(t, err)

	// Give it a second to scan the queue
	time.Sleep(time.Second)

	// Check that data the scanner logs is the same as what was enqueued
	require.Equal(t, 1, observedLogs.Len())
	allLogs := observedLogs.All()
	firstLog := allLogs[0]
	require.Equal(t, firstLog.Message, "written bytes")
	require.Equal(t, "weather,location=us-midwest temperature=82 1465839830100400200", firstLog.ContextMap()["bytes"])
}

func TestEnqueueScanLogMultiple(t *testing.T) {
	t.Parallel()

	// Initialize queue manager with zap observer (to allow assertions on log messages)
	enginePath, err := os.MkdirTemp("", "engine")
	require.NoError(t, err)
	queuePath := filepath.Join(enginePath, "replicationq")

	observedZapCore, observedLogs := observer.New(zap.InfoLevel)
	observedLogger := zap.New(observedZapCore)

	qm := NewDurableQueueManager(observedLogger, queuePath)
	defer os.RemoveAll(filepath.Dir(queuePath))

	// Create new queue
	err = qm.InitializeQueue(id1, maxQueueSizeBytes)
	require.NoError(t, err)

	// Enqueue some data
	testData1 := "weather,location=us-midwest temperature=82 1465839830100400200"
	err = qm.EnqueueData(id1, []byte(testData1))
	require.NoError(t, err)

	testData2 := "weather,location=us-midwest temperature=83 1465839830100400201"
	err = qm.EnqueueData(id1, []byte(testData2))
	require.NoError(t, err)

	// Give it a second to scan the queue
	time.Sleep(time.Second)

	// Check that data the scanner logs is the same as what was enqueued
	require.Equal(t, 2, observedLogs.Len())
	allLogs := observedLogs.All()

	require.Equal(t, allLogs[0].Message, "written bytes")
	require.Equal(t, "weather,location=us-midwest temperature=82 1465839830100400200", allLogs[0].ContextMap()["bytes"])

	require.Equal(t, allLogs[1].Message, "written bytes")
	require.Equal(t, "weather,location=us-midwest temperature=83 1465839830100400201", allLogs[1].ContextMap()["bytes"])
}

func TestCreateNewQueueDuplicateID(t *testing.T) {
	t.Parallel()

	queuePath, qm := initQueueManager(t)
	defer os.RemoveAll(filepath.Dir(queuePath))

	// Create a valid new queue
	err := qm.InitializeQueue(id1, maxQueueSizeBytes)
	require.NoError(t, err)

	// Try to initialize another queue with the same replication ID
	err = qm.InitializeQueue(id1, maxQueueSizeBytes)
	require.EqualError(t, err, "durable queue already exists for replication ID \"0000000000000001\"")
}

func TestDeleteQueueDirRemoved(t *testing.T) {
	t.Parallel()

	queuePath, qm := initQueueManager(t)
	defer os.RemoveAll(filepath.Dir(queuePath))

	// Create a valid new queue
	err := qm.InitializeQueue(id1, maxQueueSizeBytes)
	require.NoError(t, err)
	require.DirExists(t, filepath.Join(queuePath, id1.String()))

	// Delete queue and make sure its queue has been deleted from disk
	err = qm.DeleteQueue(id1)
	require.NoError(t, err)
	require.NoDirExists(t, filepath.Join(queuePath, id1.String()))
}

func TestDeleteQueueNonexistentID(t *testing.T) {
	t.Parallel()

	queuePath, qm := initQueueManager(t)
	defer os.RemoveAll(filepath.Dir(queuePath))

	// Delete nonexistent queue
	err := qm.DeleteQueue(id1)
	require.EqualError(t, err, "durable queue not found for replication ID \"0000000000000001\"")
}

func TestUpdateMaxQueueSizeNonexistentID(t *testing.T) {
	t.Parallel()

	queuePath, qm := initQueueManager(t)
	defer os.RemoveAll(filepath.Dir(queuePath))

	// Update nonexistent queue
	err := qm.UpdateMaxQueueSize(id1, influxdb.DefaultReplicationMaxQueueSizeBytes)
	require.EqualError(t, err, "durable queue not found for replication ID \"0000000000000001\"")
}

func TestStartReplicationQueue(t *testing.T) {
	t.Parallel()

	queuePath, qm := initQueueManager(t)
	defer os.RemoveAll(filepath.Dir(queuePath))

	// Create new queue
	err := qm.InitializeQueue(id1, maxQueueSizeBytes)
	require.NoError(t, err)
	require.DirExists(t, filepath.Join(queuePath, id1.String()))

	// Represents the replications tracked in sqlite, this one is tracked
	trackedReplications := make(map[platform.ID]int64)
	trackedReplications[id1] = maxQueueSizeBytes

	// Simulate server shutdown by closing all queues and clearing replicationQueues map
	shutdown(t, qm)

	// Call startup function
	err = qm.StartReplicationQueues(trackedReplications)
	require.NoError(t, err)

	// Make sure queue is stored in map
	require.NotNil(t, qm.replicationQueues[id1])

	// Ensure queue is open by trying to remove, will error if open
	err = qm.replicationQueues[id1].queue.Remove()
	require.Errorf(t, err, "queue is open")
}

func TestStartReplicationQueuePartialDelete(t *testing.T) {
	t.Parallel()

	queuePath, qm := initQueueManager(t)
	defer os.RemoveAll(filepath.Dir(queuePath))

	// Create new queue
	err := qm.InitializeQueue(id1, maxQueueSizeBytes)
	require.NoError(t, err)
	require.DirExists(t, filepath.Join(queuePath, id1.String()))

	// Represents the replications tracked in sqlite, replication above is not tracked (not present in map)
	trackedReplications := make(map[platform.ID]int64)

	// Simulate server shutdown by closing all queues and clearing replicationQueues map
	shutdown(t, qm)

	// Call startup function
	err = qm.StartReplicationQueues(trackedReplications)
	require.NoError(t, err)

	// Make sure queue is not stored in map
	require.Nil(t, qm.replicationQueues[id1])

	// Check for queue on disk, should be removed
	require.NoDirExists(t, filepath.Join(queuePath, id1.String()))
}

func TestStartReplicationQueuesMultiple(t *testing.T) {
	t.Parallel()

	queuePath, qm := initQueueManager(t)
	defer os.RemoveAll(filepath.Dir(queuePath))

	// Create queue1
	err := qm.InitializeQueue(id1, maxQueueSizeBytes)
	require.NoError(t, err)
	require.DirExists(t, filepath.Join(queuePath, id1.String()))

	// Create queue2
	err = qm.InitializeQueue(id2, maxQueueSizeBytes)
	require.NoError(t, err)
	require.DirExists(t, filepath.Join(queuePath, id2.String()))

	// Represents the replications tracked in sqlite, both replications above are tracked
	trackedReplications := make(map[platform.ID]int64)
	trackedReplications[id1] = maxQueueSizeBytes
	trackedReplications[id2] = maxQueueSizeBytes

	// Simulate server shutdown by closing all queues and clearing replicationQueues map
	shutdown(t, qm)

	// Call startup function
	err = qm.StartReplicationQueues(trackedReplications)
	require.NoError(t, err)

	// Make sure both queues are stored in map
	require.NotNil(t, qm.replicationQueues[id1])
	require.NotNil(t, qm.replicationQueues[id2])

	// Make sure both queues are present on disk
	require.DirExists(t, filepath.Join(queuePath, id1.String()))
	require.DirExists(t, filepath.Join(queuePath, id2.String()))

	// Ensure both queues are open by trying to remove, will error if open
	err = qm.replicationQueues[id1].queue.Remove()
	require.Errorf(t, err, "queue is open")
	err = qm.replicationQueues[id2].queue.Remove()
	require.Errorf(t, err, "queue is open")
}

func TestStartReplicationQueuesMultipleWithPartialDelete(t *testing.T) {
	t.Parallel()

	queuePath, qm := initQueueManager(t)
	defer os.RemoveAll(filepath.Dir(queuePath))

	// Create queue1
	err := qm.InitializeQueue(id1, maxQueueSizeBytes)
	require.NoError(t, err)
	require.DirExists(t, filepath.Join(queuePath, id1.String()))

	// Create queue2
	err = qm.InitializeQueue(id2, maxQueueSizeBytes)
	require.NoError(t, err)
	require.DirExists(t, filepath.Join(queuePath, id2.String()))

	// Represents the replications tracked in sqlite, queue1 is tracked and queue2 is not
	trackedReplications := make(map[platform.ID]int64)
	trackedReplications[id1] = maxQueueSizeBytes

	// Simulate server shutdown by closing all queues and clearing replicationQueues map
	shutdown(t, qm)

	// Call startup function
	err = qm.StartReplicationQueues(trackedReplications)
	require.NoError(t, err)

	// Make sure queue1 is in replicationQueues map and queue2 is not
	require.NotNil(t, qm.replicationQueues[id1])
	require.Nil(t, qm.replicationQueues[id2])

	// Make sure queue1 is present on disk and queue2 has been removed
	require.DirExists(t, filepath.Join(queuePath, id1.String()))
	require.NoDirExists(t, filepath.Join(queuePath, id2.String()))

	// Ensure queue1 is open by trying to remove, will error if open
	err = qm.replicationQueues[id1].queue.Remove()
	require.Errorf(t, err, "queue is open")
}

func initQueueManager(t *testing.T) (string, *durableQueueManager) {
	t.Helper()

	enginePath, err := os.MkdirTemp("", "engine")
	require.NoError(t, err)
	queuePath := filepath.Join(enginePath, "replicationq")

	logger := zaptest.NewLogger(t)
	qm := NewDurableQueueManager(logger, queuePath)

	return queuePath, qm
}

func shutdown(t *testing.T, qm *durableQueueManager) {
	t.Helper()

	// Close all queues
	err := qm.CloseAll()
	require.NoError(t, err)

	// Clear replication queues map
	emptyMap := make(map[platform.ID]*replicationQueue)
	qm.replicationQueues = emptyMap
}

func TestEnqueueData(t *testing.T) {
	t.Parallel()

	queuePath, err := os.MkdirTemp("", "testqueue")
	require.NoError(t, err)
	defer os.RemoveAll(queuePath)

	logger := zaptest.NewLogger(t)
	qm := NewDurableQueueManager(logger, queuePath)

	require.NoError(t, qm.InitializeQueue(id1, maxQueueSizeBytes))
	require.DirExists(t, filepath.Join(queuePath, id1.String()))

	sizes, err := qm.CurrentQueueSizes([]platform.ID{id1})
	require.NoError(t, err)
	// Empty queues are 8 bytes for the footer.
	require.Equal(t, map[platform.ID]int64{id1: 8}, sizes)

	data := "some fake data"

	// close the scanner goroutine to specifically test EnqueueData()
	rq, ok := qm.replicationQueues[id1]
	require.True(t, ok)
	close(rq.done)
	go func() { <-rq.receive }() // absorb the receive to avoid testcase deadlock

	require.NoError(t, qm.EnqueueData(id1, []byte(data)))
	sizes, err = qm.CurrentQueueSizes([]platform.ID{id1})
	require.NoError(t, err)
	require.Greater(t, sizes[id1], int64(8))

	written, err := qm.replicationQueues[id1].queue.Current()
	require.NoError(t, err)

	require.Equal(t, data, string(written))
}

func TestGoroutineReceives(t *testing.T) {
	path, qm := initQueueManager(t)
	defer os.RemoveAll(path)
	require.NoError(t, qm.InitializeQueue(id1, maxQueueSizeBytes))
	require.DirExists(t, filepath.Join(path, id1.String()))

	rq, ok := qm.replicationQueues[id1]
	require.True(t, ok)
	require.NotNil(t, rq)
	close(rq.done) // atypical from normal behavior, but lets us receive channels to test

	// listen on the receive channel
	ch := make(chan struct{})
	go func() {
		var hasReceived bool
		for {
			select {
			case <-ch:
				require.True(t, hasReceived)
				return
			case <-rq.receive:
				hasReceived = true
			}
		}
	}()
	require.NoError(t, qm.EnqueueData(id1, []byte("1234")))
	time.Sleep(time.Second) // give some time to receive the channel
	ch <- struct{}{}
	time.Sleep(time.Second) // give some time to check that the channel was hit
}

func TestGoroutineCloses(t *testing.T) {
	path, qm := initQueueManager(t)
	defer os.RemoveAll(path)
	require.NoError(t, qm.InitializeQueue(id1, maxQueueSizeBytes))
	require.DirExists(t, filepath.Join(path, id1.String()))

	rq, ok := qm.replicationQueues[id1]
	require.True(t, ok)
	require.NotNil(t, rq)
	require.NoError(t, qm.CloseAll())

	// wg should be zero here, indicating that the goroutine has closed
	// if this does not panic, then the routine is still active
	require.Panics(t, func() { rq.wg.Add(-1) })
}
