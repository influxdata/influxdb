package internal

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/prom"
	"github.com/influxdata/influxdb/v2/kit/prom/promtest"
	"github.com/influxdata/influxdb/v2/replications/metrics"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
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

func TestEnqueueScan(t *testing.T) {
	t.Parallel()

	queuePath, qm := initQueueManager(t)
	defer os.RemoveAll(filepath.Dir(queuePath))

	// Create new queue
	err := qm.InitializeQueue(id1, maxQueueSizeBytes)
	require.NoError(t, err)

	// Enqueue some data
	testData := "weather,location=us-midwest temperature=82 1465839830100400200"
	qm.writeFunc = getTestWriteFunc(t, testData)
	err = qm.EnqueueData(id1, []byte(testData), 1)
	require.NoError(t, err)
}

func TestEnqueueScanMultiple(t *testing.T) {
	t.Parallel()

	queuePath, qm := initQueueManager(t)
	defer os.RemoveAll(filepath.Dir(queuePath))

	// Create new queue
	err := qm.InitializeQueue(id1, maxQueueSizeBytes)
	require.NoError(t, err)

	// Enqueue some data
	testData := "weather,location=us-midwest temperature=82 1465839830100400200"
	qm.writeFunc = getTestWriteFunc(t, testData)
	err = qm.EnqueueData(id1, []byte(testData), 1)
	require.NoError(t, err)

	err = qm.EnqueueData(id1, []byte(testData), 1)
	require.NoError(t, err)
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
	qm := NewDurableQueueManager(logger, queuePath, metrics.NewReplicationsMetrics(), WriteFunc)

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

func getTestWriteFunc(t *testing.T, expected string) func([]byte) error {
	t.Helper()
	return func(b []byte) error {
		require.Equal(t, expected, string(b))
		return nil
	}
}

func TestEnqueueData(t *testing.T) {
	t.Parallel()

	queuePath, err := os.MkdirTemp("", "testqueue")
	require.NoError(t, err)
	defer os.RemoveAll(queuePath)

	logger := zaptest.NewLogger(t)
	qm := NewDurableQueueManager(logger, queuePath, metrics.NewReplicationsMetrics(), WriteFunc)

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

	require.NoError(t, qm.EnqueueData(id1, []byte(data), 1))
	sizes, err = qm.CurrentQueueSizes([]platform.ID{id1})
	require.NoError(t, err)
	require.Greater(t, sizes[id1], int64(8))

	written, err := qm.replicationQueues[id1].queue.Current()
	require.NoError(t, err)

	require.Equal(t, data, string(written))
}

func TestEnqueueData_WithMetrics(t *testing.T) {
	t.Parallel()

	queuePath, err := os.MkdirTemp("", "testqueue")
	require.NoError(t, err)
	defer os.RemoveAll(queuePath)

	logger := zaptest.NewLogger(t)

	qm := NewDurableQueueManager(logger, queuePath, metrics.NewReplicationsMetrics(), WriteFunc)
	require.NoError(t, qm.InitializeQueue(id1, maxQueueSizeBytes))

	// close the scanner goroutine to specifically test EnqueueData()
	rq, ok := qm.replicationQueues[id1]
	require.True(t, ok)
	close(rq.done)

	reg := prom.NewRegistry(zaptest.NewLogger(t))
	reg.MustRegister(qm.metrics.PrometheusCollectors()...)

	data := []byte("some fake data")
	numPointsPerData := 3
	numDataToAdd := 4

	for i := 1; i <= numDataToAdd; i++ {
		go func() { <-rq.receive }() // absorb the receive to avoid testcase deadlock
		require.NoError(t, qm.EnqueueData(id1, data, numPointsPerData))

		pointCount := getPromMetric(t, "replications_queue_management_total_points_queued", reg)
		require.Equal(t, i*numPointsPerData, int(pointCount.Counter.GetValue()))

		totalBytesQueued := getPromMetric(t, "replications_queue_management_total_bytes_queued", reg)
		require.Equal(t, i*len(data), int(totalBytesQueued.Counter.GetValue()))

		currentBytesQueued := getPromMetric(t, "replications_queue_management_current_bytes_queued", reg)
		// 8 bytes for an empty queue; 8 extra bytes for each byte slice appended to the queue
		require.Equal(t, 8+i*(8+len(data)), int(currentBytesQueued.Gauge.GetValue()))
	}

	// Reduce the max segment size so that a new segment is created & the next call to SendWrite causes the first
	// segment to be dropped and the queue size on disk to be lower than before when the queue head is advanced.
	require.NoError(t, rq.queue.SetMaxSegmentSize(8))

	queueSizeBefore := rq.queue.DiskUsage()
	rq.SendWrite(func(bytes []byte) error {
		return nil
	})

	// Ensure that the smaller queue disk size was reflected in the metrics.
	currentBytesQueued := getPromMetric(t, "replications_queue_management_current_bytes_queued", reg)
	require.Less(t, int64(currentBytesQueued.Gauge.GetValue()), queueSizeBefore)
}

func getPromMetric(t *testing.T, name string, reg *prom.Registry) *dto.Metric {
	mfs := promtest.MustGather(t, reg)
	return promtest.FindMetric(mfs, name, map[string]string{
		"replicationID": id1.String(),
	})
}

func TestGoroutineReceives(t *testing.T) {
	t.Parallel()

	path, qm := initQueueManager(t)
	defer os.RemoveAll(path)
	require.NoError(t, qm.InitializeQueue(id1, maxQueueSizeBytes))
	require.DirExists(t, filepath.Join(path, id1.String()))

	rq, ok := qm.replicationQueues[id1]
	require.True(t, ok)
	require.NotNil(t, rq)
	close(rq.done) // atypical from normal behavior, but lets us receive channels to test

	go func() { require.NoError(t, qm.EnqueueData(id1, []byte("1234"), 1)) }()
	select {
	case <-rq.receive:
		return
	case <-time.After(time.Second):
		t.Fatal("Test timed out")
		return
	}
}

func TestGoroutineCloses(t *testing.T) {
	t.Parallel()

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
