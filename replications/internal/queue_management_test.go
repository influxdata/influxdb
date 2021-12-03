package internal

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/prom"
	"github.com/influxdata/influxdb/v2/kit/prom/promtest"
	"github.com/influxdata/influxdb/v2/replications/metrics"
	replicationsMock "github.com/influxdata/influxdb/v2/replications/mock"
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

	data := "weather,location=us-midwest temperature=82 1465839830100400200"

	tests := []struct {
		name            string
		testData        []string
		writeFuncReturn error
	}{
		{
			name:            "single point with successful write",
			testData:        []string{data},
			writeFuncReturn: nil,
		},
		{
			name:            "multiple points with successful write",
			testData:        []string{data, data, data},
			writeFuncReturn: nil,
		},
		{
			name:            "single point with unsuccessful write",
			testData:        []string{data},
			writeFuncReturn: errors.New("some error"),
		},
		{
			name:            "multiple points with unsuccessful write",
			testData:        []string{data, data, data},
			writeFuncReturn: errors.New("some error"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queuePath, qm := initQueueManager(t)
			defer os.RemoveAll(filepath.Dir(queuePath))

			// Create new queue
			err := qm.InitializeQueue(id1, maxQueueSizeBytes)
			require.NoError(t, err)
			rq := qm.replicationQueues[id1]
			rq.remoteWriter = getTestRemoteWriter(t, data, tt.writeFuncReturn)

			// Enqueue the data
			for _, dat := range tt.testData {
				err = qm.EnqueueData(id1, []byte(dat), 1)
				require.NoError(t, err)
			}

			// Check queue position
			close(rq.done)
			rq.wg.Wait()
			scan, err := rq.queue.NewScanner()

			if tt.writeFuncReturn == nil {
				require.ErrorIs(t, io.EOF, err)
			} else {
				// Queue should not have advanced at all
				for range tt.testData {
					require.True(t, scan.Next())
				}
				// Should now be at the end of the queue
				require.False(t, scan.Next())
			}
		})
	}
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
	qm := NewDurableQueueManager(logger, queuePath, metrics.NewReplicationsMetrics(), replicationsMock.NewMockHttpConfigStore(nil))

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

type testRemoteWriter struct {
	writeFn func([]byte) error
}

func (tw *testRemoteWriter) Write(data []byte) error {
	return tw.writeFn(data)
}

func getTestRemoteWriter(t *testing.T, expected string, returning error) remoteWriter {
	t.Helper()

	writeFn := func(b []byte) error {
		require.Equal(t, expected, string(b))
		return returning
	}

	writer := &testRemoteWriter{}

	writer.writeFn = writeFn

	return writer
}

func TestEnqueueData(t *testing.T) {
	t.Parallel()

	queuePath, err := os.MkdirTemp("", "testqueue")
	require.NoError(t, err)
	defer os.RemoveAll(queuePath)

	logger := zaptest.NewLogger(t)
	qm := NewDurableQueueManager(logger, queuePath, metrics.NewReplicationsMetrics(), replicationsMock.NewMockHttpConfigStore(nil))

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

	path, qm := initQueueManager(t)
	defer os.RemoveAll(path)
	require.NoError(t, qm.InitializeQueue(id1, maxQueueSizeBytes))
	require.DirExists(t, filepath.Join(path, id1.String()))

	// close the scanner goroutine to specifically test EnqueueData()
	rq, ok := qm.replicationQueues[id1]
	require.True(t, ok)
	close(rq.done)

	reg := prom.NewRegistry(zaptest.NewLogger(t))
	reg.MustRegister(qm.metrics.PrometheusCollectors()...)

	data := "some fake data"
	numPointsPerData := 3
	numDataToAdd := 4
	rq.remoteWriter = getTestRemoteWriter(t, data, nil)

	for i := 1; i <= numDataToAdd; i++ {
		go func() { <-rq.receive }() // absorb the receive to avoid testcase deadlock
		require.NoError(t, qm.EnqueueData(id1, []byte(data), numPointsPerData))

		pointCount := getPromMetric(t, "replications_queue_total_points_queued", reg)
		require.Equal(t, i*numPointsPerData, int(pointCount.Counter.GetValue()))

		totalBytesQueued := getPromMetric(t, "replications_queue_total_bytes_queued", reg)
		require.Equal(t, i*len(data), int(totalBytesQueued.Counter.GetValue()))

		currentBytesQueued := getPromMetric(t, "replications_queue_current_bytes_queued", reg)
		// 8 extra bytes for each byte slice appended to the queue
		require.Equal(t, i*(8+len(data)), int(currentBytesQueued.Gauge.GetValue()))
	}

	// Queue size should be 0 after SendWrite completes
	rq.SendWrite()
	currentBytesQueued := getPromMetric(t, "replications_queue_current_bytes_queued", reg)
	require.Equal(t, float64(0), currentBytesQueued.Gauge.GetValue())
}

func TestEnqueueData_EnqueueFailure(t *testing.T) {
	t.Parallel()

	path, qm := initQueueManager(t)
	defer os.RemoveAll(path)
	require.NoError(t, qm.InitializeQueue(id1, maxQueueSizeBytes))
	require.DirExists(t, filepath.Join(path, id1.String()))

	rq, ok := qm.replicationQueues[id1]
	require.True(t, ok)
	// Close the underlying queue so an error is generated if we try to append to it
	require.NoError(t, rq.queue.Close())

	reg := prom.NewRegistry(zaptest.NewLogger(t))
	reg.MustRegister(qm.metrics.PrometheusCollectors()...)

	data := "some fake data"
	numPointsPerData := 3
	require.Error(t, qm.EnqueueData(id1, []byte(data), numPointsPerData)) // this will generate an error because of the closed queue

	droppedPoints := getPromMetric(t, "replications_queue_points_failed_to_queue", reg)
	require.Equal(t, numPointsPerData, int(droppedPoints.Counter.GetValue()))
	droppedBytes := getPromMetric(t, "replications_queue_bytes_failed_to_queue", reg)
	require.Equal(t, len(data), int(droppedBytes.Counter.GetValue()))
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

func TestEnqueueData_QueueAdvances(t *testing.T) {
	t.Parallel()

	path, qm := initQueueManager(t)
	defer os.RemoveAll(path)
	require.NoError(t, qm.InitializeQueue(id1, maxQueueSizeBytes))
	require.DirExists(t, filepath.Join(path, id1.String()))

	rq, ok := qm.replicationQueues[id1]
	require.True(t, ok)

	returnErrorOnWriteNumber := 5
	counter := 0
	var wg sync.WaitGroup
	var writesReceived [][]byte

	// Returns an error on a specific count of invocations; nil otherwise.
	writeFn := func(b []byte) error {
		counter++

		defer func() {
			// If the scanner is not advanced properly on remote writer errors, this assertion will panic due to the queue being
			// re-scanned and the WaitGroup going negative.
			require.NotPanics(t, func() { wg.Done() }, "writeFn called too many times")
		}()

		if counter == returnErrorOnWriteNumber {
			return errors.New("error")
		}

		// A remote server would receive the data on a successful remote write.
		writesReceived = append(writesReceived, b)
		return nil
	}
	writer := &testRemoteWriter{}
	writer.writeFn = writeFn
	rq.remoteWriter = writer

	wg.Add(1) // for the remote writer call that will return an error
	for i := 0; i < returnErrorOnWriteNumber*2; i++ {
		wg.Add(1)
		require.NoError(t, qm.EnqueueData(id1, []byte("test data"), 1))
	}
	wg.Wait()
	// Writes received by the hypothetical "remote" should match the enqueued data.
	require.Equal(t, returnErrorOnWriteNumber*2, len(writesReceived))
}
