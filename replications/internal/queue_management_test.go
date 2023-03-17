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
	id3               = platform.ID(3)
	maxQueueSizeBytes = 3 * influxdb.DefaultReplicationMaxQueueSizeBytes
	orgID1            = platform.ID(500)
	localBucketID1    = platform.ID(999)
	orgID2            = platform.ID(123)
	localBucketID2    = platform.ID(456)
)

func TestCreateNewQueueDirExists(t *testing.T) {
	t.Parallel()

	queuePath, qm := initQueueManager(t)

	err := qm.InitializeQueue(id1, maxQueueSizeBytes, orgID1, localBucketID1, 0)

	require.NoError(t, err)
	require.DirExists(t, filepath.Join(queuePath, id1.String()))

	shutdown(t, qm)
}

func TestEnqueueScan(t *testing.T) {
	t.Parallel()

	data1 := "weather,location=us-midwest temperature=82 1465839830100400200"
	data2 := "weather,location=us-midwest temperature=84 1465839830100400201"
	data3 := "weather,location=us-midwest temperature=86 1465839830100400202"

	tests := []struct {
		name            string
		testData        []string
		writeFuncReturn error
	}{
		{
			name:            "single point with successful write",
			testData:        []string{data1},
			writeFuncReturn: nil,
		},
		{
			name:            "multiple points with successful write",
			testData:        []string{data1, data2, data3},
			writeFuncReturn: nil,
		},
		{
			name:            "single point with unsuccessful write",
			testData:        []string{data1},
			writeFuncReturn: errors.New("some error"),
		},
		{
			name:            "multiple points with unsuccessful write",
			testData:        []string{data1, data2, data3},
			writeFuncReturn: errors.New("some error"),
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			_, qm := initQueueManager(t)

			// Create new queue
			err := qm.InitializeQueue(id1, maxQueueSizeBytes, orgID1, localBucketID1, 0)
			require.NoError(t, err)
			rq := qm.replicationQueues[id1]
			rq.remoteWriter = getTestRemoteWriterSequenced(t, tt.testData, tt.writeFuncReturn, nil)

			// Enqueue the data
			for _, dat := range tt.testData {
				err = qm.EnqueueData(id1, []byte(dat), 1)
				require.NoError(t, err)
			}

			// Check queue position
			closeRq(rq)
			scan, err := rq.queue.NewScanner()
			t.Cleanup(func() {
				require.NoError(t, rq.queue.Close())
			})

			if tt.writeFuncReturn == nil {
				require.ErrorIs(t, err, io.EOF)
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

	_, qm := initQueueManager(t)

	// Create a valid new queue
	err := qm.InitializeQueue(id1, maxQueueSizeBytes, orgID1, localBucketID1, 0)
	require.NoError(t, err)

	// Try to initialize another queue with the same replication ID
	err = qm.InitializeQueue(id1, maxQueueSizeBytes, orgID1, localBucketID1, 0)
	require.EqualError(t, err, "durable queue already exists for replication ID \"0000000000000001\"")

	shutdown(t, qm)
}

func TestDeleteQueueDirRemoved(t *testing.T) {
	t.Parallel()

	queuePath, qm := initQueueManager(t)

	// Create a valid new queue
	err := qm.InitializeQueue(id1, maxQueueSizeBytes, orgID1, localBucketID1, 0)
	require.NoError(t, err)
	require.DirExists(t, filepath.Join(queuePath, id1.String()))

	// Delete queue and make sure its queue has been deleted from disk
	err = qm.DeleteQueue(id1)
	require.NoError(t, err)
	require.NoDirExists(t, filepath.Join(queuePath, id1.String()))
}

func TestDeleteQueueNonexistentID(t *testing.T) {
	t.Parallel()

	_, qm := initQueueManager(t)

	// Delete nonexistent queue
	err := qm.DeleteQueue(id1)
	require.EqualError(t, err, "durable queue not found for replication ID \"0000000000000001\"")
}

func TestUpdateMaxQueueSizeNonexistentID(t *testing.T) {
	t.Parallel()

	_, qm := initQueueManager(t)

	// Update nonexistent queue
	err := qm.UpdateMaxQueueSize(id1, influxdb.DefaultReplicationMaxQueueSizeBytes)
	require.EqualError(t, err, "durable queue not found for replication ID \"0000000000000001\"")
}

func TestStartReplicationQueue(t *testing.T) {
	t.Parallel()

	queuePath, qm := initQueueManager(t)

	// Create new queue
	err := qm.InitializeQueue(id1, maxQueueSizeBytes, orgID1, localBucketID1, 0)
	require.NoError(t, err)
	require.DirExists(t, filepath.Join(queuePath, id1.String()))

	// Represents the replications tracked in sqlite, this one is tracked
	trackedReplications := make(map[platform.ID]*influxdb.TrackedReplication)
	trackedReplications[id1] = &influxdb.TrackedReplication{
		MaxQueueSizeBytes: maxQueueSizeBytes,
		MaxAgeSeconds:     0,
		OrgID:             orgID1,
		LocalBucketID:     localBucketID1,
	}

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

	require.NoError(t, qm.replicationQueues[id1].queue.Close())
}

func TestStartReplicationQueuePartialDelete(t *testing.T) {
	t.Parallel()

	queuePath, qm := initQueueManager(t)

	// Create new queue
	err := qm.InitializeQueue(id1, maxQueueSizeBytes, orgID1, localBucketID1, 0)
	require.NoError(t, err)
	require.DirExists(t, filepath.Join(queuePath, id1.String()))

	// Represents the replications tracked in sqlite, replication above is not tracked (not present in map)
	trackedReplications := make(map[platform.ID]*influxdb.TrackedReplication)

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

	// Create queue1
	err := qm.InitializeQueue(id1, maxQueueSizeBytes, orgID1, localBucketID1, 0)
	require.NoError(t, err)
	require.DirExists(t, filepath.Join(queuePath, id1.String()))

	// Create queue2
	err = qm.InitializeQueue(id2, maxQueueSizeBytes, orgID2, localBucketID2, 0)
	require.NoError(t, err)
	require.DirExists(t, filepath.Join(queuePath, id2.String()))

	// Represents the replications tracked in sqlite, both replications above are tracked
	trackedReplications := make(map[platform.ID]*influxdb.TrackedReplication)
	trackedReplications[id1] = &influxdb.TrackedReplication{
		MaxQueueSizeBytes: maxQueueSizeBytes,
		MaxAgeSeconds:     0,
		OrgID:             orgID1,
		LocalBucketID:     localBucketID1,
	}
	trackedReplications[id2] = &influxdb.TrackedReplication{
		MaxQueueSizeBytes: maxQueueSizeBytes,
		MaxAgeSeconds:     0,
		OrgID:             orgID2,
		LocalBucketID:     localBucketID2,
	}

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

	require.NoError(t, qm.replicationQueues[id1].queue.Close())
	require.NoError(t, qm.replicationQueues[id2].queue.Close())
}

func TestStartReplicationQueuesMultipleWithPartialDelete(t *testing.T) {
	t.Parallel()

	queuePath, qm := initQueueManager(t)

	// Create queue1
	err := qm.InitializeQueue(id1, maxQueueSizeBytes, orgID1, localBucketID1, 0)
	require.NoError(t, err)
	require.DirExists(t, filepath.Join(queuePath, id1.String()))

	// Create queue2
	err = qm.InitializeQueue(id2, maxQueueSizeBytes, orgID2, localBucketID2, 0)
	require.NoError(t, err)
	require.DirExists(t, filepath.Join(queuePath, id2.String()))

	// Represents the replications tracked in sqlite, queue1 is tracked and queue2 is not
	trackedReplications := make(map[platform.ID]*influxdb.TrackedReplication)
	trackedReplications[id1] = &influxdb.TrackedReplication{
		MaxQueueSizeBytes: maxQueueSizeBytes,
		MaxAgeSeconds:     0,
		OrgID:             orgID1,
		LocalBucketID:     localBucketID1,
	}

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

	require.NoError(t, qm.replicationQueues[id1].queue.Close())
}

func initQueueManager(t *testing.T) (string, *durableQueueManager) {
	t.Helper()

	queuePath := filepath.Join(t.TempDir(), "replicationq")

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
	writeFn func([]byte, int) (time.Duration, error)
}

func (tw *testRemoteWriter) Write(data []byte, attempt int) (time.Duration, error) {
	return tw.writeFn(data, attempt)
}

func getTestRemoteWriterSequenced(t *testing.T, expected []string, returning error, wg *sync.WaitGroup) remoteWriter {
	t.Helper()

	count := 0
	writeFn := func(b []byte, attempt int) (time.Duration, error) {
		if count >= len(expected) {
			t.Fatalf("count larger than expected len, %d > %d", count, len(expected))
		}
		require.Equal(t, expected[count], string(b))
		if wg != nil {
			wg.Done()
		}
		// only progress the "pointer" if the data is successful
		// enqueueing with a returned error means the same first point is retried
		if returning == nil {
			count++
		}
		return time.Second, returning
	}

	writer := &testRemoteWriter{}

	writer.writeFn = writeFn

	return writer
}

func getTestRemoteWriter(t *testing.T, expected string) remoteWriter {
	t.Helper()

	writer := &testRemoteWriter{
		writeFn: func(b []byte, i int) (time.Duration, error) {
			require.Equal(t, expected, string(b))
			return time.Second, nil
		},
	}

	return writer
}

func TestEnqueueData(t *testing.T) {
	t.Parallel()

	queuePath := t.TempDir()

	logger := zaptest.NewLogger(t)
	qm := NewDurableQueueManager(logger, queuePath, metrics.NewReplicationsMetrics(), replicationsMock.NewMockHttpConfigStore(nil))

	require.NoError(t, qm.InitializeQueue(id1, maxQueueSizeBytes, orgID1, localBucketID1, 0))
	require.DirExists(t, filepath.Join(queuePath, id1.String()))

	sizes, err := qm.CurrentQueueSizes([]platform.ID{id1})
	require.NoError(t, err)
	// Empty queues are 8 bytes for the footer.
	require.Equal(t, map[platform.ID]int64{id1: 8}, sizes)
	// Remaining queue should initially be empty:
	rsizes, err := qm.RemainingQueueSizes([]platform.ID{id1})
	require.NoError(t, err)
	// Empty queue = 0 bytes:
	require.Equal(t, map[platform.ID]int64{id1: 0}, rsizes)

	data := "some fake data"

	// close the scanner goroutine to specifically test EnqueueData()
	rq, ok := qm.replicationQueues[id1]
	require.True(t, ok)
	closeRq(rq)
	t.Cleanup(func() {
		require.NoError(t, rq.queue.Close())
	})
	go func() { <-rq.receive }() // absorb the receive to avoid testcase deadlock

	require.NoError(t, qm.EnqueueData(id1, []byte(data), 1))
	sizes, err = qm.CurrentQueueSizes([]platform.ID{id1})
	require.NoError(t, err)
	require.Greater(t, sizes[id1], int64(8))
	rsizes, err = qm.RemainingQueueSizes([]platform.ID{id1})
	require.NoError(t, err)
	require.Greater(t, rsizes[id1], int64(0))
	// Difference between disk size and queue should only be footer size
	require.Equal(t, sizes[id1]-rsizes[id1], int64(8))

	written, err := qm.replicationQueues[id1].queue.Current()
	require.NoError(t, err)

	require.Equal(t, data, string(written))
}

// this test ensures that data does not get incorrectly dropped from the Queue on remote write failures
func TestSendWrite(t *testing.T) {
	t.Parallel()

	// data points to test
	var pointIndex int
	points := []string{
		"this is some data",
		"this is also some data",
		"this is even more data",
	}

	path, qm := initQueueManager(t)
	require.NoError(t, qm.InitializeQueue(id1, maxQueueSizeBytes, orgID1, localBucketID1, 0))
	require.DirExists(t, filepath.Join(path, id1.String()))

	// close the scanner goroutine to test SendWrite() with more granularity
	rq, ok := qm.replicationQueues[id1]
	require.True(t, ok)
	closeRq(rq)
	t.Cleanup(func() {
		require.NoError(t, rq.queue.Close())
	})
	go func() { <-rq.receive }() // absorb the receive to avoid testcase deadlock

	// Create custom remote writer that does some expected behavior
	// Will periodically fail to simulate a timeout
	shouldFailThisWrite := false
	writer := &testRemoteWriter{}
	writer.writeFn = func(data []byte, attempt int) (time.Duration, error) {
		require.Equal(t, []byte(points[pointIndex]), data)
		if shouldFailThisWrite {
			return 100, errors.New("remote timeout")
		}
		return 0, nil // current "success" return values
	}
	rq.remoteWriter = writer

	// Write first point
	require.NoError(t, qm.EnqueueData(id1, []byte(points[pointIndex]), 1))
	// Make sure the data is in the queue
	scan, err := rq.queue.NewScanner()
	require.NoError(t, err)
	require.True(t, scan.Next())
	require.Equal(t, []byte(points[pointIndex]), scan.Bytes())
	require.NoError(t, scan.Err())
	// Initial Queue size should be size of data + footer
	rsizesI, err := qm.RemainingQueueSizes([]platform.ID{id1})
	require.NoError(t, err)
	require.Equal(t, rsizesI[id1], int64(8+len(points[pointIndex])))
	// Send the write to the "remote" with a success
	rq.SendWrite()
	// Queue becomes empty after write:
	rsizesJ, err := qm.RemainingQueueSizes([]platform.ID{id1})
	require.NoError(t, err)
	require.Equal(t, rsizesJ[id1], int64(0))

	// Make sure the data is no longer in the queue
	_, err = rq.queue.NewScanner()
	require.Equal(t, io.EOF, err)

	// Write second point
	pointIndex++
	require.NoError(t, qm.EnqueueData(id1, []byte(points[pointIndex]), 1))
	// Make sure the data is in the queue
	scan, err = rq.queue.NewScanner()
	require.NoError(t, err)
	require.True(t, scan.Next())
	require.Equal(t, []byte(points[pointIndex]), scan.Bytes())
	require.NoError(t, scan.Err())
	rsizesI, err = qm.RemainingQueueSizes([]platform.ID{id1})
	require.NoError(t, err)
	// Send the write to the "remote" with a FAILURE
	shouldFailThisWrite = true
	rq.SendWrite()
	// Queue size should not have decreased if write has failed:
	rsizesJ, err = qm.RemainingQueueSizes([]platform.ID{id1})
	require.NoError(t, err)
	require.Equal(t, rsizesJ[id1], rsizesI[id1])
	// Make sure the data is still in the queue
	scan, err = rq.queue.NewScanner()
	require.NoError(t, err)
	require.True(t, scan.Next())
	require.Equal(t, []byte(points[pointIndex]), scan.Bytes())
	require.NoError(t, scan.Err())
	// Send the write to the "remote" again, with a SUCCESS
	shouldFailThisWrite = false
	rq.SendWrite()
	// Queue Becomes empty after a successful write
	rsizesJ, err = qm.RemainingQueueSizes([]platform.ID{id1})
	require.NoError(t, err)
	require.Equal(t, rsizesJ[id1], int64(0))

	// Make sure the data is no longer in the queue
	_, err = rq.queue.NewScanner()
	require.Equal(t, io.EOF, err)
}

func TestEnqueueData_WithMetrics(t *testing.T) {
	t.Parallel()

	path, qm := initQueueManager(t)
	require.NoError(t, qm.InitializeQueue(id1, maxQueueSizeBytes, orgID1, localBucketID1, 0))
	require.DirExists(t, filepath.Join(path, id1.String()))

	// close the scanner goroutine to specifically test EnqueueData()
	rq, ok := qm.replicationQueues[id1]
	require.True(t, ok)
	closeRq(rq)
	t.Cleanup(func() {
		require.NoError(t, rq.queue.Close())
	})

	reg := prom.NewRegistry(zaptest.NewLogger(t))
	reg.MustRegister(qm.metrics.PrometheusCollectors()...)

	data := "some fake data"
	numPointsPerData := 3
	numDataToAdd := 4
	rq.remoteWriter = getTestRemoteWriter(t, data)

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
	require.NoError(t, qm.InitializeQueue(id1, maxQueueSizeBytes, orgID1, localBucketID1, 0))
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
	require.NoError(t, qm.InitializeQueue(id1, maxQueueSizeBytes, orgID1, localBucketID1, 0))
	require.DirExists(t, filepath.Join(path, id1.String()))

	rq, ok := qm.replicationQueues[id1]
	require.True(t, ok)
	require.NotNil(t, rq)
	closeRq(rq) // atypical from normal behavior, but lets us receive channels to test
	t.Cleanup(func() {
		require.NoError(t, rq.queue.Close())
	})

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
	require.NoError(t, qm.InitializeQueue(id1, maxQueueSizeBytes, orgID1, localBucketID1, 0))
	require.DirExists(t, filepath.Join(path, id1.String()))

	rq, ok := qm.replicationQueues[id1]
	require.True(t, ok)
	require.NotNil(t, rq)
	require.NoError(t, qm.CloseAll())

	// wg should be zero here, indicating that the goroutine has closed
	// if this does not panic, then the routine is still active
	require.Panics(t, func() { rq.wg.Add(-1) })
}

// closeRq closes the done channel of a replication queue so that the run() function returns, but keeps the underlying
// queue open for testing purposes.
func closeRq(rq *replicationQueue) {
	close(rq.done)
	rq.wg.Wait() // wait for run() function to return
}

func TestGetReplications(t *testing.T) {
	t.Parallel()

	path, qm := initQueueManager(t)
	t.Cleanup(func() {
		shutdown(t, qm)
	})

	// Initialize 3 queues (2nd and 3rd share the same orgID and localBucket)
	require.NoError(t, qm.InitializeQueue(id1, maxQueueSizeBytes, orgID1, localBucketID1, 0))
	require.DirExists(t, filepath.Join(path, id1.String()))

	require.NoError(t, qm.InitializeQueue(id2, maxQueueSizeBytes, orgID2, localBucketID2, 0))
	require.DirExists(t, filepath.Join(path, id1.String()))

	require.NoError(t, qm.InitializeQueue(id3, maxQueueSizeBytes, orgID2, localBucketID2, 0))
	require.DirExists(t, filepath.Join(path, id1.String()))

	// Should return one matching replication queue (repl ID 1)
	expectedRepls := []platform.ID{id1}
	repls := qm.GetReplications(orgID1, localBucketID1)
	require.ElementsMatch(t, expectedRepls, repls)

	// Should return no matching replication queues
	require.Equal(t, 0, len(qm.GetReplications(orgID1, localBucketID2)))

	// Should return two matching replication queues (repl IDs 2 and 3)
	expectedRepls = []platform.ID{id2, id3}
	repls = qm.GetReplications(orgID2, localBucketID2)
	require.ElementsMatch(t, expectedRepls, repls)
}

func TestReplicationStartMissingQueue(t *testing.T) {
	t.Parallel()

	queuePath, qm := initQueueManager(t)

	// Create new queue
	err := qm.InitializeQueue(id1, maxQueueSizeBytes, orgID1, localBucketID1, 0)
	require.NoError(t, err)
	require.DirExists(t, filepath.Join(queuePath, id1.String()))

	// Represents the replications tracked in sqlite, this one is tracked
	trackedReplications := make(map[platform.ID]*influxdb.TrackedReplication)
	trackedReplications[id1] = &influxdb.TrackedReplication{
		MaxQueueSizeBytes: maxQueueSizeBytes,
		MaxAgeSeconds:     0,
		OrgID:             orgID1,
		LocalBucketID:     localBucketID1,
	}

	// Simulate server shutdown by closing all queues and clearing replicationQueues map
	shutdown(t, qm)

	// Delete the queue to simulate restoring from a backup
	err = os.RemoveAll(filepath.Join(queuePath))
	require.NoError(t, err)

	// Call startup function
	err = qm.StartReplicationQueues(trackedReplications)
	require.NoError(t, err)
	t.Cleanup(func() {
		shutdown(t, qm)
	})

	// Make sure queue is stored in map
	require.NotNil(t, qm.replicationQueues[id1])

	// Ensure queue is open by trying to remove, will error if open
	err = qm.replicationQueues[id1].queue.Remove()
	require.Errorf(t, err, "queue is open")
}
