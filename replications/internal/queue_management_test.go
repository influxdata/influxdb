package internal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

var (
	replicationID     = platform.ID(1)
	maxQueueSizeBytes = 3 * influxdb.DefaultReplicationMaxQueueSizeBytes
)

func TestCreateNewQueueDirExists(t *testing.T) {
	t.Parallel()

	tempEnginePath, err := os.MkdirTemp("", "engine")
	require.NoError(t, err)
	defer os.RemoveAll(tempEnginePath)

	logger := zaptest.NewLogger(t)
	qm := NewDurableQueueManager(logger, tempEnginePath)
	err = qm.InitializeQueue(replicationID, maxQueueSizeBytes)

	require.NoError(t, err)
	require.DirExists(t, filepath.Join(tempEnginePath, "replicationq", replicationID.String()))
}

func TestCreateNewQueueDuplicateID(t *testing.T) {
	t.Parallel()

	tempEnginePath, err := os.MkdirTemp("", "engine")
	require.NoError(t, err)
	defer os.RemoveAll(tempEnginePath)

	// Create a valid new queue
	logger := zaptest.NewLogger(t)
	qm := NewDurableQueueManager(logger, tempEnginePath)
	err = qm.InitializeQueue(replicationID, maxQueueSizeBytes)
	require.NoError(t, err)

	// Try to initialize another queue with the same replication ID
	err = qm.InitializeQueue(replicationID, maxQueueSizeBytes)
	require.EqualError(t, err, "durable queue already exists for replication ID \"0000000000000001\"")
}

func TestDeleteQueueDirRemoved(t *testing.T) {
	t.Parallel()

	tempEnginePath, err := os.MkdirTemp("", "engine")
	require.NoError(t, err)
	defer os.RemoveAll(tempEnginePath)

	// Create a valid new queue
	logger := zaptest.NewLogger(t)
	qm := NewDurableQueueManager(logger, tempEnginePath)
	err = qm.InitializeQueue(replicationID, maxQueueSizeBytes)
	require.NoError(t, err)
	require.DirExists(t, filepath.Join(tempEnginePath, "replicationq", replicationID.String()))

	// Delete queue and make sure its queue has been deleted from disk
	err = qm.DeleteQueue(replicationID)
	require.NoError(t, err)
	require.NoDirExists(t, filepath.Join(tempEnginePath, "replicationq", replicationID.String()))
}

func TestDeleteQueueNonexistentID(t *testing.T) {
	t.Parallel()

	tempEnginePath, err := os.MkdirTemp("", "engine")
	require.NoError(t, err)
	defer os.RemoveAll(tempEnginePath)

	logger := zaptest.NewLogger(t)
	qm := NewDurableQueueManager(logger, tempEnginePath)

	// Delete nonexistent queue
	err = qm.DeleteQueue(replicationID)
	require.EqualError(t, err, "durable queue not found for replication ID \"0000000000000001\"")
}

func TestUpdateMaxQueueSizeNonexistentID(t *testing.T) {
	t.Parallel()

	tempEnginePath, err := os.MkdirTemp("", "engine")
	require.NoError(t, err)
	defer os.RemoveAll(tempEnginePath)

	logger := zaptest.NewLogger(t)
	qm := NewDurableQueueManager(logger, tempEnginePath)

	// Update nonexistent queue
	err = qm.UpdateMaxQueueSize(replicationID, influxdb.DefaultReplicationMaxQueueSizeBytes)
	require.EqualError(t, err, "durable queue not found for replication ID \"0000000000000001\"")
}
