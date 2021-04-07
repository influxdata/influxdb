package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/storage"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
	"go.uber.org/zap"
)

// StoragePointsWriterRecorder is an implementation of RunRecorder which
// writes runs via an implementation of storage PointsWriter
type StoragePointsWriterRecorder struct {
	pw storage.PointsWriter

	log *zap.Logger
}

// NewStoragePointsWriterRecorder configures and returns a new *StoragePointsWriterRecorder
func NewStoragePointsWriterRecorder(log *zap.Logger, pw storage.PointsWriter) *StoragePointsWriterRecorder {
	return &StoragePointsWriterRecorder{pw, log}
}

// Record formats the provided run as a models.Point and writes the resulting
// point to an underlying storage.PointsWriter
func (s *StoragePointsWriterRecorder) Record(ctx context.Context, orgID platform.ID, org string, bucketID platform.ID, bucket string, run *taskmodel.Run) error {
	tags := models.NewTags(map[string]string{
		statusTag: run.Status,
		taskIDTag: run.TaskID.String(),
	})

	// log an error if we have incomplete data on finish
	if !run.ID.Valid() ||
		run.ScheduledFor.IsZero() ||
		run.StartedAt.IsZero() ||
		run.FinishedAt.IsZero() ||
		run.Status == "" {
		s.log.Error("Run missing critical fields", zap.String("run", fmt.Sprintf("%+v", run)), zap.String("runID", run.ID.String()))
	}

	fields := map[string]interface{}{}
	fields[runIDField] = run.ID.String()
	fields[startedAtField] = run.StartedAt.Format(time.RFC3339Nano)
	fields[finishedAtField] = run.FinishedAt.Format(time.RFC3339Nano)
	fields[scheduledForField] = run.ScheduledFor.Format(time.RFC3339)
	fields[requestedAtField] = run.RequestedAt.Format(time.RFC3339)

	startedAt := run.StartedAt
	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	}

	logBytes, err := json.Marshal(run.Log)
	if err != nil {
		return err
	}
	fields[logField] = string(logBytes)

	point, err := models.NewPoint("runs", tags, fields, startedAt)
	if err != nil {
		return err
	}

	// TODO - fix
	return s.pw.WritePoints(ctx, orgID, bucketID, models.Points{point})
}
