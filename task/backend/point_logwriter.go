package backend

import (
	"context"
	"time"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
)

const (
	lineField         = "line"
	runIDField        = "runID"
	scheduledForField = "scheduledFor"
	requestedAtField  = "requestedAt"
	statusField       = "status"

	taskIDTag = "taskID"

	// Fixed system bucket ID for task and run logs.
	taskSystemBucketID platform.ID = 10
)

// Copy of storage.PointsWriter interface.
// Duplicating it here to avoid having tasks/backend depend directly on storage.
type PointsWriter interface {
	WritePoints(points []models.Point) error
}

// PointLogWriter writes task and run logs as time-series points.
type PointLogWriter struct {
	pointsWriter PointsWriter
}

// NewPointLogWriter returns a PointLogWriter.
func NewPointLogWriter(pw PointsWriter) *PointLogWriter {
	return &PointLogWriter{pointsWriter: pw}
}

func (p *PointLogWriter) UpdateRunState(ctx context.Context, rlb RunLogBase, when time.Time, status RunStatus) error {
	tags := models.Tags{
		models.NewTag([]byte(taskIDTag), []byte(rlb.Task.ID.String())),
	}
	fields := make(map[string]interface{}, 4)
	fields[statusField] = status.String()
	fields[runIDField] = rlb.RunID.String()
	fields[scheduledForField] = time.Unix(rlb.RunScheduledFor, 0).UTC().Format(time.RFC3339)
	if rlb.RequestedAt != 0 {
		fields[requestedAtField] = time.Unix(rlb.RequestedAt, 0).UTC().Format(time.RFC3339)
	}

	pt, err := models.NewPoint("records", tags, fields, when)
	if err != nil {
		return err
	}

	// TODO(mr): it would probably be lighter-weight to just build exploded points in the first place.
	exploded, err := tsdb.ExplodePoints(rlb.Task.Org, taskSystemBucketID, []models.Point{pt})
	if err != nil {
		return err
	}

	return p.pointsWriter.WritePoints(exploded)
}

func (p *PointLogWriter) AddRunLog(ctx context.Context, rlb RunLogBase, when time.Time, log string) error {
	tags := models.Tags{
		models.NewTag([]byte(taskIDTag), []byte(rlb.Task.ID.String())),
	}
	fields := map[string]interface{}{
		runIDField: rlb.RunID.String(),
		lineField:  log,
	}
	pt, err := models.NewPoint("logs", tags, fields, when)
	if err != nil {
		return err
	}

	// TODO(mr): it would probably be lighter-weight to just build exploded points in the first place.
	exploded, err := tsdb.ExplodePoints(rlb.Task.Org, taskSystemBucketID, []models.Point{pt})
	if err != nil {
		return err
	}

	return p.pointsWriter.WritePoints(exploded)
}
