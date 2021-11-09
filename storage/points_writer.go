package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/models"
)

// PointsWriter describes the ability to write points into a storage engine.
type PointsWriter interface {
	WritePoints(ctx context.Context, orgID platform.ID, bucketID platform.ID, points []models.Point) error
}

// LoggingPointsWriter wraps an underlying points writer but writes logs to
// another bucket when an error occurs.
type LoggingPointsWriter struct {
	// Wrapped points writer. Errored writes from here will be logged.
	Underlying PointsWriter

	// Service used to look up logging bucket.
	BucketFinder BucketFinder

	// Name of the bucket to log to.
	LogBucketName string
}

// WritePoints writes points to the underlying PointsWriter. Logs on error.
func (w *LoggingPointsWriter) WritePoints(ctx context.Context, orgID platform.ID, bucketID platform.ID, p []models.Point) error {
	if len(p) == 0 {
		return nil
	}

	// Write to underlying writer and exit immediately if successful.
	err := w.Underlying.WritePoints(ctx, orgID, bucketID, p)
	if err == nil {
		return nil
	}

	// Attempt to lookup log bucket.
	bkts, n, e := w.BucketFinder.FindBuckets(ctx, influxdb.BucketFilter{
		OrganizationID: &orgID,
		Name:           &w.LogBucketName,
	})
	if e != nil {
		return e
	} else if n == 0 {
		return fmt.Errorf("logging bucket not found: %q", w.LogBucketName)
	}

	// Log error to bucket.
	pt, e := models.NewPoint(
		"write_errors",
		nil,
		models.Fields{"error": err.Error()},
		time.Now(),
	)
	if e != nil {
		return e
	}
	if e := w.Underlying.WritePoints(ctx, orgID, bkts[0].ID, []models.Point{pt}); e != nil {
		return e
	}

	return err
}
