package storage

import (
	"context"

	"github.com/influxdata/influxdb/models"
)

// PointsWriter describes the ability to write points into a storage engine.
type PointsWriter interface {
	WritePoints(context.Context, []models.Point) error
}
