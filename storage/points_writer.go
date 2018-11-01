package storage

import (
	"github.com/influxdata/platform/models"
)

// PointsWriter describes the ability to write points into a storage engine.
type PointsWriter interface {
	WritePoints([]models.Point) error
}
