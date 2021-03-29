package mock

import (
	"context"
	"sync"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2/models"
)

// PointsWriter is a mock structure for writing points.
type PointsWriter struct {
	timesWriteCalled int
	mu               sync.RWMutex
	Points           []models.Point
	Err              error

	WritePointsFn func(ctx context.Context, orgID platform.ID, bucketID platform.ID, points []models.Point) error
}

// ForceError is for error testing, if WritePoints is called after ForceError, it will return that error.
func (p *PointsWriter) ForceError(err error) {
	p.mu.Lock()
	p.Err = err
	p.mu.Unlock()
}

// WritePoints writes points to the PointsWriter that will be exposed in the Values.
func (p *PointsWriter) WritePoints(ctx context.Context, orgID platform.ID, bucketID platform.ID, points []models.Point) error {
	if p.WritePointsFn != nil {
		return p.WritePointsFn(ctx, orgID, bucketID, points)
	}

	p.mu.Lock()
	p.timesWriteCalled++
	p.Points = append(p.Points, points...)
	err := p.Err
	p.mu.Unlock()
	return err
}

// Next returns the next (oldest) batch of values.
func (p *PointsWriter) Next() models.Point {
	var points models.Point
	p.mu.RLock()
	if len(p.Points) == 0 {
		p.mu.RUnlock()
		return points
	}
	p.mu.RUnlock()

	p.mu.Lock()
	defer p.mu.Unlock()
	points, p.Points = p.Points[0], p.Points[1:]
	return points
}

func (p *PointsWriter) WritePointsCalled() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.timesWriteCalled
}
