package mock

import (
	"sync"

	"github.com/influxdata/platform/models"
)

// PointsWriter is a mock structure for writing points.
type PointsWriter struct {
	mu     sync.RWMutex
	Points []models.Point
	Err    error
}

// ForceError is for error testing, if WritePoints is called after ForceError, it will return that error.
func (p *PointsWriter) ForceError(err error) {
	p.mu.Lock()
	p.Err = err
	p.mu.Unlock()
}

// WritePoints writes points to the PointsWriter that will be exposed in the Values.
func (p *PointsWriter) WritePoints(points []models.Point) error {
	p.mu.Lock()
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
