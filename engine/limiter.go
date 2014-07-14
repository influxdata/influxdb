package engine

import (
	"github.com/influxdb/influxdb/protocol"
)

type Limiter struct {
	shouldLimit bool
	limit       int
	limits      map[string]int
}

func NewLimiter(limit int) *Limiter {
	return &Limiter{
		limit:       limit,
		limits:      map[string]int{},
		shouldLimit: limit > 0,
	}
}

func (self *Limiter) calculateLimitAndSlicePoints(series *protocol.Series) {
	if self.shouldLimit {
		// if the limit is 0, stop returning any points
		limit := self.limitForSeries(*series.Name)
		defer func() { self.limits[*series.Name] = limit }()
		if limit == 0 {
			series.Points = nil
			return
		}
		limit -= len(series.Points)
		if limit <= 0 {
			sliceTo := len(series.Points) + limit
			series.Points = series.Points[0:sliceTo]
			limit = 0
		}
	}
}

func (self *Limiter) hitLimit(seriesName string) bool {
	if !self.shouldLimit {
		return false
	}
	return self.limitForSeries(seriesName) <= 0
}

func (self *Limiter) limitForSeries(name string) int {
	currentLimit, ok := self.limits[name]
	if !ok {
		currentLimit = self.limit
		self.limits[name] = currentLimit
	}
	return currentLimit
}
