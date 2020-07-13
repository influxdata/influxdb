package influxdb

import (
	"time"
)

// CRUDLogSetter is the interface to set the crudlog.
type CRUDLogSetter interface {
	SetCreatedAt(now time.Time)
	SetUpdatedAt(now time.Time)
}

// CRUDLog is the struct to store crud related ops.
type CRUDLog struct {
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// SetCreatedAt set the created time.
func (log *CRUDLog) SetCreatedAt(now time.Time) {
	log.CreatedAt = now
}

// SetUpdatedAt set the updated time.
func (log *CRUDLog) SetUpdatedAt(now time.Time) {
	log.UpdatedAt = now
}

// TimeGenerator represents a generator for now.
type TimeGenerator interface {
	// Now creates the generated time.
	Now() time.Time
}

// RealTimeGenerator will generate the real time.
type RealTimeGenerator struct{}

// Now returns the current time.
func (g RealTimeGenerator) Now() time.Time {
	return time.Now()
}
