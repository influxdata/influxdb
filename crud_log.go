package influxdb

import (
	"encoding/json"
	"errors"
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

// SetUpdated set the updated time.
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

// Duration is based on time.Duration to embed in any struct.
type Duration struct {
	time.Duration
}

// MarshalJSON implements json.Marshaler interface.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

// UnmarshalJSON implements json.Unmarshaler interface.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		d.Duration = time.Duration(value)
		return nil
	case string:
		var err error
		d.Duration, err = time.ParseDuration(value)
		if err != nil {
			return err
		}
		return nil
	default:
		return errors.New("invalid duration")
	}
}
