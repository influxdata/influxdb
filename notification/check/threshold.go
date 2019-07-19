package check

import (
	"encoding/json"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/notification"
)

var _ influxdb.Check = &Threshold{}

// Threshold is the threshold check.
type Threshold struct {
	Base
	Thresholds []ThresholdConfig `json:"thresholds"`
}

// Type returns the type of the check.
func (c Threshold) Type() string {
	return "threshold"
}

// Valid returns error if something is invalid.
func (c Threshold) Valid() error {
	if err := c.Base.Valid(); err != nil {
		return err
	}
	for _, cc := range c.Thresholds {
		if err := cc.Valid(); err != nil {
			return err
		}
	}
	return nil
}

type thresholdAlias Threshold

// MarshalJSON implement json.Marshaler interface.
func (c Threshold) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		struct {
			thresholdAlias
			Type string `json:"type"`
		}{
			thresholdAlias: thresholdAlias(c),
			Type:           c.Type(),
		})
}

// ThresholdConfig is the base of all threshold config.
type ThresholdConfig struct {
	// If true, only alert if all values meet threshold.
	AllValues  bool                    `json:"allValues"`
	Level      notification.CheckLevel `json:"level"`
	LowerBound *float64                `json:"lowerBound,omitempty"`
	UpperBound *float64                `json:"upperBound,omitempty"`
}

// Valid returns error if something is invalid.
func (c ThresholdConfig) Valid() error {
	if c.LowerBound == nil && c.UpperBound == nil {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "threshold must have at least one lowerBound or upperBound value",
		}
	}
	return nil
}
