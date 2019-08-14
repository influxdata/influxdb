package check

import (
	"encoding/json"
	"fmt"

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

type thresholdDecode struct {
	Base
	Thresholds []thresholdConfigDecode `json:"thresholds"`
}

type thresholdConfigDecode struct {
	ThresholdConfigBase
	Type   string  `json:"type"`
	Value  float64 `json:"value"`
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
	Within bool    `json:"within"`
}

// UnmarshalJSON implement json.Unmarshaler interface.
func (c *Threshold) UnmarshalJSON(b []byte) error {
	tdRaws := new(thresholdDecode)
	if err := json.Unmarshal(b, tdRaws); err != nil {
		return err
	}
	c.Base = tdRaws.Base
	for _, tdRaw := range tdRaws.Thresholds {
		switch tdRaw.Type {
		case "lesser":
			td := &Lesser{
				ThresholdConfigBase: tdRaw.ThresholdConfigBase,
				Value:               tdRaw.Value,
			}
			c.Thresholds = append(c.Thresholds, td)
		case "greater":
			td := &Greater{
				ThresholdConfigBase: tdRaw.ThresholdConfigBase,
				Value:               tdRaw.Value,
			}
			c.Thresholds = append(c.Thresholds, td)
		case "range":
			td := &Range{
				ThresholdConfigBase: tdRaw.ThresholdConfigBase,
				Min:                 tdRaw.Min,
				Max:                 tdRaw.Max,
				Within:              tdRaw.Within,
			}
			c.Thresholds = append(c.Thresholds, td)
		default:
			return &influxdb.Error{
				Msg: fmt.Sprintf("invalid threshold type %s", tdRaw.Type),
			}
		}
	}

	return nil
}

// ThresholdConfig is the base of all threshold config.
type ThresholdConfig interface {
	MarshalJSON() ([]byte, error)
	Valid() error
}

// Valid returns error if something is invalid.
func (c ThresholdConfigBase) Valid() error {
	return nil
}

// ThresholdConfigBase is the base of all threshold config.
type ThresholdConfigBase struct {
	// If true, only alert if all values meet threshold.
	AllValues bool                    `json:"allValues"`
	Level     notification.CheckLevel `json:"level"`
}

// Lesser threshold type.
type Lesser struct {
	ThresholdConfigBase
	Value float64 `json:"value,omitempty"`
}

type lesserAlias Lesser

// MarshalJSON implement json.Marshaler interface.
func (td Lesser) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		struct {
			lesserAlias
			Type string `json:"type"`
		}{
			lesserAlias: lesserAlias(td),
			Type:        "lesser",
		})
}

// Greater threshold type.
type Greater struct {
	ThresholdConfigBase
	Value float64 `json:"value,omitempty"`
}

type greaterAlias Greater

// MarshalJSON implement json.Marshaler interface.
func (td Greater) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		struct {
			greaterAlias
			Type string `json:"type"`
		}{
			greaterAlias: greaterAlias(td),
			Type:         "greater",
		})
}

// Range threshold type.
type Range struct {
	ThresholdConfigBase
	Min    float64 `json:"min,omitempty"`
	Max    float64 `json:"max,omitempty"`
	Within bool    `json:"within"`
}

type rangeAlias Range

// MarshalJSON implement json.Marshaler interface.
func (td Range) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		struct {
			rangeAlias
			Type string `json:"type"`
		}{
			rangeAlias: rangeAlias(td),
			Type:       "range",
		})
}

// Valid overwrite the base threshold.
func (td Range) Valid() error {
	if td.Min > td.Max {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "range threshold min can't be larger than max",
		}
	}
	return nil
}
