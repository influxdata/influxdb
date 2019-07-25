package rule

import (
	"encoding/json"

	"github.com/influxdata/influxdb"
)

// PagerDuty is the rule config of pagerduty notification.
type PagerDuty struct {
	Base
	MessageTemp string `json:"messageTemplate"`
}

type pagerDutyAlias PagerDuty

// MarshalJSON implement json.Marshaler interface.
func (c PagerDuty) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		struct {
			pagerDutyAlias
			Type string `json:"type"`
		}{
			pagerDutyAlias: pagerDutyAlias(c),
			Type:           c.Type(),
		})
}

// Valid returns where the config is valid.
func (c PagerDuty) Valid() error {
	if err := c.Base.valid(); err != nil {
		return err
	}
	if c.MessageTemp == "" {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "pagerduty invalid message template",
		}
	}
	return nil
}

// Type returns the type of the rule config.
func (c PagerDuty) Type() string {
	return "pagerduty"
}
