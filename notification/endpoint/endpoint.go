package endpoint

import (
	"encoding/json"
	"fmt"

	"github.com/influxdata/influxdb/v2"
)

// types of endpoints.
const (
	SlackType     = "slack"
	PagerDutyType = "pagerduty"
	HTTPType      = "http"
	TelegramType  = "telegram"
)

var typeToEndpoint = map[string]func() influxdb.NotificationEndpoint{
	SlackType:     func() influxdb.NotificationEndpoint { return &Slack{} },
	PagerDutyType: func() influxdb.NotificationEndpoint { return &PagerDuty{} },
	HTTPType:      func() influxdb.NotificationEndpoint { return &HTTP{} },
	TelegramType:  func() influxdb.NotificationEndpoint { return &Telegram{} },
}

// UnmarshalJSON will convert the bytes to notification endpoint.
func UnmarshalJSON(b []byte) (influxdb.NotificationEndpoint, error) {
	var raw struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, &influxdb.Error{
			Msg: "unable to detect the notification endpoint type from json",
		}
	}

	convertedFunc, ok := typeToEndpoint[raw.Type]
	if !ok {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  fmt.Sprintf("invalid notification endpoint type %s", raw.Type),
		}
	}
	converted := convertedFunc()

	if err := json.Unmarshal(b, converted); err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInternal,
			Err:  err,
		}
	}
	return converted, nil
}

// Base is the embed struct of every notification endpoint.
type Base struct {
	ID          *influxdb.ID    `json:"id,omitempty"`
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	OrgID       *influxdb.ID    `json:"orgID,omitempty"`
	Status      influxdb.Status `json:"status"`
	influxdb.CRUDLog
}

func (b Base) idStr() string {
	if b.ID == nil {
		return influxdb.ID(0).String()
	}
	return b.ID.String()
}

func (b Base) validID() bool {
	return b.ID != nil && b.ID.Valid()
}

func (b Base) valid() error {
	if !b.validID() {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "Notification Endpoint ID is invalid",
		}
	}
	if b.Name == "" {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "Notification Endpoint Name can't be empty",
		}
	}
	if b.Status != influxdb.Active && b.Status != influxdb.Inactive {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "invalid status",
		}
	}
	return nil
}

// GetID implements influxdb.Getter interface.
func (b Base) GetID() influxdb.ID {
	if b.ID == nil {
		return 0
	}
	return *b.ID
}

// GetName implements influxdb.Getter interface.
func (b *Base) GetName() string {
	return b.Name
}

// GetOrgID implements influxdb.Getter interface.
func (b Base) GetOrgID() influxdb.ID {
	return getID(b.OrgID)
}

// GetCRUDLog implements influxdb.Getter interface.
func (b Base) GetCRUDLog() influxdb.CRUDLog {
	return b.CRUDLog
}

// GetDescription implements influxdb.Getter interface.
func (b *Base) GetDescription() string {
	return b.Description
}

// GetStatus implements influxdb.Getter interface.
func (b *Base) GetStatus() influxdb.Status {
	return b.Status
}

// SetID will set the primary key.
func (b *Base) SetID(id influxdb.ID) {
	b.ID = &id
}

// SetOrgID will set the org key.
func (b *Base) SetOrgID(id influxdb.ID) {
	b.OrgID = &id
}

// SetName implements influxdb.Updator interface.
func (b *Base) SetName(name string) {
	b.Name = name
}

// SetDescription implements influxdb.Updator interface.
func (b *Base) SetDescription(description string) {
	b.Description = description
}

// SetStatus implements influxdb.Updator interface.
func (b *Base) SetStatus(status influxdb.Status) {
	b.Status = status
}

func getID(id *influxdb.ID) influxdb.ID {
	if id == nil {
		return 0
	}
	return *id
}
