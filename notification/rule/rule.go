package rule

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/notification"
)

var typToRule = map[string](func() influxdb.NotificationRule){
	"slack":     func() influxdb.NotificationRule { return &Slack{} },
	"smtp":      func() influxdb.NotificationRule { return &SMTP{} },
	"pagerduty": func() influxdb.NotificationRule { return &PagerDuty{} },
}

type rawRuleJSON struct {
	Typ string `json:"type"`
}

// UnmarshalJSON will convert
func UnmarshalJSON(b []byte) (influxdb.NotificationRule, error) {
	var raw rawRuleJSON
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, &influxdb.Error{
			Msg: "unable to detect the notification type from json",
		}
	}
	convertedFunc, ok := typToRule[raw.Typ]
	if !ok {
		return nil, &influxdb.Error{
			Msg: fmt.Sprintf("invalid notification type %s", raw.Typ),
		}
	}
	converted := convertedFunc()
	err := json.Unmarshal(b, converted)
	return converted, err
}

// Base is the embed struct of every notification rule.
type Base struct {
	ID              influxdb.ID     `json:"id,omitempty"`
	Name            string          `json:"name"`
	Description     string          `json:"description,omitempty"`
	EndpointID      *influxdb.ID    `json:"endpointID,omitempty"`
	OrgID           influxdb.ID     `json:"orgID,omitempty"`
	AuthorizationID influxdb.ID     `json:"authorizationID,omitempty"`
	Status          influxdb.Status `json:"status"`
	// SleepUntil is an optional sleeptime to start a task.
	SleepUntil *time.Time        `json:"sleepUntil,omitempty"`
	Cron       string            `json:"cron,omitempty"`
	Every      influxdb.Duration `json:"every,omitempty"`
	// Offset represents a delay before execution.
	// It gets marshalled from a string duration, i.e.: "10s" is 10 seconds
	Offset      influxdb.Duration         `json:"offset,omitempty"`
	RunbookLink string                    `json:"runbookLink"`
	TagRules    []notification.TagRule    `json:"tagRules,omitempty"`
	StatusRules []notification.StatusRule `json:"statusRules,omitempty"`
	*influxdb.Limit
	influxdb.CRUDLog
}

func (b Base) valid() error {
	if !b.ID.Valid() {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "Notification Rule ID is invalid",
		}
	}
	if b.Name == "" {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "Notification Rule Name can't be empty",
		}
	}
	if !b.AuthorizationID.Valid() {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "Notification Rule AuthorizationID is invalid",
		}
	}
	if !b.OrgID.Valid() {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "Notification Rule OrgID is invalid",
		}
	}
	if b.EndpointID != nil && !b.EndpointID.Valid() {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "Notification Rule EndpointID is invalid",
		}
	}
	if b.Status != influxdb.Active && b.Status != influxdb.Inactive {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "invalid status",
		}
	}
	for _, tagRule := range b.TagRules {
		if err := tagRule.Valid(); err != nil {
			return err
		}
	}
	if b.Limit != nil {
		if b.Limit.Every <= 0 || b.Limit.Rate <= 0 {
			return &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "if limit is set, limit and limitEvery must be larger than 0",
			}
		}
	}

	return nil
}

// GetID implements influxdb.Getter interface.
func (b Base) GetID() influxdb.ID {
	return b.ID
}

// GetOrgID implements influxdb.Getter interface.
func (b Base) GetOrgID() influxdb.ID {
	return b.OrgID
}

// GetCRUDLog implements influxdb.Getter interface.
func (b Base) GetCRUDLog() influxdb.CRUDLog {
	return b.CRUDLog
}

// GetLimit returns the limit pointer.
func (b *Base) GetLimit() *influxdb.Limit {
	return b.Limit
}

// GetName implements influxdb.Getter interface.
func (b *Base) GetName() string {
	return b.Name
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
	b.ID = id
}

// SetOrgID will set the org key.
func (b *Base) SetOrgID(id influxdb.ID) {
	b.OrgID = id
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
