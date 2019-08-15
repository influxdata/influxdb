package check

import (
	"encoding/json"
	"fmt"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/notification"
)

// Base will embed inside a check.
type Base struct {
	ID                    influxdb.ID             `json:"id,omitempty"`
	Name                  string                  `json:"name"`
	Description           string                  `json:"description,omitempty"`
	AuthorizationID       influxdb.ID             `json:"authorizationID,omitempty"`
	OrgID                 influxdb.ID             `json:"orgID,omitempty"`
	Status                influxdb.Status         `json:"status"`
	Query                 influxdb.DashboardQuery `json:"query"`
	StatusMessageTemplate string                  `json:"statusMessageTemplate"`

	Cron  string            `json:"cron,omitempty"`
	Every influxdb.Duration `json:"every,omitempty"`
	// Offset represents a delay before execution.
	// It gets marshalled from a string duration, i.e.: "10s" is 10 seconds
	Offset influxdb.Duration `json:"offset,omitempty"`

	Tags []notification.Tag `json:"tags"`
	influxdb.CRUDLog
}

// Valid returns err if the check is invalid.
func (b Base) Valid() error {
	if !b.ID.Valid() {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "Check ID is invalid",
		}
	}
	if b.Name == "" {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "Check Name can't be empty",
		}
	}
	if !b.AuthorizationID.Valid() {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "Check AuthorizationID is invalid",
		}
	}
	if !b.OrgID.Valid() {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "Check OrgID is invalid",
		}
	}
	if b.Status != influxdb.Active && b.Status != influxdb.Inactive {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "invalid status",
		}
	}
	for _, tag := range b.Tags {
		if err := tag.Valid(); err != nil {
			return err
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

var typeToCheck = map[string](func() influxdb.Check){
	"deadman":   func() influxdb.Check { return &Deadman{} },
	"threshold": func() influxdb.Check { return &Threshold{} },
}

type rawRuleJSON struct {
	Typ string `json:"type"`
}

// UnmarshalJSON will convert
func UnmarshalJSON(b []byte) (influxdb.Check, error) {
	var raw rawRuleJSON
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, &influxdb.Error{
			Msg: "unable to detect the check type from json",
		}
	}
	convertedFunc, ok := typeToCheck[raw.Typ]
	if !ok {
		return nil, &influxdb.Error{
			Msg: fmt.Sprintf("invalid check type %s", raw.Typ),
		}
	}
	converted := convertedFunc()
	err := json.Unmarshal(b, converted)
	return converted, err
}
