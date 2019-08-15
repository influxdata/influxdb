package check

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/parser"
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

	// Care should be taken to prevent TaskID from being exposed publicly.
	TaskID influxdb.ID `json:"taskID,omitempty"`

	Cron  string    `json:"cron,omitempty"`
	Every *Duration `json:"every,omitempty"`
	// Offset represents a delay before execution.
	// It gets marshalled from a string duration, i.e.: "10s" is 10 seconds
	Offset *Duration `json:"offset,omitempty"`

	Tags []notification.Tag `json:"tags"`
	influxdb.CRUDLog
}

// Duration is a custom type used for generating flux compatible durations.
type Duration ast.DurationLiteral

// MarshalJSON turns a Duration into a JSON-ified string.
func (d Duration) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	b.WriteByte('"')
	for _, d := range d.Values {
		b.WriteString(strconv.Itoa(int(d.Magnitude)))
		b.WriteString(d.Unit)
	}
	b.WriteByte('"')

	return b.Bytes(), nil
}

// UnmarshalJSON turns a flux duration literal into a Duration.
func (d *Duration) UnmarshalJSON(b []byte) error {
	dur, err := parser.ParseDuration(string(b[1 : len(b)-1]))
	if err != nil {
		return err
	}

	*d = *(*Duration)(dur)

	return nil
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

// GetTaskID retrieves the task ID for a check.
func (b Base) GetTaskID() influxdb.ID {
	return b.TaskID
}

// GetCRUDLog implements influxdb.Getter interface.
func (b Base) GetCRUDLog() influxdb.CRUDLog {
	return b.CRUDLog
}

// GetAuthID gets the authID for a check
func (b Base) GetAuthID() influxdb.ID {
	return b.AuthorizationID
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

// ClearPrivateData remove any data that we don't want to be exposed publicly.
func (b *Base) ClearPrivateData() {
	b.TaskID = 0
}

// SetTaskID sets the taskID for a check.
func (b *Base) SetTaskID(id influxdb.ID) {
	b.TaskID = id
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
