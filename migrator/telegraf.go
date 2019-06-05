package migrator

import (
	"encoding/json"

	"github.com/influxdata/influxdb"
)

// OldTelegrafConfig is the old telegraf config helper to downgrade.
// this version may contains new fields, just make sure it won't be filled with Down() func.
type OldTelegrafConfig struct {
	Version        string                       `json:"version,omitempty"`
	ID             influxdb.ID                  `json:"id"`
	OrganizationID influxdb.ID                  `json:"organizationID,omitempty"`
	OrgID          influxdb.ID                  `json:"orgID,omitempty"`
	Name           string                       `json:"name"`
	Description    string                       `json:"description"`
	Agent          influxdb.TelegrafAgentConfig `json:"agent"`
	Plugins        []json.RawMessage            `json:"plugins"`
}

// NewTelegrafConfig is a helper to encode the new telegraf config.
type NewTelegrafConfig struct {
	Version     string                       `json:"version"`
	ID          influxdb.ID                  `json:"id"`
	OrgID       influxdb.ID                  `json:"orgID,omitempty"`
	Name        string                       `json:"name"`
	Description string                       `json:"description"`
	Agent       influxdb.TelegrafAgentConfig `json:"agent"`
	Plugins     []json.RawMessage            `json:"plugins"`
}

// TelegrafConfigMigrator is migrator for telegraf config.
type TelegrafConfigMigrator struct {
	Included bool
}

// Bucket returns the bucket where data is stored.
func (m TelegrafConfigMigrator) Bucket() []byte {
	return influxdb.TelegrafBucket
}

// NeedMigration returns whether this migrator is needed at runtime.
func (m TelegrafConfigMigrator) NeedMigration() bool {
	return m.Included
}

// Up convert the old version to the new version.
func (m TelegrafConfigMigrator) Up(src []byte) (dst []byte, err error) {
	op := "TelegrafConfig Migrator Up"
	old := new(OldTelegrafConfig)
	if err = json.Unmarshal(src, old); err != nil {
		return nil, &influxdb.Error{
			Err: influxdb.ErrBadSourceType,
			Op:  op,
		}
	}

	if old.Version == influxdb.NewestTelegrafConfig {
		return src, &influxdb.Error{
			Err: influxdb.ErrNoNeedToConvert,
			Op:  op,
		}
	}
	orgID := old.OrganizationID

	if !orgID.Valid() {
		orgID = old.OrgID
	}

	new := &NewTelegrafConfig{
		Version:     influxdb.NewestTelegrafConfig,
		ID:          old.ID,
		OrgID:       orgID,
		Name:        old.Name,
		Description: old.Description,
		Agent:       old.Agent,
		Plugins:     old.Plugins,
	}
	dst, err = json.Marshal(new)
	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
			Op:  op,
		}
	}
	return dst, nil
}

// Down convert the new version to the old version.
func (m TelegrafConfigMigrator) Down(src []byte) (dst []byte, err error) {
	op := "TelegrafConfig Migrator Down"
	new := new(NewTelegrafConfig)
	if err = json.Unmarshal(src, new); err != nil {
		return nil, &influxdb.Error{
			Err: influxdb.ErrBadSourceType,
			Op:  op,
		}
	}
	if !new.OrgID.Valid() {
		return nil, &influxdb.Error{
			Err: influxdb.ErrDamagedData,
			Op:  op,
		}
	}

	old := &OldTelegrafConfig{
		ID:             new.ID,
		OrganizationID: new.OrgID,
		Name:           new.Name,
		Description:    new.Description,
		Agent:          new.Agent,
		Plugins:        new.Plugins,
	}
	dst, err = json.Marshal(old)
	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
			Op:  op,
		}
	}
	return dst, nil
}
