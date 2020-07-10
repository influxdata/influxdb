package endpoint

import (
	"encoding/json"

	"github.com/influxdata/influxdb/v2"
)

var _ influxdb.NotificationEndpoint = &Teams{}

const teamsSecretSuffix = "-token"

// Teams is the notification endpoint config of Microdoft teams.
type Teams struct {
	Base
	// URL is the teams incoming webhook URL, see https://docs.microsoft.com/en-us/microsoftteams/platform/webhooks-and-connectors/how-to/connectors-using#setting-up-a-custom-incoming-webhook ,
	// for example: https://outlook.office.com/webhook/0acbc9c2-c262-11ea-b3de-0242ac130004
	URL string `json:"url"`
	// SecretURLSuffix is an optional secret suffix that is added to URL ,
	// for example: 0acbc9c2-c262-11ea-b3de-0242ac130004 is the secret part that is added to https://outlook.office.com/webhook/
	SecretURLSuffix influxdb.SecretField `json:"secretURLSuffix"`
}

// BackfillSecretKeys fill back the secret field key during the unmarshalling
// if value of that secret field is not nil.
func (s *Teams) BackfillSecretKeys() {
	if s.SecretURLSuffix.Key == "" && s.SecretURLSuffix.Value != nil {
		s.SecretURLSuffix.Key = s.idStr() + teamsSecretSuffix
	}
}

// SecretFields return available secret fields.
func (s Teams) SecretFields() []influxdb.SecretField {
	arr := []influxdb.SecretField{}
	if s.SecretURLSuffix.Key != "" {
		arr = append(arr, s.SecretURLSuffix)
	}
	return arr
}

// Valid returns error if some configuration is invalid
func (s Teams) Valid() error {
	if err := s.Base.valid(); err != nil {
		return err
	}
	if s.URL == "" {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "empty teams URL",
		}
	}
	return nil
}

// MarshalJSON implement json.Marshaler interface.
func (s Teams) MarshalJSON() ([]byte, error) {
	type teamsAlias Teams
	return json.Marshal(
		struct {
			teamsAlias
			Type string `json:"type"`
		}{
			teamsAlias: teamsAlias(s),
			Type:       s.Type(),
		})
}

// Type returns the type.
func (s Teams) Type() string {
	return TeamsType
}
