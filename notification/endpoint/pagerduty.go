package endpoint

import (
	"encoding/json"

	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/influxdata/influxdb/v2"
)

var _ influxdb.NotificationEndpoint = &PagerDuty{}

const routingKeySuffix = "-routing-key"

// PagerDuty is the notification endpoint config of pagerduty.
type PagerDuty struct {
	Base
	// ClientURL is the url that is presented in the PagerDuty UI when this alert is triggered
	ClientURL string `json:"clientURL"`
	// RoutingKey is a version 4 UUID expressed as a 32-digit hexadecimal number.
	// This is the Integration Key for an integration on any given service.
	RoutingKey influxdb.SecretField `json:"routingKey"`
}

// BackfillSecretKeys fill back fill the secret field key during the unmarshalling
// if value of that secret field is not nil.
func (s *PagerDuty) BackfillSecretKeys() {
	if s.RoutingKey.Key == "" && s.RoutingKey.Value != nil {
		s.RoutingKey.Key = s.idStr() + routingKeySuffix
	}
}

// SecretFields return available secret fields.
func (s PagerDuty) SecretFields() []influxdb.SecretField {
	return []influxdb.SecretField{
		s.RoutingKey,
	}
}

// Valid returns error if some configuration is invalid
func (s PagerDuty) Valid() error {
	if err := s.Base.valid(); err != nil {
		return err
	}
	if s.RoutingKey.Key == "" {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "pagerduty routing key is invalid",
		}
	}
	return nil
}

type pagerdutyAlias PagerDuty

// MarshalJSON implement json.Marshaler interface.
func (s PagerDuty) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		struct {
			pagerdutyAlias
			Type string `json:"type"`
		}{
			pagerdutyAlias: pagerdutyAlias(s),
			Type:           s.Type(),
		})
}

// Type returns the type.
func (s PagerDuty) Type() string {
	return PagerDutyType
}
