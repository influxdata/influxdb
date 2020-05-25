package endpoint

import (
	"encoding/json"

	"github.com/influxdata/influxdb/v2"
)

var _ influxdb.NotificationEndpoint = &Telegram{}

const telegramTokenSuffix = "-token"

// Telegram is the notification endpoint config of telegram.
type Telegram struct {
	Base
	// Token is the telegram bot token, see https://core.telegram.org/bots#creating-a-new-bot
	Token influxdb.SecretField `json:"token"`
}

// BackfillSecretKeys fill back the secret field key during the unmarshalling
// if value of that secret field is not nil.
func (s *Telegram) BackfillSecretKeys() {
	if s.Token.Key == "" && s.Token.Value != nil {
		s.Token.Key = s.idStr() + telegramTokenSuffix
	}
}

// SecretFields return available secret fields.
func (s Telegram) SecretFields() []influxdb.SecretField {
	arr := []influxdb.SecretField{}
	if s.Token.Key != "" {
		arr = append(arr, s.Token)
	}
	return arr
}

// Valid returns error if some configuration is invalid
func (s Telegram) Valid() error {
	if err := s.Base.valid(); err != nil {
		return err
	}
	if s.Token.Key == "" {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "empty telegram bot token",
		}
	}
	return nil
}

// MarshalJSON implement json.Marshaler interface.
func (s Telegram) MarshalJSON() ([]byte, error) {
	type telegramAlias Telegram
	return json.Marshal(
		struct {
			telegramAlias
			Type string `json:"type"`
		}{
			telegramAlias: telegramAlias(s),
			Type:          s.Type(),
		})
}

// Type returns the type.
func (s Telegram) Type() string {
	return TelegramType
}
