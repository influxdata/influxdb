package endpoint

import (
	"encoding/json"
	"fmt"
	"net/smtp"
	"net/url"

	"github.com/influxdata/influxdb/v2"
)

var _ influxdb.NotificationEndpoint = &SMTP{}

const (
	smtpTokenSuffix    = "-token"
	smtpUsernameSuffix = "-username"
	smtpPasswordSuffix = "-password"
)

// SMTP is the notification endpoint config of smtp.
type SMTP struct {
	Base
	// Path is the API path of SMTP
	URL string `json:"url"`
	// Token is the bearer token for authorization
	Token           influxdb.SecretField `json:"token,omitempty"`
	Username        influxdb.SecretField `json:"username,omitempty"`
	Password        influxdb.SecretField `json:"password,omitempty"`
	ContentTemplate string               `json:"contentTemplate"`
}

// BackfillSecretKeys fill back fill the secret field key during the unmarshalling
// if value of that secret field is not nil.
func (s *SMTP) BackfillSecretKeys() {
	if s.Token.Key == "" && s.Token.Value != nil {
		s.Token.Key = s.idStr() + smtpTokenSuffix
	}
	if s.Username.Key == "" && s.Username.Value != nil {
		s.Username.Key = s.idStr() + smtpUsernameSuffix
	}
	if s.Password.Key == "" && s.Password.Value != nil {
		s.Password.Key = s.idStr() + smtpPasswordSuffix
	}
}

// SecretFields return available secret fields.
func (s SMTP) SecretFields() []influxdb.SecretField {
	arr := make([]influxdb.SecretField, 0)
	if s.Token.Key != "" {
		arr = append(arr, s.Token)
	}
	if s.Username.Key != "" {
		arr = append(arr, s.Username)
	}
	if s.Password.Key != "" {
		arr = append(arr, s.Password)
	}
	return arr
}

// Valid returns error if some configuration is invalid
func (s SMTP) Valid() error {
	if err := s.Base.valid(); err != nil {
		return err
	}
	if s.URL == "" {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "smtp endpoint URL is empty",
		}
	}
	if _, err := url.Parse(s.URL); err != nil {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  fmt.Sprintf("smtp endpoint URL is invalid: %s", err.Error()),
		}
	}
	return nil
}

// MarshalJSON implement json.Marshaler interface.
func (s SMTP) MarshalJSON() ([]byte, error) {
	type smtpAlias SMTP
	return json.Marshal(
		struct {
			smtpAlias
			Type string `json:"type"`
		}{
			smtpAlias: smtpAlias(s),
			Type:      s.Type(),
		})
}

// Type returns the type.
func (s SMTP) Type() string {
	return SMTPType
}

func send(senderMail, senderPassword string, message []byte, to []string) {
	// smtp server configuration.
	smtpHost := "smtp.gmail.com"
	smtpPort := "587"

	// Authentication.
	auth := smtp.PlainAuth("", senderMail, senderPassword, smtpHost)

	// Sending email.
	err := smtp.SendMail(smtpHost+":"+smtpPort, auth, senderMail, to, message)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Email Sent Successfully!")
}
