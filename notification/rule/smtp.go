package rule

import (
	"encoding/json"
	"regexp"
	"strings"

	"github.com/influxdata/influxdb"
)

// SMTP is the notification rule config of email.
type SMTP struct {
	Base
	SubjectTemp string `json:"subjectTemplate"`
	BodyTemp    string `json:"bodyTemplate"`
	To          string `json:"to"`
}

type smtpAlias SMTP

// MarshalJSON implement json.Marshaler interface.
func (c SMTP) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		struct {
			smtpAlias
			Type string `json:"type"`
		}{
			smtpAlias: smtpAlias(c),
			Type:      c.Type(),
		})
}

// Valid returns where the config is valid.
func (c SMTP) Valid() error {
	if err := c.Base.valid(); err != nil {
		return err
	}
	emails := strings.Split(c.To, ",")
	for _, email := range emails {
		email = strings.TrimSpace(email)
		if email == "" {
			return &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "smtp email is empty",
			}
		}
		if !emailPattern.MatchString(email) {
			return &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "smtp invalid email address: " + email,
			}
		}
	}
	if c.SubjectTemp == "" {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "smtp empty subject template",
		}
	}
	return nil
}

// Type returns the type of the rule config.
func (c SMTP) Type() string {
	return "smtp"
}

var emailPattern = regexp.MustCompile(`[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,4}`)
