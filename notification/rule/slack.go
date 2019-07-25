package rule

import (
	"encoding/json"

	"github.com/influxdata/influxdb"
)

// Slack is the notification rule config of slack.
type Slack struct {
	Base
	Channel         string `json:"channel"`
	MessageTemplate string `json:"messageTemplate"`
}

type slackAlias Slack

// MarshalJSON implement json.Marshaler interface.
func (c Slack) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		struct {
			slackAlias
			Type string `json:"type"`
		}{
			slackAlias: slackAlias(c),
			Type:       c.Type(),
		})
}

// Valid returns where the config is valid.
func (c Slack) Valid() error {
	if err := c.Base.valid(); err != nil {
		return err
	}
	if c.MessageTemplate == "" {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "slack msg template is empty",
		}
	}
	return nil
}

// Type returns the type of the rule config.
func (c Slack) Type() string {
	return "slack"
}
