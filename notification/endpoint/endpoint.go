package endpoint

import (
	"encoding/json"
	"fmt"

	"github.com/influxdata/influxdb"
)

// types of endpoints.
const (
	SlackType     = "slack"
	PagerDutyType = "pagerduty"
	HTTPType      = "http"
)

var typeToEndpoint = map[string](func() influxdb.NotificationEndpoint){
	SlackType:     func() influxdb.NotificationEndpoint { return &Slack{} },
	PagerDutyType: func() influxdb.NotificationEndpoint { return &PagerDuty{} },
	HTTPType:      func() influxdb.NotificationEndpoint { return &HTTP{} },
}

// UnmarshalJSON will convert the bytes to notification endpoint.
func UnmarshalJSON(b []byte) (influxdb.NotificationEndpoint, error) {
	var raw struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, &influxdb.Error{
			Msg: "unable to detect the notification endpoint type from json",
		}
	}
	convertedFunc, ok := typeToEndpoint[raw.Type]
	if !ok {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  fmt.Sprintf("invalid notification endpoint type %s", raw.Type),
		}
	}
	converted := convertedFunc()
	if err := json.Unmarshal(b, converted); err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInternal,
			Err:  err,
		}
	}
	return converted, nil
}
