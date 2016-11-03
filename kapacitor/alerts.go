package kapacitor

import (
	"fmt"

	"github.com/influxdata/chronograf"
)

// AlertServices generates alert chaining methods to be attached to an alert from all rule Services
func AlertServices(rule chronograf.AlertRule) (string, error) {
	alert := ""
	for _, service := range rule.Alerts {
		if err := ValidateAlert(service); err != nil {
			return "", err
		}
		alert = alert + fmt.Sprintf(".%s()", service)
	}
	return alert, nil
}
