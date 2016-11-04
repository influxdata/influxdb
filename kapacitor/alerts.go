package kapacitor

import (
	"fmt"

	"github.com/influxdata/chronograf"
)

func kapaService(alert string) (string, error) {
	switch alert {
	case "hipchat":
		return "hipChat", nil
	case "opsgenie":
		return "opsGenie", nil
	case "pagerduty":
		return "pagerDuty", nil
	case "victorops":
		return "victorOps", nil
	case "smtp":
		return "email", nil
	case "sensu", "slack", "email", "talk", "telegram":
		return alert, nil
	default:
		return "", fmt.Errorf("Unsupport alert %s", alert)
	}
}

// AlertServices generates alert chaining methods to be attached to an alert from all rule Services
func AlertServices(rule chronograf.AlertRule) (string, error) {
	alert := ""
	for _, service := range rule.Alerts {
		srv, err := kapaService(service)
		if err != nil {
			return "", err
		}
		if err := ValidateAlert(srv); err != nil {
			return "", err
		}
		alert = alert + fmt.Sprintf(".%s()", srv)
	}
	return alert, nil
}
