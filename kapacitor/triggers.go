package kapacitor

import "github.com/influxdata/chronograf"
import "fmt"

// Trigger returns the trigger mechanism for a tickscript
func Trigger(rule chronograf.AlertRule) (string, error) {
	switch rule.Trigger {
	case "deadman":
		return DeadmanTrigger, nil
	case "relative":
		return execTemplate(RelativeTrigger, rule)
	case "threshold":
		return execTemplate(ThresholdTrigger, rule)
	default:
		return "", fmt.Errorf("Unknown trigger type: %s", rule.Trigger)
	}
}
