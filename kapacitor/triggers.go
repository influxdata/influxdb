package kapacitor

import "github.com/influxdata/chronograf"
import "fmt"

// ThresholdTrigger is the trickscript trigger for alerts that exceed a value
var ThresholdTrigger = `
  var trigger = data
  |alert()
    .stateChangesOnly()
    .crit(lambda: "value" %s crit)
    .message(message)
	.id(idVar)
	.idTag(idtag)
	.levelTag(leveltag)
	.messageField(messagefield)
	.durationField(durationfield)
`

// RelativeTrigger compares one window of data versus another.
var RelativeTrigger = `
var past = data
	|shift(shift)

var current = data

var trigger = past
	|join(current)
		.as('past', 'current')
	|eval(lambda: abs(float("current.value" - "past.value"))/float("past.value"))
		.keep()
		.as('value')
    |alert()
        .stateChangesOnly()
        .crit(lambda: "value" %s crit)
        .message(message)
		.id(idVar)
		.idTag(idtag)
		.levelTag(leveltag)
		.messageField(messagefield)
		.durationField(durationfield)
`

// DeadmanTrigger checks if any data has been streamed in the last period of time
var DeadmanTrigger = `
  var trigger = data|deadman(threshold, every)
    .stateChangesOnly()
    .message(message)
	.id(idVar)
	.idTag(idtag)
	.levelTag(leveltag)
	.messageField(messagefield)
	.durationField(durationfield)
`

// Trigger returns the trigger mechanism for a tickscript
func Trigger(rule chronograf.AlertRule) (string, error) {
	switch rule.Trigger {
	case "deadman":
		return DeadmanTrigger, nil
	case "relative":
		op, err := kapaOperator(rule.TriggerValues.Relative.Operator)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf(RelativeTrigger, op), nil
	case "threshold":
		op, err := kapaOperator(rule.TriggerValues.Threshold.Operator)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf(ThresholdTrigger, op), nil
	default:
		return "", fmt.Errorf("Unknown trigger type: %s", rule.Trigger)
	}
}

// kapaOperator converts UI strings to kapacitor operators
func kapaOperator(operator string) (string, error) {
	switch operator {
	case "greater than":
		return ">", nil
	case "less than":
		return "<", nil
	case "equal to or less than":
		return "<=", nil
	case "equal to or greater than":
		return ">=", nil
	case "equal":
		return "==", nil
	case "not equal":
		return "!=", nil
	default:
		return "", fmt.Errorf("invalid operator: %s is unknown", operator)
	}
}
