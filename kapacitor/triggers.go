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
	.idTag(idTag)
	.levelTag(levelTag)
	.messageField(messageField)
	.durationField(durationField)
`

// RelativeAbsoluteTrigger compares one window of data versus another (current - past)
var RelativeAbsoluteTrigger = `
var past = data
	|shift(shift)

var current = data

var trigger = past
	|join(current)
		.as('past', 'current')
	|eval(lambda: float("current.value" - "past.value"))
		.keep()
		.as('value')
    |alert()
        .stateChangesOnly()
        .crit(lambda: "value" %s crit)
        .message(message)
		.id(idVar)
		.idTag(idTag)
		.levelTag(levelTag)
		.messageField(messageField)
		.durationField(durationField)
`

// RelativePercentTrigger compares one window of data versus another as a percent change.
var RelativePercentTrigger = `
var past = data
	|shift(shift)

var current = data

var trigger = past
	|join(current)
		.as('past', 'current')
	|eval(lambda: abs(float("current.value" - "past.value"))/float("past.value") * 100.0)
		.keep()
		.as('value')
    |alert()
        .stateChangesOnly()
        .crit(lambda: "value" %s crit)
        .message(message)
		.id(idVar)
		.idTag(idTag)
		.levelTag(levelTag)
		.messageField(messageField)
		.durationField(durationField)
`

// DeadmanTrigger checks if any data has been streamed in the last period of time
var DeadmanTrigger = `
  var trigger = data|deadman(threshold, period)
    .stateChangesOnly()
    .message(message)
	.id(idVar)
	.idTag(idTag)
	.levelTag(levelTag)
	.messageField(messageField)
	.durationField(durationField)
`

// Trigger returns the trigger mechanism for a tickscript
func Trigger(rule chronograf.AlertRule) (string, error) {
	switch rule.Trigger {
	case "deadman":
		return DeadmanTrigger, nil
	case "relative":
		op, err := kapaOperator(rule.TriggerValues.Operator)
		if err != nil {
			return "", err
		}
		if rule.TriggerValues.Change == "% change" {
			return fmt.Sprintf(RelativePercentTrigger, op), nil
		} else if rule.TriggerValues.Change == "change" {
			return fmt.Sprintf(RelativeAbsoluteTrigger, op), nil
		} else {
			return "", fmt.Errorf("Unknown change type %s", rule.TriggerValues.Change)
		}
	case "threshold":
		op, err := kapaOperator(rule.TriggerValues.Operator)
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
