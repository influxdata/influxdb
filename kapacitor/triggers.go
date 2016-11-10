package kapacitor

import "github.com/influxdata/chronograf"
import "fmt"

const (
	// Deadman triggers when data is missing for a period of time
	Deadman = "deadman"
	// Relative triggers when the value has changed compared to the past
	Relative = "relative"
	// Threshold triggers when value crosses a threshold
	Threshold = "threshold"
)

// AllAlerts are properties all alert types will have
var AllAlerts = `
    .stateChangesOnly()
    .message(message)
	.id(idVar)
	.idTag(idTag)
	.levelTag(levelTag)
	.messageField(messageField)
	.durationField(durationField)
`

// ThresholdTrigger is the trickscript trigger for alerts that exceed a value
var ThresholdTrigger = `
  var trigger = data
  |alert()
    .crit(lambda: "value" %s crit)
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
        .crit(lambda: "value" %s crit)
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
        .crit(lambda: "value" %s crit)
`

// DeadmanTrigger checks if any data has been streamed in the last period of time
var DeadmanTrigger = `
  var trigger = data|deadman(threshold, period)
`

// Trigger returns the trigger mechanism for a tickscript
func Trigger(rule chronograf.AlertRule) (string, error) {
	var trigger string
	var err error
	switch rule.Trigger {
	case Deadman:
		trigger, err = DeadmanTrigger, nil
	case Relative:
		trigger, err = relativeTrigger(rule)
	case Threshold:
		trigger, err = thresholdTrigger(rule)
	default:
		trigger, err = "", fmt.Errorf("Unknown trigger type: %s", rule.Trigger)
	}

	if err != nil {
		return "", err
	}

	return trigger + AllAlerts, nil
}

func relativeTrigger(rule chronograf.AlertRule) (string, error) {
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
}

func thresholdTrigger(rule chronograf.AlertRule) (string, error) {
	op, err := kapaOperator(rule.TriggerValues.Operator)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(ThresholdTrigger, op), nil
}
