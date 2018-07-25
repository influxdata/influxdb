package kapacitor

import (
	"fmt"

	"github.com/influxdata/platform/chronograf"
)

const (
	// Deadman triggers when data is missing for a period of time
	Deadman = "deadman"
	// Relative triggers when the value has changed compared to the past
	Relative = "relative"
	// Threshold triggers when value crosses a threshold
	Threshold = "threshold"
	// ThresholdRange triggers when a value is inside or outside a range
	ThresholdRange = "range"
	// ChangePercent triggers a relative alert when value changed by a percentage
	ChangePercent = "% change"
	// ChangeAmount triggers a relative alert when the value change by some amount
	ChangeAmount = "change"
)

// AllAlerts are properties all alert types will have
var AllAlerts = `
    .message(message)
	.id(idVar)
	.idTag(idTag)
	.levelTag(levelTag)
	.messageField(messageField)
	.durationField(durationField)
`

// Details is used only for alerts that specify detail string
var Details = `
	.details(details)
`

// ThresholdTrigger is the tickscript trigger for alerts that exceed a value
var ThresholdTrigger = `
  var trigger = data
  |alert()
    .crit(lambda: "value" %s crit)
`

// ThresholdRangeTrigger is the alert when data does not intersect the range.
var ThresholdRangeTrigger = `
	var trigger = data
	|alert()
		.crit(lambda: "value" %s lower %s "value" %s upper)
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
		if rule.TriggerValues.RangeValue == "" {
			trigger, err = thresholdTrigger(rule)
		} else {
			trigger, err = thresholdRangeTrigger(rule)
		}
	default:
		trigger, err = "", fmt.Errorf("Unknown trigger type: %s", rule.Trigger)
	}

	if err != nil {
		return "", err
	}

	// Only add stateChangesOnly to new rules
	if rule.ID == "" {
		trigger += `
				.stateChangesOnly()
		`
	}

	trigger += AllAlerts

	if rule.Details != "" {
		trigger += Details
	}
	return trigger, nil
}

func relativeTrigger(rule chronograf.AlertRule) (string, error) {
	op, err := kapaOperator(rule.TriggerValues.Operator)
	if err != nil {
		return "", err
	}
	if rule.TriggerValues.Change == ChangePercent {
		return fmt.Sprintf(RelativePercentTrigger, op), nil
	} else if rule.TriggerValues.Change == ChangeAmount {
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

func thresholdRangeTrigger(rule chronograf.AlertRule) (string, error) {
	ops, err := rangeOperators(rule.TriggerValues.Operator)
	if err != nil {
		return "", err
	}
	var iops = make([]interface{}, len(ops))
	for i, o := range ops {
		iops[i] = o
	}
	return fmt.Sprintf(ThresholdRangeTrigger, iops...), nil
}
