package kapacitor

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/influxdata/chronograf"
)

var (
	// Database is the output database for alerts.
	Database = "chronograf"
	// RP will be autogen for alerts because it is default.
	RP = "autogen"
	// Measurement will be alerts so that the app knows where to get this data.
	Measurement = "alerts"
	// IDTag is the output tag key for the ID of the alert
	IDTag = "alertID"
	//LevelTag is the output tag key for the alert level information
	LevelTag = "level"
	// MessageField is the output field key for the message in the alert
	MessageField = "message"
	// DurationField is the output field key for the duration of the alert
	DurationField = "duration"
)

// Vars builds the top level vars for a kapacitor alert script
func Vars(rule chronograf.AlertRule) (string, error) {
	common, err := commonVars(rule)
	if err != nil {
		return "", err
	}

	switch rule.Trigger {
	case Threshold:
		if rule.TriggerValues.RangeValue == "" {
			vars := `
		%s
        var crit = %s
 `
			return fmt.Sprintf(vars, common, formatValue(rule.TriggerValues.Value)), nil
		}
		vars := `
			%s
					var lower = %s
					var upper = %s
`
		return fmt.Sprintf(vars,
			common,
			rule.TriggerValues.Value,
			rule.TriggerValues.RangeValue), nil
	case Relative:
		vars := `
		%s
        var shift = %s
        var crit = %s
 `
		return fmt.Sprintf(vars,
			common,
			rule.TriggerValues.Shift,
			rule.TriggerValues.Value,
		), nil
	case Deadman:
		vars := `
		%s
        var threshold = %s
 `
		return fmt.Sprintf(vars,
			common,
			"0.0", // deadman threshold hardcoded to zero
		), nil
	default:
		return "", fmt.Errorf("Unknown trigger mechanism")
	}
}

func commonVars(rule chronograf.AlertRule) (string, error) {
	common := `
        var db = '%s'
        var rp = '%s'
        var measurement = '%s'
        var groupBy = %s
        var whereFilter = %s
		%s

		var name = '%s'
		var idVar = name + ':{{.Group}}'
		var message = '%s'
		var idTag = '%s'
		var levelTag = '%s'
		var messageField = '%s'
		var durationField = '%s'

        var outputDB = '%s'
        var outputRP = '%s'
        var outputMeasurement = '%s'
        var triggerType = '%s'
    `
	res := fmt.Sprintf(common,
		rule.Query.Database,
		rule.Query.RetentionPolicy,
		rule.Query.Measurement,
		groupBy(rule.Query),
		whereFilter(rule.Query),
		window(rule),
		rule.Name,
		rule.Message,
		IDTag,
		LevelTag,
		MessageField,
		DurationField,
		Database,
		RP,
		Measurement,
		rule.Trigger,
	)

	if rule.Details != "" {
		res += fmt.Sprintf(`
        var details = '%s'
    `, rule.Details)
	}
	return res, nil
}

// window is only used if deadman or threshold/relative with aggregate.  Will return empty
// if no period.
func window(rule chronograf.AlertRule) string {
	if rule.Trigger == Deadman {
		return fmt.Sprintf("var period = %s", rule.TriggerValues.Period)
	}
	// Period only makes sense if the field has a been grouped via a time duration.
	for _, field := range rule.Query.Fields {
		if field.Type == "func" {
			return fmt.Sprintf("var period = %s\nvar every = %s", rule.Query.GroupBy.Time, rule.Every)
		}
	}
	return ""
}

func groupBy(q *chronograf.QueryConfig) string {
	groups := []string{}
	if q != nil {
		for _, tag := range q.GroupBy.Tags {
			groups = append(groups, fmt.Sprintf("'%s'", tag))
		}
	}
	return "[" + strings.Join(groups, ",") + "]"
}

func field(q *chronograf.QueryConfig) (string, error) {
	if q != nil {
		for _, field := range q.Fields {
			if field.Type == "func" && len(field.Args) > 0 {
				return field.Args[0], nil
			}
			return field.Name, nil
		}
	}
	return "", fmt.Errorf("No fields set in query")
}

func whereFilter(q *chronograf.QueryConfig) string {
	if q != nil {
		operator := "=="
		if !q.AreTagsAccepted {
			operator = "!="
		}

		outer := []string{}
		for tag, values := range q.Tags {
			inner := []string{}
			for _, value := range values {
				inner = append(inner, fmt.Sprintf(`"%s" %s '%s'`, tag, operator, value))
			}
			outer = append(outer, "("+strings.Join(inner, " OR ")+")")
		}
		if len(outer) > 0 {
			sort.Strings(outer)
			return "lambda: " + strings.Join(outer, " AND ")
		}
	}
	return "lambda: TRUE"
}

// formatValue return the same string if a numeric type or if it is a string
// will return it as a kapacitor formatted single-quoted string
func formatValue(value string) string {
	// Test if numeric if it can be converted to a float
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return value
	}
	return "'" + value + "'"
}
