package kapacitor

import (
	"fmt"
	"sort"
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
	case "threshold":
		vars := `
		%s
		var every = %s
        var crit = %s
 `
		return fmt.Sprintf(vars,
			common,
			rule.Every,
			rule.TriggerValues.Value), nil
	case "relative":
		vars := `
		%s
		var every = %s
        var shift = -%s
        var crit = %s
 `
		return fmt.Sprintf(vars,
			common,
			rule.Every,
			rule.TriggerValues.Shift,
			rule.TriggerValues.Value,
		), nil
	case "deadman":
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
		var period = %s

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
	return fmt.Sprintf(common,
		rule.Query.Database,
		rule.Query.RetentionPolicy,
		rule.Query.Measurement,
		groupBy(rule.Query),
		whereFilter(rule.Query),
		rule.TriggerValues.Period,
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
	), nil
}

func groupBy(q chronograf.QueryConfig) string {
	groups := []string{}
	for _, tag := range q.GroupBy.Tags {
		groups = append(groups, fmt.Sprintf("'%s'", tag))
	}
	return "[" + strings.Join(groups, ",") + "]"
}

func field(q chronograf.QueryConfig) (string, error) {
	for _, field := range q.Fields {
		return field.Field, nil
	}
	return "", fmt.Errorf("No fields set in query")
}

func whereFilter(q chronograf.QueryConfig) string {
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

	return "lambda: TRUE"
}
