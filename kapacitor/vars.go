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
	//LevelField is the output field key for the alert level information
	LevelField = "level"
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
        var crit = %s
 `
		return fmt.Sprintf(vars,
			common,
			rule.Critical,
		), nil
	case "relative":
		vars := `
		%s
        var shift = -%s
        var crit = %s
 `
		return fmt.Sprintf(vars,
			common,
			rule.Shift,
			rule.Critical,
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
	fld, err := field(rule.Query)
	if err != nil {
		return "", err
	}

	common := `
        var db = '%s'
        var rp = '%s'
        var measurement = '%s'
        var field = '%s'
        var groupby = %s
        var where_filter = %s

		var period = %s
        var every = %s

		var name = '%s'
		var idVar = name + ':{{.Group}}'
		var message = '%s'
		var idtag = '%s'
		var levelfield = '%s'
		var messagefield = '%s'
		var durationfield = '%s'

        var metric = '%s'

        var output_db = '%s'
        var output_rp = '%s'
        var output_mt = '%s'
        var triggerType = '%s'
    `
	return fmt.Sprintf(common,
		rule.Query.Database,
		rule.Query.RetentionPolicy,
		rule.Query.Measurement,
		fld,
		groupBy(rule.Query),
		whereFilter(rule.Query),
		rule.Period,
		rule.Every,
		rule.Name,
		rule.Message,
		IDTag,
		LevelField,
		MessageField,
		DurationField,
		metric(rule),
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

// metric will be metric unless there are no field aggregates. If no aggregates, then it is the field name.
func metric(rule chronograf.AlertRule) string {
	for _, field := range rule.Query.Fields {
		// Deadman triggers do not need any aggregate functions
		if field.Field != "" && rule.Trigger == "deadman" {
			return field.Field
		} else if field.Field != "" && len(field.Funcs) == 0 {
			return field.Field
		}
	}
	return "metric"
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
