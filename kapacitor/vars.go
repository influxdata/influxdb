package kapacitor

import (
	"fmt"
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
)

// Vars builds the top level vars for a kapacitor alert script
func Vars(rule chronograf.AlertRule) (string, error) {
	fld, err := field(rule.Query)
	if err != nil {
		return "", err
	}
	switch rule.Trigger {
	case "threshold":
		vars := `
        var db = '%s'
        var rp = '%s'
        var measurement = '%s'
        var field = '%s'
        var groupby = %s
        var where_filter = %s

		var id = 'kapacitor/{{ .Name }}/{{ .Group }}'
		var message = '%s'
        var period = %s
        var every = %s
        var metric = '%s'
        var crit = %s
        var output_db = '%s'
        var output_rp = '%s'
        var output_mt = '%s'
    `
		return fmt.Sprintf(vars,
			rule.Query.Database,
			rule.Query.RetentionPolicy,
			rule.Query.Measurement,
			fld,
			groupBy(rule.Query),
			whereFilter(rule.Query),
			rule.Message,
			rule.Period,
			rule.Every,
			metric(rule.Query),
			rule.Critical,
			Database,
			RP,
			Measurement,
		), nil
	case "relative":
		vars := `
        var db = '%s'
        var rp = '%s'
        var measurement = '%s'
        var field = '%s'
        var groupby = %s
        var where_filter = %s

		var id = 'kapacitor/{{ .Name }}/{{ .Group }}'
		var message = '%s'
        var period = %s
        var every = %s
        var metric = '%s'
        var shift = -%s
        var crit = %s
        var output_db = '%s'
        var output_rp = '%s'
        var output_mt = '%s'
    `
		return fmt.Sprintf(vars,
			rule.Query.Database,
			rule.Query.RetentionPolicy,
			rule.Query.Measurement,
			fld,
			groupBy(rule.Query),
			whereFilter(rule.Query),
			rule.Message,
			rule.Period,
			rule.Every,
			metric(rule.Query),
			rule.Shift,
			rule.Critical,
			Database,
			RP,
			Measurement,
		), nil
	case "deadman":
		vars := `
        var db = '%s'
        var rp = '%s'
        var measurement = '%s'
        var field = '%s'
        var groupby = %s
        var where_filter = %s

		var id = 'kapacitor/{{ .Name }}/{{ .Group }}'
		var message = '%s'
        var period = %s
        var every = %s
		var metric = '%s'
        var threshold = %s
        var output_db = '%s'
        var output_rp = '%s'
        var output_mt = '%s'
    `
		return fmt.Sprintf(vars,
			rule.Query.Database,
			rule.Query.RetentionPolicy,
			rule.Query.Measurement,
			fld,
			groupBy(rule.Query),
			whereFilter(rule.Query),
			rule.Message,
			rule.Period,
			rule.Every,
			metric(rule.Query),
			"0.0", // deadman threshold hardcoded to zero
			Database,
			RP,
			Measurement,
		), nil
	default:
		return "", fmt.Errorf("Unknown trigger mechanism")
	}
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
func metric(q chronograf.QueryConfig) string {
	for _, field := range q.Fields {
		if field.Field != "" && len(field.Funcs) == 0 {
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
		return "lambda: " + strings.Join(outer, " AND ")
	}

	return "lambda: TRUE"
}
