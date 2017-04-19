package influx

import (
	"strings"

	"github.com/influxdata/chronograf"
)

// TemplateReplace replaces templates with values within the query string
func TemplateReplace(query string, templates []chronograf.TemplateVar) string {
	replacements := []string{}
	for _, v := range templates {
		newVal := v.String()
		if newVal != "" {
			replacements = append(replacements, v.Var, newVal)
		}
	}

	replacer := strings.NewReplacer(replacements...)
	replaced := replacer.Replace(query)
	return replaced
}
