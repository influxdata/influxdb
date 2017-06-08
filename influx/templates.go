package influx

import (
	"strings"

	"github.com/influxdata/chronograf"
)

// TemplateReplace replaces templates with values within the query string
func TemplateReplace(query string, templates chronograf.TemplateVars) string {
	replacements := []string{}

	for _, v := range templates {
		if evar, ok := v.(chronograf.ExecutableVar); ok {
			evar.Exec(query)
		}
		newVal := v.String()
		if newVal != "" {
			replacements = append(replacements, v.Name(), newVal)
		}
	}

	replacer := strings.NewReplacer(replacements...)
	replaced := replacer.Replace(query)
	return replaced
}
