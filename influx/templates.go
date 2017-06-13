package influx

import (
	"strings"

	"github.com/influxdata/chronograf"
)

// TemplateReplace replaces templates with values within the query string
func TemplateReplace(query string, templates chronograf.TemplateVars) string {
	tvarsByPrecedence := make(map[uint]chronograf.TemplateVars, len(templates))
	maxPrecedence := uint(0)
	for _, tmp := range templates {
		precedence := tmp.Precedence()
		if precedence > maxPrecedence {
			maxPrecedence = precedence
		}
		tvarsByPrecedence[precedence] = append(tvarsByPrecedence[precedence], tmp)
	}

	replaced := query
	for prc := uint(0); prc <= maxPrecedence; prc++ {
		replacements := []string{}

		for _, v := range tvarsByPrecedence[prc] {
			if evar, ok := v.(chronograf.ExecutableVar); ok {
				evar.Exec(replaced)
			}
			newVal := v.String()
			if newVal != "" {
				replacements = append(replacements, v.Name(), newVal)
			}
		}

		replacer := strings.NewReplacer(replacements...)
		replaced = replacer.Replace(replaced)
	}

	return replaced
}
