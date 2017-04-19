package influx

import "strings"

// TemplateValue is a value use to replace a template in an InfluxQL query
type TemplateValue struct {
	Value string `json:"value"`
	Type  string `json:"type"`
}

// TemplateVar is a named variable within an InfluxQL query to be replaced with Values
type TemplateVar struct {
	Var    string          `json:"tempVar"`
	Values []TemplateValue `json:"values"`
}

// String converts the template variable into a correct InfluxQL string based
// on its type
func (t TemplateVar) String() string {
	if len(t.Values) == 0 {
		return ""
	}
	switch t.Values[0].Type {
	case "tagKey", "fieldKey":
		return `"` + t.Values[0].Value + `"`
	case "tagValue":
		return `'` + t.Values[0].Value + `'`
	case "csv":
		return t.Values[0].Value
	default:
		return ""
	}
}

// TemplateVars are template variables to replace within an InfluxQL query
type TemplateVars struct {
	Vars []TemplateVar `json:"tempVars"`
}

// TemplateReplace replaces templates with values within the query string
func TemplateReplace(query string, templates TemplateVars) string {
	replacements := []string{}
	for _, v := range templates.Vars {
		newVal := v.String()
		if newVal != "" {
			replacements = append(replacements, v.Var, newVal)
		}
	}

	replacer := strings.NewReplacer(replacements...)
	replaced := replacer.Replace(query)
	return replaced
}
