package influx

import "strings"

// TempValue is a value use to replace a template in an InfluxQL query
type TempValue struct {
	Value string `json:"value"`
	Type  string `json:"type"`
}

// TempVar is a named variable within an InfluxQL query to be replaced with Values
type TempVar struct {
	Var    string      `json:"tempVar"`
	Values []TempValue `json:"values"`
}

// String converts the template variable into a correct InfluxQL string based
// on its type
func (t TempVar) String() string {
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

// TempVars are template variables to replace within an InfluxQL query
type TempVars struct {
	Vars []TempVar `json:"tempVars"`
}

// TemplateReplace replaces templates with values within the query string
func TemplateReplace(query string, templates TempVars) string {
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
