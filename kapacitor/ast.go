package kapacitor

import (
	"fmt"
	"regexp"
	"time"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick"
	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/influxdata/kapacitor/tick/stateful"
)

// Reverse converts tickscript to an AlertRule
func Reverse(script chronograf.TICKScript) (chronograf.AlertRule, error) {
	rule := chronograf.AlertRule{
		Alerts: []string{},
	}
	scope := stateful.NewScope()
	template, err := pipeline.CreateTemplatePipeline(string(script), pipeline.StreamEdge, scope, &deadman{})
	if err != nil {
		return chronograf.AlertRule{}, err
	}
	vars := template.Vars()
	if err := valueStr("db", &rule.Query.Database, vars); err != nil {
		return chronograf.AlertRule{}, err
	}
	rule.Query.RetentionPolicy = vars["rp"].Value.(string)
	rule.Query.Measurement = vars["measurement"].Value.(string)
	rule.Name = vars["name"].Value.(string)
	rule.Trigger = vars["triggerType"].Value.(string)
	rule.Every = vars["every"].Value.(time.Duration).String()
	// Convert to just minutes or hours
	rule.Query.GroupBy.Time = vars["period"].Value.(time.Duration).String()
	rule.Message = vars["message"].Value.(string)
	rule.Details = vars["details"].Value.(string)
	if v, ok := vars["lower"]; ok {
		rule.TriggerValues.Value = fmt.Sprintf("%v", v.Value)
	}
	if v, ok := vars["upper"]; ok {
		rule.TriggerValues.RangeValue = fmt.Sprintf("%v", v.Value)
	}
	if v, ok := vars["crit"]; ok {
		rule.TriggerValues.Value = fmt.Sprintf("%v", v.Value)
	}
	if v, ok := vars["groupBy"]; ok {
		groups := v.Value.([]tick.Var)
		rule.Query.GroupBy.Tags = make([]string, len(groups))
		for i, g := range groups {
			rule.Query.GroupBy.Tags[i] = g.Value.(string)
		}
	}
	if v, ok := vars["whereFilter"]; ok {
		rule.Query.Tags = make(map[string][]string)
		value := v.Value.(*ast.LambdaNode)
		var re = regexp.MustCompile(`(?U)"(.*)"\s+(==|!=)\s+'(.*)'`)
		for _, match := range re.FindAllStringSubmatch(value.ExpressionString(), -1) {
			if match[2] == "==" {
				rule.Query.AreTagsAccepted = true
			}
			tag, value := match[1], match[3]
			values, ok := rule.Query.Tags[tag]
			if !ok {
				values = []string{}
			}
			values = append(values, value)
			rule.Query.Tags[tag] = values
		}
	}

	// Only if non-deadman
	var re = regexp.MustCompile(`(?Um)\|(\w+)\('(.*)'\)\s+\.as\(\'.*\'\)`)
	for _, match := range re.FindAllStringSubmatch(string(script), -1) {
		fn, field := match[1], match[2]
		rule.Query.Fields = []chronograf.Field{
			chronograf.Field{
				Field: field,
				Funcs: []string{
					fn,
				},
			},
		}
	}

	p, err := pipeline.CreatePipeline(string(script), pipeline.StreamEdge, stateful.NewScope(), &deadman{}, vars)
	if err != nil {
		return chronograf.AlertRule{}, err
	}

	p.Walk(func(n pipeline.Node) error {
		switch t := n.(type) {
		case *pipeline.AlertNode:
			bin, ok := t.Crit.Expression.(*ast.BinaryNode)
			if ok {
				oper := bin.Operator
				if oper == ast.TokenAnd || oper == ast.TokenOr {
					lhs, lok := bin.Left.(*ast.BinaryNode)
					rhs, rok := bin.Right.(*ast.BinaryNode)
					if rok && lok {
						op, err := chronoRangeOperators([]string{
							lhs.String(),
							oper.String(),
							rhs.String(),
						})
						if err != nil {
							return err
						}
						rule.TriggerValues.Operator = op
					}
				} else {
					op, err := chronoOperator(bin.Operator.String())
					if err != nil {
						return err
					}
					rule.TriggerValues.Operator = op
				}
			}
			if t.VictorOps() != nil {
				rule.Alerts = append(rule.Alerts, "victorops")
			}
			if t.Slack() != nil {
				rule.Alerts = append(rule.Alerts, "slack")
			}
			if t.Email() != nil {
				rule.Alerts = append(rule.Alerts, "email")
			}
		}
		return nil
	})
	return rule, err
}

func valueStr(key string, value *string, vars map[string]tick.Var) error {
	v, ok := vars[key]
	if !ok {
		return fmt.Errorf("No %s", key)
	}
	val, ok := v.Value.(string)
	if !ok {
		return fmt.Errorf("No %s", key)
	}
	value = &val
	return nil
}
