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

func varString(kapaVar string, vars map[string]tick.Var) (string, bool) {
	var ok bool
	v, ok := vars[kapaVar]
	if !ok {
		return "", ok
	}
	strVar, ok := v.Value.(string)
	return strVar, ok
}

func varStringList(kapaVar string, vars map[string]tick.Var) ([]string, bool) {
	v, ok := vars[kapaVar]
	if !ok {
		return nil, ok
	}
	list, ok := v.Value.([]tick.Var)
	if !ok {
		return nil, ok
	}

	strs := make([]string, len(list))
	for i, l := range list {
		s, ok := l.Value.(string)
		if !ok {
			return nil, ok
		}
		strs[i] = s
	}
	return strs, ok
}

// WhereFilter filters the stream data in a TICKScript
type WhereFilter struct {
	TagValues map[string][]string // Tags are filtered by an array of values
	Operator  string              // Operator is == or !=
}

func varWhereFilter(vars map[string]tick.Var) (WhereFilter, bool) {
	// All chronograf TICKScripts have whereFilters.
	v, ok := vars["whereFilter"]
	if !ok {
		return WhereFilter{}, ok
	}
	filter := WhereFilter{}
	filter.TagValues = make(map[string][]string)

	// All chronograf TICKScript's whereFilter use a lambda function.
	value, ok := v.Value.(*ast.LambdaNode)
	if !ok {
		return WhereFilter{}, ok
	}

	lambda := value.ExpressionString()
	// Chronograf TICKScripts use lambda: TRUE as a pass-throug where clause
	// if the script does not have a where clause set.
	if lambda == "TRUE" {
		return WhereFilter{}, true
	}

	opSet := map[string]struct{}{} // All ops must be the same b/c queryConfig
	// Otherwise the lambda function will be several "tag" op 'value' expressions.
	var re = regexp.MustCompile(`(?U)"(.*)"\s+(==|!=)\s+'(.*)'`)
	for _, match := range re.FindAllStringSubmatch(lambda, -1) {
		tag, op, value := match[1], match[2], match[3]
		opSet[op] = struct{}{}
		values, ok := filter.TagValues[tag]
		if !ok {
			values = make([]string, 0)
		}
		values = append(values, value)
		filter.TagValues[tag] = values
	}

	// An obscure piece of the queryConfig is that the operator in ALL binary
	// expressions just be the same.  So, there must only be one operator
	// in our opSet
	if len(opSet) != 1 {
		return WhereFilter{}, false
	}
	for op := range opSet {
		filter.Operator = op
	}
	return filter, true
}

// CommonVars includes all the variables of a chronograf TICKScript
type CommonVars struct {
	Vars    map[string]string
	GroupBy []string
	Filter  WhereFilter
	Period  string
	Every   string
	Detail  string
}

type ThresholdVars struct {
	Crit string
}

type RangeVars struct {
	Lower string
	Upper string
}

type RelativeVars struct {
	Shift string
	Crit  string
}

type DeadmanVars struct{}

func extractCommonVars(script chronograf.TICKScript) (CommonVars, error) {
	scope := stateful.NewScope()
	template, err := pipeline.CreateTemplatePipeline(string(script), pipeline.StreamEdge, scope, &deadman{})
	if err != nil {
		return CommonVars{}, err
	}

	vars := template.Vars()
	res := CommonVars{}
	// All these variables must exist to be a chronograf TICKScript
	commonStrs := []string{
		"db",
		"rp",
		"measurement",
		"name",
		"message",
		"triggerType",
	}

	for _, v := range commonStrs {
		str, ok := varString(v, vars)
		// Didn't exist so, this isn't a tickscript we can process
		if !ok {
			return CommonVars{}, ErrNotChronoTickscript
		}
		res.Vars[v] = str
	}

	// All chronograf TICKScripts have groupBy. Possible to be empty list though.
	groups, ok := varStringList("groupBy", vars)
	if !ok {
		return CommonVars{}, ErrNotChronoTickscript
	}
	res.GroupBy = groups

	// All chronograf TICKScripts must have a whereFitler.  Could be empty.
	res.Filter, ok = varWhereFilter(vars)
	if !ok {
		return CommonVars{}, ErrNotChronoTickscript
	}

	// Some chronograf TICKScripts have details associated with the alert.
	// Typically, this is the body of an email alert.
	if detail, ok := varString("details", vars); ok {
		res.Detail = detail
	}

	// Relative and Threshold alerts may have an every variables
	if every, ok := varString("every", vars); ok {
		res.Every = every
	}

	// All alert types may have a period variables
	if period, ok := varString("period", vars); ok {
		res.Period = period
	}
	return res, nil
}

func extractAlertVars(vars map[string]tick.Var) (interface{}, error) {
	// Depending on the type of the alert the variables set will be different
	alertType, ok := varString("triggerType", vars)
	if !ok {
		return nil, ErrNotChronoTickscript
	}

	switch alertType {
	case Deadman:
		return &DeadmanVars{}, nil
	case Threshold:
		if crit, ok := varString("crit", vars); ok {
			return &ThresholdVars{
				Crit: crit,
			}, nil
		}
		r := &RangeVars{}
		// Threshold Range alerts must have both an upper and lower bound
		if r.Lower, ok = varString("lower", vars); !ok {
			return nil, ErrNotChronoTickscript
		}
		if r.Upper, ok = varString("upper", vars); !ok {
			return nil, ErrNotChronoTickscript
		}
		return r, nil
	case Relative:
		// Relative alerts must have a time shift and critical value
		r := &RelativeVars{}
		if r.Shift, ok = varString("shift", vars); !ok {
			return nil, ErrNotChronoTickscript
		}
		if r.Crit, ok = varString("crit", vars); !ok {
			return nil, ErrNotChronoTickscript
		}
		return r, nil
	default:
		return nil, ErrNotChronoTickscript
	}
}

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
				values = make([]string, 0)
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
