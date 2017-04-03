package kapacitor

import (
	"fmt"
	"regexp"
	"strings"
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

// FieldFunc represents the field used as the alert value and its optional aggregate function
type FieldFunc struct {
	Field string
	Func  string
}

func extractFieldFunc(script chronograf.TICKScript) FieldFunc {
	// If the TICKScript is relative or threshold alert with an aggregate
	// then the aggregate function and field is in the form |func('field').as('value')
	var re = regexp.MustCompile(`(?Um)\|(\w+)\('(.*)'\)\.as\('value'\)`)
	for _, match := range re.FindAllStringSubmatch(string(script), -1) {
		fn, field := match[1], match[2]
		return FieldFunc{
			Field: field,
			Func:  fn,
		}
	}

	// If the alert does not have an aggregate then the the value function will
	// be this form: |eval(lambda: "%s").as('value')
	re = regexp.MustCompile(`(?Um)\|eval\(lambda: "(.*)"\)\.as\('value'\)`)
	for _, match := range re.FindAllStringSubmatch(string(script), -1) {
		field := match[1]
		return FieldFunc{
			Field: field,
		}
	}
	// Otherwise, if this could be a deadman alert and not have a FieldFunc
	return FieldFunc{}
}

type CritCondition struct {
	Operators []string
}

func extractCrit(script chronograf.TICKScript) CritCondition {
	// Threshold and relative alerts have the form .crit(lambda: "value" op crit)
	// Threshold range alerts have the form .crit(lambda: "value" op lower op "value" op upper)
	var re = regexp.MustCompile(`(?Um)\.crit\(lambda: "value" (.*) crit\)`)
	for _, match := range re.FindAllStringSubmatch(string(script), -1) {
		op := match[1]
		return CritCondition{
			Operators: []string{
				op,
			},
		}
	}
	re = regexp.MustCompile(`(?Um)\.crit\(lambda: "value" (.*) lower (.*) "value" (.*) upper\)`)
	for _, match := range re.FindAllStringSubmatch(string(script), -1) {
		lower, compound, upper := match[1], match[2], match[3]
		return CritCondition{
			Operators: []string{
				lower,
				compound,
				upper,
			},
		}
	}

	// It's possible to not have a critical condition if this is
	// a deadman alert
	return CritCondition{}
}

// alertType reads the TICKscript and returns the specific
// alerting type. If it is unable to determine it will
// return ErrNotChronoTickscript
func alertType(script chronograf.TICKScript) (string, error) {
	t := string(script)
	if strings.Contains(t, `var triggerType = 'threshold'`) {
		if strings.Contains(t, `var crit = `) {
			return Threshold, nil
		} else if strings.Contains(t, `var lower = `) && strings.Contains(t, `var upper = `) {
			return ThresholdRange, nil
		}
		return "", ErrNotChronoTickscript
	} else if strings.Contains(t, `var triggerType = 'relative'`) {
		if strings.Contains(t, `eval(lambda: float("current.value" - "past.value"))`) {
			return ChangeAmount, nil
		} else if strings.Contains(t, `|eval(lambda: abs(float("current.value" - "past.value"))/float("past.value") * 100.0)`) {
			return ChangePercent, nil
		}
		return "", ErrNotChronoTickscript
	} else if strings.Contains(t, `var triggerType = 'deadman'`) {
		return Deadman, nil
	}
	return "", ErrNotChronoTickscript
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
			if t.HipChatHandlers != nil {

			}
			if t.OpsGenieHandlers != nil {
				rule.Alerts = append(rule.Alerts, "hipchat")
			}
			if t.PagerDutyHandlers != nil {
				rule.Alerts = append(rule.Alerts, "pagerduty")
			}
			if t.VictorOpsHandlers != nil {
				rule.Alerts = append(rule.Alerts, "victorops")
			}
			if t.EmailHandlers != nil {
				rule.Alerts = append(rule.Alerts, "smtp")
			}
			if t.PostHandlers != nil {
				rule.Alerts = append(rule.Alerts, "http")
			}
			if t.AlertaHandlers != nil {
				rule.Alerts = append(rule.Alerts, "alerta")
			}
			if t.SensuHandlers != nil {
				rule.Alerts = append(rule.Alerts, "sensu")
			}
			if t.SlackHandlers != nil {
				rule.Alerts = append(rule.Alerts, "slack")
			}
			if t.TalkHandlers != nil {
				rule.Alerts = append(rule.Alerts, "talk")
			}
			if t.TelegramHandlers != nil {
				rule.Alerts = append(rule.Alerts, "telegram")
			}
			if t.TcpHandlers != nil {
				rule.Alerts = append(rule.Alerts, "tcp")
			}
			if t.ExecHandlers != nil {
				rule.Alerts = append(rule.Alerts, "exec")
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

func extractHipchat(node *pipeline.AlertNode, rule *chronograf.AlertRule) {
	if node.HipChatHandlers == nil {
		return
	}
	rule.Alerts = append(rule.Alerts, "hipchat")
	h := node.HipChatHandlers[0]
	alert := chronograf.KapacitorNode{
		Name:       "hipchat",
		Properties: []chronograf.KapacitorProperty{},
	}

	if h.Room != "" {
		alert.Properties = append(alert.Properties, chronograf.KapacitorProperty{
			Name: "room",
			Args: []string{h.Room},
		})
	}

	if h.Token != "" {
		alert.Properties = append(alert.Properties, chronograf.KapacitorProperty{
			Name: "token",
			Args: []string{h.Token},
		})
	}
	rule.AlertNodes = append(rule.AlertNodes, alert)
}

func extractOpsgenie(node *pipeline.AlertNode, rule *chronograf.AlertRule) {
	if node.OpsGenieHandlers == nil {
		return
	}
	rule.Alerts = append(rule.Alerts, "opsgenie")
	o := node.OpsGenieHandlers[0]
	alert := chronograf.KapacitorNode{
		Name:       "opsgenie",
		Properties: []chronograf.KapacitorProperty{},
	}

	if o.RecipientsList != nil {
		alert.Properties = append(alert.Properties, chronograf.KapacitorProperty{
			Name: "recipients",
			Args: o.RecipientsList,
		})
	}

	if o.TeamsList != nil {
		alert.Properties = append(alert.Properties, chronograf.KapacitorProperty{
			Name: "teams",
			Args: o.TeamsList,
		})
	}
	rule.AlertNodes = append(rule.AlertNodes, alert)
}

func extractPagerduty(node *pipeline.AlertNode, rule *chronograf.AlertRule) {
	if node.PagerDutyHandlers == nil {
		return
	}
	rule.Alerts = append(rule.Alerts, "pagerduty")
	p := node.PagerDutyHandlers[0]
	alert := chronograf.KapacitorNode{
		Name:       "paperduty",
		Properties: []chronograf.KapacitorProperty{},
	}

	if p.ServiceKey != "" {
		alert.Properties = append(alert.Properties, chronograf.KapacitorProperty{
			Name: "serviceKey",
			Args: []string{p.ServiceKey},
		})
	}
	rule.AlertNodes = append(rule.AlertNodes, alert)
}

func extractVictorops(node *pipeline.AlertNode, rule *chronograf.AlertRule) {
	if node.VictorOpsHandlers == nil {
		return
	}
	rule.Alerts = append(rule.Alerts, "victorops")
	v := node.VictorOpsHandlers[0]
	alert := chronograf.KapacitorNode{
		Name:       "victorops",
		Properties: []chronograf.KapacitorProperty{},
	}

	if v.RoutingKey != "" {
		alert.Properties = append(alert.Properties, chronograf.KapacitorProperty{
			Name: "routingKey",
			Args: []string{v.RoutingKey},
		})
	}
	rule.AlertNodes = append(rule.AlertNodes, alert)
}

func extractEmail(node *pipeline.AlertNode, rule *chronograf.AlertRule) {
	if node.EmailHandlers == nil {
		return
	}
	rule.Alerts = append(rule.Alerts, "smtp")
	e := node.EmailHandlers[0]
	alert := chronograf.KapacitorNode{
		Name:       "smtp",
		Properties: []chronograf.KapacitorProperty{},
	}

	if e.ToList != nil {
		alert.Args = e.ToList
	}
	rule.AlertNodes = append(rule.AlertNodes, alert)
}

func extractPost(node *pipeline.AlertNode, rule *chronograf.AlertRule) {
	if node.PostHandlers == nil {
		return
	}
	rule.Alerts = append(rule.Alerts, "http")
	p := node.PostHandlers[0]
	alert := chronograf.KapacitorNode{
		Name: "http",
		Args: []string{p.URL},
	}

	rule.AlertNodes = append(rule.AlertNodes, alert)
}

func extractAlerta(node *pipeline.AlertNode, rule *chronograf.AlertRule) {
	if node.AlertaHandlers == nil {
		return
	}
	rule.Alerts = append(rule.Alerts, "alerta")
	a := node.AlertaHandlers[0]
	alert := chronograf.KapacitorNode{
		Name:       "alerta",
		Properties: []chronograf.KapacitorProperty{},
	}

	if a.Token != "" {
		alert.Properties = append(alert.Properties, chronograf.KapacitorProperty{
			Name: "token",
			Args: []string{a.Token},
		})
	}

	if a.Resource != "" {
		alert.Properties = append(alert.Properties, chronograf.KapacitorProperty{
			Name: "resource",
			Args: []string{a.Resource},
		})
	}

	if a.Event != "" {
		alert.Properties = append(alert.Properties, chronograf.KapacitorProperty{
			Name: "event",
			Args: []string{a.Event},
		})
	}

	if a.Environment != "" {
		alert.Properties = append(alert.Properties, chronograf.KapacitorProperty{
			Name: "environment",
			Args: []string{a.Environment},
		})
	}

	if a.Group != "" {
		alert.Properties = append(alert.Properties, chronograf.KapacitorProperty{
			Name: "group",
			Args: []string{a.Group},
		})
	}

	if a.Value != "" {
		alert.Properties = append(alert.Properties, chronograf.KapacitorProperty{
			Name: "value",
			Args: []string{a.Value},
		})
	}

	if a.Origin != "" {
		alert.Properties = append(alert.Properties, chronograf.KapacitorProperty{
			Name: "origin",
			Args: []string{a.Origin},
		})
	}

	if a.Service != nil {
		alert.Properties = append(alert.Properties, chronograf.KapacitorProperty{
			Name: "services",
			Args: a.Service,
		})
	}

	rule.AlertNodes = append(rule.AlertNodes, alert)
}

func extractSensu(node *pipeline.AlertNode, rule *chronograf.AlertRule) {
	if node.SensuHandlers == nil {
		return
	}
	rule.Alerts = append(rule.Alerts, "sensu")
	alert := chronograf.KapacitorNode{
		Name: "sensu",
	}

	rule.AlertNodes = append(rule.AlertNodes, alert)
}

func extractSlack(node *pipeline.AlertNode, rule *chronograf.AlertRule) {
	if node.SlackHandlers == nil {
		return
	}
	rule.Alerts = append(rule.Alerts, "slack")
	s := node.SlackHandlers[0]
	alert := chronograf.KapacitorNode{
		Name:       "slack",
		Properties: []chronograf.KapacitorProperty{},
	}

	if a.Token != "" {
		alert.Properties = append(alert.Properties, chronograf.KapacitorProperty{
			Name: "token",
			Args: []string{a.Token},
		})
	}
	rule.AlertNodes = append(rule.AlertNodes, alert)
}
