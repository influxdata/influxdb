package kapacitor

import (
	"regexp"
	"strconv"
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

func varValue(kapaVar string, vars map[string]tick.Var) (string, bool) {
	var ok bool
	v, ok := vars[kapaVar]
	if !ok {
		return "", ok
	}
	switch val := v.Value.(type) {
	case string:
		return val, true
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 32), true
	case int64:
		return strconv.FormatInt(val, 10), true
	case bool:
		return strconv.FormatBool(val), true
	case time.Time:
		return val.String(), true
	case *regexp.Regexp:
		return val.String(), true
	default:
		return "", false
	}
}

func varDuration(kapaVar string, vars map[string]tick.Var) (string, bool) {
	var ok bool
	v, ok := vars[kapaVar]
	if !ok {
		return "", ok
	}
	durVar, ok := v.Value.(time.Duration)
	if !ok {
		return "", ok
	}
	return durVar.String(), true
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
		if op != "==" && op != "!=" {
			return WhereFilter{}, false
		}
		filter.Operator = op
	}
	return filter, true
}

// CommonVars includes all the variables of a chronograf TICKScript
type CommonVars struct {
	DB          string
	RP          string
	Measurement string
	Name        string
	Message     string
	TriggerType string
	GroupBy     []string
	Filter      WhereFilter
	Period      string
	Every       string
	Detail      string
}

// ThresholdVars represents the critical value where an alert occurs
type ThresholdVars struct {
	Crit string
}

// RangeVars represents the critical range where an alert occurs
type RangeVars struct {
	Lower string
	Upper string
}

// RelativeVars represents the critical range and time in the past an alert occurs
type RelativeVars struct {
	Shift string
	Crit  string
}

// DeadmanVars represents a deadman alert
type DeadmanVars struct{}

func extractCommonVars(vars map[string]tick.Var) (CommonVars, error) {
	res := CommonVars{}
	// All these variables must exist to be a chronograf TICKScript
	// If any of these don't exist, then this isn't a tickscript we can process
	var ok bool
	res.DB, ok = varString("db", vars)
	if !ok {
		return CommonVars{}, ErrNotChronoTickscript
	}
	res.RP, ok = varString("rp", vars)
	if !ok {
		return CommonVars{}, ErrNotChronoTickscript
	}
	res.Measurement, ok = varString("measurement", vars)
	if !ok {
		return CommonVars{}, ErrNotChronoTickscript
	}
	res.Name, ok = varString("name", vars)
	if !ok {
		return CommonVars{}, ErrNotChronoTickscript
	}
	res.Message, ok = varString("message", vars)
	if !ok {
		return CommonVars{}, ErrNotChronoTickscript
	}
	res.TriggerType, ok = varString("triggerType", vars)
	if !ok {
		return CommonVars{}, ErrNotChronoTickscript
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
	if every, ok := varDuration("every", vars); ok {
		res.Every = every
	}

	// All alert types may have a period variables
	if period, ok := varDuration("period", vars); ok {
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
		if crit, ok := varValue("crit", vars); ok {
			return &ThresholdVars{
				Crit: crit,
			}, nil
		}
		r := &RangeVars{}
		// Threshold Range alerts must have both an upper and lower bound
		if r.Lower, ok = varValue("lower", vars); !ok {
			return nil, ErrNotChronoTickscript
		}
		if r.Upper, ok = varValue("upper", vars); !ok {
			return nil, ErrNotChronoTickscript
		}
		return r, nil
	case Relative:
		// Relative alerts must have a time shift and critical value
		r := &RelativeVars{}
		if r.Shift, ok = varDuration("shift", vars); !ok {
			return nil, ErrNotChronoTickscript
		}
		if r.Crit, ok = varValue("crit", vars); !ok {
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
	var re = regexp.MustCompile(`(?Um)\|(\w+)\('(.*)'\)\s*\.as\('value'\)`)
	for _, match := range re.FindAllStringSubmatch(string(script), -1) {
		fn, field := match[1], match[2]
		return FieldFunc{
			Field: field,
			Func:  fn,
		}
	}

	// If the alert does not have an aggregate then the the value function will
	// be this form: |eval(lambda: "%s").as('value')
	re = regexp.MustCompile(`(?Um)\|eval\(lambda: "(.*)"\)\s*\.as\('value'\)`)
	for _, match := range re.FindAllStringSubmatch(string(script), -1) {
		field := match[1]
		return FieldFunc{
			Field: field,
		}
	}
	// Otherwise, if this could be a deadman alert and not have a FieldFunc
	return FieldFunc{}
}

// CritCondition represents the operators that determine when the alert should go critical
type CritCondition struct {
	Operators []string
}

func extractCrit(script chronograf.TICKScript) CritCondition {
	// Threshold and relative alerts have the form .crit(lambda: "value" op crit)
	// Threshold range alerts have the form .crit(lambda: "value" op lower op "value" op upper)
	var re = regexp.MustCompile(`(?Um)\.crit\(lambda:\s+"value"\s+(.*)\s+crit\)`)
	for _, match := range re.FindAllStringSubmatch(string(script), -1) {
		op := match[1]
		return CritCondition{
			Operators: []string{
				op,
			},
		}
	}
	re = regexp.MustCompile(`(?Um)\.crit\(lambda:\s+"value"\s+(.*)\s+lower\s+(.*)\s+"value"\s+(.*)\s+upper\)`)
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
		} else if strings.Contains(t, `|eval(lambda: abs(float("current.value" - "past.value")) / float("past.value") * 100.0)`) {
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
		Query:  &chronograf.QueryConfig{},
	}
	t, err := alertType(script)
	if err != nil {
		return rule, err
	}

	scope := stateful.NewScope()
	template, err := pipeline.CreateTemplatePipeline(string(script), pipeline.StreamEdge, scope, &deadman{})
	if err != nil {
		return chronograf.AlertRule{}, err
	}
	vars := template.Vars()

	commonVars, err := extractCommonVars(vars)
	if err != nil {
		return rule, err
	}
	alertVars, err := extractAlertVars(vars)
	if err != nil {
		return rule, err
	}
	fieldFunc := extractFieldFunc(script)
	critCond := extractCrit(script)

	switch t {
	case Threshold, ChangeAmount, ChangePercent:
		if len(critCond.Operators) != 1 {
			return rule, ErrNotChronoTickscript
		}
	case ThresholdRange:
		if len(critCond.Operators) != 3 {
			return rule, ErrNotChronoTickscript
		}
	}

	rule.Name = commonVars.Name
	rule.Trigger = commonVars.TriggerType
	rule.Message = commonVars.Message
	rule.Details = commonVars.Detail
	rule.Query.Database = commonVars.DB
	rule.Query.RetentionPolicy = commonVars.RP
	rule.Query.Measurement = commonVars.Measurement
	rule.Query.GroupBy.Tags = commonVars.GroupBy

	if commonVars.Filter.Operator == "==" {
		rule.Query.AreTagsAccepted = true
	}
	rule.Query.Tags = commonVars.Filter.TagValues

	if t == Deadman {
		rule.TriggerValues.Period = commonVars.Period
	} else {
		rule.Query.GroupBy.Time = commonVars.Period
		rule.Every = commonVars.Every
		funcs := []string{}
		if fieldFunc.Func != "" {
			funcs = append(funcs, fieldFunc.Func)
		}
		rule.Query.Fields = []chronograf.Field{
			{
				Field: fieldFunc.Field,
				Funcs: funcs,
			},
		}
	}

	switch t {
	case ChangeAmount, ChangePercent:
		rule.TriggerValues.Change = t
		rule.TriggerValues.Operator, err = chronoOperator(critCond.Operators[0])
		if err != nil {
			return rule, ErrNotChronoTickscript
		}
		v, ok := alertVars.(*RelativeVars)
		if !ok {
			return rule, ErrNotChronoTickscript
		}
		rule.TriggerValues.Value = v.Crit
		rule.TriggerValues.Shift = v.Shift
	case Threshold:
		rule.TriggerValues.Operator, err = chronoOperator(critCond.Operators[0])
		if err != nil {
			return rule, ErrNotChronoTickscript
		}
		v, ok := alertVars.(*ThresholdVars)
		if !ok {
			return rule, ErrNotChronoTickscript
		}
		rule.TriggerValues.Value = v.Crit
	case ThresholdRange:
		rule.TriggerValues.Operator, err = chronoRangeOperators(critCond.Operators)
		v, ok := alertVars.(*RangeVars)
		if !ok {
			return rule, ErrNotChronoTickscript
		}
		rule.TriggerValues.Value = v.Lower
		rule.TriggerValues.RangeValue = v.Upper
	}

	p, err := pipeline.CreatePipeline(string(script), pipeline.StreamEdge, stateful.NewScope(), &deadman{}, vars)
	if err != nil {
		return chronograf.AlertRule{}, err
	}

	extractAlertNodes(p, &rule)
	return rule, err
}

func extractAlertNodes(p *pipeline.Pipeline, rule *chronograf.AlertRule) {
	p.Walk(func(n pipeline.Node) error {
		switch t := n.(type) {
		case *pipeline.AlertNode:
			extractHipchat(t, rule)
			extractOpsgenie(t, rule)
			extractPagerduty(t, rule)
			extractVictorops(t, rule)
			extractEmail(t, rule)
			extractPost(t, rule)
			extractAlerta(t, rule)
			extractSensu(t, rule)
			extractSlack(t, rule)
			extractTalk(t, rule)
			extractTelegram(t, rule)
			extractTCP(t, rule)
			extractLog(t, rule)
			extractExec(t, rule)
		}
		return nil
	})
}

func extractHipchat(node *pipeline.AlertNode, rule *chronograf.AlertRule) {
	if node.HipChatHandlers == nil {
		return
	}
	rule.Alerts = append(rule.Alerts, "hipchat")
	h := node.HipChatHandlers[0]
	alert := chronograf.KapacitorNode{
		Name: "hipchat",
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
		Name: "opsgenie",
	}

	if len(o.RecipientsList) != 0 {
		alert.Properties = append(alert.Properties, chronograf.KapacitorProperty{
			Name: "recipients",
			Args: o.RecipientsList,
		})
	}

	if len(o.TeamsList) != 0 {
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
		Name: "pagerduty",
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
		Name: "victorops",
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
		Name: "smtp",
	}

	if len(e.ToList) != 0 {
		alert.Args = e.ToList
	}
	rule.AlertNodes = append(rule.AlertNodes, alert)
}

func extractPost(node *pipeline.AlertNode, rule *chronograf.AlertRule) {
	if node.HTTPPostHandlers == nil {
		return
	}
	rule.Alerts = append(rule.Alerts, "http")
	p := node.HTTPPostHandlers[0]
	alert := chronograf.KapacitorNode{
		Name: "http",
	}

	if p.URL != "" {
		alert.Args = []string{p.URL}
	}

	if p.Endpoint != "" {
		alert.Properties = append(alert.Properties, chronograf.KapacitorProperty{
			Name: "endpoint",
			Args: []string{p.Endpoint},
		})
	}

	if len(p.Headers) > 0 {
		for k, v := range p.Headers {
			alert.Properties = append(alert.Properties, chronograf.KapacitorProperty{
				Name: "header",
				Args: []string{k, v},
			})
		}
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
		Name: "alerta",
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
		Name: "slack",
	}

	if s.Channel != "" {
		alert.Properties = append(alert.Properties, chronograf.KapacitorProperty{
			Name: "channel",
			Args: []string{s.Channel},
		})
	}

	if s.IconEmoji != "" {
		alert.Properties = append(alert.Properties, chronograf.KapacitorProperty{
			Name: "iconEmoji",
			Args: []string{s.IconEmoji},
		})
	}

	if s.Username != "" {
		alert.Properties = append(alert.Properties, chronograf.KapacitorProperty{
			Name: "username",
			Args: []string{s.Username},
		})
	}
	rule.AlertNodes = append(rule.AlertNodes, alert)
}
func extractTalk(node *pipeline.AlertNode, rule *chronograf.AlertRule) {
	if node.TalkHandlers == nil {
		return
	}
	rule.Alerts = append(rule.Alerts, "talk")
	alert := chronograf.KapacitorNode{
		Name: "talk",
	}

	rule.AlertNodes = append(rule.AlertNodes, alert)
}
func extractTelegram(node *pipeline.AlertNode, rule *chronograf.AlertRule) {
	if node.TelegramHandlers == nil {
		return
	}
	rule.Alerts = append(rule.Alerts, "telegram")
	t := node.TelegramHandlers[0]
	alert := chronograf.KapacitorNode{
		Name: "telegram",
	}

	if t.ChatId != "" {
		alert.Properties = append(alert.Properties, chronograf.KapacitorProperty{
			Name: "chatId",
			Args: []string{t.ChatId},
		})
	}

	if t.ParseMode != "" {
		alert.Properties = append(alert.Properties, chronograf.KapacitorProperty{
			Name: "parseMode",
			Args: []string{t.ParseMode},
		})
	}

	if t.IsDisableWebPagePreview {
		alert.Properties = append(alert.Properties, chronograf.KapacitorProperty{
			Name: "disableWebPagePreview",
		})
	}

	if t.IsDisableNotification {
		alert.Properties = append(alert.Properties, chronograf.KapacitorProperty{
			Name: "disableNotification",
		})
	}
	rule.AlertNodes = append(rule.AlertNodes, alert)
}

func extractTCP(node *pipeline.AlertNode, rule *chronograf.AlertRule) {
	if node.TcpHandlers == nil {
		return
	}
	rule.Alerts = append(rule.Alerts, "tcp")
	t := node.TcpHandlers[0]
	alert := chronograf.KapacitorNode{
		Name: "tcp",
	}

	if t.Address != "" {
		alert.Args = []string{t.Address}
	}

	rule.AlertNodes = append(rule.AlertNodes, alert)
}

func extractLog(node *pipeline.AlertNode, rule *chronograf.AlertRule) {
	if node.LogHandlers == nil {
		return
	}
	rule.Alerts = append(rule.Alerts, "log")
	log := node.LogHandlers[0]
	alert := chronograf.KapacitorNode{
		Name: "log",
	}

	if log.FilePath != "" {
		alert.Args = []string{log.FilePath}
	}

	rule.AlertNodes = append(rule.AlertNodes, alert)
}

func extractExec(node *pipeline.AlertNode, rule *chronograf.AlertRule) {
	if node.ExecHandlers == nil {
		return
	}
	rule.Alerts = append(rule.Alerts, "exec")
	exec := node.ExecHandlers[0]
	alert := chronograf.KapacitorNode{
		Name: "exec",
	}

	if len(exec.Command) != 0 {
		alert.Args = exec.Command
	}

	rule.AlertNodes = append(rule.AlertNodes, alert)
}
