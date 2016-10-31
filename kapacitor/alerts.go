package tickscripts

import (
	"bytes"
	"fmt"
	"log"
	"text/template"
	"time"

	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/influxdata/kapacitor/tick/stateful"
)

// Alert defines alerting strings in template rendering
type Alert struct {
	Service   string // Alerting service
	Operator  string // Operator for alert comparison
	Aggregate string // Statistic aggregate over window of data
}

// TickTemplate  task to be used by kapacitor
type TickTemplate string

// Threshold generates a tickscript template with an alert
func (a *Alert) Threshold() (TickTemplate, error) {
	if err := ValidateAlert(a); err != nil {
		return "", err
	}

	tickscript, err := execTemplate(ThresholdTemplate, a)
	if err != nil {
		return "", err
	}

	if err := validateTick(tickscript); err != nil {
		return "", err
	}

	return formatTick(tickscript)
}

// Relative creates a tickscript that alerts on relative changes over windows of data
func (a *Alert) Relative() (TickTemplate, error) {
	if err := ValidateAlert(a); err != nil {
		return "", err
	}

	tickscript, err := execTemplate(RelativeTemplate, a)
	if err != nil {
		return "", err
	}

	if err := validateTick(tickscript); err != nil {
		return "", err
	}

	return formatTick(tickscript)
}

// Deadman creates a tickscript that alerts when no data has been received for a time.
func (a *Alert) Deadman() (TickTemplate, error) {
	if err := ValidateAlert(a); err != nil {
		return "", err
	}

	tickscript, err := execTemplate(DeadmanTemplate, a)
	if err != nil {
		return "", err
	}

	if err := validateTick(tickscript); err != nil {
		return "", err
	}

	return formatTick(tickscript)
}

// ValidateAlert checks if the alert is a valid kapacitor alert service.
func ValidateAlert(alert *Alert) error {
	// Simple tick script to check alert service.
	// If a pipeline cannot be created then we know this is an invalid
	// service.  At least with this version of kapacitor!
	script := fmt.Sprintf("stream|from()|alert().%s()", alert.Service)
	return validateTick(script)
}

func formatTick(tickscript string) (TickTemplate, error) {
	node, err := ast.Parse(tickscript)
	if err != nil {
		log.Fatalf("parse execution: %s", err)
		return "", err
	}

	output := new(bytes.Buffer)
	node.Format(output, "", true)
	return TickTemplate(output.String()), nil
}

func validateTick(script string) error {
	scope := stateful.NewScope()
	_, err := pipeline.CreateTemplatePipeline(script, pipeline.StreamEdge, scope, &deadman{})
	return err
}

func execTemplate(tick string, alert *Alert) (string, error) {
	p := template.New("template")
	t, err := p.Parse(tick)
	if err != nil {
		log.Fatalf("template parse: %s", err)
		return "", err
	}
	buf := new(bytes.Buffer)
	err = t.Execute(buf, &alert)
	if err != nil {
		log.Fatalf("template execution: %s", err)
		return "", err
	}
	return buf.String(), nil
}

type deadman struct {
	interval  time.Duration
	threshold float64
	id        string
	message   string
	global    bool
}

func (d deadman) Interval() time.Duration { return d.interval }
func (d deadman) Threshold() float64      { return d.threshold }
func (d deadman) Id() string              { return d.id }
func (d deadman) Message() string         { return d.message }
func (d deadman) Global() bool            { return d.global }
